from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from agent.kafka_events import (
    make_scrape_request,
    make_voice_request,
    parse_event,
)

if TYPE_CHECKING:
    from agent.observability import RunTrace


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or not str(v).strip():
        raise RuntimeError(f"Missing required environment variable: {name}")
    return str(v).strip()


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    scrape_topic: str
    voice_topic: str


def load_kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS"),
        scrape_topic=_env("KAFKA_SCRAPE_TOPIC", "aurora.scrape"),
        voice_topic=_env("KAFKA_VOICE_TOPIC", "aurora.voice"),
    )


async def ensure_topics(cfg: KafkaConfig, num_partitions: int = 6) -> None:
    """
    Create the scrape and voice topics with num_partitions partitions if they don't
    already exist. Must be called before workers start consuming so that Kafka can
    distribute partitions across all worker instances in the consumer group.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=cfg.bootstrap_servers)
    await admin.start()
    try:
        for topic in (cfg.scrape_topic, cfg.voice_topic):
            try:
                await admin.create_topics([
                    NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)
                ])
                print(f"[admin] Created topic {topic!r} with {num_partitions} partitions")
            except TopicAlreadyExistsError:
                print(f"[admin] Topic {topic!r} already exists (partition count unchanged)")
    finally:
        await admin.close()


def _json_dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def _json_loads(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


async def _new_producer(cfg: KafkaConfig) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(bootstrap_servers=cfg.bootstrap_servers)
    await producer.start()
    return producer


async def _new_consumer(*, cfg: KafkaConfig, topic: str, group_id: str) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=cfg.bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="latest",
    )
    await consumer.start()
    return consumer


async def request_scrape_and_wait(
    *,
    cfg: KafkaConfig,
    job_id: str,
    refined_query: str,
    timeout_s: float = 60.0,
    trace: RunTrace | None = None,
) -> list[dict]:
    """
    Publish ScrapeRequest and wait for the matching ScrapeResult.
    Uses the same topic for request+result; filters by (event_type, job_id).
    """
    group_id = f"aurora-cli-scrape-{uuid4()}"
    consumer = await _new_consumer(cfg=cfg, topic=cfg.scrape_topic, group_id=group_id)
    producer = await _new_producer(cfg)
    try:
        await producer.send_and_wait(cfg.scrape_topic, _json_dumps(make_scrape_request(job_id=job_id, refined_query=refined_query)))
        if trace:
            trace.record("scrape_request_published", topic=cfg.scrape_topic, job_id=job_id, refined_query=refined_query)

        deadline = time.monotonic() + timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for scrape_result for job_id={job_id}")
            msg = await asyncio.wait_for(consumer.getone(), timeout=remaining)
            evt = parse_event(_json_loads(msg.value))
            if evt["event_type"] == "scrape_result" and evt["job_id"] == job_id:
                businesses = list(evt["businesses"])
                if trace:
                    trace.record(
                        "scrape_result_received",
                        job_id=job_id,
                        businesses_count=len(businesses),
                        businesses=businesses,
                    )
                return businesses
    finally:
        await producer.stop()
        await consumer.stop()


async def request_voice_and_wait_all(
    *,
    cfg: KafkaConfig,
    job_id: str,
    businesses: list[dict],
    timeout_s: float = 120.0,
    trace: RunTrace | None = None,
) -> list[dict]:
    """
    Publish VoiceRequest per business and wait until VoiceResult count == len(businesses).
    Returns list of per-business result dicts (from voice service).
    """
    expected = len(businesses)
    if expected == 0:
        return []

    group_id = f"aurora-cli-voice-{uuid4()}"
    consumer = await _new_consumer(cfg=cfg, topic=cfg.voice_topic, group_id=group_id)
    producer = await _new_producer(cfg)
    try:
        call_id_to_business: dict[str, str] = {}
        published: list[dict] = []
        for business in businesses:
            call_id = str(uuid4())
            call_id_to_business[call_id] = business.get("business_name", call_id)
            await producer.send_and_wait(
                cfg.voice_topic,
                _json_dumps(make_voice_request(job_id=job_id, call_id=call_id, business=business)),
            )
            published.append({"call_id": call_id, "business_name": business.get("business_name")})

        if trace:
            trace.record(
                "voice_requests_published",
                topic=cfg.voice_topic,
                job_id=job_id,
                count=expected,
                requests=published,
            )

        deadline = time.monotonic() + timeout_s
        seen: set[str] = set()
        results: list[dict] = []
        first_result_at: float | None = None

        while len(seen) < expected:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out waiting for voice_result for job_id={job_id} "
                    f"({len(seen)}/{expected} received)"
                )
            msg = await asyncio.wait_for(consumer.getone(), timeout=remaining)
            evt = parse_event(_json_loads(msg.value))
            if evt["event_type"] != "voice_result":
                continue
            if evt["job_id"] != job_id:
                continue
            call_id = evt["call_id"]
            if call_id in seen:
                continue
            seen.add(call_id)
            result = dict(evt["result"])
            results.append(result)

            if trace:
                received_at = trace.record(
                    "voice_result_received",
                    call_id=call_id,
                    business_name=call_id_to_business.get(call_id),
                    pass_fail=result.get("pass_fail"),
                    received_count=len(seen),
                    total_expected=expected,
                )
                if first_result_at is None:
                    first_result_at = received_at

        if trace and first_result_at is not None:
            last_result_at = trace._elapsed()
            spread_s = round(last_result_at - first_result_at, 3)
            trace.record(
                "voice_phase_complete",
                total_results=len(results),
                first_result_at_s=first_result_at,
                last_result_at_s=last_result_at,
                results_spread_s=spread_s,
                parallelism_note=(
                    f"{expected} voice calls dispatched simultaneously; "
                    f"all results received over {spread_s}s "
                    f"(vs ~{expected}x longer if sequential)"
                ),
            )

        return results
    finally:
        await producer.stop()
        await consumer.stop()

