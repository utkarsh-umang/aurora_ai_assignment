from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Optional
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from agent.kafka_events import (
    make_scrape_request,
    make_voice_request,
    parse_event,
)


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
        deadline = time.monotonic() + timeout_s
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Timed out waiting for scrape_result for job_id={job_id}")
            msg = await asyncio.wait_for(consumer.getone(), timeout=remaining)
            evt = parse_event(_json_loads(msg.value))
            if evt["event_type"] == "scrape_result" and evt["job_id"] == job_id:
                return list(evt["businesses"])
    finally:
        await producer.stop()
        await consumer.stop()


async def request_voice_and_wait_all(
    *,
    cfg: KafkaConfig,
    job_id: str,
    businesses: list[dict],
    timeout_s: float = 90.0,
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
        call_ids: list[str] = []
        for business in businesses:
            call_id = str(uuid4())
            call_ids.append(call_id)
            await producer.send_and_wait(
                cfg.voice_topic,
                _json_dumps(make_voice_request(job_id=job_id, call_id=call_id, business=business)),
            )

        deadline = time.monotonic() + timeout_s
        seen: set[str] = set()
        results: list[dict] = []
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
            results.append(dict(evt["result"]))

        return results
    finally:
        await producer.stop()
        await consumer.stop()

