from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv

# Make sure sibling packages are importable when run directly
_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, _PROJECT_ROOT)
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

from agent.kafka_async import KafkaConfig, ensure_topics, load_kafka_config
from agent.kafka_events import (
    make_scrape_result,
    make_voice_result,
    parse_event,
)
from data.scraper import run_scraper
from data.voice_ai import run_voice_ai


def _json_dumps(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


def _json_loads(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))


async def _producer(cfg: KafkaConfig) -> AIOKafkaProducer:
    p = AIOKafkaProducer(bootstrap_servers=cfg.bootstrap_servers)
    await p.start()
    return p


async def _consumer(cfg: KafkaConfig, *, topic: str, group_id: str) -> AIOKafkaConsumer:
    c = AIOKafkaConsumer(
        topic,
        bootstrap_servers=cfg.bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )
    await c.start()
    return c


async def run_scrape_worker(cfg: KafkaConfig, *, group_id: str) -> None:
    await ensure_topics(cfg)
    consumer = await _consumer(cfg, topic=cfg.scrape_topic, group_id=group_id)
    producer = await _producer(cfg)
    try:
        print(f"[scrape_worker] listening on topic={cfg.scrape_topic} group_id={group_id}")
        async for msg in consumer:
            evt = parse_event(_json_loads(msg.value))
            if evt["event_type"] != "scrape_request":
                continue

            job_id = evt["job_id"]
            refined_query = evt["refined_query"]
            print(f"[scrape_worker] job_id={job_id} query={refined_query!r}")
            loop = asyncio.get_running_loop()
            businesses = await loop.run_in_executor(None, run_scraper, refined_query)
            await producer.send_and_wait(cfg.scrape_topic, _json_dumps(make_scrape_result(job_id=job_id, businesses=businesses)))
            print(f"[scrape_worker] job_id={job_id} produced scrape_result businesses={len(businesses)}")
    finally:
        await producer.stop()
        await consumer.stop()


async def run_voice_worker(cfg: KafkaConfig, *, group_id: str) -> None:
    await ensure_topics(cfg)
    consumer = await _consumer(cfg, topic=cfg.voice_topic, group_id=group_id)
    producer = await _producer(cfg)
    try:
        print(f"[voice_worker] listening on topic={cfg.voice_topic} group_id={group_id}")
        async for msg in consumer:
            evt = parse_event(_json_loads(msg.value))
            if evt["event_type"] != "voice_request":
                continue

            job_id = evt["job_id"]
            call_id = evt["call_id"]
            business = evt["business"]
            print(f"[voice_worker] job_id={job_id} call_id={call_id} business={business.get('business_name')!r}")
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, run_voice_ai, business)
            await producer.send_and_wait(cfg.voice_topic, _json_dumps(make_voice_result(job_id=job_id, call_id=call_id, result=result)))
            print(f"[voice_worker] job_id={job_id} call_id={call_id} produced voice_result")
    finally:
        await producer.stop()
        await consumer.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Aurora Kafka workers")
    sub = parser.add_subparsers(dest="worker", required=True)

    scrape_p = sub.add_parser("scrape", help="Run scrape worker")
    scrape_p.add_argument("--group-id", type=str, default=os.getenv("KAFKA_SCRAPE_GROUP_ID", "aurora-scrape-worker"))

    voice_p = sub.add_parser("voice", help="Run voice worker")
    voice_p.add_argument("--group-id", type=str, default=os.getenv("KAFKA_VOICE_GROUP_ID", "aurora-voice-worker"))

    args = parser.parse_args()
    cfg = load_kafka_config()

    if args.worker == "scrape":
        asyncio.run(run_scrape_worker(cfg, group_id=args.group_id))
    elif args.worker == "voice":
        asyncio.run(run_voice_worker(cfg, group_id=args.group_id))
    else:
        raise SystemExit(f"Unknown worker: {args.worker}")


if __name__ == "__main__":
    main()

