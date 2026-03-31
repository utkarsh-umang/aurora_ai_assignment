from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Optional, TypedDict


EventType = Literal[
    "scrape_request",
    "scrape_result",
    "voice_request",
    "voice_result",
]


class ScrapeRequest(TypedDict):
    event_type: Literal["scrape_request"]
    job_id: str
    refined_query: str


class ScrapeResult(TypedDict):
    event_type: Literal["scrape_result"]
    job_id: str
    businesses: list[dict]


class VoiceRequest(TypedDict):
    event_type: Literal["voice_request"]
    job_id: str
    call_id: str
    business: dict


class VoiceResult(TypedDict):
    event_type: Literal["voice_result"]
    job_id: str
    call_id: str
    result: dict


KafkaEvent = ScrapeRequest | ScrapeResult | VoiceRequest | VoiceResult


@dataclass(frozen=True)
class EventParseError(Exception):
    message: str
    raw: Any | None = None

    def __str__(self) -> str:  # pragma: no cover
        return self.message


def parse_event(raw: Any) -> KafkaEvent:
    """
    Parse a decoded JSON object into one of the supported KafkaEvent shapes.
    Raises EventParseError on invalid input.
    """
    if not isinstance(raw, dict):
        raise EventParseError("Event must be a JSON object", raw=raw)
    event_type = raw.get("event_type")
    if event_type not in {"scrape_request", "scrape_result", "voice_request", "voice_result"}:
        raise EventParseError("Unknown or missing event_type", raw=raw)

    # Shallow validation: just check the presence of required fields.
    if event_type == "scrape_request":
        if "job_id" not in raw or "refined_query" not in raw:
            raise EventParseError("Invalid scrape_request payload", raw=raw)
        return raw  # type: ignore[return-value]
    if event_type == "scrape_result":
        if "job_id" not in raw or "businesses" not in raw:
            raise EventParseError("Invalid scrape_result payload", raw=raw)
        return raw  # type: ignore[return-value]
    if event_type == "voice_request":
        if "job_id" not in raw or "call_id" not in raw or "business" not in raw:
            raise EventParseError("Invalid voice_request payload", raw=raw)
        return raw  # type: ignore[return-value]
    if event_type == "voice_result":
        if "job_id" not in raw or "call_id" not in raw or "result" not in raw:
            raise EventParseError("Invalid voice_result payload", raw=raw)
        return raw  # type: ignore[return-value]

    raise EventParseError("Unreachable event_type", raw=raw)


def make_scrape_request(*, job_id: str, refined_query: str) -> ScrapeRequest:
    return {"event_type": "scrape_request", "job_id": job_id, "refined_query": refined_query}


def make_scrape_result(*, job_id: str, businesses: list[dict]) -> ScrapeResult:
    return {"event_type": "scrape_result", "job_id": job_id, "businesses": businesses}


def make_voice_request(*, job_id: str, call_id: str, business: dict) -> VoiceRequest:
    return {"event_type": "voice_request", "job_id": job_id, "call_id": call_id, "business": business}


def make_voice_result(*, job_id: str, call_id: str, result: dict) -> VoiceResult:
    return {"event_type": "voice_result", "job_id": job_id, "call_id": call_id, "result": result}

