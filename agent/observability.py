from __future__ import annotations

import json
import os
import time
from typing import Any


class RunTrace:
    """
    Collects timestamped events for a single pipeline run and writes them to disk.

    All elapsed_s values are seconds since the trace was created, making it easy
    to reconstruct the timeline and verify that Kafka parallelism is working
    (voice requests published nearly simultaneously; results arriving in a burst).
    """

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        self._t0 = time.monotonic()
        self.started_at = time.strftime("%Y-%m-%dT%H:%M:%S")
        self._events: list[dict] = []
        self._summary: dict[str, Any] = {}

    def _elapsed(self) -> float:
        return round(time.monotonic() - self._t0, 3)

    def record(self, step: str, **data: Any) -> float:
        """Append a timestamped event. Returns elapsed_s at the moment of recording."""
        elapsed = self._elapsed()
        self._events.append({"step": step, "elapsed_s": elapsed, **data})
        return elapsed

    def set_summary(self, **data: Any) -> None:
        """Accumulate top-level summary fields (merged on repeated calls)."""
        self._summary.update(data)

    def write(self, out_dir: str) -> str:
        """Serialise the trace to out_dir/{job_id}/trace.json and return the path."""
        job_dir = os.path.join(out_dir, self.job_id)
        os.makedirs(job_dir, exist_ok=True)
        path = os.path.join(job_dir, "trace.json")
        payload = {
            "job_id": self.job_id,
            "started_at": self.started_at,
            "total_elapsed_s": self._elapsed(),
            "summary": self._summary,
            "events": self._events,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        return path
