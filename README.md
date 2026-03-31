# Aurora AI Assignment

A multi-turn AI pipeline that takes a user query, scrapes relevant businesses, runs parallel AI voice calls to evaluate them, and summarises the results — all orchestrated with **LangGraph** and parallelised via **Kafka**.

---

## Quick Start: Run `conversation_1.json`

`conversations/conversation_1.json` contains two sequential user queries:

```json
{
  "conversations": [
    { "role": "user", "content": "I need a good plumber in San Francisco" },
    { "role": "user", "content": "what about electricians, same area" }
  ]
}
```

The `demo.sh` script runs these turns one after the other, so that memory written after turn 1 is available to the LLM during turn 2.

### Prerequisites

- Docker Desktop running
- Python virtual environment at `.venv/` with dependencies installed
- `.env` file with your `OPENAI_API_KEY` set (see `.env.example` if present)

```bash
# Install dependencies (first time only)
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run the demo

```bash
./demo.sh
```

What happens step by step:

1. Redpanda (Kafka-compatible broker) is restarted via Docker Compose
2. Four worker processes open in new Terminal windows:
   - 1 **scrape worker** — listens for scrape requests, returns a list of businesses
   - 3 **voice workers** — each handles voice AI evaluation calls in parallel
3. The script waits 5 seconds for workers to join their consumer groups
4. Each user message in `conversation_1.json` is fed to the pipeline in order:
   ```
   Turn 1: "I need a good plumber in San Francisco"
   Turn 2: "what about electricians, same area"
   ```
5. After all turns, a final summary line tells you where the output lives

You can also pass a different conversation file:

```bash
./demo.sh conversations/my_other_conversation.json
```

Or run a single query directly:

```bash
source .venv/bin/activate
python -m agent.run --query "I need a dentist in Austin"
```

---

## Inspecting the Output

### Trace files (`out/<job_id>/trace.json`)

Every run creates a new UUID-named directory under `out/`. Inside is `trace.json` — a full timestamped event log for that job.

```
out/
└── a3f7c821-...b2e/
    └── trace.json
```

**What's in `trace.json`:**

```jsonc
{
  "job_id": "a3f7c821-...",
  "started_at": "2026-04-01T10:23:00",
  "total_elapsed_s": 38.4,
  "summary": {
    "user_query": "I need a good plumber in San Francisco",
    "refined_query": "top-rated licensed plumber San Francisco",
    "businesses_found": 5,
    "completed_calls": 4,
    "passed": ["Apex Plumbing Co.", "ClearFlow Services"]
  },
  "events": [
    { "step": "job_started",               "elapsed_s": 0.001  },
    { "step": "scrape_request_published",  "elapsed_s": 0.015  },
    { "step": "scrape_result_received",    "elapsed_s": 3.2,   "businesses_count": 5 },
    { "step": "voice_requests_published",  "elapsed_s": 3.25,  "count": 5 },
    { "step": "voice_result_received",     "elapsed_s": 8.1,   "business_name": "Apex Plumbing Co.", "pass_fail": "pass" },
    { "step": "voice_result_received",     "elapsed_s": 11.3,  "business_name": "BlueWave Pros",     "pass_fail": "no pass" },
    // ... one event per result ...
    { "step": "voice_phase_complete",      "elapsed_s": 18.7,  "parallelism_note": "5 voice calls dispatched simultaneously; ..." },
    { "step": "query_refined",             "elapsed_s": 19.1,  "original": "...", "refined": "..." },
    { "step": "job_complete",              "elapsed_s": 38.4,  "final_answer": "..." }
  ]
}
```

Key things to look at:

| Field | What it tells you |
|---|---|
| `total_elapsed_s` | End-to-end wall clock time for the job |
| `summary.businesses_found` | How many businesses the scraper returned |
| `summary.passed` | Which businesses the voice AI approved |
| `voice_phase_complete.parallelism_note` | Confirms calls ran concurrently, not sequentially |
| Gap between `voice_requests_published` and `voice_phase_complete` | Actual Kafka round-trip latency for parallel voice calls |

### Memory file (`out/memory.json`)

After each turn the pipeline appends an entry to `out/memory.json`. This file persists across runs and lets the LLM recall what it found in previous conversations.

```jsonc
{
  "conversations": [
    {
      "timestamp": "2026-04-01T10:23:00",
      "job_id": "a3f7c821-...",
      "user_query": "I need a good plumber in San Francisco",
      "refined_query": "top-rated licensed plumber San Francisco",
      "passed_businesses": ["Apex Plumbing Co.", "ClearFlow Services"],
      "final_answer": "I found two highly-rated plumbers: Apex Plumbing Co. ..."
    },
    {
      "timestamp": "2026-04-01T10:24:45",
      "job_id": "b9d0e132-...",
      "user_query": "what about electricians, same area",
      // LLM saw turn 1 above and avoided re-evaluating plumbers
      ...
    }
  ]
}
```

On turn 2, the `refine_query` node loads the last 5 entries from `memory.json` and injects them into the LLM system prompt. This is how the assistant knows "we already found plumbers — now find electricians in the same area."

---

## System Design

### Overview

```
User Query
    │
    ▼
┌─────────────────────────────────────────────┐
│              LangGraph Pipeline             │
│                                             │
│  [1] refine_query ──────────────────────►  │
│        (LLM + memory context)               │
│                                             │
│  [2] run_voice_calls ──────────────────►   │
│        ├─ Publish ScrapeRequest (Kafka)     │
│        ├─ Wait for ScrapeResult (Kafka)     │
│        ├─ Publish N VoiceRequests (Kafka)   │
│        └─ Wait for N VoiceResults (Kafka)   │
│                                             │
│  [3] retry_failed_calls  (conditional) ──► │
│        (direct call, no Kafka)              │
│                                             │
│  [4] summarize ────────────────────────►   │
│        (LLM, completed results only)        │
└─────────────────────────────────────────────┘
    │
    ▼
 out/<job_id>/trace.json
 out/memory.json
```

### How LangGraph is used

The entire pipeline is a **LangGraph `StateGraph`** defined in `agent/run.py`. The graph carries a single typed state dict (`PipelineState`) through its nodes:

```python
class PipelineState(TypedDict):
    user_query: str
    job_id: str
    refined_query: str
    voice_results: list[dict]      # all results (completed + failed + callback)
    completed_results: list[dict]  # only call_status == "completed"
    retries_done: int
    final_answer: str
```

**Nodes:**

| Node | What it does |
|---|---|
| `refine_query` | Calls the LLM with the user query + last 5 memory entries; returns an optimised search string |
| `run_voice_calls` | Publishes Kafka events for scraping and voice evaluation; waits for all results |
| `retry_failed_calls` | Directly retries any failed voice calls (bypasses Kafka for speed); max 1 retry |
| `summarize` | Calls the LLM with the completed voice results; produces the final human-readable answer |

**Edges and conditional routing:**

```
START → refine_query → run_voice_calls → should_retry()?
                                              │
                              ┌───── yes ─────┤
                              ▼               │
                      retry_failed_calls      │ no
                              │               │
                              └──────┬────────┘
                                     ▼
                                 summarize → END
```

`should_retry()` returns `"retry"` only if there are failed calls **and** `retries_done < 1`. This gives you clean conditional branching without any `if/else` inside the orchestration layer — the graph structure *is* the control flow.

The main benefit of LangGraph here is that the pipeline is **declarative and inspectable**: every node is a pure function over state, and you can swap, add, or reorder nodes without touching the others.

### How Kafka is used

Kafka (via **Redpanda**) provides the parallelism layer. Instead of calling the scraper and voice AI directly, the orchestrator publishes events and workers consume them — decoupling the pipeline from the I/O-heavy work.

**Topics:**

| Topic | Direction | Purpose |
|---|---|---|
| `aurora.scrape` | CLI → scrape worker → CLI | Request businesses; receive list |
| `aurora.voice` | CLI → voice workers → CLI | Request voice call; receive result |

**Scrape phase (serial):**

```
CLI                          Scrape Worker
 │── ScrapeRequest ─────────────► │
 │                                │  run_scraper()  ~3s
 │◄── ScrapeResult ───────────────│
 │    {businesses: [...]}         │
```

**Voice phase (parallel — the key win):**

```
CLI            Voice Worker 1    Voice Worker 2    Voice Worker 3
 │── VoiceRequest(biz_1) ──►│
 │── VoiceRequest(biz_2) ────────────►│
 │── VoiceRequest(biz_3) ─────────────────────►│
 │── VoiceRequest(biz_4) ──►│              (rebalanced)
 │── VoiceRequest(biz_5) ────────────►│
 │                          │               │               │
 │                    run_voice_ai()  run_voice_ai()  run_voice_ai()
 │                    ~5–10s each     ~5–10s each     ~5–10s each
 │◄── VoiceResult(biz_3) ─────────────────────│
 │◄── VoiceResult(biz_1) ──────────│
 │◄── VoiceResult(biz_2) ─────────────────────────────────│
 │   ... (results arrive out of order, CLI collects all N)
```

All voice requests are published in a single burst. The three voice workers pick up requests from their partitions concurrently, so 5 calls that take ~8s each complete in ~10s wall-clock time rather than ~40s. The trace file records `first_result_at_s`, `last_result_at_s`, and `results_spread_s` so you can verify this directly.

**Consumer/producer setup:**

- Each CLI invocation creates a **one-shot consumer** with a unique `group_id` so it only sees its own job's results (no cross-job contamination).
- Workers use `auto_offset_reset="earliest"` so they never miss a message; the CLI uses `"latest"` since it subscribes before publishing.
- The `aurora.scrape` topic has 6 partitions; `aurora.voice` also has 6 partitions, meaning up to 6 voice workers can process in parallel before partitions become the bottleneck.

### Where LangGraph and Kafka meet

The `run_voice_calls` node is the integration point. It is an `async` function inside a LangGraph node that:

1. Calls `request_scrape_and_wait()` — a coroutine in `agent/kafka_async.py` that publishes and awaits a single Kafka reply
2. Calls `request_voice_and_wait_all()` — a coroutine that fans out N requests and collects N replies

LangGraph provides the **orchestration skeleton** (what runs, in what order, under what conditions); Kafka provides the **parallelism substrate** (how the heavy I/O work is distributed across workers). Neither component needs to know about the other's internals.

### Retry logic

If any voice calls return `call_status: "failed"`, the `should_retry` edge routes to `retry_failed_calls`. This node calls `run_voice_ai_retry()` **directly** (no Kafka round-trip) with a shorter 2-second sleep and guaranteed `"completed"` status. After one retry pass, the graph always proceeds to `summarize` regardless of remaining failures.

Businesses that returned `callback_requested` are never retried — the summarise node explicitly notes them in the final answer as "will call back later."

---

## Project Structure

```
aurora_ai_assignment/
├── agent/
│   ├── run.py            # LangGraph pipeline + CLI entry point
│   ├── worker.py         # Kafka consumer workers (scrape + voice)
│   ├── kafka_async.py    # Async publish/subscribe helpers
│   ├── kafka_events.py   # Event type definitions
│   └── observability.py  # RunTrace: timestamped event collector
├── data/
│   ├── scraper.py        # Mock web scraper (replace with real one)
│   └── voice_ai.py       # Mock voice AI (replace with real one)
├── conversations/
│   └── conversation_1.json
├── out/                  # Created at runtime
│   ├── <job_id>/
│   │   └── trace.json
│   └── memory.json
├── docker-compose.yml    # Redpanda (Kafka-compatible broker)
├── requirements.txt
├── demo.sh               # End-to-end demo runner
└── .env                  # OPENAI_API_KEY, Kafka settings
```
