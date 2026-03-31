import argparse
import asyncio
import json
import os
import sys
from uuid import uuid4

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

# Make sure sibling packages are importable when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from agent.kafka_async import load_kafka_config, request_scrape_and_wait, request_voice_and_wait_all
from agent.observability import RunTrace
from data.voice_ai import run_voice_ai_retry

load_dotenv()

_MEMORY_PATH = os.path.join(os.path.dirname(__file__), "..", "out", "memory.json")


def load_memory() -> list[dict]:
    if not os.path.exists(_MEMORY_PATH):
        return []
    try:
        with open(_MEMORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("conversations", [])
    except (json.JSONDecodeError, OSError):
        return []


def save_memory(conversations: list[dict], *, new_entry: dict) -> None:
    conversations.append(new_entry)
    os.makedirs(os.path.dirname(_MEMORY_PATH), exist_ok=True)
    with open(_MEMORY_PATH, "w", encoding="utf-8") as f:
        json.dump({"conversations": conversations}, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class PipelineState(TypedDict):
    user_query: str
    job_id: str
    refined_query: str
    voice_results: list[dict]      # all raw results from Kafka (any call_status)
    completed_results: list[dict]  # call_status=="completed" only (original + retried)
    retries_done: int
    final_answer: str


# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def refine_query(state: PipelineState) -> dict:
    print(f"\n[1/3] Refining query (LLM)...")

    conversations = load_memory()
    memory_block = ""
    if conversations:
        recent = conversations[-5:]
        lines = ["Previous searches (most recent last):"]
        for c in recent:
            lines.append(
                f"  - Query: \"{c.get('user_query', '')}\" → "
                f"Refined: \"{c.get('refined_query', '')}\" | "
                f"Passed: {c.get('passed_businesses', [])}"
            )
        memory_block = "\n" + "\n".join(lines) + "\n"

    response = llm.invoke(
        [
            {
                "role": "system",
                "content": (
                    "You are a search query optimizer. Rewrite the user's query to be "
                    "more specific and effective for finding local businesses. "
                    "Return only the refined query, nothing else."
                    + memory_block
                ),
            },
            {"role": "user", "content": state["user_query"]},
        ]
    )
    refined = response.content.strip()
    print(f"    Original : {state['user_query']}")
    print(f"    Refined  : {refined}")
    return {"refined_query": refined}


def summarize_inline(*, user_query: str, voice_ai_results: list[dict], postponed_businesses: list[dict] | None = None) -> str:
    print(f"\n[3/3] Generating final answer (LLM)...")
    results_text = json.dumps(voice_ai_results, indent=2)

    postponed_note = ""
    if postponed_businesses:
        names = [r.get("business_name", "Unknown") for r in postponed_businesses]
        postponed_note = (
            f"\n\nNote: The following businesses asked to be called back later and were not evaluated: "
            f"{', '.join(names)}. Please mention this clearly in your summary."
        )

    response = llm.invoke(
        [
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant summarizing business search results. "
                    "Given a user query and a list of evaluated businesses (each with a "
                    "pass/fail result and comment), provide a concise, friendly summary "
                    "of which businesses passed and give a clear recommendation."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"User query: {user_query}\n\n"
                    f"Evaluated businesses:\n{results_text}"
                    + postponed_note
                ),
            },
        ]
    )
    return response.content.strip()


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

def build_pipeline(*, trace: RunTrace) -> StateGraph:

    def run_voice_calls(state: PipelineState) -> dict:
        cfg = load_kafka_config()
        print(f"\n[2/3] Running scraper + voice AI via Kafka...")

        businesses = asyncio.run(
            request_scrape_and_wait(
                cfg=cfg, job_id=state["job_id"], refined_query=state["refined_query"], trace=trace
            )
        )
        print(f"    Found {len(businesses)} businesses")
        for b in businesses:
            print(f"      - {b.get('business_name')} | {b.get('phone_number')} | score: {b.get('score')}")

        voice_results = asyncio.run(
            request_voice_and_wait_all(
                cfg=cfg, job_id=state["job_id"], businesses=businesses, trace=trace
            )
        )
        completed = [r for r in voice_results if r.get("call_status") == "completed"]
        return {"voice_results": voice_results, "completed_results": completed, "retries_done": 0}

    def retry_failed_calls(state: PipelineState) -> dict:
        failed = [r for r in state["voice_results"] if r.get("call_status") == "failed"]
        print(f"\n[*] Retrying {len(failed)} failed call(s)...")
        retried = []
        for r in failed:
            print(f"    Retrying {r.get('business_name')} (waiting 2s)...")
            retried.append(run_voice_ai_retry(r))
        return {
            "completed_results": state["completed_results"] + retried,
            "retries_done": state["retries_done"] + 1,
        }

    def should_retry(state: PipelineState) -> str:
        failed = [r for r in state["voice_results"] if r.get("call_status") == "failed"]
        if failed and state["retries_done"] < 1:
            return "retry"
        return "summarize"

    def summarize_node(state: PipelineState) -> dict:
        postponed = [r for r in state["voice_results"] if r.get("call_status") == "callback_requested"]
        final_answer = summarize_inline(
            user_query=state["user_query"],
            voice_ai_results=state["completed_results"],
            postponed_businesses=postponed if postponed else None,
        )
        return {"final_answer": final_answer}

    graph = StateGraph(PipelineState)
    graph.add_node("refine_query", refine_query)
    graph.add_node("run_voice_calls", run_voice_calls)
    graph.add_node("retry_failed_calls", retry_failed_calls)
    graph.add_node("summarize", summarize_node)

    graph.add_edge(START, "refine_query")
    graph.add_edge("refine_query", "run_voice_calls")
    graph.add_conditional_edges("run_voice_calls", should_retry, {"retry": "retry_failed_calls", "summarize": "summarize"})
    graph.add_edge("retry_failed_calls", "summarize")
    graph.add_edge("summarize", END)

    return graph.compile()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Aurora AI business search pipeline")
    parser.add_argument("--query", type=str, help="The search query to process")
    args = parser.parse_args()

    query = args.query
    if not query:
        query = input("Enter your search query: ").strip()
    if not query:
        print("No query provided. Exiting.")
        sys.exit(1)

    job_id = str(uuid4())
    trace = RunTrace(job_id=job_id)
    trace.record("job_started", user_query=query)

    app = build_pipeline(trace=trace)
    final_state = app.invoke({
        "user_query": query,
        "job_id": job_id,
        "refined_query": "",
        "voice_results": [],
        "completed_results": [],
        "retries_done": 0,
        "final_answer": "",
    })

    refined_query = final_state["refined_query"]
    voice_results = final_state["voice_results"]
    completed_results = final_state["completed_results"]
    final_answer = final_state["final_answer"]

    trace.record("query_refined", original=query, refined=refined_query)
    trace.record("job_complete", final_answer=final_answer)
    trace.set_summary(
        user_query=query,
        refined_query=refined_query,
        businesses_found=len(voice_results),
        completed_calls=len(completed_results),
        passed=[r.get("business_name") for r in completed_results if r.get("pass_fail") == "pass"],
    )

    out_dir = os.path.join(os.path.dirname(__file__), "..", "out")
    trace_path = trace.write(out_dir)

    memory_entry = {
        "timestamp": trace.started_at,
        "job_id": job_id,
        "user_query": query,
        "refined_query": refined_query,
        "passed_businesses": [
            r.get("business_name") for r in completed_results if r.get("pass_fail") == "pass"
        ],
        "final_answer": final_answer,
    }
    conversations = load_memory()
    save_memory(conversations, new_entry=memory_entry)

    print("\n" + "=" * 60)
    print("FINAL ANSWER")
    print("=" * 60)
    print(final_answer)
    print("=" * 60 + "\n")
    print(f"[trace] Written to {trace_path}")


if __name__ == "__main__":
    main()
