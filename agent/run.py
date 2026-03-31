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


def handle_retries(voice_ai_results: list[dict]) -> list[dict]:
    """
    Prompt user for failed/callback calls. Returns only completed results
    (originals + accepted retries). Declined businesses are excluded from the summary.
    """
    completed = []
    for result in voice_ai_results:
        status = result.get("call_status", "completed")
        name = result.get("business_name", "Unknown Business")

        if status == "failed":
            ans = input(f"\nCall with {name} failed. Retry? (y/n): ").strip().lower()
            if ans == "y":
                print(f"  Retrying {name} (waiting 2s)...")
                completed.append(run_voice_ai_retry(result))
        elif status == "callback_requested":
            ans = input(f"\n{name} asked to call back later. Retry? (y/n): ").strip().lower()
            if ans == "y":
                print(f"  Retrying {name}...")
                completed.append(run_voice_ai_retry(result))
        else:
            completed.append(result)
    return completed


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class PipelineState(TypedDict):
    user_query: str
    refined_query: str


# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)


# ---------------------------------------------------------------------------
# Node: refine query (LangGraph)
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


def summarize_inline(*, user_query: str, voice_ai_results: list[dict]) -> str:
    print(f"\n[3/3] Generating final answer (LLM)...")
    results_text = json.dumps(voice_ai_results, indent=2)
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
                ),
            },
        ]
    )
    return response.content.strip()


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

def build_refine_graph() -> StateGraph:
    graph = StateGraph(PipelineState)
    graph.add_node("refine_query", refine_query)
    graph.add_edge(START, "refine_query")
    graph.add_edge("refine_query", END)
    return graph.compile()


async def run_kafka_steps(*, refined_query: str, job_id: str, trace: RunTrace) -> list[dict]:
    cfg = load_kafka_config()

    print(f"\n[2/3] Running scraper + voice AI via Kafka...")
    businesses = await request_scrape_and_wait(cfg=cfg, job_id=job_id, refined_query=refined_query, trace=trace)
    print(f"    Found {len(businesses)} businesses")
    for r in businesses:
        name = r.get("business_name")
        phone = r.get("phone_number")
        score = r.get("score")
        print(f"      - {name} | {phone} | score: {score}")

    voice_results = await request_voice_and_wait_all(cfg=cfg, job_id=job_id, businesses=businesses, trace=trace)
    return voice_results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Aurora AI business search pipeline")
    parser.add_argument(
        "--query",
        type=str,
        help="The search query to process",
    )
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

    app = build_refine_graph()
    refine_state = app.invoke({"user_query": query})
    refined_query = refine_state["refined_query"]
    trace.record("query_refined", original=query, refined=refined_query)

    voice_ai_results = asyncio.run(run_kafka_steps(refined_query=refined_query, job_id=job_id, trace=trace))

    completed_results = handle_retries(voice_ai_results)

    final_answer = summarize_inline(user_query=query, voice_ai_results=completed_results)
    trace.record("job_complete", final_answer=final_answer)

    trace.set_summary(
        user_query=query,
        refined_query=refined_query,
        businesses_found=len(voice_ai_results),
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
