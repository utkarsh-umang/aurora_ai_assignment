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

load_dotenv()


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
    response = llm.invoke(
        [
            {
                "role": "system",
                "content": (
                    "You are a search query optimizer. Rewrite the user's query to be "
                    "more specific and effective for finding local businesses. "
                    "Return only the refined query, nothing else."
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


async def run_kafka_steps(*, refined_query: str, job_id: str) -> list[dict]:
    cfg = load_kafka_config()

    print(f"\n[2/3] Running scraper + voice AI via Kafka...")
    businesses = await request_scrape_and_wait(cfg=cfg, job_id=job_id, refined_query=refined_query)
    print(f"    Found {len(businesses)} businesses")
    for r in businesses:
        name = r.get("business_name")
        phone = r.get("phone_number")
        score = r.get("score")
        print(f"      - {name} | {phone} | score: {score}")

    voice_results = await request_voice_and_wait_all(cfg=cfg, job_id=job_id, businesses=businesses)
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
    app = build_refine_graph()
    refine_state = app.invoke({"user_query": query})
    refined_query = refine_state["refined_query"]

    voice_ai_results = asyncio.run(run_kafka_steps(refined_query=refined_query, job_id=job_id))
    final_answer = summarize_inline(user_query=query, voice_ai_results=voice_ai_results)

    print("\n" + "=" * 60)
    print("FINAL ANSWER")
    print("=" * 60)
    print(final_answer)
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
