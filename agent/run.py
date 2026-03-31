import argparse
import json
import operator
import os
import sys
from typing import Annotated

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send
from typing_extensions import TypedDict

# Make sure sibling packages are importable when run directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.scraper import run_scraper
from data.voice_ai import run_voice_ai

load_dotenv()


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class PipelineState(TypedDict):
    user_query: str
    refined_query: str
    scraper_results: list[dict]
    voice_ai_results: Annotated[list[dict], operator.add]
    final_answer: str


# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0.3)


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------

def refine_query(state: PipelineState) -> dict:
    print(f"\n[1/4] Refining query...")
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


def scrape(state: PipelineState) -> dict:
    print(f"\n[2/4] Running scraper...")
    results = run_scraper(state["refined_query"])
    print(f"    Found {len(results)} businesses")
    for r in results:
        print(f"      - {r['business_name']} | {r['phone_number']} | score: {r['score']}")
    return {"scraper_results": results}


def voice_ai_fan_out(state: PipelineState) -> list[Send]:
    print(f"\n[3/4] Fanning out to voice AI ({len(state['scraper_results'])} parallel nodes)...")
    return [Send("voice_ai_process", business) for business in state["scraper_results"]]

# Routing function used as a conditional edge (not a node)
def _route_voice_ai(state: PipelineState) -> list[Send]:
    return voice_ai_fan_out(state)


def voice_ai_process(business: dict) -> dict:
    result = run_voice_ai(business)
    return {"voice_ai_results": [result]}


def summarize(state: PipelineState) -> dict:
    print(f"\n[4/4] Generating final answer...")
    results_text = json.dumps(state["voice_ai_results"], indent=2)
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
                    f"User query: {state['user_query']}\n\n"
                    f"Evaluated businesses:\n{results_text}"
                ),
            },
        ]
    )
    return {"final_answer": response.content.strip()}


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

def build_graph() -> StateGraph:
    graph = StateGraph(PipelineState)

    graph.add_node("refine_query", refine_query)
    graph.add_node("scrape", scrape)
    graph.add_node("voice_ai_process", voice_ai_process)
    graph.add_node("summarize", summarize)

    graph.add_edge(START, "refine_query")
    graph.add_edge("refine_query", "scrape")
    graph.add_conditional_edges("scrape", _route_voice_ai, ["voice_ai_process"])
    graph.add_edge("voice_ai_process", "summarize")
    graph.add_edge("summarize", END)

    return graph.compile()


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

    app = build_graph()

    final_state = app.invoke({"user_query": query, "voice_ai_results": []})

    print("\n" + "=" * 60)
    print("FINAL ANSWER")
    print("=" * 60)
    print(final_state["final_answer"])
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
