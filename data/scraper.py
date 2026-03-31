import random

_BUSINESS_NAMES = [
    "Apex Plumbing Co.",
    "BlueWave Services",
    "ClearFlow Solutions",
    "Delta Home Repair",
    "Eagle Eye Contractors",
    "FastFix Pros",
    "GreenLeaf Utilities",
    "Horizon Service Group",
    "IronClad Maintenance",
    "JetStream Plumbing",
]

_PHONE_PREFIXES = ["(512)", "(737)", "(210)", "(830)", "(361)"]


def _random_phone() -> str:
    prefix = random.choice(_PHONE_PREFIXES)
    number = f"{random.randint(200, 999)}-{random.randint(1000, 9999)}"
    return f"{prefix} {number}"


def run_scraper(refined_query: str) -> list[dict]:
    """
    Mock scraper that returns a list of businesses matching the refined query.

    Args:
        refined_query: The LLM-refined search query.

    Returns:
        A list of dicts with keys: business_name, phone_number, score.
    """
    count = random.randint(3, 6)
    businesses = random.sample(_BUSINESS_NAMES, count)

    results = []
    for name in businesses:
        results.append(
            {
                "business_name": name,
                "phone_number": _random_phone(),
                "score": random.randint(0, 100),
            }
        )

    return results
