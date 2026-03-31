import random

_PASS_COMMENTS = [
    "Business has high ratings and responsive communication.",
    "Score exceeds threshold; call went smoothly with clear intent match.",
    "Positive sentiment detected; business confirmed service availability.",
    "Strong match to query criteria; representative was helpful.",
    "Verified availability and pricing aligned with user expectations.",
]

_FAIL_COMMENTS = [
    "Business did not answer; unable to confirm service offering.",
    "Score below threshold; representative could not meet query requirements.",
    "Service area mismatch detected during call.",
    "Business closed or no longer operating in the requested category.",
    "Negative sentiment detected; customer experience concerns raised.",
]


def run_voice_ai(business: dict) -> dict:
    """
    Mock voice AI that evaluates a single business against the user's goal.

    Args:
        business: A dict with keys business_name, phone_number, score.

    Returns:
        The same dict with pass_fail ("pass" or "no pass") and comment added.
    """
    passed = random.random() > 0.4  # ~60% pass rate

    result = dict(business)
    result["pass_fail"] = "pass" if passed else "no pass"
    result["comment"] = random.choice(_PASS_COMMENTS if passed else _FAIL_COMMENTS)

    return result
