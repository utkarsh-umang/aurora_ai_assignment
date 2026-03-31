import random
import time

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
        The same dict with call_status, pass_fail, and comment added.
        call_status: "completed" (70%), "failed" (20%), "callback_requested" (10%).
        pass_fail and comment are None for non-completed calls.
    """
    latency = random.uniform(5.0, 10.0)
    time.sleep(latency)

    roll = random.random()
    if roll < 0.20:
        call_status = "failed"
    elif roll < 0.30:
        call_status = "callback_requested"
    else:
        call_status = "completed"

    result = dict(business)
    result["call_status"] = call_status

    if call_status == "completed":
        passed = random.random() < 0.50  # 50/50
        result["pass_fail"] = "pass" if passed else "no pass"
        result["comment"] = random.choice(_PASS_COMMENTS if passed else _FAIL_COMMENTS)
    else:
        result["pass_fail"] = None
        result["comment"] = None

    return result


def run_voice_ai_retry(business: dict) -> dict:
    """
    Direct retry for a failed/deferred call — bypasses Kafka.
    Sleeps 2 seconds, then returns 50/50 pass/fail with call_status="completed".
    Only called once per business (no second retry).
    """
    time.sleep(2)
    passed = random.random() < 0.50
    result = dict(business)
    result["call_status"] = "completed"
    result["pass_fail"] = "pass" if passed else "no pass"
    result["comment"] = random.choice(_PASS_COMMENTS if passed else _FAIL_COMMENTS)
    return result
