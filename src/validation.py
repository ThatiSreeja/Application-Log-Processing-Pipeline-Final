import math
from datetime import datetime, timezone
from .config import REQUIRED_FIELDS, MAX_LATENCY_MS, MAX_FUTURE_TIMESTAMP_SECONDS


def validate_record(record):
    """Return human-readable validation failures for one normalized record."""
    reasons = []
    for field in REQUIRED_FIELDS:
        if record.get(field) is None or str(record.get(field)).strip() == "":
            reasons.append(f"Missing required field: {field}")
    if record.get("timestamp") is not None and not record.get("_normalized_timestamp"):
        reasons.append("Invalid timestamp")
    if record.get("level") is not None and not record.get("_normalized_level"):
        reasons.append("Unknown severity level")
    if record.get("latency_ms") is not None:
        try:
            latency = float(record["latency_ms"])
            if not math.isfinite(latency):
                reasons.append("latency_ms must be finite")
            elif latency < 0:
                reasons.append("Negative latency_ms")
            elif latency > MAX_LATENCY_MS:
                reasons.append(f"latency_ms exceeds configured maximum ({MAX_LATENCY_MS})")
        except (TypeError, ValueError):
            reasons.append("latency_ms must be numeric")
    normalized = record.get("_normalized_timestamp")
    if normalized:
        try:
            event_time = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            if (event_time - now).total_seconds() > MAX_FUTURE_TIMESTAMP_SECONDS:
                reasons.append("Timestamp is too far in the future")
        except (TypeError, ValueError):
            reasons.append("Invalid normalized timestamp")
    return reasons
