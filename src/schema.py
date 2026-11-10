"""Explicit source contract and schema-drift classification."""
SCHEMA_VERSION = "1.0"
REQUIRED_TYPES = {"timestamp": str, "level": str, "service": str, "message": str}
OPTIONAL_TYPES = {"request_id": str, "latency_ms": (int, float)}
KNOWN_FIELDS = set(REQUIRED_TYPES) | set(OPTIONAL_TYPES)

def inspect_schema(record):
    """Return (errors, unexpected_fields); additive fields are safe and retained."""
    errors = []
    for field, expected in {**REQUIRED_TYPES, **OPTIONAL_TYPES}.items():
        if field not in record or record[field] is None:
            continue  # required-field presence is handled by validate_record
        if not isinstance(record[field], expected) or isinstance(record[field], bool):
            label = expected.__name__ if isinstance(expected, type) else "numeric"
            errors.append(f"Invalid type for field '{field}': expected {label}")
    unexpected = sorted(set(record) - KNOWN_FIELDS)
    return errors, unexpected
