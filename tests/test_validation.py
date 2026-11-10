from src.validation import validate_record

def test_missing_service_is_rejected():
    record = {"timestamp":"x","level":"INFO","message":"hello",
              "_normalized_timestamp":"2026-01-01T00:00:00Z","_normalized_level":"INFO"}
    assert "Missing required field: service" in validate_record(record)

def test_negative_latency_is_rejected():
    record = {"timestamp":"x","level":"INFO","service":"api","message":"hello",
              "latency_ms":-1,"_normalized_timestamp":"2026-01-01T00:00:00Z",
              "_normalized_level":"INFO"}
    assert "Negative latency_ms" in validate_record(record)

def test_excessive_latency_is_rejected():
    record = {"timestamp": "x", "level": "INFO", "service": "api", "message": "ok",
              "latency_ms": 4_000_000, "_normalized_timestamp": "2026-01-01T00:00:00Z",
              "_normalized_level": "INFO"}
    assert any("configured maximum" in reason for reason in validate_record(record))

def test_infinite_latency_is_rejected():
    record = {"timestamp": "x", "level": "INFO", "service": "api", "message": "ok",
              "latency_ms": float("inf"), "_normalized_timestamp": "2026-01-01T00:00:00Z",
              "_normalized_level": "INFO"}
    assert "latency_ms must be finite" in validate_record(record)
