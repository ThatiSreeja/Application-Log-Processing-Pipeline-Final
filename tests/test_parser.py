import pytest
from src.parser import parse_log
from src.normalization import normalize_level, normalize_timestamp

def test_parse_json():
    result = parse_log('{"timestamp":"2026-11-10T09:30:00Z","level":"ERROR","service":"api","message":"failed"}')
    assert result["level"] == "ERROR"

def test_parse_text():
    result = parse_log("2026-11-10 09:31:00 INFO auth_service User login request_id=R1 latency_ms=120")
    assert result["request_id"] == "R1"
    assert result["latency_ms"] == 120.0

def test_malformed_text():
    with pytest.raises(ValueError):
        parse_log("not a valid log")

def test_normalization():
    assert normalize_level("warn") == "WARNING"
    assert normalize_timestamp("2026-11-10T09:30:00Z").endswith("Z")
    assert normalize_timestamp("bad") is None
