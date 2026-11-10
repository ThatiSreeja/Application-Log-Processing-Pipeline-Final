import json
from datetime import datetime, timezone

from scripts.generate_data import generate


def test_generator_uses_historical_timestamps_and_is_reproducible(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    generate(25, first, seed=42)
    generate(25, second, seed=42)

    first_lines = (first / "structured_logs.jsonl").read_text(encoding="utf-8").splitlines()
    second_lines = (second / "structured_logs.jsonl").read_text(encoding="utf-8").splitlines()
    assert first_lines == second_lines

    now = datetime.now(timezone.utc)
    valid_timestamps = []
    for line in first_lines[:-3]:  # exclude the three deliberate malformed/duplicate examples
        record = json.loads(line)
        value = record.get("timestamp")
        if value != "not-a-timestamp":
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            valid_timestamps.append(parsed)
    assert valid_timestamps
    assert all(value <= now for value in valid_timestamps)
