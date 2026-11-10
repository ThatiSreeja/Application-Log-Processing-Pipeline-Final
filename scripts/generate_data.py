import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

LEVELS = ["INFO", "INFO", "INFO", "WARNING", "ERROR", "DEBUG", "FATAL"]
SERVICES = ["auth_service", "payment_service", "order_service"]

def generate(count, output_dir, seed=42):
    random.seed(seed)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "structured_logs.jsonl"
    text_path = output_dir / "application_logs.txt"
    # Use a fixed historical anchor so generated timestamps are reproducible
    # and never accidentally land in the future during validation.
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with json_path.open("w", encoding="utf-8") as jf, text_path.open("w", encoding="utf-8") as tf:
        for i in range(count):
            ts = start + timedelta(seconds=random.randint(0, max(count, 1) * 3))
            level = random.choice(LEVELS)
            service = random.choice(SERVICES)
            latency = random.randint(5, 1500)
            event = {
                "timestamp": ts.isoformat().replace("+00:00", "Z"),
                "level": level.lower() if i % 7 == 0 else level,
                "service": service,
                "message": f"Generated {service} event {i}",
                "request_id": f"REQ{i:07d}",
                "latency_ms": latency
            }
            if i % 101 == 0:
                event["optional_trace"] = f"trace-{i}"
            if i % 137 == 0:
                event["latency_ms"] = -5
            if i % 173 == 0:
                event.pop("service", None)
            if i % 199 == 0:
                event["timestamp"] = "not-a-timestamp"
            jf.write(json.dumps(event) + "\n")

            text_event = {
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "level": level,
                "service": service,
                "message": f"Text event {i}",
                "request_id": f"TXT{i:07d}",
                "latency_ms": latency
            }
            tf.write(
                f"{text_event['timestamp']} {level} {service} {text_event['message']} "
                f"request_id={text_event['request_id']} latency_ms={latency}\n"
            )

    # Add explicit malformed, duplicate and empty-field examples to the JSONL input.
    with json_path.open("a", encoding="utf-8") as jf:
        jf.write("{this is malformed json}\n")
        jf.write(json.dumps({
            "timestamp": "2026-11-10T10:00:00Z", "level": "INFO",
            "service": "auth_service", "message": "intentional duplicate",
            "request_id": "DUP1", "latency_ms": 10
        }) + "\n")
        jf.write(json.dumps({
            "timestamp": "2026-11-10T10:00:00Z", "level": "INFO",
            "service": "auth_service", "message": "intentional duplicate",
            "request_id": "DUP1", "latency_ms": 10
        }) + "\n")
    print(f"Generated {count} JSONL and {count} text records in {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--output-dir", default="data/input")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate(args.count, args.output_dir, args.seed)
