from pathlib import Path

def read_log_file(file_path):
    """Yield source line number and raw text, preserving traceability."""
    path = Path(file_path)
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            raw = line.rstrip("\r\n")
            if raw.strip():
                yield line_number, raw

def discover_input_files(input_dir):
    path = Path(input_dir)
    return sorted([p for p in path.iterdir() if p.is_file() and p.suffix.lower() in {".jsonl", ".json", ".txt", ".log"}])
