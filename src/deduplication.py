import hashlib, json
def stable_record_id(source_file, source_line_number, raw_log):
    return hashlib.sha256(f"{source_file}|{source_line_number}|{raw_log}".encode()).hexdigest()
def event_fingerprint(record):
    keys=("timestamp","level","service","message","request_id","latency_ms")
    payload={k:record.get(k) for k in keys}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,default=str).encode()).hexdigest()
