import json, re
TEXT_PATTERN = re.compile(r"^(?P<timestamp>\S+(?:[ T]\S+)?)\s+(?P<level>[A-Za-z]+)\s+(?P<service>\S+)\s+(?P<message>.*?)(?:\s+request_id=(?P<request_id>\S+))?(?:\s+latency_ms=(?P<latency_ms>-?\d+(?:\.\d+)?))?$")
def parse_log(raw):
    try:
        value=json.loads(raw)
        if not isinstance(value,dict): raise ValueError("JSON log must be an object")
        return value
    except json.JSONDecodeError:
        m=TEXT_PATTERN.match(raw.strip())
        if not m: raise ValueError("Malformed JSON or unsupported plain-text log format")
        d=m.groupdict()
        if not re.match(r"^\d{4}-\d{2}-\d{2}(?:T|\s)\d{2}:\d{2}:\d{2}", d["timestamp"]):
            raise ValueError("Malformed plain-text timestamp")
        if d.get("latency_ms") is not None: d["latency_ms"]=float(d["latency_ms"])
        return d
