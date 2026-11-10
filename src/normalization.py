from datetime import datetime, timezone
LEVEL_MAP = {"INFO":"INFO","INFORMATION":"INFO","DEBUG":"DEBUG","WARN":"WARNING",
             "WARNING":"WARNING","ERR":"ERROR","ERROR":"ERROR","CRITICAL":"FATAL","FATAL":"FATAL"}
def normalize_level(value):
    return LEVEL_MAP.get(str(value).strip().upper()) if value is not None else None
def normalize_timestamp(value):
    if value is None or not str(value).strip(): return None
    try:
        dt = datetime.fromisoformat(str(value).strip().replace("Z","+00:00"))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
    except (ValueError, TypeError, OverflowError):
        return None
