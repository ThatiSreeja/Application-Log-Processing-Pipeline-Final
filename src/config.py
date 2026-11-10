from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "data" / "input"
PROCESSED_DIR = ROOT / "data" / "processed"
QUARANTINE_DIR = ROOT / "data" / "quarantine"
AUDIT_DIR = ROOT / "data" / "audit"
REPORT_DIR = ROOT / "reports"
DB_PATH = PROCESSED_DIR / "logs.db"
ERROR_LOG_PATH = AUDIT_DIR / "pipeline_errors.jsonl"

REQUIRED_FIELDS = ("timestamp", "level", "service", "message")
ALLOWED_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"}
MAX_FILE_ATTEMPTS = 3
RETRY_BASE_DELAY_SECONDS = 0.25
MAX_LATENCY_MS = 3_600_000  # one hour; adjust for the source system
MAX_FUTURE_TIMESTAMP_SECONDS = 86400  # tolerate 24 hours of clock skew

# Reference data: services permitted to emit application logs. Extend this list
# when onboarding a new service; unknown names are quarantined by the pipeline.
SERVICE_REGISTRY = {
    "SVC_AUTH": "auth_service",
    "SVC_PAYMENT": "payment_service",
    "SVC_ORDER": "order_service",
    "SVC_API": "api",
    "SVC_PAYMENT_LEGACY": "payment",
}

LOG_LEVEL = "INFO"
LOG_FILE = AUDIT_DIR / "pipeline.log"
