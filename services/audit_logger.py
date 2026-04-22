# services/audit_logger.py
"""
Central audit logging service for the Inventory Intelligence Platform.
Provides fire-and-forget logging for all system operations.

All methods are designed to never throw exceptions — logging failures
should never break business logic.

Dual output:
    1. Database (audit_logs / system_events tables) — for dashboard queries
    2. File system (logs/ directory) — for offline analysis and production debugging

Log files (JSON Lines format):
    logs/all.log         — Every event, all categories
    logs/webhook.log     — Inbound webhook events
    logs/sync.log        — Product sync operations
    logs/stock.log       — Stock level changes and propagation
    logs/config.log      — Store/webhook configuration changes
    logs/auth.log        — Login/logout events
    logs/system.log      — System health, startup, scheduler events
    logs/reconciliation.log — Stock reconciliation runs
    logs/errors.log      — All errors across all categories

Categories:
    WEBHOOK  — Inbound webhook events
    SYNC     — Product sync operations
    STOCK    — Stock level changes and propagation
    CONFIG   — Store/webhook configuration changes
    AUTH     — Login/logout events
    SYSTEM   — System health, startup, scheduler events
    RECONCILIATION — Stock reconciliation runs
"""
import os
import json
import time
import logging
import traceback
from datetime import datetime, timezone
from typing import Optional, Any, Dict
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

from database import SessionLocal
from models import AuditLog, SystemEvent


# --- File Logging Setup ---
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Max 10 MB per file, keep 5 rotated backups per category
MAX_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 5

# Category → file mapping
CATEGORY_FILES = {
    "WEBHOOK": "webhook.log",
    "SYNC": "sync.log",
    "STOCK": "stock.log",
    "CONFIG": "config.log",
    "AUTH": "auth.log",
    "SYSTEM": "system.log",
    "RECONCILIATION": "reconciliation.log",
}


def _make_handler(filename: str) -> RotatingFileHandler:
    """Create a rotating file handler for structured JSON logs."""
    handler = RotatingFileHandler(
        os.path.join(LOG_DIR, filename),
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(logging.DEBUG)
    # Raw formatter — we write pre-formatted JSON lines
    handler.setFormatter(logging.Formatter("%(message)s"))
    return handler


# Create loggers
_loggers: Dict[str, logging.Logger] = {}

def _get_logger(name: str, filename: str) -> logging.Logger:
    """Get or create a named logger with a rotating file handler."""
    if name not in _loggers:
        logger = logging.getLogger(f"audit.{name}")
        logger.setLevel(logging.DEBUG)
        logger.propagate = False  # Don't send to root logger / console
        logger.addHandler(_make_handler(filename))
        _loggers[name] = logger
    return _loggers[name]


# Initialize all category loggers + combined + errors
_all_logger = _get_logger("all", "all.log")
_error_logger = _get_logger("errors", "errors.log")
for _cat, _file in CATEGORY_FILES.items():
    _get_logger(_cat, _file)


def _emit_to_file(
    category: str,
    action: str,
    message: str,
    severity: str = "INFO",
    actor: Optional[str] = None,
    store_id: Optional[int] = None,
    store_name: Optional[str] = None,
    target: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[int] = None,
    error_message: Optional[str] = None,
    stack_trace: Optional[str] = None,
):
    """Write a structured JSON log line to the appropriate files."""
    try:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "category": category,
            "severity": severity,
            "action": action,
            "message": message,
            "actor": actor or "system",
        }
        # Only include non-null fields to keep lines concise
        if store_id is not None:
            record["store_id"] = store_id
        if store_name:
            record["store_name"] = store_name
        if target:
            record["target"] = target
        if details:
            record["details"] = details
        if duration_ms is not None:
            record["duration_ms"] = duration_ms
        if error_message:
            record["error"] = error_message
        if stack_trace:
            record["stack_trace"] = stack_trace

        line = json.dumps(record, default=str, ensure_ascii=False)

        # Write to combined log
        _all_logger.info(line)

        # Write to category-specific log
        cat_logger = _loggers.get(category)
        if cat_logger:
            cat_logger.info(line)

        # Write errors to the dedicated error log regardless of category
        if severity in ("ERROR", "FATAL", "WARN"):
            _error_logger.info(line)

    except Exception:
        pass  # File logging must never crash the app


# --- Context Manager for safe DB writes ---
@contextmanager
def _safe_session():
    """Yield a DB session that auto-commits and never raises."""
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        # Silently fail — logging should never crash the app
    finally:
        db.close()


# --- Core Logging Functions ---

def log(
    category: str,
    action: str,
    message: str,
    severity: str = "INFO",
    actor: Optional[str] = None,
    store_id: Optional[int] = None,
    store_name: Optional[str] = None,
    target: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[int] = None,
    error_message: Optional[str] = None,
    stack_trace: Optional[str] = None,
):
    """Write a single audit log entry to both DB and file."""
    # 1. Write to file (fast, always works)
    _emit_to_file(
        category=category, action=action, message=message, severity=severity,
        actor=actor, store_id=store_id, store_name=store_name, target=target,
        details=details, duration_ms=duration_ms, error_message=error_message,
        stack_trace=stack_trace,
    )

    # 2. Write to database
    with _safe_session() as db:
        entry = AuditLog(
            category=category,
            action=action,
            message=message,
            severity=severity,
            actor=actor or "system",
            store_id=store_id,
            store_name=store_name,
            target=target,
            details=details,
            duration_ms=duration_ms,
            error_message=error_message,
            stack_trace=stack_trace,
        )
        db.add(entry)


def log_error(
    source: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
    exc: Optional[Exception] = None,
):
    """Write a system error event with optional stack trace."""
    tb = traceback.format_exception(type(exc), exc, exc.__traceback__) if exc else None
    trace_str = "".join(tb) if tb else None

    with _safe_session() as db:
        event = SystemEvent(
            level="ERROR",
            source=source,
            message=message,
            details=details,
            stack_trace=trace_str,
        )
        db.add(event)

    # Also write to audit_logs for unified view
    log(
        category="SYSTEM",
        action="error",
        message=message,
        severity="ERROR",
        target=source,
        details=details,
        error_message=str(exc) if exc else None,
        stack_trace=trace_str,
    )


# --- Domain-Specific Helpers ---

def log_webhook(
    store_id: int,
    store_name: str,
    topic: str,
    result: str = "accepted",
    duration_ms: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
):
    """Log an inbound webhook event."""
    severity = "ERROR" if error else "INFO"
    log(
        category="WEBHOOK",
        action=f"webhook_{result}",
        message=f"Webhook [{topic}] from {store_name}: {result}",
        severity=severity,
        store_id=store_id,
        store_name=store_name,
        target=topic,
        details=details,
        duration_ms=duration_ms,
        error_message=error,
    )


def log_sync(
    store_id: int,
    store_name: str,
    action: str,
    message: str,
    duration_ms: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
):
    """Log a product sync operation."""
    severity = "ERROR" if error else "INFO"
    log(
        category="SYNC",
        action=action,
        message=message,
        severity=severity,
        store_id=store_id,
        store_name=store_name,
        details=details,
        duration_ms=duration_ms,
        error_message=error,
    )


def log_stock_change(
    barcode: str,
    store_id: int,
    store_name: str,
    old_qty: int,
    new_qty: int,
    source: str = "webhook",
    details: Optional[Dict[str, Any]] = None,
):
    """Log a stock level change with before/after values."""
    delta = new_qty - old_qty
    direction = "increased" if delta > 0 else "decreased" if delta < 0 else "unchanged"
    log(
        category="STOCK",
        action=f"stock_{direction}",
        message=f"Stock for [{barcode}] on {store_name}: {old_qty} → {new_qty} (Δ{delta:+d}) via {source}",
        store_id=store_id,
        store_name=store_name,
        target=barcode,
        details={
            "old_quantity": old_qty,
            "new_quantity": new_qty,
            "delta": delta,
            "source": source,
            **(details or {}),
        },
    )


def log_propagation(
    barcode: str,
    source_store: str,
    target_store: str,
    quantity: int,
    details: Optional[Dict[str, Any]] = None,
):
    """Log a stock propagation event from one store to another."""
    log(
        category="STOCK",
        action="stock_propagated",
        message=f"Propagated [{barcode}] → {target_store}: set to {quantity} (source: {source_store})",
        target=barcode,
        store_name=target_store,
        details={
            "source_store": source_store,
            "target_store": target_store,
            "quantity": quantity,
            **(details or {}),
        },
    )


def log_config_change(
    actor: str,
    action: str,
    message: str,
    store_id: Optional[int] = None,
    store_name: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
):
    """Log a configuration change (store added, webhook created, etc.)."""
    log(
        category="CONFIG",
        action=action,
        message=message,
        actor=actor,
        store_id=store_id,
        store_name=store_name,
        details=details,
    )


def log_auth(
    username: str,
    action: str,
    success: bool,
    details: Optional[Dict[str, Any]] = None,
):
    """Log an authentication event."""
    severity = "INFO" if success else "WARN"
    result = "success" if success else "failed"
    log(
        category="AUTH",
        action=f"auth_{action}",
        message=f"Auth {action} for [{username}]: {result}",
        severity=severity,
        actor=username,
        details=details,
    )


def log_reconciliation(
    action: str,
    message: str,
    duration_ms: Optional[int] = None,
    details: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
):
    """Log a stock reconciliation event."""
    severity = "ERROR" if error else "INFO"
    log(
        category="RECONCILIATION",
        action=action,
        message=message,
        severity=severity,
        details=details,
        duration_ms=duration_ms,
        error_message=error,
    )


# --- Timer Utility ---

class Timer:
    """Context manager for timing operations in milliseconds."""
    def __init__(self):
        self.start_time = None
        self.elapsed_ms = 0

    def __enter__(self):
        self.start_time = time.monotonic()
        return self

    def __exit__(self, *args):
        self.elapsed_ms = int((time.monotonic() - self.start_time) * 1000)
