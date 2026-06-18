# services/alerting.py
"""
Lightweight alerting sink (P0/P2.3).

Fire-and-forget: an alert must never break business logic. Every alert is:
  1. Written to the audit trail (so it shows in System Monitor), and
  2. Recorded as a SystemEvent row when severity is WARNING/CRITICAL (error log view), and
  3. Optionally POSTed to an external webhook (env ALERT_WEBHOOK_URL) for paging.

Severity: INFO | WARNING | CRITICAL
"""
import os
import json
from typing import Optional, Dict, Any

from database import SessionLocal
from models import SystemEvent
from services import audit_logger

ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()


def alert(severity: str, source: str, title: str, context: Optional[Dict[str, Any]] = None) -> None:
    """Emit an alert. Never raises."""
    severity = (severity or "INFO").upper()
    context = context or {}

    # 1. Audit trail (always)
    try:
        audit_logger.log(
            category="SYSTEM",
            action="alert",
            message=f"[{severity}] {title}",
            severity=severity if severity in ("INFO", "WARN", "WARNING", "ERROR", "CRITICAL") else "WARN",
            target=str(context.get("barcode") or context.get("target") or "")[:255] or None,
            details={"source": source, "title": title, **context},
        )
    except Exception:
        pass

    # 2. SystemEvent for WARNING/CRITICAL (surfaces in the error log view)
    if severity in ("WARNING", "WARN", "CRITICAL", "ERROR"):
        db = SessionLocal()
        try:
            db.add(SystemEvent(
                level="CRITICAL" if severity == "CRITICAL" else "WARN",
                source=source,
                message=title,
                details=context,
            ))
            db.commit()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        finally:
            db.close()

    # 3. External webhook (optional, best-effort, short timeout)
    if ALERT_WEBHOOK_URL and severity in ("WARNING", "WARN", "CRITICAL", "ERROR"):
        try:
            import requests
            requests.post(
                ALERT_WEBHOOK_URL,
                json={"severity": severity, "source": source, "title": title, "context": context},
                timeout=5,
            )
        except Exception:
            pass


def critical(source: str, title: str, context: Optional[Dict[str, Any]] = None) -> None:
    alert("CRITICAL", source, title, context)


def warning(source: str, title: str, context: Optional[Dict[str, Any]] = None) -> None:
    alert("WARNING", source, title, context)
