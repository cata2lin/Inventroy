# services/alerting.py
"""
Lightweight alerting sink (P0/P2.3).

Fire-and-forget: an alert must never break business logic. Every alert is:
  1. Written to the audit trail (so it shows in System Monitor), and
  2. Recorded as a SystemEvent row when severity is WARNING/CRITICAL (error log view), and
  3. Optionally POSTed to an external webhook (env ALERT_WEBHOOK_URL) for paging, and
  4. Optionally EMAILED (env ALERT_EMAIL_TO + ALERT_SMTP_*) — CRITICAL only by default,
     throttled (per-source cooldown + hourly cap) so an alert storm cannot flood the inbox.

Severity: INFO | WARNING | CRITICAL
"""
import os
import json
import time
import smtplib
import threading
from collections import deque
from email.message import EmailMessage
from typing import Optional, Dict, Any

from database import SessionLocal
from models import SystemEvent
from services import audit_logger

ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()

# --- Email sink (config read lazily so .env is honoured; secrets live in .env, not unit files) ---
EMAIL_SOURCE_COOLDOWN_S = int(os.getenv("ALERT_EMAIL_SOURCE_COOLDOWN_S", "300"))
EMAIL_MAX_PER_HOUR = int(os.getenv("ALERT_EMAIL_MAX_PER_HOUR", "20"))

_email_lock = threading.Lock()
_email_last_by_source: Dict[str, float] = {}
_email_sent_times: deque = deque()
_email_suppressed = 0


def _email_allowed(source: str) -> bool:
    """Throttle: one email per source per cooldown, capped per hour. Suppressed alerts are
    counted and reported in the next email that goes through."""
    global _email_suppressed
    now = time.monotonic()
    with _email_lock:
        while _email_sent_times and _email_sent_times[0] < now - 3600:
            _email_sent_times.popleft()
        last = _email_last_by_source.get(source)
        if (last is not None and now - last < EMAIL_SOURCE_COOLDOWN_S) \
                or len(_email_sent_times) >= EMAIL_MAX_PER_HOUR:
            _email_suppressed += 1
            return False
        _email_last_by_source[source] = now
        _email_sent_times.append(now)
        return True


def _send_email(severity: str, source: str, title: str, context: Dict[str, Any]) -> Optional[str]:
    """Send one alert email synchronously. Returns None on success, error string on failure.
    Never raises. All config from env: ALERT_EMAIL_TO, ALERT_SMTP_HOST/PORT/USER/PASS."""
    global _email_suppressed
    to_addr = os.getenv("ALERT_EMAIL_TO", "").strip()
    host = os.getenv("ALERT_SMTP_HOST", "").strip()
    if not to_addr or not host:
        return "not configured"
    try:
        port = int(os.getenv("ALERT_SMTP_PORT", "587"))
        user = os.getenv("ALERT_SMTP_USER", "").strip()
        password = os.getenv("ALERT_SMTP_PASS", "")
        with _email_lock:
            suppressed, _email_suppressed = _email_suppressed, 0

        msg = EmailMessage()
        msg["Subject"] = f"[InventorySync {severity}] {source}: {title[:120]}"
        msg["From"] = os.getenv("ALERT_EMAIL_FROM", user or f"inventory-sync@{host}")
        msg["To"] = to_addr
        body = f"{severity} — {source}\n\n{title}\n\nContext:\n{json.dumps(context, indent=2, default=str)[:4000]}"
        if suppressed:
            body += f"\n\n({suppressed} further alert email(s) suppressed by throttling since the last one.)"
        msg.set_content(body)

        if port == 465:
            server = smtplib.SMTP_SSL(host, port, timeout=10)
        else:
            server = smtplib.SMTP(host, port, timeout=10)
        try:
            if port != 465:
                server.starttls()
            if user:
                server.login(user, password)
            server.send_message(msg)
        finally:
            server.quit()
        return None
    except Exception as e:
        return str(e)


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

    # 4. Email (optional, best-effort, throttled). CRITICAL only by default —
    #    ALERT_EMAIL_MIN_SEVERITY=WARNING widens it.
    email_severities = ("CRITICAL", "ERROR") \
        if os.getenv("ALERT_EMAIL_MIN_SEVERITY", "CRITICAL").upper() == "CRITICAL" \
        else ("WARNING", "WARN", "CRITICAL", "ERROR")
    if os.getenv("ALERT_EMAIL_TO", "").strip() and severity in email_severities:
        try:
            if _email_allowed(source):
                threading.Thread(target=_send_email, args=(severity, source, title, context),
                                 daemon=True).start()
        except Exception:
            pass


def critical(source: str, title: str, context: Optional[Dict[str, Any]] = None) -> None:
    alert("CRITICAL", source, title, context)


def warning(source: str, title: str, context: Optional[Dict[str, Any]] = None) -> None:
    alert("WARNING", source, title, context)
