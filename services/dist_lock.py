# services/dist_lock.py
"""
P2 — distributed locking via PostgreSQL session-level advisory locks.

Why session-level (not xact-level): the sync handler commits several times within one logical
operation, so a transaction-scoped lock would release too early. We hold a SESSION advisory lock
on a DEDICATED connection for the whole critical section, independent of the handler's own
commits, and release it in a finally. If the worker crashes, the connection drops and Postgres
releases the lock automatically (abandoned-lock recovery — no orphaned locks).

Multi-worker AND multi-instance safe (advisory locks are global to the database).

A dedicated bounded pool isolates lock connections from the main request pool. The in-process
threading.Lock (in inventory_sync_service) stays as a cheap first gate, so for the current
single-worker deployment we rarely even contend on the DB lock.
"""
import os
import time
import hashlib
import threading
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import create_engine, text
from database import DATABASE_URL

DIST_LOCK_ENABLED = os.getenv("DIST_LOCK_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
LOCK_TIMEOUT_S = int(os.getenv("DIST_LOCK_TIMEOUT_S", "25"))
_POLL_S = 0.25

# Dedicated, bounded pool so lock connections never starve the main request pool.
_lock_engine = create_engine(
    DATABASE_URL, pool_size=int(os.getenv("DIST_LOCK_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DIST_LOCK_MAX_OVERFLOW", "20")),
    pool_recycle=3600, pool_pre_ping=True,
)

# lightweight in-process contention metrics (also queryable via diagnostics)
_metrics_lock = threading.Lock()
_metrics = {"acquired": 0, "timeouts": 0, "waited_total_s": 0.0, "waits_over_1s": 0, "errors": 0}


def _key_to_bigint(key: str) -> int:
    """Stable signed 64-bit key for pg advisory locks (same across processes)."""
    digest = hashlib.sha1(key.encode("utf-8")).digest()[:8]
    return int.from_bytes(digest, "big", signed=True)


def metrics() -> dict:
    with _metrics_lock:
        return dict(_metrics)


class _Handle:
    __slots__ = ("conn", "k", "key", "waited")
    def __init__(self, conn, k, key, waited):
        self.conn = conn; self.k = k; self.key = key; self.waited = waited


def acquire(key: str, timeout_s: int = LOCK_TIMEOUT_S) -> Optional["_Handle"]:
    """Acquire a cross-process advisory lock for `key`. Returns a handle, or None on timeout/error.
    None means 'could not get the lock' — the caller must NOT proceed with the protected work."""
    if not DIST_LOCK_ENABLED:
        return _Handle(None, None, key, 0.0)  # disabled → no-op handle (in-process lock still applies)
    k = _key_to_bigint(key)
    started = time.monotonic()
    deadline = started + timeout_s
    try:
        conn = _lock_engine.connect()
    except Exception:
        with _metrics_lock:
            _metrics["errors"] += 1
        return None
    try:
        while True:
            got = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": k}).scalar()
            if got:
                waited = time.monotonic() - started
                with _metrics_lock:
                    _metrics["acquired"] += 1
                    _metrics["waited_total_s"] += waited
                    if waited > 1.0:
                        _metrics["waits_over_1s"] += 1
                return _Handle(conn, k, key, waited)
            if time.monotonic() >= deadline:
                conn.close()
                with _metrics_lock:
                    _metrics["timeouts"] += 1
                return None
            time.sleep(_POLL_S)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        with _metrics_lock:
            _metrics["errors"] += 1
        return None


def release(handle: Optional["_Handle"]) -> None:
    if handle is None or handle.conn is None:
        return
    try:
        handle.conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": handle.k})
    except Exception:
        pass
    finally:
        try:
            handle.conn.close()  # also releases any session locks held on this connection
        except Exception:
            pass


@contextmanager
def lock(key: str, timeout_s: int = LOCK_TIMEOUT_S):
    """Context manager. Yields True if the lock was acquired, False on timeout (caller must skip)."""
    h = acquire(key, timeout_s)
    try:
        yield h is not None
    finally:
        release(h)


def held_count() -> int:
    """Observability: number of advisory locks currently held in the database."""
    try:
        with _lock_engine.connect() as c:
            return c.execute(text("SELECT count(*) FROM pg_locks WHERE locktype='advisory'")).scalar() or 0
    except Exception:
        return -1
