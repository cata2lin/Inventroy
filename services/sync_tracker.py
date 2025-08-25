# services/sync_tracker.py
# In-memory progress tracker for background sync jobs shown on the Sync Control page.

from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Dict, Optional, List
import uuid
import time


@dataclass
class _Task:
    id: str
    title: str
    processed: int = 0
    done: bool = False
    ok: Optional[bool] = None
    note: Optional[str] = None
    created_at: float = 0.0
    updated_at: float = 0.0


# Simple in-memory store. This is reset on app restart, which is OK for UI progress.
_TASKS: Dict[str, _Task] = {}


def _now() -> float:
    return time.time()


def add_task(title: str) -> str:
    t = _Task(
        id=str(uuid.uuid4()),
        title=title,
        processed=0,
        done=False,
        ok=None,
        note=None,
        created_at=_now(),
        updated_at=_now(),
    )
    _TASKS[t.id] = t
    return t.id


def step(task_id: str, processed: int, note: Optional[str] = None):
    t = _TASKS.get(task_id)
    if not t:
        return
    t.processed = processed
    if note is not None:
        t.note = note
    t.updated_at = _now()


def finish_task(task_id: str, ok: bool, note: Optional[str] = None):
    t = _TASKS.get(task_id)
    if not t:
        return
    t.done = True
    t.ok = ok
    if note is not None:
        t.note = note
    t.updated_at = _now()


def get_task(task_id: str) -> Optional[Dict]:
    t = _TASKS.get(task_id)
    if not t:
        return None
    payload = asdict(t)
    # keep a lean payload for the UI
    return {
        "id": payload["id"],
        "title": payload["title"],
        "processed": payload["processed"],
        "done": payload["done"],
        "ok": payload["ok"],
        "note": payload["note"],
        "created_at": payload["created_at"],
        "updated_at": payload["updated_at"],
    }


def list_tasks() -> List[Dict]:
    # Return newest first
    items = sorted(_TASKS.values(), key=lambda x: x.updated_at, reverse=True)
    return [
        {
            "id": t.id,
            "title": t.title,
            "processed": t.processed,
            "done": t.done,
            "ok": t.ok,
            "note": t.note,
            "created_at": t.created_at,
            "updated_at": t.updated_at,
        }
        for t in items
    ]


def clear_finished(older_than_seconds: int = 3600) -> int:
    """Optional: prune finished tasks older than N seconds."""
    now = _now()
    to_delete = [
        k
        for k, t in _TASKS.items()
        if t.done and (now - t.updated_at) >= older_than_seconds
    ]
    for k in to_delete:
        _TASKS.pop(k, None)
    return len(to_delete)
