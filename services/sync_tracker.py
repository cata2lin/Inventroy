# services/sync_tracker.py
# tiny in-memory progress tracker used by /sync-control

import uuid
from typing import Dict, Optional


class _Task:
    __slots__ = ("id", "title", "processed", "done", "ok", "note")

    def __init__(self, title: str):
        self.id = str(uuid.uuid4())
        self.title = title
        self.processed = 0
        self.done = False
        self.ok: Optional[bool] = None
        self.note: Optional[str] = None


_TASKS: Dict[str, _Task] = {}


def create_task(title: str) -> str:
    t = _Task(title)
    _TASKS[t.id] = t
    return t.id


def step(task_id: str, processed: int, note: Optional[str] = None) -> None:
    t = _TASKS.get(task_id)
    if not t:
        return
    t.processed = processed
    if note:
        t.note = note


def finish_task(task_id: str, ok: bool, note: Optional[str] = None) -> None:
    t = _TASKS.get(task_id)
    if not t:
        return
    t.done = True
    t.ok = ok
    if note:
        t.note = note


def get_task(task_id: str):
    t = _TASKS.get(task_id)
    if not t:
        return None
    return {
        "id": t.id,
        "title": t.title,
        "processed": t.processed,
        "done": t.done,
        "ok": t.ok,
        "note": t.note,
    }
