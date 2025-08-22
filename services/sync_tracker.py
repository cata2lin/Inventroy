# services/sync_tracker.py

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, Optional, List, Literal

Status = Literal["queued", "running", "done", "error"]

@dataclass
class Task:
    id: str
    name: str
    status: Status
    created_at: datetime
    updated_at: datetime
    error: Optional[str] = None

    def to_dict(self) -> Dict:
        d = asdict(self)
        # make ISO strings for JSONability
        d["created_at"] = self.created_at.isoformat()
        d["updated_at"] = self.updated_at.isoformat()
        return d


_LOCK = threading.Lock()
_TASKS: Dict[str, Task] = {}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def create_task(name: str) -> str:
    """Create a new task in 'queued' state and return its id."""
    with _LOCK:
        tid = str(uuid.uuid4())
        t = Task(id=tid, name=name, status="queued", created_at=_now(), updated_at=_now())
        _TASKS[tid] = t
        return tid


def start_task(task_id: str) -> None:
    with _LOCK:
        t = _TASKS.get(task_id)
        if not t:
            return
        t.status = "running"
        t.updated_at = _now()


def complete_task(task_id: str) -> None:
    with _LOCK:
        t = _TASKS.get(task_id)
        if not t:
            return
        t.status = "done"
        t.updated_at = _now()


def fail_task(task_id: str, error: str) -> None:
    with _LOCK:
        t = _TASKS.get(task_id)
        if not t:
            return
        t.status = "error"
        t.error = error
        t.updated_at = _now()


def get_task_status(task_id: str) -> Optional[Dict]:
    with _LOCK:
        t = _TASKS.get(task_id)
        return t.to_dict() if t else None


def get_all_tasks() -> List[Dict]:
    with _LOCK:
        return [t.to_dict() for t in _TASKS.values()]
s