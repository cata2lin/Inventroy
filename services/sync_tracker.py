from dataclasses import dataclass, asdict
from typing import Dict, Optional, List
import uuid
import time

@dataclass
class _Task:
    id: str; title: str; processed: int = 0; done: bool = False; ok: Optional[bool] = None
    note: Optional[str] = None; created_at: float = 0.0; updated_at: float = 0.0

_TASKS: Dict[str, _Task] = {}
def _now() -> float: return time.time()
def add_task(title: str) -> str:
    t = _Task(id=str(uuid.uuid4()), title=title, created_at=_now(), updated_at=_now())
    _TASKS[t.id] = t
    return t.id
def step(task_id: str, processed: int, note: Optional[str] = None):
    if t := _TASKS.get(task_id):
        t.processed, t.note, t.updated_at = processed, note, _now()
def finish_task(task_id: str, ok: bool, note: Optional[str] = None):
    if t := _TASKS.get(task_id):
        t.done, t.ok, t.note, t.updated_at = True, ok, note, _now()
def list_tasks() -> List[Dict]:
    return [asdict(t) for t in sorted(_TASKS.values(), key=lambda x: x.updated_at, reverse=True)]
def clear_finished(older_than_seconds: int = 3600):
    now = _now()
    for k in [k for k, t in _TASKS.items() if t.done and (now - t.updated_at) >= older_than_seconds]:
        _TASKS.pop(k, None)