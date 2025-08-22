# services/sync_tracker.py

import uuid
from typing import Dict, Optional

_state: Dict[str, Dict[str, str]] = {}

def create_task(title: str) -> str:
    tid = str(uuid.uuid4())
    _state[tid] = {"title": title, "status": "queued", "message": ""}
    return tid

def step(task_id: Optional[str], message: str) -> None:
    if not task_id:
        return
    obj = _state.get(task_id)
    if obj:
        obj["status"] = "running"
        obj["message"] = message

def complete(task_id: Optional[str], message: str = "done") -> None:
    if not task_id:
        return
    obj = _state.get(task_id)
    if obj:
        obj["status"] = "done"
        obj["message"] = message

def fail(task_id: Optional[str], message: str) -> None:
    if not task_id:
        return
    obj = _state.get(task_id)
    if obj:
        obj["status"] = "failed"
        obj["message"] = message

def get(task_id: str) -> Dict[str, str]:
    return _state.get(task_id, {"title": "", "status": "unknown", "message": ""})
