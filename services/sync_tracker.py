# services/sync_tracker.py

import uuid
from typing import Dict, Any

# This will store the state of our background tasks in memory.
# In a production multi-worker setup, you'd replace this with Redis or a database.
tasks: Dict[str, Dict[str, Any]] = {}

def create_task(store_name: str) -> str:
    """Creates a new task entry and returns its ID."""
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "store_name": store_name,
        "status": "pending",
        "progress": 0,
        "total": 100,
        "message": "Initializing..."
    }
    return task_id

def update_task_progress(task_id: str, progress: int, total: int, message: str):
    """Updates the progress of a specific task."""
    if task_id in tasks:
        tasks[task_id]["progress"] = progress
        tasks[task_id]["total"] = total
        tasks[task_id]["message"] = message
        tasks[task_id]["status"] = "running"

def complete_task(task_id: str, message: str):
    """Marks a task as completed."""
    if task_id in tasks:
        tasks[task_id]["status"] = "completed"
        tasks[task_id]["progress"] = tasks[task_id]["total"]
        tasks[task_id]["message"] = message

def fail_task(task_id: str, error_message: str):
    """Marks a task as failed."""
    if task_id in tasks:
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["message"] = error_message

def get_task_status(task_id: str) -> Dict[str, Any]:
    """Retrieves the status of a single task."""
    return tasks.get(task_id, {"status": "not_found"})

def get_all_tasks() -> Dict[str, Dict[str, Any]]:
    """Retrieves the status of all tasks."""
    return tasks