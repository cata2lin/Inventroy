from typing import Iterable, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import func


def upsert_batch(
    db: Session,
    model,
    rows: List[Dict[str, Any]],
    conflict_cols: Iterable[str],
    exclude_from_update: Iterable[str] | None = None,
) -> None:
    """
    Generic Postgres upsert that:
      - Uses COALESCE(excluded.col, table.col) so NULLs from payload never erase existing values.
      - Never re-binds inventory_item_id if it's already set (important for FK to inventory_levels).
    """
    if not rows:
        return

    table = model.__table__
    stmt = pg_insert(table).values(rows)

    # Build set_ dynamically based on union of provided keys
    all_cols: set[str] = set()
    for r in rows:
        all_cols.update(r.keys())

    excl = set(exclude_from_update or ())
    excl.update(conflict_cols)

    update_cols = {}
    for c in all_cols:
        if c in excl:
            continue
        if c in table.c:
            update_cols[c] = func.coalesce(getattr(stmt.excluded, c), getattr(table.c, c))

    # One-way bind of inventory_item_id (set only if currently NULL)
    if "inventory_item_id" in table.c:
        update_cols["inventory_item_id"] = func.coalesce(
            table.c.inventory_item_id, getattr(stmt.excluded, "inventory_item_id")
        )

    stmt = stmt.on_conflict_do_update(index_elements=list(conflict_cols), set_=update_cols)
    db.execute(stmt)
