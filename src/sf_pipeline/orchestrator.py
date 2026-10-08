"""Python orchestration around the Snowflake Stream + Task pipeline."""
from __future__ import annotations

import logging
import random
import time

from snowflake.connector import DictCursor

from .config import Settings
from .sample_data import generate_orders

log = logging.getLogger(__name__)
BATCH = 10_000


def _db(settings: Settings) -> str:
    return settings.template_vars()["DATABASE"]


def table_columns(conn, db: str, schema: str, table: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(
        f"SELECT COLUMN_NAME FROM {db}.INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def upsert_source_rows(conn, db: str, rows: list[dict]) -> None:
    """Upsert rows into RAW.ORDERS (acts as the upstream source table the stream watches).
    Columns are introspected so the loader keeps working across schema migrations."""
    if not rows:
        return
    table_cols = table_columns(conn, db, "RAW", "ORDERS")
    cols = [c for c in table_cols if c in rows[0]]
    non_key = [c for c in cols if c != "ORDER_ID"]
    cur = conn.cursor()
    tmp = f"{db}.RAW._TMP_ORDERS_BATCH"
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tmp} AS "
                f"SELECT {', '.join(cols)} FROM {db}.RAW.ORDERS WHERE 1 = 0")
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        cur.executemany(
            f"INSERT INTO {tmp} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
            [tuple(r[c] for c in cols) for r in chunk],
        )
    set_clause = ", ".join([f"t.{c} = s.{c}" for c in non_key] + ["t.UPDATED_AT = CURRENT_TIMESTAMP()"])
    cur.execute(
        f"MERGE INTO {db}.RAW.ORDERS t USING {tmp} s ON t.ORDER_ID = s.ORDER_ID "
        f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) "
        f"VALUES ({', '.join('s.' + c for c in cols)})"
    )


def seed(conn, settings: Settings, n: int) -> int:
    db = _db(settings)
    cur = conn.cursor()
    cur.execute(f"SELECT COALESCE(MAX(ORDER_ID), 0) + 1 FROM {db}.RAW.ORDERS")
    start = int(cur.fetchone()[0])
    upsert_source_rows(conn, db, list(generate_orders(start, n, random.Random())))
    log.info("Inserted %d new source rows (ids from %d)", n, start)
    return n


def simulate_changes(conn, settings: Settings, inserts: int, updates: int, deletes: int) -> None:
    """Mimic OLTP activity on the source table: inserts, updates and deletes."""
    db = _db(settings)
    cur = conn.cursor()
    if inserts:
        seed(conn, settings, inserts)
    cur.execute(f"SELECT ORDER_ID FROM {db}.RAW.ORDERS ORDER BY RANDOM() LIMIT {int(updates + deletes)}")
    ids = [int(r[0]) for r in cur.fetchall()]
    upd_ids, del_ids = ids[:updates], ids[updates:updates + deletes]
    if upd_ids:
        marks = ", ".join(["%s"] * len(upd_ids))
        cur.execute(
            f"UPDATE {db}.RAW.ORDERS SET ORDER_STATUS = 'SHIPPED', "
            f"ORDER_AMOUNT = ROUND(ORDER_AMOUNT * 1.05, 2), UPDATED_AT = CURRENT_TIMESTAMP() "
            f"WHERE ORDER_ID IN ({marks})", upd_ids)
    if del_ids:
        marks = ", ".join(["%s"] * len(del_ids))
        cur.execute(f"DELETE FROM {db}.RAW.ORDERS WHERE ORDER_ID IN ({marks})", del_ids)
    log.info("Simulated changes: +%d inserts, %d updates, %d deletes", inserts, len(upd_ids), len(del_ids))


def stream_has_data(conn, db: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{db}.RAW.ORDERS_STREAM')")
    return bool(cur.fetchone()[0])


def pending_changes(conn, db: str) -> int:
    cur = conn.cursor()  # reading a stream outside DML does NOT advance its offset
    cur.execute(f"SELECT COUNT(*) FROM {db}.RAW.ORDERS_STREAM")
    return int(cur.fetchone()[0])


def latest_run(conn, db: str) -> dict | None:
    cur = conn.cursor(DictCursor)
    cur.execute(f"SELECT * FROM {db}.AUDIT.PIPELINE_RUN_LOG ORDER BY RUN_ID DESC LIMIT 1")
    return cur.fetchone()


def run_incremental(conn, settings: Settings, mode: str = "direct", timeout: int = 300) -> dict | None:
    """Process the pending delta.
    direct: CALL the merge procedure now.   task: EXECUTE TASK and wait for the audit row."""
    db = _db(settings)
    if not stream_has_data(conn, db):
        log.info("Stream is empty - nothing to do")
        return None
    log.info("%d change record(s) pending in stream", pending_changes(conn, db))
    cur = conn.cursor()
    if mode == "direct":
        cur.execute(f"CALL {db}.CORE.SP_MERGE_ORDERS()")
        return latest_run(conn, db)

    before = (latest_run(conn, db) or {}).get("RUN_ID", 0)
    cur.execute(f"EXECUTE TASK {db}.CORE.TASK_MERGE_ORDERS")
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(5)
        run = latest_run(conn, db)
        if run and run["RUN_ID"] > before:
            return run
    log.warning("No new run recorded within %ds (task skipped or still running)", timeout)
    return None


def reconcile(conn, settings: Settings) -> dict:
    """Compare row count + content hash of RAW (source) vs CORE (target)."""
    db = _db(settings)
    cur = conn.cursor()
    out = {}
    for name in ("RAW", "CORE"):
        cur.execute(
            f"SELECT COUNT(*), SUM(HASH(ORDER_ID, CUSTOMER_ID, ORDER_STATUS, ORDER_AMOUNT, ORDER_TS)) "
            f"FROM {db}.{name}.ORDERS")
        out[name] = cur.fetchone()
    out["in_sync"] = out["RAW"] == out["CORE"]
    out["stream_pending"] = stream_has_data(conn, db)
    return out


def history(conn, settings: Settings, limit: int = 10) -> list[dict]:
    cur = conn.cursor(DictCursor)
    cur.execute(f"SELECT RUN_ID, STARTED_AT, DURATION_MS, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, "
                f"STATUS FROM {_db(settings)}.AUDIT.PIPELINE_RUN_LOG ORDER BY RUN_ID DESC LIMIT {int(limit)}")
    return cur.fetchall()


def benchmark(conn, settings: Settings, changes: int) -> dict:
    """Compare processing one delta (incremental) vs rebuilding the whole table (full reload)."""
    db = _db(settings)
    run_incremental(conn, settings)  # drain anything pending so we measure a clean delta
    third = changes // 3
    simulate_changes(conn, settings, third, third, changes - 2 * third)

    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(f"CALL {db}.CORE.SP_MERGE_ORDERS()")
    inc = time.perf_counter() - t0

    t0 = time.perf_counter()
    cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE {db}.CORE.ORDERS_FULL_RELOAD_BENCH AS "
                f"SELECT * FROM {db}.RAW.ORDERS")
    full = time.perf_counter() - t0
    cur.execute(f"SELECT COUNT(*) FROM {db}.RAW.ORDERS")
    total = int(cur.fetchone()[0])
    cur.execute(f"DROP TABLE IF EXISTS {db}.CORE.ORDERS_FULL_RELOAD_BENCH")
    return {"total_rows": total, "changed_rows": changes, "incremental_s": round(inc, 2),
            "full_reload_s": round(full, 2), "speedup_x": round(full / inc, 2) if inc else None}
