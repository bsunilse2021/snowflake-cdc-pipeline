"""Streamlit dashboard for the Snowflake CDC incremental pipeline.

Run locally:   streamlit run app.py
"""
from __future__ import annotations

import hmac
import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Snowflake CDC Pipeline", page_icon="❄️", layout="wide")

sys.path.insert(0, str(Path(__file__).parent / "src"))

# Streamlit Cloud / local .streamlit/secrets.toml -> environment variables
try:
    for _k, _v in st.secrets.items():
        if isinstance(_v, (str, int, float)):
            os.environ.setdefault(_k, str(_v))
except Exception:
    pass  # no secrets file: rely on real environment variables / .env

from sf_pipeline import migrate as mig  # noqa: E402
from sf_pipeline import orchestrator as orch  # noqa: E402
from sf_pipeline.config import Settings  # noqa: E402
from sf_pipeline.connection import connect  # noqa: E402


def gate() -> None:
    """Optional password gate: set APP_PASSWORD in secrets to protect write actions."""
    expected = os.getenv("APP_PASSWORD")
    if not expected or st.session_state.get("auth"):
        return
    entered = st.text_input("Password", type="password")
    if entered and hmac.compare_digest(entered, expected):
        st.session_state["auth"] = True
        st.rerun()
    elif entered:
        st.error("Wrong password")
    st.stop()


gate()

try:
    settings = Settings.from_env()
    DB = settings.template_vars()["DATABASE"]
except Exception as exc:
    st.error(f"Configuration problem: {exc}")
    st.stop()

st.title("❄️ Snowflake CDC Incremental Load Pipeline")
st.caption(f"Database `{DB}` · Warehouse `{settings.warehouse}` · Streams + Tasks + MERGE")

if "flash" in st.session_state:
    st.success(st.session_state.pop("flash"))


def load_overview() -> dict:
    with connect(settings) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {DB}.RAW.ORDERS")
        raw = int(cur.fetchone()[0])
        cur.execute(f"SELECT COUNT(*) FROM {DB}.CORE.ORDERS")
        core = int(cur.fetchone()[0])
        pending = orch.pending_changes(conn, DB)
        latest = orch.latest_run(conn, DB)
        hist = orch.history(conn, settings, 50)
        try:
            cur.execute(f"SHOW TASKS LIKE 'TASK_MERGE_ORDERS' IN SCHEMA {DB}.CORE")
            cols = [c[0].lower() for c in cur.description]
            row = cur.fetchone()
            task_state = dict(zip(cols, row)).get("state", "unknown") if row else "not found"
        except Exception:
            task_state = "unknown"
    return {"raw": raw, "core": core, "pending": pending, "latest": latest,
            "hist": hist, "task_state": task_state}


def do_action(label: str, fn) -> None:
    with st.spinner(f"{label}..."):
        try:
            with connect(settings) as conn:
                result = fn(conn)
            st.session_state["flash"] = f"{label} finished. {result or ''}"
        except Exception as exc:
            st.error(f"{label} failed: {exc}")
            return
    st.rerun()


top = st.columns([6, 1])
top[1].button("🔄 Refresh")

try:
    data = load_overview()
except Exception as exc:
    st.error(
        "Could not read pipeline data. Have you run `sfpipe migrate`? "
        f"Details: {exc}"
    )
    st.stop()

tab_over, tab_hist, tab_demo, tab_bench, tab_mig = st.tabs(
    ["Overview", "Run history", "Demo controls", "Benchmark", "Schema migrations"]
)

with tab_over:
    latest = data["latest"]
    c = st.columns(4)
    c[0].metric("Pending changes in stream", f"{data['pending']:,}")
    c[1].metric("Source rows (RAW)", f"{data['raw']:,}")
    c[2].metric("Target rows (CORE)", f"{data['core']:,}")
    c[3].metric("Task state", str(data["task_state"]).upper())
    st.subheader("Last run")
    if latest:
        c = st.columns(5)
        c[0].metric("Status", latest["STATUS"])
        c[1].metric("Inserted", f"{latest['ROWS_INSERTED']:,}")
        c[2].metric("Updated", f"{latest['ROWS_UPDATED']:,}")
        c[3].metric("Deleted", f"{latest['ROWS_DELETED']:,}")
        c[4].metric("Duration (ms)", f"{latest['DURATION_MS'] or 0:,}")
        if latest.get("MESSAGE"):
            st.error(latest["MESSAGE"])
    else:
        st.info("No runs yet. Use the Demo controls tab to seed data and run the pipeline.")

    if st.button("Check RAW vs CORE reconciliation"):
        try:
            with connect(settings) as conn:
                res = orch.reconcile(conn, settings)
            if res["in_sync"]:
                st.success("RAW and CORE match (row count and content hash).")
            elif res["stream_pending"]:
                st.warning("Not in sync yet: changes are still pending in the stream. Run the pipeline.")
            else:
                st.error("Mismatch detected between RAW and CORE.")
            st.json({k: str(v) for k, v in res.items()})
        except Exception as exc:
            st.error(f"Reconcile failed: {exc}")

with tab_hist:
    if data["hist"]:
        df = pd.DataFrame(data["hist"]).sort_values("RUN_ID")
        st.dataframe(df.sort_values("RUN_ID", ascending=False), use_container_width=True, hide_index=True)
        st.subheader("Rows processed per run")
        st.bar_chart(df.set_index("RUN_ID")[["ROWS_INSERTED", "ROWS_UPDATED", "ROWS_DELETED"]])
        st.subheader("Run duration (ms)")
        st.line_chart(df.set_index("RUN_ID")[["DURATION_MS"]])
    else:
        st.info("No run history yet.")

with tab_demo:
    st.write("Simulate upstream activity, then let the pipeline process only the delta.")
    st.markdown("**1. Seed source data**")
    rows = st.number_input("Rows to insert", 100, 1_000_000, 10_000, step=1000)
    if st.button("Seed source table"):
        do_action("Seed", lambda conn: f"{orch.seed(conn, settings, int(rows)):,} rows inserted into RAW.ORDERS.")

    st.markdown("**2. Simulate source changes (CDC)**")
    a, b, d = st.columns(3)
    ins = a.number_input("Inserts", 0, 100_000, 500, step=100)
    upd = b.number_input("Updates", 0, 100_000, 300, step=100)
    dele = d.number_input("Deletes", 0, 100_000, 100, step=100)
    if st.button("Simulate changes"):
        do_action("Simulate changes",
                  lambda conn: orch.simulate_changes(conn, settings, int(ins), int(upd), int(dele)))

    st.markdown("**3. Run the incremental load**")

    def _run(conn):
        run = orch.run_incremental(conn, settings, "direct")
        if not run:
            return "Stream was empty, nothing to process."
        return (f"Inserted {run['ROWS_INSERTED']}, updated {run['ROWS_UPDATED']}, "
                f"deleted {run['ROWS_DELETED']} in {run['DURATION_MS']} ms.")

    if st.button("Run incremental load now", type="primary"):
        do_action("Incremental run", _run)
    st.caption("The Snowflake task also does this automatically on its schedule when the stream has data.")

with tab_bench:
    st.write("Compares processing one delta against rebuilding the whole table.")
    changes = st.number_input("Changed rows in the delta", 30, 100_000, 3000, step=500)
    if st.button("Run benchmark"):
        with st.spinner("Benchmarking..."):
            try:
                with connect(settings) as conn:
                    st.session_state["bench"] = orch.benchmark(conn, settings, int(changes))
            except Exception as exc:
                st.error(f"Benchmark failed: {exc}")
    bench = st.session_state.get("bench")
    if bench:
        c = st.columns(4)
        c[0].metric("Total rows", f"{bench['total_rows']:,}")
        c[1].metric("Incremental (s)", bench["incremental_s"])
        c[2].metric("Full reload (s)", bench["full_reload_s"])
        c[3].metric("Speedup", f"{bench['speedup_x']}x" if bench["speedup_x"] else "n/a")
        st.bar_chart(pd.DataFrame({"seconds": [bench["incremental_s"], bench["full_reload_s"]]},
                                  index=["Incremental", "Full reload"]))
        st.caption("Only quote numbers measured on your own data volumes.")

with tab_mig:
    st.write("Warehouse schema is versioned SQL in Git (`sql/migrations`). Rollback is done from the CLI.")
    try:
        st.dataframe(pd.DataFrame(mig.status(settings), columns=["Version", "Description", "State"]),
                     use_container_width=True, hide_index=True)
    except Exception as exc:
        st.error(f"Could not read migration status: {exc}")
    st.code("sfpipe migrate\nsfpipe rollback --steps 1", language="bash")
