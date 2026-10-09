# Snowflake Incremental Load Pipeline (CDC with Streams & Tasks)

Python-orchestrated incremental pipeline on Snowflake. A **Stream** captures row-level changes (CDC) on a source table, a **Task** fires only when the stream has data, and a **MERGE** procedure applies just the delta to the curated table. The entire warehouse (schemas, tables, stream, procedure, task) is versioned as SQL in Git with checksummed migrations and rollback scripts.

**Stack:** Python 3.10+ · Snowflake · SQL · Git/GitHub Actions

## Architecture
See [docs/architecture.md](docs/architecture.md) (Mermaid diagram renders on GitHub).

```
RAW.ORDERS --> RAW.ORDERS_STREAM --> TASK_MERGE_ORDERS --> SP_MERGE_ORDERS --> CORE.ORDERS
                                                                     \--> AUDIT.PIPELINE_RUN_LOG
```

## Repo layout
```
sql/migrations/   V001..V007 forward migrations (schemas, tables, stream, proc, task, schema change)
sql/rollback/     U002..U007 rollback scripts (V001 intentionally manual)
src/sf_pipeline/  config, connection, migrate (runner), orchestrator, cli, sample_data
tests/            unit tests (no Snowflake needed)
.github/workflows CI (lint+test), deploy (migrate/rollback), run-pipeline
```

## Quick start
1. **Snowflake account:** a free trial works (https://signup.snowflake.com). Note your account identifier (`orgname-accountname`).
2. **One-time grant** (as ACCOUNTADMIN) so tasks can run:
   ```sql
   GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
   ```
3. **Install & configure**
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -e ".[dev]"
   cp .env.example .env      # fill in account, user, password/key
   ```
4. **Deploy schema from code**
   ```bash
   sfpipe migrate
   sfpipe status
   ```
5. **Run the pipeline**
   ```bash
   sfpipe seed --rows 100000                   # load source data
   sfpipe run                                  # first run processes everything in the stream
   sfpipe simulate --inserts 500 --updates 300 --deletes 100
   sfpipe run --mode direct                    # or --mode task to trigger the Snowflake task
   sfpipe reconcile                            # RAW vs CORE row count + content hash
   sfpipe history                              # audit log
   sfpipe benchmark --changes 3000             # incremental vs full reload timing
   ```
   The task also runs on its own every `TASK_SCHEDULE` whenever the stream has data.
6. **Schema rollback**
   ```bash
   sfpipe rollback --steps 1     # undoes V007 (restores old procedure, drops ORDER_CHANNEL)
   sfpipe migrate                # re-applies it
   ```


## Dashboard (Streamlit)
```bash
pip install -r requirements.txt     # adds streamlit + pandas
cp .streamlit/secrets.toml.example .streamlit/secrets.toml   # fill in (or keep using .env)
streamlit run app.py
```
Tabs: **Overview** (pending stream changes, row counts, task state, last run, reconciliation) · **Run history** (charts) · **Demo controls** (seed, simulate changes, run incremental load) · **Benchmark** · **Schema migrations**.

Free hosting on **Streamlit Community Cloud**: push the repo to GitHub, go to share.streamlit.io, click *Create app*, pick the repo, set the main file to `app.py`, then paste the contents of your secrets file under *Advanced settings > Secrets*. Set `APP_PASSWORD` so strangers cannot trigger runs on your Snowflake credits.

## Where it is "hosted"
- **Pipeline runtime:** Snowflake itself (Stream + Task + stored procedure run inside your account; nothing to host).
- **Code:** GitHub. CI runs lint + tests on every push; `deploy.yml` applies migrations on version tags (add secrets `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` and variables `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` under a `production` environment).
- **Optional Python orchestrator hosting:** `run-pipeline.yml` (manual or cron on GitHub Actions) or the included `Dockerfile` on any container host.
- **Cost control:** XSMALL warehouse with 60s auto-suspend; the task's `WHEN SYSTEM$STREAM_HAS_DATA` check does not resume the warehouse when idle. Suspend the task when you are done: `ALTER TASK CDC_DEMO.CORE.TASK_MERGE_ORDERS SUSPEND;`

## Design notes
- Stream offset advances only when the consuming DML commits, so a failed run is retried without losing changes.
- Applied migrations are immutable (checksum drift is rejected); changes ship as new versions.
- The loader introspects table columns, so it keeps working across schema migrations.
- To add another table: copy the V002/V004/V005 pattern (table, stream, merge procedure) and a task, or chain tasks in a DAG.

## Resume metrics
Run `sfpipe benchmark` on your own data volumes and quote **your measured** numbers (e.g. "reduced refresh time from X s to Y s on N rows"). Do not claim figures you have not measured.
