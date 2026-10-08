"""Tiny schema-migration runner (Flyway-style) so the whole warehouse is code in Git.

  sql/migrations/V003__desc.sql   forward migration (applied in version order)
  sql/rollback/U003__desc.sql     matching rollback script

Applied versions + SHA-256 checksums are tracked in <DB>.AUDIT.SCHEMA_VERSIONS.
Editing an already-applied migration is detected (checksum drift) and refused.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path

from .config import Settings

log = logging.getLogger(__name__)

REPO_ROOT = Path(os.getenv("SF_PIPELINE_ROOT", Path(__file__).resolve().parents[2]))
MIGRATIONS_DIR = REPO_ROOT / "sql" / "migrations"
ROLLBACK_DIR = REPO_ROOT / "sql" / "rollback"

_MIG_RE = re.compile(r"^V(\d{3})__([A-Za-z0-9_]+)\.sql$")
_PLACEHOLDER = re.compile(r"\{\{\s*([A-Z_]+)\s*\}\}")


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    path: Path
    checksum: str


def checksum(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def discover(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    found: list[Migration] = []
    for p in sorted(directory.glob("V*.sql")):
        m = _MIG_RE.match(p.name)
        if not m:
            raise ValueError(f"Bad migration filename: {p.name} (expected V001__description.sql)")
        found.append(Migration(int(m.group(1)), m.group(2), p, checksum(p.read_text("utf-8"))))
    versions = [m.version for m in found]
    if len(versions) != len(set(versions)):
        raise ValueError("Duplicate migration versions found")
    return sorted(found, key=lambda m: m.version)


def rollback_script(version: int, directory: Path = ROLLBACK_DIR) -> Path | None:
    matches = list(directory.glob(f"U{version:03d}__*.sql"))
    return matches[0] if matches else None


def render(sql: str, variables: dict[str, str]) -> str:
    def repl(m: re.Match) -> str:
        key = m.group(1)
        if key not in variables:
            raise KeyError(f"Unknown placeholder {{{{ {key} }}}} in SQL")
        return variables[key]

    return _PLACEHOLDER.sub(repl, sql)


def _bootstrap(settings: Settings) -> None:
    from .connection import connect

    v = settings.template_vars()
    with connect(settings, bootstrap=True) as conn:
        cur = conn.cursor()
        cur.execute(
            f"CREATE WAREHOUSE IF NOT EXISTS {v['WAREHOUSE']} WAREHOUSE_SIZE='XSMALL' "
            "AUTO_SUSPEND=60 AUTO_RESUME=TRUE INITIALLY_SUSPENDED=TRUE"
        )
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {v['DATABASE']}")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {v['DATABASE']}.AUDIT")
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {v['DATABASE']}.AUDIT.SCHEMA_VERSIONS (
                  VERSION     NUMBER PRIMARY KEY,
                  DESCRIPTION VARCHAR,
                  CHECKSUM    VARCHAR(64),
                  APPLIED_AT  TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                  APPLIED_BY  VARCHAR DEFAULT CURRENT_USER())"""
        )


def _applied(conn, db: str) -> dict[int, str]:
    cur = conn.cursor()
    cur.execute(f"SELECT VERSION, CHECKSUM FROM {db}.AUDIT.SCHEMA_VERSIONS ORDER BY VERSION")
    return {int(r[0]): r[1] for r in cur.fetchall()}


def migrate(settings: Settings, target: int | None = None) -> int:
    """Apply pending migrations. Returns number applied."""
    from .connection import connect

    _bootstrap(settings)
    variables = settings.template_vars()
    db = variables["DATABASE"]
    migrations = discover()

    with connect(settings) as conn:
        done = _applied(conn, db)
        for m in migrations:  # drift check
            if m.version in done and done[m.version] != m.checksum:
                raise RuntimeError(
                    f"Checksum drift on V{m.version:03d} ({m.path.name}): applied migrations "
                    "are immutable. Add a new migration instead."
                )
        pending = [m for m in migrations
                   if m.version not in done and (target is None or m.version <= target)]
        if done and pending and pending[0].version < max(done):
            raise RuntimeError("Out-of-order migration detected; renumber it above the latest applied")

        cur = conn.cursor()
        for m in pending:
            log.info("Applying V%03d %s", m.version, m.description)
            conn.execute_string(render(m.path.read_text("utf-8"), variables))
            cur.execute(
                f"INSERT INTO {db}.AUDIT.SCHEMA_VERSIONS (VERSION, DESCRIPTION, CHECKSUM) "
                "VALUES (%s, %s, %s)",
                (m.version, m.description, m.checksum),
            )
        log.info("Applied %d migration(s)", len(pending))
        return len(pending)


def rollback(settings: Settings, steps: int = 1) -> list[int]:
    """Undo the most recent `steps` migrations using sql/rollback scripts."""
    from .connection import connect

    variables = settings.template_vars()
    db = variables["DATABASE"]
    with connect(settings) as conn:
        done = _applied(conn, db)
        targets = sorted(done, reverse=True)[:steps]
        scripts = {}
        for v in targets:  # pre-flight: refuse to start if any script is missing
            s = rollback_script(v)
            if s is None:
                raise RuntimeError(f"No rollback script for V{v:03d}; refusing to roll back")
            scripts[v] = s
        cur = conn.cursor()
        for v in targets:
            log.warning("Rolling back V%03d using %s", v, scripts[v].name)
            conn.execute_string(render(scripts[v].read_text("utf-8"), variables))
            cur.execute(f"DELETE FROM {db}.AUDIT.SCHEMA_VERSIONS WHERE VERSION = %s", (v,))
        return targets


def status(settings: Settings) -> list[tuple[int, str, str]]:
    """Return (version, description, state) for every migration on disk."""
    from .connection import connect

    db = settings.template_vars()["DATABASE"]
    with connect(settings) as conn:
        done = _applied(conn, db)
    rows = []
    for m in discover():
        if m.version not in done:
            state = "PENDING"
        elif done[m.version] != m.checksum:
            state = "DRIFT"
        else:
            state = "APPLIED"
        rows.append((m.version, m.description, state))
    return rows
