"""Settings loaded from environment variables (or a local .env file)."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass

try:  # optional: keeps unit tests free of third-party imports
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_SCHEDULE = re.compile(r"^[A-Za-z0-9 */,\-_:]+$")


def _req(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    account: str
    user: str
    role: str
    warehouse: str
    database: str
    password: str | None = None
    private_key_path: str | None = None
    private_key_passphrase: str | None = None
    task_schedule: str = "5 MINUTE"

    @classmethod
    def from_env(cls) -> Settings:
        s = cls(
            account=_req("SNOWFLAKE_ACCOUNT"),
            user=_req("SNOWFLAKE_USER"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PIPELINE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "CDC_DEMO"),
            password=os.getenv("SNOWFLAKE_PASSWORD") or None,
            private_key_path=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH") or None,
            private_key_passphrase=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None,
            task_schedule=os.getenv("TASK_SCHEDULE", "5 MINUTE"),
        )
        if not (s.password or s.private_key_path):
            raise RuntimeError("Set SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
        return s

    def template_vars(self) -> dict[str, str]:
        """Values substituted into {{ PLACEHOLDERS }} in SQL files (validated to block injection)."""
        for label, val in (("DATABASE", self.database), ("WAREHOUSE", self.warehouse),
                           ("ROLE", self.role)):
            if not _IDENT.match(val):
                raise ValueError(f"Invalid identifier for {label}: {val!r}")
        if not _SCHEDULE.match(self.task_schedule):
            raise ValueError(f"Invalid TASK_SCHEDULE: {self.task_schedule!r}")
        return {
            "DATABASE": self.database.upper(),
            "WAREHOUSE": self.warehouse.upper(),
            "ROLE": self.role.upper(),
            "TASK_SCHEDULE": self.task_schedule,
        }
