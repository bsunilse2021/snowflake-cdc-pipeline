import pytest

from sf_pipeline import migrate as m
from sf_pipeline.config import Settings

VARS = {"DATABASE": "CDC_DEMO", "WAREHOUSE": "PIPELINE_WH", "ROLE": "SYSADMIN", "TASK_SCHEDULE": "5 MINUTE"}


def test_versions_are_sequential_and_unique():
    versions = [x.version for x in m.discover()]
    assert versions == list(range(1, len(versions) + 1))


def test_every_migration_except_v001_has_rollback():
    for mig in m.discover():
        if mig.version == 1:
            continue  # dropping schemas is intentionally manual
        assert m.rollback_script(mig.version) is not None, f"missing rollback for V{mig.version:03d}"


def test_all_sql_renders_without_unknown_placeholders():
    for p in list(m.MIGRATIONS_DIR.glob("*.sql")) + list(m.ROLLBACK_DIR.glob("*.sql")):
        out = m.render(p.read_text(), VARS)
        assert "{{" not in out


def test_render_unknown_placeholder_raises():
    with pytest.raises(KeyError):
        m.render("SELECT {{ NOPE }}", VARS)


def test_checksum_changes_with_content():
    assert m.checksum("a") != m.checksum("b")
    assert m.checksum("a") == m.checksum("a")


def test_settings_reject_bad_identifier():
    s = Settings("acc", "u", "SYSADMIN", "WH", "DB; DROP TABLE x", password="p")
    with pytest.raises(ValueError):
        s.template_vars()
