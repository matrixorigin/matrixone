"""Offline contracts for the CDC catalog-visibility fallback.

MatrixOne hides ``mo_catalog.mo_cdc_task`` / ``mo_cdc_watermark`` from ordinary
tenants and reports the miss as parser error 1064 rather than MySQL's 1146, so
the fallback must key on the reported table, not on a fixed errno list.
"""

from __future__ import annotations

from typing import List

import pytest

from matrixone.cdc import BaseCDCManager, CDCManager
from matrixone.exceptions import QueryError

MISSING_TASK = 'SQL parser error: table "mo_cdc_task" does not exist'
MISSING_WATERMARK = 'SQL parser error: table "mo_catalog.mo_cdc_watermark" does not exist'


@pytest.mark.parametrize(
    "message, errno, expected",
    [
        # MatrixOne: parser error 1064 for a hidden catalog relation.
        (MISSING_TASK, 1064, True),
        (MISSING_WATERMARK, 1064, True),
        # MySQL-compatible wording, both quoted forms and schema qualification.
        ("Table 'mo_catalog.mo_cdc_task' doesn't exist", 1146, True),
        ('Table "mo_cdc_watermark" does not exist', 1146, True),
        ("no such table: mo_catalog.mo_cdc_task", 20403, True),
        # Privilege failures on the CDC catalog still belong to the fallback.
        ("Access denied for user 'tenant1' to database 'mo_catalog'", 1045, True),
        ("Access denied for database 'mo_catalog'", 1044, True),
        # A syntax error is a defect, never a visibility problem.
        ("SQL parser error: You have an error in your SQL syntax near 'mo_cdc_task'", 1064, False),
        # Unknown column means the catalog exists but its shape drifted.
        ("SQL parser error: Unknown column 't.task_id' in 'field list'", 1064, False),
        # A different relation is missing: not our fallback.
        ('SQL parser error: table "other_table" does not exist', 1064, False),
        # Non-parser failures propagate even when the message mentions the table.
        ("Lost connection to server during query on mo_cdc_task", 2013, False),
        ("Lock wait timeout exceeded on mo_cdc_task", 1205, False),
    ],
)
def test_catalog_unavailable_classification(message: str, errno: int, expected: bool) -> None:
    error = QueryError(message, errno=errno)
    assert BaseCDCManager._catalog_unavailable(error) is expected


def test_catalog_unavailable_without_errno_uses_table_proof() -> None:
    """A DBAPI error without errno must still be recognized, but only on table proof."""
    assert BaseCDCManager._catalog_unavailable(QueryError(MISSING_TASK)) is True
    assert BaseCDCManager._catalog_unavailable(QueryError("unrelated failure")) is False


class _ResultRows:
    """Minimal result wrapper accepted by ``rows_from_result``."""

    def __init__(self, rows):
        self._rows = list(rows)

    @property
    def rows(self):
        return list(self._rows)

    def fetchall(self):
        return list(self._rows)


class _FakeExecutor:
    """Return the catalog result, or raise, then answer the SHOW fallback."""

    def __init__(self, failure=None, show_rows: List[tuple] = ()):
        self.failure = failure
        self.show_rows = list(show_rows)
        self.statements: List[str] = []

    def execute(self, sql: str):
        self.statements.append(sql)
        if "mo_cdc_task" in sql and "SHOW" not in sql.upper():
            raise self.failure
        return _ResultRows(self.show_rows)


def _manager(executor: _FakeExecutor) -> CDCManager:
    manager = CDCManager.__new__(CDCManager)
    manager.executor = executor
    manager.client = None
    return manager


def test_list_falls_back_to_show_cdc_on_missing_catalog() -> None:
    row = (1, "task_a", "mysql://src", '{"_":"mysql"}', "running", None, "{}", "2026-01-01 00:00:00")
    executor = _FakeExecutor(failure=QueryError(MISSING_TASK, errno=1064), show_rows=[row])
    tasks = _manager(executor).list()
    assert [task.task_name for task in tasks] == ["task_a"]
    assert len(executor.statements) == 2
    assert executor.statements[-1].upper().startswith("SHOW")


def test_list_propagates_non_visibility_errors() -> None:
    executor = _FakeExecutor(failure=QueryError("SQL parser error: syntax error near 'SELEC'", errno=1064))
    with pytest.raises(QueryError):
        _manager(executor).list()
    assert len(executor.statements) == 1
