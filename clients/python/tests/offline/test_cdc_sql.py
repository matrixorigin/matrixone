"""Offline tests for CDC manager SQL generation and helpers."""

from __future__ import annotations

import json
import pytest
from unittest.mock import Mock, AsyncMock

from matrixone.cdc import CDCManager, AsyncCDCManager


class TestCDCManagerSQLGeneration:
    """Validate SQL emitted by synchronous CDC manager."""

    def setup_method(self):
        self.mock_client = Mock()
        self.mock_client.execute = Mock()
        self.manager = CDCManager(self.mock_client)

    def test_create_cdc_with_options(self):
        options = {"Level": "database", "NoFull": True, "Frequency": "1s"}
        self.manager.create(
            task_name="cdc_task",
            source_uri="mysql://acc#user:pwd@127.0.0.1:6001",
            sink_type="mysql",
            sink_uri="mysql://root:pwd@127.0.0.1:3306",
            table_mapping="test_db.*:sink_db.*",
            options=options,
        )

        sql = self.mock_client.execute.call_args[0][0]
        assert sql.startswith("CREATE CDC cdc_task")
        assert "'Level'='database'" in sql
        assert "'NoFull'='true'" in sql
        assert "'Frequency'='1s'" in sql

    def test_drop_and_pause_variants(self):
        self.manager.drop("cdc_task")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "DROP CDC TASK cdc_task"

        self.manager.drop_all()
        assert self.mock_client.execute.call_args_list[-1][0][0] == "DROP CDC ALL"

        self.manager.pause("cdc_task")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "PAUSE CDC TASK cdc_task"

        self.manager.pause_all()
        assert self.mock_client.execute.call_args_list[-1][0][0] == "PAUSE CDC ALL"

    def test_resume_and_restart(self):
        self.manager.resume("task1")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "RESUME CDC TASK task1"

        self.manager.restart("task1")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "RESUME CDC TASK task1 'restart'"

    def test_show_operations(self):
        self.manager.show_all()
        assert self.mock_client.execute.call_args_list[-1][0][0] == "SHOW CDC ALL"

        self.manager.show_task("task_x")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "SHOW CDC TASK task_x"

    def test_list_tasks_parses_rows(self):
        tables_payload = json.dumps({"table": "test_db.*:sink_db.*"})
        additional_payload = json.dumps({"MaxSqlLength": 8192})

        mock_row = (
            "task-id-1",
            "cdc_task",
            "mysql://src",
            "mysql://sink",
            "mysql",
            tables_payload,
            None,
            None,
            None,
            "running",
            "2025-11-06 10:30:00",
            0,
            None,
            "2025-11-06 10:00:00",
            additional_payload,
        )

        self.mock_client.execute.return_value = Mock(rows=[mock_row])
        tasks = self.manager.list()

        self.mock_client.execute.assert_called_with(
            "SELECT task_id, task_name, source_uri, sink_uri, sink_type, tables, filters, "
            "start_ts, end_ts, state, checkpoint, no_full, err_msg, task_create_time, additional_config "
            "FROM mo_catalog.mo_cdc_task WHERE account_id = current_account_id()"
        )

        assert len(tasks) == 1
        task = tasks[0]
        assert task.task_id == "task-id-1"
        assert task.table_mapping == "test_db.*:sink_db.*"
        assert task.additional_config == {"MaxSqlLength": 8192}
        assert task.no_full is False

    def test_get_task_not_found(self):
        self.mock_client.execute.return_value = Mock(rows=[])
        with pytest.raises(ValueError):
            self.manager.get("missing")

    def test_list_watermarks(self):
        watermark_row = (
            "task-id-1",
            "cdc_task",
            "test_db",
            "orders",
            "2025-11-06 10:30:00",
            None,
        )
        self.mock_client.execute.return_value = Mock(rows=[watermark_row])
        marks = self.manager.list_watermarks("cdc_task")

        self.mock_client.execute.assert_called_with(
            "SELECT w.task_id, t.task_name, w.db_name, w.table_name, w.watermark, w.err_msg "
            "FROM mo_catalog.mo_cdc_watermark w JOIN mo_catalog.mo_cdc_task t ON w.task_id = t.task_id "
            "WHERE w.account_id = current_account_id() AND t.task_name = 'cdc_task'"
        )

        assert len(marks) == 1
        assert marks[0].table == "orders"


@pytest.mark.asyncio
class TestAsyncCDCManagerSQLGeneration:
    """Validate SQL emitted by asynchronous CDC manager."""

    async def test_async_create_and_drop(self):
        mock_executor = Mock()
        mock_executor.execute = AsyncMock()
        manager = AsyncCDCManager(client=None, executor=mock_executor)

        await manager.create(
            task_name="async_task",
            source_uri="mysql://src",
            sink_type="matrixone",
            sink_uri="mysql://sink",
            table_mapping="*.*:*.*",
            options={"Level": "account"},
        )

        mock_executor.execute.assert_called_with(
            "CREATE CDC async_task 'mysql://src' 'matrixone' 'mysql://sink' '*.*:*.*' {'Level'='account'}"
        )

        await manager.drop("async_task")
        mock_executor.execute.assert_called_with("DROP CDC TASK async_task")

    async def test_async_list(self):
        rows = [
            (
                "task-id-2",
                "async_task",
                "mysql://src",
                "mysql://sink",
                "mysql",
                json.dumps({"table": "db.table"}),
                None,
                None,
                None,
                "paused",
                None,
                1,
                "error",
                "2025-11-06 11:00:00",
                None,
            )
        ]
        mock_executor = Mock()
        mock_executor.execute = AsyncMock()
        mock_executor.execute.return_value = Mock(rows=rows)
        manager = AsyncCDCManager(client=None, executor=mock_executor)

        tasks = await manager.list()

        mock_executor.execute.assert_called_with(
            "SELECT task_id, task_name, source_uri, sink_uri, sink_type, tables, filters, "
            "start_ts, end_ts, state, checkpoint, no_full, err_msg, task_create_time, additional_config "
            "FROM mo_catalog.mo_cdc_task WHERE account_id = current_account_id()"
        )

        assert tasks[0].no_full is True
        assert tasks[0].err_msg == "error"

