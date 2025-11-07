
"""Offline tests for CDC manager SQL generation and helpers."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, Mock

import pytest

from matrixone.cdc import AsyncCDCManager, CDCManager, CDCSinkType, build_mysql_uri


def test_build_mysql_uri() -> None:
    base = build_mysql_uri(host="127.0.0.1", port=6001, user="root", password="111")
    assert base == "mysql://root:111@127.0.0.1:6001"

    scoped = build_mysql_uri(host="127.0.0.1", port=6001, user="admin", password="pwd", account="acc")
    assert scoped == "mysql://acc#admin:pwd@127.0.0.1:6001"

    with pytest.raises(ValueError):
        build_mysql_uri(host="", port=6001, user="root", password="111")

    with pytest.raises(ValueError):
        build_mysql_uri(host="127.0.0.1", port="6001", user="root", password="111")


class TestCDCManagerSQLGeneration:
    """Validate SQL emitted by synchronous CDC manager."""

    def setup_method(self) -> None:
        self.mock_client = Mock()
        self.mock_client.execute = Mock()
        self.manager = CDCManager(self.mock_client)

    def test_create_cdc_with_options(self) -> None:
        options = {"Level": "database", "NoFull": True, "Frequency": "1h"}
        self.manager.create(
            task_name="cdc_task",
            source_uri="mysql://acc#user:pwd@127.0.0.1:6001",
            sink_type=CDCSinkType.MYSQL,
            sink_uri="mysql://root:pwd@127.0.0.1:3306",
            table_mapping="test_db.*:sink_db.*",
            options=options,
        )

        sql = self.mock_client.execute.call_args[0][0]
        assert sql.startswith("CREATE CDC cdc_task")
        assert "'Level'='database'" in sql
        assert "'NoFull'='true'" in sql
        assert "'Frequency'='1h'" in sql

    def test_create_database_task_helper(self) -> None:
        self.mock_client.execute.reset_mock()
        self.manager.create_database_task(
            task_name="cdc_db",
            source_uri="mysql://source",
            sink_type=CDCSinkType.MATRIXONE,
            sink_uri="mysql://sink",
            source_database="db1",
            sink_database="db2",
            options={"Frequency": "30m"},
        )

        sql = self.mock_client.execute.call_args[0][0]
        assert "CREATE CDC cdc_db" in sql
        assert "'Level'='database'" in sql
        assert "db1:db2" in sql

    def test_create_database_task_level_mismatch(self) -> None:
        with pytest.raises(ValueError):
            self.manager.create_database_task(
                task_name="cdc_db",
                source_uri="mysql://source",
                sink_type="matrixone",
                sink_uri="mysql://sink",
                source_database="db1",
                options={"Level": "table"},
            )

    def test_create_table_task_helper(self) -> None:
        self.mock_client.execute.reset_mock()
        self.manager.create_table_task(
            task_name="cdc_tables",
            source_uri="mysql://src",
            sink_type=CDCSinkType.MYSQL,
            sink_uri="mysql://sink",
            table_mappings=[
                ("db1", "orders", "db2", "orders_copy"),
                ("db1.customers", "db2.customers"),
            ],
            options={"NoFull": True},
        )

        sql = self.mock_client.execute.call_args[0][0]
        assert "CREATE CDC cdc_tables" in sql
        assert "db1.orders:db2.orders_copy" in sql
        assert "db1.customers:db2.customers" in sql
        assert "'Level'='table'" in sql

    def test_create_table_task_level_mismatch(self) -> None:
        with pytest.raises(ValueError):
            self.manager.create_table_task(
                task_name="cdc_tables",
                source_uri="mysql://src",
            sink_type=CDCSinkType.MYSQL,
                sink_uri="mysql://sink",
                table_mappings=[("db1", "t1")],
                options={"Level": "database"},
            )

    def test_create_table_task_sql_examples(self) -> None:
        """Verify SQL strings for varied table-level CDC configurations."""

        base_kwargs = {
            "source_uri": "mysql://src",
            "sink_uri": "mysql://sink",
        }

        cases = [
            (
                "table_alpha",
                [("db1", "orders", "backup_db", "orders")],
                {"NoFull": True, "Frequency": "1h"},
                "CREATE CDC table_alpha 'mysql://src' 'matrixone' 'mysql://sink' "
                "'db1.orders:backup_db.orders' {'NoFull'='true', 'Frequency'='1h', 'Level'='table'}",
            ),
            (
                "table_beta",
                [("db1.customers", "backup_db.customers")],
                {"Frequency": "2h", "MaxSqlLength": 2097152},
                "CREATE CDC table_beta 'mysql://src' 'matrixone' 'mysql://sink' "
                "'db1.customers:backup_db.customers' {'Frequency'='2h', 'MaxSqlLength'='2097152', 'Level'='table'}",
            ),
            (
                "table_gamma",
                [("db1", "events", "backup_db", "events")],
                {"Frequency": "24h", "SendSqlTimeout": "45m", "InitSnapshotSplitTxn": False},
                "CREATE CDC table_gamma 'mysql://src' 'matrixone' 'mysql://sink' "
                "'db1.events:backup_db.events' {'Frequency'='24h', 'SendSqlTimeout'='45m', "
                "'InitSnapshotSplitTxn'='false', 'Level'='table'}",
            ),
        ]

        for task_name, mappings, options, expected_sql in cases:
            self.mock_client.execute.reset_mock()
            self.manager.create_table_task(
                task_name=task_name,
                sink_type=CDCSinkType.MATRIXONE,
                table_mappings=mappings,
                options=options,
                **base_kwargs,
            )

            sql = self.mock_client.execute.call_args[0][0]
            assert sql == expected_sql

    def test_drop_and_pause_variants(self) -> None:
        self.manager.drop("cdc_task")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "DROP CDC TASK cdc_task"

        self.manager.drop_all()
        assert self.mock_client.execute.call_args_list[-1][0][0] == "DROP CDC ALL"

        self.manager.pause("cdc_task")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "PAUSE CDC TASK cdc_task"

        self.manager.pause_all()
        assert self.mock_client.execute.call_args_list[-1][0][0] == "PAUSE CDC ALL"

    def test_resume_and_restart(self) -> None:
        self.manager.resume("task1")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "RESUME CDC TASK task1"

        self.manager.restart("task1")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "RESUME CDC TASK task1 'restart'"

    def test_show_operations(self) -> None:
        self.manager.show_all()
        assert self.mock_client.execute.call_args_list[-1][0][0] == "SHOW CDC ALL"

        self.manager.show_task("task_x")
        assert self.mock_client.execute.call_args_list[-1][0][0] == "SHOW CDC TASK task_x"

    def test_list_tasks_parses_rows(self) -> None:
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

    def test_get_task_not_found(self) -> None:
        self.mock_client.execute.return_value = Mock(rows=[])
        with pytest.raises(ValueError):
            self.manager.get("missing")

    def test_list_watermarks(self) -> None:
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

    async def test_async_create_and_drop(self) -> None:
        mock_executor = Mock()
        mock_executor.execute = AsyncMock()
        manager = AsyncCDCManager(client=None, executor=mock_executor)

        await manager.create(
            task_name="async_task",
            source_uri="mysql://src",
            sink_type=CDCSinkType.MATRIXONE,
            sink_uri="mysql://sink",
            table_mapping="*.*:*.*",
            options={"Level": "account"},
        )

        mock_executor.execute.assert_called_with(
            "CREATE CDC async_task 'mysql://src' 'matrixone' 'mysql://sink' '*.*:*.*' {'Level'='account'}"
        )

        await manager.drop("async_task")
        mock_executor.execute.assert_called_with("DROP CDC TASK async_task")

    async def test_async_create_database_task(self) -> None:
        mock_executor = Mock()
        mock_executor.execute = AsyncMock()
        manager = AsyncCDCManager(client=None, executor=mock_executor)

        await manager.create_database_task(
            task_name="async_db",
            source_uri="mysql://src",
            sink_type=CDCSinkType.MATRIXONE,
            sink_uri="mysql://sink",
            source_database="db1",
            sink_database="db2",
            options={"Frequency": "1h"},
        )

        sql = mock_executor.execute.call_args[0][0]
        assert "CREATE CDC async_db" in sql
        assert "db1:db2" in sql
        assert "'Level'='database'" in sql
        assert "'Frequency'='1h'" in sql

    async def test_async_create_table_task(self) -> None:
        mock_executor = Mock()
        mock_executor.execute = AsyncMock()
        manager = AsyncCDCManager(client=None, executor=mock_executor)

        await manager.create_table_task(
            task_name="async_tables",
            source_uri="mysql://src",
            sink_type=CDCSinkType.MYSQL,
            sink_uri="mysql://sink",
            table_mappings=[("db1", "orders", "db2", "orders_copy")],
        )

        sql = mock_executor.execute.call_args[0][0]
        assert "CREATE CDC async_tables" in sql
        assert "db1.orders:db2.orders_copy" in sql
        assert "'Level'='table'" in sql

    async def test_async_list(self) -> None:
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
