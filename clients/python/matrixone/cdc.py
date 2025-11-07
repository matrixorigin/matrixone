# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""CDC management utilities for MatrixOne Python SDK."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence



def build_mysql_uri(host: str, port: int, user: str, password: str = "", account: Optional[str] = None) -> str:
    """Construct a MatrixOne/MySQL URI usable in CDC configuration."""

    if not host:
        raise ValueError("host is required")
    if port is None:
        raise ValueError("port is required")
    if not isinstance(port, int):
        raise ValueError("port must be an integer")
    if not user:
        raise ValueError("user is required")

    netloc = f"{user}:{password or ''}@{host}:{port}"
    if account:
        return f"mysql://{account}#{netloc}"
    return f"mysql://{netloc}"



@dataclass
class CDCTaskInfo:
    """High-level CDC task metadata returned by manager helper methods."""

    task_id: Optional[str]
    task_name: str
    source_uri: Optional[str]
    sink_uri: Optional[str]
    sink_type: Optional[str]
    table_mapping: Optional[str]
    state: Optional[str]
    checkpoint: Optional[Any]
    err_msg: Optional[str]
    created_time: Optional[Any]
    no_full: Optional[bool] = None
    filters: Optional[str] = None
    additional_config: Optional[Dict[str, Any]] = None
    raw_row: Optional[Sequence[Any]] = field(default=None, repr=False)


@dataclass
class CDCWatermarkInfo:
    """Per-table CDC watermark information."""

    task_id: str
    task_name: Optional[str]
    database: Optional[str]
    table: Optional[str]
    watermark: Optional[str]
    err_msg: Optional[str]
    raw_row: Optional[Sequence[Any]] = field(default=None, repr=False)


class BaseCDCManager:
    """Shared SQL builders and helpers for CDC managers."""

    def __init__(self, client):
        self.client = client

    def _build_create_sql(
        self,
        task_name: str,
        source_uri: str,
        sink_type: str,
        sink_uri: str,
        table_mapping: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> str:
        sql_parts = [
            "CREATE CDC",
            task_name,
            f"'{source_uri}'",
            f"'{sink_type}'",
            f"'{sink_uri}'",
            f"'{table_mapping}'",
        ]

        options_sql = self._build_options(options)
        if options_sql:
            sql_parts.append(options_sql)

        return " ".join(sql_parts)

    def _build_drop_sql(self, task_name: Optional[str] = None, all_tasks: bool = False) -> str:
        if all_tasks:
            return "DROP CDC ALL"
        if not task_name:
            raise ValueError("task_name is required when all_tasks is False")
        return f"DROP CDC TASK {task_name}"

    def _build_pause_sql(self, task_name: Optional[str] = None, all_tasks: bool = False) -> str:
        if all_tasks:
            return "PAUSE CDC ALL"
        if not task_name:
            raise ValueError("task_name is required when all_tasks is False")
        return f"PAUSE CDC TASK {task_name}"

    def _build_resume_sql(self, task_name: str, restart: bool = False) -> str:
        if not task_name:
            raise ValueError("task_name is required")
        if restart:
            return f"RESUME CDC TASK {task_name} 'restart'"
        return f"RESUME CDC TASK {task_name}"

    @staticmethod
    def _build_show_all_sql() -> str:
        return "SHOW CDC ALL"

    @staticmethod
    def _build_show_task_sql(task_name: str) -> str:
        if not task_name:
            raise ValueError("task_name is required")
        return f"SHOW CDC TASK {task_name}"

    @staticmethod
    def _build_select_tasks_sql(task_name: Optional[str] = None) -> str:
        base_sql = (
            "SELECT task_id, task_name, source_uri, sink_uri, sink_type, tables, filters, "
            "start_ts, end_ts, state, checkpoint, no_full, err_msg, task_create_time, additional_config "
            "FROM mo_catalog.mo_cdc_task "
            "WHERE account_id = current_account_id()"
        )
        if task_name:
            base_sql += f" AND task_name = '{task_name}'"
        return base_sql

    @staticmethod
    def _build_select_watermarks_sql(task_name: Optional[str] = None) -> str:
        base_sql = (
            "SELECT w.task_id, t.task_name, w.db_name, w.table_name, w.watermark, w.err_msg "
            "FROM mo_catalog.mo_cdc_watermark w "
            "JOIN mo_catalog.mo_cdc_task t ON w.task_id = t.task_id "
            "WHERE w.account_id = current_account_id()"
        )
        if task_name:
            base_sql += f" AND t.task_name = '{task_name}'"
        return base_sql

    @staticmethod
    def _normalise_table_mappings(table_mappings) -> str:
        """Normalise Python structures into CDC table mapping strings."""

        if table_mappings is None:
            raise ValueError("table_mappings cannot be None")

        if isinstance(table_mappings, str):
            return table_mappings

        parts = []
        if isinstance(table_mappings, dict):
            for src, dest in table_mappings.items():
                parts.append(f"{src}:{dest}")
        else:
            for item in table_mappings:
                if isinstance(item, str):
                    parts.append(item)
                    continue

                if not isinstance(item, (list, tuple)):
                    raise ValueError(f"Unsupported table mapping entry: {item!r}")

                if len(item) == 2:
                    src, dest = item
                    if "." in src and "." in dest:
                        parts.append(f"{src}:{dest}")
                    else:
                        src_db, src_table = src, dest
                        parts.append(f"{src_db}.{src_table}:{src_db}.{src_table}")
                elif len(item) == 3:
                    src_db, src_table, sink_table = item
                    parts.append(f"{src_db}.{src_table}:{src_db}.{sink_table}")
                elif len(item) == 4:
                    src_db, src_table, sink_db, sink_table = item
                    parts.append(f"{src_db}.{src_table}:{sink_db}.{sink_table}")
                else:
                    raise ValueError(f"Unsupported table mapping tuple: {item!r}")

        if not parts:
            raise ValueError("table_mappings cannot be empty")
        return ",".join(parts)

    @staticmethod
    def _prepare_options(options: Optional[Dict[str, Any]], level: Optional[str] = None) -> Dict[str, Any]:
        """Copy and normalise options, enforcing Level when provided."""

        prepared = dict(options or {})
        if level:
            existing = prepared.get("Level")
            if existing is not None and str(existing).lower() != level.lower():
                raise ValueError(f"Level option mismatch: expected '{level}', got '{existing}'")
            prepared.setdefault("Level", level)
        return prepared


    def _build_options(self, options: Optional[Dict[str, Any]]) -> Optional[str]:
        if not options:
            return None

        option_pairs = []
        for key, value in options.items():
            key_str = str(key)
            if isinstance(value, bool):
                value_str = "true" if value else "false"
            else:
                value_str = str(value)
            option_pairs.append(f"'{key_str}'='{value_str}'")

        return "{" + ", ".join(option_pairs) + "}"

    def _rows_to_tasks(self, rows: Iterable[Sequence[Any]]) -> List[CDCTaskInfo]:
        tasks: List[CDCTaskInfo] = []
        for row in rows or []:
            (
                task_id,
                task_name,
                source_uri,
                sink_uri,
                sink_type,
                tables,
                filters,
                start_ts,
                end_ts,
                state,
                checkpoint,
                no_full,
                err_msg,
                task_create_time,
                additional_config,
            ) = row

            table_mapping = None
            if tables:
                try:
                    parsed_tables = json.loads(tables)
                    if isinstance(parsed_tables, dict):
                        table_mapping = parsed_tables.get("table") or tables
                    else:
                        table_mapping = tables
                except (json.JSONDecodeError, TypeError):
                    table_mapping = tables

            parsed_additional = None
            if additional_config:
                try:
                    parsed = json.loads(additional_config)
                    if isinstance(parsed, dict):
                        parsed_additional = parsed
                except (json.JSONDecodeError, TypeError):
                    parsed_additional = None

            tasks.append(
                CDCTaskInfo(
                    task_id=task_id,
                    task_name=task_name,
                    source_uri=source_uri,
                    sink_uri=sink_uri,
                    sink_type=sink_type,
                    table_mapping=table_mapping,
                    state=state,
                    checkpoint=checkpoint,
                    err_msg=err_msg,
                    created_time=task_create_time,
                    no_full=bool(no_full) if no_full is not None else None,
                    filters=filters,
                    additional_config=parsed_additional,
                    raw_row=row,
                )
            )
        return tasks

    def _rows_to_watermarks(self, rows: Iterable[Sequence[Any]]) -> List[CDCWatermarkInfo]:
        watermarks: List[CDCWatermarkInfo] = []
        for row in rows or []:
            task_id, task_name, db_name, table_name, watermark, err_msg = row
            watermarks.append(
                CDCWatermarkInfo(
                    task_id=task_id,
                    task_name=task_name,
                    database=db_name,
                    table=table_name,
                    watermark=watermark,
                    err_msg=err_msg,
                    raw_row=row,
                )
            )
        return watermarks


class CDCManager(BaseCDCManager):
    """Synchronous CDC manager for MatrixOne."""

    def __init__(self, client, executor=None):
        super().__init__(client)
        self.executor = executor

    def _get_executor(self):
        return self.executor if self.executor else self.client

    def create(
        self,
        task_name: str,
        source_uri: str,
        sink_type: str,
        sink_uri: str,
        table_mapping: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> CDCTaskInfo:
        sql = self._build_create_sql(task_name, source_uri, sink_type, sink_uri, table_mapping, options)
        self._get_executor().execute(sql)
        return CDCTaskInfo(
            task_id=None,
            task_name=task_name,
            source_uri=source_uri,
            sink_uri=sink_uri,
            sink_type=sink_type,
            table_mapping=table_mapping,
            state=None,
            checkpoint=None,
            err_msg=None,
            created_time=None,
            additional_config=options,
        )

    def create_database_task(
        self,
        task_name: str,
        source_uri: str,
        sink_type: str,
        sink_uri: str,
        source_database: str,
        sink_database: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> CDCTaskInfo:
        """Create a database-level CDC task with pythonic parameters."""

        target_database = sink_database or source_database
        prepared_options = self._prepare_options(options, level="database")
        mapping = f"{source_database}:{target_database}"
        return self.create(
            task_name=task_name,
            source_uri=source_uri,
            sink_type=sink_type,
            sink_uri=sink_uri,
            table_mapping=mapping,
            options=prepared_options,
        )

    def create_table_task(
        self,
        task_name: str,
        source_uri: str,
        sink_type: str,
        sink_uri: str,
        table_mappings,
        options: Optional[Dict[str, Any]] = None,
    ) -> CDCTaskInfo:
        """Create a table-level CDC task with high-level mapping helpers."""

        mapping = self._normalise_table_mappings(table_mappings)
        prepared_options = self._prepare_options(options, level="table")
        return self.create(
            task_name=task_name,
            source_uri=source_uri,
            sink_type=sink_type,
            sink_uri=sink_uri,
            table_mapping=mapping,
            options=prepared_options,
        )

    def drop(self, task_name: str) -> None:
        sql = self._build_drop_sql(task_name=task_name)
        self._get_executor().execute(sql)

    def drop_all(self) -> None:
        sql = self._build_drop_sql(all_tasks=True)
        self._get_executor().execute(sql)

    def pause(self, task_name: str) -> None:
        sql = self._build_pause_sql(task_name=task_name)
        self._get_executor().execute(sql)

    def pause_all(self) -> None:
        sql = self._build_pause_sql(all_tasks=True)
        self._get_executor().execute(sql)

    def resume(self, task_name: str) -> None:
        sql = self._build_resume_sql(task_name, restart=False)
        self._get_executor().execute(sql)

    def restart(self, task_name: str) -> None:
        sql = self._build_resume_sql(task_name, restart=True)
        self._get_executor().execute(sql)

    def show_all(self):
        sql = self._build_show_all_sql()
        return self._get_executor().execute(sql)

    def show_task(self, task_name: str):
        sql = self._build_show_task_sql(task_name)
        return self._get_executor().execute(sql)

    def list(self, task_name: Optional[str] = None) -> List[CDCTaskInfo]:
        sql = self._build_select_tasks_sql(task_name)
        result = self._get_executor().execute(sql)
        if hasattr(result, "rows"):
            rows = result.rows
        else:
            rows = result.fetchall() if result else []
        return self._rows_to_tasks(rows)

    def get(self, task_name: str) -> CDCTaskInfo:
        tasks = self.list(task_name)
        if not tasks:
            raise ValueError(f"CDC task '{task_name}' not found")
        return tasks[0]

    def exists(self, task_name: str) -> bool:
        try:
            self.get(task_name)
            return True
        except ValueError:
            return False

    def list_watermarks(self, task_name: Optional[str] = None) -> List[CDCWatermarkInfo]:
        sql = self._build_select_watermarks_sql(task_name)
        result = self._get_executor().execute(sql)
        if hasattr(result, "rows"):
            rows = result.rows
        else:
            rows = result.fetchall() if result else []
        return self._rows_to_watermarks(rows)


class AsyncCDCManager(BaseCDCManager):
    """Asynchronous CDC manager for MatrixOne."""

    def __init__(self, client, executor=None):
        super().__init__(client)
        self.executor = executor

    def _get_executor(self):
        return self.executor if self.executor else self.client

    async def create(
        self,
        task_name: str,
        source_uri: str,
        sink_type: str,
        sink_uri: str,
        table_mapping: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> CDCTaskInfo:
        sql = self._build_create_sql(task_name, source_uri, sink_type, sink_uri, table_mapping, options)
        await self._get_executor().execute(sql)
        return CDCTaskInfo(
            task_id=None,
            task_name=task_name,
            source_uri=source_uri,
            sink_uri=sink_uri,
            sink_type=sink_type,
            table_mapping=table_mapping,
            state=None,
            checkpoint=None,
            err_msg=None,
            created_time=None,
            additional_config=options,
        )

    async def create_database_task(
        self,
        task_name: str,
        source_uri: str,
        sink_type: str,
        sink_uri: str,
        source_database: str,
        sink_database: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> CDCTaskInfo:
        """Create a database-level CDC task asynchronously."""

        target_database = sink_database or source_database
        prepared_options = self._prepare_options(options, level="database")
        mapping = f"{source_database}:{target_database}"
        return await self.create(
            task_name=task_name,
            source_uri=source_uri,
            sink_type=sink_type,
            sink_uri=sink_uri,
            table_mapping=mapping,
            options=prepared_options,
        )

    async def create_table_task(
        self,
        task_name: str,
        source_uri: str,
        sink_type: str,
        sink_uri: str,
        table_mappings,
        options: Optional[Dict[str, Any]] = None,
    ) -> CDCTaskInfo:
        """Create a table-level CDC task asynchronously."""

        mapping = self._normalise_table_mappings(table_mappings)
        prepared_options = self._prepare_options(options, level="table")
        return await self.create(
            task_name=task_name,
            source_uri=source_uri,
            sink_type=sink_type,
            sink_uri=sink_uri,
            table_mapping=mapping,
            options=prepared_options,
        )

    async def drop(self, task_name: str) -> None:
        sql = self._build_drop_sql(task_name=task_name)
        await self._get_executor().execute(sql)

    async def drop_all(self) -> None:
        sql = self._build_drop_sql(all_tasks=True)
        await self._get_executor().execute(sql)

    async def pause(self, task_name: str) -> None:
        sql = self._build_pause_sql(task_name=task_name)
        await self._get_executor().execute(sql)

    async def pause_all(self) -> None:
        sql = self._build_pause_sql(all_tasks=True)
        await self._get_executor().execute(sql)

    async def resume(self, task_name: str) -> None:
        sql = self._build_resume_sql(task_name, restart=False)
        await self._get_executor().execute(sql)

    async def restart(self, task_name: str) -> None:
        sql = self._build_resume_sql(task_name, restart=True)
        await self._get_executor().execute(sql)

    async def show_all(self):
        sql = self._build_show_all_sql()
        return await self._get_executor().execute(sql)

    async def show_task(self, task_name: str):
        sql = self._build_show_task_sql(task_name)
        return await self._get_executor().execute(sql)

    async def list(self, task_name: Optional[str] = None) -> List[CDCTaskInfo]:
        sql = self._build_select_tasks_sql(task_name)
        result = await self._get_executor().execute(sql)
        if hasattr(result, "rows"):
            rows = result.rows
        else:
            rows = result.fetchall() if result else []
        return self._rows_to_tasks(rows)

    async def get(self, task_name: str) -> CDCTaskInfo:
        tasks = await self.list(task_name)
        if not tasks:
            raise ValueError(f"CDC task '{task_name}' not found")
        return tasks[0]

    async def exists(self, task_name: str) -> bool:
        try:
            await self.get(task_name)
            return True
        except ValueError:
            return False

    async def list_watermarks(self, task_name: Optional[str] = None) -> List[CDCWatermarkInfo]:
        sql = self._build_select_watermarks_sql(task_name)
        result = await self._get_executor().execute(sql)
        if hasattr(result, "rows"):
            rows = result.rows
        else:
            rows = result.fetchall() if result else []
        return self._rows_to_watermarks(rows)
