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

"""MatrixOne Change Data Capture (CDC) client helpers.

The module exposes synchronous :class:`CDCManager` and asynchronous
:class:`AsyncCDCManager` helpers that wrap common CDC SQL commands and convert
result sets into typed data classes.  Each manager only requires a connected
MatrixOne client (or session) and takes care of building the appropriate SQL
statements.

Typical usage::

    from matrixone import Client
    from matrixone.cdc import CDCManager

    client = Client()
    client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")

    cdc = CDCManager(client)
    task = cdc.create_table_task(
        task_name="orders_sync",
        source_uri="mysql://sys#root:111@127.0.0.1:6001",
        sink_type="matrixone",
        sink_uri="mysql://sys#root:111@127.0.0.1:6001",
        table_mappings=[("sales", "orders", "backup", "orders")],
        options={"Frequency": "1h"},
    )
    # ... pause/resume/list/drop as required ...
    cdc.drop(task.task_name)

Refer to ``examples/example_31_cdc_operations.py`` for a more complete
end-to-end lifecycle demonstration.
"""

from __future__ import annotations

import datetime
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence


class CDCSinkType(str, Enum):
    """Supported CDC sink types."""

    MYSQL = "mysql"
    MATRIXONE = "matrixone"

    def __str__(self) -> str:  # pragma: no cover - str(Enum)
        return self.value


def build_mysql_uri(host: str, port: int, user: str, password: str = "", account: Optional[str] = None) -> str:
    """Construct a CDC-friendly MySQL URI.

    Parameters
    ----------
    host:
        Hostname or IP address of the MatrixOne/MySQL endpoint.
    port:
        TCP port number.  Must be supplied as an integer.
    user:
        Login username.
    password:
        Optional password.  Defaults to the empty string which generates a
        trailing ``":"`` segment in the authority portion of the URI.
    account:
        Optional MatrixOne account name.  When provided, the URI is rendered in
        ``account#user`` form which is required for cross-account CDC tasks.

    Returns
    -------
    str
        A URI compatible with MatrixOne CDC ``CREATE CDC`` statements.

    Examples
    --------
    >>> build_mysql_uri(host="127.0.0.1", port=6001, user="root", password="111")
    'mysql://root:111@127.0.0.1:6001'
    >>> build_mysql_uri("127.0.0.1", 6001, user="admin", password="pwd", account="demo")
    'mysql://demo#admin:pwd@127.0.0.1:6001'
    """

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


_FREQUENCY_PATTERN = re.compile(r"^(?P<value>\d+)(?P<unit>ms|s|m|h)$", re.IGNORECASE)


def _parse_frequency_to_timedelta(value: str) -> datetime.timedelta:
    match = _FREQUENCY_PATTERN.match(value.strip())
    if not match:
        raise ValueError(f"Unsupported frequency value: {value!r}")
    amount = int(match.group("value"))
    unit = match.group("unit").lower()
    if unit == "ms":
        return datetime.timedelta(milliseconds=amount)
    if unit == "s":
        return datetime.timedelta(seconds=amount)
    if unit == "m":
        return datetime.timedelta(minutes=amount)
    if unit == "h":
        return datetime.timedelta(hours=amount)
    raise ValueError(f"Unsupported frequency unit: {unit}")


def _parse_watermark_timestamp(value: Optional[str]) -> Optional[datetime.datetime]:
    if not value:
        return None
    value = value.strip()
    dt: Optional[datetime.datetime] = None
    for candidate in (value, value.replace(" ", "T")):
        try:
            dt = datetime.datetime.fromisoformat(candidate)
            break
        except ValueError:
            dt = None
    if dt is None:
        formats = (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S.%f%z",
        )
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(value, fmt)
                break
            except ValueError:
                continue
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt


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
    """Shared SQL builders and helpers for CDC managers.

    The base class is primarily an implementation detail – it exposes utility
    methods for constructing CDC SQL statements and parsing result sets.  Most
    applications should interact with :class:`CDCManager` or
    :class:`AsyncCDCManager` instead of instantiating this class directly.
    """

    def __init__(self, client):
        self.client = client

    def _build_create_sql(
        self,
        task_name: str,
        source_uri: str,
        sink_type: CDCSinkType | str,
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
    """Synchronous helper for managing MatrixOne CDC tasks.

    Parameters
    ----------
    client:
        A connected :class:`~matrixone.client.Client` (or session) used to
        execute CDC SQL statements. The manager does **not** open or close
        connections on your behalf.
    executor:
        Optional alternative executor object. When provided the executor must
        expose an ``execute(sql: str)`` method. This is primarily used by the
        SQLAlchemy integration where a session object is supplied.

    Notes
    -----
    All methods in :class:`CDCManager` return Python data classes or ``None``
    and never attempt to parse or coerce complex result sets. For advanced
    scenarios where raw cursor access is required, call
    :meth:`matrixone.client.Client.execute` directly.

    Examples
    --------
    >>> from matrixone import Client
    >>> from matrixone.cdc import CDCManager
    >>> client = Client()
    >>> client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")
    >>> cdc = CDCManager(client)
    >>> task = cdc.create_table_task(
    ...     task_name="orders_sync",
    ...     source_uri="mysql://sys#root:111@127.0.0.1:6001",
    ...     sink_type="matrixone",
    ...     sink_uri="mysql://sys#root:111@127.0.0.1:6001",
    ...     table_mappings=[("sales", "orders", "backup", "orders")],
    ...     options={"Frequency": "1h"},
    ... )
    >>> cdc.pause(task.task_name)
    >>> cdc.resume(task.task_name)
    >>> cdc.drop(task.task_name)
    """

    def __init__(self, client, executor=None):
        super().__init__(client)
        self.executor = executor

    def _get_executor(self):
        return self.executor if self.executor else self.client

    def create(
        self,
        task_name: str,
        source_uri: str,
        sink_type: CDCSinkType | str,
        sink_uri: str,
        table_mapping: str,
        options: Optional[Dict[str, Any]] = None,
    ) -> CDCTaskInfo:
        """Create a CDC task from a raw table-mapping string.

        Parameters
        ----------
        task_name:
            Name of the CDC task. Must be unique within the account.
        source_uri:
            MatrixOne connection URI identifying the source cluster/account.
        sink_type:
            Target sink type. ``"mysql"`` and ``"matrixone"`` are supported.
        sink_uri:
            Connection URI for the target sink.
        table_mapping:
            Raw table mapping string (``source_db.table:sink_db.table``). Use
            :meth:`create_table_task` or :meth:`create_database_task` for
            higher-level helpers.
        options:
            Optional dictionary of ``CREATE CDC`` options. Boolean values are
            converted to ``true``/``false`` automatically.

        Returns
        -------
        CDCTaskInfo
            Lightweight descriptor describing the newly created task. Note that
            MatrixOne assigns task IDs asynchronously, therefore ``task_id`` in
            the returned object is always ``None``.

        Examples
        --------
        >>> cdc.create(
        ...     task_name="simple_sync",
        ...     source_uri="mysql://sys#root:111@127.0.0.1:6001",
        ...     sink_type="matrixone",
        ...     sink_uri="mysql://sys#root:111@127.0.0.1:6001",
        ...     table_mapping="sales.orders:backup.orders",
        ...     options={"NoFull": True},
        ... )
        """
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
        sink_type: CDCSinkType | str,
        sink_uri: str,
        source_database: str,
        sink_database: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> CDCTaskInfo:
        """Create a CDC task that replicates an entire database.

        Parameters
        ----------
        task_name:
            Name of the CDC task.
        source_uri:
            MatrixOne URI of the source cluster/account.
        sink_type:
            CDC sink type (``"mysql"`` or ``"matrixone"``).
        sink_uri:
            Target connection URI.
        source_database:
            Name of the source database.
        sink_database:
            Optional sink database.  When omitted, the source database name is
            reused.
        options:
            Additional ``CREATE CDC`` options. The ``Level`` option is enforced
            to ``"database"``.

        Returns
        -------
        CDCTaskInfo
            Minimal descriptor of the created task.
        """

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
        """Create a CDC task for one or more tables.

        Parameters
        ----------
        task_name:
            Name of the CDC task.
        source_uri:
            MatrixOne URI of the source cluster/account.
        sink_type:
            CDC sink type (``"mysql"`` or ``"matrixone"``).
        sink_uri:
            Target connection URI.
        table_mappings:
            Table mapping specification expressed as

            * raw mapping string, e.g. ``"db1.t1:backup.t1"``;
            * sequence of mapping strings;
            * dictionary mapping source to sink tables;
            * sequence of tuples describing source/sink components (see
              :meth:`_normalise_table_mappings`).
        options:
            Additional ``CREATE CDC`` options. ``Level`` is enforced to
            ``"table"``.

        Returns
        -------
        CDCTaskInfo
            Minimal descriptor of the created task.
        """

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
        """Drop a single CDC task."""
        sql = self._build_drop_sql(task_name=task_name)
        self._get_executor().execute(sql)

    def drop_all(self) -> None:
        """Drop all CDC tasks owned by the current account."""
        sql = self._build_drop_sql(all_tasks=True)
        self._get_executor().execute(sql)

    def pause(self, task_name: str) -> None:
        """Pause a running CDC task."""
        sql = self._build_pause_sql(task_name=task_name)
        self._get_executor().execute(sql)

    def pause_all(self) -> None:
        """Pause every CDC task in the account."""
        sql = self._build_pause_sql(all_tasks=True)
        self._get_executor().execute(sql)

    def resume(self, task_name: str) -> None:
        """Resume a paused CDC task."""
        sql = self._build_resume_sql(task_name, restart=False)
        self._get_executor().execute(sql)

    def restart(self, task_name: str) -> None:
        """Restart a CDC task (pause + resume)."""
        sql = self._build_resume_sql(task_name, restart=True)
        self._get_executor().execute(sql)

    def show_all(self):
        """Execute ``SHOW CDC ALL`` and return the raw cursor/result."""
        sql = self._build_show_all_sql()
        return self._get_executor().execute(sql)

    def show_task(self, task_name: str):
        """Execute ``SHOW CDC TASK`` for a specific task."""
        sql = self._build_show_task_sql(task_name)
        return self._get_executor().execute(sql)

    def list(self, task_name: Optional[str] = None) -> List[CDCTaskInfo]:
        """Return CDC tasks as :class:`CDCTaskInfo` objects."""
        sql = self._build_select_tasks_sql(task_name)
        result = self._get_executor().execute(sql)
        if hasattr(result, "rows"):
            rows = result.rows
        else:
            rows = result.fetchall() if result else []
        return self._rows_to_tasks(rows)

    def get(self, task_name: str) -> CDCTaskInfo:
        """Retrieve a single CDC task or raise :class:`ValueError`."""
        tasks = self.list(task_name)
        if not tasks:
            raise ValueError(f"CDC task '{task_name}' not found")
        return tasks[0]

    def exists(self, task_name: str) -> bool:
        """Return ``True`` when the task exists, ``False`` otherwise."""
        try:
            self.get(task_name)
            return True
        except ValueError:
            return False

    def list_watermarks(self, task_name: Optional[str] = None) -> List[CDCWatermarkInfo]:
        """Return CDC watermark rows as :class:`CDCWatermarkInfo`."""
        sql = self._build_select_watermarks_sql(task_name)
        result = self._get_executor().execute(sql)
        if hasattr(result, "rows"):
            rows = result.rows
        else:
            rows = result.fetchall() if result else []
        return self._rows_to_watermarks(rows)

    # ------------------------------------------------------------------
    # Operational helpers
    # ------------------------------------------------------------------
    def list_failing_tasks(self) -> List[CDCTaskInfo]:
        """Return tasks with non-empty ``err_msg`` fields."""

        tasks = self.list()
        return [task for task in tasks if task.err_msg]

    def list_stuck_tasks(self) -> List[CDCTaskInfo]:
        """Return running tasks whose tables report errors."""

        failing_tasks: List[CDCTaskInfo] = []
        for task in self.list():
            if task.state and task.state.lower() != "running":
                continue
            watermarks = self.list_watermarks(task.task_name)
            if any(mark.err_msg for mark in watermarks):
                failing_tasks.append(task)
        return failing_tasks

    def list_late_table_watermarks(
        self,
        *,
        task_name: Optional[str] = None,
        default_threshold: datetime.timedelta = datetime.timedelta(minutes=10),
        thresholds: Optional[Dict[str, datetime.timedelta]] = None,
    ) -> List[CDCWatermarkInfo]:
        """Find tables whose watermarks lag behind their expected schedule.

        For each watermark the helper attempts to determine an acceptable lag
        threshold:

        * If ``thresholds`` contains an entry for ``task_name.table`` (or just
          ``table`` when ``task_name`` is not provided) that value is used.
        * Otherwise the task's configured ``Frequency`` option is used plus a
          10 minute buffer.
        * If no frequency can be determined the ``default_threshold`` is used.
        """

        tasks_by_name = {task.task_name: task for task in self.list(task_name)}
        thresholds = thresholds or {}
        now = datetime.datetime.now(datetime.timezone.utc)

        def resolve_threshold(task: CDCTaskInfo, table: str) -> datetime.timedelta:
            keys = [f"{task.task_name}.{table}", table]
            for key in keys:
                if key in thresholds:
                    return thresholds[key]

            freq = None
            if task.additional_config:
                freq = task.additional_config.get("Frequency")
            if not freq and task.table_mapping:
                task_options = task.additional_config or {}
                freq = task_options.get("Frequency")
            if freq:
                try:
                    return _parse_frequency_to_timedelta(str(freq)) + datetime.timedelta(minutes=10)
                except ValueError:
                    pass
            return default_threshold

        late_watermarks: List[CDCWatermarkInfo] = []
        for mark in self.list_watermarks(task_name):
            task = tasks_by_name.get(mark.task_name or "")
            if task is None:
                continue

            watermark_time = _parse_watermark_timestamp(mark.watermark)
            if watermark_time is None:
                continue
            threshold = resolve_threshold(task, mark.table or "")
            if now - watermark_time > threshold:
                late_watermarks.append(mark)

        return late_watermarks


class AsyncCDCManager(BaseCDCManager):
    """Asynchronous CDC manager for MatrixOne.

    The asynchronous API mirrors :class:`CDCManager` but all mutating
    operations are ``async`` coroutines.
    """

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
