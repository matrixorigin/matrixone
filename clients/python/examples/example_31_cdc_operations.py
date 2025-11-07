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
"""Example: Manage CDC lifecycle with the MatrixOne Python SDK."""

from __future__ import annotations

import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional

from matrixone import Client, build_mysql_uri
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger


class CDCOperationsExample:
    """Run-through of CDC task lifecycle operations."""

    def __init__(self) -> None:
        self.logger = create_default_logger()
        self.account = os.getenv("MATRIXONE_TEST_ACCOUNT", "sys")
        self.task_name: Optional[str] = None
        self.pitr_name: Optional[str] = None
        self.backup_database: Optional[str] = None
        self.connection_kwargs: Optional[dict] = None
        self.table_tasks: list[str] = []
        self.table_names: list[str] = []
        self.results = {
            "steps_run": 0,
            "steps_passed": 0,
            "steps_failed": 0,
            "step_details": [],
        }

    def run(self) -> None:
        host, port, user, password, database = get_connection_params()
        print_config()

        self.connection_kwargs = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }

        client = Client()
        client.connect(**self.connection_kwargs)

        try:
            self.logger.info(
                "Connected to MatrixOne", extra={"host": host, "port": port, "database": database}
            )
            self._run_lifecycle(client, host, port, user, password, database)
        except Exception as err:
            self.logger.error("CDC lifecycle execution failed", exc_info=err)
            raise
        finally:
            self._print_summary()
            self._cleanup(client)
            try:
                client.disconnect()
            except Exception as err:  # pragma: no cover - best effort cleanup
                self.logger.warning("Failed to disconnect cleanly", exc_info=err)

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------
    def _run_lifecycle(
        self,
        client: Client,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ) -> None:
        """Create, pause, resume, and inspect a CDC task."""

        self.task_name = f"example_cdc_{uuid.uuid4().hex[:8]}"
        self.pitr_name = f"example_cdc_pitr_{uuid.uuid4().hex[:8]}"
        self.backup_database = f"{database}_cdc_demo_{uuid.uuid4().hex[:6]}"
        source_uri = build_mysql_uri(host, port, user, password, account=self.account)

        self._execute_step("Create CDC task", lambda: self._create_task(client, database, source_uri))
        self._execute_step("Pause CDC task", lambda: self._pause_and_verify(client))
        self._execute_step("Resume CDC task", lambda: self._resume_and_verify(client))
        self._execute_step(
            "Create table-level CDC tasks",
            lambda: self._create_table_tasks(client, database, source_uri),
        )
        self._execute_step(
            "Verify table-level CDC tasks",
            lambda: self._verify_table_tasks(client),
        )
        self._execute_step(
            "Drop table-level CDC tasks",
            lambda: self._drop_table_tasks(client),
        )

    def _create_task(self, client: Client, database: str, source_uri: str) -> None:
        assert self.task_name and self.backup_database
        self.logger.info(
            "Creating CDC task",
            extra={
                "task_name": self.task_name,
                "backup_database": self.backup_database,
                "pitr_name": self.pitr_name,
                "account": self.account,
            },
        )
        self._prepare_environment(client, database)

        def _do_create():
            client.cdc.create_database_task(
                task_name=self.task_name,
                source_uri=source_uri,
                sink_type="matrixone",
                sink_uri=source_uri,
                source_database=database,
                sink_database=self.backup_database,
                options={"Frequency": "1h"},
            )

        self._retry(_do_create, attempts=3, delay=6.0)
        self._wait_for_task_state(client, self.task_name, "running")
        self._log_task_state(client, "running after create", self.task_name)

    def _pause_and_verify(self, client: Client) -> None:
        self._pause_task(client)
        self._wait_for_task_state(client, self.task_name, "paused")
        self._log_task_state(client, "paused", self.task_name)

    def _resume_and_verify(self, client: Client) -> None:
        self._resume_task(client)
        self._wait_for_task_state(client, self.task_name, "running")
        self._log_task_state(client, "running after resume", self.task_name)

    def _create_table_tasks(self, client: Client, database: str, source_uri: str) -> None:
        assert self.connection_kwargs and self.backup_database

        table_configs = [
            {"label": "alpha", "options": {"NoFull": True, "Frequency": "1h"}},
            {"label": "beta", "options": {"Frequency": "2h", "MaxSqlLength": 2097152}},
            {
                "label": "gamma",
                "options": {"Frequency": "24h", "SendSqlTimeout": "45m", "InitSnapshotSplitTxn": False},
            },
        ]

        for cfg in table_configs:
            table = f"cdc_demo_table_{cfg['label']}"
            if table not in self.table_names:
                self.table_names.append(table)
            client.execute(
                f"CREATE TABLE IF NOT EXISTS {database}.{table} (id INT PRIMARY KEY, value VARCHAR(255))"
            )
            client.execute(
                f"CREATE TABLE IF NOT EXISTS {self.backup_database}.{table} (id INT PRIMARY KEY, value VARCHAR(255))"
            )

        def worker(cfg):
            table = f"cdc_demo_table_{cfg['label']}"
            task_name = f"table_{cfg['label']}_{uuid.uuid4().hex[:6]}"
            worker_client = Client()
            worker_client.connect(**self.connection_kwargs)
            try:
                worker_client.cdc.create_table_task(
                    task_name=task_name,
                    source_uri=source_uri,
                    sink_type="matrixone",
                    sink_uri=source_uri,
                    table_mappings=[(database, table, self.backup_database, table)],
                    options=cfg["options"],
                )
                return task_name
            finally:
                try:
                    worker_client.disconnect()
                except Exception:
                    pass

        with ThreadPoolExecutor(max_workers=len(table_configs)) as executor:
            futures = [executor.submit(worker, cfg) for cfg in table_configs]
            for future in as_completed(futures):
                task_name = future.result()
                self.table_tasks.append(task_name)
                self._wait_for_task_state(client, task_name, "running")

    def _verify_table_tasks(self, client: Client) -> None:
        for task_name in self.table_tasks:
            assert client.cdc.exists(task_name), f"CDC task {task_name} should exist"
            show_result = client.cdc.show_task(task_name)
            assert show_result.rows, f"SHOW CDC TASK returned no rows for {task_name}"
            entries = client.cdc.list(task_name)
            assert entries, f"CDC list empty for {task_name}"

    def _drop_table_tasks(self, client: Client) -> None:
        while self.table_tasks:
            task_name = self.table_tasks.pop()
            try:
                client.cdc.drop(task_name)
                self.logger.info("Dropped table-level CDC task", extra={"task_name": task_name})
            except Exception as err:  # pragma: no cover
                self.logger.warning("Failed to drop table-level CDC task", exc_info=err)

    def _prepare_environment(self, client: Client, database: str) -> None:
        assert self.pitr_name and self.backup_database

        existing = client.pitr.list(database_name=database) or []
        for existing_pitr in existing:
            try:
                client.pitr.delete(existing_pitr.name)
            except Exception as err:  # pragma: no cover - best effort cleanup
                self.logger.warning(
                    "Failed to drop existing PITR", extra={"pitr": existing_pitr.name}, exc_info=err
                )

        client.execute(f"CREATE DATABASE IF NOT EXISTS {self.backup_database}")
        # Ensure PITR window comfortably exceeds the longest CDC frequency used (24h)
        client.pitr.create_database_pitr(self.pitr_name, database, range_value=48, range_unit="h")

    def _pause_task(self, client: Client) -> None:
        assert self.task_name
        self.logger.info("Pausing CDC task", extra={"task_name": self.task_name})
        self._retry(lambda: client.cdc.pause(self.task_name), attempts=3, delay=6.0)

    def _resume_task(self, client: Client) -> None:
        assert self.task_name
        self.logger.info("Resuming CDC task", extra={"task_name": self.task_name})
        self._retry(lambda: client.cdc.resume(self.task_name), attempts=3, delay=6.0)

    def _log_task_state(self, client: Client, label: str, task_name: Optional[str]) -> None:
        show_result = client.cdc.show_task(task_name)
        state_info = dict(zip(show_result.columns or [], show_result.rows[0])) if show_result.rows else {}

        entries = client.cdc.list(task_name)
        watermarks = client.cdc.list_watermarks(task_name)

        self.logger.info(
            "CDC task snapshot",
            extra={
                "label": label,
                "show_task": state_info,
                "list_count": len(entries),
                "watermark_count": len(watermarks),
            },
        )

    def _wait_for_task_state(
        self, client: Client, task_name: str, expected: str, timeout: float = 240.0
    ) -> None:
        expected = expected.lower()
        deadline = time.time() + timeout
        last_state: Optional[str] = None

        while time.time() < deadline:
            info = client.cdc.get(task_name)
            last_state = (info.state or "").lower()
            if last_state == expected:
                return
            time.sleep(2.0)

        raise RuntimeError(
            f"Task '{task_name}' did not reach state '{expected}' within {timeout}s"
            f" (last: {last_state})"
        )

    def _execute_step(self, label: str, func: Callable[[], None]) -> None:
        self.results["steps_run"] += 1
        self.logger.info("=== %s ===" % label)
        try:
            func()
        except Exception as err:
            self.results["steps_failed"] += 1
            self.results["step_details"].append({"step": label, "status": "failed", "error": str(err)})
            self.logger.error("%s failed: %s" % (label, err))
            raise
        else:
            self.results["steps_passed"] += 1
            self.results["step_details"].append({"step": label, "status": "passed"})
            self.logger.info("%s succeeded" % label)

    def _print_summary(self) -> None:
        print("\n=== CDC Lifecycle Summary ===")
        print(f"Steps run: {self.results['steps_run']}")
        print(f"Steps passed: {self.results['steps_passed']}")
        print(f"Steps failed: {self.results['steps_failed']}")
        if self.results["step_details"]:
            for detail in self.results["step_details"]:
                status = detail["status"].upper()
                if detail["status"] == "failed":
                    print(f" - {status}: {detail['step']} (error: {detail['error']})")
                else:
                    print(f" - {status}: {detail['step']}")
        print("==============================\n")

    def _retry(self, func: Callable[[], None], *, attempts: int = 30, delay: float = 0.1) -> None:
        for attempt in range(1, attempts + 1):
            try:
                func()
                return
            except Exception:
                if attempt == attempts:
                    raise
                time.sleep(delay)

    def _cleanup(self, client: Client) -> None:
        while self.table_tasks:
            task_name = self.table_tasks.pop()
            try:
                client.cdc.drop(task_name)
                self.logger.info("Dropped table-level CDC task", extra={"task_name": task_name})
            except Exception as err:  # pragma: no cover
                self.logger.warning("Failed to drop table-level CDC task", exc_info=err)

        if self.task_name:
            try:
                client.cdc.drop(self.task_name)
                self.logger.info("Dropped CDC task", extra={"task_name": self.task_name})
            except Exception as err:  # pragma: no cover
                self.logger.warning("Failed to drop CDC task", exc_info=err)

        if self.pitr_name:
            try:
                client.pitr.delete(self.pitr_name)
                self.logger.info("Dropped PITR", extra={"pitr": self.pitr_name})
            except Exception as err:  # pragma: no cover
                self.logger.warning("Failed to drop PITR", exc_info=err)

        if self.backup_database:
            try:
                client.execute(f"DROP DATABASE IF EXISTS {self.backup_database}")
                self.logger.info("Dropped backup database", extra={"database": self.backup_database})
            except Exception as err:  # pragma: no cover
                self.logger.warning("Failed to drop backup database", exc_info=err)

        if self.connection_kwargs and self.table_names:
            source_db = self.connection_kwargs["database"]
            for table in self.table_names:
                try:
                    client.execute(f"DROP TABLE IF EXISTS {source_db}.{table}")
                except Exception:  # pragma: no cover
                    pass


if __name__ == "__main__":
    example = CDCOperationsExample()
    example.run()
