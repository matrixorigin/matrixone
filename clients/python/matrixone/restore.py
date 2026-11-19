# Copyright 2021 - 2022 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Snapshot restore operations.

Restore is performed by the server in its own transaction. It cannot be made
atomic with statements in a client session, so session-bound managers reject it.
"""

from typing import Optional

from ._sql_literals import sql_identifier, sql_string_token
from .exceptions import RestoreError


def _restore_sql(level, snapshot, account=None, database=None, table=None, to_account=None):
    sql_identifier(snapshot)  # Snapshot names have the same identifier limits.
    targets = {
        'CLUSTER': (),
        'ACCOUNT': (account,),
        'DATABASE': (account, database),
        'TABLE': (account, database, table),
    }
    names = targets[level]
    target = '.'.join(sql_identifier(name) for name in names)
    sql = 'RESTORE ' + level + (' ' + target if target else '')
    sql += '{snapshot=' + sql_string_token(snapshot, context='Snapshot') + '}'
    if to_account is not None:
        sql += ' TO ACCOUNT ' + sql_identifier(to_account)
    return sql


class _RestoreBase:
    def __init__(self, client, executor=None):
        self.client = client
        self.executor = executor

    def _statement(self, level, snapshot, account=None, database=None, table=None, to_account=None):
        if self.executor is not None:
            raise RestoreError('Snapshot restore cannot participate in a session transaction; use client.restore')
        return _restore_sql(level, snapshot, account, database, table, to_account)


class RestoreManager(_RestoreBase):
    """Restore snapshots through the client, outside session transactions."""

    def _restore(self, level, snapshot, account=None, database=None, table=None, to_account=None):
        try:
            sql = self._statement(level, snapshot, account, database, table, to_account)
            return self.client.execute(sql) is not None
        except RestoreError:
            raise
        except Exception as exc:
            raise RestoreError(f'Failed to restore {level.lower()} from snapshot {snapshot!r}: {exc}') from exc

    def restore_cluster(self, snapshot_name: str) -> bool:
        return self._restore('CLUSTER', snapshot_name)

    def restore_tenant(self, snapshot_name: str, account_name: str, to_account: Optional[str] = None) -> bool:
        return self._restore('ACCOUNT', snapshot_name, account_name, to_account=to_account)

    def restore_database(
        self, snapshot_name: str, account_name: str, database_name: str, to_account: Optional[str] = None
    ) -> bool:
        return self._restore('DATABASE', snapshot_name, account_name, database_name, to_account=to_account)

    def restore_table(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        table_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        return self._restore('TABLE', snapshot_name, account_name, database_name, table_name, to_account)


class AsyncRestoreManager(_RestoreBase):
    """Async snapshot restore through the client, outside session transactions."""

    async def _restore(self, level, snapshot, account=None, database=None, table=None, to_account=None):
        try:
            sql = self._statement(level, snapshot, account, database, table, to_account)
            return await self.client.execute(sql) is not None
        except RestoreError:
            raise
        except Exception as exc:
            raise RestoreError(f'Failed to restore {level.lower()} from snapshot {snapshot!r}: {exc}') from exc

    async def restore_cluster(self, snapshot_name: str) -> bool:
        return await self._restore('CLUSTER', snapshot_name)

    async def restore_tenant(self, snapshot_name: str, account_name: str, to_account: Optional[str] = None) -> bool:
        return await self._restore('ACCOUNT', snapshot_name, account_name, to_account=to_account)

    async def restore_database(
        self, snapshot_name: str, account_name: str, database_name: str, to_account: Optional[str] = None
    ) -> bool:
        return await self._restore('DATABASE', snapshot_name, account_name, database_name, to_account=to_account)

    async def restore_table(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        table_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        return await self._restore('TABLE', snapshot_name, account_name, database_name, table_name, to_account)
