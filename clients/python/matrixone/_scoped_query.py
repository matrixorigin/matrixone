# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Buffered fulltext execution with one owner for temporary session settings."""

from contextlib import nullcontext

from ._sql_literals import sql_literal
from ._utils import discard_unrestored_connection
from .exceptions import QueryError


def _read(connection):
    result = connection.exec_driver_sql('SELECT @@ft_relevancy_algorithm')
    try:
        return result.scalar()
    finally:
        result.close()


def execute_fulltext(client, sql, algorithm):
    from .client import ResultSet

    owner = getattr(client, 'client', None) or client
    context = owner._engine.connect() if client is owner else nullcontext(client.connection())
    with context as connection:
        previous = _read(connection)
        failed = False
        try:
            connection.exec_driver_sql('SET ft_relevancy_algorithm=' + sql_literal(algorithm)).close()
            result = owner._execute_with_logging(connection, sql)
            try:
                return ResultSet(list(result.keys()), result.fetchall())
            finally:
                result.close()
        except BaseException:
            failed = True
            raise
        finally:
            try:
                connection.exec_driver_sql('SET ft_relevancy_algorithm=' + sql_literal(previous)).close()
            except BaseException as error:
                try:
                    discard_unrestored_connection(connection)
                except BaseException:
                    if not failed:
                        raise
                if not failed:
                    if not isinstance(error, Exception):
                        raise
                    raise QueryError('Fulltext query completed but session setting restoration failed') from error


async def execute_fulltext_async(client, sql, algorithm):
    from contextlib import asynccontextmanager
    from .async_client import AsyncResultSet

    owner = getattr(client, 'client', None) or client

    @asynccontextmanager
    async def checkout():
        if client is owner:
            async with owner._engine.connect() as connection:
                yield connection
        else:
            yield await client.connection()

    async with checkout() as connection:
        result = await connection.exec_driver_sql('SELECT @@ft_relevancy_algorithm')
        try:
            previous = result.scalar()
        finally:
            result.close()
        failed = False
        try:
            result = await connection.exec_driver_sql('SET ft_relevancy_algorithm=' + sql_literal(algorithm))
            result.close()
            result = await owner._execute_with_logging(connection, sql)
            try:
                return AsyncResultSet(list(result.keys()), result.fetchall())
            finally:
                result.close()
        except BaseException:
            failed = True
            raise
        finally:
            try:
                result = await connection.exec_driver_sql('SET ft_relevancy_algorithm=' + sql_literal(previous))
                result.close()
            except BaseException as error:
                try:
                    await connection.run_sync(discard_unrestored_connection)
                except BaseException:
                    if not failed:
                        raise
                if not failed:
                    if not isinstance(error, Exception):
                        raise
                    raise QueryError('Fulltext query completed but session setting restoration failed') from error
