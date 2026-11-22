# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""The index health report includes tables whose only index is FULLTEXT."""

from contextlib import closing
import sqlite3
from types import SimpleNamespace

from matrixone import cli_tools


def test_fulltext_only_table_is_reported_without_primary_or_other_database(monkeypatch, capsys):
    monkeypatch.setattr(cli_tools, 'PROMPT_TOOLKIT_AVAILABLE', False)
    with closing(sqlite3.connect(':memory:')) as database:
        database.executescript(
            "ATTACH DATABASE ':memory:' AS mo_catalog;"
            "CREATE TABLE mo_catalog.mo_tables(rel_id INT, relname TEXT, reldatabase TEXT);"
            "CREATE TABLE mo_catalog.mo_indexes(table_id INT, type TEXT);"
            "INSERT INTO mo_catalog.mo_tables VALUES(1,'ft_only','main'),(2,'primary_only','main'),(3,'other_db','other');"
            "INSERT INTO mo_catalog.mo_indexes VALUES(1,'FULLTEXT'),(1,'FULLTEXT'),(2,'PRIMARY'),(3,'FULLTEXT');"
            "CREATE TABLE ft_only(id INT); INSERT INTO ft_only VALUES(1);"
        )

        def execute(sql, params=()):
            with closing(database.execute(sql, params)) as cursor:
                rows = cursor.fetchall()
            return SimpleNamespace(fetchall=lambda: rows, fetchone=lambda: rows[0] if rows else None)

        details = []

        def indexes(table, schema):
            details.append((table, schema))
            return [dict(index_name='ft_body', algo='fulltext')]

        client = SimpleNamespace(
            _connection_params=dict(database='main'), execute=execute, get_table_indexes_detail=indexes
        )
        cli_tools.MatrixOneCLI(client).onecmd('show_all_indexes')
        output = capsys.readouterr().out
        assert details == [('ft_only', 'main')]
        assert 'ft_only' in output and 'Fulltext index' in output and '1 rows' in output
        assert 'Total: 1 tables with indexes' in output
        assert 'primary_only' not in output and 'other_db' not in output
