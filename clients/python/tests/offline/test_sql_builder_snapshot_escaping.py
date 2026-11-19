# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import pytest

from matrixone.sql_builder import MatrixOneSQLBuilder


@pytest.mark.parametrize(
    "snapshot, expected",
    [
        ("daily_snapshot", "'daily_snapshot'"),
        ("release'1", "'release''1'"),
        ("s' OR '1'='1' -- ", "'s'' OR ''1''=''1'' -- '"),
    ],
)
def test_snapshot_name_is_rendered_as_a_safe_string_token(snapshot, expected):
    sql, params = MatrixOneSQLBuilder().select_all().from_table("records", snapshot=snapshot).build()

    assert sql == f"SELECT * FROM records{{snapshot = {expected}}}"
    assert params == []


@pytest.mark.parametrize("snapshot", ["snapshot\\name", "snapshot\x00name"])
def test_snapshot_name_rejects_sql_mode_dependent_characters(snapshot):
    builder = MatrixOneSQLBuilder().select_all().from_table("records", snapshot=snapshot)

    with pytest.raises(ValueError, match="Snapshot string tokens require text without NUL or backslashes"):
        builder.build()
