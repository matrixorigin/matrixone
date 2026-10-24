# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from matrixone.analytics import AnalyticsManager


class FaultEngine:
    def __init__(self, phase, cleanup_failure):
        self.phase = phase
        self.cleanup_failure = cleanup_failure
        self.catalog = set()
        self.operations = []
        self.connections = 0
        self.primary = ConnectionError('CREATE acknowledgement unavailable')
        self.cleanup = PermissionError('DROP unavailable')

    @contextmanager
    def begin(self):
        self.connections += 1
        if self.phase == 'acquire' and self.connections == 1:
            raise self.primary
        connection_id = self.connections
        operation = []

        def execute(sql, params=None):
            name = sql.split('`')[1]
            action = 'CREATE' if sql.startswith('CREATE') else 'DROP'
            operation.append(action)
            self.operations.append((action, name, connection_id))
            if action == 'CREATE':
                if self.phase == 'execute':
                    raise self.primary
                self.catalog.add(name)
            else:
                if self.cleanup_failure is True:
                    raise self.cleanup
                self.catalog.discard(name)

        yield SimpleNamespace(exec_driver_sql=execute)
        if operation and operation[0] == 'CREATE' and self.phase == 'commit':
            raise self.primary
        if operation and operation[0] == 'DROP' and self.cleanup_failure == 'commit':
            raise self.cleanup
        if operation and operation[0] == 'CREATE' and self.phase == 'collision':
            from pymysql.err import OperationalError as DriverError
            from sqlalchemy.exc import OperationalError

            raise OperationalError('COMMIT', None, DriverError(1050, 'Concurrent table already exists'))


@pytest.mark.parametrize(
    'phase,cleanup_failure',
    [
        ('normal', False),
        ('acquire', False),
        ('execute', False),
        ('commit', False),
        ('execute', True),
        ('commit', True),
        ('commit', 'commit'),
    ],
)
def test_relation_construction_ownership_faults(tmp_path, phase, cleanup_failure):
    path = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'id': [1]}), path)
    engine = FaultEngine(phase, cleanup_failure)
    logger = Mock()
    logger.warning.side_effect = RuntimeError('logging unavailable')
    client = SimpleNamespace(_engine=engine, logger=logger)
    manager = AnalyticsManager(client)
    if phase == 'normal':
        relation = manager.read_parquet(path)
        assert len(engine.catalog) == 1
        engine.cleanup_failure = False
        relation.close()
        assert not engine.catalog
        assert [op[0] for op in engine.operations] == ['CREATE', 'DROP']
        return
    with pytest.raises(ConnectionError) as caught:
        manager.read_parquet(path)
    error = caught.value
    assert error is engine.primary
    attempted = error.attempted_tables
    if phase == 'acquire':
        assert attempted == error.owned_tables == ()
        assert engine.operations == [] and error.file_relation._closed
        return
    assert len(attempted) == 1
    assert [op[0] for op in engine.operations] == ['CREATE', 'DROP']
    assert engine.operations[0][1] == engine.operations[1][1] == attempted[0]
    assert engine.operations[0][2] != engine.operations[1][2]
    if cleanup_failure:
        assert error.cleanup_error is engine.cleanup
        assert error.owned_tables == attempted
        replacement = FaultEngine('normal', False)
        replacement.catalog.update(attempted)
        client._engine = replacement
        engine.cleanup_failure = False
        error.file_relation.close()
        assert engine.operations[-1][0] == 'DROP'
        assert replacement.catalog == set(attempted)
        assert replacement.operations == []
    else:
        assert error.owned_tables == ()
        assert error.file_relation._closed
    assert not engine.catalog


def test_commit_collision_does_not_drop_foreign_table(tmp_path):
    from sqlalchemy.exc import OperationalError

    path = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'id': [1]}), path)
    engine = FaultEngine('collision', False)
    manager = AnalyticsManager(SimpleNamespace(_engine=engine, logger=Mock()))
    with pytest.raises(OperationalError) as caught:
        manager.read_parquet(path)
    assert caught.value.owned_tables == ()
    assert len(engine.catalog) == 1
    assert [operation[0] for operation in engine.operations] == ['CREATE']
