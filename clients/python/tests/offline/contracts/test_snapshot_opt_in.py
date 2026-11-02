"""Importing MatrixOne must not add a global method to SQLAlchemy Select."""

import subprocess
import sys

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, select as sa_select

from matrixone import select, with_snapshot, compile_select_to_sql
from matrixone.sqlalchemy_ext import compile_select, with_snapshot as ext_with_snapshot


def test_imports_and_explicit_compatibility_method_in_fresh_process():
    script = '''
from sqlalchemy.sql.selectable import Select
from sqlalchemy import select
assert not hasattr(Select, 'with_snapshot')
import matrixone
import matrixone.sqlalchemy_ext.snapshot
assert not hasattr(Select, 'with_snapshot')
from matrixone.sqlalchemy_ext import enable_snapshot_method, with_snapshot
prior = select(1)
enable_snapshot_method()
assert Select.with_snapshot is with_snapshot
enable_snapshot_method()
assert prior.with_snapshot('nightly')._annotations['matrixone_snapshot'] == 'nightly'
assert not prior._annotations
'''
    subprocess.run([sys.executable, '-c', script], check=True, capture_output=True, text=True, timeout=20)


@pytest.mark.parametrize('foreign', ['None', 'lambda self, name: self', 'property(lambda self: None)'])
def test_opt_in_never_overwrites_an_existing_owner(foreign):
    script = '''
from sqlalchemy.sql.selectable import Select
foreign = FOREIGN
Select.with_snapshot = foreign
import matrixone
assert Select.__dict__['with_snapshot'] is foreign
try:
    matrixone.enable_snapshot_method()
except RuntimeError as error:
    assert 'another owner' in str(error)
else:
    raise AssertionError('foreign method was overwritten')
assert Select.__dict__['with_snapshot'] is foreign
'''.replace('FOREIGN', foreign)
    subprocess.run([sys.executable, '-c', script], check=True, capture_output=True, text=True, timeout=20)


def test_standalone_and_keyword_share_the_original_annotation_owner():
    table = Table('users', MetaData(), Column('id', Integer), schema='db')
    live = sa_select(table.c.id)
    history = with_snapshot(live, 'nightly')
    assert ext_with_snapshot is with_snapshot
    assert not live._annotations
    assert compile_select(history) == compile_select_to_sql(select(table.c.id, snapshot='nightly'))
    assert "{snapshot = 'nightly'}" in compile_select(history)
    for invalid in (table, table.c.id, object(), None):
        with pytest.raises(TypeError, match='Select'):
            with_snapshot(invalid, 'nightly')
