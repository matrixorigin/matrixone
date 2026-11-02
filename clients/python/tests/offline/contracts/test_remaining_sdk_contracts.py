"""Public boundary regressions for restore, load, status and diagnostics."""

import builtins
from unittest.mock import Mock

import pytest

from matrixone import Client
from matrixone.sqlalchemy_ext.ivf_config import IVFConfig


@pytest.mark.parametrize('value,expected', [('ON', True), ('off', False), ('1', True), ('0', False), (b'on', True)])
def test_ivf_status_decodes_server_boolean(value, expected):
    conn = Mock()
    conn.exec_driver_sql.side_effect = [
        Mock(fetchone=Mock(return_value=('experimental_ivf_index', value))),
        Mock(fetchone=Mock(return_value=('probe_limit', '3'))),
    ]
    engine = Mock()
    engine.begin.return_value.__enter__ = Mock(return_value=conn)
    engine.begin.return_value.__exit__ = Mock(return_value=False)
    assert IVFConfig(engine).get_ivf_status() == {'ivf_enabled': expected, 'probe_limit': 3, 'error': None}


def test_analytics_dependency_failure_is_not_reported_as_missing_extra(monkeypatch):
    real_import = builtins.__import__

    def fail(name, *args, **kwargs):
        if name == 'pyarrow':
            raise ImportError('incompatible numpy')
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', fail)
    with pytest.raises(ImportError, match='incompatible') as error:
        Client().analytics.query('SELECT 1')
    assert str(error.value.__cause__) == 'incompatible numpy'


def test_ivf_status_reports_unrecognized_value():
    conn = Mock()
    conn.exec_driver_sql.return_value.fetchone.return_value = ('experimental_ivf_index', 'unknown')
    engine = Mock()
    engine.begin.return_value.__enter__ = Mock(return_value=conn)
    engine.begin.return_value.__exit__ = Mock(return_value=False)
    status = IVFConfig(engine).get_ivf_status()
    assert status['ivf_enabled'] is None and 'Unrecognized' in status['error']


def test_analytics_missing_extra_diagnostic(monkeypatch):
    real_import = builtins.__import__

    def fail(name, *args, **kwargs):
        if name == 'pyarrow':
            raise ModuleNotFoundError("No module named 'pyarrow'", name='pyarrow')
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', fail)
    with pytest.raises(ImportError, match=r'install|Install.*\[analytics\]'):
        Client().analytics.query('SELECT 1')
