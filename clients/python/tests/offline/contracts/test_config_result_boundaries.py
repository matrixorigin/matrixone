"""Local configuration admission and buffered result consumption contracts."""

from decimal import Decimal

import numpy as np
import pytest

from matrixone import config
from matrixone.client import ResultSet
from matrixone.async_client import AsyncResultSet


@pytest.fixture
def clean_config_env(monkeypatch):
    for variable in config.MatrixOneConfig.ENV_MAPPING.values():
        monkeypatch.delenv(variable, raising=False)


CONFIG_OUTPUTS = [
    config.MatrixOneConfig.get_config,
    config.get_config,
    config.MatrixOneConfig.get_connection_kwargs,
    config.get_connection_kwargs,
    config.get_client_kwargs,
]


@pytest.mark.parametrize('output', CONFIG_OUTPUTS)
@pytest.mark.parametrize(
    'key,variable,value', [('port', 'MATRIXONE_PORT', 6002), ('connect_timeout', 'MATRIXONE_CONNECT_TIMEOUT', 7)]
)
def test_explicit_config_precedes_invalid_environment(clean_config_env, monkeypatch, output, key, variable, value):
    monkeypatch.setenv(variable, 'invalid')
    result = output(**{key: value})
    result_key = (
        'connection_timeout'
        if key == 'connect_timeout' and output.__name__ in ('get_client_kwargs', 'get_connection_kwargs')
        else key
    )
    assert result[result_key] == value


@pytest.mark.parametrize('output', [config.get_connection_params, config.MatrixOneConfig.get_connection_params])
def test_tuple_config_preserves_override_order(clean_config_env, monkeypatch, output):
    monkeypatch.setenv('MATRIXONE_PORT', 'invalid')
    monkeypatch.setenv('MATRIXONE_CONNECT_TIMEOUT', 'invalid')
    assert output(port=6002, connect_timeout=7) == ('127.0.0.1', 6002, 'root', '111', 'test')


def test_falsey_overrides_are_supplied_values(clean_config_env, monkeypatch):
    overrides = dict(port=0, connect_timeout=0, autocommit=False, password='', on_connect=None)
    for key in overrides:
        monkeypatch.setenv(config.MatrixOneConfig.ENV_MAPPING[key], 'invalid')
    real_getenv = config.os.getenv
    overridden_variables = {config.MatrixOneConfig.ENV_MAPPING[key] for key in overrides}

    def getenv(variable, default=None):
        assert variable not in overridden_variables
        return real_getenv(variable, default)

    monkeypatch.setattr(config.os, 'getenv', getenv)
    result = config.get_config(**overrides)
    kwargs = config.get_connection_kwargs(**overrides)
    assert (
        kwargs['port'],
        kwargs['connection_timeout'],
        kwargs['auto_commit'],
        kwargs['password'],
        kwargs['on_connect'],
    ) == (0, 0, False, '', None)
    assert {key: result[key] for key in overrides} == overrides


@pytest.mark.parametrize('variable', ['MATRIXONE_PORT', 'MATRIXONE_CONNECT_TIMEOUT'])
def test_invalid_selected_environment_remains_an_error(clean_config_env, monkeypatch, variable):
    monkeypatch.setenv(variable, 'invalid')
    with pytest.raises(ValueError):
        config.get_connection_kwargs(host='explicit-host')


def test_selected_environment_still_converts_types(clean_config_env, monkeypatch):
    monkeypatch.setenv('MATRIXONE_PORT', '6003')
    monkeypatch.setenv('MATRIXONE_CONNECT_TIMEOUT', '9')
    monkeypatch.setenv('MATRIXONE_AUTOCOMMIT', 'false')
    result = config.get_connection_kwargs()
    assert (result['port'], result['connection_timeout'], result['auto_commit']) == (6003, 9, False)
    assert config.get_connection_kwargs(port=6002)['port'] == 6002


@pytest.mark.parametrize('result_type', [ResultSet, AsyncResultSet])
@pytest.mark.parametrize(
    'rows,consumed', [([], 0), ([(10,), (20,), (30,)], 0), ([(10,), (20,), (30,)], 1), ([(10,), (20,), (30,)], 3)]
)
@pytest.mark.parametrize('size', [-1, -100])
def test_negative_fetch_preserves_remaining_rows(result_type, rows, consumed, size):
    result = result_type(['id'], rows, affected_rows=7)
    assert result.fetchmany(consumed) == rows[:consumed]
    with pytest.raises(ValueError, match='non-negative'):
        result.fetchmany(size)
    assert result.rows is rows and result.columns == ['id'] and result.affected_rows == 7
    assert result.fetchall() == rows[consumed:]
    assert result.fetchone() is None


@pytest.mark.parametrize('result_type', [ResultSet, AsyncResultSet])
def test_fetch_zero_default_and_oversized_controls(result_type):
    result = result_type(['id'], [(10,), (20,), (30,)])
    assert result.fetchmany(0) == []
    assert result.fetchmany() == [(10,)]
    assert result.fetchmany(100) == [(20,), (30,)]
    assert result.fetchmany() == []


@pytest.mark.parametrize('result_type', [ResultSet, AsyncResultSet])
@pytest.mark.parametrize('size', [-1.0, 1.0, Decimal('-1'), None, '1'])
def test_noninteger_fetch_preserves_type_error_and_rows(result_type, size):
    result = result_type(['id'], [(10,), (20,)])
    assert result.fetchone() == (10,)
    with pytest.raises(TypeError):
        result.fetchmany(size)
    assert result.fetchall() == [(20,)]


@pytest.mark.parametrize('result_type', [ResultSet, AsyncResultSet])
def test_fetch_accepts_integer_index_protocol(result_type):
    result = result_type(['id'], [(10,), (20,), (30,)])
    assert result.fetchmany(np.int64(1)) == [(10,)]
    with pytest.raises(ValueError, match='non-negative'):
        result.fetchmany(np.int64(-1))
    assert result.fetchmany(False) == []
    assert result.fetchmany(True) == [(20,)]
    assert result.fetchall() == [(30,)]


@pytest.mark.parametrize('output', [config.print_config, config.MatrixOneConfig.print_config])
def test_print_config_observes_override_and_masks_password(clean_config_env, monkeypatch, capsys, output):
    monkeypatch.setenv('MATRIXONE_PORT', 'invalid')
    monkeypatch.setenv('MATRIXONE_CONNECT_TIMEOUT', 'invalid')
    output(port=6002, connect_timeout=7, password='not-for-display')
    printed = capsys.readouterr().out
    assert 'port: 6002' in printed and 'connect_timeout: 7' in printed
    assert 'not-for-display' not in printed


def test_configuration_calls_keep_defaults_independent(clean_config_env):
    defaults = config.MatrixOneConfig.DEFAULT_CONFIG.copy()
    first = config.get_config(custom='value')
    first['host'] = 'changed'
    assert config.get_config() == defaults
    assert config.MatrixOneConfig.DEFAULT_CONFIG == defaults
