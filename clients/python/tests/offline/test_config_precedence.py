# Copyright 2026 Matrix Origin
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

"""Configuration precedence through the SDK public configuration entry points."""

import pytest

from matrixone.config import (
    MatrixOneConfig,
    get_config,
    get_connection_kwargs,
    get_connection_params,
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    for name in MatrixOneConfig.ENV_MAPPING.values():
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize('key,env', [
    ('port', 'MATRIXONE_PORT'),
    ('connect_timeout', 'MATRIXONE_CONNECT_TIMEOUT'),
])
@pytest.mark.parametrize('value', [0, 42])
def test_explicit_numeric_override_ignores_invalid_environment(monkeypatch, key, env, value):
    monkeypatch.setenv(env, 'invalid')
    assert get_config(**{key: value})[key] == value
    # Ignoring an overridden value must not mutate defaults or the environment.
    with pytest.raises(ValueError):
        get_config()


def test_connection_outputs_respect_overrides(monkeypatch):
    monkeypatch.setenv('MATRIXONE_PORT', 'invalid')
    monkeypatch.setenv('MATRIXONE_CONNECT_TIMEOUT', 'invalid')
    monkeypatch.setenv('MATRIXONE_HOST', 'environment-host')
    overrides = dict(port=6002, connect_timeout=0, password='', autocommit=False)
    kwargs = get_connection_kwargs(**overrides)
    assert kwargs['host'] == 'environment-host'
    assert kwargs['port'] == 6002
    assert kwargs['connection_timeout'] == 0
    assert kwargs['password'] == ''
    assert kwargs['auto_commit'] is False
    assert get_connection_params(**overrides) == ('environment-host', 6002, 'root', '', 'test')


@pytest.mark.parametrize('env', ['MATRIXONE_PORT', 'MATRIXONE_CONNECT_TIMEOUT'])
def test_unoverridden_invalid_environment_still_rejected(monkeypatch, env):
    monkeypatch.setenv(env, 'invalid')
    with pytest.raises(ValueError):
        get_config(host='explicit-host')


def test_environment_and_defaults_still_apply(monkeypatch):
    monkeypatch.setenv('MATRIXONE_PORT', '6003')
    monkeypatch.setenv('MATRIXONE_CONNECT_TIMEOUT', '12')
    monkeypatch.setenv('MATRIXONE_AUTOCOMMIT', 'false')
    config = get_config()
    assert config['port'] == 6003
    assert config['connect_timeout'] == 12
    assert config['autocommit'] is False
    assert config['host'] == '127.0.0.1'
    monkeypatch.delenv('MATRIXONE_PORT')
    assert get_config()['port'] == 6001
