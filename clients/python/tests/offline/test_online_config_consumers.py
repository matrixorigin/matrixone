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

"""Exercise real online connection entry points without opening a database."""

import asyncio
import importlib.util
import inspect
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.online import test_config as config_module

from . import test_online_config_validation as validation

ENVIRONMENT = {
    'MATRIXONE_HOST': 'isolated-cn.example',
    'MATRIXONE_PORT': '16001',
    'MATRIXONE_USER': 'sdk_ci',
    'MATRIXONE_PASSWORD': 'synthetic',
    'MATRIXONE_DATABASE': 'sdk_ci_test',
}
REMOTE = dict(host='isolated-cn.example', port=16001, user='sdk_ci', password='synthetic', database='sdk_ci_test')
DEFAULT = dict(host='127.0.0.1', port=6001, user='root', password='111', database='test')
ONLINE = Path(__file__).resolve().parents[1] / 'online'


@pytest.fixture
def online_module(monkeypatch, tmp_path):
    """Load actual consumers with import effects confined to this test."""
    package = sys.modules['tests.online']
    missing = object()
    loaded = []
    monkeypatch.setattr(sys, 'path', list(sys.path))
    monkeypatch.chdir(tmp_path)

    def load(name):
        fullname = f'tests.online.{name}'
        loaded.append((fullname, name, sys.modules.get(fullname, missing), vars(package).get(name, missing)))
        spec = importlib.util.spec_from_file_location(fullname, ONLINE / f'{name}.py')
        module = importlib.util.module_from_spec(spec)
        # Import-time tmpfiles paths belong to tmp_path, never the checkout.
        module.__file__ = str(tmp_path / 'tests' / 'online' / f'{name}.py')
        sys.modules[fullname] = module
        setattr(package, name, module)
        spec.loader.exec_module(module)
        return module

    try:
        yield load
    finally:
        for fullname, name, old_module, old_attribute in reversed(loaded):
            if old_module is missing:
                sys.modules.pop(fullname, None)
            else:
                sys.modules[fullname] = old_module
            if old_attribute is missing:
                delattr(package, name)
            else:
                setattr(package, name, old_attribute)


class StopAtConnection(RuntimeError):
    """Prevent SQL and resource acquisition after observing connection selection."""


# Every migrated constructor, method and unwrapped fixture has a named oracle.
CONSUMERS = [
    ('test_client_online', 'TestClientOnline.setUpClass'),
    ('test_index_verification_online', 'TestIndexVerificationOnline.setUpClass'),
    ('test_export_online', 'test_client'),
    ('test_stage_online', 'test_client'),
    ('test_json_online', 'client'),
    ('test_async_transaction_simple_query', 'TestAsyncTransactionSimpleQuery.async_client_setup'),
    ('test_load_data_online', 'TestAsyncLoadDataOperations.test_async_basic_csv_load'),
    ('test_load_data_online', 'TestAsyncLoadDataOperations.test_async_jsonline_load'),
    ('test_load_data_online', 'TestAsyncLoadDataOperations.test_async_transaction_load'),
    ('test_load_data_online', 'TestAsyncLoadDataOperations.test_async_stage_load'),
    ('test_load_data_online', 'TestAsyncLoadDataOperations.test_async_inline_load'),
    ('test_stage_online', 'TestAsyncStageOperations.test_async_create_and_list'),
    ('test_stage_online', 'TestAsyncStageOperations.test_async_load_from_stage'),
    ('test_stage_online', 'TestAsyncStageOperations.test_async_create_local'),
    ('test_stage_online', 'TestAsyncStageOperations.test_async_create_s3_with_env'),
    ('test_stage_online', 'TestAsyncStageExtendedOperations.test_async_stage_show'),
    ('test_stage_online', 'TestAsyncStageExtendedOperations.test_async_session_stage_operations'),
    ('test_pinecone_filter', 'TestPineconeFilter.test_async_filter_functionality'),
    ('test_pinecone_filter', 'TestPineconeFilter.test_async_query_with_all_parameters'),
    ('test_pinecone_filter', 'TestPineconeFilter.test_async_query_with_complex_filter'),
]


def invoke_until_connection(module, entry, monkeypatch, error=StopAtConnection):
    calls = []

    class CapturingClient:
        def __init__(self, **kwargs):
            if kwargs:
                self.connect(**kwargs)

        def connect(self, **kwargs):
            calls.append(kwargs)
            raise error('injected at connection boundary')

    monkeypatch.setattr(module, 'Client', CapturingClient, raising=False)
    monkeypatch.setattr(module, 'AsyncClient', CapturingClient, raising=False)
    target = module
    for name in entry.split('.'):
        target = getattr(target, name)
    target = inspect.unwrap(target)
    args = {}
    for name in inspect.signature(target).parameters:
        args[name] = {
            'self': SimpleNamespace(),
            'cls': SimpleNamespace(),
            'async_client': CapturingClient(),
            'test_data_setup': None,
            'test_database': 'fixture_owned_db',
        }[name]
    with pytest.raises(error, match='injected at connection boundary'):
        result = target(**args)
        if inspect.iscoroutine(result):
            asyncio.run(result)
        elif inspect.isasyncgen(result):

            async def advance():
                try:
                    await result.__anext__()
                finally:
                    await result.aclose()

            asyncio.run(advance())
        elif inspect.isgenerator(result):
            try:
                next(result)
            finally:
                result.close()
    assert len(calls) == 1, 'failed admission must not retry against a default target'
    return calls[0]


@pytest.mark.parametrize('deployment', ['remote', 'default'])
@pytest.mark.parametrize('name,entry', CONSUMERS, ids=[f'{name}:{entry}' for name, entry in CONSUMERS])
def test_real_consumers_select_configured_deployment(online_module, monkeypatch, deployment, name, entry):
    for key, value in ENVIRONMENT.items():
        if deployment == 'remote':
            monkeypatch.setenv(key, value)
        else:
            monkeypatch.delenv(key, raising=False)
    module = online_module(name)
    monkeypatch.setattr(module, 'online_config', config_module.OnlineTestConfig())
    expected = dict(REMOTE if deployment == 'remote' else DEFAULT)
    if name == 'test_pinecone_filter':
        expected['database'] = 'fixture_owned_db'
    assert invoke_until_connection(module, entry, monkeypatch) == expected


@pytest.mark.parametrize('name,entry', [CONSUMERS[2], CONSUMERS[6]])
def test_connection_error_does_not_fall_back(online_module, monkeypatch, name, entry):
    for key, value in ENVIRONMENT.items():
        monkeypatch.setenv(key, value)
    module = online_module(name)
    monkeypatch.setattr(module, 'online_config', config_module.OnlineTestConfig())
    assert invoke_until_connection(module, entry, monkeypatch, error=ConnectionError) == REMOTE


def test_orm_bootstrap_worker_and_cleanup_keep_database_ownership(online_module, monkeypatch):
    for key, value in ENVIRONMENT.items():
        monkeypatch.setenv(key, value)
    module = online_module('test_async_orm_online')
    monkeypatch.setattr(module, 'online_config', config_module.OnlineTestConfig())
    connections, disconnects = [], []

    class Client:
        async def connect(self, **kwargs):
            connections.append(kwargs)

        async def execute(self, sql):
            pass

        async def disconnect(self):
            disconnects.append(self)

    monkeypatch.setattr(module, 'AsyncClient', Client)

    async def run():
        generator = inspect.unwrap(module.TestAsyncORMOnline.test_async_client)(None, 'fixture_owned_db')
        try:
            await generator.__anext__()
        finally:
            await generator.aclose()

    asyncio.run(run())
    assert connections == [REMOTE, {**REMOTE, 'database': 'fixture_owned_db'}, REMOTE]
    assert len(disconnects) == 3 and len({id(client) for client in disconnects}) == 3


@pytest.mark.parametrize('initial', ['populated', 'absent', 'mixed'])
@pytest.mark.parametrize('exit_mode', ['success', 'assertion', 'constructor'])
def test_actual_environment_case_restores_values_and_absence(monkeypatch, initial, exit_mode):
    for index, (key, value) in enumerate(ENVIRONMENT.items()):
        if initial == 'populated' or (initial == 'mixed' and index % 2 == 0):
            monkeypatch.setenv(key, value)
        else:
            monkeypatch.delenv(key, raising=False)
    before = {key: os.environ[key] for key in ENVIRONMENT if key in os.environ}
    binding = config_module.online_config
    with monkeypatch.context() as patch:
        if exit_mode == 'assertion':
            patch.setattr(config_module, 'OnlineTestConfig', lambda: SimpleNamespace(host='wrong'))
        elif exit_mode == 'constructor':

            def fail():
                raise RuntimeError('constructor failed')

            patch.setattr(config_module, 'OnlineTestConfig', fail)
        case = validation.TestConfigValidation().test_config_respects_environment_variables
        if exit_mode == 'success':
            case(monkeypatch)
        else:
            with pytest.raises(AssertionError if exit_mode == 'assertion' else RuntimeError):
                case(monkeypatch)
    assert {key: os.environ[key] for key in ENVIRONMENT if key in os.environ} == before
    assert config_module.online_config is binding


def test_next_real_consumer_and_next_configuration_generation(online_module, monkeypatch):
    for key, value in ENVIRONMENT.items():
        monkeypatch.setenv(key, value)
    module = online_module('test_index_verification_online')
    monkeypatch.setattr(module, 'online_config', config_module.OnlineTestConfig())
    before = invoke_until_connection(module, 'TestIndexVerificationOnline.setUpClass', monkeypatch)
    validation.TestConfigValidation().test_config_respects_environment_variables(monkeypatch)
    monkeypatch.setattr(module, 'online_config', config_module.OnlineTestConfig())
    assert invoke_until_connection(module, 'TestIndexVerificationOnline.setUpClass', monkeypatch) == before == REMOTE
    monkeypatch.setenv('MATRIXONE_HOST', 'next-generation.example')
    monkeypatch.setattr(module, 'online_config', config_module.OnlineTestConfig())
    assert invoke_until_connection(module, 'TestIndexVerificationOnline.setUpClass', monkeypatch) == {
        **REMOTE,
        'host': 'next-generation.example',
    }


def test_connection_arguments_are_fresh_and_preserve_explicit_empty_database():
    config = config_module.OnlineTestConfig()
    first = config.get_connection_kwargs()
    first['host'] = 'must-not-mutate-config'
    assert config.get_connection_kwargs()['host'] == config.host
    assert config.get_connection_kwargs(database='')['database'] == ''
