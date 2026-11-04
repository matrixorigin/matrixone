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

"""Exercise SDK logging through real standard-library handlers."""

import inspect
import logging
import uuid

import pytest

from matrixone.logger import MatrixOneLogger


@pytest.fixture
def logging_target():
    name = 'sdk_logger_' + uuid.uuid4().hex
    target = logging.getLogger(name)
    records = []

    class Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = Capture()
    target.addHandler(handler)
    target.propagate = False
    try:
        yield target, records
    finally:
        target.removeHandler(handler)
        handler.close()
        logging.Logger.manager.loggerDict.pop(name, None)


@pytest.mark.parametrize(
    'level,expected',
    [
        (logging.DEBUG, ['debug', 'info', 'warning', 'error', 'critical']),
        (logging.WARNING, ['warning', 'error', 'critical']),
        (logging.ERROR, ['error', 'critical']),
    ],
)
def test_sdk_respects_custom_logger_level(logging_target, level, expected):
    target, records = logging_target
    target.setLevel(level)
    sdk = MatrixOneLogger(logger=target)
    for name in ['debug', 'info', 'warning', 'error', 'critical']:
        getattr(sdk, name)(name)
    assert [record.getMessage() for record in records] == expected


def test_sdk_respects_inherited_level(logging_target):
    target, records = logging_target
    target.parent = logging.Logger('parent', logging.ERROR)
    target.setLevel(logging.NOTSET)
    sdk = MatrixOneLogger(logger=target)
    sdk.warning('hidden')
    sdk.error('visible')
    assert [record.getMessage() for record in records] == ['visible']


def test_sdk_respects_global_disable(logging_target):
    target, records = logging_target
    target.setLevel(logging.DEBUG)
    sdk = MatrixOneLogger(logger=target)
    previous = logging.root.manager.disable
    try:
        logging.disable(logging.CRITICAL)
        sdk.critical('hidden')
        logging.disable(logging.NOTSET)
        sdk.error('visible')
    finally:
        logging.disable(previous)
    assert [record.getMessage() for record in records] == ['visible']


def test_query_logging_and_runtime_level_changes(logging_target):
    target, records = logging_target
    target.setLevel(logging.ERROR)
    sdk = MatrixOneLogger(logger=target, sql_log_mode='full')
    sdk.log_query('SELECT 1', success=True)
    sdk.log_query('SELECT 2', success=False)
    sdk.set_level(logging.INFO)
    sdk.log_connection('localhost', 6001, 'user', 'test')
    assert [record.levelno for record in records] == [logging.ERROR, logging.INFO]
    assert '[ERROR] SELECT 2' in records[0].getMessage()
    assert 'Connected to MatrixOne' in records[1].getMessage()


def test_enabled_logs_preserve_caller_location(logging_target):
    target, records = logging_target
    target.setLevel(logging.INFO)
    sdk = MatrixOneLogger(logger=target)
    line = inspect.currentframe().f_lineno + 1
    sdk.info('visible')
    assert records[0].pathname == __file__
    assert records[0].lineno == line
