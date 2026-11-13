# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Qualify native prerequisites without masking unknown server failures."""

import os

import pytest

from matrixone import Client
from scripts.run_contracts import validate_server_identity


@pytest.fixture(scope='session')
def native_arrow_load_check():
    return _require_native_arrow_load


@pytest.fixture(scope='session')
def native_arrow_load(native_arrow_load_check):
    native_arrow_load_check()


def _require_native_arrow_load():
    # Only this exact released grammar has been independently qualified here.
    # Current/unknown versions must run the positive tests and expose failures.
    if os.environ.get('MO_ANALYTICS_ONLINE') != '1' or os.environ.get('MO_SDK_TARGET') != '4.2.8':
        return
    client = Client(sql_log_mode='off')
    try:
        client.connect(
            host=os.environ['MO_HOST'],
            port=int(os.environ['MO_PORT']),
            user=os.environ['MO_USER'],
            password=os.environ['MO_PASSWORD'],
            database=os.environ['MO_DATABASE'],
        )
        version, revision = client.execute('SELECT version(),git_version()').rows[0]
        validate_server_identity(version, revision, '4.2.8', os.environ.get('MO_SDK_EXPECT_REVISION'))
        capability = client.get_capabilities()['arrow_load']
        if capability.state == 'unsupported':
            pytest.skip('MatrixOne 4.2.8 rejects native Arrow LOAD; positive Arrow write path was not validated')
    finally:
        client.disconnect()
