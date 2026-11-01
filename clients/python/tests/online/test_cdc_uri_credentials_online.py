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

"""Exercise CDC URI credentials through a real MatrixOne service."""

from contextlib import ExitStack
from uuid import uuid4

import pytest

from matrixone import Client
from matrixone.cdc import build_mysql_uri
from .test_config import online_config


@pytest.mark.online
@pytest.mark.parametrize('password', ['plain111', 'p:a:ss', 'p%40word'])
def test_cdc_credentials_roundtrip(password):
    account = 'cdc_uri_' + uuid4().hex[:12]
    root = Client()
    with ExitStack() as cleanup:
        cleanup.callback(root.disconnect)
        root.connect(host=online_config.host, port=online_config.port, user=online_config.user,
                     password=online_config.password, database='mo_catalog')
        root.execute(f"CREATE ACCOUNT `{account}` ADMIN_NAME 'admin' IDENTIFIED BY '{password}'")
        cleanup.callback(root.execute, f'DROP ACCOUNT `{account}`')

        tenant = Client()
        cleanup.callback(tenant.disconnect)
        tenant.connect(host=online_config.host, port=online_config.port, user='admin', account=account,
                       password=password, database='mo_catalog')
        tenant.execute('CREATE DATABASE cdc_src')
        cleanup.callback(tenant.execute, 'DROP DATABASE cdc_src')
        tenant.execute('CREATE TABLE cdc_src.items (id INT PRIMARY KEY)')
        tenant.execute("CREATE PITR pitr_cdc_uri FOR DATABASE cdc_src RANGE 2 'h'")
        cleanup.callback(tenant.execute, 'DROP PITR pitr_cdc_uri')
        uri = build_mysql_uri(online_config.host, online_config.port, 'admin', password, account)
        result = tenant.cdc.create_database_task('cdc_uri_task', uri, 'matrixone', uri, 'cdc_src', 'cdc_sink')
        cleanup.callback(tenant.cdc.drop, 'cdc_uri_task')
        assert result.task_name == 'cdc_uri_task'
        rows = tenant.cdc.show_task('cdc_uri_task').rows
        assert any(row[1] == 'cdc_uri_task' for row in rows)
