#!/usr/bin/env python3

"""Basic online tests for CDC manager integration."""

# Copyright 2021 - 2022 Matrix Origin
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

from __future__ import annotations

import uuid
import pytest

from matrixone.cdc import CDCTaskInfo, CDCWatermarkInfo


@pytest.mark.online
def test_cdc_exists_for_missing_task(test_client):
    """`Client.cdc.exists()` should return False for non-existent task names."""

    task_name = f"pytest_cdc_{uuid.uuid4().hex[:8]}"
    assert test_client.cdc.exists(task_name) is False


@pytest.mark.online
def test_cdc_list_returns_dataclasses(test_client):
    """`Client.cdc.list()` should return CDCTaskInfo objects for existing tasks."""

    tasks = test_client.cdc.list()
    assert isinstance(tasks, list)
    for task in tasks:
        assert isinstance(task, CDCTaskInfo)
        assert isinstance(task.task_name, str)


@pytest.mark.online
def test_cdc_list_watermarks_returns_dataclasses(test_client):
    """`Client.cdc.list_watermarks()` returns CDCWatermarkInfo objects."""

    watermarks = test_client.cdc.list_watermarks()
    assert isinstance(watermarks, list)
    for info in watermarks:
        assert isinstance(info, CDCWatermarkInfo)
        assert isinstance(info.task_id, str)

