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

"""
Offline tests for matrixone.vecblock cell layout and SQL helpers, which need no PyTorch.
The tensor functions are tested in tests/torch.

The reference cells are what MatrixOne stores for the same input:
vecblock_binary(CAST('[1,-2,0.5,3,0,-0.25,7,100]' AS vecf8(8))) and
vecblock_binary(CAST('[1,-2,0.5,3,0,-0.25,7]' AS vecf4(7))).
"""

import sys

import pytest

from matrixone import vecblock

VECF8_CELL = bytes.fromhex("01010000080000000000803F7D48D0405400B85E7C")
VECF4_CELL = bytes.fromhex("0102000007000000ABAA2A3B7EB2510007")


def test_layout():
    assert vecblock.cell_size("vecf8", 8) == len(VECF8_CELL) == 12 + 1 + 8
    assert vecblock.cell_size("vecf4", 7) == len(VECF4_CELL) == 12 + 1 + 4
    assert vecblock.cell_size("vecf8", 768) == 804
    assert vecblock.cell_size("vecf4", 768) == 444
    assert vecblock.cell_format(VECF8_CELL) == "vecf8"
    assert vecblock.cell_format(VECF4_CELL) == "vecf4"


def test_cell_format_rejects_non_cells():
    for bad in [b"", VECF8_CELL[:11], bytes([2]) + VECF8_CELL[1:], VECF8_CELL[:1] + bytes([3]) + VECF8_CELL[2:]]:
        with pytest.raises(ValueError):
            vecblock.cell_format(bad)


def test_sql_helpers():
    assert vecblock.blob_literal(b"\x01\xab") == "x'01ab'"
    assert vecblock.cell_sql(VECF8_CELL) == f"CAST(CAST(x'{VECF8_CELL.hex()}' AS BLOB) AS vecf8(8))"
    assert vecblock.cell_sql(VECF4_CELL).endswith("AS vecf4(7))")


def test_tensor_functions_need_torch(monkeypatch):
    monkeypatch.setitem(sys.modules, "torch", None)
    for call in (
        lambda: vecblock.from_cells([VECF8_CELL]),
        lambda: vecblock.quantize([[1.0]], "vecf8"),
        lambda: vecblock.to_blocked([[1]]),
    ):
        with pytest.raises(ImportError, match="matrixone-python-sdk\\[torch\\]"):
            call()
