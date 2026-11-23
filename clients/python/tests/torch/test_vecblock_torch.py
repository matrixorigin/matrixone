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
Tests for the matrixone.vecblock tensor functions. They need PyTorch; the scaled_mm tests also need a GPU
with block-scaled torch._scaled_mm. CI does not install PyTorch and does not collect this directory.

The reference cells are what MatrixOne stores for the same input:
vecblock_binary(CAST('[1,-2,0.5,3,0,-0.25,7,100]' AS vecf8(8))) and
vecblock_binary(CAST('[1,-2,0.5,3,0,-0.25,7]' AS vecf4(7))).
"""

import math

import pytest

torch = pytest.importorskip("torch")

from matrixone import vecblock  # noqa: E402

VECF8_INPUT = [1, -2, 0.5, 3, 0, -0.25, 7, 100]
VECF8_CELL = bytes.fromhex("01010000080000000000803F7D48D0405400B85E7C")
VECF8_DECODED = [1, -2, 0.5, 3, 0, -0.25, 7, 96]

VECF4_INPUT = [1, -2, 0.5, 3, 0, -0.25, 7]
VECF4_CELL = bytes.fromhex("0102000007000000ABAA2A3B7EB2510007")
VECF4_DECODED = [1.1666667, -1.7500001, 0.5833334, 3.5000002, 0, 0, 7.0000005]


def f32(values):
    return torch.tensor(values, dtype=torch.float32)


def test_split_dtypes():
    b8 = vecblock.from_cells([VECF8_CELL])
    assert b8.element_tensor().dtype == torch.float8_e4m3fn and tuple(b8.element_tensor().shape) == (1, 8)
    assert b8.scale_tensor().dtype == torch.float8_e8m0fnu and tuple(b8.scale_tensor().shape) == (1, 1)
    assert b8.global_scale.tolist() == [1.0]
    b4 = vecblock.from_cells([VECF4_CELL])
    assert b4.element_tensor().dtype == torch.float4_e2m1fn_x2 and tuple(b4.element_tensor().shape) == (1, 4)
    assert b4.scale_tensor().dtype == torch.float8_e4m3fn and tuple(b4.scale_tensor().shape) == (1, 1)


def test_decode_equals_matrixone():
    assert torch.equal(vecblock.from_cells([VECF8_CELL]).to_float()[0], f32(VECF8_DECODED))
    assert torch.equal(vecblock.from_cells([VECF4_CELL]).to_float()[0], f32(VECF4_DECODED))


def test_quantize_equals_matrixone():
    assert vecblock.quantize(f32([VECF8_INPUT]), "vecf8").to_cells() == [VECF8_CELL]
    assert vecblock.quantize(f32([VECF4_INPUT]), "vecf4").to_cells() == [VECF4_CELL]


def test_round_trip():
    x = torch.randn(37, 70, generator=torch.Generator().manual_seed(1)) * 10
    for fmt in ("vecf8", "vecf4"):
        cells = vecblock.quantize(x, fmt).to_cells()
        assert all(len(c) == vecblock.cell_size(fmt, 70) for c in cells)
        assert vecblock.from_cells(cells).to_cells() == cells


def test_quantize_zeros_and_signs():
    for fmt in ("vecf8", "vecf4"):
        cells = vecblock.quantize(f32([[0.0] * 33, [-0.0] * 33]), fmt).to_cells()
        assert cells[0] == cells[1]
        b = vecblock.from_cells(cells)
        assert int(b.elements.sum()) == 0 and int(b.scales.sum()) == 0
        assert torch.equal(b.to_float(), torch.zeros(2, 33))


def test_quantize_error_bound():
    x = torch.randn(64, 256, generator=torch.Generator().manual_seed(2))
    for fmt, bound in (("vecf8", 0.05), ("vecf4", 0.2)):
        dec = vecblock.quantize(x, fmt).to_float()
        rel = ((dec - x).norm(dim=1) / x.norm(dim=1)).max().item()
        assert rel < bound, (fmt, rel)


def test_quantize_rejects_non_finite():
    for bad in (math.nan, math.inf):
        with pytest.raises(ValueError):
            vecblock.quantize(f32([[1.0, bad]]), "vecf8")
    with pytest.raises(ValueError):
        vecblock.quantize(f32([1.0, 2.0]), "vecf8")
    with pytest.raises(ValueError):
        vecblock.quantize(f32([[1.0]]), "vecf6")


def test_from_cells_rejects_mixed_cells():
    with pytest.raises(ValueError):
        vecblock.from_cells([VECF8_CELL, VECF4_CELL])
    with pytest.raises(ValueError):
        vecblock.from_cells([])


def test_to_blocked_layout():
    scales = torch.arange(130 * 5, dtype=torch.uint8).view(130, 5)
    blocked = vecblock.to_blocked(scales)
    assert tuple(blocked.shape) == (256 * 8,)
    # row r, column c lands in tile (r // 128, c // 4) at 32-row interleaved position
    r, c = 129, 4
    tile = (r // 128) * 2 + (c // 4)
    offset = tile * 512 + (r % 32) * 16 + ((r % 128) // 32) * 4 + (c % 4)
    assert int(blocked[offset]) == int(scales[r, c])


# MatrixOne's cells for CAST('[2^-149]' AS vecf4(1)), CAST('[1.5*2^-140,6*2^-140]' AS vecf4(2)),
# CAST('[3.0e38]' AS vecf8(1)) and CAST('[3.4028234663852886e38]' AS vecf4(1)).
VECF4_SUBNORMAL_CELL = bytes.fromhex("0102000001000000010000002307")
VECF4_SUBNORMAL_PAIR_CELL = bytes.fromhex("0102000002000000010000007E73")
VECF8_NEAR_MAX_CELL = bytes.fromhex("01010000010000000000803FF776")
VECF4_MAX_CELL = bytes.fromhex("0102000001000000300CC3797E07")


def test_decode_subnormal_block_scale():
    tiny = math.ldexp(1.0, -149)
    assert vecblock.quantize(f32([[tiny]]), "vecf4").to_cells() == [VECF4_SUBNORMAL_CELL]
    assert vecblock.from_cells([VECF4_SUBNORMAL_CELL]).to_float().tolist() == [[tiny]]
    # elements 1.5 and 6 under a subnormal global * blockScale keep their 1:4 ratio
    b = vecblock.quantize(f32([[1.5 * 2**-140, 6 * 2**-140]]), "vecf4")
    assert b.to_cells() == [VECF4_SUBNORMAL_PAIR_CELL]
    dec = b.to_float()[0].double()
    assert dec[1] / dec[0] == 4


def test_quantize_rejects_infinite_decode():
    fmax = torch.finfo(torch.float32).max
    for v in (fmax, -fmax, 3.3e38):
        with pytest.raises(ValueError, match="decodes to an infinite value"):
            vecblock.quantize(f32([[v, 1.0]]), "vecf8")
    assert vecblock.quantize(f32([[3.0e38]]), "vecf8").to_cells() == [VECF8_NEAR_MAX_CELL]
    b = vecblock.quantize(f32([[fmax]]), "vecf4")
    assert b.to_cells() == [VECF4_MAX_CELL]
    assert torch.isfinite(b.to_float()).all()


def _scaled_mm_available():
    if not torch.cuda.is_available():
        return False
    try:
        x = vecblock.quantize(torch.ones(128, 32), "vecf8").to("cuda")
        vecblock.scaled_mm(x, x)
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _scaled_mm_available(), reason="needs a GPU with block-scaled torch._scaled_mm")
def test_scaled_mm_matches_decoded_product():
    gen = torch.Generator().manual_seed(3)
    rows, queries = torch.randn(256, 128, generator=gen), torch.randn(16, 128, generator=gen)
    for fmt in ("vecf8", "vecf4"):
        r, q = vecblock.quantize(rows, fmt), vecblock.quantize(queries, fmt)
        got = vecblock.scaled_mm(r.to("cuda"), q.to("cuda")).cpu().double()
        want = r.to_float().double() @ q.to_float().double().T
        assert ((got - want).abs().max() / want.abs().max()).item() < 1e-5, fmt


@pytest.mark.skipif(not _scaled_mm_available(), reason="needs a GPU with block-scaled torch._scaled_mm")
def test_scaled_mm_applies_global_scales_in_double():
    gen = torch.Generator().manual_seed(5)
    r = vecblock.quantize(torch.randn(128, 64, generator=gen), "vecf4")
    q = vecblock.quantize(torch.randn(128, 64, generator=gen), "vecf4")
    ones = torch.ones(128, dtype=torch.float32)
    unit_r = vecblock.VecBlockBatch(r.fmt, r.dim, r.elements, r.scales, ones)
    unit_q = vecblock.VecBlockBatch(q.fmt, q.dim, q.elements, q.scales, ones)
    acc = vecblock.scaled_mm(unit_r.to("cuda"), unit_q.to("cuda")).cpu()
    want = (acc.double() * r.global_scale.double()[:, None] * q.global_scale.double()[None, :]).float()
    in_float32 = acc * (r.global_scale[:, None] * q.global_scale[None, :])
    assert not torch.equal(in_float32, want)
    got = vecblock.scaled_mm(r.to("cuda"), q.to("cuda")).cpu()
    assert torch.equal(got, want)
