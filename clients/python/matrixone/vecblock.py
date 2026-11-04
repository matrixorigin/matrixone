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
Exchange MatrixOne ``vecf8`` / ``vecf4`` cells with PyTorch tensors.

A MatrixOne block-scaled vector cell (``vecblock_binary(v)``) is laid out as::

    offset 0    version (1 byte) = 1
    offset 1    format  (1 byte) = 1 for vecf8 (MXFP8), 2 for vecf4 (NVFP4)
    offset 2    reserved (2 bytes, zero)
    offset 4    dim     (uint32, little-endian)
    offset 8    global scale (float32, little-endian; always 1.0 for vecf8)
    offset 12   block scales: ceil(dim/32) E8M0 bytes (vecf8) or ceil(dim/16) UE4M3 bytes (vecf4)
    then        elements: dim E4M3 bytes (vecf8) or ceil(dim/2) bytes of two E2M1 codes,
                the first in the low nibble (vecf4)

Element ``i`` decodes as ``element[i] * (global * scale[i // block])`` in float32. The element and scale
bytes reinterpret directly as ``torch.float8_e4m3fn`` / ``torch.float4_e2m1fn_x2`` and
``torch.float8_e8m0fnu`` / ``torch.float8_e4m3fn``, so cells move between MatrixOne and
PyTorch without loss, and ``torch._scaled_mm`` multiplies them on Blackwell GPUs.

PyTorch is imported on first use; the module imports without it.
"""

import math
import struct
from dataclasses import dataclass
from typing import List, Sequence

HEADER_SIZE = 12
CELL_VERSION = 1
FORMAT_CODES = {"vecf8": 1, "vecf4": 2}
BLOCK_SIZES = {"vecf8": 32, "vecf4": 16}
E4M3_MAX = 448.0
E2M1_MAX = 6.0
# 2^-126; a block scale below it is applied in float64 when decoding
SMALLEST_NORMAL_FLOAT32 = math.ldexp(1.0, -126)
# E2M1 code -> value, codes 0-15 (bit 3 is the sign)
E2M1_VALUES = (0.0, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0, -0.0, -0.5, -1.0, -1.5, -2.0, -3.0, -4.0, -6.0)


def _torch():
    try:
        import torch
    except ImportError as e:  # pragma: no cover - exercised only without torch
        raise ImportError(
            "matrixone.vecblock tensor functions need PyTorch (pip install 'matrixone-python-sdk[torch]')"
        ) from e
    return torch


def scale_count(fmt: str, dim: int) -> int:
    """Number of block scales in a cell of format ``fmt`` and dimension ``dim``."""
    return -(-dim // BLOCK_SIZES[fmt])


def element_bytes(fmt: str, dim: int) -> int:
    """Number of element bytes in a cell of format ``fmt`` and dimension ``dim``."""
    return dim if fmt == "vecf8" else -(-dim // 2)


def cell_size(fmt: str, dim: int) -> int:
    """Byte length of a cell of format ``fmt`` and dimension ``dim``."""
    return HEADER_SIZE + scale_count(fmt, dim) + element_bytes(fmt, dim)


def cell_format(cell: bytes) -> str:
    """The format of a cell, ``"vecf8"`` or ``"vecf4"``, from its header."""
    if len(cell) < HEADER_SIZE or cell[0] != CELL_VERSION:
        raise ValueError("not a vecf8/vecf4 cell")
    for name, code in FORMAT_CODES.items():
        if cell[1] == code:
            return name
    raise ValueError(f"unknown block-scaled format {cell[1]}")


def blob_literal(data: bytes) -> str:
    """A SQL hex literal ``x'..'`` for ``data``, e.g. for ``SET @q = x'..'``.

    It is a binary string: pass it as ``CAST(@q AS BLOB)`` where a BLOB is expected. Keep
    large values in a user variable set once: a literal in a statement that runs longer than
    the server's long-query time is recorded with the statement's plan.
    """
    return "x'" + bytes(data).hex() + "'"


def cell_sql(cell: bytes) -> str:
    """A SQL expression for one cell as a value of its column type, e.g. in INSERT ... VALUES.

    ``x'..'`` alone is a binary string, which casts to a vector as text; the BLOB cast makes
    MatrixOne take the bytes as a stored cell.
    """
    fmt = cell_format(cell)
    dim = struct.unpack_from("<I", cell, 4)[0]
    return f"CAST(CAST({blob_literal(cell)} AS BLOB) AS {fmt}({dim}))"


@dataclass
class VecBlockBatch:
    """A batch of cells of one format and dimension, as uint8 tensors.

    ``elements`` is ``[n, element_bytes]``, ``scales`` is ``[n, scale_count]`` and
    ``global_scale`` is ``[n]`` float32.
    """

    fmt: str
    dim: int
    elements: "object"
    scales: "object"
    global_scale: "object"

    def __len__(self) -> int:
        return int(self.elements.shape[0])

    def to(self, device) -> "VecBlockBatch":
        return VecBlockBatch(
            self.fmt, self.dim, self.elements.to(device), self.scales.to(device), self.global_scale.to(device)
        )

    def element_tensor(self):
        """The elements as ``float8_e4m3fn`` (vecf8) or ``float4_e2m1fn_x2`` (vecf4)."""
        torch = _torch()
        return self.elements.view(torch.float8_e4m3fn if self.fmt == "vecf8" else torch.float4_e2m1fn_x2)

    def scale_tensor(self):
        """The block scales as ``float8_e8m0fnu`` (vecf8) or ``float8_e4m3fn`` (vecf4)."""
        torch = _torch()
        return self.scales.view(torch.float8_e8m0fnu if self.fmt == "vecf8" else torch.float8_e4m3fn)

    def to_float(self, dtype=None):
        """The decoded values ``[n, dim]``, by default float32."""
        torch = _torch()
        dtype = dtype or torch.float32
        if self.fmt == "vecf8":
            vals = self.element_tensor().to(torch.float32)
        else:
            lut = torch.tensor(E2M1_VALUES, dtype=torch.float32, device=self.elements.device)
            lo = lut[(self.elements & 0xF).long()]
            hi = lut[(self.elements >> 4).long()]
            vals = torch.stack([lo, hi], dim=2).reshape(len(self), -1)[:, : self.dim]
        # MatrixOne's order: element * (global * blockScale), each product rounded to float32. Where
        # global * blockScale is below the smallest normal float32, the product is element *
        # blockScale * global in float64, rounded once to float32.
        block = BLOCK_SIZES[self.fmt]
        block_scale = self.scale_tensor().to(torch.float32).repeat_interleave(block, dim=1)[:, : self.dim]
        scale = block_scale * self.global_scale[:, None]
        out = vals * scale
        small = scale < SMALLEST_NORMAL_FLOAT32
        if small.any():
            wide = (vals.double() * block_scale.double() * self.global_scale.double()[:, None]).float()
            out = torch.where(small, wide, out)
        return out.to(dtype)

    def blocked_scales(self):
        """The scales in the 128x4 tiled layout ``torch._scaled_mm`` (cuBLASLt) expects."""
        return to_blocked(self.scale_tensor())

    def to_cells(self) -> List[bytes]:
        """The cells, one ``bytes`` per row, as MatrixOne stores them."""
        header = bytes([CELL_VERSION, FORMAT_CODES[self.fmt], 0, 0]) + struct.pack("<I", self.dim)
        el = self.elements.cpu().numpy()
        sc = self.scales.cpu().numpy()
        gl = self.global_scale.cpu().numpy()
        return [header + struct.pack("<f", float(gl[i])) + sc[i].tobytes() + el[i].tobytes() for i in range(len(self))]


def from_cells(cells: Sequence[bytes], device=None) -> VecBlockBatch:
    """Split cells of one format and dimension (e.g. ``vecblock_binary(v)`` values)."""
    torch = _torch()
    if len(cells) == 0:
        raise ValueError("no cells")
    fmt = cell_format(cells[0])
    dim = struct.unpack_from("<I", cells[0], 4)[0]
    size, n_scales = cell_size(fmt, dim), scale_count(fmt, dim)
    for c in cells:
        if len(c) != size or c[:8] != cells[0][:8]:
            raise ValueError(f"cells differ in format or dimension, or are not {fmt}({dim}) cells")
    raw = torch.frombuffer(bytearray(b"".join(cells)), dtype=torch.uint8).view(len(cells), size)
    batch = VecBlockBatch(
        fmt,
        dim,
        raw[:, HEADER_SIZE + n_scales :].contiguous(),
        raw[:, HEADER_SIZE : HEADER_SIZE + n_scales].contiguous(),
        raw[:, 8:12].reshape(-1).clone().view(torch.float32),
    )
    return batch.to(device) if device is not None else batch


def to_blocked(scales):
    """``[rows, cols]`` block scales -> the cuBLASLt 128x4 tiled layout, flattened."""
    torch = _torch()
    rows, cols = scales.shape
    r, c = -(-rows // 128), -(-cols // 4)
    padded = torch.zeros((r * 128, c * 4), dtype=scales.dtype, device=scales.device)
    padded[:rows, :cols] = scales
    blocks = padded.view(r, 128, c, 4).permute(0, 2, 1, 3)
    return blocks.reshape(-1, 4, 32, 4).transpose(1, 2).reshape(-1, 32, 16).flatten()


def quantize(x, fmt: str) -> VecBlockBatch:
    """Quantize ``x`` ``[n, dim]`` to cells with MatrixOne's encoder rules.

    vecf8: per 32 elements the smallest power-of-two scale ``s`` with ``448 * s >= amax``,
    elements rounded once to E4M3. vecf4: a per-vector global ``max|x| / 2688``, per 16
    elements the smallest UE4M3 scale ``s`` with ``6 * s * global >= amax``, elements rounded
    once to E2M1 (ties to even). A zero of either sign is code 0. The cells equal the ones
    ``CAST(v AS vecf8(N))`` / ``vecf4(N)`` stores for the same float32 values.
    """
    torch = _torch()
    if fmt not in BLOCK_SIZES:
        raise ValueError(f"unknown format {fmt!r}")
    x = torch.as_tensor(x).to(torch.float32)
    if x.dim() != 2:
        raise ValueError("x must be [n, dim]")
    if not torch.isfinite(x).all():
        raise ValueError("x has a NaN or infinite value")
    n, dim = x.shape
    block = BLOCK_SIZES[fmt]
    nb = scale_count(fmt, dim)
    xd = x.double()
    padded = torch.zeros((n, nb * block), dtype=torch.float64, device=x.device)
    padded[:, :dim] = xd
    blocks = padded.view(n, nb, block)
    amax = blocks.abs().amax(dim=2)  # [n, nb]

    if fmt == "vecf8":
        glob = torch.ones(n, dtype=torch.float32, device=x.device)
        safe = torch.where(amax > 0, amax, torch.ones_like(amax))
        e = torch.ceil(torch.log2(safe / E4M3_MAX))
        e = torch.where(E4M3_MAX * torch.exp2(e - 1) >= safe, e - 1, e)
        e = torch.where(E4M3_MAX * torch.exp2(e) < safe, e + 1, e)
        e = e.clamp(-127, 127)
        codes = torch.where(amax > 0, e + 127, torch.zeros_like(e)).to(torch.uint8)
        div = torch.where(amax > 0, torch.exp2(e), torch.ones_like(e))
        q = (blocks / div[:, :, None]).to(torch.float32).to(torch.float8_e4m3fn).view(torch.uint8)
        q = torch.where((q & 0x7F) == 0, torch.zeros_like(q), q)
        elems = q.reshape(n, -1)[:, :dim].contiguous()
        return _finite(VecBlockBatch(fmt, dim, elems, codes.contiguous(), glob), x)

    vmax = xd.abs().amax(dim=1)
    glob64 = vmax / (E2M1_MAX * E4M3_MAX)
    glob = glob64.to(torch.float32)
    tiny = torch.tensor(math.ldexp(1.0, -149), dtype=torch.float32, device=x.device)
    glob = torch.where((vmax > 0) & (glob == 0), tiny, glob)
    target = amax / (E2M1_MAX * glob.double()[:, None])
    t8 = target.to(torch.float32).to(torch.float8_e4m3fn)
    c = t8.view(torch.uint8).clone()
    c = torch.where(t8.to(torch.float64) < target, c + 1, c)
    c = torch.where(target >= E4M3_MAX, torch.full_like(c, 0x7E), c)
    c = torch.where(target > 0, c, torch.zeros_like(c))
    scale_val = c.view(torch.float8_e4m3fn).to(torch.float64)
    div = glob.double()[:, None] * scale_val
    q = blocks / torch.where(c[:, :, None] > 0, div[:, :, None], torch.ones_like(div[:, :, None]))
    q = torch.where(c[:, :, None] > 0, q, torch.zeros_like(q))
    mags = torch.tensor(E2M1_VALUES[:8], dtype=torch.float64, device=x.device)
    a = q.abs().clamp(max=E2M1_MAX)
    hi = torch.searchsorted(mags, a.contiguous(), right=False).clamp(max=7)
    lo = (hi - 1).clamp(min=0)
    d_lo, d_hi = a - mags[lo], mags[hi] - a
    pick = torch.where(d_lo < d_hi, lo, torch.where(d_hi < d_lo, hi, torch.where(lo % 2 == 0, lo, hi)))
    pick = torch.where(a == mags[hi], hi, pick)
    code = pick.to(torch.uint8) | torch.where(q < 0, 8, 0).to(torch.uint8)
    code = torch.where(pick == 0, torch.zeros_like(code), code)
    code = code.reshape(n, -1)[:, :dim]
    if dim % 2:
        code = torch.cat([code, torch.zeros((n, 1), dtype=torch.uint8, device=x.device)], dim=1)
    elems = (code[:, 0::2] | (code[:, 1::2] << 4)).contiguous()
    return _finite(VecBlockBatch(fmt, dim, elems, c.contiguous(), glob), x)


def _finite(batch: VecBlockBatch, x) -> VecBlockBatch:
    """``batch``, or ValueError if an element decodes to an infinite value, as MatrixOne rejects it."""
    torch = _torch()
    bad = (~torch.isfinite(batch.to_float())).nonzero()
    if len(bad):
        r, i = (int(v) for v in bad[0])
        raise ValueError(
            f"{batch.fmt} row {r} element {i} value {float(x[r, i])!r} is out of range: "
            "it decodes to an infinite value"
        )
    return batch


def scaled_mm(rows: VecBlockBatch, queries: VecBlockBatch):
    """``rows @ queries.T`` ``[len(rows), len(queries)]`` float32 with ``torch._scaled_mm``.

    Needs a CUDA device with block-scaled tensor cores (Blackwell) and both batches on it.
    The global scales are applied after the GEMM as MatrixOne's engine applies them: the
    float32 GEMM output times the row and query global scales in float64, rounded once to
    float32.
    """
    torch = _torch()
    if rows.fmt != queries.fmt or rows.dim != queries.dim:
        raise ValueError("rows and queries differ in format or dimension")
    block = BLOCK_SIZES[rows.fmt]
    if rows.dim % block:
        raise ValueError(f"torch._scaled_mm needs a dimension that is a multiple of {block}")
    out = torch._scaled_mm(
        rows.element_tensor(),
        queries.element_tensor().t(),
        scale_a=rows.blocked_scales(),
        scale_b=queries.blocked_scales(),
        out_dtype=torch.float32,
    )
    out = out.double() * rows.global_scale.double()[:, None] * queries.global_scale.double()[None, :]
    return out.float()
