// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// P2: independent (global x blockScale x element) decode matrix on HAND-BUILT cells.
//
// A block-scaled cell is a persistent format: ParseBlockScaledCell accepts any valid bytes, so the
// decode must be correct for triples the current encoder never emits (it couples the three factors:
// global = amax/(6*448), and it chooses the block scale and element codes). The encoder-driven sweep
// (P1) therefore cannot reach an arbitrary (global, blockScale, element). Here the cell bytes are
// built directly so each factor is swept independently, and every valid cell is checked against the
// GPU oracle (gpu* in distance_func_vecblock_oracle_test.go). This is the persisted-data-compat
// coverage the #29554 review kept in scope.

// buildNVFP4Cell assembles a raw vecf4 cell: version 1, format, dim, global, nScales copies of
// scaleCode, and dim element nibbles (packed 2 per byte, padding nibble zero). Layout per
// ParseBlockScaledCell; version 1 is the format spec.
func buildNVFP4Cell(dim int, global float32, scaleCode, elemCode byte) []byte {
	cell := make([]byte, types.BlockScaledCellSize(types.BlockScaledNVFP4, dim))
	cell[0] = 1
	cell[1] = byte(types.BlockScaledNVFP4)
	binary.LittleEndian.PutUint32(cell[4:8], uint32(dim))
	binary.LittleEndian.PutUint32(cell[8:12], math.Float32bits(global))
	nScales := types.BlockScaledScaleCount(types.BlockScaledNVFP4, dim)
	for b := 0; b < nScales; b++ {
		cell[types.BlockScaledHeaderSize+b] = scaleCode
	}
	elems := cell[types.BlockScaledHeaderSize+nScales:]
	for i := 0; i < dim; i++ {
		if i%2 == 0 {
			elems[i/2] |= elemCode & 0x0f
		} else {
			elems[i/2] |= (elemCode & 0x0f) << 4
		}
	}
	return cell
}

// TestVecBlockCellBuilderMatchesWitness29554 anchors the hand builder to the exact bytes the review
// reported, so the matrix below is built the same way the format is persisted.
func TestVecBlockCellBuilderMatchesWitness29554(t *testing.T) {
	// global = smallest positive float32, scale code 0x23 (0.171875), element code 0x07 (value 6).
	cell := buildNVFP4Cell(1, math.SmallestNonzeroFloat32, 0x23, 0x07)
	require.Equal(t, "0102000001000000010000002307", hex.EncodeToString(cell))
	c, err := types.ParseBlockScaledCell(cell)
	require.NoError(t, err)
	op := &VecBlockOperand{Cell: c}
	d, err := VecBlockCosineDistance(op, op)
	require.NoError(t, err)
	require.Equal(t, 0.0, d, "the review witness is a nonzero vector: self cosine distance 0")
}

// TestVecBlockCellMatrix29554 sweeps the three factors independently over hand-built cells and
// checks every valid cell against the GPU oracle: decode finite, At == Dequantize, and the CPU self
// cosine zero/nonzero classification matches the oracle. A cell the parser rejects (e.g. a block
// that decodes to +Inf) is a separate validated contract and skipped.
func TestVecBlockCellMatrix29554(t *testing.T) {
	// Boundary global magnitudes, including the subnormal region where a float32 global*blockScale
	// underflows (the #29554 class).
	globals := []float32{
		math.SmallestNonzeroFloat32, // 2^-149
		0x1p-140,
		0x1p-126, // smallest normal
		0x1p-120,
		1,
		0x1p60,
		0x1p120,
		math.MaxFloat32,
	}
	// Boundary NVFP4 block-scale codes (E4M3 value table): smallest nonzero, 0.171875, 1.0, and the
	// E4M3 max 448. A signed or NaN scale code is invalid and excluded.
	f8, _, _ := types.BlockScaledTables()
	scaleCodes := []byte{0x01, 0x23}
	for code := 0; code <= 0x7e; code++ { // find the codes for 1.0 and 448
		switch f8[code] {
		case 1:
			scaleCodes = append(scaleCodes, byte(code))
		case 448:
			scaleCodes = append(scaleCodes, byte(code))
		}
	}
	// All 8 unsigned E2M1 element codes (0..6 magnitudes); code 0 is the zero element.
	_, _, f4 := types.BlockScaledTables()
	elemCodes := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}

	for _, dim := range []int{1, 16, 17} {
		for _, g := range globals {
			for _, sc := range scaleCodes {
				for _, ec := range elemCodes {
					cell := buildNVFP4Cell(dim, g, sc, ec)
					c, err := types.ParseBlockScaledCell(cell)
					if err != nil {
						continue // validly rejected (e.g. decodes to +Inf)
					}
					// The correctly-rounded float32 decode of this (element, blockScale, global) triple.
					// The decode contract is zero-ness: At must be nonzero iff the fully-scaled value
					// is representable as a nonzero float32. A value below float32 range (e.g. 2^-159)
					// correctly rounds to 0 -- that is NOT the #29554 bug, which was a *representable*
					// subnormal (>= 2^-149) wrongly decoding to zero.
					trueF32 := float32(float64(f4[ec][0]) * float64(f8[sc]) * float64(g))

					deq := make([]float32, dim)
					c.Dequantize(deq)
					for i := 0; i < dim; i++ {
						at := c.At(i)
						require.Falsef(t, math.IsInf(float64(at), 0) || math.IsNaN(float64(at)),
							"dim=%d g=%v sc=%#x ec=%#x At(%d)=%v not finite", dim, g, sc, ec, i, at)
						require.Equalf(t, at, deq[i], "dim=%d g=%v sc=%#x ec=%#x: At != Dequantize at %d", dim, g, sc, ec, i)
						require.Equalf(t, trueF32 != 0, at != 0,
							"dim=%d g=%v sc=%#x ec=%#x: At(%d)=%v zero-ness must match the correctly-rounded float32 decode %v",
							dim, g, sc, ec, i, at, trueF32)
					}

					op := &VecBlockOperand{Cell: c}
					d, err := VecBlockCosineDistance(op, op)
					require.NoError(t, err)
					// CPU self cosine is 1 iff the vector decodes to all-zero float32 (trueF32 == 0,
					// since every element shares the code here).
					require.Equalf(t, trueF32 == 0, d == 1,
						"dim=%d g=%v sc=%#x ec=%#x: CPU self cosine distance=%v, expected zero=%v", dim, g, sc, ec, d, trueF32 == 0)
					// CPU and the GPU oracle agree whenever the value is float32-representable. They may
					// differ ONLY below float32 range: the CPU rounds to 0, the GPU (global in double on
					// the norm) can stay nonzero. That is a representability boundary, not reachable from
					// encoded float32 data; assert the divergence happens only there.
					if (d == 1) != gpuZeroVector(&c) {
						require.Zerof(t, trueF32,
							"dim=%d g=%v sc=%#x ec=%#x: CPU/GPU zero classification may differ only below float32 range, but value %v is representable",
							dim, g, sc, ec, trueF32)
					}
				}
			}
		}
	}
}
