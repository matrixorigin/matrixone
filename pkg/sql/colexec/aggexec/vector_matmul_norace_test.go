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

//go:build !race

package aggexec

import (
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// A malformed or over-limit vecblock JSON text is rejected before its elements are
// collected: the decode makes as many allocations for 100 objects before the error as for
// 10,000. TestVectorMatmulVecblockJSONRejectedBeforeCollecting covers the errors.
func TestVectorMatmulVecblockJSONAllocationsIndependentOfLength(t *testing.T) {
	typ := types.New(types.T_array_float8, 4, 0)
	allocs := func(text string) float64 {
		return testing.AllocsPerRun(3, func() { _, _ = decodeVectorMatmulCells(text, false, typ) })
	}
	require.Equal(t, allocs(vmVecblockMalformed(100)), allocs(vmVecblockMalformed(10000)))
	overLimit := "[" + strings.Repeat("{},", vectorMatmulMaxQueries) + "{}]"
	require.Equal(t, allocs(overLimit), allocs("["+strings.Repeat("{},", 20000)+"{}]"))
}

// The bound covers what parsing allocates, for every query form and column type.
func TestVectorMatmulParseBoundCoversParse(t *testing.T) {
	for _, shape := range [][2]int{{32, 256}, {2, 4096}, {500, 3}} {
		vmParseBoundCoversParse(t, shape[0], shape[1])
	}
}

func vmParseBoundCoversParse(t *testing.T, nq, dim int) {
	values := make([]float32, nq*dim)
	for i := range values {
		values[i] = float32(i%7) - 3
	}
	for _, oid := range []types.T{types.T_array_float32, types.T_array_float16, types.T_array_bf16, types.T_array_int8, types.T_array_float8, types.T_array_float4} {
		typ := types.New(oid, int32(dim), 0)
		configs := map[string][]byte{
			"json":      EncodeVectorMatmulConfig(4, vmJSONQueries(nq, dim, false), "", false),
			"json-null": EncodeVectorMatmulConfig(4, vmJSONQueries(nq, dim, true), "", false),
			"binary":    EncodeVectorMatmulBinaryConfig(4, types.ArrayToBytes(values), `{"metric":"cosine"}`, false),
		}
		if f, ok := oid.BlockScaledFormat(); ok {
			var cells []byte
			texts := make([]string, nq)
			for q := 0; q < nq; q++ {
				cell, err := types.AppendBlockScaled(nil, f, values[q*dim:(q+1)*dim])
				require.NoError(t, err)
				cells = append(cells, cell...)
				texts[q], err = types.BlockScaledToJSON(cell)
				require.NoError(t, err)
			}
			opts := `{"query_format":"vecblock"}`
			configs["vecblock-binary"] = EncodeVectorMatmulBinaryConfig(4, cells, opts, false)
			configs["vecblock-json"] = EncodeVectorMatmulConfig(4, "["+strings.Join(texts, ",")+"]", opts, false)
		}
		for name, raw := range configs {
			bound, err := vectorMatmulParseBound(raw, typ)
			require.NoError(t, err, "%s %s nq=%d dim=%d", oid, name, nq, dim)
			// other goroutines can only add to the process's allocation; take the least of
			// several runs
			var parseErr error
			alloc := uint64(math.MaxUint64)
			for range 5 {
				alloc = min(alloc, vmGoAlloc(func() { _, parseErr = parseVectorMatmulConfig(raw, typ) }))
			}
			if name == "json-null" {
				require.Error(t, parseErr)
			} else {
				require.NoError(t, parseErr, "%s %s", oid, name)
			}
			require.LessOrEqual(t, alloc, bound, "%s %s nq=%d dim=%d", oid, name, nq, dim)
		}
	}
}
