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

package aggexec

import (
	"fmt"
	"math"
	"runtime"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// A malformed or over-limit vecblock JSON text is rejected before its elements are
// collected: the decode makes as many allocations for 100 objects before the error as for
// 10,000. Collecting would regrow a slice at least once per doubling (7 or more extra
// allocations). A decode's count is the least over several runs: a sync.Pool refill
// (json.Valid's scanner, fmt's printer) only adds allocations, and under -race the race
// runtime drops pool puts on purpose, so a run's count varies by several.
// TestVectorMatmulVecblockJSONRejectedBeforeCollecting covers the errors.
func TestVectorMatmulVecblockJSONAllocationsIndependentOfLength(t *testing.T) {
	const tolerance = 1
	typ := types.New(types.T_array_float8, 4, 0)
	allocs := func(text string) float64 {
		least := math.Inf(1)
		for range 20 {
			least = min(least, testing.AllocsPerRun(1, func() { _, _ = decodeVectorMatmulCells(text, false, typ) }))
		}
		return least
	}
	require.InDelta(t, allocs(vmVecblockMalformed(100)), allocs(vmVecblockMalformed(10000)), tolerance)
	overLimit := "[" + strings.Repeat("{},", vectorMatmulMaxQueries) + "{}]"
	require.InDelta(t, allocs(overLimit), allocs("["+strings.Repeat("{},", 20000)+"{}]"), tolerance)
}

// The executor reserves the bound before parsing and commits the configuration's bytes
// after it, so the bound must cover what a parse holds at once: the retained configuration
// and the score scratch the commit charges, one sonic decoder parser for JSON queries, and
// the transient decoding. Checked for every query form and column type.
func TestVectorMatmulParseBoundCoversParse(t *testing.T) {
	// {1, 32}: a small configuration, where one decoder parser dwarfs the rest
	for _, shape := range [][2]int{{1, 32}, {32, 256}, {2, 4096}, {500, 3}} {
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
			msg := fmt.Sprintf("%s %s nq=%d dim=%d", oid, name, nq, dim)
			bound, err := vectorMatmulParseBound(raw, typ)
			require.NoError(t, err, msg)

			cfg, parseErr := parseVectorMatmulConfig(raw, typ)
			var committed uint64
			if name == "json-null" {
				require.Error(t, parseErr, msg)
			} else {
				require.NoError(t, parseErr, msg)
				// what vectorMatmulExec.configBytes commits after the parse
				committed = uint64(cfg.retained) + uint64(cfg.nq)*8
			}
			held := committed
			if !strings.Contains(name, "binary") {
				held += vectorMatmulJSONDecoderScratch
			}
			require.LessOrEqual(t, held, bound, msg)

			// The transient decoding: a parse after a garbage collection, as production
			// parses each configuration once, allocates at most the bound in total. Vecblock
			// JSON decodes each text through sonic's pooled parser; with several texts every
			// pool refill allocates another parser while only one is ever live, and the race
			// runtime drops pool puts on purpose, so its total is not what it holds at once.
			// A single text decodes once and allocates at most one parser.
			if name == "vecblock-json" && nq > 1 {
				continue
			}
			// other goroutines can only add to the process's allocation; take the least of
			// several runs. Each is cold: sync.Pool keeps a victim cache through one garbage
			// collection, so collect once more than vmGoAlloc does to empty sonic's pools.
			alloc := uint64(math.MaxUint64)
			for range 5 {
				runtime.GC()
				alloc = min(alloc, vmGoAlloc(func() { _, _ = parseVectorMatmulConfig(raw, typ) }))
			}
			require.LessOrEqual(t, alloc, bound, msg)
		}
	}
}
