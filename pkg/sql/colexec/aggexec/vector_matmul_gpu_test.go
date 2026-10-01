//go:build gpu

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
	"bytes"
	"encoding/json"
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/stretchr/testify/require"
)

type vmGPUHit struct {
	id    string
	score float64
}

func vmGPUParse(t *testing.T, out string) [][]vmGPUHit {
	var raw [][][]any
	require.NoError(t, json.Unmarshal([]byte(out), &raw))
	res := make([][]vmGPUHit, len(raw))
	for q, hits := range raw {
		for _, h := range hits {
			res[q] = append(res[q], vmGPUHit{h[0].(string), h[1].(float64)})
		}
	}
	return res
}

// vmGPURun fills an executor with the rows split over groups, merging a second executor
// that still holds rows in its tile, and returns the flushed JSON per group.
func vmGPURun(t *testing.T, mp *mpool.MPool, format types.BlockScaledFormat, dim int, gpu bool,
	queries [][]float32, topk, groups int, ids []int64, rows [][]float32) []string {
	oid := types.T_array_float8
	if format == types.BlockScaledNVFP4 {
		oid = types.T_array_float4
	}
	vt := types.New(oid, int32(dim), 0)
	q, _ := json.Marshal(queries)
	cfg := EncodeVectorMatmulConfig(int64(topk), string(q), "", gpu)
	mk := func() *vectorMatmulExec {
		exec, err := makeVectorMatmul(mp, AggIdOfVectorMatmul, false, []types.Type{types.T_int64.ToType(), vt})
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(groups))
		require.NoError(t, exec.SetExtraInformation(cfg, 0))
		return exec.(*vectorMatmulExec)
	}
	build := func(lo, hi int) []*vector.Vector {
		idv := vector.NewVec(types.T_int64.ToType())
		vv := vector.NewVec(vt)
		for i := lo; i < hi; i++ {
			require.NoError(t, vector.AppendFixed(idv, ids[i], false, mp))
			cell, err := types.AppendBlockScaled(nil, format, rows[i])
			require.NoError(t, err)
			require.NoError(t, vector.AppendBytes(vv, cell, false, mp))
		}
		return []*vector.Vector{idv, vv}
	}
	fill := func(exec *vectorMatmulExec, lo, hi int) {
		const batch = 1000
		for b := lo; b < hi; b += batch {
			e := min(hi, b+batch)
			vecs := build(b, e)
			grp := make([]uint64, e-b)
			for i := range grp {
				grp[i] = uint64((b+i)%groups) + 1
			}
			require.NoError(t, exec.BatchFill(0, grp, vecs))
			vmFree(mp, vecs)
		}
	}
	half := len(rows) / 2
	a, b := mk(), mk()
	defer a.Free()
	defer b.Free()
	fill(a, 0, half)
	fill(b, half, len(rows))
	if gpu {
		require.NotNil(t, a.engine)
		require.NotEmpty(t, b.tile.groups, "rows wait in the tile until a read drains it")
	}
	for g := 0; g < groups; g++ {
		require.NoError(t, a.Merge(b, g, g))
	}

	// round trip through the intermediate result before the final flush
	var buf bytes.Buffer
	flags := make([]uint8, groups)
	for i := range flags {
		flags[i] = 1
	}
	require.NoError(t, a.SaveIntermediateResult(int64(groups), [][]uint8{flags}, &buf))
	c := mk()
	defer c.Free()
	require.NoError(t, c.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))
	return vmFlush(t, mp, c)
}

func TestVectorMatmulGPUMatchesCPU(t *testing.T) {
	if n, err := cuvs.GetGpuDeviceCount(); err != nil || n == 0 {
		t.Skip("no GPU")
	}
	// small tiles: several drains while filling
	saved := vectorMatmulTileBytes
	vectorMatmulTileBytes = 16 << 10
	defer func() { vectorMatmulTileBytes = saved }()
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	r := rand.New(rand.NewSource(20567))
	for _, format := range []types.BlockScaledFormat{types.BlockScaledMXFP8, types.BlockScaledNVFP4} {
		for _, dim := range []int{4, 100} {
			const nrows, topk, groups = 5000, 7, 3
			ids := make([]int64, nrows)
			rows := make([][]float32, nrows)
			for i := range rows {
				ids[i] = int64(i)
				rows[i] = make([]float32, dim)
				for k := range rows[i] {
					rows[i][k] = float32(r.NormFloat64())
				}
			}
			queries := make([][]float32, 3)
			for j := range queries {
				queries[j] = make([]float32, dim)
				for k := range queries[j] {
					queries[j][k] = float32(r.NormFloat64())
				}
			}
			cpu := vmGPURun(t, mp, format, dim, false, queries, topk, groups, ids, rows)
			gpu := vmGPURun(t, mp, format, dim, true, queries, topk, groups, ids, rows)
			require.Len(t, gpu, groups)
			for g := range cpu {
				want, got := vmGPUParse(t, cpu[g]), vmGPUParse(t, gpu[g])
				require.Len(t, got, len(want))
				for q := range want {
					require.Len(t, got[q], len(want[q]))
					for i := range want[q] {
						require.InDelta(t, want[q][i].score, got[q][i].score, 1e-4*math.Max(1, math.Abs(want[q][i].score)),
							"%s dim %d group %d query %d rank %d", format, dim, g, q, i)
					}
				}
				// summation order may swap near-equal scores; the id sets of each query agree
				for q := range want {
					ws, gs := map[string]bool{}, map[string]bool{}
					for i := range want[q] {
						ws[want[q][i].id] = true
						gs[got[q][i].id] = true
					}
					require.Equal(t, ws, gs, "%s dim %d group %d query %d", format, dim, g, q)
				}
			}
		}
	}
}
