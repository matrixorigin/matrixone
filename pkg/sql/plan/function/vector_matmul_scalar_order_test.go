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

package function

import (
	"encoding/json"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestVectorMatmulCPUMatchesScalarOrder runs CPU vector_matmul with topk 1 and with every
// row, and requires the rows, in order, and the distance text of ORDER BY <scalar
// function>(v, q), id-text LIMIT topk: inner_product, cosine_distance and l2_distance_sq
// evaluated by the SQL functions on the same cells. Rows 0 and 1 differ only in a
// coordinate of 1e-4 alone in the last vecf8/vecf4 block, where the query is 0: their
// squared L2 distances differ in float64 and are equal in float32.
func TestVectorMatmulCPUMatchesScalarOrder(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	mp := proc.Mp()
	const dim, nrows = 33, 48
	query := make([]float32, dim)
	for d := range query {
		query[d] = float32(d % 3)
	}
	query[dim-1] = 0
	for _, oid := range []types.T{
		types.T_array_float32, types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8, types.T_array_float8, types.T_array_float4,
	} {
		integer := oid == types.T_array_int8 || oid == types.T_array_uint8
		r := rand.New(rand.NewSource(29554))
		rows := make([][]float32, nrows)
		for i := range rows {
			rows[i] = make([]float32, dim)
			for d := range rows[i] {
				if integer {
					rows[i][d] = float32(r.Intn(5))
				} else {
					rows[i][d] = float32(r.NormFloat64())
				}
			}
		}
		if !integer {
			for d := range rows[0] {
				rows[0][d], rows[1][d] = query[d], query[d]
			}
			rows[0][0], rows[1][0] = query[0]+1, query[0]+1
			rows[0][dim-1] = 1e-4
		}
		typ := types.New(oid, dim, 0)
		cast := func(t *testing.T, vs [][]float32) *vector.Vector {
			from := vector.NewVec(types.New(types.T_array_float32, dim, 0))
			t.Cleanup(func() { from.Free(mp) })
			for _, v := range vs {
				require.NoError(t, vector.AppendArray(from, v, false, mp))
			}
			if oid == types.T_array_float32 {
				return from
			}
			target := vector.NewConstNull(typ, 1, mp)
			t.Cleanup(func() { target.Free(mp) })
			result := vector.NewFunctionResultWrapper(typ, mp)
			t.Cleanup(result.Free)
			require.NoError(t, result.PreExtendAndReset(len(vs)))
			require.NoError(t, NewCast([]*vector.Vector{from, target}, result, proc, len(vs), nil))
			return result.GetResultVector()
		}
		for _, mc := range []struct{ metric, fn string }{
			{"inner_product", "inner_product"}, {"cosine", "cosine_distance"}, {"l2sq", "l2_distance_sq"},
		} {
			t.Run(oid.String()+"/"+mc.metric, func(t *testing.T) {
				vecs := cast(t, rows)
				qcell := cast(t, [][]float32{query}).GetBytesAt(0)

				// the scalar function with a constant query, as ORDER BY evaluates it
				qconst, err := vector.NewConstBytes(typ, qcell, nrows, mp)
				require.NoError(t, err)
				t.Cleanup(func() { qconst.Free(mp) })
				fn, err := GetFunctionByName(proc.Ctx, mc.fn, []types.Type{typ, typ})
				require.NoError(t, err)
				scalar, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{vecs, qconst}, nrows)
				require.NoError(t, err)
				t.Cleanup(func() { scalar.Free(mp) })
				dist := vector.MustFixedColNoTypeCheck[float64](scalar)
				order := make([]int, nrows)
				for i := range order {
					order[i] = i
				}
				sort.Slice(order, func(a, b int) bool {
					if dist[order[a]] != dist[order[b]] {
						return dist[order[a]] < dist[order[b]]
					}
					return strconv.Itoa(order[a]) < strconv.Itoa(order[b])
				})
				if (oid == types.T_array_float8 || oid == types.T_array_float4) && mc.metric == "l2sq" {
					require.Greater(t, dist[0], dist[1], "l2_distance_sq separates rows 0 and 1")
					require.Equal(t, float32(dist[0]), float32(dist[1]), "rows 0 and 1 are equal in float32")
					require.Equal(t, 1, order[0], "row 1 is nearest")
				}

				// CPU vector_matmul over the same cells; the query as a float32 BLOB, exact in
				// every plain type, or as the vecf8/vecf4 cell
				ids := vector.NewVec(types.T_int64.ToType())
				t.Cleanup(func() { ids.Free(mp) })
				groups := make([]uint64, nrows)
				for i := range groups {
					require.NoError(t, vector.AppendFixed(ids, int64(i), false, mp))
					groups[i] = 1
				}
				for _, topk := range []int64{1, nrows} {
					raw := aggexec.EncodeVectorMatmulBinaryConfig(topk, types.ArrayToBytes(query), `{"metric":"`+mc.metric+`"}`, false)
					if oid == types.T_array_float8 || oid == types.T_array_float4 {
						raw = aggexec.EncodeVectorMatmulBinaryConfig(topk, qcell, `{"metric":"`+mc.metric+`","query_format":"vecblock"}`, false)
					}
					ag, err := aggexec.MakeGroupAgg(mp, aggexec.AggIdOfVectorMatmul, false, nil, raw, types.T_int64.ToType(), typ)
					require.NoError(t, err)
					t.Cleanup(ag.Free)
					require.NoError(t, ag.GroupGrow(1))
					require.NoError(t, ag.BatchFill(0, groups, []*vector.Vector{ids, vecs}))
					out, err := ag.Flush()
					require.NoError(t, err)
					t.Cleanup(func() {
						for _, v := range out {
							v.Free(mp)
						}
					})
					var hits [][][]json.RawMessage
					require.NoError(t, json.Unmarshal([]byte(types.DecodeJson(out[0].GetBytesAt(0)).String()), &hits))
					require.Len(t, hits, 1)
					require.Len(t, hits[0], int(topk))
					for rank, i := range order[:topk] {
						var id string
						require.NoError(t, json.Unmarshal(hits[0][rank][0], &id))
						require.Equal(t, strconv.Itoa(i), id, "topk %d rank %d", topk, rank)
						want := strconv.FormatFloat(dist[i], 'g', -1, 64)
						if float64(float32(dist[i])) == dist[i] {
							want = strconv.FormatFloat(dist[i], 'g', -1, 32)
						}
						if dist[i] == 0 {
							want = "0"
						}
						require.Equal(t, want, string(hits[0][rank][1]), "topk %d rank %d id %d", topk, rank, i)
					}
				}
			})
		}
	}
}
