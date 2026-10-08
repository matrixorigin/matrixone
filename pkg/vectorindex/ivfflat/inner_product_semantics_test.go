// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ivfflat

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestInnerProductEntrySQLAndExactRanking(t *testing.T) {
	require.Equal(t, "-inner_product(v, q)", entryDistanceSQL(metric.Metric_InnerProduct, "v", "q"))
	require.Equal(t, "l2_distance_sq(v, q)", entryDistanceSQL(metric.Metric_L2sqDistance, "v", "q"))
	keys, distances, data, nulls := sortAndLimitExactResults(
		[]any{int64(1), int64(2), int64(3)}, []float64{-32, 14, 32}, []string{"payload"},
		map[string][]any{"payload": {"negative", nil, "positive"}},
		map[string][]bool{"payload": {false, true, false}}, 2, true)
	require.Equal(t, []any{int64(3), int64(2)}, keys)
	require.Equal(t, []float64{32, 14}, distances)
	require.Equal(t, []any{"positive", nil}, data["payload"])
	require.Equal(t, []bool{false, true}, nulls["payload"])
}

func TestInnerProductEntryScanDistanceBoundary(t *testing.T) {
	for _, storage := range []bool{false, true} {
		name := "scalar_with_range"
		if storage {
			name = "storage_topk"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			sqlproc := sqlexec.NewSqlProcess(proc)
			sqlproc.IndexReaderParam = &plan.IndexReaderParam{
				OrigFuncName: metric.DistFn_InnerProduct,
				OrderBy:      []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST}},
			}
			if !storage {
				sqlproc.IndexReaderParam.DistRange = &plan.DistRange{
					LowerBoundType: plan.BoundType_EXCLUSIVE, LowerBound: ivfFloat64Expr(10),
					UpperBoundType: plan.BoundType_UNBOUNDED,
				}
			}
			require.Equal(t, plan.OrderBySpec_ASC|plan.OrderBySpec_NULLS_LAST, ivfOrderFlag(sqlproc.IndexReaderParam))
			scanner := &scriptedRelationScanner{t: t}
			scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
				require.Equal(t, !storage, req.PostFilterTopOnly)
				require.Equal(t, plan.OrderBySpec_ASC|plan.OrderBySpec_NULLS_LAST, req.IndexParam.OrderBy[0].Flag)
				if storage {
					require.Equal(t, "inner_product", req.IndexParam.OrderBy[0].Expr.GetF().Func.ObjName)
				} else {
					require.Nil(t, req.IndexParam.OrderBy[0].Expr)
				}
				size := len(req.Columns)
				if storage {
					size++
				}
				bat := batch.NewWithSize(size)
				for i := 0; i < 3; i++ {
					bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
				}
				bat.Vecs[3] = vector.NewVec(types.New(types.T_array_float32, 3, 0))
				bat.Vecs[4] = vector.NewVec(types.T_varchar.ToType())
				if storage {
					bat.Vecs[5] = vector.NewVec(types.T_float64.ToType())
				}
				res := executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
				// 先完成全部分配并注册失败清理，再填充数据；成功时转移给 scanner。
				owned := true
				defer func() {
					if owned {
						res.Close()
					}
				}()
				for row, dot := range []float64{32, -32, 14} {
					for i := 0; i < 3; i++ {
						require.NoError(t, vector.AppendFixed(bat.Vecs[i], int64(row+1), false, mp))
					}
					values := [][]float32{{4, 5, 6}, {-4, -5, -6}, {1, 2, 3}}[row]
					require.NoError(t, vector.AppendArray(bat.Vecs[3], values, false, mp))
					require.NoError(t, vector.AppendBytes(bat.Vecs[4], []byte("key"), false, mp))
					if storage {
						require.NoError(t, vector.AppendFixed(bat.Vecs[5], -dot, false, mp))
					}
				}
				bat.SetRowCount(3)
				owned = false
				return res
			}
			sqlproc.RelationScanner = scanner
			idx := &IvfflatSearchIndex[float32]{Version: 1, QuantMul: 1}
			cfg := vectorindex.IndexConfig{}
			cfg.Ivfflat.Metric = uint16(metric.Metric_InnerProduct)
			cfg.Ivfflat.VectorType = int32(types.T_array_float32)
			res, err := idx.scanEntries(sqlproc, cfg, vectorindex.IndexTableConfig{
				DbName: "db", EntriesTable: "entries", PKeyType: int32(types.T_int64), OrigFuncName: "inner_product",
			}, []float32{1, 2, 3}, 1, []int64{1}, nil, nil, 2)
			require.NoError(t, err)
			defer res.Close()
			want := []float64{-32, 32, -14}
			if !storage {
				want = []float64{-32, -14}
			}
			require.Equal(t, want, vector.MustFixedColNoTypeCheck[float64](res.Batches[0].Vecs[1]))
			for row, d := range want {
				require.Equal(t, -d, idx.scoreFromQuantized(d, "inner_product", metric.Metric_InnerProduct), "row %d", row)
			}
		})
	}
}
