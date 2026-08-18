// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ivfflat

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type scriptedRelationScanner struct {
	t        *testing.T
	requests []sqlexec.RelationScanRequest
	run      func(sqlexec.RelationScanRequest) executor.Result
}

func (s *scriptedRelationScanner) ScanRelation(req sqlexec.RelationScanRequest) (executor.Result, error) {
	s.requests = append(s.requests, req)
	res := s.run(req)
	if req.BatchTransform != nil {
		for _, bat := range res.Batches {
			if err := req.BatchTransform(bat); err != nil {
				res.Close()
				return executor.Result{}, err
			}
		}
	}
	return res, nil
}

func TestGetVersionUsesTypedRelationScan(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	scanner := &scriptedRelationScanner{t: t}
	scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
		require.Equal(t, "db1", req.Schema)
		require.Equal(t, "meta1", req.Table)
		require.Equal(t, int32(1), req.PartitionCount)
		require.Equal(t, []string{
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_key,
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_val,
		}, req.Columns)
		require.NotNil(t, req.Filter)
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("version"), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("17"), false, mp))
		bat.SetRowCount(1)
		return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RelationScanner = scanner

	version, err := GetVersion(sqlproc, vectorindex.IndexTableConfig{DbName: "db1", MetadataTable: "meta1"})
	require.NoError(t, err)
	require.Equal(t, int64(17), version)
	require.Len(t, scanner.requests, 1)
}

func TestScanEntriesUsesTypedFilterAndPhysicalTop(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	scanner := &scriptedRelationScanner{t: t}
	scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
		require.Equal(t, "entries1", req.Table)
		require.NotNil(t, req.Filter)
		require.NotNil(t, req.IndexParam)
		require.True(t, req.PostFilterTopOnly)
		require.Equal(t, uint64(3), req.IndexParam.GetLimit().GetLit().GetU64Val())
		require.Empty(t, req.FilterHint.MembershipFilterBytes)

		bat := batch.NewWithSize(5) // version, centroid, pk, entry, include
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[3] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
		bat.Vecs[4] = vector.NewVec(types.T_int32.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(4), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(7), false, mp))
		require.NoError(t, vector.AppendArray(bat.Vecs[3], []float32{1, 2}, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], int32(9), false, mp))
		bat.SetRowCount(1)
		return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RelationScanner = scanner
	idx := &IvfflatSearchIndex[float32]{Version: 4, QuantMul: 1}
	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2sqDistance)
	idxcfg.Ivfflat.VectorType = int32(types.T_array_float32)

	res, err := idx.scanEntries(sqlproc, idxcfg, vectorindex.IndexTableConfig{
		DbName:         "db1",
		EntriesTable:   "entries1",
		IncludeColumns: []string{"payload"},
	}, []float32{0, 0}, 4, []int64{2, 3}, []string{"payload"}, nil, 3)
	require.NoError(t, err)
	defer res.Close()
	require.Len(t, res.Batches, 1)
	require.Len(t, res.Batches[0].Vecs, 3)
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](res.Batches[0].Vecs[0], 0))
	require.Equal(t, float64(5), vector.GetFixedAtNoTypeCheck[float64](res.Batches[0].Vecs[1], 0))
	require.Equal(t, int32(9), vector.GetFixedAtNoTypeCheck[int32](res.Batches[0].Vecs[2], 0))
}

func TestRuntimeMembershipLowersToTypedSourcePkPredicate(t *testing.T) {
	mp := mpool.MustNewZero()
	keys := vector.NewVec(types.T_int32.ToType())
	defer keys.Free(mp)
	require.NoError(t, vector.AppendFixedList(keys, []int32{3, 4}, nil, mp))
	data, err := keys.MarshalBinary()
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", mp)

	expr, err := ivfRuntimeMembershipExpr(proc.Ctx, data,
		ivfColExpr(2, plan.Type{Id: int32(types.T_int32)}))

	require.NoError(t, err)
	require.Equal(t, function.InFunctionName, expr.GetF().Func.ObjName)
	require.Equal(t, int32(2), expr.GetF().Args[0].GetCol().ColPos)
	require.Equal(t, int32(2), expr.GetF().Args[1].GetVec().Len)
	require.Equal(t, data, expr.GetF().Args[1].GetVec().Data)
}

func TestPlanReaderSortsAndBoundsCandidates(t *testing.T) {
	r := &planReader{
		spec:         &plan.VectorIndexScan{},
		req:          searchplugin.Request{CandidateLimit: 2},
		keys:         []any{int64(3), int64(1), int64(2)},
		distances:    []float64{3, 1, 2},
		includeData:  map[string][]any{"payload": {int32(30), int32(10), int32(20)}},
		includeNulls: map[string][]bool{"payload": {false, false, false}},
	}
	r.sortAndLimit()
	require.Equal(t, []any{int64(1), int64(2)}, r.keys)
	require.Equal(t, []float64{1, 2}, r.distances)
	require.Equal(t, []any{int32(10), int32(20)}, r.includeData["payload"])
}

func TestAdvancePlanCursorExpandsDisjointCentroidWindows(t *testing.T) {
	cursor := &vectorindex.IvfSearchCursor{
		RankedCentroidIDs:  []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Round:              1,
		NextBucketOffset:   0,
		CurrentBucketCount: 2,
	}
	advancePlanCursor(cursor)
	require.Equal(t, uint(2), cursor.NextBucketOffset)
	require.Equal(t, uint(4), cursor.CurrentBucketCount)
	require.False(t, cursor.Exhausted)

	cursor.Round++
	advancePlanCursor(cursor)
	require.Equal(t, uint(6), cursor.NextBucketOffset)
	require.Equal(t, uint(4), cursor.CurrentBucketCount)

	cursor.Round++
	advancePlanCursor(cursor)
	require.True(t, cursor.Exhausted)
	require.Equal(t, uint(10), cursor.NextBucketOffset)
}

func TestCompactRelationTopBoundsRowsWithClearedEntryVector(t *testing.T) {
	mp := mpool.MustNewZero()
	makeBatch := func(pk int64, dist float64) *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.New(types.T_array_float32, 2, 0)) // consumed entry slot
		bat.Vecs[2] = vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], pk, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], dist, false, mp))
		bat.SetRowCount(1)
		return bat
	}
	res := executor.Result{Mp: mp, Batches: []*batch.Batch{
		makeBatch(1, 3), makeBatch(2, 1), makeBatch(3, 2),
	}}
	require.NoError(t, compactRelationTop(&res, 2, false))
	defer res.Close()
	require.Len(t, res.Batches, 1)
	require.Equal(t, 2, res.Batches[0].RowCount())
	require.Equal(t, []int64{2, 3}, vector.MustFixedColWithTypeCheck[int64](res.Batches[0].Vecs[0]))
	require.Zero(t, res.Batches[0].Vecs[1].Length())
	require.Equal(t, []float64{1, 2}, vector.MustFixedColWithTypeCheck[float64](res.Batches[0].Vecs[2]))
}

func TestDistanceRangeFiltersBeforeTopInSourceUnits(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	for row, rawDistance := range []float64{1, 4, 9, 16} {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(row+1), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], rawDistance, false, mp))
	}
	bat.SetRowCount(4)
	res := executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	defer res.Close()
	idx := &IvfflatSearchIndex[float32]{QuantMul: 1}
	floatLiteral := func(value float64) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Dval{Dval: value},
			}},
		}
	}
	distRange := &plan.DistRange{
		LowerBoundType: plan.BoundType_EXCLUSIVE,
		LowerBound:     floatLiteral(1),
		UpperBoundType: plan.BoundType_INCLUSIVE,
		UpperBound:     floatLiteral(3),
	}

	require.NoError(t, idx.filterEntryDistanceRange(
		&res, distRange, metric.DistFn_L2Distance, metric.Metric_L2sqDistance))
	require.NoError(t, compactRelationTop(&res, 2, false))

	require.Equal(t, []int64{2, 3}, vector.MustFixedColWithTypeCheck[int64](res.Batches[0].Vecs[0]))
	require.Equal(t, []float64{4, 9}, vector.MustFixedColWithTypeCheck[float64](res.Batches[0].Vecs[1]))
	require.False(t, vectorDistanceInRange(math.NaN(), distRange, 1, true, 3, true))
	require.False(t, vectorDistanceInRange(2, distRange, math.NaN(), true, 3, true))
}

func TestFilterRelationBatchAppliesResidualBeforeTop(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	for _, value := range []string{"clustering_start", "version"} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, mp))
	}
	for _, value := range []string{"timestamp", "17"} {
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(value), false, mp))
	}
	bat.SetRowCount(2)
	filter, err := ivfFuncExpr(proc.Ctx, "=",
		ivfColExpr(0, plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}),
		ivfStringExpr("version"))
	require.NoError(t, err)
	executor, err := colexec.NewExpressionExecutor(proc, filter)
	require.NoError(t, err)
	defer executor.Free()
	require.NoError(t, filterRelationBatch(proc, executor, bat))
	defer bat.Clean(mp)
	require.Equal(t, 1, bat.RowCount())
	require.Equal(t, "version", bat.Vecs[0].GetStringAt(0))
	require.Equal(t, "17", bat.Vecs[1].GetStringAt(0))
}
