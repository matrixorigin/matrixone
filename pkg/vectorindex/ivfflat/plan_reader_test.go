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
	"context"
	"errors"
	"math"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
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

func TestRelationScanPolicyAssignsInMemoryRowsOnlyToPartitionZero(t *testing.T) {
	require.True(t, ownsInMemoryPartition(1, 0))
	require.True(t, ownsInMemoryPartition(2, 0))
	require.False(t, ownsInMemoryPartition(2, 1))
	require.Equal(t, engine.DataCollectPolicy(engine.Policy_CollectAllData), relationScanPolicy(1, false))
	require.Equal(t, engine.DataCollectPolicy(engine.Policy_CollectAllData), relationScanPolicy(2, true))
	require.Equal(t, engine.DataCollectPolicy(engine.Policy_CollectCommittedPersistedData), relationScanPolicy(2, false))
	advancePlanCursor(nil)
	advancePlanCursor(&vectorindex.IvfSearchCursor{Exhausted: true})
}

func TestValidateIvfQueryDimensions(t *testing.T) {
	require.NoError(t, validateIvfQueryDimensions(3, 3))
	require.NoError(t, validateIvfQueryDimensions(0, 3))
	err := validateIvfQueryDimensions(128, 9)
	require.Error(t, err)
	require.Contains(t, err.Error(), "vector ops between different dimensions (128, 9) is not permitted")
}

func TestScanEntriesUsesTypedFilterAndPhysicalTop(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	scanner := &scriptedRelationScanner{t: t}
	scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
		require.Equal(t, "entries1", req.Table)
		require.NotNil(t, req.Filter)
		require.NotNil(t, req.IndexParam)
		require.False(t, req.PostFilterTopOnly)
		require.Equal(t, uint64(3), req.IndexParam.GetLimit().GetLit().GetU64Val())
		require.Equal(t, plan.OrderBySpec_ASC, req.IndexParam.OrderBy[0].Flag)
		require.Empty(t, req.FilterHint.MembershipFilterBytes)
		require.Equal(t, []string{
			catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
			catalog.SystemSI_IVFFLAT_IncludeColPrefix + "payload",
			catalog.CPrimaryKeyColName,
		}, req.Columns)

		filterFn := req.Filter.GetF()
		require.NotNil(t, filterFn)
		require.Equal(t, function.PrefixInFunctionName, filterFn.Func.ObjName)
		require.Equal(t, []*plan.Expr{req.Filter}, req.BlockFilters)
		require.Equal(t, catalog.CPrimaryKeyColName, filterFn.Args[0].GetCol().Name)
		require.Equal(t, int32(5), filterFn.Args[0].GetCol().ColPos)
		prefixes := new(vector.Vector)
		require.NoError(t, prefixes.UnmarshalBinary(filterFn.Args[1].GetVec().Data))
		require.Equal(t, 2, prefixes.Length())
		packer := types.NewPacker()
		defer packer.Close()
		for row, centroidID := range []int64{2, 3} {
			packer.Reset()
			packer.EncodeInt64(4)
			packer.EncodeInt64(centroidID)
			require.Equal(t, packer.Bytes(), prefixes.GetBytesAt(row))
		}

		orderFn := req.IndexParam.OrderBy[0].Expr.GetF()
		require.NotNil(t, orderFn)
		require.Equal(t, metric.MetricTypeToDistFuncName[metric.Metric_L2sqDistance], orderFn.Func.ObjName)
		require.Equal(t, int32(3), orderFn.Args[0].GetCol().ColPos)
		require.Equal(t, types.ArrayToBytes([]float32{0, 0}), []byte(orderFn.Args[1].GetLit().GetVecVal()))

		bat := batch.NewWithSize(7) // version, centroid, pk, entry, include, cpkey, distance
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[3] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
		bat.Vecs[4] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[5] = vector.NewVec(types.T_varchar.ToType())
		bat.Vecs[6] = vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(4), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(7), false, mp))
		require.NoError(t, vector.AppendArray(bat.Vecs[3], []float32{1, 2}, false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[4], int32(9), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte("cpkey"), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[6], float64(5), false, mp))
		bat.SetRowCount(1)
		return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RelationScanner = scanner
	sqlproc.IndexReaderParam = &plan.IndexReaderParam{
		OrderBy: []*plan.OrderBySpec{{Flag: plan.OrderBySpec_ASC}},
	}
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

func TestScanEntriesKeepsPostFilterTopKForResiduals(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	residual, err := ivfFuncExpr(proc.Ctx, "=", ivfInt64Expr(1), ivfInt64Expr(1))
	require.NoError(t, err)
	scanner := &scriptedRelationScanner{t: t}
	scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
		require.True(t, req.PostFilterTopOnly)
		require.Nil(t, req.IndexParam.OrderBy[0].Expr)
		require.Equal(t, plan.OrderBySpec_DESC, req.IndexParam.OrderBy[0].Flag)
		require.NotNil(t, req.Filter)
		require.Empty(t, req.BlockFilters)
		require.NotContains(t, req.Columns, catalog.CPrimaryKeyColName)

		bat := batch.NewWithSize(4)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[3] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(4), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(2), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(7), false, mp))
		require.NoError(t, vector.AppendArray(bat.Vecs[3], []float32{1, 2}, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RelationScanner = scanner
	sqlproc.IndexReaderParam = &plan.IndexReaderParam{
		OrderBy: []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}},
	}
	idx := &IvfflatSearchIndex[float32]{QuantMul: 1}
	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2sqDistance)
	idxcfg.Ivfflat.VectorType = int32(types.T_array_float32)

	res, err := idx.scanEntries(sqlproc, idxcfg, vectorindex.IndexTableConfig{
		DbName:       "db1",
		EntriesTable: "entries1",
	}, []float32{0, 0}, 4, []int64{2, 3}, nil, []*plan.Expr{residual}, 3)
	require.NoError(t, err)
	defer res.Close()
	require.Len(t, res.Batches, 1)
	require.Len(t, res.Batches[0].Vecs, 2)
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](res.Batches[0].Vecs[0], 0))
	require.Equal(t, float64(5), vector.GetFixedAtNoTypeCheck[float64](res.Batches[0].Vecs[1], 0))
}

func TestStorageTopKEligibility(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	centroids := []int64{1}
	require.True(t, canUseStorageTopK(sqlproc, centroids, nil, 1))
	require.False(t, canUseStorageTopK(nil, centroids, nil, 1))
	require.False(t, canUseStorageTopK(sqlproc, nil, nil, 1))
	require.False(t, canUseStorageTopK(sqlproc, centroids, []*plan.Expr{ivfInt64Expr(1)}, 1))
	require.False(t, canUseStorageTopK(sqlproc, centroids, nil, 0))

	sqlproc.IvfHasMembershipFilter = true
	require.False(t, canUseStorageTopK(sqlproc, centroids, nil, 1))
	sqlproc.IvfHasMembershipFilter = false
	sqlproc.IndexReaderParam = &plan.IndexReaderParam{DistRange: &plan.DistRange{
		LowerBoundType: plan.BoundType_INCLUSIVE,
	}}
	require.False(t, canUseStorageTopK(sqlproc, centroids, nil, 1))
	sqlproc.IndexReaderParam.DistRange = &plan.DistRange{
		LowerBoundType: plan.BoundType_UNBOUNDED,
		UpperBoundType: plan.BoundType_UNBOUNDED,
	}
	require.True(t, canUseStorageTopK(sqlproc, centroids, nil, 1))

	require.Equal(t, plan.OrderBySpec_ASC, ivfOrderFlag(nil))
	sqlproc.IndexReaderParam.OrderBy = []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}}
	require.Equal(t, plan.OrderBySpec_DESC, ivfOrderFlag(sqlproc.IndexReaderParam))
	require.False(t, canUseStorageTopK(sqlproc, centroids, nil, 1))
}

func TestStorageTopKEligibilityMatchesVectorTopNDirection(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	entries := vector.NewVec(types.New(types.T_array_float32, 1, 0))
	defer entries.Free(mp)
	for _, entry := range [][]float32{{1}, {10}} {
		require.NoError(t, vector.AppendArray(entries, entry, false, mp))
	}

	storageTop := &objectio.IndexReaderTopOp{
		Typ:        types.T_array_float32,
		MetricType: metric.Metric_L2sqDistance,
		NumVec:     types.ArrayToBytes([]float32{0}),
		Limit:      1,
		Desc:       true,
	}
	rows, distances, err := objectio.TopNVector(context.Background(), nil, entries, storageTop)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, rows)
	require.Equal(t, []float64{1}, distances)

	proc := testutil.NewProcessWithMPool(t, "", mp)
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.IndexReaderParam = &plan.IndexReaderParam{
		OrderBy: []*plan.OrderBySpec{{Flag: plan.OrderBySpec_DESC}},
	}
	require.False(t, canUseStorageTopK(sqlproc, []int64{1}, nil, 1),
		"descending requests must use local Top-K until storage implements descending vector selection")
}

func TestScanEntriesFailsClosedAtTopKBoundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	baseConfig := func() vectorindex.IndexConfig {
		cfg := vectorindex.IndexConfig{}
		cfg.Ivfflat.Metric = uint16(metric.Metric_L2sqDistance)
		cfg.Ivfflat.VectorType = int32(types.T_array_float32)
		return cfg
	}
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", EntriesTable: "entries", PKeyType: int32(types.T_int64)}
	query := []float32{0, 0}

	t.Run("malformed membership", func(t *testing.T) {
		sqlproc := sqlexec.NewSqlProcess(proc)
		sqlproc.IvfHasMembershipFilter = true
		sqlproc.IvfRuntimeFilterData = []byte("not-a-vector")
		_, err := (&IvfflatSearchIndex[float32]{}).scanEntries(
			sqlproc, baseConfig(), tblcfg, query, 1, []int64{2}, nil, nil, 1)
		require.Error(t, err)
	})

	t.Run("unknown storage metric", func(t *testing.T) {
		cfg := baseConfig()
		cfg.Ivfflat.Metric = math.MaxUint16
		sqlproc := sqlexec.NewSqlProcess(proc)
		_, err := (&IvfflatSearchIndex[float32]{}).scanEntries(
			sqlproc, cfg, tblcfg, query, 1, []int64{2}, nil, nil, 1)
		require.Error(t, err)
	})

	t.Run("storage omits distance", func(t *testing.T) {
		scanner := &scriptedRelationScanner{t: t, run: func(req sqlexec.RelationScanRequest) executor.Result {
			bat := batch.NewWithSize(len(req.Columns))
			for i := range bat.Vecs {
				bat.Vecs[i] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(bat.Vecs[i], int64(0), false, mp))
			}
			bat.SetRowCount(1)
			return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
		}}
		sqlproc := sqlexec.NewSqlProcess(proc)
		sqlproc.RelationScanner = scanner
		_, err := (&IvfflatSearchIndex[float32]{}).scanEntries(
			sqlproc, baseConfig(), tblcfg, query, 1, []int64{2}, nil, nil, 1)
		require.ErrorContains(t, err, "storage Top-K returned")
	})

	t.Run("local distance evaluation fails", func(t *testing.T) {
		residual, err := ivfFuncExpr(proc.Ctx, "=", ivfInt64Expr(1), ivfInt64Expr(1))
		require.NoError(t, err)
		scanner := &scriptedRelationScanner{t: t, run: func(sqlexec.RelationScanRequest) executor.Result {
			bat := batch.NewWithSize(4)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(2), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[2], int64(3), false, mp))
			require.NoError(t, vector.AppendArray(bat.Vecs[3], []float32{1, 2}, false, mp))
			bat.SetRowCount(1)
			return executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}
		}}
		cfg := baseConfig()
		cfg.Ivfflat.Metric = math.MaxUint16
		sqlproc := sqlexec.NewSqlProcess(proc)
		sqlproc.RelationScanner = scanner
		_, err = (&IvfflatSearchIndex[float32]{}).scanEntries(
			sqlproc, cfg, tblcfg, query, 1, []int64{2}, nil, []*plan.Expr{residual}, 1)
		require.Error(t, err)
	})

	_, err := ivfCentroidPrefixFilter(proc.Ctx, mp, 1, nil, 4)
	require.ErrorContains(t, err, "requires at least one centroid")
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
	_, err = ivfRuntimeMembershipExpr(proc.Ctx, nil, nil)
	require.ErrorContains(t, err, "runtime membership filter is empty")
	_, err = ivfRuntimeMembershipExpr(proc.Ctx, []byte("not-a-vector"),
		ivfColExpr(2, plan.Type{Id: int32(types.T_int32)}))
	require.Error(t, err)
}

func TestPlanReaderSortsAndBoundsCandidates(t *testing.T) {
	r := &planReader{
		spec:         &plan.VectorIndexScan{},
		req:          searchplugin.Request{CandidateBudget: 2},
		keys:         []any{int64(3), int64(1), int64(2)},
		distances:    []float64{3, 1, 2},
		includeData:  map[string][]any{"payload": {int32(30), int32(10), int32(20)}},
		includeNulls: map[string][]bool{"payload": {false, false, false}},
	}
	r.sortAndLimit(2)
	require.Equal(t, []any{int64(1), int64(2)}, r.keys)
	require.Equal(t, []float64{1, 2}, r.distances)
	require.Equal(t, []any{int32(10), int32(20)}, r.includeData["payload"])
}

func TestPlanReaderSortAndLimitHandlesMaxUint64(t *testing.T) {
	r := &planReader{
		spec:         &plan.VectorIndexScan{},
		keys:         []any{int64(1)},
		distances:    []float64{1},
		includeData:  map[string][]any{},
		includeNulls: map[string][]bool{},
	}
	require.NotPanics(t, func() { r.sortAndLimit(math.MaxUint64) })
	require.Equal(t, []any{int64(1)}, r.keys)
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

	desc := executor.Result{Mp: mp, Batches: []*batch.Batch{
		makeBatch(1, 3), makeBatch(2, 1), makeBatch(3, 2),
	}}
	require.NoError(t, compactRelationTop(&desc, 2, true))
	defer desc.Close()
	require.Equal(t, []int64{1, 3}, vector.MustFixedColWithTypeCheck[int64](desc.Batches[0].Vecs[0]))
	require.Equal(t, []float64{3, 2}, vector.MustFixedColWithTypeCheck[float64](desc.Batches[0].Vecs[2]))
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

func TestRelationFilterAndTopHelpersCoverEmptyAndDescendingCases(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	filter, err := ivfFuncExpr(proc.Ctx, "=",
		ivfColExpr(0, plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}),
		ivfStringExpr("keep"))
	require.NoError(t, err)
	executor, err := colexec.NewExpressionExecutor(proc, filter)
	require.NoError(t, err)
	defer executor.Free()
	makeBatch := func(values ...string) *batch.Batch {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		for _, value := range values {
			require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte(value), false, mp))
		}
		bat.SetRowCount(len(values))
		return bat
	}
	all := makeBatch("keep", "keep")
	require.NoError(t, filterRelationBatch(proc, executor, all))
	require.Equal(t, 2, all.RowCount())
	all.Clean(mp)
	none := makeBatch("drop")
	require.NoError(t, filterRelationBatch(proc, executor, none))
	require.Zero(t, none.RowCount())
	none.Clean(mp)

	_, ok := relationVectorTopLimit(nil, false)
	require.False(t, ok)
	_, ok = relationVectorTopLimit(&plan.IndexReaderParam{Limit: ivfUint64Expr(0), OrderBy: []*plan.OrderBySpec{{}}}, true)
	require.False(t, ok)
	require.NoError(t, compactRelationTop(nil, 1, true))
}

func TestRelationSearchBoundaryBranches(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	floatLiteral := func(value float64) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Dval{Dval: value},
			}},
		}
	}

	_, typ, err := (&IvfflatSearchIndex[float64]{}).entryQueryBytes(vectorindex.IndexConfig{}, []float64{1, 2})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_array_float64), typ.Id)

	emptyKeys := vector.NewVec(types.T_int64.ToType())
	emptyKeyData, err := emptyKeys.MarshalBinary()
	require.NoError(t, err)
	emptyKeys.Free(mp)
	_, err = ivfRuntimeMembershipExpr(proc.Ctx, emptyKeyData,
		ivfColExpr(0, plan.Type{Id: int32(types.T_int64)}))
	require.ErrorContains(t, err, "runtime membership key set is empty")

	_, hasBound, err := vectorDistanceBound(plan.BoundType_UNBOUNDED, nil)
	require.NoError(t, err)
	require.False(t, hasBound)
	_, _, err = vectorDistanceBound(plan.BoundType_INCLUSIVE, nil)
	require.ErrorContains(t, err, "did not fold to a numeric literal")
	_, _, err = vectorDistanceBound(plan.BoundType(99), nil)
	require.ErrorContains(t, err, "invalid IVF distance bound type")

	rangeExclusive := &plan.DistRange{
		LowerBoundType: plan.BoundType_EXCLUSIVE,
		LowerBound:     floatLiteral(1),
		UpperBoundType: plan.BoundType_EXCLUSIVE,
		UpperBound:     floatLiteral(3),
	}
	require.False(t, vectorDistanceInRange(1, rangeExclusive, 1, true, 3, true))
	require.False(t, vectorDistanceInRange(3, rangeExclusive, 1, true, 3, true))
	require.True(t, vectorDistanceInRange(2, rangeExclusive, 1, true, 3, true))

	newResult := func(values ...float64) executor.Result {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
		for _, value := range values {
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
		}
		bat.SetRowCount(len(values))
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	}
	idx := &IvfflatSearchIndex[float32]{QuantMul: 1}
	require.NoError(t, idx.filterEntryDistanceRange(nil, rangeExclusive, metric.DistFn_L2sqDistance, metric.Metric_L2sqDistance))
	unbounded := &plan.DistRange{LowerBoundType: plan.BoundType_UNBOUNDED, UpperBoundType: plan.BoundType_UNBOUNDED}
	all := newResult(1, 2)
	require.NoError(t, idx.filterEntryDistanceRange(&all, unbounded, metric.DistFn_L2sqDistance, metric.Metric_L2sqDistance))
	require.Equal(t, 2, all.Batches[0].RowCount())
	all.Close()
	all = newResult(1, 2)
	noneRange := &plan.DistRange{LowerBoundType: plan.BoundType_INCLUSIVE, LowerBound: floatLiteral(3), UpperBoundType: plan.BoundType_UNBOUNDED}
	require.NoError(t, idx.filterEntryDistanceRange(&all, noneRange, metric.DistFn_L2sqDistance, metric.Metric_L2sqDistance))
	require.Zero(t, all.Batches[0].RowCount())
	all.Close()

	emptyResult := executor.Result{Mp: mp, Batches: []*batch.Batch{batch.NewWithSize(0)}}
	require.NoError(t, appendEntryDistances(nil, &emptyResult, nil, plan.Type{}, ""))
	emptyResult.Close()
	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[3] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
	require.NoError(t, vector.AppendArray(bat.Vecs[3], []float32{1, 2}, false, mp))
	bat.SetRowCount(1)
	badFunctionResult := executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	require.Error(t, appendEntryDistances(sqlexec.NewSqlProcess(proc), &badFunctionResult,
		types.ArrayToBytes([]float32{0, 0}), plan.Type{Id: int32(types.T_array_float32), Width: 2}, "not_a_distance"))
	badFunctionResult.Close()
}

func TestPlanReaderReadStreamsAndCloses(t *testing.T) {
	mp := mpool.MustNewZero()
	r := &planReader{
		initialized:  true,
		keys:         []any{int64(7), int64(9)},
		distances:    []float64{0.25, 0.5},
		includeData:  map[string][]any{"payload": {int32(70), int32(90)}},
		includeNulls: map[string][]bool{"payload": {false, true}},
	}
	out := batch.NewWithSize(3)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	out.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	out.Vecs[2] = vector.NewVec(types.T_int32.ToType())
	defer out.Clean(mp)

	attrs := []string{"pkid", "score", catalog.SystemSI_IVFFLAT_IncludeColPrefix + "payload"}
	end, err := r.Read(context.Background(), attrs, nil, mp, out)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, []int64{7, 9}, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0]))
	require.Equal(t, []float64{0.25, 0.5}, vector.MustFixedColWithTypeCheck[float64](out.Vecs[1]))
	require.Equal(t, int32(70), vector.GetFixedAtNoTypeCheck[int32](out.Vecs[2], 0))
	require.True(t, out.Vecs[2].IsNull(1))

	end, err = r.Read(context.Background(), attrs, nil, mp, out)
	require.NoError(t, err)
	require.True(t, end)
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
	end, err = r.Read(context.Background(), attrs, nil, mp, out)
	require.NoError(t, err)
	require.True(t, end)
}

func TestPlanReaderReadRejectsCancelledAndInvalidOutput(t *testing.T) {
	mp := mpool.MustNewZero()
	newReader := func() *planReader {
		return &planReader{
			initialized:  true,
			keys:         []any{int64(7)},
			distances:    []float64{0.25},
			includeData:  map[string][]any{},
			includeNulls: map[string][]bool{},
		}
	}
	newOutput := func(typ types.Type) *batch.Batch {
		out := batch.NewWithSize(1)
		out.Vecs[0] = vector.NewVec(typ)
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := newOutput(types.T_int64.ToType())
	_, err := newReader().Read(ctx, []string{"pkid"}, nil, mp, out)
	require.ErrorIs(t, err, context.Canceled)
	out.Clean(mp)

	out = newOutput(types.T_int32.ToType())
	_, err = newReader().Read(context.Background(), []string{catalog.SystemSI_IVFFLAT_IncludeColPrefix + "missing"}, nil, mp, out)
	require.ErrorContains(t, err, "include output \"missing\" is not aligned")
	out.Clean(mp)

	out = newOutput(types.T_int32.ToType())
	_, err = newReader().Read(context.Background(), []string{"unexpected"}, nil, mp, out)
	require.ErrorContains(t, err, "unknown ivfflat vector scan output \"unexpected\"")
	out.Clean(mp)
}

func TestPlanReaderPureHelpersCoverDecodedQueriesAndScanBatches(t *testing.T) {
	r := &planReader{
		spec: &plan.VectorIndexScan{HiddenTables: []*plan.VectorIndexTableRef{{
			Role:   "entries",
			Object: &plan.ObjectRef{ObjName: "entries_hidden"},
		}}},
	}
	require.Equal(t, "entries_hidden", r.hiddenTable("entries"))
	require.Empty(t, r.hiddenTable("missing"))

	r.req.QueryType = plan.Type{Id: int32(types.T_array_float32)}
	r.req.QueryVector = types.ArrayToBytes([]float32{1, 2})
	query32, err := r.queryFloat32()
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2}, query32)

	r.req.QueryType = plan.Type{Id: int32(types.T_array_float64)}
	r.req.QueryVector = types.ArrayToBytes([]float64{1.5, 2.5})
	query32, err = r.queryFloat32()
	require.NoError(t, err)
	require.Equal(t, []float32{1.5, 2.5}, query32)
	query64, err := r.queryFloat64()
	require.NoError(t, err)
	require.Equal(t, []float64{1.5, 2.5}, query64)

	r.req.QueryType = plan.Type{Id: int32(types.T_int64)}
	_, err = r.queryFloat32()
	require.ErrorContains(t, err, "unsupported IVF query type")
	_, err = r.queryFloat64()
	require.ErrorContains(t, err, "f64 IVF centroids require a VECF64 query")

	for _, test := range []struct {
		typ  types.T
		data []byte
	}{
		{types.T_array_bf16, types.ArrayToBytes(types.Float32ToBF16Slice([]float32{1, 2}))},
		{types.T_array_float16, types.ArrayToBytes(types.Float32ToFloat16Slice([]float32{1, 2}))},
		{types.T_array_int8, types.ArrayToBytes([]int8{1, 2})},
		{types.T_array_uint8, types.ArrayToBytes([]uint8{1, 2})},
	} {
		r.req.QueryType = plan.Type{Id: int32(test.typ)}
		r.req.QueryVector = test.data
		query32, err = r.queryFloat32()
		require.NoError(t, err)
		require.Equal(t, []float32{1, 2}, query32)
	}

	param := &plan.IndexReaderParam{
		Limit:   ivfUint64Expr(4),
		OrderBy: []*plan.OrderBySpec{{}},
	}
	limit, ok := relationVectorTopLimit(param, true)
	require.True(t, ok)
	require.Equal(t, 4, limit)
	_, ok = relationVectorTopLimit(param, false)
	require.False(t, ok)

	tableDef := &plan.TableDef{
		Name:          "entries",
		Cols:          []*plan.ColDef{{Name: "key", Typ: plan.Type{Id: int32(types.T_int64)}}},
		Name2ColIndex: map[string]int32{"key": 0},
	}
	bat, err := makeRelationScanBatch(tableDef, []string{"KEY"})
	require.NoError(t, err)
	require.Equal(t, types.T_int64, bat.Vecs[0].GetType().Oid)
	bat.Clean(nil)
	_, err = makeRelationScanBatch(tableDef, []string{"missing"})
	require.ErrorContains(t, err, "hidden column \"missing\" not found")
}

type fixedRelationReader struct {
	emitted bool
	closed  int
	rows    [][2]int64
}

var _ engine.Reader = (*fixedRelationReader)(nil)

func (r *fixedRelationReader) Read(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.emitted {
		return true, nil
	}
	for _, row := range r.rows {
		if err := vector.AppendFixed(out.Vecs[0], row[0], false, mp); err != nil {
			return false, err
		}
		if err := vector.AppendFixed(out.Vecs[1], row[1], false, mp); err != nil {
			return false, err
		}
	}
	out.SetRowCount(len(r.rows))
	r.emitted = true
	return false, nil
}

func (r *fixedRelationReader) Close() error                  { r.closed++; return nil }
func (*fixedRelationReader) SetOrderBy([]*plan.OrderBySpec)  {}
func (*fixedRelationReader) GetOrderBy() []*plan.OrderBySpec { return nil }
func (*fixedRelationReader) SetIndexParam(*plan.IndexReaderParam) {
}
func (*fixedRelationReader) SetFilterZM(objectio.ZoneMap) {}

type fillRelationReader struct {
	emitted bool
	closed  int
	fill    func(*batch.Batch, *mpool.MPool) error
}

var _ engine.Reader = (*fillRelationReader)(nil)

func (r *fillRelationReader) Read(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.emitted {
		return true, nil
	}
	if err := r.fill(out, mp); err != nil {
		return false, err
	}
	r.emitted = true
	return false, nil
}

func (r *fillRelationReader) Close() error                  { r.closed++; return nil }
func (*fillRelationReader) SetOrderBy([]*plan.OrderBySpec)  {}
func (*fillRelationReader) GetOrderBy() []*plan.OrderBySpec { return nil }
func (*fillRelationReader) SetIndexParam(*plan.IndexReaderParam) {
}
func (*fillRelationReader) SetFilterZM(objectio.ZoneMap) {}

func TestRelationScannerExecutesTypedReaderLifecycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	proc.Base.SessionInfo.StorageEngine = eng

	tableDef := &plan.TableDef{
		Name: "entries",
		Cols: []*plan.ColDef{
			{Name: "version", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
		Name2ColIndex: map[string]int32{"version": 0, "id": 1},
	}
	reader := &fixedRelationReader{rows: [][2]int64{{7, 11}, {7, 12}}}
	eng.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "entries", proc).Return(rel, nil)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(tableDef)
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, param engine.RangesParam) (engine.RelData, error) {
			require.Equal(t, engine.DataCollectPolicy(engine.Policy_CollectCommittedPersistedData), param.Policy)
			require.Equal(t, int32(2), param.Rsp.CNCNT)
			require.Equal(t, int32(1), param.Rsp.CNIDX)
			require.True(t, param.Rsp.ShuffleByObjectID)
			return nil, nil
		})
	rel.EXPECT().BuildReaders(
		gomock.Any(), proc, nil, gomock.Nil(), 1, 0, false,
		gomock.Any(), gomock.Any()).Return([]engine.Reader{reader}, nil)

	scanner := &relationScanner{
		proc:           proc,
		partitionCount: 2,
		partitionIndex: 1,
		ownsInMemory:   false,
	}
	res, err := scanner.ScanRelation(sqlexec.RelationScanRequest{
		Schema:  "db",
		Table:   "entries",
		Columns: []string{"version", "id"},
	})
	require.NoError(t, err)
	defer res.Close()
	require.Len(t, res.Batches, 1)
	require.Equal(t, []int64{7, 7}, vector.MustFixedColWithTypeCheck[int64](res.Batches[0].Vecs[0]))
	require.Equal(t, []int64{11, 12}, vector.MustFixedColWithTypeCheck[int64](res.Batches[0].Vecs[1]))
	require.Equal(t, 1, reader.closed)
}

func TestRelationScannerPropagatesStorageFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	eng := mock_frontend.NewMockEngine(ctrl)
	proc.Base.SessionInfo.StorageEngine = eng
	eng.EXPECT().Database(gomock.Any(), "db", nil).Return(nil, errors.New("database unavailable"))

	res, err := (&relationScanner{proc: proc}).ScanRelation(sqlexec.RelationScanRequest{Schema: "db", Table: "entries"})
	require.ErrorContains(t, err, "database unavailable")
	require.Empty(t, res.Batches)
}

func TestRelationScannerUsesSnapshotCloneAndPublisherAccount(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	original := mock_frontend.NewMockTxnOperator(ctrl)
	clone := mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.TxnOperator = original
	snapshotTS := timestamp.Timestamp{PhysicalTime: 8}
	original.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 10}})
	original.EXPECT().CloneSnapshotOp(snapshotTS).Return(clone)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	proc.Base.SessionInfo.StorageEngine = eng
	eng.EXPECT().Database(gomock.Any(), "db", clone).DoAndReturn(
		func(ctx context.Context, _ string, _ any) (engine.Database, error) {
			accountID, err := defines.GetAccountId(ctx)
			require.NoError(t, err)
			require.Equal(t, uint32(42), accountID)
			return db, nil
		})
	db.EXPECT().Relation(gomock.Any(), "entries", proc).Return(nil, errors.New("snapshot relation unavailable"))

	accountID := uint32(42)
	reader, err := NewPlanReader(proc, &plan.VectorIndexScan{
		Index:       &plan.IndexDef{},
		SourceTable: &plan.ObjectRef{PubInfo: &plan.PubInfo{TenantId: 42}},
		ScanSnapshot: &plan.Snapshot{
			TS:     &snapshotTS,
			Tenant: &plan.SnapshotTenant{TenantID: 99},
		},
	}, searchplugin.Request{Identity: searchplugin.ScanIdentity{
		PhysicalAccountID: &accountID,
		Snapshot:          &plan.Snapshot{TS: &snapshotTS},
		PartitionCount:    1,
	}})
	require.NoError(t, err)
	_, err = reader.(*planReader).scanner.ScanRelation(sqlexec.RelationScanRequest{Schema: "db", Table: "entries"})
	require.ErrorContains(t, err, "snapshot relation unavailable")
	require.Same(t, clone, proc.GetCloneTxnOperator())
}

func TestRelationScannerKeepsCurrentTxnForEqualAndAheadSnapshots(t *testing.T) {
	for _, snapshotTS := range []timestamp.Timestamp{
		{PhysicalTime: 10},
		{PhysicalTime: 11},
	} {
		t.Run(snapshotTS.DebugString(), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			original := mock_frontend.NewMockTxnOperator(ctrl)
			proc.Base.TxnOperator = original
			original.EXPECT().Txn().Return(txn.TxnMeta{
				SnapshotTS: timestamp.Timestamp{PhysicalTime: 10},
			})

			eng := mock_frontend.NewMockEngine(ctrl)
			proc.Base.SessionInfo.StorageEngine = eng
			eng.EXPECT().Database(gomock.Any(), "db", original).
				Return(nil, errors.New("current relation unavailable"))

			reader, err := NewPlanReader(proc, &plan.VectorIndexScan{
				Index:       &plan.IndexDef{},
				SourceTable: &plan.ObjectRef{},
			}, searchplugin.Request{Identity: searchplugin.ScanIdentity{
				Snapshot:       &plan.Snapshot{TS: &snapshotTS},
				PartitionCount: 1,
			}})
			require.NoError(t, err)
			_, err = reader.(*planReader).scanner.ScanRelation(
				sqlexec.RelationScanRequest{Schema: "db", Table: "entries"})
			require.ErrorContains(t, err, "current relation unavailable")
			require.Nil(t, proc.GetCloneTxnOperator())
		})
	}
}

func TestRelationScannerPropagatesRelationSetupFailures(t *testing.T) {
	validDef := &plan.TableDef{Name: "entries"}
	for _, test := range []struct {
		name  string
		setup func(*mock_frontend.MockEngine, *mock_frontend.MockDatabase, *mock_frontend.MockRelation)
	}{
		{
			name: "relation lookup",
			setup: func(eng *mock_frontend.MockEngine, db *mock_frontend.MockDatabase, _ *mock_frontend.MockRelation) {
				eng.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "entries", gomock.Any()).Return(nil, errors.New("relation unavailable"))
			},
		},
		{
			name: "missing table definition",
			setup: func(eng *mock_frontend.MockEngine, db *mock_frontend.MockDatabase, rel *mock_frontend.MockRelation) {
				eng.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "entries", gomock.Any()).Return(rel, nil)
				rel.EXPECT().GetTableDef(gomock.Any()).Return(nil)
			},
		},
		{
			name: "range collection",
			setup: func(eng *mock_frontend.MockEngine, db *mock_frontend.MockDatabase, rel *mock_frontend.MockRelation) {
				eng.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "entries", gomock.Any()).Return(rel, nil)
				rel.EXPECT().GetTableDef(gomock.Any()).Return(validDef)
				rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, errors.New("ranges unavailable"))
			},
		},
		{
			name: "reader construction",
			setup: func(eng *mock_frontend.MockEngine, db *mock_frontend.MockDatabase, rel *mock_frontend.MockRelation) {
				eng.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil)
				db.EXPECT().Relation(gomock.Any(), "entries", gomock.Any()).Return(rel, nil)
				rel.EXPECT().GetTableDef(gomock.Any()).Return(validDef)
				rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil)
				rel.EXPECT().BuildReaders(
					gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
					gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("readers unavailable"))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			proc.Base.SessionInfo.StorageEngine = eng
			test.setup(eng, db, rel)
			_, err := (&relationScanner{proc: proc}).ScanRelation(sqlexec.RelationScanRequest{Schema: "db", Table: "entries"})
			require.Error(t, err)
		})
	}
}

func TestRelationScannerFiltersBeforeApplyingTopLimit(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	rel := mock_frontend.NewMockRelation(ctrl)
	proc.Base.SessionInfo.StorageEngine = eng
	tableDef := &plan.TableDef{
		Name: "ranked_entries",
		Cols: []*plan.ColDef{
			{Name: "pk", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "score", Typ: plan.Type{Id: int32(types.T_float64)}},
		},
		Name2ColIndex: map[string]int32{"pk": 0, "score": 1},
	}
	reader := &fillRelationReader{fill: func(out *batch.Batch, mp *mpool.MPool) error {
		for _, row := range []struct {
			pk    int64
			score float64
		}{{1, 2}, {2, 1}} {
			if err := vector.AppendFixed(out.Vecs[0], row.pk, false, mp); err != nil {
				return err
			}
			if err := vector.AppendFixed(out.Vecs[1], row.score, false, mp); err != nil {
				return err
			}
		}
		out.SetRowCount(2)
		return nil
	}}
	eng.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil)
	db.EXPECT().Relation(gomock.Any(), "ranked_entries", proc).Return(rel, nil)
	rel.EXPECT().GetTableDef(gomock.Any()).Return(tableDef)
	rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil)
	rel.EXPECT().BuildReaders(
		gomock.Any(), proc, gomock.Any(), gomock.Nil(), 1, 0, false,
		gomock.Any(), gomock.Any()).Return([]engine.Reader{reader}, nil)

	filter, err := ivfFuncExpr(proc.Ctx, "=", ivfInt64Expr(1), ivfInt64Expr(1))
	require.NoError(t, err)
	transformed := false
	res, err := (&relationScanner{proc: proc}).ScanRelation(sqlexec.RelationScanRequest{
		Schema:            "db",
		Table:             "ranked_entries",
		Columns:           []string{"pk", "score"},
		Filter:            filter,
		IndexParam:        &plan.IndexReaderParam{Limit: ivfUint64Expr(1), OrderBy: []*plan.OrderBySpec{{}}},
		PostFilterTopOnly: true,
		BatchTransform: func(*batch.Batch) error {
			transformed = true
			return nil
		},
	})
	require.NoError(t, err)
	defer res.Close()
	require.True(t, transformed)
	require.Len(t, res.Batches, 1)
	require.Equal(t, 1, res.Batches[0].RowCount())
	require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](res.Batches[0].Vecs[0], 0))
	require.Equal(t, 1, reader.closed)
}

func TestPlanReaderShortCircuitsEmptySearches(t *testing.T) {
	r := &planReader{req: searchplugin.Request{CandidateBudget: 0}}
	require.NoError(t, r.initialize())
	r.req = searchplugin.Request{CandidateBudget: 1, HasMembershipFilter: true}
	require.NoError(t, r.initialize())
	r.req = searchplugin.Request{CandidateBudget: 1}
	r.spec = &plan.VectorIndexScan{Index: &plan.IndexDef{IndexAlgoParams: "not-json"}}
	require.Error(t, r.initialize())

	r = &planReader{req: searchplugin.Request{CandidateBudget: 0}, spec: &plan.VectorIndexScan{}}
	require.NoError(t, searchPlanReader(r, nil, vectorindex.IndexConfig{}, vectorindex.IndexTableConfig{}, []float32{1}))
	require.Empty(t, r.keys)
}

func TestPlanReaderRejectsMalformedIndexMetadataBeforeStorageAccess(t *testing.T) {
	base := func() *plan.VectorIndexScan {
		return &plan.VectorIndexScan{
			Index: &plan.IndexDef{
				IndexAlgoParams: `{"lists":"1","op_type":"vector_l2_ops"}`,
				Parts:           []string{"embedding"},
			},
			SourceTable: &plan.ObjectRef{SchemaName: "db"},
			SourceTableDef: &plan.TableDef{
				Name: "source",
				Cols: []*plan.ColDef{
					{Name: "pk", Typ: plan.Type{Id: int32(types.T_int64)}},
					{Name: "embedding", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2}},
				},
				Name2ColIndex: map[string]int32{"pk": 0, "embedding": 1},
				Pkey:          &plan.PrimaryKeyDef{PkeyColName: "pk"},
			},
			HiddenTables: []*plan.VectorIndexTableRef{
				{Role: catalog.SystemSI_IVFFLAT_TblType_Metadata, Object: &plan.ObjectRef{ObjName: "metadata"}},
				{Role: catalog.SystemSI_IVFFLAT_TblType_Centroids, Object: &plan.ObjectRef{ObjName: "centroids"}},
				{Role: catalog.SystemSI_IVFFLAT_TblType_Entries, Object: &plan.ObjectRef{ObjName: "entries"}},
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(*plan.VectorIndexScan)
		want   string
	}{
		{
			name: "invalid lists",
			mutate: func(spec *plan.VectorIndexScan) {
				spec.Index.IndexAlgoParams = `{"lists":"0","op_type":"vector_l2_ops"}`
			},
			want: "invalid IVF lists",
		},
		{
			name: "invalid metric",
			mutate: func(spec *plan.VectorIndexScan) {
				spec.Index.IndexAlgoParams = `{"lists":"1","op_type":"not-a-metric"}`
			},
			want: "invalid IVF op_type",
		},
		{
			name: "missing index part",
			mutate: func(spec *plan.VectorIndexScan) {
				spec.Index.Parts = nil
			},
			want: "incomplete IVF source/index metadata",
		},
		{
			name: "missing hidden table",
			mutate: func(spec *plan.VectorIndexScan) {
				spec.HiddenTables = nil
			},
			want: "missing hidden-table references",
		},
		{
			name: "missing vector column",
			mutate: func(spec *plan.VectorIndexScan) {
				spec.Index.Parts = []string{"missing"}
			},
			want: "source vector column \"missing\" not found",
		},
		{
			name: "missing primary key column",
			mutate: func(spec *plan.VectorIndexScan) {
				spec.SourceTableDef.Pkey.PkeyColName = "missing"
			},
			want: "source primary key \"missing\" not found",
		},
		{
			name: "missing included column",
			mutate: func(spec *plan.VectorIndexScan) {
				spec.Index.IncludedColumns = []string{"missing"}
			},
			want: "included column \"missing\" not found",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := base()
			test.mutate(spec)
			err := (&planReader{spec: spec, req: searchplugin.Request{CandidateBudget: 1}}).initialize()
			require.ErrorContains(t, err, test.want)
		})
	}
}

func TestSearchPlanReaderValidatesRoundLimitsBeforeScanning(t *testing.T) {
	idxcfg := vectorindex.IndexConfig{}
	tblcfg := vectorindex.IndexTableConfig{IndexTable: "round_limit_validation"}
	reader := &planReader{
		spec: &plan.VectorIndexScan{},
		req: searchplugin.Request{
			CandidateBudget: 1,
			HasFirstRound:   true,
			FirstRoundLimit: math.MaxUint64,
		},
	}
	err := searchPlanReader(reader, nil, idxcfg, tblcfg, []float32{0})
	require.ErrorContains(t, err, "first-round limit is not a platform uint")

	reader = &planReader{
		spec: &plan.VectorIndexScan{SourceTable: &plan.ObjectRef{PubInfo: &plan.PubInfo{TenantId: 7}}},
		req: searchplugin.Request{Identity: searchplugin.ScanIdentity{
			PartitionCount: 2,
			PartitionIndex: 1,
		}},
	}
	require.NoError(t, searchPlanReader(reader, nil, idxcfg, tblcfg, []float32{0}))
}

func TestSearchPlanReaderUsesDirectCentroidAndEntriesRelations(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	scanner := &scriptedRelationScanner{t: t}
	scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
		switch req.Table {
		case "centroids_plan_reader":
			bat := batch.NewWithSize(3)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[2] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(77), false, mp))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(0), false, mp))
			require.NoError(t, vector.AppendArray(bat.Vecs[2], []float32{0, 0}, false, mp))
			bat.SetRowCount(1)
			return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
		case "entries_plan_reader":
			require.NotNil(t, req.IndexParam)
			require.Equal(t, uint64(12), req.IndexParam.GetLimit().GetLit().GetU64Val())
			require.False(t, req.PostFilterTopOnly)
			bat := batch.NewWithSize(7)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[3] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
			bat.Vecs[4] = vector.NewVec(types.T_int32.ToType())
			bat.Vecs[5] = vector.NewVec(types.T_varchar.ToType())
			bat.Vecs[6] = vector.NewVec(types.T_float64.ToType())
			for _, row := range []struct {
				pk  int64
				vec []float32
			}{{1, []float32{0, 0}}, {2, []float32{1, 0}}, {3, []float32{2, 0}}, {4, []float32{3, 0}}, {5, []float32{4, 0}}} {
				require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(77), false, mp))
				require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(0), false, mp))
				require.NoError(t, vector.AppendFixed(bat.Vecs[2], row.pk, false, mp))
				require.NoError(t, vector.AppendArray(bat.Vecs[3], row.vec, false, mp))
				require.NoError(t, vector.AppendFixed(bat.Vecs[4], int32(row.pk*10), false, mp))
				require.NoError(t, vector.AppendBytes(bat.Vecs[5], []byte("cpkey"), false, mp))
				require.NoError(t, vector.AppendFixed(bat.Vecs[6], float64((row.pk-1)*(row.pk-1)), false, mp))
			}
			bat.SetRowCount(5)
			return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
		default:
			t.Fatalf("unexpected relation scan of %q", req.Table)
			return executor.Result{}
		}
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RelationScanner = scanner
	sqlproc.IndexReaderParam = &plan.IndexReaderParam{
		Limit:        ivfUint64Expr(2),
		OrigFuncName: metric.DistFn_L2Distance,
	}
	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Lists = 1
	idxcfg.Ivfflat.Version = 77
	idxcfg.Ivfflat.Dimensions = 2
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2sqDistance)
	idxcfg.Ivfflat.VectorType = int32(types.T_array_float32)
	tblcfg := vectorindex.IndexTableConfig{
		DbName:             "db",
		IndexTable:         "centroids_plan_reader",
		EntriesTable:       "entries_plan_reader",
		OrigFuncName:       metric.DistFn_L2Distance,
		IncludeColumns:     []string{"payload"},
		IncludeColumnTypes: []int32{int32(types.T_int32)},
	}
	r := &planReader{
		spec: &plan.VectorIndexScan{
			InitialProbeCount: 1,
			DistanceFunction:  metric.DistFn_L2Distance,
			IncludedColumns:   []string{"payload"},
			SourceTable:       &plan.ObjectRef{PubInfo: &plan.PubInfo{TenantId: 42}},
		},
		req: searchplugin.Request{
			ResultLimit:     2,
			CandidateBudget: 12,
			Identity: searchplugin.ScanIdentity{
				PartitionCount: 2,
				PartitionIndex: 1,
			},
		},
	}

	require.NoError(t, searchPlanReader(r, sqlproc, idxcfg, tblcfg, []float32{0, 0}))
	require.Equal(t, []any{int64(1), int64(2), int64(3), int64(4), int64(5)}, r.keys)
	require.Equal(t, []float64{0, 1, 2, 3, 4}, r.distances)
	require.Equal(t, []any{int32(10), int32(20), int32(30), int32(40), int32(50)}, r.includeData["payload"])
	require.Len(t, scanner.requests, 2)
}

func TestPlanReaderInitializesThroughTypedEngineRelations(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	eng := mock_frontend.NewMockEngine(ctrl)
	db := mock_frontend.NewMockDatabase(ctrl)
	proc.Base.SessionInfo.StorageEngine = eng

	metadataDef := &plan.TableDef{
		Name: "metadata_init",
		Cols: []*plan.ColDef{
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_key, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Metadata_val, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		},
		Name2ColIndex: map[string]int32{
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_key: 0,
			catalog.SystemSI_IVFFLAT_TblCol_Metadata_val: 1,
		},
	}
	centroidDef := &plan.TableDef{
		Name: "centroids_init",
		Cols: []*plan.ColDef{
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_version, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_id, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid, Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2}},
		},
		Name2ColIndex: map[string]int32{
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_version:  0,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_id:       1,
			catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid: 2,
		},
	}
	entriesDef := &plan.TableDef{
		Name: "entries_init",
		Cols: []*plan.ColDef{
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_version, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_id, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_pk, Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: catalog.SystemSI_IVFFLAT_TblCol_Entries_entry, Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2}},
			{Name: catalog.CPrimaryKeyColName, Typ: plan.Type{
				Id: int32(types.T_varchar), Width: types.MaxVarcharLen, Charset: uint32(types.CharsetBinary),
			}},
		},
		Name2ColIndex: map[string]int32{
			catalog.SystemSI_IVFFLAT_TblCol_Entries_version: 0,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_id:      1,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_pk:      2,
			catalog.SystemSI_IVFFLAT_TblCol_Entries_entry:   3,
			catalog.CPrimaryKeyColName:                      4,
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{
				catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
				catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
				catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			},
			PkeyColName: catalog.CPrimaryKeyColName,
		},
	}
	metadataReader := &fillRelationReader{fill: func(out *batch.Batch, mp *mpool.MPool) error {
		if err := vector.AppendBytes(out.Vecs[0], []byte("version"), false, mp); err != nil {
			return err
		}
		if err := vector.AppendBytes(out.Vecs[1], []byte("991"), false, mp); err != nil {
			return err
		}
		out.SetRowCount(1)
		return nil
	}}
	centroidReader := &fillRelationReader{fill: func(out *batch.Batch, mp *mpool.MPool) error {
		if err := vector.AppendFixed(out.Vecs[0], int64(991), false, mp); err != nil {
			return err
		}
		if err := vector.AppendFixed(out.Vecs[1], int64(0), false, mp); err != nil {
			return err
		}
		if err := vector.AppendArray(out.Vecs[2], []float32{0, 0}, false, mp); err != nil {
			return err
		}
		out.SetRowCount(1)
		return nil
	}}
	entryPacker := types.NewPacker()
	defer entryPacker.Close()
	entriesReader := &fillRelationReader{fill: func(out *batch.Batch, mp *mpool.MPool) error {
		out.Vecs = append(out.Vecs, vector.NewVec(types.T_float64.ToType()))
		for _, row := range []struct {
			pk  int64
			vec []float32
		}{{1, []float32{0, 0}}, {2, []float32{1, 0}}} {
			if err := vector.AppendFixed(out.Vecs[0], int64(991), false, mp); err != nil {
				return err
			}
			if err := vector.AppendFixed(out.Vecs[1], int64(0), false, mp); err != nil {
				return err
			}
			if err := vector.AppendFixed(out.Vecs[2], row.pk, false, mp); err != nil {
				return err
			}
			if err := vector.AppendArray(out.Vecs[3], row.vec, false, mp); err != nil {
				return err
			}
			entryPacker.Reset()
			entryPacker.EncodeInt64(991)
			entryPacker.EncodeInt64(0)
			entryPacker.EncodeInt64(row.pk)
			if err := vector.AppendBytes(out.Vecs[4], entryPacker.Bytes(), false, mp); err != nil {
				return err
			}
			if err := vector.AppendFixed(out.Vecs[5], float64((row.pk-1)*(row.pk-1)), false, mp); err != nil {
				return err
			}
		}
		out.SetRowCount(2)
		return nil
	}}

	metadataRel := mock_frontend.NewMockRelation(ctrl)
	centroidRel := mock_frontend.NewMockRelation(ctrl)
	entriesRel := mock_frontend.NewMockRelation(ctrl)
	configureRelation := func(rel *mock_frontend.MockRelation, def *plan.TableDef, reader engine.Reader) {
		rel.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
		rel.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		rel.EXPECT().BuildReaders(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{reader}, nil).AnyTimes()
	}
	configureRelation(metadataRel, metadataDef, metadataReader)
	configureRelation(centroidRel, centroidDef, centroidReader)
	configureRelation(entriesRel, entriesDef, entriesReader)
	eng.EXPECT().Database(gomock.Any(), "db", nil).Return(db, nil).AnyTimes()
	db.EXPECT().Relation(gomock.Any(), gomock.Any(), proc).DoAndReturn(
		func(_ context.Context, name string, _ any) (engine.Relation, error) {
			switch name {
			case "metadata_init":
				return metadataRel, nil
			case "centroids_init":
				return centroidRel, nil
			case "entries_init":
				return entriesRel, nil
			default:
				return nil, errors.New("unexpected relation " + name)
			}
		}).AnyTimes()

	spec := &plan.VectorIndexScan{
		Index: &plan.IndexDef{
			IndexAlgoParams: `{"lists":"1","op_type":"vector_l2_ops"}`,
			Parts:           []string{"embedding"},
		},
		SourceTable: &plan.ObjectRef{SchemaName: "db"},
		SourceTableDef: &plan.TableDef{
			Name: "source",
			Cols: []*plan.ColDef{
				{Name: "pk", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "embedding", Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2}},
			},
			Name2ColIndex: map[string]int32{"pk": 0, "embedding": 1},
			Pkey:          &plan.PrimaryKeyDef{PkeyColName: "pk"},
		},
		HiddenTables: []*plan.VectorIndexTableRef{
			{Role: catalog.SystemSI_IVFFLAT_TblType_Metadata, Object: &plan.ObjectRef{ObjName: "metadata_init"}},
			{Role: catalog.SystemSI_IVFFLAT_TblType_Centroids, Object: &plan.ObjectRef{ObjName: "centroids_init"}},
			{Role: catalog.SystemSI_IVFFLAT_TblType_Entries, Object: &plan.ObjectRef{ObjName: "entries_init"}},
		},
		QueryVector:       &plan.Expr{Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2}},
		InitialProbeCount: 1,
		DistanceFunction:  metric.DistFn_L2Distance,
	}
	r := &planReader{
		proc: proc,
		spec: spec,
		req: searchplugin.Request{
			QueryVector:     types.ArrayToBytes([]float32{0, 0}),
			QueryType:       spec.QueryVector.Typ,
			ResultLimit:     2,
			CandidateBudget: 2,
		},
		scanner: &relationScanner{proc: proc},
	}
	out := batch.NewWithSize(2)
	out.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	out.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	defer out.Clean(proc.Mp())
	end, err := r.Read(context.Background(), []string{"pkid", "score"}, nil, proc.Mp(), out)
	require.NoError(t, err)
	require.False(t, end)
	require.Equal(t, []int64{1, 2}, vector.MustFixedColWithTypeCheck[int64](out.Vecs[0]))
	require.Equal(t, []float64{0, 1}, vector.MustFixedColWithTypeCheck[float64](out.Vecs[1]))
	require.Equal(t, 1, metadataReader.closed)
	require.Equal(t, 1, centroidReader.closed)
	require.Equal(t, 1, entriesReader.closed)
}

func TestNewPlanReaderOwnsItsExecutionState(t *testing.T) {
	require.Error(t, func() error {
		_, err := NewPlanReader(nil, nil, searchplugin.Request{})
		return err
	}())

	ctrl := gomock.NewController(t)
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	proc.Base.TxnOperator = mock_frontend.NewMockTxnOperator(ctrl)
	proc.Base.SessionInfo.StorageEngine = mock_frontend.NewMockEngine(ctrl)
	_, err := NewPlanReader(proc, nil, searchplugin.Request{})
	require.ErrorContains(t, err, "missing source or index metadata")
	_, err = NewPlanReader(proc, &plan.VectorIndexScan{
		Index:       &plan.IndexDef{},
		SourceTable: &plan.ObjectRef{},
	}, searchplugin.Request{ResultLimit: 2, CandidateBudget: 1})
	require.ErrorContains(t, err, "candidate budget is smaller")
	reader, err := NewPlanReader(proc, &plan.VectorIndexScan{
		Index:       &plan.IndexDef{},
		SourceTable: &plan.ObjectRef{},
	}, searchplugin.Request{Identity: searchplugin.ScanIdentity{
		PartitionCount: 2,
		PartitionIndex: 1,
	}})
	require.NoError(t, err)
	r := reader.(*planReader)
	require.False(t, r.scanner.ownsInMemory)
	r.SetOrderBy(nil)
	require.Nil(t, r.GetOrderBy())
	r.SetIndexParam(nil)
	r.SetFilterZM(objectio.ZoneMap{})
	require.NoError(t, r.Close())

	publisherID := uint32(42)
	reader, err = NewPlanReader(proc, &plan.VectorIndexScan{
		Index:       &plan.IndexDef{},
		SourceTable: &plan.ObjectRef{PubInfo: &plan.PubInfo{TenantId: 42}},
	}, searchplugin.Request{Identity: searchplugin.ScanIdentity{
		PhysicalAccountID: &publisherID,
		PartitionCount:    1,
	}})
	require.NoError(t, err)
	require.Equal(t, uint32(42), *reader.(*planReader).scanner.accountID)
	snapshotTS := &timestamp.Timestamp{PhysicalTime: 8}
	snapshotPublisherID := uint32(3)
	snapshotReader, err := NewPlanReader(proc, &plan.VectorIndexScan{
		Index:       &plan.IndexDef{},
		SourceTable: &plan.ObjectRef{PubInfo: &plan.PubInfo{TenantId: 3}},
		ScanSnapshot: &plan.Snapshot{
			TS:     &timestamp.Timestamp{PhysicalTime: 8},
			Tenant: &plan.SnapshotTenant{TenantID: 99},
		},
	}, searchplugin.Request{Identity: searchplugin.ScanIdentity{
		PhysicalAccountID: &snapshotPublisherID,
		Snapshot: &plan.Snapshot{
			TS:     snapshotTS,
			Tenant: &plan.SnapshotTenant{TenantID: 99},
		},
		PartitionCount: 1,
	}})
	require.NoError(t, err)
	snapshotPlanReader := snapshotReader.(*planReader)
	require.Equal(t, uint32(3), *snapshotPlanReader.scanner.accountID)
	require.Equal(t, int64(8), snapshotPlanReader.scanner.snapshot.TS.PhysicalTime)
}
