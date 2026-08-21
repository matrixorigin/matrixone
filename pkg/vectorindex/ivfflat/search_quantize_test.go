// Copyright 2025 Matrix Origin
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

package ivfflat

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// mockQuantizeBoundsResult builds a (key, val) result mirroring the single
// `WHERE key IN ('quantize_min','quantize_max')` query loadQuantizeBounds issues.
func mockQuantizeBoundsResult(m *mpool.MPool, qmin, qmax float64) executor.Result {
	bat := batch.NewWithSize(2)
	keyVec := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendBytes(keyVec, []byte(catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin), false, m)
	_ = vector.AppendBytes(keyVec, []byte(catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax), false, m)
	valVec := vector.NewVec(types.T_float64.ToType())
	_ = vector.AppendFixed(valVec, qmin, false, m)
	_ = vector.AppendFixed(valVec, qmax, false, m)
	bat.Vecs[0] = keyVec
	bat.Vecs[1] = valVec
	bat.SetRowCount(2)
	return executor.Result{Mp: m, Batches: []*batch.Batch{bat}}
}

func mockRawQuantizeMetadataResult(m *mpool.MPool) executor.Result {
	bat := batch.NewWithSize(2)
	keyVec := vector.NewVec(types.T_varchar.ToType())
	valVec := vector.NewVec(types.T_varchar.ToType())
	for _, row := range [][2]string{
		{"clustering_start", "2026-08-18 12:34:56"},
		{catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin, "-2"},
		{catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax, "6"},
	} {
		_ = vector.AppendBytes(keyVec, []byte(row[0]), false, m)
		_ = vector.AppendBytes(valVec, []byte(row[1]), false, m)
	}
	bat.Vecs[0] = keyVec
	bat.Vecs[1] = valVec
	bat.SetRowCount(3)
	return executor.Result{Mp: m, Batches: []*batch.Batch{bat}}
}

func TestLoadQuantizeBounds(t *testing.T) {
	defer func() { runSql = sqlexec.RunSql }()

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	var tblcfg vectorindex.IndexTableConfig

	const qmin, qmax = -2.0, 6.0

	// both bounds present → params derived per element type
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return mockQuantizeBoundsResult(m, qmin, qmax), nil
	}
	for _, vt := range []types.T{types.T_array_int8, types.T_array_uint8} {
		idx := &IvfflatSearchIndex[float32]{QuantMul: 1, QuantAdd: 0}
		require.NoError(t, idx.loadQuantizeBounds(sqlproc, tblcfg, vt))

		wantMul, wantAdd := quantizer.Int8Params(qmin, qmax)
		if vt == types.T_array_uint8 {
			wantMul, wantAdd = quantizer.Uint8Params(qmin, qmax)
		}
		require.Equal(t, wantMul, idx.QuantMul)
		require.Equal(t, wantAdd, idx.QuantAdd)
	}

	// bounds absent → params left at identity (1,0)
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, nil
	}
	idx := &IvfflatSearchIndex[float32]{QuantMul: 1, QuantAdd: 0}
	require.NoError(t, idx.loadQuantizeBounds(sqlproc, tblcfg, types.T_array_int8))
	require.Equal(t, 1.0, idx.QuantMul)
	require.Equal(t, 0.0, idx.QuantAdd)

	// sql error propagates
	runSql = mock_runSql_parser_error
	require.Error(t, (&IvfflatSearchIndex[float32]{}).loadQuantizeBounds(sqlproc, tblcfg, types.T_array_int8))
}

func TestLoadQuantizeBoundsDirectScanCarriesTypedFilter(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	scanner := &scriptedRelationScanner{t: t}
	scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
		require.Equal(t, int32(1), req.PartitionCount)
		require.NotNil(t, req.Filter)
		require.Equal(t, "or", req.Filter.GetF().Func.ObjName)
		// Relation scans return the metadata table's raw varchar values. Keep an
		// unrelated timestamp row in the batch to reproduce the CI failure even
		// if a scanner implementation does not push the typed filter down.
		return mockRawQuantizeMetadataResult(m)
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RelationScanner = scanner
	idx := &IvfflatSearchIndex[float32]{QuantMul: 1}

	require.NoError(t, idx.loadQuantizeBounds(sqlproc,
		vectorindex.IndexTableConfig{DbName: "db", MetadataTable: "meta"}, types.T_array_int8))
	wantMul, wantAdd := quantizer.Int8Params(-2, 6)
	require.Equal(t, wantMul, idx.QuantMul)
	require.Equal(t, wantAdd, idx.QuantAdd)
	require.Len(t, scanner.requests, 1)
}

func TestDirectCentroidLoadFeedsRankAndProbeSearch(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	scanner := &scriptedRelationScanner{t: t}
	scanner.run = func(req sqlexec.RelationScanRequest) executor.Result {
		require.Equal(t, "centroids", req.Table)
		require.Equal(t, int32(1), req.PartitionCount)
		require.NotNil(t, req.Filter)
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[2] = vector.NewVec(types.New(types.T_array_float32, 2, 0))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(7), false, mp))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], int64(0), false, mp))
		require.NoError(t, vector.AppendArray(bat.Vecs[2], []float32{0, 0}, false, mp))
		bat.SetRowCount(1)
		return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RelationScanner = scanner
	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Lists = 1
	idxcfg.Ivfflat.Version = 7
	idxcfg.Ivfflat.Dimensions = 2
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2sqDistance)
	idxcfg.Ivfflat.VectorType = int32(types.T_array_float32)
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", IndexTable: "centroids"}
	idx := &IvfflatSearchIndex[float32]{}

	require.NoError(t, idx.LoadIndex(sqlproc, idxcfg, tblcfg, 1))
	t.Cleanup(idx.Destroy)
	require.Equal(t, int64(7), idx.Version)
	require.NotNil(t, idx.Centroids)
	ranked, err := idx.rankCentroids(sqlproc, []float32{0, 0}, idxcfg)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, ranked)
	probed, err := idx.findCentroids(sqlproc, []float32{0, 0}, idxcfg, 4, 1)
	require.NoError(t, err)
	require.Equal(t, []int64{0}, probed)
}

func TestEntryQueryBytesPreservesTheEntriesPhysicalType(t *testing.T) {
	query := []float32{1.25, -2.5}
	for _, typ := range []types.T{
		types.T_array_float32,
		types.T_array_bf16,
		types.T_array_float16,
		types.T_array_int8,
		types.T_array_uint8,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			cfg := vectorindex.IndexConfig{}
			cfg.Ivfflat.VectorType = int32(typ)
			bytes, gotType, err := (&IvfflatSearchIndex[float32]{QuantMul: 2, QuantAdd: 1}).entryQueryBytes(cfg, query)
			require.NoError(t, err)
			require.NotEmpty(t, bytes)
			require.Equal(t, int32(typ), gotType.Id)
			require.Equal(t, int32(len(query)), gotType.Width)
		})
	}

	cfg := vectorindex.IndexConfig{}
	cfg.Ivfflat.VectorType = int32(types.T_array_float64)
	_, _, err := (&IvfflatSearchIndex[float32]{}).entryQueryBytes(cfg, query)
	require.ErrorContains(t, err, "cannot encode []float32 query for entries vector type VECF64")
}

func TestCentroidAndExactSearchHelpersHandleBoundaryStates(t *testing.T) {
	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Lists = 2
	idx := &IvfflatSearchIndex[float32]{}
	ranked, err := idx.rankCentroids(nil, []float32{1, 2}, idxcfg)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, ranked)
	probed, err := idx.findCentroids(nil, []float32{1, 2}, idxcfg, 3, 1)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, probed)

	cursor := &vectorindex.IvfSearchCursor{}
	require.Nil(t, buildActiveCentroidIDs(cursor, 2))
	require.True(t, cursor.Exhausted)
	cursor = &vectorindex.IvfSearchCursor{RankedCentroidIDs: []int64{3, 4, 5}}
	require.Equal(t, []int64{3, 4}, buildActiveCentroidIDs(cursor, 2))
	require.False(t, cursor.Exhausted)
	cursor.NextBucketOffset = 3
	require.Nil(t, buildActiveCentroidIDs(cursor, 2))
	require.True(t, cursor.Exhausted)

	keys, distances, data, nulls := sortAndLimitExactResults(
		[]any{int64(3), int64(1), int64(2)}, []float64{3, 1, 2}, []string{"payload"},
		map[string][]any{"payload": {"c", "a", "b"}}, map[string][]bool{"payload": {false, true, false}}, 2)
	require.Equal(t, []any{int64(1), int64(2)}, keys)
	require.Equal(t, []float64{1, 2}, distances)
	require.Equal(t, []any{"a", "b"}, data["payload"])
	require.Equal(t, []bool{true, false}, nulls["payload"])

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	require.Equal(t, uint(3), exactResultLimit(sqlproc, 3))
	sqlproc.IndexReaderParam = &plan.IndexReaderParam{Limit: ivfUint64Expr(9)}
	require.Equal(t, uint(9), exactResultLimit(sqlproc, 3))
}

func TestIvfflatSearchLifecycleWrappers(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	scanner := &scriptedRelationScanner{t: t}
	scanner.run = func(sqlexec.RelationScanRequest) executor.Result {
		return executor.Result{Mp: mp}
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RelationScanner = scanner
	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Lists = 1
	idxcfg.Ivfflat.VectorType = int32(types.T_array_float32)
	s := NewIvfflatSearch[float32](idxcfg, vectorindex.IndexTableConfig{IndexTable: "centroids"})

	_, _, err := s.Search(sqlproc, "not-a-vector", vectorindex.RuntimeConfig{})
	require.ErrorContains(t, err, "query not match with index type")
	contains, err := s.Contains(99)
	require.NoError(t, err)
	require.True(t, contains)
	require.ErrorContains(t, s.SearchInto(sqlproc, nil, vectorindex.RuntimeConfig{}, nil), "SearchInto not supported")
	require.NoError(t, s.Load(sqlproc))
	require.NotNil(t, s.Index)
	s.Destroy()
	require.Nil(t, s.Index)
}

// TestScoreFromQuantized verifies that the distance the entries query measures in
// the quantized domain is rescaled back to source units. The regression: with
// QUANTIZATION='int8' over [0,1] (mul=255), a source L2 of 0.5 was reported near
// 128 (mul*0.5) and range predicates in source units were off by mul^2.
func TestScoreFromQuantized(t *testing.T) {
	const sourceL2 = 0.5
	const sourceSq = sourceL2 * sourceL2 // 0.25

	// Identity quantizer (f32/f64/bf16/f16): QuantMul==1 → pass through, with the
	// squared->L2 conversion applied when the user asked for l2_distance.
	id := &IvfflatSearchIndex[float32]{QuantMul: 1, QuantAdd: 0}
	require.InDelta(t, sourceL2, id.scoreFromQuantized(sourceSq, metric.DistFn_L2Distance, metric.Metric_L2sqDistance), 1e-9)
	require.InDelta(t, sourceSq, id.scoreFromQuantized(sourceSq, metric.DistFn_L2sqDistance, metric.Metric_L2sqDistance), 1e-9)

	// int8 quantizer for [0,1] → mul=255 (the reviewer's example). The entries
	// query returns the squared L2 in the quantized domain = mul^2 * sourceSq.
	mul, _ := quantizer.Int8Params(0, 1)
	require.Equal(t, 255.0, mul)
	q := &IvfflatSearchIndex[float32]{QuantMul: mul, QuantAdd: -128}
	quantizedSq := mul * mul * sourceSq

	// Returned distance (user asked l2_distance) must be the SOURCE L2 (0.5),
	// NOT ~mul*0.5 (~127.5) as before the rescale.
	got := q.scoreFromQuantized(quantizedSq, metric.DistFn_L2Distance, metric.Metric_L2sqDistance)
	require.InDelta(t, sourceL2, got, 1e-6)
	require.Less(t, got, 1.0, "regression: quantized L2 must not be reported near mul*0.5")

	// Range predicate in source SQUARED units (user asked l2_distance_sq).
	gotSq := q.scoreFromQuantized(quantizedSq, metric.DistFn_L2sqDistance, metric.Metric_L2sqDistance)
	require.InDelta(t, sourceSq, gotSq, 1e-6)
}

func TestIncludeSearchSQLUsesEntriesVectorType(t *testing.T) {
	query := []float32{-2.25, 0.5, 3.75}
	mul, add := 10.0, -3.0

	tests := []struct {
		name       string
		vectorType types.T
		quantMul   float64
		quantAdd   float64
		decoder    string
		payload    string
	}{
		{
			name:       "bf16",
			vectorType: types.T_array_bf16,
			decoder:    "vecbf16_from_base64",
			payload:    types.ArrayToBase64(types.Float32ToBF16Slice(query)),
		},
		{
			name:       "float16",
			vectorType: types.T_array_float16,
			decoder:    "vecf16_from_base64",
			payload:    types.ArrayToBase64(types.Float32ToFloat16Slice(query)),
		},
		{
			name:       "int8 narrow",
			vectorType: types.T_array_int8,
			quantMul:   1,
			decoder:    "vecint8_from_base64",
			payload:    types.ArrayToBase64(quantizer.ApplyInt8(query, 1, 0)),
		},
		{
			name:       "uint8 narrow",
			vectorType: types.T_array_uint8,
			quantMul:   1,
			decoder:    "vecuint8_from_base64",
			payload:    types.ArrayToBase64(quantizer.ApplyUint8(query, 1, 0)),
		},
		{
			name:       "int8 quantized f32 or f64 base",
			vectorType: types.T_array_int8,
			quantMul:   mul,
			quantAdd:   add,
			decoder:    "vecint8_from_base64",
			payload:    types.ArrayToBase64(quantizer.ApplyInt8(query, mul, add)),
		},
		{
			name:       "uint8 quantized f32 or f64 base",
			vectorType: types.T_array_uint8,
			quantMul:   mul,
			quantAdd:   add,
			decoder:    "vecuint8_from_base64",
			payload:    types.ArrayToBase64(quantizer.ApplyUint8(query, mul, add)),
		},
	}

	tblcfg := vectorindex.IndexTableConfig{DbName: "db", EntriesTable: "entries"}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idxcfg := vectorindex.IndexConfig{}
			idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2sqDistance)
			idxcfg.Ivfflat.VectorType = int32(tc.vectorType)
			idx := &IvfflatSearchIndex[float32]{QuantMul: tc.quantMul, QuantAdd: tc.quantAdd}
			wantExpr := tc.decoder + "('" + tc.payload + "')"

			roundSQL, err := idx.buildSearchRoundSQL(idxcfg, tblcfg, query, []int64{1, 2}, 7, []string{"payload"}, "", 5)
			require.NoError(t, err)
			require.Contains(t, roundSQL, wantExpr)
			require.NotContains(t, roundSQL, "vecf32_from_base64")

			exactSQL, err := idx.buildExactSearchSQL(idxcfg, tblcfg, query, 7, "11,12", []string{"payload"}, "")
			require.NoError(t, err)
			require.Contains(t, exactSQL, wantExpr)
			require.NotContains(t, exactSQL, "vecf32_from_base64")
			require.Contains(t, exactSQL, "`__mo_index_pri_col` IN (11,12)")
		})
	}

	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)
	idxcfg.Ivfflat.VectorType = int32(types.T_array_float64)
	idx64 := &IvfflatSearchIndex[float64]{}
	f64SQL, err := idx64.buildSearchRoundSQL(idxcfg, tblcfg, []float64{1, 2}, []int64{1}, 7, nil, "", 2)
	require.NoError(t, err)
	require.Contains(t, f64SQL, "vecf64_from_base64")

	_, err = (&IvfflatSearchIndex[float32]{}).entryQueryExpression(idxcfg, query)
	require.ErrorContains(t, err, "cannot encode []float32 query for entries vector type VECF64")
}

func TestIncludeExactPkSearchAppliesQueryQuantizerAndRestoresDistance(t *testing.T) {
	oldRunSQL := runSql
	defer func() { runSql = oldRunSQL }()

	const (
		mul = 10.0
		add = -5.0
	)
	query := []float32{1, 2, 3}
	var capturedSQL string

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		capturedSQL = sql
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, m))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], float64(400), false, m))
		require.NoError(t, vector.AppendBytes(bat.Vecs[2], []byte("covered"), false, m))
		bat.SetRowCount(1)
		return executor.Result{Mp: m, Batches: []*batch.Batch{bat}}, nil
	}

	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2sqDistance)
	idxcfg.Ivfflat.VectorType = int32(types.T_array_int8)
	tblcfg := vectorindex.IndexTableConfig{
		DbName:         "db",
		EntriesTable:   "entries",
		IncludeColumns: []string{"payload"},
	}
	idx := &IvfflatSearchIndex[float32]{Version: 7, QuantMul: mul, QuantAdd: add}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.ExactPkFilter = "42"
	includeResult := &vectorindex.IvfIncludeResult{}
	rt := vectorindex.RuntimeConfig{
		Limit:                   3,
		Probe:                   1,
		OrigFuncName:            metric.DistFn_L2Distance,
		RequestedIncludeColumns: []string{"payload"},
		IncludeResult:           includeResult,
		SearchCursor:            &vectorindex.IvfSearchCursor{},
		SearchRoundLimit:        3,
	}

	keys, distances, err := idx.Search(sqlproc, idxcfg, tblcfg, query, rt, 1)
	require.NoError(t, err)
	require.Equal(t, []any{int64(42)}, keys)
	require.Len(t, distances, 1)
	require.InDelta(t, 2.0, distances[0], 1e-9)
	require.Equal(t, []any{[]byte("covered")}, includeResult.Data["payload"])

	wantPayload := types.ArrayToBase64(quantizer.ApplyInt8(query, mul, add))
	require.Contains(t, capturedSQL, "vecint8_from_base64('"+wantPayload+"')")
	require.NotContains(t, capturedSQL, "vecf32_from_base64")
	require.Contains(t, capturedSQL, "`__mo_index_pri_col` IN (42)")
	require.NotContains(t, capturedSQL, "ORDER BY vec_dist")
	require.True(t, rt.SearchCursor.Exhausted)
}
