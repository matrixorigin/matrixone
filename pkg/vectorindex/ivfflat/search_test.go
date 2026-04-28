// Copyright 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

// give blob
func mock_runSql(
	sqlproc *sqlexec.SqlProcess,
	sql string,
) (executor.Result, error) {
	return executor.Result{}, nil
}

func mock_runSql_parser_error(
	sqlproc *sqlexec.SqlProcess,
	sql string,
) (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalErrorNoCtx("sql parser error")
}

func TestIvfSearchRace(t *testing.T) {

	runSql = mock_runSql

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(sqlproc, idxcfg, tblcfg, v, rt, 4)
	require.Nil(t, err)

}

func TestIvfSearchParserError(t *testing.T) {

	runSql = mock_runSql_parser_error

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(sqlproc, idxcfg, tblcfg, v, rt, 4)
	require.NotNil(t, err)
}

func TestIvfSearchSQLIncludesRequestedColumnsAndPushdown(t *testing.T) {
	oldRunSQL := runSql
	defer func() {
		runSql = oldRunSQL
	}()

	var capturedSQL string
	runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		capturedSQL = sql
		require.Nil(t, sqlproc.IvfBloomFilter)

		bat := batch.NewWithSize(4)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[3] = vector.NewVec(types.T_bool.ToType())

		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, sqlproc.Proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], float64(1.25), false, sqlproc.Proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], int32(7), false, sqlproc.Proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[3], true, false, sqlproc.Proc.Mp()))
		bat.SetRowCount(1)

		return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{bat}}, nil
	}

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	tblcfg := vectorindex.IndexTableConfig{
		DbName:             "test_db",
		EntriesTable:       "test_entries",
		IncludeColumns:     []string{"rank", "flag"},
		IncludeColumnTypes: []int32{int32(types.T_int32), int32(types.T_bool)},
	}

	rt := vectorindex.RuntimeConfig{
		Limit:                   5,
		Probe:                   1,
		OrigFuncName:            "l2_distance",
		RequestedIncludeColumns: []string{"rank", "flag"},
		PushdownFilterSQL:       "`__mo_index_include_flag` = true",
		IncludeResult:           &vectorindex.IvfIncludeResult{},
		SearchCursor:            &vectorindex.IvfSearchCursor{},
		SearchRoundLimit:        3,
	}

	idx := &IvfflatSearchIndex[float32]{Version: 7}
	sqlproc := sqlexec.NewSqlProcess(proc)

	keys, distances, err := idx.Search(sqlproc, idxcfg, tblcfg, []float32{0, 0, 0}, rt, 4)
	require.NoError(t, err)

	require.Contains(t, capturedSQL, "__mo_index_include_rank")
	require.Contains(t, capturedSQL, "__mo_index_include_flag")
	require.Contains(t, capturedSQL, "AND `__mo_index_include_flag` = true")
	require.Contains(t, capturedSQL, "vecf32_from_base64")
	require.Contains(t, capturedSQL, "LIMIT 3")

	require.Equal(t, []any{int64(42)}, keys.([]any))
	require.Equal(t, []float64{1.25}, distances)
	require.Equal(t, []string{"rank", "flag"}, rt.IncludeResult.ColNames)
	require.Equal(t, []any{int32(7)}, rt.IncludeResult.Data["rank"])
	require.Equal(t, []any{true}, rt.IncludeResult.Data["flag"])
	require.Equal(t, uint(0), rt.SearchCursor.NextBucketOffset)
	require.Equal(t, uint(1), rt.SearchCursor.CurrentBucketCount)
	require.Equal(t, uint(1), rt.SearchCursor.Round)
}

func TestIvfSearchSequentialCallsDoNotLeakQueryScopedRuntimeState(t *testing.T) {
	oldRunSQL := runSql
	defer func() {
		runSql = oldRunSQL
	}()

	var capturedSQL []string
	runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		capturedSQL = append(capturedSQL, sql)

		switch len(capturedSQL) {
		case 1:
			bat := batch.NewWithSize(3)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
			bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())

			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(7), false, sqlproc.Proc.Mp()))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], float64(0.25), false, sqlproc.Proc.Mp()))
			require.NoError(t, vector.AppendFixed(bat.Vecs[2], int32(9), false, sqlproc.Proc.Mp()))
			bat.SetRowCount(1)
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{bat}}, nil
		case 2:
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())

			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(8), false, sqlproc.Proc.Mp()))
			require.NoError(t, vector.AppendFixed(bat.Vecs[1], float64(0.5), false, sqlproc.Proc.Mp()))
			bat.SetRowCount(1)
			return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{bat}}, nil
		default:
			return executor.Result{}, moerr.NewInternalErrorNoCtx("unexpected extra Search call")
		}
	}

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	tblcfg := vectorindex.IndexTableConfig{
		DbName:             "test_db",
		EntriesTable:       "test_entries",
		IncludeColumns:     []string{"rank"},
		IncludeColumnTypes: []int32{int32(types.T_int32)},
	}

	idx := &IvfflatSearchIndex[float32]{Version: 9}
	sqlproc := sqlexec.NewSqlProcess(proc)

	rt1 := vectorindex.RuntimeConfig{
		Limit:                   4,
		Probe:                   1,
		OrigFuncName:            "l2_distance",
		RequestedIncludeColumns: []string{"rank"},
		PushdownFilterSQL:       "`__mo_index_include_rank` > 3",
		IncludeResult:           &vectorindex.IvfIncludeResult{},
		SearchCursor:            &vectorindex.IvfSearchCursor{},
		SearchRoundLimit:        2,
	}
	keys1, distances1, err := idx.Search(sqlproc, idxcfg, tblcfg, []float32{0, 0, 0}, rt1, 4)
	require.NoError(t, err)

	rt2 := vectorindex.RuntimeConfig{
		Limit:         4,
		Probe:         1,
		OrigFuncName:  "l2_distance",
		IncludeResult: &vectorindex.IvfIncludeResult{},
		SearchCursor:  &vectorindex.IvfSearchCursor{},
	}
	keys2, distances2, err := idx.Search(sqlproc, idxcfg, tblcfg, []float32{0, 0, 0}, rt2, 4)
	require.NoError(t, err)

	require.Len(t, capturedSQL, 2)
	require.Contains(t, capturedSQL[0], "__mo_index_include_rank")
	require.Contains(t, capturedSQL[0], "AND `__mo_index_include_rank` > 3")
	require.NotContains(t, capturedSQL[1], "__mo_index_include_rank")
	require.NotContains(t, capturedSQL[1], " > 3")

	require.Equal(t, []any{int64(7)}, keys1.([]any))
	require.Equal(t, []float64{0.25}, distances1)
	require.Equal(t, []string{"rank"}, rt1.IncludeResult.ColNames)
	require.Equal(t, []any{int32(9)}, rt1.IncludeResult.Data["rank"])
	require.Equal(t, uint(1), rt1.SearchCursor.Round)

	require.Equal(t, []any{int64(8)}, keys2.([]any))
	require.Equal(t, []float64{0.5}, distances2)
	require.Empty(t, rt2.IncludeResult.ColNames)
	require.Empty(t, rt2.IncludeResult.Data)
	require.Equal(t, uint(1), rt2.SearchCursor.Round)
}

func TestGetBloomFilterUsesRuntimeFilterPayloadForExactPk(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{{UseBloomFilter: true}}

	keyVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(keyVec, int64(11), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(keyVec, int64(22), false, proc.Mp()))

	rawPayload, err := keyVec.MarshalBinary()
	require.NoError(t, err)
	sqlproc.IvfRuntimeFilterData = rawPayload

	idx := &IvfflatSearchIndex[float32]{Version: 3}
	err = idx.getBloomFilter(sqlproc, vectorindex.IndexConfig{}, vectorindex.IndexTableConfig{}, []int64{7})
	require.NoError(t, err)
	require.Equal(t, "11,22", sqlproc.ExactPkFilter)
	require.Nil(t, sqlproc.IvfBloomFilter)
}

func TestIvfSearchIncludeModeConvertsRuntimePayloadBeforeRunSql(t *testing.T) {
	oldRunSQL := runSql
	defer func() {
		runSql = oldRunSQL
	}()

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.RuntimeFilterSpecs = []*plan.RuntimeFilterSpec{{UseBloomFilter: true}}

	keyVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(keyVec, int64(42), false, proc.Mp()))
	rawPayload, err := keyVec.MarshalBinary()
	require.NoError(t, err)

	var capturedSQL string
	runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		capturedSQL = sql
		require.Nil(t, sqlproc.IvfBloomFilter)
		require.Equal(t, "42", sqlproc.ExactPkFilter)

		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_int32.ToType())

		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, sqlproc.Proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[1], float64(0.125), false, sqlproc.Proc.Mp()))
		require.NoError(t, vector.AppendFixed(bat.Vecs[2], int32(9), false, sqlproc.Proc.Mp()))
		bat.SetRowCount(1)

		return executor.Result{Mp: sqlproc.Proc.Mp(), Batches: []*batch.Batch{bat}}, nil
	}

	idxcfg := vectorindex.IndexConfig{}
	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	tblcfg := vectorindex.IndexTableConfig{
		DbName:             "test_db",
		EntriesTable:       "test_entries",
		IncludeColumns:     []string{"rank"},
		IncludeColumnTypes: []int32{int32(types.T_int32)},
	}

	rt := vectorindex.RuntimeConfig{
		Limit:                   3,
		Probe:                   1,
		OrigFuncName:            "l2_distance",
		RequestedIncludeColumns: []string{"rank"},
		IncludeResult:           &vectorindex.IvfIncludeResult{},
		SearchCursor: &vectorindex.IvfSearchCursor{
			RankedCentroidIDs: []int64{101},
		},
		SearchRoundLimit: 3,
		BloomFilter:      rawPayload,
	}

	idx := &IvfflatSearchIndex[float32]{Version: 7}
	keys, distances, err := idx.Search(sqlproc, idxcfg, tblcfg, []float32{0, 0, 0}, rt, 4)
	require.NoError(t, err)
	require.Equal(t, []any{int64(42)}, keys.([]any))
	require.Equal(t, []float64{0.125}, distances)
	require.Equal(t, []string{"rank"}, rt.IncludeResult.ColNames)
	require.Equal(t, []any{int32(9)}, rt.IncludeResult.Data["rank"])
	require.Contains(t, capturedSQL, "`"+catalog.SystemSI_IVFFLAT_TblCol_Entries_pk+"`")
	require.Contains(t, capturedSQL, "IN (42)")
	require.NotContains(t, capturedSQL, "`"+catalog.SystemSI_IVFFLAT_TblCol_Entries_id+"` IN")
	require.NotContains(t, capturedSQL, "ORDER BY vec_dist")
	require.NotContains(t, capturedSQL, "LIMIT 3")
	require.True(t, rt.SearchCursor.Exhausted)
}

func TestBuildActiveCentroidIDsUsesNonOverlappingBucketSlices(t *testing.T) {
	cursor := &vectorindex.IvfSearchCursor{
		RankedCentroidIDs: []int64{11, 22, 33, 44, 55},
	}

	first := buildActiveCentroidIDs(cursor, 2)
	require.Equal(t, []int64{11, 22}, first)
	require.Equal(t, uint(0), cursor.NextBucketOffset)
	require.Equal(t, uint(2), cursor.CurrentBucketCount)
	require.False(t, cursor.Exhausted)

	cursor.Round = 1
	cursor.NextBucketOffset = 2
	cursor.CurrentBucketCount = 2
	second := buildActiveCentroidIDs(cursor, 2)
	require.Equal(t, []int64{33, 44}, second)
	require.Equal(t, uint(2), cursor.NextBucketOffset)
	require.Equal(t, uint(2), cursor.CurrentBucketCount)
	require.False(t, cursor.Exhausted)

	cursor.Round = 2
	cursor.NextBucketOffset = 4
	cursor.CurrentBucketCount = 2
	third := buildActiveCentroidIDs(cursor, 2)
	require.Equal(t, []int64{55}, third)
	require.Equal(t, uint(4), cursor.NextBucketOffset)
	require.Equal(t, uint(1), cursor.CurrentBucketCount)
	require.True(t, cursor.Exhausted)
}
