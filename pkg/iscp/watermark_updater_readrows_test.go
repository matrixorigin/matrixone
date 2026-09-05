// Copyright 2024 Matrix Origin
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

package iscp

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestGetTableIDCountsRowsAcrossBatches(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	result, mp := newTableIDResult(t, [][]uint64{{10}, {20}}, [][]uint64{{100}, {200}})
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return result, nil
	}

	_, _, err := getTableID(context.Background(), "", nil, 0, "db", "tbl")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid rows 2")
}

func TestMarshalJobSpecPreservesSQLCharacters(t *testing.T) {
	spec, err := MarshalJobSpec(&JobSpec{ConsumerInfo: ConsumerInfo{
		RefreshSQL: "select date_trunc('minute', ts) where status >= 500",
	}})
	require.NoError(t, err)
	require.Contains(t, spec, "status >= 500")
	require.NotContains(t, spec, `\u003e`)

	byteJSON, err := types.ParseStringToByteJson(spec)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(byteJSON)
	require.NoError(t, err)
	decoded, err := UnmarshalJobSpec(encoded)
	require.NoError(t, err)
	require.Equal(t, "select date_trunc('minute', ts) where status >= 500", decoded.RefreshSQL)
}

func TestMaterializedViewJobReferencesSingleAndMultipleSources(t *testing.T) {
	legacy := &JobSpec{ConsumerInfo: ConsumerInfo{
		ConsumerType: int8(ConsumerType_MaterializedView),
		SrcTable:     TableInfo{TableID: 10},
	}}
	require.True(t, materializedViewJobReferencesSource(legacy, 10))
	require.False(t, materializedViewJobReferencesSource(legacy, 11))

	multi := &JobSpec{ConsumerInfo: ConsumerInfo{
		ConsumerType: int8(ConsumerType_MaterializedView),
		SrcTables:    []TableInfo{{TableID: 10}, {TableID: 11}},
	}}
	require.True(t, materializedViewJobReferencesSource(multi, 10))
	require.True(t, materializedViewJobReferencesSource(multi, 11))

	indexJob := &JobSpec{ConsumerInfo: ConsumerInfo{
		ConsumerType: int8(ConsumerType_IndexSync), SrcTable: TableInfo{TableID: 10},
	}}
	require.False(t, materializedViewJobReferencesSource(indexJob, 10))
}

func TestMaterializedViewJobMatchesTargetAvoidsJobNameCollisions(t *testing.T) {
	spec := &JobSpec{ConsumerInfo: ConsumerInfo{
		ConsumerType: int8(ConsumerType_MaterializedView), DBName: "a_b", TableName: "c",
	}}
	require.True(t, materializedViewJobMatchesTarget(spec, "a_b", "c"))
	require.True(t, materializedViewJobMatchesTarget(spec, "A_B", "C"))
	require.False(t, materializedViewJobMatchesTarget(spec, "a", "b_c"))
	spec.ConsumerType = int8(ConsumerType_IndexSync)
	require.False(t, materializedViewJobMatchesTarget(spec, "a_b", "c"))
}

func TestMarkJobsErrorBySourceTableAllowsMissingISCPLog(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() { ExecWithResult = oldExecWithResult }()

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(42))
	ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return executor.Result{}, moerr.NewNoSuchTableNoCtx("mo_catalog", catalog.MO_ISCP_LOG)
	}
	require.NoError(t, MarkJobsErrorBySourceTable(ctx, "", nil, 10, "source table was dropped"))

	expected := errors.New("executor unavailable")
	ExecWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return executor.Result{}, expected
	}
	require.ErrorIs(t, MarkJobsErrorBySourceTable(ctx, "", nil, 10, "source table was dropped"), expected)
}

func TestMarkJobsErrorBySourceTableUpdatesEveryMatchingGeneration(t *testing.T) {
	oldExecWithResult := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExecWithResult })

	matching := encodeMaterializedViewJobSpec(t, &JobSpec{ConsumerInfo: ConsumerInfo{
		ConsumerType: int8(ConsumerType_MaterializedView),
		SrcTables:    []TableInfo{{TableID: 10}, {TableID: 11}},
	}})
	nonMatching := encodeMaterializedViewJobSpec(t, &JobSpec{ConsumerInfo: ConsumerInfo{
		ConsumerType: int8(ConsumerType_MaterializedView), SrcTable: TableInfo{TableID: 12},
	}})
	result, mp := newMaterializedViewJobsResult(t,
		[]uint64{100, 101, 102}, []string{"mv'job", "other", "invalid"}, []uint64{1, 2, 3},
		[]string{matching, nonMatching, "not-json"})
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	var updates []string
	calls := 0
	ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
		calls++
		if calls == 1 {
			return result, nil
		}
		updates = append(updates, sql)
		return executor.Result{}, nil
	}
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(42))
	require.NoError(t, MarkJobsErrorBySourceTable(ctx, "cn", nil, 11, "source's table was dropped"))
	require.Len(t, updates, 1)
	require.Contains(t, updates[0], "table_id = 100")
	require.Contains(t, updates[0], "job_name = 'mv''job'")
	require.Contains(t, updates[0], "source''s table was dropped")
}

func TestUnregisterMaterializedViewUsesTargetIdentity(t *testing.T) {
	oldExecWithResult := ExecWithResult
	t.Cleanup(func() { ExecWithResult = oldExecWithResult })

	matching := encodeMaterializedViewJobSpec(t, &JobSpec{ConsumerInfo: ConsumerInfo{
		ConsumerType: int8(ConsumerType_MaterializedView), DBName: "db", TableName: "mv",
	}})
	collision := encodeMaterializedViewJobSpec(t, &JobSpec{ConsumerInfo: ConsumerInfo{
		ConsumerType: int8(ConsumerType_MaterializedView), DBName: "d", TableName: "b_mv",
	}})
	result, mp := newMaterializedViewJobsResult(t,
		[]uint64{100, 101, 102}, []string{"materialized_view_db_mv", "materialized_view_db_mv", "invalid"}, []uint64{1, 2, 3},
		[]string{matching, collision, "not-json"})
	t.Cleanup(func() {
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	var updates []string
	calls := 0
	ExecWithResult = func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
		calls++
		if calls == 1 {
			return result, nil
		}
		updates = append(updates, sql)
		return executor.Result{}, nil
	}
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(42))
	require.NoError(t, unregisterMaterializedView(ctx, "cn", nil, "DB", "MV"))
	require.Len(t, updates, 1)
	require.True(t, strings.Contains(strings.ToLower(updates[0]), "update mo_catalog.mo_iscp_log"))
	require.Contains(t, updates[0], "table_id = 100")
}

func encodeMaterializedViewJobSpec(t *testing.T, spec *JobSpec) string {
	t.Helper()
	raw, err := MarshalJobSpec(spec)
	require.NoError(t, err)
	byteJSON, err := types.ParseStringToByteJson(raw)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(byteJSON)
	require.NoError(t, err)
	return string(encoded)
}

func newMaterializedViewJobsResult(
	t *testing.T,
	tableIDs []uint64,
	names []string,
	jobIDs []uint64,
	specs []string,
) (executor.Result, *mpool.MPool) {
	t.Helper()
	require.Len(t, tableIDs, len(names))
	require.Len(t, tableIDs, len(jobIDs))
	require.Len(t, tableIDs, len(specs))
	mp := mpool.MustNewZero()
	memRes := executor.NewMemResult([]types.Type{
		types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(tableIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 0, tableIDs))
	require.NoError(t, executor.AppendStringRows(memRes, 1, names))
	require.NoError(t, executor.AppendFixedRows(memRes, 2, jobIDs))
	require.NoError(t, executor.AppendStringRows(memRes, 3, specs))
	return memRes.GetResult(), mp
}

func TestUnregisterJobsByDBNameEscapesDatabaseLiteral(t *testing.T) {
	oldExecWithResult := ExecWithResult
	defer func() {
		ExecWithResult = oldExecWithResult
	}()

	mp := mpool.MustNewZero()
	memResult := executor.NewMemResult([]types.Type{types.T_uint64.ToType()}, mp)
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
		mpool.DeleteMPool(mp)
	}()

	var capturedSQL string
	ExecWithResult = func(_ context.Context, sql string, _ string, _ client.TxnOperator) (executor.Result, error) {
		capturedSQL = sql
		return memResult.GetResult(), nil
	}

	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, uint32(7))
	err := unregisterJobsByDBName(ctx, "", nil, `db_'name\path`)

	require.NoError(t, err)
	require.Contains(t, capturedSQL, "account_id = 7")
	require.Contains(t, capturedSQL, `reldatabase = 'db_''name\\path'`)
}

func newTableIDResult(t *testing.T, tableIDBatches, dbIDBatches [][]uint64) (executor.Result, *mpool.MPool) {
	t.Helper()
	require.Len(t, tableIDBatches, len(dbIDBatches))

	mp := mpool.MustNewZero()
	memRes := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_uint64.ToType()}, mp)
	for i := range tableIDBatches {
		require.Len(t, tableIDBatches[i], len(dbIDBatches[i]))
		memRes.NewBatchWithRowCount(len(tableIDBatches[i]))
		require.NoError(t, executor.AppendFixedRows(memRes, 0, tableIDBatches[i]))
		require.NoError(t, executor.AppendFixedRows(memRes, 1, dbIDBatches[i]))
	}
	return memRes.GetResult(), mp
}
