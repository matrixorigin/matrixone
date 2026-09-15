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

package compile

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCheckPitrGranularityRejectsWildcardNoPrimaryKey(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := defines.AttachAccountId(context.Background(), 7)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	exec := &recordingInternalSQLExecutor{mocker: func(sql string) (executor.Result, error) {
		if strings.Contains(sql, catalog.MO_TABLES) {
			result := executor.NewMemResult([]types.Type{
				types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
				types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint32.ToType(), types.T_blob.ToType(),
			}, proc.Mp())
			result.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{1}))
			require.NoError(t, executor.AppendStringRows(result, 1, []string{"without_pk"}))
			require.NoError(t, executor.AppendFixedRows(result, 2, []uint64{1}))
			require.NoError(t, executor.AppendStringRows(result, 3, []string{"db"}))
			require.NoError(t, executor.AppendStringRows(result, 4, []string{""}))
			require.NoError(t, executor.AppendFixedRows(result, 5, []uint32{7}))
			require.NoError(t, executor.AppendBytesRows(result, 6, [][]byte{{}}))
			return result.GetResult(), nil
		}
		result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, proc.Mp())
		result.NewBatchWithRowCount(0)
		return result.GetResult(), nil
	}}
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, exec)
		}
	})

	c := NewCompile("", "", "create cdc", "", "", nil, proc, nil, false, nil, time.Now())
	defer c.Release()
	pts := &cdc.PatternTuples{Pts: []*cdc.PatternTuple{{Source: cdc.PatternTable{
		Database: "db", Table: cdc.CDCPitrGranularity_All,
	}}}}
	err := c.checkPitrGranularity(ctx, pts, "")
	require.Error(t, err)
	require.Len(t, exec.sqls, 2)
	require.Contains(t, exec.sqls[0], "mo_tables")
}

func TestCheckPitrGranularityWildcardExcludeAndPrimaryKey(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := defines.AttachAccountId(context.Background(), 7)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	candidateResult := func(table string) executor.Result {
		result := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint32.ToType(), types.T_blob.ToType()}, proc.Mp())
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []uint64{1}))
		require.NoError(t, executor.AppendStringRows(result, 1, []string{table}))
		require.NoError(t, executor.AppendFixedRows(result, 2, []uint64{1}))
		require.NoError(t, executor.AppendStringRows(result, 3, []string{"db"}))
		require.NoError(t, executor.AppendStringRows(result, 4, []string{""}))
		require.NoError(t, executor.AppendFixedRows(result, 5, []uint32{7}))
		require.NoError(t, executor.AppendBytesRows(result, 6, [][]byte{{}}))
		return result.GetResult()
	}
	validPitrResult := func() executor.Result {
		result := executor.NewMemResult([]types.Type{types.T_uint8.ToType(), types.T_varchar.ToType()}, proc.Mp())
		result.NewBatchWithRowCount(1)
		require.NoError(t, executor.AppendFixedRows(result, 0, []uint8{24}))
		require.NoError(t, executor.AppendStringRows(result, 1, []string{"h"}))
		return result.GetResult()
	}

	for _, tc := range []struct{ name, table, exclude string }{
		{name: "excluded no primary key", table: "without_pk", exclude: `^db\.without_pk$`},
		{name: "visible primary key", table: "with_pk"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &recordingInternalSQLExecutor{mocker: func(sql string) (executor.Result, error) {
				if strings.Contains(sql, catalog.MO_TABLES) {
					return candidateResult(tc.table), nil
				}
				if strings.Contains(sql, catalog.MO_COLUMNS) && tc.table == "with_pk" {
					result := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, proc.Mp())
					result.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendStringRows(result, 0, []string{"id"}))
					return result.GetResult(), nil
				}
				return validPitrResult(), nil
			}}
			rt := moruntime.ServiceRuntime(proc.GetService())
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
			c := NewCompile("", "", "create cdc", "", "", nil, proc, nil, false, nil, time.Now())
			defer c.Release()
			pts := &cdc.PatternTuples{Pts: []*cdc.PatternTuple{{Source: cdc.PatternTable{Database: "db", Table: cdc.CDCPitrGranularity_All}}}}
			require.NoError(t, c.checkPitrGranularity(ctx, pts, tc.exclude))
		})
	}
}

type cdcRecordingSQLExecutor struct {
	queries []string
}

func (e *cdcRecordingSQLExecutor) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, nil
}

func (e *cdcRecordingSQLExecutor) ExecContext(
	_ context.Context, query string, _ ...interface{},
) (sql.Result, error) {
	e.queries = append(e.queries, query)
	return cdcRowsAffectedResult(1), nil
}

func (e *cdcRecordingSQLExecutor) QueryContext(
	context.Context, string, ...interface{},
) (*sql.Rows, error) {
	return nil, nil
}

type cdcRowsAffectedResult int64

func (r cdcRowsAffectedResult) LastInsertId() (int64, error) { return 0, nil }
func (r cdcRowsAffectedResult) RowsAffected() (int64, error) { return int64(r), nil }

func TestCDCCreateTaskOptionsPreservePatternValidationError(t *testing.T) {
	const tables = "db1.t1:db2.t1,db1.t1:db2.t2"
	const expected = "internal error: one db/table: db1.t1 can't be used as multi sources in a cdc task"

	opts := &CDCCreateTaskOptions{}
	err := opts.handleLevel(context.Background(), nil, cdc.CDCPitrGranularity_Table, tables)
	require.EqualError(t, err, expected)
	require.NotContains(t, err.Error(), "invalid level")

	err = opts.handleFrequency(
		context.Background(), nil, cdc.CDCPitrGranularity_Table, "1h", tables,
	)
	require.EqualError(t, err, expected)
	require.NotContains(t, err.Error(), "invalid level")
}

func TestCDCCreateTaskMetadataUsesCapabilityFence(t *testing.T) {
	legacy := (&CDCCreateTaskOptions{TaskId: "legacy"}).BuildTaskMetadata()
	require.Equal(t, task.TaskCode_InitCdc, legacy.Executor)

	stableOpts := fmt.Sprintf(
		`{"%s":"%s"}`,
		cdc.CDCTaskExtraOptions_InitialSnapshotProtocol,
		cdc.CDCInitialSnapshotProtocolStableEpoch,
	)
	stable := (&CDCCreateTaskOptions{
		TaskId:    "stable",
		ExtraOpts: stableOpts,
	}).BuildTaskMetadata()
	require.Equal(t, task.TaskCode_InitCdcStableEpoch, stable.Executor)

	noFull := (&CDCCreateTaskOptions{
		TaskId: "no-full", NoFull: true, ExtraOpts: stableOpts,
	}).BuildTaskMetadata()
	require.Equal(t, task.TaskCode_InitCdc, noFull.Executor)
	lossless := (&CDCCreateTaskOptions{
		TaskId: "no-full-hlc", NoFull: true,
		ExtraOpts: fmt.Sprintf(`{"%s":"%s"}`, cdc.CDCTaskExtraOptions_InitialSnapshotProtocol, cdc.CDCInitialSnapshotProtocolNoFullHLC),
	}).BuildTaskMetadata()
	require.Equal(t, task.TaskCode_InitCdcLosslessStart, lossless.Executor)
}

func TestCDCCreateTaskOptionsSetNoFullStartTS(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	snapshot := timestamp.Timestamp{PhysicalTime: time.Date(2026, 9, 9, 1, 2, 3, 456789000, time.UTC).UnixNano()}

	opts := &CDCCreateTaskOptions{NoFull: true}
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(snapshot)
	setNoFullStartTS(opts, txnOp)
	require.Equal(t, snapshot.DebugString(), opts.StartTs)

	// Explicit StartTs and absent transaction operators do not alter the start.
	opts.StartTs = "2026-09-01T00:00:00Z"
	setNoFullStartTS(opts, txnOp)
	require.Equal(t, "2026-09-01T00:00:00Z", opts.StartTs)
	setNoFullStartTS(&CDCCreateTaskOptions{NoFull: true}, nil)

	zeroTxnOp := mock_frontend.NewMockTxnOperator(ctrl)
	zeroTxnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{})
	noSnapshot := &CDCCreateTaskOptions{NoFull: true}
	setNoFullStartTS(noSnapshot, zeroTxnOp)
	require.Empty(t, noSnapshot.StartTs)
}

func TestValidateStableInitialSnapshotCompileProtocol(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := &Compile{proc: proc}
	rt := moruntime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion47)
	require.ErrorContains(t, validateStableInitialSnapshotCompileProtocol(
		context.Background(), c, true), "protocol version 48")
	require.NoError(t, validateStableInitialSnapshotCompileProtocol(
		context.Background(), c, false))

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion48)
	require.NoError(t, validateStableInitialSnapshotCompileProtocol(
		context.Background(), c, true))

	// Missing runtime/process information fails closed for stable creation.
	require.Error(t, validateStableInitialSnapshotCompileProtocol(
		context.Background(), nil, true))
}

func TestValidateLosslessNoFullStartCompileProtocol(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := &Compile{proc: proc}
	rt := moruntime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion93)
	require.ErrorContains(t, validateLosslessNoFullStartCompileProtocol(
		context.Background(), c), "protocol version 94")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion94)
	require.NoError(t, validateLosslessNoFullStartCompileProtocol(
		context.Background(), c))
}

func TestDeleteManyWatermarkRetainsSnapshotEpochOnRestart(t *testing.T) {
	keys := map[taskservice.CDCTaskKey]struct{}{
		{AccountId: 7, TaskId: "task"}: {},
	}

	restartExecutor := &cdcRecordingSQLExecutor{}
	deleted, err := deleteManyWatermark(t.Context(), restartExecutor, keys, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	require.Len(t, restartExecutor.queries, 1)
	require.Contains(t, restartExecutor.queries[0], "mo_cdc_watermark")
	require.NotContains(t, restartExecutor.queries[0], "mo_cdc_snapshot")

	cancelExecutor := &cdcRecordingSQLExecutor{}
	deleted, err = deleteManyWatermark(t.Context(), cancelExecutor, keys, true)
	require.NoError(t, err)
	require.Equal(t, int64(1), deleted)
	require.Len(t, cancelExecutor.queries, 2)
	require.True(t, strings.Contains(cancelExecutor.queries[0], "mo_cdc_watermark"))
	require.True(t, strings.Contains(cancelExecutor.queries[1], "mo_cdc_snapshot"))
}

func TestCDCStableWatermarkUpsertParses(t *testing.T) {
	sql := cdc.CDCSQLBuilder.OnDuplicateUpdateMonotonicWatermarkSQL(
		"(1, 'task', 'db', 'tbl', '100-2')",
	)
	statements, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
}

func TestCDCStableWatermarkErrorUpdateParses(t *testing.T) {
	sql := cdc.CDCSQLBuilder.GuardedOwnedWatermarkErrorUpdateSQL(
		"SELECT 1 AS account_id, 'task' AS task_id, 'db' AS db_name, "+
			"'tbl' AS table_name, 'failed' AS err_msg, 123 AS owner_generation",
		"(account_id = 1 AND task_id = 'task')",
	)
	statements, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
}
