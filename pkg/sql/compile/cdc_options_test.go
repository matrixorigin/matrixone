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

	"github.com/matrixorigin/matrixone/pkg/cdc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

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
