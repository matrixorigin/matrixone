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

package table_function

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type indexStatsStorageFixture struct {
	fail bool
}

func (f indexStatsStorageFixture) Size(context.Context, string) (uint64, error) {
	return 123, nil
}

func (f indexStatsStorageFixture) GetColumMetadataScanInfo(_ context.Context, column string, tombstones bool) ([]*plan.MetadataScanInfo, error) {
	if f.fail {
		return nil, errors.New("metadata read failed")
	}
	if column != "*" {
		return nil, errors.New("expected all columns")
	}
	if tombstones {
		return []*plan.MetadataScanInfo{{ObjectName: "delete1"}, {ObjectName: "delete1"}}, nil
	}
	return []*plan.MetadataScanInfo{{ObjectName: "data1"}, {ObjectName: "data1"}, {ObjectName: "data2"}}, nil
}

func TestMoIndexStatsStorageCountsObjectsOnceAcrossColumns(t *testing.T) {
	size, data, tombstones, err := indexStatsRelationStorage(context.Background(), indexStatsStorageFixture{})
	require.NoError(t, err)
	require.Equal(t, uint64(123), size)
	require.Equal(t, 2, data)
	require.Equal(t, 1, tombstones)
	_, _, _, err = indexStatsRelationStorage(context.Background(), indexStatsStorageFixture{fail: true})
	require.ErrorContains(t, err, "metadata read failed")
}

func TestMoIndexStatsAsyncCoverageUsesSourceCommitsAndLogicalTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name, watermark, jobStatus, wantStatus, wantState string
		state                                             int64
		source                                            types.TS
		caught                                            any
		lag                                               any
		sourceError                                       bool
	}{
		{"caught_up", "2000000000-2", `{"Stage":1}`, "ok", "running", 3, types.BuildTS(2000000000, 2), true, float64(0), false},
		{"logical_lag", "2000000000-2", `{"Stage":1}`, "ok", "running", 3, types.BuildTS(2000000000, 3), false, float64(0), false},
		{"physical_lag", "2000000000-0", `{"Stage":1}`, "ok", "running", 3, types.BuildTS(4000000000, 0), false, float64(2), false},
		{"idle_source", "4000000000-0", `{"Stage":1}`, "ok", "running", 3, types.BuildTS(2000000000, 0), true, float64(0), false},
		{"initializing", "9223372036854775807-4294967295", `{"Stage":0}`, "initializing", "running", 1, types.TS{}, false, nil, false},
		{"sentinel", "9223372036854775807-4294967295", `{"Stage":1}`, "checkpoint_unavailable", "stopped", 4, types.TS{}, nil, nil, false},
		{"no_stage", "2000000000-0", `{}`, "job_status_unavailable", "running", 3, types.TS{}, nil, nil, false},
		{"retrying", "2000000000-0", `{"Stage":1,"ErrorCode":7,"ErrorMsg":"try again"}`, "ok", "retrying", 2, types.BuildTS(2000000000, 0), true, float64(0), false},
		{"stopped", "2000000000-0", `{"Stage":1,"ErrorCode":7}`, "ok", "stopped", 4, types.BuildTS(2000000000, 0), true, float64(0), false},
		{"source_unknown", "2000000000-0", `{"Stage":1}`, "source_commit_ts_unavailable", "running", 3, types.TS{}, nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 7)
			oldSQL, oldSource := indexStatsRunSQL, indexStatsSourceCommit
			t.Cleanup(func() { indexStatsRunSQL, indexStatsSourceCommit = oldSQL, oldSource })
			indexStatsRunSQL = func(sqlproc *sqlexec.SqlProcess, query string) (executor.Result, error) {
				require.Equal(t, uint32(catalog.System_Account), *sqlproc.AccountIDOverride)
				require.Contains(t, query, "account_id = 7 AND table_id = 42")
				require.Contains(t, query, "job_name = 'index_i''vf'")
				require.Contains(t, query, "drop_at IS NULL ORDER BY job_id DESC LIMIT 1")
				watermark := vector.NewVec(types.T_varchar.ToType())
				require.NoError(t, vector.AppendBytes(watermark, []byte(tc.watermark), false, proc.Mp()))
				status := vector.NewVec(types.T_varchar.ToType())
				require.NoError(t, vector.AppendBytes(status, []byte(tc.jobStatus), false, proc.Mp()))
				return statsTestBatch(proc.Mp(), statsTestInts(t, proc.Mp(), tc.state), watermark, status), nil
			}
			indexStatsSourceCommit = func(_ *process.Process, _ *sqlexec.SqlProcess, _ indexstats.Config) (types.TS, error) {
				if tc.sourceError {
					return types.TS{}, errors.New("transaction-local writes")
				}
				return tc.source, nil
			}
			metrics, err := readIndexStatsMaintenance(proc, sqlexec.NewSqlProcess(proc), indexstats.Config{TableID: 42, Index: "i'vf"})
			require.NoError(t, err)
			if tc.wantStatus == "ok" {
				require.NotContains(t, metrics, "status")
			} else {
				require.Equal(t, tc.wantStatus, metrics["status"])
			}
			require.Equal(t, tc.wantState, metrics["task_state"])
			for _, key := range []string{"job_id", "job_state", "watermark", "source_commit_ts"} {
				require.NotContains(t, metrics, key)
			}
			if strings.Contains(tc.jobStatus, "ErrorCode") {
				require.Equal(t, int64(7), metrics["error_code"])
			} else {
				require.NotContains(t, metrics, "error_code")
			}
			require.Equal(t, tc.caught, metrics["caught_up"])
			require.Equal(t, tc.lag, metrics["lag_seconds"])
			require.NotContains(t, metrics, "pending_rows")
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestMoIndexStatsMissingAsyncJobAndUninitializedIndex(t *testing.T) {
	proc := testutil.NewProc(t)
	oldSQL, oldStorage := indexStatsRunSQL, indexStatsReadStorage
	t.Cleanup(func() { indexStatsRunSQL, indexStatsReadStorage = oldSQL, oldStorage })
	indexStatsRunSQL = func(_ *sqlexec.SqlProcess, query string) (executor.Result, error) {
		require.True(t, strings.HasPrefix(query, "SELECT "))
		return executor.Result{}, nil
	}
	indexStatsReadStorage = func(_ *process.Process, _ *sqlexec.SqlProcess, _ indexstats.Config) (map[string]any, error) {
		return map[string]any{"size_bytes": uint64(0)}, nil
	}
	tf := &TableFunction{}
	state := &indexStatsState{config: indexstats.Config{Async: true}}
	require.NoError(t, state.start(tf, proc, 0, nil))
	metrics := state.rowMetrics(nil)
	require.Nil(t, metrics["index_version"])
	require.Equal(t, "index_not_initialized", metrics["status"])
	require.Equal(t, "task_missing", state.maintenance["status"])
	require.Nil(t, state.maintenance["caught_up"])
	require.NotContains(t, metrics, "diagnosis")
	state.free(tf, proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMoIndexStatsInvalidWatermarks(t *testing.T) {
	for _, value := range []string{"", "0-0", "-1-0", "1", "a-2", "1-a", "1-4294967296", "9223372036854775807-0"} {
		_, ok := indexStatsWatermark(value)
		require.False(t, ok, value)
	}
	_, ok := indexStatsWatermark(types.BuildTS(math.MaxInt64-1, math.MaxUint32).ToString())
	require.True(t, ok)
}

type primaryStatsStorageFixture struct {
	indexStatsStorageFixture
	columns []string
}

func (f *primaryStatsStorageFixture) Size(_ context.Context, column string) (uint64, error) {
	f.columns = append(f.columns, column)
	if column == "a" {
		return 8, nil
	}
	if column == "b" {
		return 16, nil
	}
	return 1000000, nil
}

func TestMoIndexStatsPrimaryStorageExcludesOtherSourceColumns(t *testing.T) {
	f := &primaryStatsStorageFixture{}
	bytes, data, tombstones, err := indexStatsRelationStorage(t.Context(), f, "a", "b")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, f.columns)
	require.Equal(t, uint64(24), bytes)
	require.Equal(t, 2, data)
	require.Equal(t, 1, tombstones)
}
