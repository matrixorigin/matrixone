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

package table_function

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func TestIVFBuildInfoUsesBuildTransaction(t *testing.T) {
	proc := testutil.NewProc(t)
	op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
	op.EXPECT().Txn().Return(txn.TxnMeta{SnapshotTS: timestamp.Timestamp{PhysicalTime: 12345, LogicalTime: 8}}).AnyTimes()
	proc.Base.TxnOperator = op
	sp := sqlexec.NewSqlProcess(proc)
	cfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "source", MetadataTable: "metadata", DataSize: 50, SourceTableID: 42}
	previous := ivf_runSql
	t.Cleanup(func() { ivf_runSql = previous })
	failure := errors.New("metadata write failed")
	calls := 0
	ivf_runSql = func(actual *sqlexec.SqlProcess, query string) (executor.Result, error) {
		calls++
		require.Same(t, sp, actual, "publication borrows the build transaction")
		for _, text := range []string{`"build_ts":"12345-8"`, `"index_version":7`, `"nrow":50`, `"train_nsample":30`, `"source_table_id":42`, `"historical":false`} {
			require.Contains(t, query, text)
		}
		return executor.Result{}, failure
	}
	require.NoError(t, writeIVFBuildInfo(proc, sp, cfg, 7, 0, 2))
	require.NoError(t, writeIVFBuildInfo(proc, sp, cfg, 7, 1, 2))
	require.Zero(t, calls, "untrained builds must not publish a trained baseline")
	require.ErrorIs(t, writeIVFBuildInfo(proc, sp, cfg, 7, 30, 2), failure, "a failed baseline write must abort its build")
	require.Equal(t, 1, calls)
}
