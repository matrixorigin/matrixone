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

package hnsw

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func countHnswSpillFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "hnsw") {
			n++
		}
	}
	return n
}

// TestDownloadAllPlacesModelsInResolvedSpillDir guards #29253 fix 1: DownloadAll must create each
// downloaded model's local file in the directory the ISCP writer resolved (sync.tmpDir), not
// os.TempDir(). LoadMetadata builds models with an empty TmpDir, so without copying sync.tmpDir onto
// them the view=false spills land in os.TempDir() -- outside the tree HostSpillDir.sweepOnce
// reclaims. Both metadata models load here; their files must appear under spillDir, and Destroy must
// remove them.
func TestDownloadAllPlacesModelsInResolvedSpillDir(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	oldRunSql, oldStreaming := runSql, runSql_streaming
	t.Cleanup(func() { runSql, runSql_streaming = oldRunSql, oldStreaming })
	runSql = mock_runSql_2files
	runSql_streaming = mock_runSql_streaming_2files

	spillDir := t.TempDir()
	sync, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", mockMoIndexes(),
		int32(types.T_array_float32), 3, spillDir)
	require.NoError(t, err)
	require.Equal(t, 2, countHnswSpillFiles(t, spillDir),
		"both downloaded models must spill into the resolved directory, not os.TempDir()")

	sync.Destroy()
	require.Equal(t, 0, countHnswSpillFiles(t, spillDir),
		"Destroy must remove every committed model file")
}

// TestNewHnswSyncRemovesCommittedFilesOnDownloadAllError guards #29253 fix 2: when a later model's
// download fails after earlier models already committed their view=false files, NewHnswSync must
// Destroy the sync before returning the error, or those committed files orphan (runHnsw only
// Destroys a non-nil sync). Model abc-0 loads and commits its file; model abc-1's stream fails; the
// resolved spill dir must be clean afterward.
func TestNewHnswSyncRemovesCommittedFilesOnDownloadAllError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	oldRunSql, oldStreaming := runSql, runSql_streaming
	t.Cleanup(func() { runSql, runSql_streaming = oldRunSql, oldStreaming })
	runSql = mock_runSql_2files
	runSql_streaming = func(ctx context.Context, sp *sqlexec.SqlProcess, sql string,
		ch chan executor.Result, errch chan error) (executor.Result, error) {
		if strings.Contains(sql, "abc-1") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("injected model-1 download failure")
		}
		ch <- executor.Result{Mp: sp.Proc.Mp(), Batches: []*batch.Batch{makeIndexBatch2Files(sp.Proc, 0)}}
		return executor.Result{}, nil
	}

	spillDir := t.TempDir()
	_, err := NewHnswSync[float32](sqlproc, "db", "src", "idx", mockMoIndexes(),
		int32(types.T_array_float32), 3, spillDir)
	require.Error(t, err, "a failed model download must surface the error")
	require.Equal(t, 0, countHnswSpillFiles(t, spillDir),
		"the committed earlier model must not orphan on the DownloadAll error path")
}
