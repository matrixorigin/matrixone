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

//go:build gpu

package cagra

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// tempArtifacts lists the fetch scratch files loadIndexes creates. FetchArtifact
// uses os.CreateTemp("", "cagra"), so a leaked download is visible by name.
func tempArtifacts(t *testing.T) map[string]bool {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(os.TempDir(), "cagra*"))
	require.NoError(t, err)
	out := make(map[string]bool, len(matches))
	for _, m := range matches {
		out[m] = true
	}
	return out
}

// loadIndexes downloads every sub-index BEFORE it admits the aggregate, so between
// the first fetch and the gate it owns N tars that nothing else will ever remove:
// Load returns on error before it assigns s.Indexes or arms its deferred Destroy.
//
// Regression: while the download lived inside LoadIndex, LoadIndex's own defer
// removed the scratch file on every return. Hoisting it into FetchArtifact moved
// the tars out from under that defer, and an early return -- a failed fetch, an
// unreadable tar, or a refusal from the aggregate gate -- leaked the whole
// multi-gigabyte download, again on every retried query.
//
// Driven here by failing the SECOND sub-index's fetch, which is the cheapest way
// to reach the early return with the first one's tar already on disk. The gate's
// own refusal takes the identical path.
func TestLoadIndexesRemovesFetchedTarsOnError(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	built := buildTestModel(t, "leak-a", nil)
	tarPath := built.Path
	t.Cleanup(func() { os.Remove(tarPath) })

	origRunSql := runSql
	t.Cleanup(func() { runSql = origRunSql })
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "AND tag = 1") || strings.Contains(sql, "AND tag = 2") {
			return executor.Result{Mp: proc.Mp()}, nil
		}
		// Two sub-indexes, so the first is already fetched when the second fails.
		return executor.Result{
			Mp: proc.Mp(),
			Batches: []*batch.Batch{
				makeMetaBatch(proc, "leak-a", built.Checksum, 0, built.FileSize),
				makeMetaBatch(proc, "leak-b", built.Checksum, 0, built.FileSize),
			},
		}, nil
	}

	origStream := runSql_streaming
	t.Cleanup(func() { runSql_streaming = origStream })
	runSql_streaming = func(_ context.Context, _ *sqlexec.SqlProcess, sql string,
		ch chan executor.Result, _ chan error) (executor.Result, error) {
		if strings.Contains(sql, "leak-b") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("chunk stream failed")
		}
		ch <- executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc, tarPath)}}
		return executor.Result{}, nil
	}

	before := tempArtifacts(t)

	s := NewCagraSearch[float32, float32](testIdxcfg(), testTblcfg(), []int{0})
	err := s.Load(sqlproc)
	require.Error(t, err, "a failed fetch must fail the load")
	t.Cleanup(s.Destroy)

	// Every scratch tar this load created is gone. A survivor here is the leak:
	// nothing downstream of loadIndexes ever sees these paths.
	for p := range tempArtifacts(t) {
		require.True(t, before[p], "loadIndexes leaked the fetched artifact %s", p)
	}
}
