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
	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// testBudget implements memory.DeviceBudget with only the free-VRAM bound, which
// is the one the load gate consults. MaxAdmissible errors rather than returning a
// number, so a test that reaches the CREATE gate by accident fails loudly instead
// of silently comparing against zero.
type testBudget struct {
	rows func(dev int, perRow uint64) (int64, uint64, error)
	// max is the permanent hardware ceiling. admitIndexes asks THIS one: its gate is
	// "can this ever fit on the card", which no eviction can change. The situational
	// free-VRAM question (rows) moved to Load, after the cache has evicted.
	max uint64
}

func (b testBudget) MaxAdmissible(int) (uint64, error) {
	if b.max == 0 {
		return 0, moerr.NewInternalErrorNoCtx("testBudget: MaxAdmissible not configured")
	}
	return b.max, nil
}

func (b testBudget) RowsFitting(dev int, perRow uint64) (int64, uint64, error) {
	return b.rows(dev, perRow)
}

// tempArtifacts lists the fetch scratch files admitIndexes creates. FetchArtifact
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

// admitIndexes downloads every sub-index BEFORE it admits the aggregate, so between
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
	// nothing downstream of admitIndexes ever sees these paths.
	for p := range tempArtifacts(t) {
		require.True(t, before[p], "admitIndexes leaked the fetched artifact %s", p)
	}
}

// The gate runs after EACH tar so a doomed load stops downloading. Nothing else
// in the suite proves the loop actually stops: every existing assertion is about
// what DeviceAggregateFitsFree decides, and a version that fetched all N tars
// before deciding would satisfy all of them.
//
// Driven with three sub-indexes and a budget that holds one but not two: the
// refusal must land on the second, so the THIRD is never fetched. Counting
// fetches is the only observable that distinguishes fail-fast from fetch-all.
func TestLoadIndexesStopsFetchingOnceOverBudget(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	built := buildTestModel(t, "stop-0", nil)
	tarPath := built.Path
	t.Cleanup(func() { os.Remove(tarPath) })

	// All three sub-indexes share one artifact, so each contributes the same
	// device bytes and the budget can be stated as a multiple of them.
	sizes, err := cuvs.MeasureTar(tarPath)
	require.NoError(t, err)
	require.Positive(t, sizes.Device, "the fixture must carry device-resident components")
	budget := uint64(sizes.Device) * 3 / 2 // one sub-index fits, two do not

	ids := []string{"stop-0", "stop-1", "stop-2"}

	origRunSql := runSql
	t.Cleanup(func() { runSql = origRunSql })
	runSql = func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "AND tag = 1") || strings.Contains(sql, "AND tag = 2") {
			return executor.Result{Mp: proc.Mp()}, nil
		}
		bats := make([]*batch.Batch, 0, len(ids))
		for _, id := range ids {
			bats = append(bats, makeMetaBatch(proc, id, built.Checksum, 0, built.FileSize))
		}
		return executor.Result{Mp: proc.Mp(), Batches: bats}, nil
	}

	fetches := 0
	origStream := runSql_streaming
	t.Cleanup(func() { runSql_streaming = origStream })
	runSql_streaming = func(_ context.Context, _ *sqlexec.SqlProcess, _ string,
		ch chan executor.Result, _ chan error) (executor.Result, error) {
		fetches++
		ch <- executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc, tarPath)}}
		return executor.Result{}, nil
	}

	// Stands in for the free-VRAM half of cuvs.BudgetFor: perRow = 1 means the answer IS the
	// byte budget, and the real one clamps to a minimum of 1.
	rowsFitting := func(_ int, perRow uint64) (int64, uint64, error) {
		rows := int64(budget / perRow)
		if rows < 1 {
			rows = 1
		}
		return rows, budget, nil
	}

	s := NewCagraSearch[float32, float32](testIdxcfg(), testTblcfg(), []int{0})
	t.Cleanup(s.Destroy)

	indexes, lerr := LoadMetadata[float32, float32](sqlproc, s.Tblcfg.DbName, s.Tblcfg.MetadataTable)
	require.NoError(t, lerr)
	require.Len(t, indexes, len(ids))

	err = s.admitIndexes(sqlproc, indexes, testBudget{rows: rowsFitting, max: budget})
	require.Error(t, err, "two sub-indexes exceed the card, so the load must be refused")
	require.Contains(t, err.Error(), "at least",
		"a refusal on a partial aggregate must not state its figure as the whole index")
	require.Contains(t, err.Error(), "after 2 sub-index(es)",
		"the permanent gate names how far it got before stopping")
	require.Contains(t, err.Error(), "could never be queried on this GPU",
		"admitIndexes now asks the PERMANENT question; the situational one runs in Load")

	// The point of the gate living inside the fetch loop: the third tar was never
	// downloaded. That early abort is why the PERMANENT check stayed here when the
	// situational one moved to Load -- a hardware refusal is final, so there is nothing
	// to gain by fetching the rest.
	require.Equal(t, 2, fetches,
		"admitIndexes must stop at the sub-index that broke the budget, not fetch all %d", len(ids))

	// And the two it did fetch are gone -- an early return owns its downloads.
	for _, idx := range indexes {
		require.Empty(t, idx.Path, "a refused load must not leave a fetched tar behind")
	}
}
