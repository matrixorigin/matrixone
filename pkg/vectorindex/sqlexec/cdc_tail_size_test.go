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

package sqlexec

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func twoInt64Result(mp *mpool.MPool, a, b int64) executor.Result {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendFixed[int64](bat.Vecs[0], a, false, mp)
	_ = vector.AppendFixed[int64](bat.Vecs[1], b, false, mp)
	bat.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func int64Result(mp *mpool.MPool, v int64) executor.Result {
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendFixed[int64](b.Vecs[0], v, false, mp)
	b.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{b}}
}

// The overflow index is built INSIDE Load, so its real size does not exist when admission
// decides. A CDC-only generation sized at 0 would reserve nothing and allocate unreserved, and
// no post-load pass can undo an allocation. These pin what the estimate must be in each state.

// Fully described: every chunk is covered by a frame row, so the sum stands alone.
func TestCdcTailRowsFullyCoveredUsesTheExactSum(t *testing.T) {
	mp := mpool.MustNewZero()
	prev := runSqlForTest
	runSqlForTest = func(_ *SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			return int64Result(mp, 8), nil // 8 chunks stored
		}
		return twoInt64Result(mp, 4096, 8), nil // rows=4096 covering 8 chunks
	}
	t.Cleanup(func() { runSqlForTest = prev })

	got, err := CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "store", 512)
	require.NoError(t, err)
	require.Equal(t, int64(4096), got, "coverage is complete, so nothing is added")
}

// The state the review named: a pre-upgrade tail already holds rows, its tenant migrates (columns
// added, no rows for the flushes already on disk), and ONE later flush writes a row. Summing the
// rows reports that one flush while loadCdcTail replays everything.
func TestCdcTailRowsBoundsChunksNoFrameRowCovers(t *testing.T) {
	mp := mpool.MustNewZero()
	const legacyChunks, newRows, newChunks, vecBytes = 400, 1, 1, 512
	prev := runSqlForTest
	runSqlForTest = func(_ *SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			return int64Result(mp, legacyChunks+newChunks), nil
		}
		return twoInt64Result(mp, newRows, newChunks), nil
	}
	t.Cleanup(func() { runSqlForTest = prev })

	got, err := CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "store", vecBytes)
	require.NoError(t, err)
	perChunk := int64(vectorindex.MaxChunkSize) / vecBytes
	require.Equal(t, int64(newRows)+legacyChunks*perChunk, got)
	require.Equal(t, int64(1+400*128), got)
	require.Greater(t, got, int64(50000),
		"the legacy chunks dominate; it must not be sized at the 1 row that carries a frame row")
}

// A NARROW table has no nrow column, so the sum ERRORS. That is an ordinary rollout state, and
// reserving zero for it is what lets a load allocate outside the budget.
func TestCdcTailRowsFallsBackWhenTheColumnIsAbsent(t *testing.T) {
	mp := mpool.MustNewZero()
	const chunks, vecBytes = 10, 512
	prev := runSqlForTest
	runSqlForTest = func(_ *SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			return int64Result(mp, chunks), nil
		}
		return executor.Result{}, moerr.NewInternalErrorNoCtx("unknown column 'nrow'")
	}
	t.Cleanup(func() { runSqlForTest = prev })

	got, err := CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "store", vecBytes)
	require.NoError(t, err, "a narrow table is a supported state, not a load failure")
	require.Equal(t, int64(chunks)*(int64(vectorindex.MaxChunkSize)/vecBytes), got)
	require.Positive(t, got, "it must not silently reserve zero")
}

// The overflow index is built INSIDE Load, so its real size does not exist when admission
// decides. A CDC-only generation would therefore be sized at 0, reserve nothing, and allocate
// its VRAM unreserved -- and no post-load pass can undo an allocation. The tail's own metadata
// rows answer it without reading the tail.

// A tail written before the frame rows existed has none to sum. Reading the tail to count would
// double the cost of every cache miss, so its chunk count bounds it: a chunk holds at most
// MaxChunkSize, and a record is at least its vector.

// The chunk COUNT is what says whether the frame rows describe the whole tail. Discarding its
// error made a migrated tail -- 400 legacy chunks with no rows, plus one recorded 1-row flush --
// read as fully covered at 1 row, because an unreadable count came back as 0 chunks and 0 - 1 is
// not positive. Load then replays all 401. Unknown is not zero.
func TestCdcTailRowsErrorsWhenTheChunkCountIsUnreadable(t *testing.T) {
	mp := mpool.MustNewZero()
	prev := runSqlForTest
	runSqlForTest = func(_ *SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("count unreadable")
		}
		return twoInt64Result(mp, 1, 1), nil // the one flush that did record its row
	}
	t.Cleanup(func() { runSqlForTest = prev })

	rows, err := CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "store", 512)
	require.Error(t, err, "coverage cannot be judged without the chunks; it must not answer 'covered'")
	require.Zero(t, rows)
}

// Narrow table AND an unreadable count: the sum errors, so the chunks are the only bound left,
// and they cannot be read either. Reporting 0 here reserves nothing for a tail of any size.
func TestCdcTailRowsErrorsWhenNeitherSourceCanBeRead(t *testing.T) {
	prev := runSqlForTest
	runSqlForTest = func(_ *SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("unreadable")
	}
	t.Cleanup(func() { runSqlForTest = prev })

	rows, err := CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "store", 512)
	require.Error(t, err, "no source could be read, so there is no bound -- and no reservation")
	require.Zero(t, rows)
}

// No storage table to count is a real 0, not an unknown: there is no tail.
func TestCdcTailRowsNoStorageTableIsZeroNotUnknown(t *testing.T) {
	mp := mpool.MustNewZero()
	prev := runSqlForTest
	runSqlForTest = func(_ *SqlProcess, _ string) (executor.Result, error) {
		return twoInt64Result(mp, 0, 0), nil
	}
	t.Cleanup(func() { runSqlForTest = prev })

	rows, err := CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "", 512)
	require.NoError(t, err)
	require.Zero(t, rows)
}

// A SIZING probe must never CRASH a load. RunSql reaches the executor, which PANICS rather than
// erroring when the process has no lock service -- a shape internal callers and unit contexts
// really do reach. The recover turns that into an error; it does not turn it into an estimate.
// An unreadable tail is unknown, and unknown reaches Preload as a refusal rather than as the 0
// that would let Load allocate the overflow with nothing reserved for it.
func TestCdcTailSizingSurvivesAProcessWithNoLockService(t *testing.T) {
	proc := testutil.NewProcess(t)
	sqlproc := NewSqlProcess(proc)

	require.NotPanics(t, func() {
		rows, err := CdcTailRowsUpperBound(sqlproc, "db", "meta", "store", 512)
		require.Error(t, err, "every read is unreadable here, so there is no bound to give")
		require.Zero(t, rows)
	})
}
