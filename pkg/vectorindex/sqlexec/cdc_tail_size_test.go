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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func int64Result(mp *mpool.MPool, v int64) executor.Result {
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendFixed[int64](b.Vecs[0], v, false, mp)
	b.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{b}}
}

// The overflow index is built INSIDE Load, so its real size does not exist when admission
// decides. A CDC-only generation would therefore be sized at 0, reserve nothing, and allocate
// its VRAM unreserved -- and no post-load pass can undo an allocation. The tail's own metadata
// rows answer it without reading the tail.
func TestCdcTailRowsUpperBoundPrefersTheFrameRows(t *testing.T) {
	mp := mpool.MustNewZero()
	var asked []string
	prev := runSqlForTest
	runSqlForTest = func(_ *SqlProcess, sql string) (executor.Result, error) {
		asked = append(asked, sql)
		return int64Result(mp, 4096), nil
	}
	t.Cleanup(func() { runSqlForTest = prev })

	got, err := CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "store", 512)
	require.NoError(t, err)
	require.Equal(t, int64(4096), got, "the frame rows' nrow sums to the tail's record count")
	require.Len(t, asked, 1, "and it must not also pay for the chunk count")
	require.Contains(t, asked[0], vectorindex.TailFrameMetaPrefix)
	require.NotContains(t, asked[0], "LENGTH(", "sizing must never read the blob column")
}

// A tail written before the frame rows existed has none to sum. Reading the tail to count would
// double the cost of every cache miss, so its chunk count bounds it: a chunk holds at most
// MaxChunkSize, and a record is at least its vector.
func TestCdcTailRowsUpperBoundFallsBackToChunks(t *testing.T) {
	mp := mpool.MustNewZero()
	const chunks, vecBytes = 10, 512
	prev := runSqlForTest
	runSqlForTest = func(_ *SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			return int64Result(mp, chunks), nil
		}
		return int64Result(mp, 0), nil // no frame rows
	}
	t.Cleanup(func() { runSqlForTest = prev })

	got, err := CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "store", vecBytes)
	require.NoError(t, err)
	require.Equal(t, int64(chunks*vectorindex.MaxChunkSize/vecBytes), got)
	require.Positive(t, got, "a legacy tail must not be sized at zero")

	// Without a vector width the division is meaningless, so there is no bound to give.
	got, err = CdcTailRowsUpperBound(&SqlProcess{}, "db", "meta", "store", 0)
	require.NoError(t, err)
	require.Zero(t, got)
}

// A SIZING probe must never be the thing that breaks a load. RunSql reaches the executor, which
// PANICS rather than erroring when the process has no lock service -- a shape internal callers
// and unit contexts really do reach -- and this query runs inside Preload, on the path to every
// cold load. Unguarded it converts a missing executor into a crashed load; the honest answer is
// no estimate, which is exactly the behaviour that existed before the estimate did.
func TestCdcTailSizingSurvivesAProcessWithNoLockService(t *testing.T) {
	proc := testutil.NewProcess(t)
	sqlproc := NewSqlProcess(proc)

	require.NotPanics(t, func() {
		rows, err := CdcTailRowsUpperBound(sqlproc, "db", "meta", "store", 512)
		require.NoError(t, err, "a probe that cannot run is not a load failure")
		require.Zero(t, rows, "no estimate, so admission behaves as it did before the estimate")
	})
}
