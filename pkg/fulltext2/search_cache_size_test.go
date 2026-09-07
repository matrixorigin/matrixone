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

package fulltext2

import (
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// GetIndexSize answers in both states: after Preload from the counted docs, and
// after Load from the loaded segments. Nothing is device resident.
func TestFulltext2SearchGetIndexSize(t *testing.T) {
	t.Run("before preload", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		host, device := s.GetIndexSize()
		require.Equal(t, int64(0), host)
		require.Equal(t, int64(0), device)
	})

	// Between Preload and Load the entry reports the count Preload measured.
	t.Run("after preload", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		s.preloadNdoc, s.preloaded = 100, true

		host, device := s.GetIndexSize()
		require.Equal(t, int64(100*estBytesPerDocHeap), host)
		require.Equal(t, int64(0), device)
	})

	// Once loaded the segments supersede the preload count; a nil segment adds nothing.
	t.Run("after load", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		s.preloadNdoc, s.preloaded = 100, true
		// Built directly: NewIndex resolves every segment, so it cannot hold the
		// nil entry GetIndexSize defends against.
		s.idx = &Index{segments: []*Segment{{N: 3}, nil, {N: 4}}}
		s.loaded = true

		host, device := s.GetIndexSize()
		require.Equal(t, int64(7*estBytesPerDocHeap), host)
		require.Equal(t, int64(0), device)
	})

	// loaded with a nil index falls back to the preload figure.
	t.Run("loaded but no index", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		s.preloadNdoc, s.preloaded = 5, true
		s.loaded = true

		host, _ := s.GetIndexSize()
		require.Equal(t, int64(5*estBytesPerDocHeap), host)
	})
}

// baseDocCountAndBytes sums nrow AND filesize across the tag=0 bases, in one round trip: the
// cache charges for the doc heap and for the file each entry maps. Empty batches are skipped.
func TestBaseDocCount(t *testing.T) {
	mp := mpool.MustNewZero()
	cfg := TableConfig{DbName: "db", MetadataTable: "meta"}

	t.Run("sum", func(t *testing.T) {
		swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 42, 4096)}}, nil
		})
		got, err := baseDocCount(nil, cfg)
		require.NoError(t, err)
		require.Equal(t, int64(42), got)
	})

	t.Run("empty batches are skipped", func(t *testing.T) {
		empty := docsAndBytesBatch(mp, 0, 0)
		empty.SetRowCount(0)
		swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{empty, docsAndBytesBatch(mp, 7, 512)}}, nil
		})
		got, err := baseDocCount(nil, cfg)
		require.NoError(t, err)
		require.Equal(t, int64(7), got)
	})

	t.Run("no rows at all", func(t *testing.T) {
		swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
			return executor.Result{Mp: mp}, nil
		})
		got, err := baseDocCount(nil, cfg)
		require.NoError(t, err)
		require.Equal(t, int64(0), got)
	})

	t.Run("sql error", func(t *testing.T) {
		swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("boom")
		})
		_, err := baseDocCount(nil, cfg)
		require.Error(t, err)
	})
}

// preloadStub answers the two reads Preload makes: the base doc/byte sums, which EXCLUDE the
// tail rows, and the tail's own byte sum, which selects only them.
func preloadStub(t *testing.T, mp *mpool.MPool, ndoc, bytes, tailBytes int64) {
	t.Helper()
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, TailFrameMetaPrefix) && !strings.Contains(sql, "NOT LIKE"):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, tailBytes, 0)}}, nil
		case strings.Contains(sql, "COUNT(*)"):
			// The legacy fallback: no frame rows to sum, so it counts chunks instead.
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
		}
		return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, ndoc, bytes)}}, nil
	})
}

// Preload records the count in preloadNdoc and sets preloaded.
func TestFulltext2SearchPreload(t *testing.T) {
	mp := mpool.MustNewZero()
	preloadStub(t, mp, 9, 0, 0)

	s := NewFulltext2Search(TableConfig{DbName: "db", MetadataTable: "meta"})
	require.NoError(t, s.Preload(nil))
	require.True(t, s.preloaded)
	require.Equal(t, int64(9), s.preloadNdoc)

	host, _ := s.GetIndexSize()
	require.Equal(t, int64(9*estBytesPerDocHeap), host, "the governor charges what Preload measured")
}

// A failed count leaves preloaded false.
func TestFulltext2SearchPreload_Error(t *testing.T) {
	swapRunSql(t, func(*sqlexec.SqlProcess, string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("boom")
	})

	s := NewFulltext2Search(TableConfig{DbName: "db", MetadataTable: "meta"})
	require.Error(t, s.Preload(nil))
	require.False(t, s.preloaded)
}

// The mapping is the dominant cost and belongs to ONE cache entry: LoadFromStorage spills each
// base to a fresh LOCAL file and mmaps it whole, so N named-snapshot keys of the same index map
// N copies. Charging the doc heap alone reported a few hundred bytes for a multi-megabyte
// mapping, and the governor bounded N of them at nothing.
func TestGetIndexSizeChargesTheMappingItOwns(t *testing.T) {
	const mapped = 16 << 20

	t.Run("after load, the mapping dominates", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		s.idx = &Index{segments: []*Segment{{N: 1, mmapData: make([]byte, mapped)}}}
		s.loaded = true

		host, device := s.GetIndexSize()
		require.Zero(t, device)
		require.Equal(t, int64(1*estBytesPerDocHeap+mapped), host)
		require.Greater(t, host, int64(mapped),
			"a one-doc segment holding a 16 MiB mapping must not read as a few hundred bytes")
	})

	t.Run("before load, the metadata filesize stands in for it", func(t *testing.T) {
		s := NewFulltext2Search(TableConfig{})
		s.preloadNdoc, s.preloadBytes, s.preloaded = 1, mapped, true

		host, _ := s.GetIndexSize()
		require.Equal(t, int64(1*estBytesPerDocHeap+mapped), host,
			"admission must reserve the mapping before Load creates it")
	})

	t.Run("N generations are charged N mappings", func(t *testing.T) {
		one := &Fulltext2Search{idx: &Index{segments: []*Segment{{N: 1, mmapData: make([]byte, mapped)}}}, loaded: true}
		hostOne, _ := one.GetIndexSize()

		// A second snapshot key maps its own copy; the charge is per entry, not shared.
		three := int64(0)
		for i := 0; i < 3; i++ {
			s := &Fulltext2Search{idx: &Index{segments: []*Segment{{N: 1, mmapData: make([]byte, mapped)}}}, loaded: true}
			h, _ := s.GetIndexSize()
			three += h
		}
		require.Equal(t, 3*hostOne, three)
		require.Greater(t, three, int64(3*mapped))
	})
}

// A CDC-only index counts ZERO base docs. Publishing (0,0) put it outside admission entirely:
// makeRoom takes its "nothing to account for" exit before registering a reservation.
func TestFulltext2PreloadDeclaresTheCdcTail(t *testing.T) {
	mp := mpool.MustNewZero()
	const stored = 8 << 20
	preloadStub(t, mp, 0, 0, stored)

	s := NewFulltext2Search(TableConfig{DbName: "db", MetadataTable: "meta", IndexTable: "idx"})
	require.NoError(t, s.Preload(nil))

	host, device := s.GetIndexSize()
	require.Zero(t, device)
	require.Equal(t, int64(stored*tailLoadPeakFactor), host,
		"a tail-only index must declare the tail at the peak the load holds")
}

// The tail size is read from the per-frame metadata rows, which recorded it when the frames were
// written. Not SUM(LENGTH(data)): data is a blob, so that would make the scan read the whole tail
// off storage just to size it.
func TestTailPeakBytesReadsTheFrameRows(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	var seen string
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		seen = sql
		return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 4096, 0)}}, nil
	})
	got, err := tailPeakBytes(sp, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(4096*tailLoadPeakFactor), got)
	require.Contains(t, seen, TailFrameMetaPrefix)
	require.NotContains(t, seen, "LENGTH(",
		"LENGTH over a blob column makes the scan read every byte of the tail")
}

// Free memory alone is not a bound when two loads sample it at once: both read the same
// pre-allocation figure, both see room for one tail, and both allocate. The bytes promised to
// arrivals ahead of this one are what make the second refuse.
func TestTailBudgetSubtractsWhatIsAlreadyPromised(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	// One tail stores 192 KiB, so its peak is 576 KiB. Give the machine room for one, not two.
	const stored = 192 << 10
	need := int64(stored * tailLoadPeakFactor)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, stored, 0)}}, nil
	})

	origTotal, origGo := memTotalFn, memGolangFn
	t.Cleanup(func() { memTotalFn, memGolangFn = origTotal, origGo })
	// avail = total*0.8 - heap. Size it at 1.5 tails.
	memGolangFn = func() int { return 0 }
	memTotalFn = func() uint64 { return uint64(need*3/2) * 10 / 8 }

	require.NoError(t, checkTailLoadBudget(sp, cfg, 0),
		"alone, the tail fits")
	err := checkTailLoadBudget(sp, cfg, need)
	require.Error(t, err, "with one tail already promised, the second does not")
	require.Contains(t, err.Error(), "promised to loads already in flight")
}

// A tail written before the per-frame rows existed has chunks but nothing to sum. Reading 0
// there would report "no tail" and leave the OOM guard with nothing to refuse -- on exactly the
// clusters carrying the largest un-compacted tails. It falls back to bounding by chunk count.
func TestTailPeakBytesFallsBackWhenAFrameHasNoRow(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	const chunks = 4
	var sawCount bool
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "COUNT(*)") {
			sawCount = true
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, chunks, 0)}}, nil
		}
		// No frame rows: the legacy shape.
		return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
	})

	got, err := tailPeakBytes(sp, cfg)
	require.NoError(t, err)
	require.True(t, sawCount, "it must fall back rather than report no tail")
	require.Equal(t, int64(chunks*vectorindex.MaxChunkSize*tailLoadPeakFactor), got,
		"bounded from above by the chunk cap, which is the safe direction")
	require.Positive(t, got)
}
