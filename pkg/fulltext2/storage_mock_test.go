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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// swapRunSql / swapRunStreamingSql install a mock and return a restore func. The engine
// indirects sqlexec.RunSql / RunStreamingSql through these package vars so DB round-trips
// are mockable.
func swapRunSql(t *testing.T, fn func(*sqlexec.SqlProcess, string) (executor.Result, error)) {
	prev := runSql
	runSql = fn
	t.Cleanup(func() { runSql = prev })
}

func swapRunStreamingSql(t *testing.T, fn func(context.Context, *sqlexec.SqlProcess, string, chan executor.Result, chan error) (executor.Result, error)) {
	prev := runStreamingSql
	runStreamingSql = fn
	t.Cleanup(func() { runStreamingSql = prev })
}

func int64Batch(mp *mpool.MPool, v int64) *batch.Batch {
	b := batch.NewWithSize(1)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendFixed[int64](b.Vecs[0], v, false, mp)
	b.SetRowCount(1)
	return b
}

func metaBatch(mp *mpool.MPool, checksum string, filesize, recency int64) *batch.Batch {
	b := batch.NewWithSize(3)
	b.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	b.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendBytes(b.Vecs[0], []byte(checksum), false, mp)
	_ = vector.AppendFixed[int64](b.Vecs[1], filesize, false, mp)
	_ = vector.AppendFixed[int64](b.Vecs[2], recency, false, mp)
	b.SetRowCount(1)
	return b
}

// chunkBatch splits buf into (chunk_id, data) rows of <= MaxChunkSize bytes, the shape
// the base-chunk streaming reader consumes.
func chunkBatch(mp *mpool.MPool, buf []byte) *batch.Batch {
	nchunks := (len(buf) + vectorindex.MaxChunkSize - 1) / vectorindex.MaxChunkSize
	if nchunks == 0 {
		nchunks = 0
	}
	b := batch.NewWithSize(2)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	for i := 0; i < nchunks; i++ {
		off := i * vectorindex.MaxChunkSize
		end := off + vectorindex.MaxChunkSize
		if end > len(buf) {
			end = len(buf)
		}
		_ = vector.AppendFixed[int64](b.Vecs[0], int64(i), false, mp)
		_ = vector.AppendBytes(b.Vecs[1], buf[off:end], false, mp)
	}
	b.SetRowCount(nchunks)
	return b
}

func mockSqlProc(t *testing.T) (*sqlexec.SqlProcess, *mpool.MPool) {
	proc := testutil.NewProc(t)
	return sqlexec.NewSqlProcess(proc), proc.Mp()
}

func TestReadMetadata(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	// found row.
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, "chk", 42, 7)}}, nil
	})
	checksum, filesize, recency, found, err := readMetadata(sp, cfg, "id0")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "chk", checksum)
	require.Equal(t, int64(42), filesize)
	require.Equal(t, int64(7), recency)

	// no rows → found=false.
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: nil}, nil
	})
	_, _, _, found, err = readMetadata(sp, cfg, "id0")
	require.NoError(t, err)
	require.False(t, found)

	// runSql error is surfaced.
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("boom")
	})
	_, _, _, _, err = readMetadata(sp, cfg, "id0")
	require.Error(t, err)
}

func TestScanHelpers(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 13)}}, nil
	})

	n, err := CountTailChunks(sp, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(13), n)

	n, err = SumBaseNrow(sp, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(13), n)

	n, err = NextTailChunkId(sp, cfg)
	require.NoError(t, err)
	require.Equal(t, int64(13), n)

	// error path.
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("boom")
	})
	_, err = SumBaseNrow(sp, cfg)
	require.Error(t, err)
}

func TestLoadBudgetGates(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	// small doc/byte counts fit comfortably → nil.
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 100)}}, nil
	})
	require.NoError(t, checkBaseLoadBudget(sp, cfg))
	require.NoError(t, checkTailLoadBudget(sp, cfg))

	// an enormous count exceeds the CN budget → actionable error (no int64 overflow).
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, int64(1)<<40)}}, nil
	})
	require.Error(t, checkBaseLoadBudget(sp, cfg))
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, int64(1)<<50)}}, nil
	})
	require.Error(t, checkTailLoadBudget(sp, cfg))
}

func TestLoadAllBasesEmpty(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	// enumerate returns no ids → no bases (never touches LoadFromStorage).
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: nil}, nil
	})
	trace := &loadTrace{start: time.Now()}
	bases, err := loadAllBasesUncached(sp, cfg, trace)
	require.NoError(t, err)
	require.Empty(t, bases)
}

func TestLoadTailSegmentsEmpty(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	// dispatch by SQL: the budget gate is a SUM(LENGTH(...)) 1-col scan; the tail SELECT
	// (chunk_id, data) returns no rows → empty tail.
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "LENGTH(") {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 0)}}, nil
		}
		return executor.Result{Mp: mp, Batches: nil}, nil
	})
	trace := &loadTrace{start: time.Now()}
	segs, deletes, _, err := loadTailSegmentsAfter(sp, cfg, 7, trace)
	require.NoError(t, err)
	require.Empty(t, segs)
	require.Empty(t, deletes)
	require.GreaterOrEqual(t, trace.phase.internalSQL, time.Duration(0))
}

// TestLoadFromStorageRoundTrip mocks the metadata read + base-chunk stream with a REAL
// serialized segment, exercising the full decode path (spill → mmap → checksum → decode).
func TestLoadFromStorageRoundTrip(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	feed(t, b, int64(2), "hello", "matrix")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = "seg0"
	seg.Recency = 5
	buf, err := seg.Serialize()
	require.NoError(t, err)
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))

	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, filesize, 5)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})

	trace := &loadTrace{start: time.Now()}
	loaded, err := loadFromStorage(sp, cfg, "seg0", trace)
	require.NoError(t, err)
	require.Equal(t, "seg0", loaded.Id)
	require.Equal(t, int64(5), loaded.Recency)
	require.Equal(t, seg.N, loaded.N)
	require.Equal(t, filesize, trace.event.BaseBytes)
	require.Positive(t, trace.phase.tempWrite)
	loaded.Free()

	// a checksum mismatch (corrupt stream) is detected and rejected.
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		bad := append([]byte(nil), buf...)
		bad[0] ^= 0xFF
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, bad)}}
		return executor.Result{}, nil
	})
	_, err = LoadFromStorage(sp, cfg, "seg0")
	require.ErrorContains(t, err, "checksum mismatch")

	// missing metadata → clear error.
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: nil}, nil
	})
	_, err = LoadFromStorage(sp, cfg, "seg0")
	require.ErrorContains(t, err, "metadata not found")
}

// tailChunkBatch renders forged (chunk_id, data) rows as the tail-data result batch.
func tailChunkBatch(mp *mpool.MPool, chunks []TailChunk) *batch.Batch {
	b := batch.NewWithSize(2)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	for _, c := range chunks {
		_ = vector.AppendFixed[int64](b.Vecs[0], c.ChunkId, false, mp)
		_ = vector.AppendBytes(b.Vecs[1], c.Data, false, mp)
	}
	b.SetRowCount(len(chunks))
	return b
}

// TestCompactSegmentsFoldsTail drives the full MERGE: no bases + a forged tag=1 tail
// insert frame ⇒ the tail is folded into a fresh base. runSql dispatches by SQL text
// (the user's "different runSql for different SQLs") and DELETE/INSERT writes succeed.
func TestCompactSegmentsFoldsTail(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	// forge a tail insert frame carrying two docs.
	tb := NewBuilder("tail", int32(types.T_int64))
	feed(t, tb, int64(1), "hello", "world")
	feed(t, tb, int64(2), "hello", "matrix")
	tseg, err := tb.Finish()
	require.NoError(t, err)
	framed, err := FrameSegment(tseg)
	require.NoError(t, err)
	chunks := splitFrameChunks(1, framed)

	var deleteAllRan, insertRan bool
	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "GREATEST"): // NextTailChunkId
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 100)}}, nil
		case strings.Contains(sql, "LENGTH("): // checkTailLoadBudget
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 1)}}, nil
		case strings.Contains(sql, vectorindex.CdcTailId) && strings.Contains(sql, "SELECT"): // tail data
			return executor.Result{Mp: mp, Batches: []*batch.Batch{tailChunkBatch(mp, chunks)}}, nil
		case strings.HasPrefix(strings.TrimSpace(sql), "SELECT"): // LoadAllBases enumerate → no bases
			return executor.Result{Mp: mp, Batches: nil}, nil
		default: // DELETE / INSERT writes succeed
			if strings.HasPrefix(sql, "DELETE") && strings.Contains(sql, "TRUE") {
				deleteAllRan = true
			}
			if strings.HasPrefix(sql, "INSERT") {
				insertRan = true
			}
			return executor.Result{Mp: mp}, nil
		}
	})

	trace := &loadTrace{start: time.Now()}
	tracedSegs, tracedDeletes, maxChunk, err := loadTailSegmentsAfter(sp, cfg, -1, trace)
	require.NoError(t, err)
	require.Len(t, tracedSegs, 1)
	require.Empty(t, tracedDeletes)
	require.Equal(t, chunks[len(chunks)-1].ChunkId, maxChunk)
	require.Positive(t, trace.event.TailBytes)
	freeSegs(tracedSegs)

	nlive, err := CompactSegments(sp, cfg, 0, 0)
	require.NoError(t, err)
	require.Equal(t, 2, nlive) // both docs are live → folded into the fresh base
	require.True(t, deleteAllRan, "MERGE must clear prior bases first")
	require.True(t, insertRan, "MERGE must persist the rebuilt base")
}

// twoIdBatch renders a 2-row index_id enumerate result.
func twoIdBatch(mp *mpool.MPool, a, b string) *batch.Batch {
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendBytes(bat.Vecs[0], []byte(a), false, mp)
	_ = vector.AppendBytes(bat.Vecs[0], []byte(b), false, mp)
	bat.SetRowCount(2)
	return bat
}

// loadOneBase round-trips a real serialized segment through LoadFromStorage (mocked
// metadata + chunk stream), returning the mapped segment.
func loadOneBase(t *testing.T, sp *sqlexec.SqlProcess, mp *mpool.MPool, cfg TableConfig, id string) *Segment {
	t.Helper()
	b := NewBuilder(id, int32(types.T_int64))
	feed(t, b, int64(1), "hello")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = id
	buf, err := seg.Serialize()
	require.NoError(t, err)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, vectorindex.CheckSumFromBuffer(buf), int64(len(buf)), 0)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})
	m, err := LoadFromStorage(sp, cfg, id)
	require.NoError(t, err)
	return m
}

// TestFreeSegsReleasesMmap pins that freeSegs munmaps a loaded segment (nils mmapData),
// the primitive LoadAllBases now uses to avoid leaking on a partial load.
func TestFreeSegsReleasesMmap(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()
	m := loadOneBase(t, sp, mp, cfg, "s0")
	require.NotNil(t, m.mmapData) // mapped
	freeSegs([]*Segment{m, nil})  // nil entry must be a no-op
	require.Nil(t, m.mmapData)    // munmapped
}

// TestLoadAllBasesFreesOnPartialFailure: enumerate returns two ids; base "s0" maps, then
// base "s1" fails (metadata missing). LoadAllBases must free the already-mapped s0 before
// returning the error rather than leaking its mmap (+ spill file) — the #4 fix.
func TestLoadAllBasesFreesOnPartialFailure(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	b := NewBuilder("s0", int32(types.T_int64))
	feed(t, b, int64(1), "hello")
	seg, err := b.Finish()
	require.NoError(t, err)
	buf, err := seg.Serialize()
	require.NoError(t, err)

	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		switch {
		case strings.Contains(sql, "'s1'"): // readMetadata for s1 → missing → LoadFromStorage errors
			return executor.Result{Mp: mp, Batches: nil}, nil
		case strings.Contains(sql, "checksum"): // readMetadata for s0
			return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, vectorindex.CheckSumFromBuffer(buf), int64(len(buf)), 0)}}, nil
		default: // enumerate index_id → [s0, s1]
			return executor.Result{Mp: mp, Batches: []*batch.Batch{twoIdBatch(mp, "s0", "s1")}}, nil
		}
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})

	trace := &loadTrace{start: time.Now()}
	bases, err := loadAllBasesUncached(sp, cfg, trace)
	require.Error(t, err, "s1 fails to load")
	require.Nil(t, bases, "no partial slice is returned (s0 was freed)")
}

// TestCompactSegmentsNoDelta covers the early-out: empty bases + empty tail ⇒ nothing to
// compact.
func TestCompactSegmentsNoDelta(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		if strings.Contains(sql, "LENGTH(") {
			return executor.Result{Mp: mp, Batches: []*batch.Batch{int64Batch(mp, 0)}}, nil
		}
		return executor.Result{Mp: mp, Batches: nil}, nil // empty enumerate + empty tail
	})
	nlive, err := CompactSegments(sp, cfg, 0, 0)
	require.NoError(t, err)
	require.Equal(t, 0, nlive)
}
