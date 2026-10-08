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
	"errors"
	"os"
	"strings"
	"sync/atomic"
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
	cuvscdc "github.com/matrixorigin/matrixone/pkg/vectorindex/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// swapRunSql / swapRunStreamingSql install a mock and return a restore func. The engine
// indirects sqlexec.RunSql / RunStreamingSql through these package vars so DB round-trips
// are mockable.
func swapRunSql(t *testing.T, fn func(*sqlexec.SqlProcess, string) (executor.Result, error)) {
	prev := runSql
	prevWithContext := runSqlWithContext
	runSql = fn
	runSqlWithContext = func(_ context.Context, sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		return fn(sqlproc, sql)
	}
	t.Cleanup(func() { runSql = prev })
	t.Cleanup(func() { runSqlWithContext = prevWithContext })
}

func swapRunStreamingSql(t *testing.T, fn func(context.Context, *sqlexec.SqlProcess, string, chan executor.Result, chan error) (executor.Result, error)) {
	prev := runStreamingSql
	runStreamingSql = fn
	t.Cleanup(func() { runStreamingSql = prev })
}

// docsAndBytesBatch is what baseDocCountAndBytes reads: SUM(nrow), SUM(filesize).
func docsAndBytesBatch(mp *mpool.MPool, ndoc, bytes int64) *batch.Batch {
	b := batch.NewWithSize(2)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	_ = vector.AppendFixed[int64](b.Vecs[0], ndoc, false, mp)
	_ = vector.AppendFixed[int64](b.Vecs[1], bytes, false, mp)
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

// mockSqlProcWithIdentity uses the background SQL context so the optional
// Base-file pool can exercise its durable owner/account key. The ordinary
// process helper intentionally has no lock service and therefore represents
// the fail-closed identity-missing path.
func mockSqlProcWithIdentity(t *testing.T, service string) (*sqlexec.SqlProcess, *mpool.MPool) {
	_, mp := mockSqlProc(t)
	sp := sqlexec.NewSqlProcessWithContext(
		sqlexec.NewSqlContext(context.Background(), service, nil, 0, nil))
	return sp, mp
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

func TestBaseFileOwnerCloseCancelsOrdinaryFallback(t *testing.T) {
	requestCtx, requestCancel := context.WithCancel(context.Background())
	_, mp := mockSqlProc(t)
	sp := sqlexec.NewSqlProcessWithContext(
		sqlexec.NewSqlContext(requestCtx, "fallback-close", nil, 0, nil))
	cfg := testStorageCfg()
	const filesize = int64(8)

	// Make pool admission fail before any pooled fill starts. The loader must
	// then use the ordinary path, but that path remains an admitted owner
	// operation and must observe owner shutdown while streaming source bytes.
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, "fallback-checksum", filesize, 7)}}, nil
	})
	streamStarted := make(chan struct{})
	streamCanceled := make(chan struct{})
	swapRunStreamingSql(t, func(ctx context.Context, _ *sqlexec.SqlProcess, _ string, _ chan executor.Result, _ chan error) (executor.Result, error) {
		close(streamStarted)
		<-ctx.Done()
		close(streamCanceled)
		return executor.Result{}, ctx.Err()
	})

	owner := newBaseFileOwner(1, 1)
	loadDone := make(chan error, 1)
	closeDone := make(chan error, 1)
	loadExited := make(chan struct{})
	closeExited := make(chan struct{})
	var closeStarted atomic.Bool
	t.Cleanup(func() {
		// This is an independent rescue path: it must release the request even
		// when the owner cancellation under test is broken.  Wait for every
		// goroutine before the mock restorers registered above run.
		requestCancel()
		wait := func(name string, done <-chan struct{}) bool {
			select {
			case <-done:
				return true
			case <-time.After(5 * time.Second):
				t.Errorf("%s goroutine did not terminate during cleanup", name)
				return false
			}
		}
		loadStopped := wait("ordinary fallback", loadExited)
		if closeStarted.Load() {
			wait("owner close", closeExited)
		} else if loadStopped {
			if err := owner.close(); err != nil && !errors.Is(err, errBaseFileOwnerPending) {
				t.Errorf("cleanup owner close: %v", err)
			}
		}
	})
	go func() {
		defer close(loadExited)
		_, err := loadFromStorageWithOwner(sp, cfg, "seg-fallback", owner, owner.pool)
		loadDone <- err
	}()
	select {
	case <-streamStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("ordinary fallback did not reach source streaming")
	}

	closeStarted.Store(true)
	go func() {
		defer close(closeExited)
		closeDone <- owner.close()
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("owner close waited for an ordinary fallback that ignored cancellation")
	}
	select {
	case <-streamCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("owner close did not cancel the ordinary loader stream")
	}
	require.ErrorIs(t, <-loadDone, context.Canceled)
}

func TestBaseFileOwnerCloseCancelsMetadataReadAndPreservesIdentity(t *testing.T) {
	requestCtx, requestCancel := context.WithCancel(context.Background())
	_, mp := mockSqlProc(t)
	sp := sqlexec.NewSqlProcessWithContext(
		sqlexec.NewSqlContext(requestCtx, "metadata-close", nil, 7, nil))
	cfg := testStorageCfg()
	metadataStarted := make(chan struct{})
	metadataCanceled := make(chan struct{})
	identity := make(chan *sqlexec.SqlProcess, 1)
	prevWithContext := runSqlWithContext
	runSqlWithContext = func(ctx context.Context, got *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		identity <- got
		close(metadataStarted)
		<-ctx.Done()
		close(metadataCanceled)
		return executor.Result{Mp: mp}, ctx.Err()
	}
	t.Cleanup(func() { runSqlWithContext = prevWithContext })

	owner := newBaseFileOwner(1, 1)
	loadDone := make(chan error, 1)
	closeDone := make(chan error, 1)
	loadExited := make(chan struct{})
	closeExited := make(chan struct{})
	var closeStarted atomic.Bool
	t.Cleanup(func() {
		// Request cancellation is independent of owner.close and is the rescue
		// path if the metadata propagation under test fails before shutdown can
		// start.
		requestCancel()
		loadStopped := false
		select {
		case <-loadExited:
			loadStopped = true
		case <-time.After(5 * time.Second):
			t.Errorf("metadata fallback did not terminate during cleanup")
		}
		if !loadStopped {
			return
		}
		if closeStarted.Load() {
			select {
			case <-closeExited:
			case <-time.After(5 * time.Second):
				t.Errorf("owner close did not terminate during cleanup")
			}
		} else {
			if err := owner.close(); err != nil && !errors.Is(err, errBaseFileOwnerPending) {
				t.Errorf("cleanup owner close: %v", err)
			}
		}
	})
	go func() {
		defer close(loadExited)
		_, err := loadFromStorageWithOwner(sp, cfg, "seg-metadata-fallback", owner, owner.pool)
		loadDone <- err
	}()
	select {
	case <-metadataStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("metadata read did not start")
	}
	got := <-identity
	require.Same(t, sp, got, "metadata executor must receive the original SqlProcess")
	require.Equal(t, "metadata-close", got.GetService())
	account, err := got.GetAccountID()
	require.NoError(t, err)
	require.Equal(t, uint32(7), account)

	closeStarted.Store(true)
	go func() {
		defer close(closeExited)
		closeDone <- owner.close()
	}()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("owner close did not finish")
	}
	select {
	case err := <-loadDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("metadata load did not finish after owner close")
	}
	select {
	case <-metadataCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("owner close did not cancel metadata executor context")
	}
}

func TestScanHelpers(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 13, 0)}}, nil
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
		return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 100, 0)}}, nil
	})
	require.NoError(t, checkBaseLoadBudget(sp, cfg))
	require.NoError(t, checkTailLoadBudget(sp, cfg, 0))

	// an enormous count exceeds the CN budget → actionable error (no int64 overflow).
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, int64(1)<<40, 0)}}, nil
	})
	require.Error(t, checkBaseLoadBudget(sp, cfg))
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, int64(1)<<50, 0)}}, nil
	})
	require.Error(t, checkTailLoadBudget(sp, cfg, 0))
}

func TestLoadAllBasesEmpty(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()

	// enumerate returns no ids → no bases (never touches LoadFromStorage).
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: nil}, nil
	})
	bases, err := LoadAllBases(sp, cfg)
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
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
		}
		return executor.Result{Mp: mp, Batches: nil}, nil
	})
	segs, deletes, err := LoadTailSegments(sp, cfg)
	require.NoError(t, err)
	require.Empty(t, segs)
	require.Empty(t, deletes)
}

func TestLoadAllBasesRunSqlError(t *testing.T) {
	sp, _ := mockSqlProc(t)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("base enumeration failed")
	})

	_, err := LoadAllBases(sp, testStorageCfg())
	require.ErrorContains(t, err, "base enumeration failed")
}

func TestLoadFromStorageStreamError(t *testing.T) {
	sp, mp := mockSqlProc(t)
	cfg := testStorageCfg()
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, "chk", 1, 0)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, _ chan executor.Result, _ chan error) (executor.Result, error) {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("stream failed")
	})

	_, err := LoadFromStorage(sp, cfg, "seg0")
	require.ErrorContains(t, err, "stream failed")
}

func TestLoadTailSegmentsErrorPaths(t *testing.T) {
	loadChunks := func(t *testing.T, chunks []TailChunk) ([]*Segment, map[any]int64, error) {
		t.Helper()
		sp, mp := mockSqlProc(t)
		calls := 0
		swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			calls++
			if calls == 1 {
				return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
			}
			return executor.Result{Mp: mp, Batches: []*batch.Batch{tailChunkBatch(mp, chunks)}}, nil
		})
		return LoadTailSegments(sp, testStorageCfg())
	}

	t.Run("budget", func(t *testing.T) {
		sp, _ := mockSqlProc(t)
		swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			return executor.Result{}, moerr.NewInternalErrorNoCtx("tail budget failed")
		})
		_, _, err := LoadTailSegments(sp, testStorageCfg())
		require.ErrorContains(t, err, "tail budget failed")
	})

	t.Run("tail query", func(t *testing.T) {
		sp, mp := mockSqlProc(t)
		calls := 0
		swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
			calls++
			if calls == 1 {
				return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
			}
			return executor.Result{}, moerr.NewInternalErrorNoCtx("tail query failed")
		})
		_, _, err := LoadTailSegments(sp, testStorageCfg())
		require.ErrorContains(t, err, "tail query failed")
	})

	t.Run("gap", func(t *testing.T) {
		_, _, err := loadChunks(t, []TailChunk{{ChunkId: 1, Data: []byte("x")}, {ChunkId: 3, Data: []byte("x")}})
		require.ErrorContains(t, err, "gap or duplicate")
	})

	t.Run("bad frame", func(t *testing.T) {
		_, _, err := loadChunks(t, []TailChunk{{ChunkId: 0, Data: []byte("bad frame")}})
		require.Error(t, err)
	})

	t.Run("invalid insert payload", func(t *testing.T) {
		frame := cuvscdc.FrameCdcChunk([]byte{1}, nil, 1, 0, 0)
		_, _, err := loadChunks(t, []TailChunk{{ChunkId: 0, Data: frame}})
		require.Error(t, err)
	})

	t.Run("invalid delete payload", func(t *testing.T) {
		frame := cuvscdc.FrameCdcChunk([]byte{1}, nil, 0, 1, 0)
		_, _, err := loadChunks(t, []TailChunk{{ChunkId: 0, Data: frame}})
		require.Error(t, err)
	})

	t.Run("valid delete", func(t *testing.T) {
		frame, err := FrameDeletes(int32(types.T_int64), []DeleteRecord{{Pk: int64(7)}})
		require.NoError(t, err)
		segs, deletes, err := loadChunks(t, []TailChunk{{ChunkId: 4, Data: frame}})
		require.NoError(t, err)
		require.Empty(t, segs)
		require.Equal(t, int64(4), deletes[int64(7)])
	})
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

	loaded, err := LoadFromStorage(sp, cfg, "seg0")
	require.NoError(t, err)
	t.Cleanup(loaded.Free)
	require.Equal(t, "seg0", loaded.Id)
	require.Equal(t, int64(5), loaded.Recency)
	require.Equal(t, seg.N, loaded.N)
	df, ok := loaded.lookupLoadedDF("hello")
	require.True(t, ok)
	require.Equal(t, 2, df)

	// A checksum authenticates bytes, not their internal structure. Persist a
	// matching checksum for a blob whose "hello" entry has a valid DF header but
	// an invalid block directory. Load does not validate all directories: header DF
	// remains readable, while search rejects the corrupted entry independently.
	badDirectory := corruptSerializedTermDirectory(t, buf, "hello")
	badDirectoryChecksum := vectorindex.CheckSumFromBuffer(badDirectory)
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, badDirectoryChecksum, int64(len(badDirectory)), 5)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, badDirectory)}}
		return executor.Result{}, nil
	})
	loaded, err = LoadFromStorage(sp, cfg, "seg0")
	require.NoError(t, err)
	t.Cleanup(loaded.Free)
	df, ok = loaded.lookupLoadedDF("hello")
	require.True(t, ok)
	require.Equal(t, 2, df)
	_, ok = loaded.LookupLoaded("hello")
	require.False(t, ok)
	_, ok = loaded.LookupLoaded("world")
	require.True(t, ok)

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

// TestLoadFromStoragePoolReusesOnlyTheImmutableBaseFile proves the prototype's
// ownership boundary: the second load skips chunk materialization, but receives
// a distinct mmap/Segment and therefore cannot share decoded state or liveness.
func TestLoadFromStoragePoolUsesExecutionTenant(t *testing.T) {
	sp, mp := mockSqlProcWithIdentity(t, "tenant-pool")
	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "hello")
	seg, err := b.Finish()
	require.NoError(t, err)
	t.Cleanup(seg.Free)
	buf, err := seg.Serialize()
	require.NoError(t, err)
	size := int64(len(buf))
	pool := newBaseFilePool(4*size, 4)
	t.Cleanup(pool.Close)
	var reads int
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, vectorindex.CheckSumFromBuffer(buf), size, 1)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		reads++
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})
	// One calling session reads two publishers. Even identical physical names
	// and bytes must not reuse a file across the effective tenant boundary.
	for _, account := range []uint32{42, 43, 42} {
		func() {
			loaded, err := loadFromStorageWithPool(sp.WithExecutionIdentity(account, "publisher"), testStorageCfg(), "seg0", pool)
			require.NoError(t, err)
			defer loaded.Free()
			require.Equal(t, int64(1), loaded.N)
		}()
	}
	require.Equal(t, 2, reads)
}

func TestLoadFromStoragePoolReusesOnlyTheImmutableBaseFile(t *testing.T) {
	if !experimentalBaseFileReuseEnabled {
		t.Skip("Base-file pool is only enabled in the experimental build")
	}
	sp, mp := mockSqlProcWithIdentity(t, "test-service")
	cfg := testStorageCfg()
	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	feed(t, b, int64(2), "hello", "matrix")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = "seg0"
	buf, err := seg.Serialize()
	require.NoError(t, err)
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))
	var streamCalls atomic.Int32
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, filesize, 7)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		streamCalls.Add(1)
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})

	pool := newBaseFilePool(filesize*2, 2)
	first, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	second, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	require.Equal(t, int32(1), streamCalls.Load(), "the immutable Base file is materialized once")
	require.NotSame(t, first, second)
	require.NotEmpty(t, first.mmapData)
	require.NotEmpty(t, second.mmapData)
	require.False(t, &first.mmapData[0] == &second.mmapData[0], "each Segment gets an independent mapping")
	firstPosting, ok := first.LookupLoaded("hello")
	require.True(t, ok)
	secondPosting, ok := second.LookupLoaded("hello")
	require.True(t, ok)
	require.Equal(t, firstPosting.materializeDocIDs(), secondPosting.materializeDocIDs())
	first.Free()
	second.Free()
	fallbackPool := newBaseFilePool(filesize-1, 1)
	streamCalls.Store(0)
	fallback, err := loadFromStorageWithPool(sp, cfg, "seg0", fallbackPool)
	require.NoError(t, err, "pool admission must fall back to ordinary loading")
	require.Nil(t, fallback.mmapRelease, "a capacity fallback must not retain a pool lease")
	fallback.Free()
	fallbackPool.Close()
	require.Equal(t, int32(1), streamCalls.Load(), "ordinary fallback still materializes the source once")
	pool.Close()
}

func TestBaseFileReuseSurvivesSearchHandleDestroy(t *testing.T) {
	if !experimentalBaseFileReuseEnabled {
		t.Skip("Base-file pool is only enabled in the experimental build")
	}
	sp, mp := mockSqlProcWithIdentity(t, "test-service")
	cfg := testStorageCfg()
	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	feed(t, b, int64(2), "hello", "matrix")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = "seg0"
	buf, err := seg.Serialize()
	require.NoError(t, err)
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))
	var streamCalls atomic.Int32
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, filesize, 7)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		streamCalls.Add(1)
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})

	pool := newBaseFilePool(filesize*2, 2)
	firstSearch := newFulltext2SearchWithBasePool(cfg, pool)
	firstSeg, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	firstSearch.idx = NewIndex([]*Segment{firstSeg}, nil)
	firstSearch.loaded = true
	firstSearch.Destroy()
	require.Nil(t, firstSearch.basePool)

	secondSearch := newFulltext2SearchWithBasePool(cfg, pool)
	secondSeg, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	secondSearch.idx = NewIndex([]*Segment{secondSeg}, nil)
	secondSearch.loaded = true
	require.Equal(t, int32(1), streamCalls.Load(), "a caller-owned pool survives search-handle destruction")
	secondSearch.Destroy()
	pool.Close()
}

func TestBaseFileOwnerSurvivesSearchHandleDestroy(t *testing.T) {
	if !experimentalBaseFileReuseEnabled {
		t.Skip("Base-file owner is only enabled in the experimental build")
	}
	sp, mp := mockSqlProcWithIdentity(t, "test-service")
	cfg := testStorageCfg()
	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	feed(t, b, int64(2), "hello", "matrix")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = "seg0"
	buf, err := seg.Serialize()
	require.NoError(t, err)
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))
	var streamCalls atomic.Int32
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, filesize, 7)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		streamCalls.Add(1)
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})

	owner := newBaseFileOwner(filesize*2, 2)
	pool, err := owner.poolForSearch()
	require.NoError(t, err)
	firstSearch := newFulltext2SearchWithBaseOwner(cfg, owner)
	firstSeg, err := loadFromStorageWithOwner(sp, cfg, "seg0", owner, pool)
	require.NoError(t, err)
	firstSearch.idx = NewIndex([]*Segment{firstSeg}, nil)
	firstSearch.loaded = true
	firstSearch.Destroy()
	require.Same(t, owner, firstSearch.baseOwner, "a Search retry must retain its service owner")
	require.False(t, firstSearch.baseOwnerClosed, "an open owner remains usable after ordinary Destroy")

	secondSearch := newFulltext2SearchWithBaseOwner(cfg, owner)
	secondSeg, err := loadFromStorageWithOwner(sp, cfg, "seg0", owner, pool)
	require.NoError(t, err)
	secondSearch.idx = NewIndex([]*Segment{secondSeg}, nil)
	secondSearch.loaded = true
	require.Equal(t, int32(1), streamCalls.Load(), "the service owner survives the first Search destruction")
	secondSearch.Destroy()
	require.NoError(t, owner.close())
}

func TestLoadFromStoragePoolInvalidatesCorruptReadyFile(t *testing.T) {
	if !experimentalBaseFileReuseEnabled {
		t.Skip("Base-file pool is only enabled in the experimental build")
	}
	sp, mp := mockSqlProcWithIdentity(t, "test-service")
	cfg := testStorageCfg()
	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	feed(t, b, int64(2), "hello", "matrix")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = "seg0"
	buf, err := seg.Serialize()
	require.NoError(t, err)
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))
	var streamCalls atomic.Int32
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, filesize, 7)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		streamCalls.Add(1)
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})

	pool := newBaseFilePool(filesize*2, 2)
	loaded, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	loaded.Free()
	pool.mu.Lock()
	var pooledFile *os.File
	for _, entry := range pool.entries {
		pooledFile = entry.handle.file
		break
	}
	pool.mu.Unlock()
	require.NotNil(t, pooledFile)
	_, err = pooledFile.WriteAt([]byte{buf[0] ^ 0xff}, 0)
	require.NoError(t, err)

	reloaded, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err, "a corrupt READY hit falls back to one ordinary source load")
	reloaded.Free()
	require.Equal(t, int32(2), streamCalls.Load(), "the corrupt READY hit must retire before one fallback fill")
	// The ordinary fallback deliberately does not publish a READY entry. The
	// following request therefore misses the pool and refills it.
	reloaded, err = loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	reloaded.Free()
	require.Equal(t, int32(3), streamCalls.Load())

	pool.mu.Lock()
	pooledFile = nil
	for _, entry := range pool.entries {
		pooledFile = entry.handle.file
		break
	}
	pool.mu.Unlock()
	require.NotNil(t, pooledFile)
	require.NoError(t, pooledFile.Truncate(filesize+1))
	reloaded, err = loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err, "a size-corrupt READY hit falls back to one ordinary source load")
	reloaded.Free()
	require.Equal(t, int32(4), streamCalls.Load())
	reloaded, err = loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	reloaded.Free()
	require.Equal(t, int32(5), streamCalls.Load())
	pool.Close()
}

func TestLoadFromStoragePoolValidationMunmapFailureRetainsOwner(t *testing.T) {
	if !experimentalBaseFileReuseEnabled {
		t.Skip("Base-file pool is only enabled in the experimental build")
	}
	sp, mp := mockSqlProcWithIdentity(t, "test-service")
	cfg := testStorageCfg()
	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "hello", "world")
	seg, err := b.Finish()
	require.NoError(t, err)
	seg.Id = "seg0"
	buf, err := seg.Serialize()
	require.NoError(t, err)
	checksum := vectorindex.CheckSumFromBuffer(buf)
	filesize := int64(len(buf))
	var streamCalls atomic.Int32
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, filesize, 7)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		streamCalls.Add(1)
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, buf)}}
		return executor.Result{}, nil
	})

	pool := newBaseFilePool(filesize*2, 2)
	original := munmapFn
	t.Cleanup(func() { munmapFn = original })
	munmapFn = func(data []byte) error {
		return errors.New("synthetic validation munmap failure")
	}
	_, err = loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.ErrorContains(t, err, "validation mmap release")
	require.Equal(t, 1, pool.deferredCount(), "the failed validation mapping must remain owned by the pool")

	munmapFn = original
	pool.retryDeferred()
	require.Zero(t, pool.deferredCount())
	reloaded, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	reloaded.Free()
	require.Equal(t, int32(2), streamCalls.Load())
	pool.Close()
}

func TestLoadFromStoragePoolDecodeFailureRetainsValidationOwner(t *testing.T) {
	if !experimentalBaseFileReuseEnabled {
		t.Skip("Base-file pool is only enabled in the experimental build")
	}
	sp, mp := mockSqlProcWithIdentity(t, "test-service")
	cfg := testStorageCfg()
	bad := []byte("not a serialized fulltext2 segment")
	checksum := vectorindex.CheckSumFromBuffer(bad)
	filesize := int64(len(bad))
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, filesize, 7)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, bad)}}
		return executor.Result{}, nil
	})

	pool := newBaseFilePool(filesize*2, 1)
	original := munmapFn
	t.Cleanup(func() { munmapFn = original })
	munmapFn = func([]byte) error {
		return errors.New("synthetic validation munmap failure")
	}
	_, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.Error(t, err, "the malformed validation blob must fail before READY publication")
	require.Equal(t, 1, pool.deferredCount(), "decode failure must retain its validation mapping for the pool owner")

	munmapFn = original
	pool.retryDeferred()
	require.Zero(t, pool.deferredCount())
	pool.Close()
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
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 100, 0)}}, nil
		// Scalar sums come first: the tail-size and base-total queries both mention the tail
		// id (one selects those rows, the other excludes them), so matching on the id alone
		// would hand them a batch of chunk data.
		case strings.Contains(sql, "SUM("):
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 1, 0)}}, nil
		case strings.Contains(sql, vectorindex.CdcTailId) && strings.Contains(sql, "SELECT") &&
			!strings.Contains(sql, notTailFrame()): // tail chunk data
			return executor.Result{Mp: mp, Batches: []*batch.Batch{tailChunkBatch(mp, chunks)}}, nil
		case strings.HasPrefix(strings.TrimSpace(sql), "SELECT"): // LoadAllBases enumerate → no bases
			return executor.Result{Mp: mp, Batches: nil}, nil
		default: // DELETE / INSERT writes succeed
			// The bases' metadata delete is the one that SPARES the tail frame rows;
			// DeleteTailSqls' own delete names the same prefix without the negation.
			if strings.HasPrefix(sql, "DELETE") && strings.Contains(sql, notTailFrame()) {
				deleteAllRan = true
			}
			if strings.HasPrefix(sql, "INSERT") {
				insertRan = true
			}
			return executor.Result{Mp: mp}, nil
		}
	})

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

	bases, err := LoadAllBases(sp, cfg)
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
			return executor.Result{Mp: mp, Batches: []*batch.Batch{docsAndBytesBatch(mp, 0, 0)}}, nil
		}
		return executor.Result{Mp: mp, Batches: nil}, nil // empty enumerate + empty tail
	})
	nlive, err := CompactSegments(sp, cfg, 0, 0)
	require.NoError(t, err)
	require.Equal(t, 0, nlive)
}
