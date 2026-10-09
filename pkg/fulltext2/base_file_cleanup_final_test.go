// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build (darwin || linux) && fulltext2_base_file_reuse

package fulltext2

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type detachedCleanupSearch struct {
	*Fulltext2Search
	entered chan struct{}
	resume  chan struct{}
}

func (s *detachedCleanupSearch) Destroy() { close(s.entered); <-s.resume; s.Fulltext2Search.Destroy() }

func TestBaseFileCleanupDetachedEvictionLifetime(t *testing.T) {
	for _, mode := range []string{"fallback-fault", "fallback-clean", "pooled"} {
		t.Run(mode, func(t *testing.T) {
			sp, cfg, buf := cleanupReworkSource(t, nil, "")
			capacity := int64(1)
			if mode == "pooled" {
				capacity = int64(len(buf)) * 2
			}
			owner := newBaseFileOwner(capacity, 2)
			previous := veccache.Cache
			c := veccache.NewVectorIndexCache()
			veccache.Cache = c
			t.Cleanup(func() { veccache.Cache = previous })
			withExperimentalOwnerState(t, owner, "cleanup-rework", false)
			token, err := InitializeBaseFileReuseOwner("cleanup-rework")
			require.NoError(t, err)
			p, err := owner.poolForSearch()
			require.NoError(t, err)
			selectMapping, restore, released := cleanupReworkFault(t, owner)
			seg, err := loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
			require.NoError(t, err)
			path := seg.mmapPath
			ptr := &seg.mmapData[0]
			selectMapping(seg.mmapData)
			if mode != "fallback-fault" {
				restore()
			}
			s := newFulltext2SearchWithBaseOwner(cfg, owner)
			s.idx = NewIndex([]*Segment{seg}, nil)
			s.loaded = true
			blocked := &detachedCleanupSearch{Fulltext2Search: s, entered: make(chan struct{}), resume: make(chan struct{})}
			entry := &veccache.VectorIndexSearch{Algo: blocked}
			entry.Cond = sync.NewCond(entry.Mutex.RLocker())
			entry.Status.Store(veccache.STATUS_LOADED)
			c.IndexMap.Store("detached", entry)
			joined := make(chan struct{})
			var resume sync.Once
			go func() { c.Remove("detached"); close(joined) }()
			t.Cleanup(func() {
				resume.Do(func() { close(blocked.resume) })
				<-joined
				restore()
				_ = owner.close()
				seg.Free()
				if path != "" {
					_ = os.Remove(path)
				}
			})
			select {
			case <-blocked.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("real eviction did not reach destructor")
			}
			_, present := c.IndexMap.Load("detached")
			require.False(t, present)
			require.NotNil(t, seg.mmapData, "mapping remains live after cache removal")
			require.ErrorIs(t, CloseBaseFileReuseOwner(token), errBaseFileOwnerPending, "pre-Free close must not unregister a live consumer")
			_, err = InitializeBaseFileReuseOwner("cleanup-rework")
			require.ErrorIs(t, err, errBaseFileOwnerPending)
			resume.Do(func() { close(blocked.resume) })
			<-joined
			if mode == "fallback-fault" {
				require.ErrorIs(t, CloseBaseFileReuseOwner(token), errBaseFileOwnerPending)
				requireDeferredSegmentCharge(t, p, int64(len(buf))+estBytesPerDocHeap)
				require.Zero(t, released[ptr])
				restore()
			}
			require.NoError(t, CloseBaseFileReuseOwner(token))
			require.False(t, owner.hasResources())
			requireDeferredSegmentCharge(t, p, 0)
			require.Equal(t, 1, released[ptr])
			require.Nil(t, seg.mmapData)
			if path != "" {
				_, err = os.Stat(path)
				require.True(t, os.IsNotExist(err))
			}
			next, err := InitializeBaseFileReuseOwner("cleanup-rework")
			require.NoError(t, err)
			require.NoError(t, CloseBaseFileReuseOwner(token))
			require.False(t, experimentalOwnerForSQL("cleanup-rework").closed)
			require.NoError(t, CloseBaseFileReuseOwner(next))
		})
	}
}

func TestBaseFileCleanupLinkedRemoveFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("real directory EACCES requires a non-root uid")
	}
	for _, mode := range []string{"ready-close", "ready-eviction", "failed-fill", "already-removed"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("TMPDIR", dir)
			sp, cfg, buf := cleanupReworkSource(t, nil, "")
			owner := newBaseFileOwner(int64(len(buf)), 1)
			withExperimentalOwnerState(t, owner, "cleanup-rework", false)
			token, err := InitializeBaseFileReuseOwner("cleanup-rework")
			require.NoError(t, err)
			p, err := owner.poolForSearch()
			require.NoError(t, err)
			var path string
			var fd *os.File
			t.Cleanup(func() {
				_ = os.Chmod(dir, 0700)
				_ = owner.close()
				if path != "" {
					_ = os.Remove(path)
				}
			})
			if mode == "failed-fill" {
				swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, _ chan executor.Result, _ chan error) (executor.Result, error) {
					paths, e := filepath.Glob(filepath.Join(dir, "ft2idx_pool*"))
					require.NoError(t, e)
					require.Len(t, paths, 1)
					path = paths[0]
					require.NoError(t, os.Chmod(dir, 0500))
					return executor.Result{}, errors.New("selected source error")
				})
				_, err = loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
				require.ErrorContains(t, err, "selected source error")
			} else {
				seg, e := loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
				require.NoError(t, e)
				t.Cleanup(seg.Free)
				p.mu.Lock()
				for _, entry := range p.entries {
					path = entry.handle.path
					fd = entry.handle.file
				}
				p.mu.Unlock()
				require.NotEmpty(t, path, "real materializer must use its linked fallback")
				seg.Free()
				require.NoError(t, os.Chmod(dir, 0500))
				if mode == "ready-eviction" {
					p.mu.Lock()
					e = p.makeRoomLocked(int64(len(buf)))
					p.mu.Unlock()
					require.ErrorIs(t, e, errBaseFileCleanupPending, "failed unlink must quarantine new mappings")
				}
			}
			require.ErrorIs(t, CloseBaseFileReuseOwner(token), errBaseFileOwnerPending, "unlink failure cannot report clean")
			require.FileExists(t, path)
			p.mu.Lock()
			require.Equal(t, int64(len(buf)), p.deferredFileBytes)
			require.Len(t, p.deferredFiles, 1)
			p.mu.Unlock()
			if fd != nil {
				_, err = fd.Stat()
				require.ErrorIs(t, err, os.ErrClosed, "failed unlink must not retain a fictional open FD")
			}
			p.mu.Lock()
			require.Zero(t, p.openFiles)
			require.Zero(t, p.reservedFiles)
			p.mu.Unlock()
			_, err = InitializeBaseFileReuseOwner("cleanup-rework")
			require.ErrorIs(t, err, errBaseFileOwnerPending)
			require.NoError(t, os.Chmod(dir, 0700))
			if mode == "already-removed" {
				require.NoError(t, os.Remove(path))
			}
			require.NoError(t, CloseBaseFileReuseOwner(token))
			require.False(t, owner.hasResources())
			p.mu.Lock()
			require.Zero(t, p.deferredFileBytes)
			require.Empty(t, p.deferredFiles)
			p.mu.Unlock()
			_, err = os.Stat(path)
			require.True(t, os.IsNotExist(err))
			require.NoError(t, CloseBaseFileReuseOwner(token))
		})
	}
}

// The oracle is the ordinary loader over the same fixed source, independently
// materialized before the experiment. Stream counts distinguish READY reuse
// from a warm Search and from real capacity fallback. This is a component test,
// not an embedded SQL file-hit counter or a complete ranking acceptance gate.
func TestBaseFileCleanupOrdinaryScoreOracle(t *testing.T) {
	b := NewBuilder("seg0", int32(types.T_int64))
	feed(t, b, int64(1), "alpha", "beta", "gamma")
	feed(t, b, int64(2), "beta", "gamma", "delta")
	feed(t, b, int64(3), "gamma", "delta", "epsilon")
	feed(t, b, int64(4), "alpha", "zeta")
	source, err := b.Finish()
	require.NoError(t, err)
	t.Cleanup(source.Free)
	buf, err := source.Serialize()
	require.NoError(t, err)
	sp, cfg, _ := cleanupReworkSource(t, buf, "")
	originalStream := runStreamingSql
	streams := 0
	runStreamingSql = func(ctx context.Context, sp *sqlexec.SqlProcess, q string, sc chan executor.Result, ec chan error) (executor.Result, error) {
		streams++
		return originalStream(ctx, sp, q, sc, ec)
	}
	t.Cleanup(func() { runStreamingSql = originalStream })
	scores := func(seg *Segment) map[int64]uint32 {
		t.Helper()
		results, e := NewIndex([]*Segment{seg}, nil).SearchQuery([]byte("alpha"), true, ParserDefault, BM25, 10, nil)
		require.NoError(t, e)
		got := map[int64]uint32{}
		for _, r := range results {
			got[r.Pk.(int64)] = math.Float32bits(r.Score)
		}
		require.Len(t, got, 2)
		require.Contains(t, got, int64(1))
		require.Contains(t, got, int64(4))
		return got
	}
	ordinary, err := LoadFromStorage(sp, cfg, "seg0")
	require.NoError(t, err)
	t.Cleanup(ordinary.Free)
	want := scores(ordinary)
	require.Equal(t, 1, streams)
	pool := newBaseFilePool(int64(len(buf)), 1)
	t.Cleanup(pool.Close)
	first, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	t.Cleanup(first.Free)
	require.Equal(t, want, scores(first))
	first.Free()
	require.Equal(t, 2, streams)
	hit, err := loadFromStorageWithPool(sp, cfg, "seg0", pool)
	require.NoError(t, err)
	t.Cleanup(hit.Free)
	require.NotSame(t, first, hit)
	require.Equal(t, want, scores(hit))
	require.Equal(t, 2, streams, "READY skips actual source stream for a new Segment")
	fallbackPool := newBaseFilePool(1, 1)
	t.Cleanup(fallbackPool.Close)
	fallback, err := loadFromStorageWithPool(sp, cfg, "seg0", fallbackPool)
	require.NoError(t, err)
	t.Cleanup(fallback.Free)
	require.Nil(t, fallback.mmapRelease, "ordinary fallback owns no pooled lease")
	require.True(t, fallback.mmapFallbackPinned)
	require.Equal(t, want, scores(fallback))
	require.Equal(t, 3, streams, "capacity fallback rematerializes source once")
}

func TestBaseFileCleanupFallbackUnlinkAfterUnmap(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("real directory EACCES requires a non-root uid")
	}
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	sp, cfg, buf := cleanupReworkSource(t, nil, "")
	owner := newBaseFileOwner(1, 1)
	p, err := owner.poolForSearch()
	require.NoError(t, err)
	selectMapping, restore, released := cleanupReworkFault(t, owner)
	seg, err := loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
	require.NoError(t, err)
	path := seg.mmapPath
	ptr := &seg.mmapData[0]
	selectMapping(seg.mmapData)
	restore()
	t.Cleanup(func() { _ = os.Chmod(dir, 0700); seg.Free(); _ = owner.close(); _ = os.Remove(path) })
	require.NoError(t, os.Chmod(dir, 0500))
	seg.Free()
	require.Nil(t, seg.mmapData, "actual munmap already succeeded")
	require.Equal(t, 1, released[ptr])
	require.FileExists(t, path)
	requireDeferredSegmentCharge(t, p, int64(len(buf))+estBytesPerDocHeap)
	require.ErrorIs(t, owner.close(), errBaseFileOwnerPending)
	require.Equal(t, 1, released[ptr], "path retry must not repeat successful munmap")
	p.mu.Lock()
	require.Equal(t, 1, p.liveFallbacks)
	require.Zero(t, p.openFiles)
	p.mu.Unlock()
	require.NoError(t, os.Chmod(dir, 0700))
	require.NoError(t, owner.close())
	require.Equal(t, 1, released[ptr])
	require.False(t, owner.hasResources())
	requireDeferredSegmentCharge(t, p, 0)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}
