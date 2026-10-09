// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build (darwin || linux) && fulltext2_base_file_reuse

package fulltext2

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func cleanupReworkSource(t *testing.T, payload []byte, checksum string) (*sqlexec.SqlProcess, TableConfig, []byte) {
	sp, mp := mockSqlProcWithIdentity(t, "cleanup-rework")
	if payload == nil {
		b := NewBuilder("seg0", int32(types.T_int64))
		feed(t, b, int64(1), "hello", "world")
		s, err := b.Finish()
		require.NoError(t, err)
		payload, err = s.Serialize()
		require.NoError(t, err)
	}
	if checksum == "" {
		checksum = vectorindex.CheckSumFromBuffer(payload)
	}
	swapRunSql(t, func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, checksum, int64(len(payload)), 7)}}, nil
	})
	swapRunStreamingSql(t, func(_ context.Context, _ *sqlexec.SqlProcess, _ string, sc chan executor.Result, _ chan error) (executor.Result, error) {
		sc <- executor.Result{Mp: mp, Batches: []*batch.Batch{chunkBatch(mp, payload)}}
		return executor.Result{}, nil
	})
	return sp, testStorageCfg(), payload
}

// Retain an independent address ledger even on the old candidate's owner-loss
// path. Cleanup restores munmap, retries the owner, then rescues only mappings
// which were never successfully released; this test cannot itself leak.
func cleanupReworkFault(t *testing.T, owner *baseFileOwner) (func([]byte), func(), map[*byte]int) {
	original := munmapFn
	selected := map[*byte][]byte{}
	released := map[*byte]int{}
	failing := true
	munmapFn = func(data []byte) error {
		ptr := &data[0]
		if _, ok := selected[ptr]; ok && failing {
			return errors.New("selected cleanup fault")
		}
		err := original(data)
		if err == nil {
			released[ptr]++
		}
		return err
	}
	t.Cleanup(func() {
		defer func() { munmapFn = original }()
		munmapFn = func(data []byte) error {
			err := original(data)
			if err == nil {
				released[&data[0]]++
			}
			return err
		}
		_ = owner.close()
		for ptr, data := range selected {
			if released[ptr] == 0 {
				require.NoError(t, original(data))
				released[ptr]++
			}
		}
		munmapFn = original
	})
	return func(data []byte) { selected[&data[0]] = data; released[&data[0]] = 0 }, func() { failing = false }, released
}

func cleanupReworkEvict(t *testing.T, cfg TableConfig, owner *baseFileOwner, seg *Segment) {
	c := veccache.NewVectorIndexCache()
	s := newFulltext2SearchWithBaseOwner(cfg, owner)
	s.idx = NewIndex([]*Segment{seg}, nil)
	s.loaded = true
	entry := &veccache.VectorIndexSearch{Algo: s}
	entry.Cond = sync.NewCond(entry.Mutex.RLocker())
	entry.Status.Store(veccache.STATUS_LOADED)
	c.IndexMap.Store("cleanup", entry)
	c.Remove("cleanup")
	_, exists := c.IndexMap.Load("cleanup")
	require.False(t, exists)
	require.Nil(t, s.idx)
}

func TestBaseFileCleanupReadyEvictionRejectsUnchargedReload(t *testing.T) {
	sp, cfg, buf := cleanupReworkSource(t, nil, "")
	owner := newBaseFileOwner(int64(len(buf))*2, 2)
	p, err := owner.poolForSearch()
	require.NoError(t, err)
	selectMapping, restore, released := cleanupReworkFault(t, owner)
	first, err := loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
	require.NoError(t, err)
	t.Cleanup(first.Free)
	second, err := loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
	require.NoError(t, err)
	t.Cleanup(second.Free)
	selectMapping(first.mmapData)
	selectMapping(second.mmapData)
	ptr1, ptr2 := &first.mmapData[0], &second.mmapData[0]
	cleanupReworkEvict(t, cfg, owner, first)
	for i := 0; i < 3; i++ {
		var extra *Segment
		var err error
		if i%2 == 0 {
			extra, err = loadFromStorageWithPool(sp, cfg, "seg0", p)
		} else {
			extra, err = loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
		}
		if extra != nil {
			extra.Free()
		}
		require.Error(t, err, "READY reload must reject while an evicted mapping remains owned")
	}
	cleanupReworkEvict(t, cfg, owner, second)
	require.Equal(t, 2, p.deferredCount())
	require.ErrorIs(t, owner.close(), errBaseFileOwnerPending)
	require.Zero(t, released[ptr1])
	require.Zero(t, released[ptr2])
	requireDeferredSegmentCharge(t, p, 2*(int64(len(buf))+estBytesPerDocHeap))
	restore()
	require.NoError(t, owner.close())
	require.False(t, owner.hasResources())
	requireDeferredSegmentCharge(t, p, 0)
	require.Equal(t, 1, released[ptr1])
	require.Equal(t, 1, released[ptr2])
	require.NoError(t, owner.close())
	require.Equal(t, 1, released[ptr1])
	require.Equal(t, 1, released[ptr2])
}

func TestBaseFileCleanupCapacityFallbackEvictionRetainsOwner(t *testing.T) {
	sp, cfg, buf := cleanupReworkSource(t, nil, "")
	owner := newBaseFileOwner(1, 1)
	previousCache := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache = previousCache })
	withExperimentalOwnerState(t, owner, "cleanup-rework", false)
	token, err := InitializeBaseFileReuseOwner("cleanup-rework")
	require.NoError(t, err)
	p, err := owner.poolForSearch()
	require.NoError(t, err)
	selectMapping, restore, released := cleanupReworkFault(t, owner)
	seg, err := loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
	require.NoError(t, err)
	require.Equal(t, len(buf), len(seg.mmapData))
	require.NotEmpty(t, seg.mmapPath)
	path := seg.mmapPath
	ptr := &seg.mmapData[0]
	selectMapping(seg.mmapData)
	t.Cleanup(func() { _ = os.Remove(path) })
	cleanupReworkEvict(t, cfg, owner, seg)
	require.ErrorIs(t, owner.close(), errBaseFileOwnerPending, "fallback must prevent false clean owner shutdown")
	require.Equal(t, 1, p.deferredCount())
	require.ErrorIs(t, CloseBaseFileReuseOwner(token), errBaseFileOwnerPending)
	_, err = InitializeBaseFileReuseOwner("cleanup-rework")
	require.ErrorIs(t, err, errBaseFileOwnerPending)
	require.Zero(t, released[ptr])
	requireDeferredSegmentCharge(t, p, int64(len(buf))+estBytesPerDocHeap)
	restore()
	require.NoError(t, owner.close())
	require.False(t, owner.hasResources())
	requireDeferredSegmentCharge(t, p, 0)
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	require.NoError(t, owner.close())
	require.Equal(t, 1, released[ptr])
	require.NoError(t, CloseBaseFileReuseOwner(token))
	nextToken, err := InitializeBaseFileReuseOwner("cleanup-rework")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, CloseBaseFileReuseOwner(nextToken)) })
	next := NewFulltext2SearchForExecution(cfg, "cleanup-rework")
	require.NotSame(t, owner, next.baseOwner)
	nextPool, err := next.baseOwner.poolForSearch()
	require.NoError(t, err)
	nextSeg, err := loadFromStorageWithOwner(sp, cfg, "seg0", next.baseOwner, nextPool)
	require.NoError(t, err)
	t.Cleanup(nextSeg.Free)
	require.NoError(t, CloseBaseFileReuseOwner(token))
	df, ok := nextSeg.lookupLoadedDF("hello")
	require.True(t, ok)
	require.Equal(t, 1, df)
	nextSeg.Free()
	require.NoError(t, CloseBaseFileReuseOwner(nextToken))
}

// Validation errors occur before a Segment can be published to Search. Capture
// the actual mapping at its first attempted release, never a fake Segment.
func TestBaseFileCleanupFallbackValidationRetainsOwner(t *testing.T) {
	for _, name := range []string{"checksum", "decode"} {
		t.Run(name, func(t *testing.T) {
			var payload []byte
			checksum := "mismatch"
			if name == "decode" {
				payload = []byte("not a serialized segment")
				checksum = ""
			}
			sp, cfg, _ := cleanupReworkSource(t, payload, checksum)
			owner := newBaseFileOwner(1, 1)
			p, err := owner.poolForSearch()
			require.NoError(t, err)
			selectMapping, restore, released := cleanupReworkFault(t, owner)
			previous := munmapFn
			var ptr *byte
			munmapFn = func(data []byte) error {
				if ptr == nil {
					ptr = &data[0]
					selectMapping(data)
				}
				return previous(data)
			}
			_, err = loadFromStorageWithOwner(sp, cfg, "seg0", owner, p)
			require.Error(t, err)
			require.NotNil(t, ptr)
			require.Equal(t, 1, p.deferredCount())
			require.Positive(t, deferredSegmentCharge(p))
			require.ErrorIs(t, owner.close(), errBaseFileOwnerPending)
			restore()
			require.NoError(t, owner.close())
			requireDeferredSegmentCharge(t, p, 0)
			require.Equal(t, 1, released[ptr])
			require.False(t, owner.hasResources())
		})
	}
}

func TestBaseFileCleanupFallbackFullSearchFailureRetainsOwner(t *testing.T) {
	for _, mode := range []string{"partial-base", "cancel-after-base"} {
		t.Run(mode, func(t *testing.T) {
			sp, cfg, buf := cleanupReworkSource(t, nil, "")
			_, mp := mockSqlProc(t)
			ctx, cancel := context.WithCancel(sp.GetTopContext())
			defer cancel()
			sp = sp.WithContext(ctx)
			owner := newBaseFileOwner(1, 1)
			p, err := owner.poolForSearch()
			require.NoError(t, err)
			selectMapping, restore, released := cleanupReworkFault(t, owner)
			previous := munmapFn
			var ptr *byte
			munmapFn = func(data []byte) error {
				if ptr == nil {
					ptr = &data[0]
					selectMapping(data)
				}
				return previous(data)
			}
			swapRunSql(t, func(_ *sqlexec.SqlProcess, sql string) (executor.Result, error) {
				switch {
				case strings.Contains(sql, "SUM("):
					return executor.Result{}, nil
				case strings.Contains(sql, "checksum"):
					if strings.Contains(sql, "'s1'") {
						return executor.Result{}, errors.New("second base unavailable")
					}
					return executor.Result{Mp: mp, Batches: []*batch.Batch{metaBatch(mp, vectorindex.CheckSumFromBuffer(buf), int64(len(buf)), 7)}}, nil
				case strings.Contains(sql, "SELECT index_id"):
					if mode == "partial-base" {
						return executor.Result{Mp: mp, Batches: []*batch.Batch{twoIdBatch(mp, "seg0", "s1")}}, nil
					}
					return executor.Result{Mp: mp, Batches: []*batch.Batch{twoIdBatch(mp, "seg0", "seg0")}}, nil
				default:
					if mode == "cancel-after-base" {
						cancel()
						return executor.Result{}, context.Canceled
					}
					return executor.Result{}, nil
				}
			})
			s := newFulltext2SearchWithBaseOwner(cfg, owner)
			t.Cleanup(s.Destroy)
			err = s.Load(sp)
			require.Error(t, err)
			require.Nil(t, s.idx)
			require.False(t, s.loaded)
			require.NotNil(t, ptr)
			require.Positive(t, p.deferredCount())
			require.ErrorIs(t, owner.close(), errBaseFileOwnerPending)
			restore()
			require.NoError(t, owner.close())
			requireDeferredSegmentCharge(t, p, 0)
			require.Equal(t, 1, released[ptr])
			require.False(t, owner.hasResources())
		})
	}
}

// Reflection keeps the exact regression source executable on the original
// candidate, which lacked this field; absence is zero rather than a PASS.
func deferredSegmentCharge(p *baseFilePool) int64 {
	v := reflect.ValueOf(p).Elem().FieldByName("deferredSegmentBytes")
	if !v.IsValid() {
		return 0
	}
	return v.Int()
}
func requireDeferredSegmentCharge(t *testing.T, p *baseFilePool, want int64) {
	t.Helper()
	require.Equal(t, want, deferredSegmentCharge(p))
}
