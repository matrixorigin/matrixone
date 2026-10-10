// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build (darwin || linux) && fulltext2_base_file_reuse

package fulltext2

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/stretchr/testify/require"
)

func TestExperimentalOwnerCloseFinalizationExcludesNewGeneration(t *testing.T) {
	for _, first := range []string{"dedicated", "global"} {
		t.Run(first, func(t *testing.T) {
			withExperimentalOwnerState(t, nil, "", false)
			previousCache := veccache.Cache
			c := veccache.NewVectorIndexCache()
			veccache.Cache = c
			t.Cleanup(func() { veccache.Cache = previousCache })
			const service = "generation-finalization"
			oldToken, err := InitializeBaseFileReuseOwner(service)
			require.NoError(t, err)
			experimentalOwnerState.Lock()
			oldEntry := experimentalOwnerState.owners[service]
			experimentalOwnerState.Unlock()

			atFinalization, secondValidated := make(chan struct{}), make(chan struct{})
			releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
			var firstOnce, secondOnce sync.Once
			resumeFirst := func() { firstOnce.Do(func() { close(releaseFirst) }) }
			resumeSecond := func() { secondOnce.Do(func() { close(releaseSecond) }) }
			previousCurrent, previousFinalize := experimentalOwnerCurrentBarrier, experimentalOwnerFinalizeBarrier
			var validated, finalized atomic.Int32
			experimentalOwnerCurrentBarrier = func(entry *experimentalOwnerEntry) {
				if entry == oldEntry && validated.Add(1) == 2 {
					close(secondValidated)
					<-releaseSecond
				}
			}
			experimentalOwnerFinalizeBarrier = func(entry *experimentalOwnerEntry) {
				if entry == oldEntry && finalized.Add(1) == 1 {
					close(atFinalization)
					<-releaseFirst
				}
			}
			var workers sync.WaitGroup
			var next *BaseFileReuseOwnerToken
			// Independent rescue releases both controlled windows and joins every
			// closer before restoring hooks/cache or freeing any live mapping.
			t.Cleanup(func() {
				resumeFirst()
				resumeSecond()
				workers.Wait()
				experimentalOwnerCurrentBarrier, experimentalOwnerFinalizeBarrier = previousCurrent, previousFinalize
				_ = CloseBaseFileReuseOwner(oldToken)
				_ = CloseBaseFileReuseOwner(next)
				c.Destroy()
			})
			firstDone := make(chan error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				if first == "global" {
					c.Destroy()
					firstDone <- nil
				} else {
					firstDone <- CloseBaseFileReuseOwner(oldToken)
				}
			}()
			select {
			case <-atFinalization:
			case <-time.After(5 * time.Second):
				t.Fatal("first close did not reach registry finalization")
			}
			_, err = InitializeBaseFileReuseOwner(service)
			require.ErrorIs(t, err, errExperimentalOwnerShutdown, "restart must reject unfinished finalization")
			secondDone := make(chan error, 1)
			workers.Add(1)
			go func() { defer workers.Done(); secondDone <- CloseBaseFileReuseOwner(oldToken) }()

			// Old code validates the entry after A unlocked and before A finalized.
			// Fixed code holds the entry mutex until finalization. A durable mutex
			// stack only confirms B's phase; actual new cache/mapping survival is
			// the oracle, not a scheduling delay or a fake owner identity.
			crossed := false
			deadline := time.Now().Add(5 * time.Second)
			for {
				select {
				case <-secondValidated:
					crossed = true
				default:
				}
				if crossed || generationCloserWaitingOnMutex() {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("second closer reached neither current-check nor entry mutex")
				}
				runtime.Gosched()
			}
			resumeFirst()
			require.NoError(t, <-firstDone)
			if !crossed {
				require.NoError(t, <-secondDone, "queued old closer must become a stale no-op")
			}
			require.False(t, oldToken.owner.hasResources())
			require.True(t, oldToken.owner.isClosing())
			next, err = InitializeBaseFileReuseOwner(service)
			require.NoError(t, err)
			require.NotSame(t, oldToken.owner, next.owner)
			require.NotSame(t, oldToken, next)
			experimentalOwnerState.Lock()
			newEntry := experimentalOwnerState.owners[service]
			experimentalOwnerState.Unlock()
			require.NotSame(t, oldEntry, newEntry)

			sp, cfg, _ := cleanupReworkSource(t, nil, "")
			p, err := next.owner.poolForSearch()
			require.NoError(t, err)
			selectMapping, restore, released := cleanupReworkFault(t, next.owner)
			restore() // observe real successful munmap, without injecting a fault
			seg, err := loadFromStorageWithOwner(sp, cfg, "seg0", next.owner, p)
			require.NoError(t, err)
			selectMapping(seg.mmapData)
			ptr := &seg.mmapData[0]
			search := newFulltext2SearchWithBaseOwnerForService(cfg, next.owner, service)
			search.idx, search.loaded = NewIndex([]*Segment{seg}, nil), true
			entry := &veccache.VectorIndexSearch{Algo: search}
			entry.Cond = sync.NewCond(entry.Mutex.RLocker())
			entry.Status.Store(veccache.STATUS_LOADED)
			const key = "generation-finalization-entry"
			c.IndexMap.Store(key, entry)
			t.Cleanup(func() { resumeSecond(); workers.Wait(); c.Remove(key); seg.Free() })
			resumeSecond()
			if crossed {
				require.NoError(t, <-secondDone)
			}
			workers.Wait()
			resident, present := c.IndexMap.Load(key)
			require.True(t, present, "old current-entry closer destroyed the replacement generation")
			require.Same(t, entry, resident)
			require.Equal(t, int32(veccache.STATUS_LOADED), entry.Status.Load())
			require.NotNil(t, search.idx)
			require.NotNil(t, seg.mmapData)
			require.Zero(t, released[ptr], "old close must not unmap replacement Base")
			require.True(t, next.owner.hasResources())
			require.False(t, next.owner.isClosing())
			experimentalOwnerState.Lock()
			current, token := experimentalOwnerState.owners[service], experimentalOwnerState.owners[service].token
			experimentalOwnerState.Unlock()
			require.Same(t, newEntry, current)
			require.Same(t, next, token)
			require.NoError(t, CloseBaseFileReuseOwner(oldToken))
			require.False(t, next.owner.isClosing())
			require.NoError(t, CloseBaseFileReuseOwner(next))
			require.Nil(t, seg.mmapData)
			require.Equal(t, 1, released[ptr])
			require.False(t, next.owner.hasResources())
			_, present = c.IndexMap.Load(key)
			require.False(t, present)
			experimentalOwnerState.Lock()
			_, present = experimentalOwnerState.owners[service]
			experimentalOwnerState.Unlock()
			require.False(t, present)
			t.Log("controlled finalization/restart window: registry, token, cache and real mapping terminal verified; workers joined")
		})
	}
}

func generationCloserWaitingOnMutex() bool {
	stack := make([]byte, 1<<20)
	n := runtime.Stack(stack, true)
	for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
		if strings.Contains(goroutine, "closeExperimentalOwnerEntry(") &&
			(strings.Contains(goroutine, "internal/sync.(*Mutex).lockSlow(") || strings.Contains(goroutine, "sync.(*Mutex).lockSlow(")) {
			return true
		}
	}
	return false
}
