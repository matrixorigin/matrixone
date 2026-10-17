// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rscthrottler

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

// This test closes the pressure-controller -> actual FileService cache
// boundary. It does not simulate the host's 84 GiB residency discrepancy.
func TestMemThrottlerPhysicalHeadroomRealCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	const entrySize = 64 << 10
	const capacity = 4 * entrySize
	const owner = "local-headroom-consumer-probe"
	cache := fileservice.NewMemCache(fscache.ConstCapacity(capacity), nil, nil, owner)
	t.Cleanup(func() { cache.Close(context.WithoutCancel(ctx)) })
	t.Cleanup(func() { fileservice.ClearMemoryCachePressureTargetByOwner(owner) })

	write := func(i int) {
		t.Helper()
		vector := &fileservice.IOVector{
			FilePath: owner,
			Entries: []fileservice.IOEntry{{
				Offset: int64(i * entrySize), Size: entrySize,
				CachedData: cache.AllocateCacheData(ctx, entrySize),
			}},
		}
		defer vector.Release()
		for j := range vector.Entries[0].CachedData.Bytes() {
			vector.Entries[0].CachedData.Bytes()[j] = byte(i + 1)
		}
		require.NoError(t, cache.Update(ctx, vector, false))
	}
	read := func(i int) *fileservice.IOVector {
		t.Helper()
		vector := &fileservice.IOVector{
			FilePath: owner,
			Entries:  []fileservice.IOEntry{{Offset: int64(i * entrySize), Size: entrySize}},
		}
		release := sync.OnceFunc(vector.Release)
		t.Cleanup(release)
		if err := cache.Read(ctx, vector); err != nil {
			release()
			t.Fatal(err)
		}
		return vector
	}
	for i := 0; i < 4; i++ {
		write(i)
	}
	// Keep an actual cached buffer pinned across eviction, as an executing read
	// would. Releasing the cache's ownership must not invalidate this reference.
	pinned := read(0)
	require.NotNil(t, pinned.Entries[0].CachedData)
	want := bytes.Repeat([]byte{1}, entrySize)
	require.Equal(t, want, pinned.Entries[0].CachedData.Bytes())

	oldFree := freeOSMemory
	completed := make(chan struct{}, 1)
	freeOSMemory = func() { completed <- struct{}{} }
	t.Cleanup(func() { freeOSMemory = oldFree })
	results := make(chan map[string]int64, 1)
	m := &memThrottler{}
	m.options.enableRSSScavenging = true
	m.options.rssCacheTargetSetter = func(percent int64) {
		fileservice.SetMemoryCachePressureTargetPercentByOwner(owner, percent, time.Now().Add(2*time.Minute))
	}
	m.options.rssCacheTargetClearer = func() { fileservice.ClearMemoryCachePressureTargetByOwner(owner) }
	m.options.rssCacheEvictor = func(evictCtx context.Context, percent int64) {
		results <- fileservice.EvictMemoryCachesToCapacityPercent(evictCtx, percent)
	}
	m.actualTotalMemory.Store(84 * mpool.GB)
	m.rss.Store(62 * mpool.GB)
	m.limit.Store(80 * mpool.GB)
	m.reserved.Store(60 * mpool.GB) // only the async cache path calls the stub
	now := int64(2 * time.Minute)
	m.tryScavengeRSS(now, 62*mpool.GB, 4*mpool.GB, true)
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("real cache eviction did not complete", ctx.Err())
	}
	result := <-results
	used, ok := result[owner]
	require.True(t, ok, "the registered real cache must be selected")
	require.LessOrEqual(t, used, int64(capacity/2))
	require.Equal(t, want, pinned.Entries[0].CachedData.Bytes(), "active reader must retain its payload")

	write(4)
	require.Nil(t, read(4).Entries[0].CachedData, "pressure must prevent immediate refill past target")
	m.tryScavengeRSS(now+int64(time.Second), 62*mpool.GB, 25*mpool.GB, true)
	require.Equal(t, rssPressureNone, rssPressureState(m.rssPressureState.Load()))
	write(5)
	recovered := read(5)
	require.NotNil(t, recovered.Entries[0].CachedData, "cleared target must permit normal cache admission")
	require.Equal(t, bytes.Repeat([]byte{6}, entrySize), recovered.Entries[0].CachedData.Bytes())
	t.Logf("real cache: capacity=%d, used-after-eviction=%d; pin preserved, pressure admission rejected, recovery admitted", capacity, used)
}
