// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestPrivateCacheDataAllocatorCopiesAndReleasesFreshBacking(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(func() int64 { return 1 << 20 }, nil, nil, t.Name())
	defer cache.Close(ctx)
	for _, tc := range []struct {
		name      string
		allocator CacheDataAllocator
	}{
		{name: "normal", allocator: cache},
		{name: "no-evict", allocator: &cache.nonEvictingDecoder},
		{name: "transient", allocator: &cache.transientDecoder},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte("private JSON cache source")
			data := NewPrivateCacheDataAllocator(tc.allocator).CopyToCacheData(ctx, raw)
			buffer, ok := data.(*Bytes)
			require.True(t, ok)
			require.True(t, buffer.doNotReuse)
			raw[0] = 'X'
			require.Equal(t, "private JSON cache source", string(data.Bytes()))
			data.Release()
			require.Zero(t, cache.reservedBytes.Load())
			require.Zero(t, cache.backingBytes.Load())
		})
	}
}

func TestLocalFSNoEvictScanRetainsFittingPrefix(t *testing.T) {
	ctx := context.Background()
	backing := DefaultCacheDataAllocator().BackingSize(3)
	fs, err := NewLocalFS(ctx, "test", t.TempDir(), CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](toml.ByteSize(2 * backing))}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	for _, path := range []string{"a", "b", "c", "d"} {
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: path, Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}}))
	}
	for pass := range 3 {
		for index, path := range []string{"a", "b", "c", "d"} {
			func() {
				// The estimate deliberately exceeds capacity. Actual allocation size
				// governs admission, including on the very first touch.
				v := &IOVector{FilePath: path, Policy: MemoryCacheNoEvict, Entries: []IOEntry{{Size: 3, CachedDataSize: 1 << 20, ToCacheData: CacheOriginalData}}}
				defer v.Release()
				require.NoError(t, fs.Read(ctx, v))
				require.Equal(t, "abc", string(v.Entries[0].CachedData.Bytes()))
				if pass > 0 && index < 2 {
					require.Same(t, fs.memCache, v.Entries[0].fromCache)
				}
				if index >= 2 {
					require.False(t, fs.memCache.cacheAdmissionAllowed(v.Entries[0].CachedData))
				}
			}()
		}
		require.Equal(t, int64(2*backing), fs.memCache.cache.Used())
		require.Equal(t, int64(2*backing), fs.memCache.backingBytes.Load())
	}
}

func TestNoEvictAllocationCompetingReservations(t *testing.T) {
	ctx := context.Background()
	const request = 256
	cache := NewMemCache(fscache.ConstCapacity(int64(DefaultCacheDataAllocator().BackingSize(request))), nil, nil, "")
	t.Cleanup(func() { cache.Close(ctx) })
	start, results := make(chan struct{}), make(chan fscache.Data, 8)
	for range 8 {
		go func() {
			<-start
			results <- cache.nonEvictingDecoder.AllocateCacheDataWithHint(ctx, request, malloc.NoClear|malloc.DoNotReuse)
		}()
	}
	close(start)
	held := make([]fscache.Data, 0, 8)
	t.Cleanup(func() {
		for _, data := range held {
			data.Release()
		}
	})
	admitted := 0
	for range 8 {
		data := <-results
		held = append(held, data)
		if cache.cacheAdmissionAllowed(data) {
			admitted++
		}
	}
	require.Equal(t, 1, admitted)
	require.Equal(t, int64(cache.BackingSize(request)), cache.backingBytes.Load())
	for _, data := range held {
		data.Release()
	}
	held = nil
	require.Zero(t, cache.backingBytes.Load())
	require.Zero(t, cache.reservedBytes.Load())
}

func TestNoEvictKnownRefusalDoesNotAdvertiseMemoryFill(t *testing.T) {
	ctx := t.Context()
	cache := NewMemCache(fscache.ConstCapacity(4096), nil, nil, "")
	t.Cleanup(func() { cache.Close(ctx) })
	fs := &S3FS{memCache: cache}
	v := &IOVector{FilePath: "value", Policy: MemoryCacheNoEvict,
		Entries: []IOEntry{{Size: 3, CachedDataSize: 3, ToCacheData: CacheOriginalData}}}
	_, eligible := fs.singleEntryRangeMergeKey(v)
	require.True(t, eligible)
	data := cache.AllocateCacheDataWithHint(ctx, 4096, malloc.DoNotReuse)
	release := sync.OnceFunc(data.Release)
	t.Cleanup(release)
	seed := &IOVector{FilePath: "resident", Entries: []IOEntry{{Size: 4096, CachedData: data}}}
	require.NoError(t, cache.Update(ctx, seed, false))
	release()
	require.True(t, cache.cannotReserveWithoutEviction(3))
	_, eligible = fs.singleEntryRangeMergeKey(v)
	require.False(t, eligible, "a transient read cannot publish to memory for a waiting I/O follower")
	v.Policy = 0
	_, eligible = fs.singleEntryRangeMergeKey(v)
	require.True(t, eligible, "ordinary fills retain their existing eviction permission")
	cache.Flush(ctx)
	pending := cache.AllocateCacheDataWithHint(ctx, 4096, malloc.DoNotReuse)
	defer pending.Release()
	require.False(t, cache.cannotReserveWithoutEviction(3), "an existing pending fill may still accept a join")
}

func TestNoEvictAllocationCountsEvictedHeldBacking(t *testing.T) {
	ctx := context.Background()
	const request = 256
	cache := NewMemCache(fscache.ConstCapacity(int64(DefaultCacheDataAllocator().BackingSize(request))), nil, nil, "")
	t.Cleanup(func() { cache.Close(ctx) })
	data := cache.CopyToCacheData(ctx, make([]byte, request))
	v := &IOVector{FilePath: "held", Entries: []IOEntry{{Size: request, CachedData: data}}}
	release := sync.OnceFunc(v.Release)
	t.Cleanup(release)
	require.NoError(t, cache.Update(ctx, v, false))
	cache.EvictToTarget(ctx, 0)
	require.Zero(t, cache.cache.Used())
	require.Equal(t, data.Capacity(), cache.backingBytes.Load())
	blocked := cache.nonEvictingDecoder.AllocateCacheDataWithHint(ctx, request, malloc.DoNotReuse)
	releaseBlocked := sync.OnceFunc(blocked.Release)
	t.Cleanup(releaseBlocked)
	require.False(t, cache.cacheAdmissionAllowed(blocked))
	releaseBlocked()
	release()
	require.Zero(t, cache.backingBytes.Load())
	available := cache.nonEvictingDecoder.AllocateCacheDataWithHint(ctx, request, malloc.DoNotReuse)
	releaseAvailable := sync.OnceFunc(available.Release)
	t.Cleanup(releaseAvailable)
	require.True(t, cache.cacheAdmissionAllowed(available))
	releaseAvailable()
	require.Zero(t, cache.backingBytes.Load())
}

func TestNoEvictPublicationRefusesPressureShrinkAndForeignData(t *testing.T) {
	ctx := context.Background()
	for _, reason := range []string{"pressure", "shrink", "foreign"} {
		t.Run(reason, func(t *testing.T) {
			var capacity atomic.Int64
			capacity.Store(4096)
			cache := NewMemCache(capacity.Load, nil, nil, "")
			t.Cleanup(func() { cache.Close(ctx) })
			data := cache.nonEvictingDecoder.AllocateCacheDataWithHint(ctx, 256, malloc.DoNotReuse)
			switch reason {
			case "pressure":
				SetMemoryCachePressureTargetPercentByOwner(t.Name(), 50, time.Now().Add(time.Hour))
				t.Cleanup(func() { ClearMemoryCachePressureTargetByOwner(t.Name()) })
			case "shrink":
				capacity.Store(1)
			case "foreign":
				data.Release()
				data = DefaultCacheDataAllocator().AllocateCacheData(ctx, 256)
			}
			defer data.Release()
			v := &IOVector{FilePath: "value", Policy: MemoryCacheNoEvict, Entries: []IOEntry{{Size: 256, CachedData: data}}}
			require.NoError(t, cache.Update(ctx, v, false))
			require.Zero(t, cache.cache.Used())
		})
	}
}

func TestNoEvictAllocationFailureAndDecodeCancellation(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(fscache.ConstCapacity(4096), nil, nil, "")
	t.Cleanup(func() { cache.Close(ctx) })
	original := cache.allocator.allocator
	cache.allocator.allocator = oomExactAllocator{}
	require.Panics(t, func() { cache.nonEvictingDecoder.AllocateCacheData(ctx, 256).Release() })
	require.Zero(t, cache.backingBytes.Load())
	require.Zero(t, cache.reservedBytes.Load())
	cache.allocator.allocator = original
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	v := &IOVector{FilePath: "cancel", Policy: MemoryCacheNoEvict, Entries: []IOEntry{{Size: 3, ToCacheData: func(ctx context.Context, _ io.Reader, raw []byte, allocator CacheDataAllocator) (fscache.Data, error) {
		data := allocator.AllocateCacheDataWithHint(ctx, len(raw), malloc.DoNotReuse)
		cancel()
		return data, nil
	}}}}
	require.NoError(t, cache.Read(ctx, v))
	require.ErrorIs(t, decodeEntryBytes(canceled, v, []byte("abc"), cache), context.Canceled)
	require.Zero(t, cache.backingBytes.Load())
	require.Zero(t, cache.reservedBytes.Load())
	require.Nil(t, v.Entries[0].CachedData)
}

func TestNoEvictExplicitSkipAndDestination(t *testing.T) {
	ctx := context.Background()
	first := NewMemCache(fscache.ConstCapacity(4096), nil, nil, "")
	second := NewMemCache(fscache.ConstCapacity(4096), nil, nil, "")
	t.Cleanup(func() { first.Close(ctx); second.Close(ctx) })
	for _, policy := range []Policy{MemoryCacheNoEvict, MemoryCacheNoEvict | SkipMemoryCacheWrites} {
		func() {
			v := &IOVector{FilePath: "value", Policy: policy, Entries: []IOEntry{{Size: 3, ToCacheData: CacheOriginalData}}}
			defer v.Release()
			require.NoError(t, first.Read(ctx, v))
			require.NoError(t, decodeEntryBytes(ctx, v, []byte("abc"), first))
			require.NoError(t, second.Update(ctx, v, false))
			require.Zero(t, second.cache.Used())
			require.Zero(t, second.backingBytes.Load())
			require.NoError(t, first.Update(ctx, v, false))
			if policy.Any(SkipMemoryCacheWrites) {
				require.Zero(t, first.cache.Used())
			} else {
				require.Positive(t, first.cache.Used())
			}
		}()
		first.Flush(ctx)
	}
	closed := &IOVector{FilePath: "closed", Policy: MemoryCacheNoEvict, Entries: []IOEntry{{Size: 3, ToCacheData: func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
		return nil, moerr.NewInternalErrorNoCtx("decode failure")
	}}}}
	require.NoError(t, first.Read(ctx, closed))
	require.Error(t, decodeEntryBytes(ctx, closed, []byte("abc"), first))
	require.Zero(t, first.backingBytes.Load())
}

func TestNoEvictSharedDecodeDiskAndByteConversion(t *testing.T) {
	for _, path := range []string{"disk", "bytes"} {
		t.Run(path, func(t *testing.T) {
			ctx := t.Context()
			fs := newSelectedDecodeFS(t, true)
			seed := &IOVector{FilePath: "resident", Entries: []IOEntry{{Size: 128 << 10,
				CachedData: fs.memCache.AllocateCacheData(ctx, 128<<10)}}}
			releaseSeed := sync.OnceFunc(seed.Release)
			t.Cleanup(releaseSeed)
			require.NoError(t, fs.memCache.Update(ctx, seed, false))
			releaseSeed()
			require.NoError(t, fs.diskCache.Update(ctx, &IOVector{FilePath: "columns", Policy: SkipFullFilePreloads,
				Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}}, false))
			var conversions atomic.Int32
			vector := func(policy Policy) *IOVector {
				return &IOVector{FilePath: "columns", Policy: policy | SkipFullFilePreloads,
					Entries: []IOEntry{{Size: 3, CachedDataSize: 3, DecodeSharing: DecodeSharing{Codec: "no-evict-test"},
						ToCacheData: func(ctx context.Context, _ io.Reader, raw []byte, allocator CacheDataAllocator) (fscache.Data, error) {
							conversions.Add(1)
							return allocator.CopyToCacheData(ctx, raw), nil
						}}}}
			}
			read := func(v *IOVector) {
				if path == "bytes" {
					require.NoError(t, fs.DecodeFromBytes(ctx, v, []byte("abc")))
				} else {
					require.NoError(t, fs.Read(ctx, v))
					require.Same(t, fs.diskCache, v.Entries[0].fromCache)
				}
			}
			first, second := vector(MemoryCacheNoEvict), vector(MemoryCacheNoEvict)
			t.Cleanup(first.Release)
			t.Cleanup(second.Release)
			read(first)
			read(second)
			require.Same(t, first.Entries[0].CachedData, second.Entries[0].CachedData)
			require.Equal(t, int32(1), conversions.Load())
			require.Equal(t, "abc", string(second.Entries[0].CachedData.Bytes()))
			require.False(t, fs.memCache.cacheAdmissionAllowed(second.Entries[0].CachedData))
			require.Equal(t, int64(128<<10), fs.memCache.cache.Used())
			normal := vector(0)
			t.Cleanup(normal.Release)
			read(normal)
			require.Equal(t, int32(2), conversions.Load(), "ordinary conversion cannot inherit a transient result")
			require.True(t, fs.memCache.cacheAdmissionAllowed(normal.Entries[0].CachedData))
		})
	}
}
