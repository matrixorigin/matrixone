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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestLocalFSReuseAdmissionBeforeDecode(t *testing.T) {
	ctx := context.Background()
	fs, err := NewLocalFS(ctx, "test", t.TempDir(), CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](1 << 20)}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "value", Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}}))
	read := func(policy Policy, size int64) *IOVector {
		return &IOVector{FilePath: "value", Policy: policy, Entries: []IOEntry{{Size: 3, CachedDataSize: size, ToCacheData: CacheOriginalData}}}
	}
	first := read(MemoryCacheReuseOnly, 3)
	require.NoError(t, fs.Read(ctx, first))
	require.True(t, first.Entries[0].cacheAdmissionBypass)
	require.Equal(t, "abc", string(first.Entries[0].CachedData.Bytes()))
	require.Zero(t, fs.memCache.cache.Used())
	require.Zero(t, fs.memCache.reservedBytes.Load(), "first touch must not reserve or evict payload capacity")
	first.Release()
	second := read(MemoryCacheReuseOnly, 3)
	require.NoError(t, fs.Read(ctx, second))
	require.False(t, second.Entries[0].cacheAdmissionBypass)
	require.Positive(t, fs.memCache.cache.Used())
	second.Release()
	third := read(MemoryCacheReuseOnly, 3)
	require.NoError(t, fs.Read(ctx, third))
	require.Same(t, fs.memCache, third.Entries[0].fromCache)
	third.Release()
	fs.memCache.Flush(ctx)
	again := read(MemoryCacheReuseOnly, 3)
	require.NoError(t, fs.Read(ctx, again))
	require.True(t, again.Entries[0].cacheAdmissionBypass, "Flush must discard reuse history")
	again.Release()
}

func TestReuseAdmissionRetryAndExplicitSkip(t *testing.T) {
	ctx := context.Background()
	cache := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	t.Cleanup(func() { cache.Close(ctx) })
	read := func(policy Policy) *IOVector {
		return &IOVector{FilePath: "value", Policy: policy, Entries: []IOEntry{{Size: 3, CachedDataSize: 3, ToCacheData: CacheOriginalData}}}
	}
	first := read(MemoryCacheReuseOnly)
	require.NoError(t, cache.Read(ctx, first))
	require.True(t, first.Entries[0].cacheAdmissionBypass)
	require.NoError(t, cache.Read(ctx, first))
	require.True(t, first.Entries[0].cacheAdmissionBypass, "retry is not a new observation")
	cache.Flush(ctx)
	skipped := read(MemoryCacheReuseOnly | SkipMemoryCacheWrites)
	require.NoError(t, cache.Read(ctx, skipped))
	next := read(MemoryCacheReuseOnly)
	require.NoError(t, cache.Read(ctx, next))
	require.True(t, next.Entries[0].cacheAdmissionBypass, "explicit skip must not populate history")
	unknown := read(MemoryCacheReuseOnly)
	unknown.Entries[0].CachedDataSize = 0
	require.NoError(t, cache.Read(ctx, unknown))
	require.True(t, unknown.Entries[0].cacheAdmissionBypass)
}

func TestReuseAdmissionIsDestinationScoped(t *testing.T) {
	ctx := context.Background()
	first := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	t.Cleanup(func() { first.Close(ctx) })
	second := NewMemCache(fscache.ConstCapacity(1<<20), nil, nil, "")
	t.Cleanup(func() { second.Close(ctx) })
	makeRead := func() *IOVector {
		return &IOVector{FilePath: "value", Policy: MemoryCacheReuseOnly, Entries: []IOEntry{{Size: 3, CachedDataSize: 3, ToCacheData: CacheOriginalData}}}
	}
	require.NoError(t, second.Read(ctx, makeRead()))
	v := makeRead()
	t.Cleanup(v.Release)
	require.NoError(t, first.Read(ctx, v))
	require.True(t, v.Entries[0].cacheAdmissionBypass)
	require.NoError(t, second.Read(ctx, v))
	require.False(t, v.Entries[0].cacheAdmissionBypass, "another cache's first touch must not override this destination's reuse")
	require.Same(t, second, v.Entries[0].cacheAdmissionOwner)
	require.NoError(t, decodeEntryBytes(ctx, v, []byte("abc"), second))
	require.NoError(t, second.Update(ctx, v, false))
	require.Positive(t, second.cache.Used())
	require.NoError(t, first.Update(ctx, v, false))
	require.Zero(t, first.cache.Used(), "another destination's decision cannot admit a cold payload")
}
