// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

func byteDecodeVector() IOVector {
	return IOVector{FilePath: "absent", Policy: SkipFullFilePreloads, Entries: []IOEntry{{Size: 3, CachedDataSize: 3,
		ToCacheData: CacheOriginalData, DecodeSharing: DecodeSharing{Codec: "test-copy"}}}}
}

func TestDecodeFromBytesCacheAndSharing(t *testing.T) {
	fs := newSelectedDecodeFS(t, false)
	// The file deliberately does not exist: every path must remain storage-free.
	first := byteDecodeVector()
	require.NoError(t, DecodeFromBytes(t.Context(), fs, &first, []byte("abc")))
	defer first.ReleaseReadResultOnError()
	second := byteDecodeVector()
	require.NoError(t, DecodeFromBytes(t.Context(), fs, &second, []byte("abc")))
	defer second.ReleaseReadResultOnError()
	require.Same(t, first.Entries[0].CachedData, second.Entries[0].CachedData)
	require.True(t, second.Entries[0].WasFromCache())
	first.ReleaseReadResultOnError()
	second.ReleaseReadResultOnError()
	fs.FlushCache(t.Context())
	pinned := fs.AllocateCacheData(t.Context(), 128<<10)
	defer pinned.Release()
	services, err := NewFileServices(fs.Name(), fs)
	require.NoError(t, err)
	sub := SubPath(services, "prefix")
	first, second = byteDecodeVector(), byteDecodeVector()
	require.NoError(t, DecodeFromBytes(t.Context(), sub, &first, []byte("abc")))
	second.FilePath = "prefix/absent"
	require.NoError(t, DecodeFromBytes(t.Context(), fs, &second, []byte("abc")))
	require.Same(t, first.Entries[0].CachedData, second.Entries[0].CachedData)
	require.NotNil(t, second.Entries[0].decodeLease)
	first.ReleaseReadResultOnError()
	second.ReleaseReadResultOnError()
	requireDecodeDrained(t, fs.decodedReads)

	fs.memCache.cache = &failingDecodeCache{DataCache: fs.memCache.cache}
	// Skip-memory policy bypasses both admission and sharing, but still converts.
	v := byteDecodeVector()
	v.Policy |= SkipMemoryCache
	require.NoError(t, DecodeFromBytes(t.Context(), fs, &v, []byte("abc")))
	require.Nil(t, v.Entries[0].decodeLease)
	v.Release()
}

func TestDecodeFromBytesErrors(t *testing.T) {
	fs := newSelectedDecodeFS(t, false)
	for _, change := range []func(*IOVector){
		func(v *IOVector) { v.Entries = nil },
		func(v *IOVector) { v.Caches = []IOVectorCache{nil} },
		func(v *IOVector) { v.Entries[0].Size++ },
		func(v *IOVector) { v.Entries[0].ToCacheData = nil },
		func(v *IOVector) { v.Entries[0].Data = []byte("abc") },
	} {
		v := byteDecodeVector()
		change(&v)
		require.Error(t, DecodeFromBytes(t.Context(), fs, &v, []byte("abc")))
	}
	require.Error(t, fs.DecodeFromBytes(t.Context(), nil, nil))
	for _, resultErr := range []error{nil, errors.New("injected conversion error")} {
		v := byteDecodeVector()
		v.Entries[0].ToCacheData = func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
			return nil, resultErr
		}
		require.Error(t, DecodeFromBytes(t.Context(), fs, &v, []byte("abc")))
		require.Nil(t, v.Entries[0].CachedData)
		requireDecodeDrained(t, fs.decodedReads)
	}
	fs.memCache.cache = &failingDecodeCache{DataCache: fs.memCache.cache}
	v := byteDecodeVector()
	require.ErrorContains(t, DecodeFromBytes(t.Context(), fs, &v, []byte("abc")), "cache update failure")
	require.Nil(t, v.Entries[0].CachedData)
	requireDecodeDrained(t, fs.decodedReads)
	// No capability: use owned conversion and do not call even ReadCache.
	v = byteDecodeVector()
	require.NoError(t, DecodeFromBytes(t.Context(), nil, &v, []byte("abc")))
	require.Equal(t, []byte("abc"), v.Entries[0].CachedData.Bytes())
	v.Release()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	owned := NewBytes([]byte("abc"))
	v = byteDecodeVector()
	v.Entries[0].ToCacheData = func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
		cancel()
		return owned, nil
	}
	require.ErrorIs(t, DecodeFromBytes(ctx, nil, &v, []byte("abc")), context.Canceled)
	require.Zero(t, owned.refs.Load())
}

func TestDecodeFromBytesConcurrent(t *testing.T) {
	fs := newSelectedDecodeFS(t, false)
	pin := fs.AllocateCacheData(t.Context(), 128<<10)
	defer pin.Release()
	var conversions atomic.Int32
	vectors := make([]IOVector, 8)
	done := make(chan error, len(vectors))
	for i := range vectors {
		vectors[i] = byteDecodeVector()
		vectors[i].Entries[0].ToCacheData = func(ctx context.Context, r io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
			conversions.Add(1)
			return CacheOriginalData(ctx, r, data, a)
		}
		go func() { done <- DecodeFromBytes(t.Context(), fs, &vectors[i], []byte("abc")) }()
	}
	var firstErr error
	for range vectors {
		if err := <-done; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	t.Cleanup(func() {
		for i := range vectors {
			vectors[i].ReleaseReadResultOnError()
		}
	})
	require.NoError(t, firstErr)
	require.Equal(t, int32(1), conversions.Load())
	owner := vectors[0].Entries[0].CachedData
	for i := range vectors {
		require.Same(t, owner, vectors[i].Entries[0].CachedData)
		vectors[i].ReleaseReadResultOnError()
	}
	requireDecodeDrained(t, fs.decodedReads)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	v := byteDecodeVector()
	require.ErrorIs(t, DecodeFromBytes(ctx, fs, &v, []byte("abc")), context.Canceled)
	require.Zero(t, fs.decodedReads.reads)
}

func TestDecodeFromBytesClose(t *testing.T) {
	t.Run("shared", func(t *testing.T) { testDecodeFromBytesClose(t, true) })
	t.Run("unshared", func(t *testing.T) { testDecodeFromBytesClose(t, false) })
}

func testDecodeFromBytesClose(t *testing.T, shared bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fs := newSelectedDecodeFS(t, false)
	entered, unblock := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	v := byteDecodeVector()
	if !shared {
		v.Entries[0].DecodeSharing = DecodeSharing{}
	}
	v.Entries[0].ToCacheData = func(ctx context.Context, r io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
		close(entered)
		<-unblock
		return CacheOriginalData(ctx, r, data, a)
	}
	var readErr error
	done := make(chan struct{})
	go func() { readErr = DecodeFromBytes(ctx, fs, &v, []byte("abc")); close(done) }()
	t.Cleanup(func() { release(); <-done; v.ReleaseReadResultOnError() })
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("conversion did not start")
	}
	closed := make(chan struct{})
	go func() { fs.Close(ctx); close(closed) }()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("Close waited for conversion")
	}
	require.False(t, fs.memCache.closed.Load())
	release()
	<-done
	if shared {
		require.ErrorContains(t, readErr, "closed")
		require.Nil(t, v.Entries[0].CachedData)
	} else {
		require.NoError(t, readErr)
		require.Equal(t, []byte("abc"), v.Entries[0].CachedData.Bytes())
		v.ReleaseReadResultOnError()
	}
	require.True(t, fs.memCache.closed.Load())
	requireDecodeDrained(t, fs.decodedReads)
}

func TestReleaseReadBuffers(t *testing.T) {
	releases := 0
	v := IOVector{Entries: []IOEntry{
		{Data: []byte("raw"), CachedData: NewBytes([]byte("owned")), ToCacheData: CacheOriginalData, releaseData: func() { releases++ }},
		{Data: []byte("raw"), releaseData: func() { releases++ }},
	}}
	v.ReleaseReadBuffers()
	require.Equal(t, 1, releases)
	require.Nil(t, v.Entries[0].Data)
	require.Equal(t, []byte("owned"), v.Entries[0].CachedData.Bytes())
	require.NotNil(t, v.Entries[1].Data)
	v.ReleaseReadBuffers()
	v.Release()
	require.Equal(t, 2, releases)
}
