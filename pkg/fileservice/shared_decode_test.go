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
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

type decodedTestResult struct {
	data  fscache.Data
	lease *decodedReadLease
	err   error
}

func testDecode(r *decodedReadRegistry, ctx context.Context, key decodedReadKey, convert func() (fscache.Data, error)) decodedTestResult {
	d, l, e := r.decode(ctx, key, 16, convert)
	return decodedTestResult{d, l, e}
}

func releaseDecodedTestResult(result decodedTestResult) {
	if result.data != nil {
		result.data.Release()
	}
	result.lease.release()
}

func requireDecodeDrained(t *testing.T, r *decodedReadRegistry) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Empty(t, r.entries)
	require.Zero(t, r.count)
	require.Zero(t, r.bytes)
}

func TestSharedDecodeConcurrentLifetime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := newDecodedReadRegistry(128)
	key := decodedReadKey{path: "column"}
	entered, unblock := make(chan struct{}), make(chan struct{})
	unblockLeader := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(unblockLeader)
	var calls atomic.Int32
	convert := func() (fscache.Data, error) {
		calls.Add(1)
		close(entered)
		<-unblock
		return NewBytes([]byte("payload")), nil
	}
	results := make(chan decodedTestResult, 8)
	go func() { results <- testDecode(r, ctx, key, convert) }()
	<-entered
	for i := 0; i < 7; i++ {
		waitCtx := &waitStartedContext{Context: ctx, started: make(chan struct{})}
		go func() {
			results <- testDecode(r, waitCtx, key, func() (fscache.Data, error) { calls.Add(1); return NewBytes([]byte("unexpected")), nil })
		}()
		select {
		case <-waitCtx.started:
		case <-ctx.Done():
			unblockLeader()
			t.Fatal("follower did not join")
		}
	}
	unblockLeader()
	var held []decodedTestResult
	for i := 0; i < 8; i++ {
		v := <-results
		require.NoError(t, v.err)
		held = append(held, v)
	}
	require.Equal(t, int32(1), calls.Load())
	for _, v := range held {
		require.Same(t, held[0].data, v.data)
		require.Equal(t, []byte("payload"), v.data.Bytes())
	}
	// A late reader shares while a prior consumer still owns its ticket.
	late := testDecode(r, ctx, key, func() (fscache.Data, error) { t.Fatal("ready result decoded again"); return nil, nil })
	require.NoError(t, late.err)
	require.Same(t, held[0].data, late.data)
	for _, v := range held {
		releaseDecodedTestResult(v)
	}
	require.Equal(t, []byte("payload"), late.data.Bytes())
	owner := late.data.(*Bytes)
	releaseDecodedTestResult(late)
	require.Zero(t, owner.refs.Load())
	requireDecodeDrained(t, r)
}

func TestSharedDecodeCancellationAndTimeout(t *testing.T) {
	for _, kind := range []string{"follower", "leader", "timeout", "close", "error"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := newDecodedReadRegistry(128)
			key := decodedReadKey{path: "column"}
			wantError := errors.New("invalid compressed column")
			leaderCtx, cancelLeader := context.WithCancel(ctx)
			defer cancelLeader()
			entered, unblock := make(chan struct{}), make(chan struct{})
			unblockLeader := sync.OnceFunc(func() { close(unblock) })
			t.Cleanup(unblockLeader)
			leader := make(chan decodedTestResult, 1)
			require.True(t, r.beginRead())
			go func() {
				leader <- testDecode(r, leaderCtx, key, func() (fscache.Data, error) {
					close(entered)
					select {
					case <-unblock:
						if kind == "error" {
							return nil, wantError
						}
						return NewBytes([]byte("leader")), nil
					case <-leaderCtx.Done():
						return nil, leaderCtx.Err()
					}
				})
			}()
			<-entered
			followerCtx, cancelFollower := context.WithCancel(ctx)
			defer cancelFollower()
			waiting := &waitStartedContext{Context: followerCtx, started: make(chan struct{})}
			follower := make(chan decodedTestResult, 1)
			go func() {
				follower <- testDecode(r, waiting, key, func() (fscache.Data, error) { return NewBytes([]byte("fallback")), nil })
			}()
			<-waiting.started
			var retired atomic.Int32
			switch kind {
			case "follower":
				cancelFollower()
			case "leader":
				cancelLeader()
			case "close":
				r.close(func() { retired.Add(1) })
			case "error":
				unblockLeader()
			}
			var result decodedTestResult
			select {
			case result = <-follower:
			case <-ctx.Done():
				unblockLeader()
				t.Fatal("follower depended on blocked leader")
			}
			if kind == "follower" {
				require.ErrorIs(t, result.err, context.Canceled)
			} else if kind == "error" {
				require.ErrorIs(t, result.err, wantError)
			} else if kind == "close" {
				require.ErrorContains(t, result.err, "closed")
				require.Zero(t, retired.Load())
			} else {
				require.NoError(t, result.err)
				require.Nil(t, result.lease)
				require.Equal(t, []byte("fallback"), result.data.Bytes())
			}
			releaseDecodedTestResult(result)
			unblockLeader()
			result = <-leader
			if kind == "leader" {
				require.ErrorIs(t, result.err, context.Canceled)
			} else if kind == "error" {
				require.ErrorIs(t, result.err, wantError)
			} else if kind == "close" {
				require.ErrorContains(t, result.err, "closed")
			} else {
				require.NoError(t, result.err)
			}
			releaseDecodedTestResult(result)
			r.endRead()
			if kind == "close" {
				require.Equal(t, int32(1), retired.Load())
				r.close(func() { t.Fatal("retired twice") })
			}
			requireDecodeDrained(t, r)
		})
	}
}

func TestSharedDecodeAdmissionFailureAndGenerations(t *testing.T) {
	ctx := context.Background()
	key := decodedReadKey{path: "column"}
	convert := func() (fscache.Data, error) { return NewBytes([]byte("payload")), nil }
	t.Run("bytes and oversize", func(t *testing.T) {
		r := newDecodedReadRegistry(8)
		v := testDecode(r, ctx, key, convert)
		require.NoError(t, v.err)
		require.Nil(t, v.lease)
		releaseDecodedTestResult(v)
		requireDecodeDrained(t, r)
		r = newDecodedReadRegistry(128)
		v = testDecode(r, ctx, key, func() (fscache.Data, error) { return NewBytes(make([]byte, 17)), nil })
		require.NoError(t, v.err)
		require.Nil(t, v.lease)
		releaseDecodedTestResult(v)
		requireDecodeDrained(t, r)
	})
	t.Run("participants and entries", func(t *testing.T) {
		r := newDecodedReadRegistry(4096)
		var held []decodedTestResult
		for i := 0; i < sharedDecodeMaxParticipants; i++ {
			v := testDecode(r, ctx, key, convert)
			require.NotNil(t, v.lease)
			held = append(held, v)
		}
		v := testDecode(r, ctx, key, convert)
		require.Nil(t, v.lease)
		releaseDecodedTestResult(v)
		for _, v := range held {
			releaseDecodedTestResult(v)
		}
		requireDecodeDrained(t, r)
		held = nil
		for i := 0; i < sharedDecodeMaxEntries; i++ {
			k := key
			k.offset = int64(i)
			v := testDecode(r, ctx, k, convert)
			require.NotNil(t, v.lease)
			held = append(held, v)
		}
		k := key
		k.offset = 1000
		v = testDecode(r, ctx, k, convert)
		require.Nil(t, v.lease)
		releaseDecodedTestResult(v)
		for _, v := range held {
			releaseDecodedTestResult(v)
		}
		requireDecodeDrained(t, r)
	})
	t.Run("error and abandoned", func(t *testing.T) {
		r := newDecodedReadRegistry(128)
		want := errors.New("invalid compressed bytes")
		v := testDecode(r, ctx, key, func() (fscache.Data, error) { return nil, want })
		require.ErrorIs(t, v.err, want)
		require.Nil(t, v.lease)
		requireDecodeDrained(t, r)
		partial := NewBytes([]byte("partial"))
		v = testDecode(r, ctx, key, func() (fscache.Data, error) { return partial, want })
		require.ErrorIs(t, v.err, want)
		require.Nil(t, v.data)
		require.Zero(t, partial.refs.Load())
		requireDecodeDrained(t, r)
		require.Panics(t, func() { testDecode(r, ctx, key, func() (fscache.Data, error) { panic("abandoned") }) })
		requireDecodeDrained(t, r)
	})
	t.Run("old release cannot remove replacement", func(t *testing.T) {
		r := newDecodedReadRegistry(128)
		old, leader, err := r.acquire(key, 16)
		require.NoError(t, err)
		require.True(t, leader)
		r.finish(old.entry, nil, errors.New("old failure"))
		v := testDecode(r, ctx, key, convert)
		require.NotNil(t, v.lease)
		old.release()
		require.Same(t, v.lease.entry, r.entries[key])
		releaseDecodedTestResult(v)
		requireDecodeDrained(t, r)
	})
	t.Run("ready survives close and error cleanup", func(t *testing.T) {
		r := newDecodedReadRegistry(128)
		v := testDecode(r, ctx, key, convert)
		var retired bool
		r.close(func() { retired = true })
		require.True(t, retired)
		require.Equal(t, []byte("payload"), v.data.Bytes())
		vec := IOVector{Entries: []IOEntry{{CachedData: v.data, decodeLease: v.lease}}}
		vec.ReleaseReadResultOnError()
		vec.Release()
		requireDecodeDrained(t, r)
		_, _, err := r.acquire(key, 16)
		require.ErrorContains(t, err, "closed")
	})
}

func TestSharedDecodeKeyIsolation(t *testing.T) {
	r := newDecodedReadRegistry(1024)
	base := decodedReadKey{path: "a", offset: 1, size: 2, decoded: 16, codec: DecodeSharing{Codec: "x"}}
	keys := []decodedReadKey{base}
	for _, change := range []func(*decodedReadKey){func(k *decodedReadKey) { k.path = "b" }, func(k *decodedReadKey) { k.offset++ }, func(k *decodedReadKey) { k.size++ }, func(k *decodedReadKey) { k.decoded++ }, func(k *decodedReadKey) { k.policy = SkipFullFilePreloads }, func(k *decodedReadKey) { k.codec.Codec = "y" }, func(k *decodedReadKey) { k.codec.Parameters[0]++ }} {
		k := base
		change(&k)
		keys = append(keys, k)
	}
	held := make([]decodedTestResult, 0, len(keys))
	calls := 0
	for _, key := range keys {
		v := testDecode(r, context.Background(), key, func() (fscache.Data, error) { calls++; return NewBytes([]byte("data")), nil })
		require.NoError(t, v.err)
		held = append(held, v)
	}
	require.Equal(t, len(keys), calls)
	for _, v := range held {
		releaseDecodedTestResult(v)
	}
	requireDecodeDrained(t, r)
}

func TestS3FSSharedDecodeEligibility(t *testing.T) {
	r := newDecodedReadRegistry(1024)
	fs := &S3FS{name: "sharing", decodedReads: r}
	for _, tc := range []struct {
		name   string
		change func(*IOVector)
	}{
		{"no descriptor", func(v *IOVector) { v.Entries[0].DecodeSharing = DecodeSharing{} }},
		{"parameters without codec", func(v *IOVector) {
			v.Entries[0].DecodeSharing = DecodeSharing{Parameters: [2]uint64{1, 2}}
		}},
		{"large codec", func(v *IOVector) { v.Entries[0].DecodeSharing.Codec = string(make([]byte, 65)) }},
		{"unknown size", func(v *IOVector) { v.Entries[0].Size = -1 }},
		{"unknown decoded size", func(v *IOVector) { v.Entries[0].CachedDataSize = 0 }},
		{"negative offset", func(v *IOVector) { v.Entries[0].Offset = -1 }},
		{"caller data", func(v *IOVector) { v.Entries[0].Data = []byte{1} }},
		{"stream", func(v *IOVector) { var closer io.ReadCloser; v.Entries[0].ReadCloserForRead = &closer }},
		{"writer", func(v *IOVector) { v.Entries[0].WriterForRead = io.Discard }},
		{"no converter", func(v *IOVector) { v.Entries[0].ToCacheData = nil }},
		{"multiple marked entries", func(v *IOVector) { v.Entries = append(v.Entries, v.Entries[0]) }},
		{"already done", func(v *IOVector) { v.Entries[0].done = true }},
		{"custom cache", func(v *IOVector) { v.Caches = []IOVectorCache{nil} }},
		{"skip memory writes", func(v *IOVector) { v.Policy = SkipMemoryCacheWrites }},
		{"skip memory reads", func(v *IOVector) { v.Policy = SkipMemoryCacheReads }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := IOVector{FilePath: "column", Entries: []IOEntry{{Size: 3, CachedDataSize: 3,
				DecodeSharing: DecodeSharing{Codec: "test"}, ToCacheData: CacheOriginalData}}}
			tc.change(&v)
			finish, err := fs.prepareSharedDecode(&v)
			require.NoError(t, err)
			require.Nil(t, finish)
			require.Zero(t, r.reads)
			requireDecodeDrained(t, r)
		})
	}
}

// The same three-column request exercises each helper that copies IOEntry
// values. Hold the first result to make overlap deterministic without sleeps.
func TestS3FSSelectedDecodeReadPaths(t *testing.T) {
	for _, path := range []string{"range", "stream", "individual"} {
		t.Run(path, func(t *testing.T) {
			ctx := t.Context()
			fs := newSelectedDecodeFS(t, path == "stream")
			pinned := fs.AllocateCacheData(ctx, 128<<10)
			defer pinned.Release()
			for selected := range 3 {
				t.Run(string(rune('0'+selected)), func(t *testing.T) {
					if path == "stream" {
						require.NoError(t, fs.diskCache.DeletePaths(ctx, []string{"columns"}))
					}
					var calls [3]int
					makeVector := func() IOVector {
						v := selectedDecodeVector(selected)
						if path == "stream" {
							v.Policy = 0
						}
						for i := range v.Entries {
							v.Entries[i].ToCacheData = func(ctx context.Context, _ io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
								calls[i]++
								return a.CopyToCacheData(ctx, data), nil
							}
						}
						return v
					}
					read := func(v *IOVector) {
						original := reflect.ValueOf(v.Entries[selected].ToCacheData).Pointer()
						if path == "individual" {
							finish, err := fs.prepareSharedDecode(v)
							require.NoError(t, err)
							require.NotNil(t, finish)
							err = fs.readEntriesIndividually(ctx, v)
							finish()
							require.NoError(t, err)
						} else {
							if path == "stream" {
								require.True(t, fs.shouldStreamFullObjectToDiskCache(v))
							}
							require.NoError(t, fs.Read(ctx, v))
						}
						require.Equal(t, original, reflect.ValueOf(v.Entries[selected].ToCacheData).Pointer())
					}
					first, second := makeVector(), makeVector()
					defer first.ReleaseReadResultOnError()
					defer second.ReleaseReadResultOnError()
					read(&first)
					read(&second)
					owner := first.Entries[selected].CachedData
					for i := range first.Entries {
						require.Equal(t, []byte("abcdefghi")[3*i:3*i+3], second.Entries[i].CachedData.Bytes())
						if i == selected {
							require.Equal(t, 1, calls[i])
							require.Same(t, owner, second.Entries[i].CachedData)
							require.NotNil(t, second.Entries[i].decodeLease)
						} else {
							require.Equal(t, 2, calls[i])
							require.NotSame(t, first.Entries[i].CachedData, second.Entries[i].CachedData)
							require.Nil(t, second.Entries[i].decodeLease)
						}
					}
					first.ReleaseReadResultOnError()
					require.Equal(t, []byte("abcdefghi")[3*selected:3*selected+3], owner.Bytes())
					second.ReleaseReadResultOnError()
					require.Zero(t, owner.(*Bytes).refs.Load())
					requireDecodeDrained(t, fs.decodedReads)
				})
			}
		})
	}
}

func newSelectedDecodeFS(t *testing.T, disk bool) *S3FS {
	t.Helper()
	config := CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](128 << 10)}
	if disk {
		config.DiskPath = ptrTo(t.TempDir())
		config.DiskCapacity = ptrTo[toml.ByteSize](1 << 20)
	}
	fs, err := NewS3FS(t.Context(), ObjectStorageArguments{Name: "selected-decode", Endpoint: "disk", Bucket: t.TempDir()}, config, nil, false, false)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	t.Cleanup(func() { fs.Close(context.Background()) })
	require.NoError(t, fs.Write(t.Context(), IOVector{FilePath: "columns", Policy: SkipAllCache,
		Entries: []IOEntry{{Size: 9, Data: []byte("abcdefghi")}}}))
	return fs
}

func selectedDecodeVector(selected int) IOVector {
	v := IOVector{FilePath: "columns", Policy: SkipFullFilePreloads, Entries: make([]IOEntry, 3)}
	for i := range v.Entries {
		v.Entries[i] = IOEntry{Offset: int64(3 * i), Size: 3, CachedDataSize: 3, ToCacheData: CacheOriginalData}
		if i == selected {
			v.Entries[i].DecodeSharing = DecodeSharing{Codec: "test-copy"}
		}
	}
	return v
}

func TestS3FSSelectedDecodeMemoryHits(t *testing.T) {
	fs := newSelectedDecodeFS(t, false)
	for _, hot := range [][]int{{0}, {1}, {0, 1, 2}} {
		fs.FlushCache(t.Context())
		prime := selectedDecodeVector(-1)
		entries := make([]IOEntry, 0, len(hot))
		for _, i := range hot {
			entries = append(entries, prime.Entries[i])
		}
		prime.Entries = entries
		t.Cleanup(prime.ReleaseReadResultOnError)
		require.NoError(t, fs.Read(t.Context(), &prime))
		v := selectedDecodeVector(1)
		t.Cleanup(v.ReleaseReadResultOnError)
		require.NoError(t, fs.Read(t.Context(), &v))
		for j, i := range hot {
			require.Same(t, prime.Entries[j].CachedData, v.Entries[i].CachedData)
			require.Nil(t, v.Entries[i].decodeLease, "memory hits never join a generation")
		}
		if len(hot) == 1 && hot[0] == 0 {
			require.NotNil(t, v.Entries[1].decodeLease, "a sibling hit must not disable the selected miss")
		}
		prime.ReleaseReadResultOnError()
		v.ReleaseReadResultOnError()
		requireDecodeDrained(t, fs.decodedReads)
	}
}

type failingDecodeCache struct {
	fscache.DataCache
	calls int
}

func (f *failingDecodeCache) Set(context.Context, fscache.CacheKey, fscache.Data) (bool, error) {
	f.calls++
	return false, errors.New("injected cache update failure")
}

func TestS3FSSelectedDecodeFailureIsolation(t *testing.T) {
	fs := newSelectedDecodeFS(t, false)
	pinned := fs.AllocateCacheData(t.Context(), 128<<10)
	defer pinned.Release()
	first := selectedDecodeVector(1)
	defer first.ReleaseReadResultOnError()
	require.NoError(t, fs.Read(t.Context(), &first))
	owner := first.Entries[1].CachedData
	for _, failed := range []int{0, 2} {
		v := selectedDecodeVector(1)
		t.Cleanup(v.ReleaseReadResultOnError)
		v.Entries[failed].ToCacheData = func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
			return nil, errors.New("injected sibling failure")
		}
		require.ErrorContains(t, fs.Read(t.Context(), &v), "sibling failure")
		if failed == 0 {
			require.Nil(t, v.Entries[1].decodeLease)
		} else {
			require.Same(t, owner, v.Entries[1].CachedData)
			require.NotNil(t, v.Entries[1].decodeLease)
		}
		v.ReleaseReadResultOnError()
		require.Zero(t, fs.decodedReads.reads)
		require.Equal(t, []byte("def"), owner.Bytes())
	}
	first.ReleaseReadResultOnError()
	requireDecodeDrained(t, fs.decodedReads)

	// Fail an admission-enabled deferred update after the target has decoded.
	// S3FS currently keeps memory-cache update errors local; they must not
	// invalidate the caller's data or detach its generation ticket.
	fs = newSelectedDecodeFS(t, false)
	failedCache := &failingDecodeCache{DataCache: fs.memCache.cache}
	fs.memCache.cache = failedCache
	v := selectedDecodeVector(1)
	defer v.ReleaseReadResultOnError()
	require.NoError(t, fs.Read(t.Context(), &v))
	require.Equal(t, 1, failedCache.calls)
	require.NotNil(t, v.Entries[1].decodeLease)
	v.ReleaseReadResultOnError()
	require.Zero(t, fs.decodedReads.reads)
	requireDecodeDrained(t, fs.decodedReads)
}

func TestS3FSSelectedDecodeCloseDuringSibling(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fs := newSelectedDecodeFS(t, false)
	entered, unblock := make(chan struct{}), make(chan struct{})
	unblockSibling := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(unblockSibling)
	v := selectedDecodeVector(1)
	v.Entries[2].ToCacheData = func(ctx context.Context, r io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
		close(entered)
		select {
		case <-unblock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return CacheOriginalData(ctx, r, data, a)
	}
	var readErr error
	done := make(chan struct{})
	go func() { readErr = fs.Read(ctx, &v); close(done) }()
	t.Cleanup(func() {
		unblockSibling()
		<-done
		v.ReleaseReadResultOnError()
	})
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("sibling conversion did not start")
	}
	// Close must neither wait for the sibling nor retire the read's allocator.
	closed := make(chan struct{})
	go func() { fs.Close(ctx); close(closed) }()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("close waited for sibling conversion")
	}
	require.False(t, fs.memCache.closed.Load())
	unblockSibling()
	<-done
	require.NoError(t, readErr)
	require.True(t, fs.memCache.closed.Load())
	require.Equal(t, []byte("def"), v.Entries[1].CachedData.Bytes())
	require.NotNil(t, v.Entries[1].decodeLease)
	v.ReleaseReadResultOnError()
	requireDecodeDrained(t, fs.decodedReads)
}

func TestS3FSSharedDecodeDiskHits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var counters perfcounter.CounterSet
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "shared-decode", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](128 << 10), DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](1 << 20)}, []*perfcounter.CounterSet{&counters}, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	raw := bytes.Repeat([]byte("abcd"), 16<<10)
	compressed := make([]byte, lz4.CompressBlockBound(len(raw)))
	n, err := lz4.CompressBlock(raw, compressed, nil)
	require.NoError(t, err)
	require.Positive(t, n)
	compressed = compressed[:n]
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "column", Entries: []IOEntry{{Size: int64(n), Data: compressed}}, Policy: SkipAllCache}))
	require.NoError(t, fs.PrefetchFile(ctx, "column"))
	fs.FlushCache(ctx)
	pinned := fs.memCache.AllocateCacheData(ctx, 128<<10)
	t.Cleanup(func() {
		if pinned != nil {
			pinned.Release()
		}
	})
	var calls atomic.Int32
	decode := func(ctx context.Context, _ io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
		calls.Add(1)
		out := allocator.AllocateCacheData(ctx, len(raw))
		_, err := lz4.UncompressBlock(data, out.Bytes())
		if err != nil {
			out.Release()
			return nil, err
		}
		return out, nil
	}
	makeVector := func() IOVector {
		return IOVector{FilePath: "column", Entries: []IOEntry{{Size: int64(n), CachedDataSize: int64(len(raw)), ToCacheData: decode, DecodeSharing: DecodeSharing{Codec: "test-lz4-v1"}}}}
	}
	before := counters.FileService.S3.Get.Load()
	vectors := make([]IOVector, 8)
	t.Cleanup(func() {
		for i := range vectors {
			vectors[i].ReleaseReadResultOnError()
		}
	})
	done := make(chan error, 8)
	for i := range vectors {
		vectors[i] = makeVector()
		go func(i int) { done <- fs.Read(ctx, &vectors[i]) }(i)
	}
	for range vectors {
		require.NoError(t, <-done)
	}
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, before, counters.FileService.S3.Get.Load())
	owner := vectors[0].Entries[0].CachedData
	for i := range vectors {
		require.Same(t, owner, vectors[i].Entries[0].CachedData)
		require.Equal(t, raw, owner.Bytes())
		vectors[i].ReleaseReadResultOnError()
	}
	require.Zero(t, owner.(*Bytes).refs.Load())
	requireDecodeDrained(t, fs.decodedReads)
	pinned.Release()
	pinned = nil
	// The cache can now admit the result. A later memory hit must not decode or
	// acquire a sharing lease, and uses the same original converter contract.
	v := makeVector()
	require.NoError(t, fs.Read(ctx, &v))
	v.Release()
	beforeCalls := calls.Load()
	v = makeVector()
	require.NoError(t, fs.Read(ctx, &v))
	require.Equal(t, beforeCalls, calls.Load())
	require.Nil(t, v.Entries[0].decodeLease)
	v.Release()
	requireDecodeDrained(t, fs.decodedReads)
}

type finalReleaseBarrier struct {
	fscache.Data
	base    *Bytes
	entered chan struct{}
	unblock chan struct{}
}

func (d *finalReleaseBarrier) Release() {
	if d.base.refs.Load() == 1 {
		close(d.entered)
		<-d.unblock
	}
	d.Data.Release()
}

func TestSharedDecodeQuotaWaitsForFinalRelease(t *testing.T) {
	r := newDecodedReadRegistry(16)
	base := NewBytes([]byte("data"))
	data := &finalReleaseBarrier{Data: base, base: base, entered: make(chan struct{}), unblock: make(chan struct{})}
	unblockRelease := sync.OnceFunc(func() { close(data.unblock) })
	t.Cleanup(unblockRelease)
	v := testDecode(r, context.Background(), decodedReadKey{path: "a"}, func() (fscache.Data, error) { return data, nil })
	require.NoError(t, v.err)
	v.data.Release()
	done := make(chan struct{})
	go func() { v.lease.release(); close(done) }()
	<-data.entered
	lease, _, err := r.acquire(decodedReadKey{path: "b"}, 16)
	require.NoError(t, err)
	require.Nil(t, lease, "quota must cover deferred physical release")
	unblockRelease()
	<-done
	requireDecodeDrained(t, r)
}

func TestS3FSSharedDecodeCloseDefersCacheRetirement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "decode-close", Endpoint: "disk", Bucket: t.TempDir()}, CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](128 << 10)}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "column", Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}, Policy: SkipAllCache}))
	entered, unblock := make(chan struct{}), make(chan struct{})
	unblockLeader := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(unblockLeader)
	var original fscache.Data
	vec := IOVector{FilePath: "column", Entries: []IOEntry{{Size: 3, CachedDataSize: 3, DecodeSharing: DecodeSharing{Codec: "test-copy"}, ToCacheData: func(ctx context.Context, _ io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
		original = a.CopyToCacheData(ctx, data)
		close(entered)
		<-unblock
		return original, nil
	}}}}
	done := make(chan error, 1)
	go func() { done <- fs.Read(ctx, &vec) }()
	<-entered
	closeDone := make(chan struct{})
	go func() { fs.Close(ctx); close(closeDone) }()
	select {
	case <-closeDone:
	case <-ctx.Done():
		t.Fatal("close waited for conversion")
	}
	require.False(t, fs.memCache.closed.Load(), "active read must keep its allocator/cache-update lifetime")
	unblockLeader()
	require.ErrorContains(t, <-done, "closed")
	vec.ReleaseReadResultOnError()
	require.True(t, fs.memCache.closed.Load())
	requireDecodeDrained(t, fs.decodedReads)
	require.Zero(t, original.(*Bytes).refs.Load())
}

func TestS3FSSharedDecodeCloseDuringCacheUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, unblock := make(chan struct{}), make(chan struct{})
	unblockUpdate := sync.OnceFunc(func() { close(unblock) })
	var once sync.Once
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "decode-update-close", Endpoint: "disk", Bucket: t.TempDir()},
		CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](128 << 10), CacheCallbacks: CacheCallbacks{
			PostSet: []CacheCallbackFunc{func(fscache.CacheKey, fscache.Data) {
				once.Do(func() { close(entered); <-unblock })
			}},
		}}, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(context.Background()) })
	t.Cleanup(unblockUpdate)
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "column", Entries: []IOEntry{{Size: 3, Data: []byte("abc")}}, Policy: SkipAllCache}))
	vec := IOVector{FilePath: "column", Entries: []IOEntry{{Size: 3, CachedDataSize: 3,
		DecodeSharing: DecodeSharing{Codec: "test-copy"}, ToCacheData: CacheOriginalData}}}
	done := make(chan error, 1)
	go func() { done <- fs.Read(ctx, &vec) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("cache update did not start")
	}
	closeDone := make(chan struct{})
	go func() { fs.Close(ctx); close(closeDone) }()
	select {
	case <-closeDone:
	case <-ctx.Done():
		t.Fatal("close waited for cache update")
	}
	require.False(t, fs.memCache.closed.Load(), "close must not wait behind or retire an active cache update")
	unblockUpdate()
	require.NoError(t, <-done)
	require.True(t, fs.memCache.closed.Load())
	require.Equal(t, []byte("abc"), vec.Entries[0].CachedData.Bytes(), "consumer owns its data after cache retirement")
	vec.Release()
	requireDecodeDrained(t, fs.decodedReads)
}
