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
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
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

func sharedTestBytes(data []byte) *Bytes {
	return DefaultCacheDataAllocator().CopyToCacheData(context.Background(), data).(*Bytes)
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
	require.Empty(t, r.fills)
	require.Zero(t, r.count)
	require.Zero(t, r.bytes)
	require.Zero(t, r.fillCount)
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
		return sharedTestBytes([]byte("payload")), nil
	}
	results := make(chan decodedTestResult, 8)
	go func() { results <- testDecode(r, ctx, key, convert) }()
	<-entered
	for i := 0; i < 7; i++ {
		waitCtx := &waitStartedContext{Context: ctx, started: make(chan struct{})}
		go func() {
			results <- testDecode(r, waitCtx, key, func() (fscache.Data, error) { calls.Add(1); return sharedTestBytes([]byte("unexpected")), nil })
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
	for _, kind := range []string{"follower", "leader", "timeout", "close", "error", "independent error"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := newDecodedReadRegistry(128)
			key := decodedReadKey{path: "column"}
			wantError := errors.New("invalid compressed column")
			followerError := errors.New("follower conversion failed")
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
						if kind == "error" || kind == "independent error" {
							return nil, wantError
						}
						return sharedTestBytes([]byte("leader")), nil
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
				follower <- testDecode(r, waiting, key, func() (fscache.Data, error) {
					if kind == "independent error" {
						return nil, followerError
					}
					return sharedTestBytes([]byte("fallback")), nil
				})
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
			case "error", "independent error":
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
			} else if kind == "independent error" {
				require.ErrorIs(t, result.err, followerError)
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
			} else if kind == "error" || kind == "independent error" {
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
	convert := func() (fscache.Data, error) { return sharedTestBytes([]byte("payload")), nil }
	t.Run("bytes and oversize", func(t *testing.T) {
		r := newDecodedReadRegistry(8)
		v := testDecode(r, ctx, key, convert)
		require.NoError(t, v.err)
		require.Nil(t, v.lease)
		releaseDecodedTestResult(v)
		requireDecodeDrained(t, r)
		r = newDecodedReadRegistry(128)
		v = testDecode(r, ctx, key, func() (fscache.Data, error) { return sharedTestBytes(make([]byte, 17)), nil })
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
		r.finish(old.entry, nil)
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

type measuredSharedData struct {
	fscache.Data
	logical, backing int64
	bytesCalls       int
}

func (d *measuredSharedData) Bytes() []byte {
	d.bytesCalls++
	return d.Data.Bytes()
}

func (d *measuredSharedData) Size() int64        { return d.logical }
func (d *measuredSharedData) BackingSize() int64 { return d.backing }

type unknownSharedData struct{ fscache.Data }

func TestSharedDecodeBackingAccounting(t *testing.T) {
	for _, tc := range []struct {
		name             string
		logical, backing int64
		unknown, unsized bool
		shared           bool
	}{
		{name: "bounded backing", logical: 9, backing: 16, shared: true},
		{name: "oversized backing", logical: 9, backing: 32},
		{name: "oversized result", logical: 17, backing: 32},
		{name: "unknown backing", unknown: true},
		{name: "unsized data", unsized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newDecodedReadRegistry(32)
			length, capacity := 9, 16
			if tc.logical > 0 {
				length, capacity = int(tc.logical), int(tc.backing)
			}
			base := NewBytes(make([]byte, length, capacity))
			measured := &measuredSharedData{Data: base, logical: tc.logical, backing: tc.backing}
			var value fscache.Data = measured
			if tc.unknown {
				value = base
			} else if tc.unsized {
				value = &unknownSharedData{Data: base}
			}
			data, lease, err := r.decode(t.Context(), decodedReadKey{path: "column"}, 9,
				func() (fscache.Data, error) { return value, nil })
			require.NoError(t, err)
			require.Same(t, value, data)
			require.Equal(t, tc.shared, lease != nil)
			require.Zero(t, measured.bytesCalls, "size checks must not read or clone the payload")
			if tc.shared {
				require.Equal(t, int64(16), r.bytes, "charge the allocator size class")
			}
			releaseDecodedTestResult(decodedTestResult{data: data, lease: lease})
			requireDecodeDrained(t, r)
		})
	}
}

func TestSharedDecodeKeyIsolation(t *testing.T) {
	r := newDecodedReadRegistry(1024)
	base := decodedReadKey{path: "a", offset: 1, size: 2, decoded: 16, codec: DecodeSharing{Codec: "x"}}
	keys := make([]decodedReadKey, 1, 8)
	keys[0] = base
	for _, change := range []func(*decodedReadKey){func(k *decodedReadKey) { k.path = "b" }, func(k *decodedReadKey) { k.offset++ }, func(k *decodedReadKey) { k.size++ }, func(k *decodedReadKey) { k.decoded++ }, func(k *decodedReadKey) { k.policy = SkipFullFilePreloads }, func(k *decodedReadKey) { k.codec.Codec = "y" }, func(k *decodedReadKey) { k.codec.Parameters[0]++ }} {
		k := base
		change(&k)
		keys = append(keys, k)
	}
	held := make([]decodedTestResult, 0, len(keys))
	calls := 0
	for _, key := range keys {
		v := testDecode(r, context.Background(), key, func() (fscache.Data, error) { calls++; return sharedTestBytes([]byte("data")), nil })
		require.NoError(t, v.err)
		held = append(held, v)
	}
	require.Equal(t, len(keys), calls)
	for _, v := range held {
		releaseDecodedTestResult(v)
	}
	requireDecodeDrained(t, r)
}

func TestSharedDecodeFillAdmissionAndKeyIsolation(t *testing.T) {
	base := decodedReadKey{path: "a", offset: 1, size: 2, decoded: 16, policy: SkipFullFilePreloads,
		codec: DecodeSharing{Codec: "x", Parameters: [2]uint64{1, 2}}}

	t.Run("start hint preserves existing joins", func(t *testing.T) {
		r := newDecodedReadRegistry(1024)
		leader, err := r.acquireFill(base, true)
		require.NoError(t, err)
		t.Cleanup(leader.finish)
		follower, err := r.acquireFill(base, false)
		require.NoError(t, err)
		t.Cleanup(follower.finish)
		require.NotNil(t, follower)
		require.False(t, follower.leader)
		require.Same(t, leader.generation, follower.generation)
		other := base
		other.offset++
		rejected, err := r.acquireFill(other, false)
		require.NoError(t, err)
		t.Cleanup(rejected.finish)
		require.Nil(t, rejected)
		leader.finish()
		follower.finish()
		requireDecodeDrained(t, r)
	})

	t.Run("participants", func(t *testing.T) {
		r := newDecodedReadRegistry(1024)
		leader, err := r.acquireFill(base, true)
		require.NoError(t, err)
		require.True(t, leader.leader)
		followers := make([]*decodedFillTicket, 0, sharedDecodeFillMaxParticipants-1)
		for i := 1; i < sharedDecodeFillMaxParticipants; i++ {
			follower, err := r.acquireFill(base, true)
			require.NoError(t, err)
			require.NotNil(t, follower)
			require.False(t, follower.leader)
			followers = append(followers, follower)
		}
		extra, err := r.acquireFill(base, true)
		require.NoError(t, err)
		require.Nil(t, extra)
		leader.finish()
		for _, follower := range followers {
			follower.finish()
		}
		requireDecodeDrained(t, r)
	})

	t.Run("generations", func(t *testing.T) {
		r := newDecodedReadRegistry(1024)
		leaders := make([]*decodedFillTicket, 0, sharedDecodeFillMaxGenerations)
		followers := make([]*decodedFillTicket, 0, sharedDecodeFillMaxGenerations)
		for i := 0; i < sharedDecodeFillMaxGenerations; i++ {
			key := base
			key.offset = int64(i)
			leader, err := r.acquireFill(key, true)
			require.NoError(t, err)
			require.NotNil(t, leader)
			require.True(t, leader.leader)
			leaders = append(leaders, leader)
			follower, err := r.acquireFill(key, true)
			require.NoError(t, err)
			require.NotNil(t, follower)
			require.False(t, follower.leader)
			followers = append(followers, follower)
		}
		for _, leader := range leaders {
			leader.finish()
		}
		r.mu.Lock()
		fillCount, fillMapLen, bytes := r.fillCount, len(r.fills), r.bytes
		r.mu.Unlock()
		require.Zero(t, fillMapLen, "completed generations must detach from lookup")
		require.Equal(t, sharedDecodeFillMaxGenerations, fillCount)
		require.Zero(t, bytes)
		extra, err := r.acquireFill(decodedReadKey{path: "full"}, true)
		require.NoError(t, err)
		require.Nil(t, extra)
		followers[0].finish()
		fresh, err := r.acquireFill(decodedReadKey{path: "fresh"}, true)
		require.NoError(t, err)
		require.NotNil(t, fresh)
		require.True(t, fresh.leader)
		fresh.finish()
		for _, follower := range followers[1:] {
			follower.finish()
		}
		requireDecodeDrained(t, r)
	})

	t.Run("key and properties", func(t *testing.T) {
		r := newDecodedReadRegistry(1024)
		keys := make([]decodedReadKey, 1, 8)
		keys[0] = base
		for _, change := range []func(*decodedReadKey){
			func(k *decodedReadKey) { k.path = "b" },
			func(k *decodedReadKey) { k.offset++ },
			func(k *decodedReadKey) { k.size++ },
			func(k *decodedReadKey) { k.decoded++ },
			func(k *decodedReadKey) { k.policy = SkipMemoryCacheReads },
			func(k *decodedReadKey) { k.codec.Codec = "y" },
			func(k *decodedReadKey) { k.codec.Parameters[0]++ },
		} {
			key := base
			change(&key)
			keys = append(keys, key)
		}
		leaders := make([]*decodedFillTicket, 0, len(keys))
		for _, key := range keys {
			leader, err := r.acquireFill(key, true)
			require.NoError(t, err)
			require.NotNil(t, leader)
			require.True(t, leader.leader)
			leaders = append(leaders, leader)
		}
		require.Len(t, leaders, len(keys))
		for _, leader := range leaders {
			leader.finish()
		}
		requireDecodeDrained(t, r)
	})
}

func TestSharedDecodeFillWaitCancellationAndClose(t *testing.T) {
	t.Run("timeout and follower cancellation keep leader", func(t *testing.T) {
		r := newDecodedReadRegistry(128)
		key := decodedReadKey{path: "column"}
		leader, err := r.acquireFill(key, true)
		require.NoError(t, err)
		follower, err := r.acquireFill(key, true)
		require.NoError(t, err)
		require.NotNil(t, follower)
		require.NoError(t, follower.wait(context.Background(), time.Millisecond))
		r.mu.Lock()
		finished, current := leader.generation.finished, r.fills[key]
		r.mu.Unlock()
		require.False(t, finished)
		require.Same(t, leader.generation, current)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, follower.wait(ctx, time.Second), context.Canceled)
		follower.finish()
		leader.finish()
		requireDecodeDrained(t, r)
	})

	t.Run("leader outcome is notification only", func(t *testing.T) {
		r := newDecodedReadRegistry(128)
		key := decodedReadKey{path: "column"}
		leader, err := r.acquireFill(key, true)
		require.NoError(t, err)
		follower, err := r.acquireFill(key, true)
		require.NoError(t, err)
		leader.finish() // The caller's conversion may have errored or canceled.
		require.NoError(t, follower.wait(context.Background(), time.Second))
		follower.finish()
		requireDecodeDrained(t, r)
	})

	t.Run("close wakes waiters and defers retirement", func(t *testing.T) {
		r := newDecodedReadRegistry(128)
		require.True(t, r.beginRead())
		key := decodedReadKey{path: "column"}
		leader, err := r.acquireFill(key, true)
		require.NoError(t, err)
		follower, err := r.acquireFill(key, true)
		require.NoError(t, err)
		var retired atomic.Int32
		r.close(func() { retired.Add(1) })
		require.ErrorContains(t, follower.wait(context.Background(), time.Second), "closed")
		require.ErrorContains(t, follower.waitResult(context.Background()), "closed")
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, follower.waitResult(canceled), context.Canceled)
		_, err = r.acquireFill(key, true)
		require.ErrorContains(t, err, "closed")
		require.Zero(t, retired.Load())
		follower.finish()
		leader.finish()
		r.endRead()
		require.Equal(t, int32(1), retired.Load())
		r.close(func() { t.Fatal("retired twice") })
		requireDecodeDrained(t, r)
	})

	t.Run("completed generation detaches without stale deletion", func(t *testing.T) {
		r := newDecodedReadRegistry(128)
		key := decodedReadKey{path: "column"}
		oldLeader, err := r.acquireFill(key, true)
		require.NoError(t, err)
		oldFollower, err := r.acquireFill(key, true)
		require.NoError(t, err)
		oldLeader.finish()
		newLeader, err := r.acquireFill(key, true)
		require.NoError(t, err)
		require.NotNil(t, newLeader)
		require.True(t, newLeader.leader)
		require.NotSame(t, oldLeader.generation, newLeader.generation)
		oldFollower.finish()
		r.mu.Lock()
		current := r.fills[key]
		r.mu.Unlock()
		require.Same(t, newLeader.generation, current)
		newLeader.finish()
		requireDecodeDrained(t, r)
	})
}

func waitForSharedDecodeFillParticipants(t *testing.T, r *decodedReadRegistry, want int) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		for _, generation := range r.fills {
			if generation.participants >= want {
				r.mu.Unlock()
				return
			}
		}
		r.mu.Unlock()
		select {
		case <-deadline.C:
			t.Fatal("fill follower did not join")
		default:
			runtime.Gosched()
		}
	}
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
		{"multiple entries", func(v *IOVector) { v.Entries = append(v.Entries, v.Entries[0]) }},
		{"custom cache", func(v *IOVector) { v.Caches = []IOVectorCache{nil} }},
		{"skip memory writes", func(v *IOVector) { v.Policy = SkipMemoryCacheWrites }},
		{"skip memory reads", func(v *IOVector) { v.Policy = SkipMemoryCacheReads }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := IOVector{FilePath: "column", Entries: []IOEntry{{Size: 3, CachedDataSize: 3,
				DecodeSharing: DecodeSharing{Codec: "test"}, ToCacheData: CacheOriginalData}}}
			tc.change(&v)
			scope, ticket, attempted, err := fs.prepareSharedDecodeForRead(&v)
			require.NoError(t, err)
			require.Nil(t, scope)
			require.Nil(t, ticket)
			require.False(t, attempted)
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
			admission := rejectSharedDecodeMemoryAdmission(t, fs)
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
							scope, ticket, attempted, err := fs.prepareSharedDecodeForRead(v)
							require.NoError(t, err)
							require.NotNil(t, scope)
							require.Nil(t, ticket)
							require.False(t, attempted)
							err = fs.readEntriesIndividually(ctx, v)
							scope.finish()
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
					beforeRejections := admission.rejected.Load()
					read(&first)
					read(&second)
					if path != "individual" {
						require.Greater(t, admission.rejected.Load(), beforeRejections)
					}
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

type observedAdmissionCache struct {
	fscache.DataCache
	rejected atomic.Int32
	afterGet func(bool)
}

func (c *observedAdmissionCache) Get(ctx context.Context, key fscache.CacheKey) (fscache.Data, bool) {
	data, ok := c.DataCache.Get(ctx, key)
	if c.afterGet != nil {
		c.afterGet(ok)
	}
	return data, ok
}

func (c *observedAdmissionCache) Set(ctx context.Context, key fscache.CacheKey, data fscache.Data) error {
	err := c.DataCache.Set(ctx, key, data)
	if errors.Is(err, fscache.ErrCacheAdmissionRejected) {
		c.rejected.Add(1)
	}
	return err
}

// Reject publication through the real FIFO pressure admission path. A unique
// owner and cleanup keep this process-wide target scoped to the test.
func rejectSharedDecodeMemoryAdmission(t *testing.T, fs *S3FS) *observedAdmissionCache {
	t.Helper()
	cache := &observedAdmissionCache{DataCache: fs.memCache.cache}
	fs.memCache.cache = cache
	owner := t.Name()
	SetMemoryCachePressureTargetPercentByOwner(owner, 1, time.Now().Add(10*time.Minute))
	t.Cleanup(func() { ClearMemoryCachePressureTargetByOwner(owner) })
	return cache
}

func newSelectedDecodeFS(t *testing.T, disk bool) *S3FS {
	t.Helper()
	config := CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](128)}
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

func newSharedDecodeDiskFillFS(t *testing.T, callbacks CacheCallbacks, prefetch bool) (*S3FS, *perfcounter.CounterSet) {
	t.Helper()
	counters := new(perfcounter.CounterSet)
	config := CacheConfig{
		MemoryCapacity: ptrTo[toml.ByteSize](128 << 10),
		DiskPath:       ptrTo(t.TempDir()),
		DiskCapacity:   ptrTo[toml.ByteSize](1 << 20),
		CacheCallbacks: callbacks,
	}
	fs, err := NewS3FS(t.Context(), ObjectStorageArguments{Name: "shared-decode-fill", Endpoint: "disk", Bucket: t.TempDir()}, config, []*perfcounter.CounterSet{counters}, false, false)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	t.Cleanup(func() { fs.Close(context.Background()) })
	require.NoError(t, fs.Write(t.Context(), IOVector{FilePath: "columns", Policy: SkipAllCache,
		Entries: []IOEntry{{Size: 9, Data: []byte("abcdefghi")}}}))
	if prefetch {
		require.NoError(t, fs.PrefetchFile(t.Context(), "columns"))
		fs.FlushCache(t.Context())
	}
	return fs, counters
}

func holdSharedDecodeQuota(t *testing.T, fs *S3FS) []decodedTestResult {
	t.Helper()
	held := make([]decodedTestResult, 0, sharedDecodeMaxEntries)
	for i := 0; i < sharedDecodeMaxEntries; i++ {
		key := decodedReadKey{path: "quota", offset: int64(i)}
		result := testDecode(fs.decodedReads, context.Background(), key, func() (fscache.Data, error) {
			return sharedTestBytes([]byte("held")), nil
		})
		require.NoError(t, result.err)
		require.NotNil(t, result.lease)
		held = append(held, result)
	}
	return held
}

func TestS3FSSharedDecodeDiskFillEligibility(t *testing.T) {
	fs, _ := newSharedDecodeDiskFillFS(t, CacheCallbacks{}, false)
	multi := selectedDecodeVector(1)
	entry := multi.Entries[1]
	require.False(t, sharedDecodeDiskFillAvailable(fs.diskCache, "columns", &multi))
	inserted, rejected := fs.diskCache.cache.Set(t.Context(), fs.diskCache.pathForIOEntry("columns", entry), struct{}{}, entry.Size)
	require.True(t, inserted)
	require.False(t, rejected)
	single := IOVector{FilePath: "columns", Entries: []IOEntry{entry}}
	require.True(t, sharedDecodeDiskFillAvailable(fs.diskCache, "columns", &single))
	require.False(t, sharedDecodeDiskFillAvailable(fs.diskCache, "columns", &multi))
	inserted, rejected = fs.diskCache.cache.Set(t.Context(), fs.diskCache.pathForFile("columns"), struct{}{}, 9)
	require.True(t, inserted)
	require.False(t, rejected)
	require.True(t, sharedDecodeDiskFillAvailable(fs.diskCache, "columns", &multi))
}

func TestS3FSSharedDecodeDiskFillMemoryEligibility(t *testing.T) {
	for _, tc := range []struct {
		name     string
		size     int64
		used     bool
		pressure bool
		want     bool
	}{
		{name: "available", size: 3, want: true},
		{name: "evictable cache", size: 3, used: true, want: true},
		{name: "oversized", size: 128<<10 + 1},
		{name: "pressure", size: 3, pressure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _ := newSharedDecodeDiskFillFS(t, CacheCallbacks{}, true)
			t.Cleanup(func() { requireDecodeDrained(t, fs.decodedReads) })
			m := fs.memCache
			const capacity = 128 << 10
			if tc.used {
				v := IOVector{FilePath: "occupied", Entries: []IOEntry{{Size: capacity,
					CachedData: DefaultCacheDataAllocator().AllocateCacheData(t.Context(), capacity)}}}
				t.Cleanup(v.ReleaseReadResultOnError)
				require.NoError(t, m.Update(t.Context(), &v, false))
				v.ReleaseReadResultOnError()
				require.Equal(t, int64(capacity), m.cache.Used())
			}
			if tc.pressure {
				owner := t.Name()
				t.Cleanup(func() { ClearMemoryCachePressureTargetByOwner(owner) })
				SetMemoryCachePressureTargetPercentByOwner(owner, 50, time.Now().Add(time.Minute))
			}
			used := m.cache.Used()
			require.Equal(t, tc.want, sharedDecodeFillMayStart(m, tc.size))
			v := selectedDecodeVector(1)
			v.Entries[1].CachedDataSize = tc.size
			scope, ticket, attempted, err := fs.prepareSharedDecodeForRead(&v)
			if scope != nil {
				t.Cleanup(scope.finish)
			}
			t.Cleanup(ticket.finish)
			require.NoError(t, err)
			require.Equal(t, tc.want, attempted)
			if tc.want {
				require.NotNil(t, ticket)
				require.True(t, ticket.leader)
			} else {
				require.Nil(t, ticket)
			}
			require.Equal(t, used, m.cache.Used(), "the hint must not evict data")
		})
	}
}

func TestS3FSCacheAllocationEvictsBeforeAllocating(t *testing.T) {
	for _, withHint := range []bool{false, true} {
		t.Run(fmt.Sprintf("hint=%t", withHint), func(t *testing.T) {
			ctx := t.Context()
			m := NewMemCache(fscache.ConstCapacity(8), nil, nil, t.Name())
			t.Cleanup(func() { m.Close(ctx) })
			v := IOVector{FilePath: "column", Entries: []IOEntry{{Size: 8, CachedData: NewBytes([]byte("12345678"))}}}
			t.Cleanup(v.ReleaseReadResultOnError)
			require.NoError(t, m.Update(ctx, &v, false))
			v.ReleaseReadResultOnError()
			require.Equal(t, int64(8), m.cache.Used())
			fs := &S3FS{memCache: m}
			var allocated fscache.Data
			if withHint {
				allocated = fs.AllocateCacheDataWithHint(ctx, 8, malloc.NoClear)
			} else {
				allocated = fs.AllocateCacheData(ctx, 8)
			}
			t.Cleanup(allocated.Release)
			require.Zero(t, m.cache.Used())
		})
	}
}

func TestS3FSSharedDecodeDiskFill(t *testing.T) {
	for _, tc := range []struct {
		name     string
		saturate bool
	}{
		{name: "admitted", saturate: false},
		{name: "decode quota saturated", saturate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decodeEntered, releaseDecode := make(chan struct{}), make(chan struct{})
			publishEntered, releasePublish := make(chan struct{}), make(chan struct{})
			var decodeStarted, publishStarted sync.Once
			callbacks := CacheCallbacks{PostSet: []CacheCallbackFunc{func(fscache.CacheKey, fscache.Data) {
				publishStarted.Do(func() { close(publishEntered) })
				<-releasePublish
			}}}
			fs, counters := newSharedDecodeDiskFillFS(t, callbacks, true)
			var releaseDecodeOnce, releasePublishOnce sync.Once
			var held []decodedTestResult
			var first, second IOVector
			var workers sync.WaitGroup
			vectorsReleased := false
			releaseVectors := func() {
				if vectorsReleased {
					return
				}
				vectorsReleased = true
				first.ReleaseReadResultOnError()
				second.ReleaseReadResultOnError()
			}
			heldReleased := false
			releaseHeld := func() {
				if heldReleased {
					return
				}
				heldReleased = true
				for _, result := range held {
					releaseDecodedTestResult(result)
				}
				held = nil
			}
			t.Cleanup(func() {
				releaseDecodeOnce.Do(func() { close(releaseDecode) })
				releasePublishOnce.Do(func() { close(releasePublish) })
				workers.Wait()
				releaseVectors()
				releaseHeld()
			})
			if tc.saturate {
				held = holdSharedDecodeQuota(t, fs)
			}
			var calls atomic.Int32
			decode := func(ctx context.Context, _ io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
				calls.Add(1)
				decodeStarted.Do(func() { close(decodeEntered) })
				<-releaseDecode
				return allocator.CopyToCacheData(ctx, data), nil
			}
			first, second = selectedDecodeVector(1), selectedDecodeVector(1)
			first.Entries[1].ToCacheData = decode
			second.Entries[1].ToCacheData = decode
			beforeS3 := counters.FileService.S3.Get.Load()
			beforeDisk := counters.FileService.Cache.Disk.Read.Load()
			firstDone := make(chan error, 1)
			secondDone := make(chan error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				firstDone <- fs.Read(t.Context(), &first)
			}()
			select {
			case <-decodeEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("disk-fill leader did not reach conversion")
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				secondDone <- fs.Read(t.Context(), &second)
			}()
			waitForSharedDecodeFillParticipants(t, fs.decodedReads, 2)
			releaseDecodeOnce.Do(func() { close(releaseDecode) })
			select {
			case <-publishEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("disk-fill leader did not reach memory publication")
			}
			diskAtPublish := counters.FileService.Cache.Disk.Read.Load()
			require.Greater(t, diskAtPublish, beforeDisk)
			select {
			case err := <-secondDone:
				t.Fatalf("follower completed before memory publication: %v", err)
			default:
			}
			require.Equal(t, int32(1), calls.Load())
			releasePublishOnce.Do(func() { close(releasePublish) })
			require.NoError(t, <-firstDone)
			require.NoError(t, <-secondDone)
			workers.Wait()
			require.Equal(t, diskAtPublish, counters.FileService.Cache.Disk.Read.Load(), "follower must not reread disk")
			require.Equal(t, beforeS3, counters.FileService.S3.Get.Load())
			require.Equal(t, int32(1), calls.Load())
			require.Equal(t, []byte("def"), first.Entries[1].CachedData.Bytes())
			require.Same(t, first.Entries[1].CachedData, second.Entries[1].CachedData)
			require.Nil(t, second.Entries[1].decodeLease)
			if tc.saturate {
				require.Nil(t, first.Entries[1].decodeLease)
			} else {
				require.NotNil(t, first.Entries[1].decodeLease)
			}
			releaseVectors()
			releaseHeld()
			requireDecodeDrained(t, fs.decodedReads)
		})
	}
}

func TestS3FSSharedDecodeDiskFillFailureIsolation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		leaderError    bool
		leaderCancel   bool
		followerCancel bool
	}{
		{name: "leader_error", leaderError: true},
		{name: "leader_cancel", leaderCancel: true},
		{name: "follower_cancel", followerCancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, counters := newSharedDecodeDiskFillFS(t, CacheCallbacks{}, true)
			leaderCtx, cancelLeader := context.WithCancel(t.Context())
			followerCtx, cancelFollower := context.WithCancel(t.Context())
			defer cancelLeader()
			defer cancelFollower()
			leaderEntered, releaseLeader := make(chan struct{}), make(chan struct{})
			var leaderStarted, releaseLeaderOnce sync.Once
			wantLeaderError := errors.New("leader conversion failed")
			var leaderCalls, followerCalls atomic.Int32
			leaderDecode := func(ctx context.Context, _ io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
				leaderCalls.Add(1)
				leaderStarted.Do(func() { close(leaderEntered) })
				if tc.leaderCancel {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-releaseLeader:
					}
				} else {
					<-releaseLeader
				}
				if tc.leaderError {
					return nil, wantLeaderError
				}
				return allocator.CopyToCacheData(ctx, data), nil
			}
			followerDecode := func(ctx context.Context, _ io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
				followerCalls.Add(1)
				return allocator.CopyToCacheData(ctx, data), nil
			}
			first, second := selectedDecodeVector(1), selectedDecodeVector(1)
			first.Entries[1].ToCacheData = leaderDecode
			second.Entries[1].ToCacheData = followerDecode
			beforeDisk := counters.FileService.Cache.Disk.Read.Load()
			beforeS3 := counters.FileService.S3.Get.Load()
			leaderDone, followerDone := make(chan error, 1), make(chan error, 1)
			var workers sync.WaitGroup
			vectorsReleased := false
			releaseVectors := func() {
				if vectorsReleased {
					return
				}
				vectorsReleased = true
				first.ReleaseReadResultOnError()
				second.ReleaseReadResultOnError()
			}
			workers.Add(1)
			t.Cleanup(func() {
				cancelLeader()
				cancelFollower()
				releaseLeaderOnce.Do(func() { close(releaseLeader) })
				workers.Wait()
				releaseVectors()
			})
			go func() {
				defer workers.Done()
				leaderDone <- fs.Read(leaderCtx, &first)
			}()
			select {
			case <-leaderEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("disk-fill leader did not reach conversion")
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				followerDone <- fs.Read(followerCtx, &second)
			}()
			waitForSharedDecodeFillParticipants(t, fs.decodedReads, 2)

			waitResult := func(name string, done <-chan error) error {
				select {
				case err := <-done:
					return err
				case <-time.After(5 * time.Second):
					t.Fatalf("%s read did not finish", name)
					return nil
				}
			}
			var leaderErr, followerErr error
			switch {
			case tc.followerCancel:
				cancelFollower()
				followerErr = waitResult("follower", followerDone)
				releaseLeaderOnce.Do(func() { close(releaseLeader) })
				leaderErr = waitResult("leader", leaderDone)
				require.ErrorIs(t, followerErr, context.Canceled)
				require.NoError(t, leaderErr)
			case tc.leaderCancel:
				cancelLeader()
				leaderErr = waitResult("leader", leaderDone)
				followerErr = waitResult("follower", followerDone)
				require.ErrorIs(t, leaderErr, context.Canceled)
				require.NoError(t, followerErr)
			case tc.leaderError:
				releaseLeaderOnce.Do(func() { close(releaseLeader) })
				leaderErr = waitResult("leader", leaderDone)
				followerErr = waitResult("follower", followerDone)
				// Disk conversion errors are misses. The leader's fallback may
				// recover through the follower's successful same-key decode.
				if leaderErr == nil {
					require.Equal(t, []byte("def"), first.Entries[1].CachedData.Bytes())
				} else {
					require.ErrorIs(t, leaderErr, wantLeaderError)
				}
				require.NoError(t, followerErr)
			}
			workers.Wait()
			diskReads := counters.FileService.Cache.Disk.Read.Load()
			s3Reads := counters.FileService.S3.Get.Load()
			require.Greater(t, diskReads, beforeDisk)
			if !tc.leaderError {
				require.Equal(t, beforeS3, s3Reads)
			}
			require.Greater(t, leaderCalls.Load(), int32(0))
			if tc.followerCancel {
				require.Zero(t, followerCalls.Load())
				require.Equal(t, beforeDisk+3, diskReads)
				require.Equal(t, []byte("def"), first.Entries[1].CachedData.Bytes())
			} else {
				require.Equal(t, int32(1), followerCalls.Load())
				require.GreaterOrEqual(t, diskReads, beforeDisk+2)
				require.Equal(t, []byte("def"), second.Entries[1].CachedData.Bytes())
			}
			releaseLeaderOnce.Do(func() { close(releaseLeader) })
			releaseVectors()
			requireDecodeDrained(t, fs.decodedReads)
		})
	}
}

func TestS3FSSharedDecodeDiskFillClose(t *testing.T) {
	decodeEntered, releaseDecode := make(chan struct{}), make(chan struct{})
	var decodeStarted, releaseDecodeOnce sync.Once
	fs, counters := newSharedDecodeDiskFillFS(t, CacheCallbacks{}, true)
	storage := &blockingReadObjectStorage{ObjectStorage: fs.storage, readStarted: make(chan struct{}), releaseRead: make(chan struct{})}
	fs.storage = storage
	releaseStorage := sync.OnceFunc(func() { close(storage.releaseRead) })
	decode := func(ctx context.Context, _ io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
		decodeStarted.Do(func() { close(decodeEntered) })
		<-releaseDecode
		return allocator.CopyToCacheData(ctx, data), nil
	}
	var first, second IOVector
	var workers sync.WaitGroup
	vectorsReleased := false
	releaseVectors := func() {
		if vectorsReleased {
			return
		}
		vectorsReleased = true
		first.ReleaseReadResultOnError()
		second.ReleaseReadResultOnError()
	}
	closeDone := make(chan struct{})
	closeStarted := false
	t.Cleanup(func() {
		releaseDecodeOnce.Do(func() { close(releaseDecode) })
		releaseStorage()
		workers.Wait()
		if closeStarted {
			<-closeDone
		}
		releaseVectors()
	})
	first, second = selectedDecodeVector(1), selectedDecodeVector(1)
	first.Entries[1].ToCacheData = decode
	second.Entries[1].ToCacheData = decode
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		firstDone <- fs.Read(t.Context(), &first)
	}()
	select {
	case <-decodeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("disk-fill leader did not reach conversion")
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		secondDone <- fs.Read(t.Context(), &second)
	}()
	waitForSharedDecodeFillParticipants(t, fs.decodedReads, 2)
	beforeDisk := counters.FileService.Cache.Disk.Read.Load()
	beforeS3 := counters.FileService.S3.Get.Load()
	closeStarted = true
	go func() { fs.Close(t.Context()); close(closeDone) }()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("close waited for active disk-fill read")
	}
	_, registered := allMemoryCaches.Load(fs.memCache)
	require.True(t, registered)
	select {
	case err := <-secondDone:
		require.ErrorContains(t, err, "closed")
	case <-time.After(5 * time.Second):
		t.Fatal("closed fill follower did not return")
	}
	require.Equal(t, beforeDisk, counters.FileService.Cache.Disk.Read.Load())
	require.Equal(t, beforeS3, counters.FileService.S3.Get.Load())
	releaseDecodeOnce.Do(func() { close(releaseDecode) })
	select {
	case err := <-firstDone:
		require.ErrorContains(t, err, "closed")
	case <-storage.readStarted:
		releaseStorage()
		<-firstDone
		t.Fatal("closed disk decode fell back to object storage")
	case <-time.After(5 * time.Second):
		t.Fatal("closed disk decode did not return")
	}
	workers.Wait()
	require.Zero(t, storage.readCount.Load())
	require.Equal(t, beforeS3, counters.FileService.S3.Get.Load())
	_, registered = allMemoryCaches.Load(fs.memCache)
	require.False(t, registered)
	releaseVectors()
	requireDecodeDrained(t, fs.decodedReads)
}

func TestS3FSSharedDecodeDiskCanceledDoesNotFallback(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			fs, counters := newSharedDecodeDiskFillFS(t, CacheCallbacks{}, true)
			storage := &blockingReadObjectStorage{ObjectStorage: fs.storage, readStarted: make(chan struct{}), releaseRead: make(chan struct{})}
			fs.storage = storage
			releaseStorage := sync.OnceFunc(func() { close(storage.releaseRead) })
			entered, release := make(chan struct{}), make(chan struct{})
			releaseDecode := sync.OnceFunc(func() { close(release) })
			var ctx context.Context
			var cancel context.CancelFunc
			if deadline {
				ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
			} else {
				ctx, cancel = context.WithCancel(t.Context())
			}
			v := selectedDecodeVector(1)
			v.Entries[1].ToCacheData = func(ctx context.Context, _ io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
				close(entered)
				<-release
				return a.CopyToCacheData(ctx, data), nil
			}
			done := make(chan error, 1)
			workerFinished := make(chan struct{})
			go func() { defer close(workerFinished); done <- fs.Read(ctx, &v) }()
			t.Cleanup(func() {
				cancel()
				releaseDecode()
				releaseStorage()
				<-workerFinished
				v.ReleaseReadResultOnError()
			})
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("disk decode did not start")
			}
			if !deadline {
				cancel()
			}
			<-ctx.Done()
			beforeS3 := counters.FileService.S3.Get.Load()
			releaseDecode()
			select {
			case err := <-done:
				require.ErrorIs(t, err, ctx.Err())
			case <-storage.readStarted:
				releaseStorage()
				<-done
				t.Fatal("canceled disk decode fell back to object storage")
			case <-time.After(5 * time.Second):
				t.Fatal("canceled disk decode did not return")
			}
			require.Zero(t, storage.readCount.Load())
			require.Equal(t, beforeS3, counters.FileService.S3.Get.Load())
			v.ReleaseReadResultOnError()
			requireDecodeDrained(t, fs.decodedReads)
		})
	}
}

func TestS3FSSharedDecodeDiskFillSkipsColdPaths(t *testing.T) {
	for _, tc := range []struct {
		name     string
		disk     bool
		prefetch bool
		skipDisk bool
	}{
		{name: "no disk cache", disk: false},
		{name: "disk miss", disk: true},
		{name: "skip disk reads", disk: true, prefetch: true, skipDisk: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var fs *S3FS
			if tc.disk {
				fs, _ = newSharedDecodeDiskFillFS(t, CacheCallbacks{}, tc.prefetch)
			} else {
				fs = newSelectedDecodeFS(t, false)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var started, releaseOnce sync.Once
			v := selectedDecodeVector(1)
			if tc.skipDisk {
				v.Policy |= SkipDiskCacheReads
			}
			v.Entries[1].ToCacheData = func(ctx context.Context, r io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
				started.Do(func() { close(entered) })
				<-release
				return CacheOriginalData(ctx, r, data, allocator)
			}
			var workers sync.WaitGroup
			released := false
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				workers.Wait()
				if !released {
					v.ReleaseReadResultOnError()
					released = true
				}
			})
			done := make(chan error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				done <- fs.Read(t.Context(), &v)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("cold read did not reach conversion")
			}
			fs.decodedReads.mu.Lock()
			fillMapLen, fillCount := len(fs.decodedReads.fills), fs.decodedReads.fillCount
			fs.decodedReads.mu.Unlock()
			require.Zero(t, fillMapLen)
			require.Zero(t, fillCount)
			releaseOnce.Do(func() { close(release) })
			require.NoError(t, <-done)
			workers.Wait()
			if !released {
				v.ReleaseReadResultOnError()
				released = true
			}
			requireDecodeDrained(t, fs.decodedReads)
		})
	}
}

func TestS3FSSharedDecodeFallbackDoesNotRejoin(t *testing.T) {
	fs, counters := newSharedDecodeDiskFillFS(t, CacheCallbacks{}, true)
	beforeS3 := counters.FileService.S3.Get.Load()
	calls, generationsOnFallback := 0, -1
	v := IOVector{FilePath: "columns", Entries: []IOEntry{{
		Size: 3, CachedDataSize: 3, DecodeSharing: DecodeSharing{Codec: "test-retry"},
		ToCacheData: func(ctx context.Context, _ io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("injected disk conversion failure")
			}
			fs.decodedReads.mu.Lock()
			generationsOnFallback = fs.decodedReads.count
			fs.decodedReads.mu.Unlock()
			return allocator.CopyToCacheData(ctx, data), nil
		},
	}}}
	t.Cleanup(v.ReleaseReadResultOnError)
	require.NoError(t, fs.Read(t.Context(), &v))
	require.Equal(t, 2, calls)
	require.Equal(t, 0, generationsOnFallback, "storage fallback must not join another decode generation")
	require.Greater(t, counters.FileService.S3.Get.Load(), beforeS3)
	require.Equal(t, []byte("abc"), v.Entries[0].CachedData.Bytes())
	v.ReleaseReadResultOnError()
	requireDecodeDrained(t, fs.decodedReads)
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

func (f *failingDecodeCache) Set(context.Context, fscache.CacheKey, fscache.Data) error {
	f.calls++
	return errors.New("injected cache update failure")
}

func TestS3FSSelectedDecodeFailureIsolation(t *testing.T) {
	fs := newSelectedDecodeFS(t, false)
	admission := rejectSharedDecodeMemoryAdmission(t, fs)
	first := selectedDecodeVector(1)
	defer first.ReleaseReadResultOnError()
	require.NoError(t, fs.Read(t.Context(), &first))
	require.Positive(t, admission.rejected.Load())
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
	_, registered := allMemoryCaches.Load(fs.memCache)
	require.True(t, registered)
	unblockSibling()
	<-done
	require.NoError(t, readErr)
	_, registered = allMemoryCaches.Load(fs.memCache)
	require.False(t, registered)
	require.Equal(t, []byte("def"), v.Entries[1].CachedData.Bytes())
	require.NotNil(t, v.Entries[1].decodeLease)
	v.ReleaseReadResultOnError()
	requireDecodeDrained(t, fs.decodedReads)
}

func TestS3FSSelectedDecodeDiskSiblingFallbackAfterClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fs, counters := newSharedDecodeDiskFillFS(t, CacheCallbacks{}, true)
	beforeS3 := counters.FileService.S3.Get.Load()
	beforeDiskHits := counters.FileService.Cache.Disk.Hit.Load()
	entered, release := make(chan struct{}), make(chan struct{})
	releaseSibling := sync.OnceFunc(func() { close(release) })
	v := selectedDecodeVector(0)
	calls := 0
	v.Entries[1].ToCacheData = func(ctx context.Context, _ io.Reader, data []byte, a CacheDataAllocator) (fscache.Data, error) {
		calls++
		if calls == 1 {
			close(entered)
			<-release
			return nil, errors.New("injected sibling disk conversion failure")
		}
		return a.CopyToCacheData(ctx, data), nil
	}
	done, finished := make(chan error, 1), make(chan struct{})
	closeDone := make(chan struct{})
	closeStarted := false
	go func() { defer close(finished); done <- fs.Read(ctx, &v) }()
	t.Cleanup(func() {
		releaseSibling()
		cancel()
		select {
		case <-finished:
			v.ReleaseReadResultOnError()
		case <-time.After(5 * time.Second):
			t.Error("read did not stop during cleanup")
		}
		if closeStarted {
			select {
			case <-closeDone:
			case <-time.After(5 * time.Second):
				t.Error("close did not stop during cleanup")
			}
		}
	})
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("disk read did not reach the sibling conversion")
	}
	// The selected column was already decoded from disk before the sibling
	// failed. Closing must not erase it or prevent the sibling's normal retry.
	closeStarted = true
	go func() { fs.Close(ctx); close(closeDone) }()
	select {
	case <-closeDone:
	case <-ctx.Done():
		t.Fatal("close waited for the active read")
	}
	releaseSibling()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("sibling fallback did not complete")
	}
	require.Equal(t, 2, calls)
	require.Greater(t, counters.FileService.Cache.Disk.Hit.Load(), beforeDiskHits)
	require.Greater(t, counters.FileService.S3.Get.Load(), beforeS3)
	require.NotNil(t, v.Entries[0].decodeLease)
	for i := range v.Entries {
		require.Equal(t, []byte("abcdefghi")[3*i:3*i+3], v.Entries[i].CachedData.Bytes())
	}
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
	admission := rejectSharedDecodeMemoryAdmission(t, fs)
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
	beforeDisk := counters.FileService.Cache.Disk.Read.Load()
	vectors := make([]IOVector, 8)
	ready, proceed := make(chan struct{}, len(vectors)), make(chan struct{})
	releaseReads := sync.OnceFunc(func() { close(proceed) })
	var memoryHits atomic.Int32
	admission.afterGet = func(hit bool) {
		if hit {
			memoryHits.Add(1)
		}
		ready <- struct{}{}
		<-proceed
	}
	var workers sync.WaitGroup
	t.Cleanup(func() {
		releaseReads()
		workers.Wait()
		for i := range vectors {
			vectors[i].ReleaseReadResultOnError()
		}
	})
	done := make(chan error, 8)
	for i := range vectors {
		vectors[i] = makeVector()
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			done <- fs.Read(ctx, &vectors[i])
		}(i)
	}
	for range vectors {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("readers did not reach the memory-cache barrier")
		}
	}
	require.Zero(t, memoryHits.Load())
	releaseReads()
	for range vectors {
		require.NoError(t, <-done)
	}
	workers.Wait()
	admission.afterGet = nil
	require.Positive(t, admission.rejected.Load())
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, before, counters.FileService.S3.Get.Load())
	require.Equal(t, beforeDisk+8, counters.FileService.Cache.Disk.Read.Load())
	owner := vectors[0].Entries[0].CachedData
	for i := range vectors {
		require.Same(t, owner, vectors[i].Entries[0].CachedData)
		require.Equal(t, raw, owner.Bytes())
		vectors[i].ReleaseReadResultOnError()
	}
	require.Zero(t, owner.(*Bytes).refs.Load())
	requireDecodeDrained(t, fs.decodedReads)
	ClearMemoryCachePressureTargetByOwner(t.Name())
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

func (d *finalReleaseBarrier) Size() int64        { return d.base.Size() }
func (d *finalReleaseBarrier) BackingSize() int64 { return d.base.BackingSize() }

func (d *finalReleaseBarrier) Release() {
	if d.base.refs.Load() == 1 {
		close(d.entered)
		<-d.unblock
	}
	d.Data.Release()
}

func TestSharedDecodeQuotaWaitsForFinalRelease(t *testing.T) {
	r := newDecodedReadRegistry(16)
	base := sharedTestBytes([]byte("data"))
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
	unblockLeader()
	require.ErrorContains(t, <-done, "closed")
	vec.ReleaseReadResultOnError()
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
	unblockUpdate()
	require.NoError(t, <-done)
	require.Equal(t, []byte("abc"), vec.Entries[0].CachedData.Bytes(), "consumer owns its data after cache retirement")
	vec.Release()
	requireDecodeDrained(t, fs.decodedReads)
}
