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

package fileservice

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExactReadSkipsGapBeforeTail(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tailSize int64
	}{
		{name: "known_size", tailSize: 16},
		{name: "read_to_end", tailSize: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newPrefetchRangeFS(t, 0, 1024)
			vector := &IOVector{
				FilePath: "object",
				Policy:   ExactColumnsRead | SkipAllCache,
				Entries:  []IOEntry{{Offset: 0, Size: 16}, {Offset: 1008, Size: tc.tailSize}},
			}
			defer vector.Release()
			require.NoError(t, fs.Read(context.Background(), vector))
			require.Equal(t, data[:16], vector.Entries[0].Data)
			require.Equal(t, data[1008:], vector.Entries[1].Data)
			storage.mu.Lock()
			defer storage.mu.Unlock()
			require.Equal(t, []int64{0, 1008}, storage.starts, "exact reads should open each entry without reading the intervening bytes")
		})
	}
}

func TestPrefetchUnknownReadCountGetsWholeObjectPriority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		blocks int
	}{
		{name: "object_block_count_unknown"},
		{name: "object_block_count_known", blocks: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			gate := newGatedRun("hold", "unknown", "partial")
			p := newPrefetcher(1, func() int64 { return 2 }, func(ObjectPrefetch) bool { return false }, gate.run)
			defer p.close(context.Background())
			defer gate.releaseAll()
			next := func() string {
				t.Helper()
				select {
				case path := <-gate.started:
					return path
				case <-ctx.Done():
					t.Fatal("prefetch worker did not start the next request")
					return ""
				}
			}
			p.add(ObjectPrefetch{FilePath: "hold", Size: 1})
			require.Equal(t, "hold", next())
			p.add(ObjectPrefetch{FilePath: "unknown", Size: 1, Due: 2, Blocks: tc.blocks})
			p.add(ObjectPrefetch{FilePath: "partial", Size: 1, Due: 1, Reads: 1, Blocks: 10})
			close(gate.release["hold"])
			require.Equal(t, "unknown", next(), "Reads=0 means unknown and should rank as a whole-object read")
		})
	}
}

type idlePartStorage struct {
	ObjectStorage
	opened   atomic.Bool
	canceled chan struct{}
	proceed  chan struct{}
}

func (s *idlePartStorage) Read(ctx context.Context, path string, min, max *int64) (io.ReadCloser, error) {
	if min != nil && *min == 0 && s.opened.CompareAndSwap(false, true) {
		return io.NopCloser(&slowCancelReader{ctx: ctx, canceled: s.canceled, proceed: s.proceed}), nil
	}
	return s.ObjectStorage.Read(ctx, path, min, max)
}

func TestJoinedWholeReadFallsBackAfterPartIdleTimeout(t *testing.T) {
	shortIdle(t, 50*time.Millisecond)
	for _, tc := range []struct {
		name  string
		parts int
	}{
		{name: "single_stream", parts: 1},
		{name: "split_download", parts: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: tc.parts, min: 1}, 256<<10)
			fs.diskCache.downloadMaxExtraWait = time.Minute
			<-fs.diskCache.load.done
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			storage := &idlePartStorage{
				ObjectStorage: fs.governor.raw,
				canceled:      make(chan struct{}),
				proceed:       make(chan struct{}),
			}
			fs.governor.raw = storage
			unblock := sync.OnceFunc(func() { close(storage.proceed) })
			prefetchDone := make(chan struct{})
			go func() {
				defer close(prefetchDone)
				_ = fs.PrefetchFile(ctx, "object")
			}()
			defer func() { cancel(); unblock(); <-prefetchDone }()
			select {
			case <-storage.canceled:
			case <-ctx.Done():
				t.Fatal("prefetch part did not hit its idle timeout")
			}
			download := fs.diskCache.download(fs.diskCache.objectKey("object"))
			require.NotNil(t, download)
			vector := &IOVector{FilePath: "object", Policy: ExactColumnsRead | SkipMemoryCache, Entries: []IOEntry{{Offset: 0, Size: 16}}}
			readDone := make(chan struct{})
			var readErr error
			go func() {
				defer close(readDone)
				readErr = fs.Read(ctx, vector)
			}()
			defer func() { cancel(); unblock(); <-readDone; vector.Release() }()
			require.Eventually(t, func() bool {
				download.idle.mu.Lock()
				defer download.idle.mu.Unlock()
				return download.idle.waiting > 0
			}, time.Second, time.Millisecond, "foreground read should join before the stalled part returns")
			unblock()
			select {
			case <-readDone:
				require.NoError(t, readErr, "a late joiner should fall back to its exact bytes after a prefetch idle timeout")
				require.Equal(t, data[:16], vector.Entries[0].Data)
			case <-ctx.Done():
				t.Fatal("joined read failed to return")
			}
		})
	}
}
