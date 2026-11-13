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
	"bytes"
	"context"
	"io"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A size told before a delete is never stored after it, however the store and the
// delete interleave.
func TestPrefetchedSizeStoredOnlyWithoutDelete(t *testing.T) {
	var o prefetchedObjects
	told := o.deletes.Load()
	o.forget("object")
	o.storeCurrent("object", 10, told)
	_, known := o.size("object")
	require.False(t, known)
	o.storeCurrent("object", 20, o.deletes.Load())
	size, known := o.size("object")
	require.True(t, known)
	require.Equal(t, int64(20), size)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 200 {
				o.forget("other")
			}
		}()
		go func() {
			defer wg.Done()
			for range 200 {
				told := o.deletes.Load()
				o.storeCurrent("other", 1, told)
			}
		}()
	}
	wg.Wait()
	o.forget("other")
	_, known = o.size("other")
	require.False(t, known)
}

// A delete is seen by the reads begun before it and only for its object; once the
// remembered deletes are forgotten, every read begun before then counts as deleted.
func TestObjectDeletesSince(t *testing.T) {
	var o objectDeletes
	start := o.now()
	require.False(t, o.since("a", start))
	o.deleted("a")
	require.True(t, o.since("a", start))
	require.False(t, o.since("b", start))
	require.False(t, o.since("a", o.now()))

	before := o.now()
	for i := range objectDeletesMax {
		o.deleted(strconv.Itoa(i))
	}
	o.deleted("c")
	require.True(t, o.since("b", before), "the deletes since are forgotten")
	require.False(t, o.since("b", o.now()))
}

// heldStorage reads the first read's bytes at once and returns them when released.
type heldStorage struct {
	ObjectStorage
	once     sync.Once
	read     chan struct{}
	released chan struct{}
}

func (h *heldStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	r, err := h.ObjectStorage.Read(ctx, key, min, max)
	if err != nil {
		return nil, err
	}
	held := false
	h.once.Do(func() { held = true })
	if !held {
		return r, nil
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	close(h.read)
	select {
	case <-h.released:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// A column range download that read an object before it was deleted and recreated does
// not cache the old bytes after the delete, which cancels it: a later read returns the
// new object.
func TestColumnRangeReadBeforeDeleteIsNotCached(t *testing.T) {
	for _, async := range []bool{false, true} {
		fs, _, data := newPrefetchRangeFS(t, 64<<10, 300<<10)
		fs.SetAsyncUpdate(async)
		ctx := context.Background()
		held := &heldStorage{ObjectStorage: fs.storage, read: make(chan struct{}), released: make(chan struct{})}
		fs.storage = held
		req := ObjectPrefetch{FilePath: "object", Size: 10, Ranges: []PrefetchRange{{Offset: 100, Size: 10}}}
		done := make(chan struct{})
		go func() { defer close(done); fs.prefetchObject(ctx, req) }()
		<-held.read

		require.NoError(t, fs.Delete(ctx, "object"))
		recreated := slices.Clone(data)
		for i := range recreated {
			recreated[i] ^= 0xff
		}
		require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(recreated)), Data: recreated}}, Policy: SkipAllCache}))
		close(held.released)
		<-done
		fs.diskCache.flushAsyncUpdates(ctx)

		require.Equal(t, recreated[102:107], readRangeOf(t, fs, ColumnPrefetch, 102, 5), "async %v", async)
		_, cached := fs.diskCache.ranges.covering(fs.diskCache.rangeObjectKey("object"), 100, 10)
		require.False(t, cached, "the old range is not cached, async %v", async)
	}
}

// A read that skips disk cache reads is not served from cached prefetch ranges.
func TestPrefetchRangeSkipsDiskCacheReads(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	require.Equal(t, data[5000:5002], readRangeOf(t, fs, 0, 5000, 2))
	require.Equal(t, int64(1), storage.reads.Load())
	require.Equal(t, data[5010:5012], readRangeOf(t, fs, SkipDiskCacheReads, 5010, 2))
	require.Equal(t, int64(2), storage.reads.Load(), "read from object storage")
}

// Past the bytes of the prefetch range runs under way, a read reads only its entries;
// the bytes of runs that end are given back.
func TestPrefetchRangeRunsAreBounded(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 4096, 64<<10)
	// A run's buffer is at least 64 KiB, the bytes it counts for.
	fs.diskCache.prefetchFlightBytes = 64 << 10
	f := &fs.diskCache.prefetchFlights
	f.mu.Lock()
	f.bytes = 1
	f.mu.Unlock()
	require.Equal(t, data[5000:5002], readRangeOf(t, fs, 0, 5000, 2))
	require.Equal(t, int64(2), storage.bytes.Load(), "only the entry")

	f.mu.Lock()
	f.bytes = 0
	f.mu.Unlock()
	require.Equal(t, data[9000:9002], readRangeOf(t, fs, 0, 9000, 2))
	require.Equal(t, int64(2+4096), storage.bytes.Load(), "the prefetch range")
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Zero(t, f.bytes)
}

// A probe of a few columns reads exactly their entries, never the whole object, even when
// the entries lie far apart; policy 0 downloads the whole object.
func TestExactColumnsReadNeverDownloadsTheObject(t *testing.T) {
	for _, policy := range []Policy{ExactColumnsRead, 0} {
		fs, storage, data := newPrefetchRangeFS(t, 4096, 300<<10)
		vec := &IOVector{FilePath: "object", Policy: policy | SkipMemoryCache,
			Entries: []IOEntry{{Offset: 100, Size: 10}, {Offset: 200 << 10, Size: 10}}}
		require.NoError(t, fs.Read(context.Background(), vec))
		require.Equal(t, data[100:110], vec.Entries[0].Data)
		require.Equal(t, data[200<<10:200<<10+10], vec.Entries[1].Data)
		vec.Release()
		if policy == ExactColumnsRead {
			require.Equal(t, int64(2), storage.reads.Load())
			require.Equal(t, int64(20), storage.bytes.Load(), "only the entries")
		} else {
			require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")), "policy 0 caches the whole object")
		}
	}
}

// firstReadGate holds the first read until released; later reads pass.
type firstReadGate struct {
	ObjectStorage
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (g *firstReadGate) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	first := false
	g.once.Do(func() { first = true })
	if first {
		close(g.started)
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.ObjectStorage.Read(ctx, key, min, max)
}

// A column read of bytes a column range download is reading waits for the download
// instead of reading them from object storage again, and so do further downloads of them.
func TestColumnReadJoinsRangeDownload(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 64<<10, 300<<10)
	gate := &firstReadGate{ObjectStorage: fs.storage, started: make(chan struct{}), release: make(chan struct{})}
	fs.storage = gate
	ctx := context.Background()
	req := ObjectPrefetch{FilePath: "object", Size: 64 << 10, Ranges: []PrefetchRange{{Offset: 0, Size: 64 << 10}}}
	downloaded := make(chan struct{})
	go func() { defer close(downloaded); fs.prefetchObject(ctx, req) }()
	<-gate.started

	read := make(chan []byte, 1)
	go func() { read <- readRangeOf(t, fs, ColumnPrefetch, 100, 10) }()
	again := make(chan struct{})
	go func() { defer close(again); fs.prefetchObject(ctx, req) }()
	require.Never(t, func() bool { return len(read) > 0 }, 100*time.Millisecond, 5*time.Millisecond,
		"the read waits for the download")
	close(gate.release)
	require.Equal(t, data[100:110], <-read)
	<-downloaded
	<-again
	require.Equal(t, int64(1), storage.reads.Load(), "one read of object storage")
	require.Equal(t, data[200:210], readRangeOf(t, fs, ColumnPrefetch, 200, 10))
	require.Equal(t, int64(1), storage.reads.Load(), "served from the cached range")
}

// An entry read before a delete is never found by a read after the delete returns: a
// delete before its publication is claimed refuses it, and one after waits for the
// publication to end and removes the entry.
func TestDeleteWaitsForPublicationOfEarlierRead(t *testing.T) {
	for _, async := range []bool{false, true} {
		for _, claimed := range []bool{false, true} {
			fs, _, data := newPrefetchRangeFS(t, 64<<10, 300<<10)
			d := fs.diskCache
			ctx := context.Background()
			key := d.rangeObjectKey("object")
			entry := IOEntry{Offset: 100, Size: 10, Data: slices.Clone(data[100:110]), readDeletes: d.readDeletes()}

			deleted := make(chan struct{})
			var deleteErr error
			deleteObject := func() { defer close(deleted); deleteErr = d.DeletePaths(ctx, []string{"object"}) }
			var published, seenAfterDelete atomic.Bool
			if claimed {
				d.afterPublicationClaim = func() {
					go deleteObject()
					select {
					case <-deleted:
					case <-time.After(100 * time.Millisecond):
					}
				}
				d.afterFilePublication = func() {
					published.Store(true)
					select {
					case <-deleted:
						if _, cached := d.ranges.covering(key, 100, 10); cached {
							seenAfterDelete.Store(true)
						}
					default:
					}
				}
			} else {
				d.beforeFilePublication = deleteObject
				d.afterFilePublication = func() { published.Store(true) }
			}
			require.NoError(t, d.Update(ctx, &IOVector{FilePath: "object", Entries: []IOEntry{entry}}, async))
			d.flushAsyncUpdates(ctx)
			<-deleted
			require.NoError(t, deleteErr)

			require.Equal(t, claimed, published.Load(), "async %v claimed %v", async, claimed)
			require.False(t, seenAfterDelete.Load(), "a read after the delete found the entry, async %v", async)
			_, cached := d.ranges.covering(key, 100, 10)
			require.False(t, cached, "async %v claimed %v", async, claimed)
		}
	}
}
