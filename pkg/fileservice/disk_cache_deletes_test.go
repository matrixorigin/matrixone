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
	"testing"

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
	<-h.released
	return io.NopCloser(bytes.NewReader(data)), nil
}

// A column range download that read an object before it was deleted and recreated does
// not cache the old bytes after the delete: a later read returns the new object.
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
	fs.diskCache.prefetchFlightBytes = 4096
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
