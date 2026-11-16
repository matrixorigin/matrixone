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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// readUnit reads size bytes at offset of the object with prefetch ranges.
func readUnit(ctx context.Context, fs *S3FS, offset, size int64) ([]byte, error) {
	vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
		Entries: []IOEntry{{Offset: offset, Size: size}}}
	err := fs.Read(ctx, vec)
	data := slices.Clone(vec.Entries[0].Data)
	vec.Release()
	return data, err
}

func fileObjects(d *DiskCache) int {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, objects := range f.m {
		for _, o := range objects {
			if _, ok := o.(*filePrefetchObject); ok {
				n++
			}
		}
	}
	return n
}

// Two reads of a run past disk-prefetch-memory-max that miss at once download it once, as
// one FilePrefetchObject both read from.
func TestFilePrefetchObjectReadsJoinOneDownload(t *testing.T) {
	const unit = 64 << 10
	fs, _, data := newPrefetchRangeFS(t, unit, 4*unit)
	fs.diskCache.memoryPrefetchMax = 16 << 10
	storage := &gateStorage{ObjectStorage: fs.storage, release: make(chan struct{})}
	fs.storage = storage

	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 2)
	for _, off := range []int64{100, 200} {
		go func() {
			got, err := readUnit(context.Background(), fs, off, 10)
			done <- result{got, err}
		}()
	}
	require.Eventually(t, func() bool { return storage.opens.Load() == 1 }, 10*time.Second, time.Millisecond)
	require.Never(t, func() bool { return storage.opens.Load() > 1 }, 200*time.Millisecond, time.Millisecond,
		"the second read joins the first one's download")
	close(storage.release)
	got := map[string]bool{}
	for range 2 {
		r := <-done
		require.NoError(t, r.err)
		got[string(r.data)] = true
	}
	require.True(t, got[string(data[100:110])])
	require.True(t, got[string(data[200:210])])
	fs.diskCache.prefetchObjects.reads.Wait()
	require.Equal(t, int64(1), storage.opens.Load())
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForIOEntry("object", IOEntry{Offset: 0, Size: unit})))
}

// A FilePrefetchObject a read starts is stopped when the read ends before its download
// starts, and reads nothing.
func TestFilePrefetchObjectOfEndedReadStops(t *testing.T) {
	const unit = 64 << 10
	fs, storage, _ := newPrefetchRangeFS(t, unit, 4*unit)
	d := fs.diskCache
	d.memoryPrefetchMax = 16 << 10
	d.maxDownloadConns = 1
	d.downloadConns.Store(1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := readUnit(ctx, fs, 100, 10)
		done <- err
	}()
	require.Eventually(t, func() bool {
		d.connsMu.Lock()
		defer d.connsMu.Unlock()
		return d.connsFreed != nil
	}, 10*time.Second, time.Millisecond, "the download waits for a connection")
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Eventually(t, func() bool { return fileObjects(d) == 0 }, 10*time.Second, time.Millisecond)
	d.prefetchObjects.reads.Wait()
	d.releaseDownloadConns(1)
	require.Zero(t, storage.reads.Load())
	require.Zero(t, d.downloadConns.Load())
}

// A read that leaves caching to the prefetch range does not join a MemoryPrefetchObject
// that caches nothing: it reads the range itself, which is cached.
func TestPrefetchRangeDoesNotJoinUncachedObject(t *testing.T) {
	const unit = 4096
	fs, _, data := newPrefetchRangeFS(t, unit, 64<<10)
	d := fs.diskCache
	held := make(chan struct{})
	opened := make(chan struct{})
	fetched := make(chan error, 1)
	go func() {
		_, _, _, err := d.fetchRange(context.Background(), "object", 0, 0, unit, func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
			close(opened)
			<-held
			return fs.storage.Read(ctx, "object", &start, &end)
		})
		fetched <- err
	}()
	<-opened
	got, err := readUnit(context.Background(), fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Eventually(t, func() bool {
		_, cached := d.ranges.covering(d.rangeObjectKey("object"), 0, unit)
		return cached
	}, 10*time.Second, time.Millisecond, "the read's own prefetch range is cached")
	close(held)
	require.NoError(t, <-fetched)
}

// A delete of an object stops the download a whole-object read started; the read then
// reads its range and returns its bytes.
func TestDeleteDuringWholeObjectReadReadsRange(t *testing.T) {
	fs, storage, data := newDownloadContractFS(t, time.Hour)
	vec, done := readAsync(fs, 6<<20, 17)
	require.Eventually(t, func() bool { return fs.diskCache.download(fs.diskCache.pathForFile("object")) != nil },
		10*time.Second, time.Millisecond)
	require.NoError(t, fs.diskCache.DeletePaths(context.Background(), []string{"object"}))
	requireReturns(t, done)
	require.Equal(t, data[6<<20:6<<20+17], vec.Entries[0].Data)
	require.Positive(t, storage.ranges.Load(), "the read fetched its range")
}
