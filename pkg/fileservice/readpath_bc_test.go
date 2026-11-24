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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// probeStorage holds the first read from offset at until gate is closed, then answers it
// through answer, given the reader of object storage; it counts the other reads.
type probeStorage struct {
	ObjectStorage
	at     int64
	gate   chan struct{}
	answer func(io.ReadCloser) (io.ReadCloser, error)
	hit    atomic.Bool
	others atomic.Int64
}

func (s *probeStorage) Read(ctx context.Context, key string, from, to *int64) (io.ReadCloser, error) {
	if from != nil && *from == s.at && s.hit.CompareAndSwap(false, true) {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		r, err := s.ObjectStorage.Read(ctx, key, from, to)
		if err != nil {
			return nil, err
		}
		return s.answer(r)
	}
	s.others.Add(1)
	return s.ObjectStorage.Read(ctx, key, from, to)
}

type readCloser2 struct {
	io.Reader
	io.Closer
}

// failAfter returns a reader of n bytes of r that then fails with err.
func failAfter(n int64, err error) func(io.ReadCloser) (io.ReadCloser, error) {
	return func(r io.ReadCloser) (io.ReadCloser, error) {
		return readCloser2{Reader: io.MultiReader(io.LimitReader(r, n), failingReader{err}), Closer: r}, nil
	}
}

// shortBy returns a reader of r without its last n bytes.
func shortBy(r PrefetchRange, n int64) func(io.ReadCloser) (io.ReadCloser, error) {
	return func(rc io.ReadCloser) (io.ReadCloser, error) {
		return readCloser2{Reader: io.LimitReader(rc, r.Size-n), Closer: rc}, nil
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

// joinedColumnRead prefetches r of the object, held in s, and returns what a column read of
// r that joins the prefetch returns once s answers it.
func joinedColumnRead(t *testing.T, fileBacked bool, r PrefetchRange, answer func(io.ReadCloser) (io.ReadCloser, error)) ([]byte, []byte, error, *probeStorage) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
	<-fs.diskCache.load.done
	if fileBacked {
		fs.diskCache.memoryPrefetchMax = r.Size - 1
		fs.diskCache.downloadMaxExtraWait = time.Hour
	}
	s := &probeStorage{ObjectStorage: storage, at: r.Offset, gate: make(chan struct{}), answer: answer}
	fs.storage = s
	release := sync.OnceFunc(func() { close(s.gate) })
	t.Cleanup(release)
	require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r}}))
	require.Eventually(t, s.hit.Load, 5*time.Second, time.Millisecond)
	result := make(chan error, 1)
	var got []byte
	go func() {
		b, err := readColumnRange(ctx, fs, r)
		got = b
		result <- err
	}()
	diskPath := fs.diskCache.pathForFile("object")
	require.Eventually(t, func() bool {
		if !fileBacked {
			return memoryWaiters(fs, diskPath) == 1
		}
		o, _ := fs.diskCache.fileDownload(diskPath, r.Offset, r.Offset+r.Size)
		if o == nil {
			return false
		}
		o.mu.Lock()
		defer o.mu.Unlock()
		return len(o.waiters) == 1
	}, 5*time.Second, time.Millisecond, "the read joins the prefetch")
	release()
	err := <-result
	waitPrefetcherIdle(t, fs)
	return got, data[r.Offset : r.Offset+r.Size], err, s
}

var joinedKinds = []struct {
	name       string
	fileBacked bool
}{{name: "memory"}, {name: "file", fileBacked: true}}

// BC2: a read whose joined prefetch object fails with an unexpected end of its source gets
// the error a read of its own reports.
func TestJoinedReadReportsUnexpectedEOFAsOwnRead(t *testing.T) {
	r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
	for _, k := range joinedKinds {
		t.Run(k.name, func(t *testing.T) {
			_, _, err, _ := joinedColumnRead(t, k.fileBacked, r, failAfter(100, io.ErrUnexpectedEOF))
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF), "got %v", err)
		})
	}
}

// BC1: a read whose joined prefetch object finds the object gone fails as its own read
// would, without reading object storage again.
func TestJoinedReadFailsOnMissingObject(t *testing.T) {
	r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
	for _, k := range joinedKinds {
		t.Run(k.name, func(t *testing.T) {
			_, _, err, s := joinedColumnRead(t, k.fileBacked, r, func(rc io.ReadCloser) (io.ReadCloser, error) {
				_ = rc.Close()
				return nil, moerr.NewFileNotFoundNoCtx("object")
			})
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "got %v", err)
			require.Zero(t, s.others.Load(), "the read does not read the object again")
		})
	}
}

// BC3: a read whose joined prefetch object's source ends short of its bytes reads them
// itself.
func TestJoinedReadOfShortSourceReadsItsBytes(t *testing.T) {
	r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
	for _, k := range joinedKinds {
		for _, short := range []int64{100, r.Size} {
			t.Run(k.name+"/"+map[bool]string{true: "empty", false: "short"}[short == r.Size], func(t *testing.T) {
				got, want, err, s := joinedColumnRead(t, k.fileBacked, r, shortBy(r, short))
				require.NoError(t, err)
				require.True(t, bytes.Equal(want, got))
				require.EqualValues(t, 1, s.others.Load(), "the read reads its bytes itself")
			})
		}
	}
}

// BC4: a read that started a whole-object download and cannot read the bytes the download
// wrote, from the download's file, reads them elsewhere.
func TestStreamedReadFallsBackOnUnreadableDownloadFile(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 300<<10)
	d := fs.diskCache
	<-d.load.done
	storage := newBlackBoxStorage(fs.storage, func(_ string, offset int64) bool { return offset == 0 })
	fs.storage = storage
	release := sync.OnceFunc(func() { close(storage.gate) })
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	vec := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 1000, Size: 4000}}}
	defer vec.Release()
	result := make(chan error, 1)
	go func() { result <- fs.Read(ctx, vec) }()
	diskPath := d.pathForFile("object")
	var download *objectDownload
	require.Eventually(t, func() bool {
		if download = d.download(diskPath); download == nil {
			return false
		}
		download.mu.Lock()
		defer download.mu.Unlock()
		return len(download.waiters) == 1
	}, 5*time.Second, time.Millisecond, "the read waits for its download")
	download.mu.Lock()
	require.NoError(t, download.file.Close())
	download.mu.Unlock()
	release()
	require.NoError(t, <-result)
	require.True(t, bytes.Equal(data[1000:5000], vec.Entries[0].Data))
}

// stalledWholeDownload starts a whole-object download of the object whose source sends
// nothing until the returned function is called.
func stalledWholeDownload(t *testing.T, fs *S3FS) (*objectDownload, func()) {
	d := fs.diskCache
	<-d.load.done
	storage := newBlackBoxStorage(fs.storage, func(_ string, offset int64) bool { return offset == 0 })
	fs.storage = storage
	release := sync.OnceFunc(func() { close(storage.gate) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = fs.PrefetchFile(context.Background(), "object")
	}()
	t.Cleanup(func() {
		release()
		<-done
	})
	var download *objectDownload
	require.Eventually(t, func() bool {
		download = d.download(d.pathForFile("object"))
		return download != nil
	}, 5*time.Second, time.Millisecond)
	return download, release
}

// BC12: a read of the disk cache whose context ends while it waits for a download returns
// its context's error.
func TestDownloadWaitEndsWithReadContext(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	fs.diskCache.downloadMaxExtraWait = time.Hour
	download, _ := stalledWholeDownload(t, fs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	vec := &IOVector{FilePath: "object", Entries: []IOEntry{{Size: 4096}}}
	defer vec.Release()
	result := make(chan error, 1)
	go func() { result <- fs.diskCache.Read(ctx, vec) }()
	require.Eventually(t, func() bool {
		download.mu.Lock()
		defer download.mu.Unlock()
		return len(download.waiters) == 1
	}, 5*time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

// BC19: a prefetch whose object is deleted while it downloads counts as ended, not failed.
func TestPrefetchOfDeletedObjectEnds(t *testing.T) {
	for _, whole := range []bool{false, true} {
		t.Run(map[bool]string{true: "whole", false: "ranges"}[whole], func(t *testing.T) {
			fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
			<-fs.diskCache.load.done
			storage := newBlackBoxStorage(fs.storage, func(string, int64) bool { return true })
			fs.storage = storage
			t.Cleanup(func() { close(storage.gate) })
			failed := promtestutil.ToFloat64(metric.FSPrefetchFailedCounter)
			ended := promtestutil.ToFloat64(metric.FSPrefetchEndedCounter)
			req := ObjectPrefetch{FilePath: "object", Size: 300 << 10}
			if !whole {
				req.Ranges = []PrefetchRange{{Offset: 4096, Size: 4096}}
			}
			require.NoError(t, fs.PrefetchObject(context.Background(), req))
			key := fs.pathToKey("object")
			require.Eventually(t, func() bool {
				cur, _, _ := storage.counts(key)
				return cur == 1
			}, 5*time.Second, time.Millisecond)
			require.NoError(t, fs.Delete(context.Background(), "object"))
			waitPrefetcherIdle(t, fs)
			require.Equal(t, failed, promtestutil.ToFloat64(metric.FSPrefetchFailedCounter))
			require.Equal(t, ended+1, promtestutil.ToFloat64(metric.FSPrefetchEndedCounter))
		})
	}
}

// BC6: an entry of no bytes is an empty range for the disk cache too.
func TestDiskCacheReadOfEmptyEntryIsEmptyRange(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 64<<10)
	<-fs.diskCache.load.done
	vec := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 10}}}
	defer vec.Release()
	err := fs.diskCache.Read(context.Background(), vec)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrEmptyRange), "got %v", err)
}

// BC7: the index-file cache serves every entry it can, also those after one read to the
// end of the object.
func TestIndexObjectServesEntriesAfterReadToEnd(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	cacheIndexObject(t, fs, "object", data)
	vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipMemoryCache,
		Entries: []IOEntry{{Offset: 0, Size: -1}, {Offset: 100, Size: 10}}}
	defer vec.Release()
	require.NoError(t, fs.readIndexObject(context.Background(), vec))
	require.False(t, vec.Entries[0].done)
	require.True(t, vec.Entries[1].done)
	require.Equal(t, data[100:110], vec.Entries[1].Data)
}

// BC8: the bytes a read finds cached once a prefetch object it joined ended are read from
// cached ranges that hold them together, or from the cached whole object.
func TestCachedBytesAcrossRangesAndWholeObject(t *testing.T) {
	const part = 64 << 10
	ctx := context.Background()
	t.Run("ranges", func(t *testing.T) {
		fs, _, data := newPrefetchRangeFS(t, 0, 4*part)
		d := fs.diskCache
		<-d.load.done
		require.NoError(t, d.Update(ctx, &IOVector{FilePath: "object", Entries: []IOEntry{
			{Size: part, Data: data[:part]},
			{Offset: part, Size: part, Data: data[part : 2*part]},
		}}, false))
		dst := make([]byte, 32)
		var deadline time.Time
		require.True(t, d.readCachedBytes(ctx, "object", part-16, dst, &deadline))
		require.Equal(t, data[part-16:part+16], dst)
	})
	t.Run("whole", func(t *testing.T) {
		fs, _, data := newPrefetchRangeFS(t, 0, 4*part)
		d := fs.diskCache
		<-d.load.done
		require.NoError(t, d.setFileContent(ctx, "object", data, false))
		dst := make([]byte, 32)
		var deadline time.Time
		require.True(t, d.readCachedBytes(ctx, "object", part-16, dst, &deadline))
		require.Equal(t, data[part-16:part+16], dst)
		require.False(t, d.readCachedBytes(ctx, "object", int64(len(data))-16, dst, &deadline), "bytes past the object's end")
		require.True(t, d.cache.Contains(d.pathForFile("object")), "a read past its end keeps the whole object")
	})
}

// BC17: a read whose whole-object fill was skipped, as for an object cached meanwhile,
// reads the disk cache again before object storage.
func TestSkippedWholeFillReadsCacheAgain(t *testing.T) {
	fs, storage, data := newPrefetchRangeFS(t, 0, 64<<10)
	d := fs.diskCache
	<-d.load.done
	ctx := context.Background()
	require.NoError(t, d.setFileContent(ctx, "object", data, false))
	vec := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 1000, Size: 100}}}
	defer vec.Release()
	require.NoError(t, fs.read(ctx, vec, false, false))
	require.Equal(t, data[1000:1100], vec.Entries[0].Data)
	require.Zero(t, storage.reads.Load(), "the cached object serves the read")
}

// BC16: an exact read joins a queued prefetch of ranges holding its bytes, as a column
// read does.
func TestExactReadJoinsQueuedRanges(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 300<<10)
	<-fs.diskCache.load.done
	storage := newBlackBoxStorage(fs.storage, func(string, int64) bool { return true })
	fs.storage = storage
	t.Cleanup(func() { close(storage.gate) })
	r := PrefetchRange{Offset: 4096, Size: 4096}
	require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: 300 << 10, Ranges: []PrefetchRange{r}}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	vec := &IOVector{FilePath: "object", Policy: ExactColumnsRead, Entries: []IOEntry{{Offset: r.Offset, Size: r.Size}}}
	require.True(t, fs.joinQueued(ctx, vec, fs.sourcesFor(ctx, vec)))
}

// BC15: a read made under a download's context joins no prefetch object.
func TestReadOfDownloadJoinsNoPrefetchObject(t *testing.T) {
	r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
	for _, policy := range []Policy{ColumnPrefetch | SkipFullFilePreloads, SkipFullFilePreloads} {
		t.Run(map[bool]string{true: "column", false: "range"}[policy.Any(ColumnPrefetch)], func(t *testing.T) {
			fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
			<-fs.diskCache.load.done
			s := &probeStorage{ObjectStorage: storage, at: r.Offset, gate: make(chan struct{}),
				answer: func(rc io.ReadCloser) (io.ReadCloser, error) { return rc, nil }}
			fs.storage = s
			t.Cleanup(sync.OnceFunc(func() { close(s.gate) }))
			require.NoError(t, fs.PrefetchObject(context.Background(), ObjectPrefetch{FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r}}))
			require.Eventually(t, s.hit.Load, 5*time.Second, time.Millisecond)
			ctx, cancel := context.WithTimeout(withDownloadStarted(context.Background(), func() {}), 5*time.Second)
			defer cancel()
			vec := &IOVector{FilePath: "object", Policy: policy | SkipMemoryCache, Entries: []IOEntry{{Offset: r.Offset, Size: r.Size}}}
			defer vec.Release()
			require.NoError(t, fs.Read(ctx, vec))
			require.Equal(t, data[r.Offset:r.Offset+r.Size], vec.Entries[0].Data)
			require.EqualValues(t, 1, s.others.Load(), "the read reads its bytes itself")
		})
	}
}

// BC13: a fill of an index object that skips every cache write fills neither the
// index-file cache nor the disk cache.
func TestIndexObjectFillSkippingCacheWrites(t *testing.T) {
	fs, _, _ := newIndexObjectFS(t, true)
	ctx := WithFileServicePolicy(context.Background(), IndexFileObject|SkipCacheWrites)
	require.NoError(t, fs.PrefetchFile(ctx, "object"))
	fs.diskCache.prefetchObjects.reads.Wait()
	_, cached := fs.indexObjectCached("object")
	require.False(t, cached)
	require.Zero(t, fs.diskCache.cache.Used())
	require.True(t, FullFilePreloadSkipped(ctx, fs, IndexFileObject|SkipCacheWrites, "object"))
}

// BC14: a read of an index object that skips disk-cache reads joins its download into the
// index-file cache.
func TestIndexObjectReadSkippingDiskReadsJoinsIndexDownload(t *testing.T) {
	fs, _, data := newIndexObjectFS(t, true)
	d := fs.diskCache
	d.downloadMaxExtraWait = time.Hour
	storage := newBlackBoxStorage(fs.storage, func(_ string, offset int64) bool { return offset == 0 })
	fs.storage = storage
	release := sync.OnceFunc(func() { close(storage.gate) })
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	filled := make(chan error, 1)
	go func() { filled <- fs.PrefetchFile(WithFileServicePolicy(ctx, IndexFileObject), "object") }()
	var download *objectDownload
	require.Eventually(t, func() bool {
		download = d.download(d.pathForFile("object"))
		return download != nil
	}, 5*time.Second, time.Millisecond)
	require.True(t, download.indexFile)
	vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipDiskCacheReads | SkipMemoryCache,
		Entries: []IOEntry{{Offset: 100, Size: 4096}}}
	defer vec.Release()
	result := make(chan error, 1)
	go func() { result <- fs.Read(ctx, vec) }()
	require.Eventually(t, func() bool {
		download.mu.Lock()
		defer download.mu.Unlock()
		return len(download.waiters) == 1
	}, 5*time.Second, time.Millisecond, "the read joins the download")
	release()
	require.NoError(t, <-result)
	require.NoError(t, <-filled)
	require.Equal(t, data[100:4196], vec.Entries[0].Data)
	_, _, total := storage.counts(fs.pathToKey("object"))
	require.Equal(t, 1, total, "the download alone reads the object")
}

// BC10: a read that caches nothing joins any prefetch object holding its bytes, also one
// that caches nothing.
func TestUncachingReadJoinsUncachingFlight(t *testing.T) {
	r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
	fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
	<-fs.diskCache.load.done
	s := &probeStorage{ObjectStorage: storage, at: r.Offset, gate: make(chan struct{}),
		answer: func(rc io.ReadCloser) (io.ReadCloser, error) { return rc, nil }}
	fs.storage = s
	release := sync.OnceFunc(func() { close(s.gate) })
	t.Cleanup(release)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache, Entries: []IOEntry{{Offset: r.Offset, Size: r.Size}}}
	defer first.Release()
	firstDone := make(chan error, 1)
	go func() { firstDone <- fs.Read(ctx, first) }()
	diskPath := fs.diskCache.pathForFile("object")
	require.Eventually(t, s.hit.Load, 5*time.Second, time.Millisecond)
	var out bytes.Buffer
	second := &IOVector{FilePath: "object", Policy: ColumnPrefetch | SkipFullFilePreloads | SkipDiskCacheWrites | SkipMemoryCache,
		Entries: []IOEntry{{Offset: r.Offset, Size: r.Size, WriterForRead: &out}}}
	defer second.Release()
	secondDone := make(chan error, 1)
	go func() { secondDone <- fs.Read(ctx, second) }()
	require.Eventually(t, func() bool { return memoryWaiters(fs, diskPath) == 2 }, 5*time.Second, time.Millisecond,
		"the second read joins the first one's flight")
	release()
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, data[r.Offset:r.Offset+r.Size], out.Bytes())
	require.Zero(t, s.others.Load())
}

// BC11: a read of the disk cache takes its bytes from the fastest live FilePrefetchObject
// holding them, not from the first one registered.
func TestFileDownloadPicksFastestLiveObject(t *testing.T) {
	fs, _, _ := newPrefetchRangeFS(t, 0, 64<<10)
	d := fs.diskCache
	<-d.load.done
	diskPath := d.pathForFile("object")
	tempPath := t.TempDir() + "/download"
	require.NoError(t, os.WriteFile(tempPath, make([]byte, 64<<10), 0600))
	object := func(written int64, stopped bool) *filePrefetchObject {
		p := newFilePrefetchObject(context.Background(), 0, 64<<10)
		o := newObjectDownload(tempPath, false, nil)
		t.Cleanup(o.retire)
		o.begin(0)
		if written > 0 {
			o.advance(0, written)
		}
		p.download = o
		if stopped {
			p.cancelRead()
		}
		return p
	}
	slow, fast, stoppedFast := object(0, false), object(64<<10, false), object(64<<10, true)
	f := &d.prefetchObjects
	f.mu.Lock()
	f.registerLocked(diskPath, stoppedFast)
	f.registerLocked(diskPath, slow)
	f.registerLocked(diskPath, fast)
	f.mu.Unlock()
	got, _ := d.fileDownload(diskPath, 100, 200)
	require.Same(t, fast.download, got)
}
