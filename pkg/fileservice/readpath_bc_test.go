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
