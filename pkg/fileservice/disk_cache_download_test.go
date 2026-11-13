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
	"errors"
	"io"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

// downloading starts a whole-object fill of "obj" whose source is a pipe the test writes.
func downloading(t *testing.T) (*DiskCache, *io.PipeWriter, chan error) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close(ctx) })
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })
	filled := make(chan error, 1)
	go func() {
		filled <- cache.SetFile(ctx, "obj", func(context.Context) (io.ReadCloser, error) { return pr, nil })
	}()
	require.Eventually(t, func() bool { return cache.download(cache.pathForFile("obj")) != nil },
		diskCacheLifecycleTestTimeout, time.Millisecond)
	return cache, pw, filled
}

func readRange(ctx context.Context, cache *DiskCache, offset, size int64) chan *IOVector {
	done := make(chan *IOVector, 1)
	go func() {
		vec := &IOVector{FilePath: "obj", Entries: []IOEntry{{Offset: offset, Size: size}}}
		_ = cache.Read(ctx, vec)
		done <- vec
	}()
	return done
}

// downloadsRate sets the recent download rate for the test.
func downloadsRate(t *testing.T, bytesPerS float64) {
	rate0 := downloadRates.rate()
	t.Cleanup(func() { downloadRates.set(rate0) })
	downloadRates.set(bytesPerS)
}

// rangeCosts sets the cost of range reads for the test.
func rangeCosts(t *testing.T, latency, bytesPerS float64) {
	c0 := rangeReadCosts.get()
	t.Cleanup(func() { rangeReadCosts.set(c0.latency, c0.bytesPerS) })
	rangeReadCosts.set(latency, bytesPerS)
}

func waitVector(t *testing.T, done chan *IOVector) *IOVector {
	select {
	case vec := <-done:
		return vec
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("read did not return")
		return nil
	}
}

// rangeReadsCost makes every range read take latency for the test.
func rangeReadsCost(t *testing.T, latency time.Duration) {
	rangeCosts(t, latency.Seconds(), 1<<40)
}

// A read of an object being downloaded returns its bytes once they are written, without
// waiting for the rest of the object.
func TestDiskCacheReadWaitsForDownloadedBytes(t *testing.T) {
	rangeReadsCost(t, time.Hour)
	cache, pw, filled := downloading(t)
	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)

	vec := waitVector(t, readRange(context.Background(), cache, 1, 3))
	require.True(t, vec.Entries[0].done, "bytes already written")
	require.Equal(t, []byte("ell"), vec.Entries[0].Data)

	later := readRange(context.Background(), cache, 6, 5)
	select {
	case <-later:
		t.Fatal("read returned before its bytes were written")
	case <-time.After(20 * time.Millisecond):
	}
	_, err = pw.Write([]byte(" world"))
	require.NoError(t, err)
	vec = waitVector(t, later)
	require.True(t, vec.Entries[0].done)
	require.Equal(t, []byte("world"), vec.Entries[0].Data)

	_, err = pw.Write([]byte("!"))
	require.NoError(t, err)
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)
	require.Nil(t, cache.download(cache.pathForFile("obj")), "a finished download is forgotten")
}

// A read whose bytes the download never writes is not served from it.
func TestDiskCacheReadFallsBackWhenDownloadFails(t *testing.T) {
	rangeReadsCost(t, time.Hour)
	cache, pw, filled := downloading(t)
	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)

	pending := readRange(context.Background(), cache, 6, 5)
	require.NoError(t, pw.CloseWithError(errors.New("source failed")))
	vec := waitVector(t, pending)
	require.False(t, vec.Entries[0].done)
	require.Error(t, <-filled)
}

// A read waiting for a download stops when its context is done.
func TestDiskCacheReadOfDownloadStopsOnCancel(t *testing.T) {
	rangeReadsCost(t, time.Hour)
	cache, _, _ := downloading(t)
	ctx, cancel := context.WithCancel(context.Background())
	pending := readRange(ctx, cache, 6, 5)
	cancel()
	require.False(t, waitVector(t, pending).Entries[0].done)
}

// A download that ends with the object shorter than a read leaves the read unserved.
func TestDiskCacheReadBeyondDownloadedObject(t *testing.T) {
	rangeReadsCost(t, time.Hour)
	cache, pw, filled := downloading(t)
	pending := readRange(context.Background(), cache, 2, 10)
	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, pw.Close())
	require.False(t, waitVector(t, pending).Entries[0].done)
	require.NoError(t, <-filled)
}

// A reader holding the download's file reads the written bytes after the file is
// published, and after a failed download's file is removed.
func TestObjectDownloadReadableAfterPublishOrRemoval(t *testing.T) {
	for _, fail := range []bool{false, true} {
		cache, pw, filled := downloading(t)
		download := cache.download(cache.pathForFile("obj"))
		_, err := pw.Write([]byte("hello"))
		require.NoError(t, err)
		require.Eventually(t, func() bool { behind, _ := download.estimate(0, 5); return behind == 0 },
			diskCacheLifecycleTestTimeout, time.Millisecond)
		f, ok := download.acquire()
		require.True(t, ok)
		if fail {
			require.NoError(t, pw.CloseWithError(errors.New("source failed")))
			require.Error(t, <-filled)
		} else {
			require.NoError(t, pw.Close())
			require.NoError(t, <-filled)
		}
		b := make([]byte, 5)
		_, err = f.ReadAt(b, 0)
		require.NoError(t, err, "failed %v", fail)
		require.Equal(t, []byte("hello"), b)
		download.release()
		_, ok = download.acquire()
		require.False(t, ok, "the file is closed once the last reader is done")
	}
}

// A read that chose to wait for the download keeps waiting while its bytes keep coming,
// however late, instead of switching to a range read.
func TestDiskCacheReadKeepsWaitingForChosenDownload(t *testing.T) {
	rangeReadsCost(t, 20*time.Millisecond)
	cache, pw, filled := downloading(t)
	pending := readRange(context.Background(), cache, 12, 3)
	t0 := time.Now()
	for _, b := range []byte("0123456789abcdef") {
		time.Sleep(25 * time.Millisecond)
		_, err := pw.Write([]byte{b})
		require.NoError(t, err)
	}
	vec := waitVector(t, pending)
	require.Greater(t, time.Since(t0), 20*time.Millisecond+cache.downloadMaxExtraWait, "past the budget")
	require.True(t, vec.Entries[0].done)
	require.Equal(t, []byte("cde"), vec.Entries[0].Data)
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)
}

// A download whose source stops sending fails after the idle timeout, and a read that
// waited for it then reads its range.
func TestDiskCacheReadLeavesStalledDownload(t *testing.T) {
	rangeReadsCost(t, 20*time.Millisecond)
	idle0 := sourceIdleTimeout
	sourceIdleTimeout = 50 * time.Millisecond
	t.Cleanup(func() { sourceIdleTimeout = idle0 })
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close(ctx) })
	filled := make(chan error, 1)
	go func() {
		filled <- cache.SetFile(ctx, "obj", func(ctx context.Context) (io.ReadCloser, error) {
			return io.NopCloser(&stalledReader{ctx: ctx, data: []byte("0")}), nil
		})
	}()
	require.Eventually(t, func() bool { return cache.download(cache.pathForFile("obj")) != nil },
		diskCacheLifecycleTestTimeout, time.Millisecond)
	vec := waitVector(t, readRange(ctx, cache, 1, 3))
	require.False(t, vec.Entries[0].done)
	select {
	case err := <-filled:
		require.ErrorIs(t, err, errSourceIdle)
	case <-time.After(diskCacheLifecycleTestTimeout):
		t.Fatal("the stalled download did not fail")
	}
}

// stalledReader returns data, then blocks until ctx is done.
type stalledReader struct {
	ctx  context.Context
	data []byte
}

func (r *stalledReader) Read(b []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(b, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

// A read whose bytes the download writes later than a range read fetches them goes to a
// range read at once, and a read of bytes already written still reads the download.
func TestDiskCacheReadDeclinesDownloadFarBehind(t *testing.T) {
	rangeReadsCost(t, time.Nanosecond)
	downloadsRate(t, 1)
	cache, pw, filled := downloading(t)
	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)
	download := cache.download(cache.pathForFile("obj"))
	require.Eventually(t, func() bool { behind, _ := download.estimate(0, 5); return behind == 0 },
		diskCacheLifecycleTestTimeout, time.Millisecond)

	vec := &IOVector{FilePath: "obj", Entries: []IOEntry{{Offset: 1, Size: 3}, {Offset: 100 << 20, Size: 3}}}
	require.NoError(t, cache.Read(context.Background(), vec))
	require.True(t, vec.Entries[0].done, "bytes already written")
	require.Equal(t, []byte("ell"), vec.Entries[0].Data)
	require.False(t, vec.Entries[1].done)
	require.True(t, vec.downloadDeclined)

	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)
}

func TestDiskDownloadMaxExtraWaitConfig(t *testing.T) {
	config := func(extra *toml.Duration) CacheConfig {
		path := t.TempDir()
		capacity := toml.ByteSize(1 << 20)
		return CacheConfig{DiskPath: &path, DiskCapacity: &capacity, DiskDownloadMaxExtraWait: extra}
	}
	caches := newTestFileServiceCaches(t, config(nil))
	require.Equal(t, DefaultDiskDownloadMaxExtraWait, caches.disk.downloadMaxExtraWait)
	caches = newTestFileServiceCaches(t, config(&toml.Duration{Duration: 7 * time.Millisecond}))
	require.Equal(t, 7*time.Millisecond, caches.disk.downloadMaxExtraWait)
}

// A read whose bytes are most of what the download still writes before them waits for
// the download even without a margin, instead of fetching those bytes again.
func TestDiskCacheReadWaitsWhenItWouldDuplicateTheDownload(t *testing.T) {
	rangeReadsCost(t, time.Nanosecond)
	downloadsRate(t, 1)
	cache, pw, filled := downloading(t)
	cache.downloadMaxExtraWait = 0
	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)
	download := cache.download(cache.pathForFile("obj"))
	require.Eventually(t, func() bool { behind, _ := download.estimate(0, 5); return behind == 0 },
		diskCacheLifecycleTestTimeout, time.Millisecond)

	pending := readRange(context.Background(), cache, 5, 20)
	select {
	case <-pending:
		t.Fatal("a read most of the download's next bytes did not wait for them")
	case <-time.After(20 * time.Millisecond):
	}
	_, err = pw.Write([]byte("0123456789abcdefghij"))
	require.NoError(t, err)
	vec := waitVector(t, pending)
	require.True(t, vec.Entries[0].done)
	require.Equal(t, []byte("0123456789abcdefghij"), vec.Entries[0].Data)
	require.False(t, vec.downloadDeclined)
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)
}

// Bytes a read takes from a download count as read from the cached file once the
// download publishes it.
func TestDiskCacheDownloadReadsCountAsCachedFileReads(t *testing.T) {
	rangeReadsCost(t, time.Hour)
	cache, pw, filled := downloading(t)
	_, err := pw.Write([]byte("hello"))
	require.NoError(t, err)
	require.True(t, waitVector(t, readRange(context.Background(), cache, 1, 3)).Entries[0].done)
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)

	cache.preloads.mu.Lock()
	served, ok := cache.preloads.served[cache.pathForFile("obj")]
	cache.preloads.mu.Unlock()
	require.True(t, ok)
	require.Equal(t, int64(3), served)
}

// Caching content already in memory does not feed the download rate, while a fill from
// object storage does; both count toward the size of whole objects.
func TestDiskCacheContentFillDoesNotFeedDownloadRate(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(64<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	content := make([]byte, 4<<20)

	downloadsRate(t, 1)
	require.NoError(t, cache.setFileContent(ctx, "content", content, false))
	require.Equal(t, float64(1), downloadRates.rate())
	require.Equal(t, int64(len(content)), cache.wholeFileMax.Load())

	require.NoError(t, cache.SetFile(ctx, "download", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(content)), nil
	}))
	require.NotEqual(t, float64(1), downloadRates.rate())
}

// A range read is timed only when the file service drains it itself.
func TestS3FSRangeReadCostTimesOnlyDrainedReads(t *testing.T) {
	ctx := context.Background()
	rangeCosts(t, rangeReadCosts.get().latency, rangeReadCosts.get().bytesPerS)
	fs, err := NewS3FS(ctx, ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir()},
		DisabledCacheConfig, nil, false, false)
	require.NoError(t, err)
	defer fs.Close(ctx)
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: 1 << 10, Data: make([]byte, 1<<10)}}}))
	const sentinel = 1e6
	rangeReadCosts.set(sentinel, rangeReadCosts.get().bytesPerS)

	var streamed io.ReadCloser
	require.NoError(t, fs.Read(ctx, &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 1, Size: 100, ReadCloserForRead: &streamed}}}))
	_, err = io.ReadAll(streamed)
	require.NoError(t, err)
	require.NoError(t, streamed.Close())
	require.Equal(t, float64(sentinel), rangeReadCosts.get().latency, "a read streamed to the caller")

	require.NoError(t, fs.Read(ctx, &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: 1, Size: 100}}}))
	require.NotEqual(t, float64(sentinel), rangeReadCosts.get().latency, "a read the file service drains")
}

// A fill from memory is tracked as a download that readers can read, without measuring
// its rate.
func TestDiskCacheContentFillIsAnUnmeasuredDownload(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	pr, pw := io.Pipe()
	filled := make(chan error, 1)
	go func() {
		filled <- cache.fillFile(ctx, "obj", func(context.Context) (io.ReadCloser, error) { return pr, nil }, false, contentFill, nil)
	}()
	require.Eventually(t, func() bool { return cache.download(cache.pathForFile("obj")) != nil },
		diskCacheLifecycleTestTimeout, time.Millisecond)
	require.False(t, cache.download(cache.pathForFile("obj")).measured)
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)
}

// Bytes read from a download count toward its file's read share also when the file is
// evicted as soon as it is published.
func TestDiskCacheDownloadReadsCountWhenEvictedOnPublish(t *testing.T) {
	rangeReadsCost(t, time.Hour)
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1024), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	const group = 7
	diskPath := cache.pathForFile("obj")
	cache.preloads.narrowRead(diskPath, group, func() bool { return true })
	pr, pw := io.Pipe()
	filled := make(chan error, 1)
	go func() {
		filled <- cache.SetFile(ctx, "obj", func(context.Context) (io.ReadCloser, error) { return pr, nil })
	}()
	require.Eventually(t, func() bool { return cache.download(diskPath) != nil },
		diskCacheLifecycleTestTimeout, time.Millisecond)
	data := make([]byte, 4096)
	_, err = pw.Write(data)
	require.NoError(t, err)
	require.True(t, waitVector(t, readRange(ctx, cache, 0, 4096)).Entries[0].done)
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)

	require.False(t, cache.cache.Contains(diskPath), "a file larger than the cache is evicted")
	cache.preloads.mu.Lock()
	defer cache.preloads.mu.Unlock()
	require.NotNil(t, cache.preloads.groups[group])
	require.Greater(t, cache.preloads.groups[group].average, 0.0)
}

type blockingWriter struct {
	entered, release chan struct{}
}

func (w blockingWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}

// Bytes of a read still running when its download publishes the file, which is then
// evicted at once, count toward the file's read share.
func TestDiskCacheDownloadReadOverlappingPublicationCounts(t *testing.T) {
	rangeReadsCost(t, time.Hour)
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1024), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	const group = 7
	diskPath := cache.pathForFile("obj")
	cache.preloads.narrowRead(diskPath, group, func() bool { return true })
	pr, pw := io.Pipe()
	filled := make(chan error, 1)
	go func() {
		filled <- cache.SetFile(ctx, "obj", func(context.Context) (io.ReadCloser, error) { return pr, nil })
	}()
	require.Eventually(t, func() bool { return cache.download(diskPath) != nil },
		diskCacheLifecycleTestTimeout, time.Millisecond)
	_, err = pw.Write(make([]byte, 4096))
	require.NoError(t, err)

	w := blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	read := make(chan error, 1)
	go func() {
		read <- cache.Read(ctx, &IOVector{FilePath: "obj", Entries: []IOEntry{{Offset: 0, Size: 4096, WriterForRead: w}}})
	}()
	<-w.entered
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)
	require.False(t, cache.cache.Contains(diskPath), "a file larger than the cache is evicted")
	close(w.release)
	require.NoError(t, <-read)

	cache.preloads.mu.Lock()
	defer cache.preloads.mu.Unlock()
	require.NotNil(t, cache.preloads.groups[group])
	require.Greater(t, cache.preloads.groups[group].average, 0.0)
}

// Bytes the read that started a download captures count toward the file's read share also
// when the file is evicted as soon as it is published.
func TestDiskCacheDownloadCaptureCountsWhenEvictedOnPublish(t *testing.T) {
	rangeReadsCost(t, time.Hour)
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1024), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	const group = 7
	diskPath := cache.pathForFile("obj")
	cache.preloads.narrowRead(diskPath, group, func() bool { return true })
	pr, pw := io.Pipe()
	filled := make(chan error, 1)
	go func() {
		filled <- cache.SetFile(ctx, "obj", func(context.Context) (io.ReadCloser, error) { return pr, nil })
	}()
	require.Eventually(t, func() bool { return cache.download(diskPath) != nil },
		diskCacheLifecycleTestTimeout, time.Millisecond)
	_, err = pw.Write(make([]byte, 4096))
	require.NoError(t, err)
	read := newFullObjectDiskCacheReader(&IOVector{FilePath: "obj", Entries: []IOEntry{{Offset: 0, Size: 4096}}})
	require.True(t, read.capture(ctx, cache.download(diskPath)))
	require.NoError(t, pw.Close())
	require.NoError(t, <-filled)

	require.False(t, cache.cache.Contains(diskPath), "a file larger than the cache is evicted")
	cache.preloads.mu.Lock()
	defer cache.preloads.mu.Unlock()
	require.NotNil(t, cache.preloads.groups[group])
	require.Greater(t, cache.preloads.groups[group].average, 0.0)
}
