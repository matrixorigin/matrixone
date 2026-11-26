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
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestLargeReadPastDeclinedFilePrefetch(t *testing.T) {
	const size = 9 << 20
	for _, tc := range []struct {
		name   string
		policy Policy
	}{
		{name: "baseline_skip_cache_writes", policy: SkipDiskCacheWrites},
		{name: "cache_writes_enabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rangeCosts(t, 0.001, 100<<20)
			downloadsRate(t, 1024)

			fs, _, data := newPrefetchRangeFS(t, 0, 2*size)
			d := fs.diskCache
			d.memoryPrefetchMax = minDiskPrefetchMemoryMax
			d.downloadMaxExtraWait = 0
			d.objectSplit.parts = 1
			<-d.load.done

			var opens atomic.Int64
			storage := newBlackBoxStorage(fs.server.raw, func(string, int64) bool {
				return opens.Add(1) == 1
			})
			fs.server.raw = storage
			defer close(storage.gate)

			ctx := context.Background()
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
				FilePath: "object", Size: size, Ranges: []PrefetchRange{{Size: size}},
			}))
			require.Eventually(t, func() bool { return opens.Load() == 1 }, 5*time.Second, time.Millisecond)
			download, _ := d.fileDownload(d.objectKey("object"), 0, size)
			require.NotNil(t, download)
			_, eta := download.estimate(0, size)
			require.Greater(t, eta, rangeReadCosts.cost(size), "the original file prefetch is too slow to join")

			readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			vec := &IOVector{
				FilePath: "object", Policy: ExactColumnsRead | SkipMemoryCache | tc.policy,
				Entries: []IOEntry{{Size: size}},
			}
			defer vec.Release()
			err := fs.Read(readCtx, vec)
			require.NoError(t, err, "an exact read that declines the slow prefetch should open its own source; source opens=%d", opens.Load())
			require.True(t, bytes.Equal(data[:size], vec.Entries[0].Data), "the exact range should be returned")
			require.EqualValues(t, 2, opens.Load(), "the read should fall back to object storage")
		})
	}
}

func TestJoinedFileRangeWaitsForCaller(t *testing.T) {
	const (
		size = 192 << 10
		wait = 100 * time.Millisecond
	)
	for _, tc := range []struct {
		name    string
		ioBound time.Duration
	}{
		{name: "baseline_long_io_wait", ioBound: 5 * time.Second},
		{name: "short_io_wait", ioBound: wait},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalBound := maxIOWaitDuration
			maxIOWaitDuration = tc.ioBound
			defer func() { maxIOWaitDuration = originalBound }()
			fs, _, data := newPrefetchRangeFS(t, 0, 256<<10)
			d := fs.diskCache
			d.memoryPrefetchMax = 128 << 10
			d.downloadMaxExtraWait = 5 * time.Second
			d.objectSplit.parts = 1
			<-d.load.done
			storage := newBlackBoxStorage(fs.server.raw, func(_ string, start int64) bool { return start == 0 })
			fs.server.raw = storage
			unblock := sync.OnceFunc(func() { close(storage.gate) })
			defer unblock()
			key := fs.pathToKey("object")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
				FilePath: "object", Size: size, Ranges: []PrefetchRange{{Size: size}},
			}))
			require.Eventually(t, func() bool {
				cur, _, _ := storage.counts(key)
				return cur == 1
			}, time.Second, time.Millisecond)
			download, _ := d.fileDownload(d.objectKey("object"), 16, 80)
			require.NotNil(t, download)

			var dst bytes.Buffer
			vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache | SkipDiskCacheWrites,
				Entries: []IOEntry{{Offset: 16, Size: 64, WriterForRead: &dst}}}
			defer vec.Release()
			result := make(chan error, 1)
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				result <- fs.Read(ctx, vec)
			}()
			defer func() {
				unblock()
				<-readDone
			}()
			require.Eventually(t, func() bool {
				download.mu.Lock()
				defer download.mu.Unlock()
				return len(download.waiters) == 1
			}, time.Second, time.Millisecond, "the read joins the file-backed range")
			select {
			case err := <-result:
				_, _, total := storage.counts(key)
				t.Fatalf("read returned before its joined range sent bytes: err=%v, source opens=%d", err, total)
			case <-time.After(3 * wait):
			}
			unblock()
			require.NoError(t, <-result)
			require.Equal(t, data[16:80], dst.Bytes())
			_, _, total := storage.counts(key)
			require.Equal(t, 1, total, "the joined range alone supplies the bytes")
		})
	}
}

func TestConcurrentSplitAdmissionAtLimitOne(t *testing.T) {
	const size = 300 << 10
	for _, tc := range []struct {
		name             string
		firstAlreadyOpen bool
	}{
		{name: "baseline_first_download_registered", firstAlreadyOpen: true},
		{name: "both_admitted_before_registration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10, downloads: 1}, size)
			d := fs.diskCache
			<-d.load.done
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: "other", Policy: SkipAllCache,
				Entries: []IOEntry{{Size: size, Data: data}}}))
			d.prefetched.store(d.objectKey("other"), size)

			staging := make(chan chan struct{}, 2)
			cancelStaging := make(chan struct{})
			d.hooks.beforeCacheTempFileCreate = func() {
				gate := make(chan struct{})
				staging <- gate
				select {
				case <-gate:
				case <-cancelStaging:
				}
			}
			storage := newBlackBoxStorage(fs.server.raw, func(key string, _ int64) bool {
				return key == fs.pathToKey("object")
			})
			fs.server.raw = storage
			allowSource := sync.OnceFunc(func() { close(storage.gate) })
			var readers sync.WaitGroup
			defer func() {
				close(cancelStaging)
				allowSource()
				readers.Wait()
			}()
			read := func(file string, result chan<- error) {
				readers.Go(func() { result <- fs.PrefetchFile(ctx, file) })
			}
			waitForStage := func() chan struct{} {
				t.Helper()
				select {
				case gate := <-staging:
					return gate
				case <-ctx.Done():
					t.Fatal("download did not reach staging", ctx.Err())
					return nil
				}
			}
			waitForFirstOpens := func() {
				t.Helper()
				require.Eventually(t, func() bool {
					cur, _, _ := storage.counts(fs.pathToKey("object"))
					return cur == 2
				}, time.Second, time.Millisecond, "the first fill's split parts should still be in flight")
			}

			first, second := make(chan error, 1), make(chan error, 1)
			read("object", first)
			firstStage := waitForStage()
			if tc.firstAlreadyOpen {
				close(firstStage)
				waitForFirstOpens()
			}
			read("other", second)
			secondStage := waitForStage()
			if !tc.firstAlreadyOpen {
				close(firstStage)
				waitForFirstOpens()
			}
			close(secondStage)
			require.NoError(t, <-second)
			allowSource()
			require.NoError(t, <-first)
			_, _, firstOpens := storage.counts(fs.pathToKey("object"))
			_, _, secondOpens := storage.counts(fs.pathToKey("other"))
			require.Equal(t, 3, firstOpens+secondOpens,
				"with a split-download limit of one, only one fill should split; source opens by fill: %d and %d", firstOpens, secondOpens)
		})
	}
}

func TestUnwritableIndexCacheKeepsTableCacheSeparate(t *testing.T) {
	const size = 9 << 20
	for _, tc := range []struct {
		name     string
		readOnly bool
	}{
		{name: "baseline_writable_index_cache"},
		{name: "unwritable_index_cache", readOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.readOnly && !permissionsEnforced(t) {
				t.Skip("filesystem permissions are not enforced")
			}
			ctx := context.Background()
			config := indexCacheConfig(t, 64<<20, 64<<20)
			config.MemoryCapacity = ptrTo[toml.ByteSize](0)
			config.DiskPrefetchMemoryMax = ptrTo[toml.ByteSize](minDiskPrefetchMemoryMax)
			fs, err := NewS3FS(ctx, ObjectStorageArguments{
				Name: defines.SharedFileServiceName, Endpoint: "disk", Bucket: t.TempDir(),
			}, config, nil, false, false)
			require.NoError(t, err)
			defer fs.Close(ctx)
			fs.SetAsyncUpdate(false)
			<-fs.diskCache.load.done
			<-fs.indexFileCache.disk.load.done

			data := bytes.Repeat([]byte{0x59}, size)
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Policy: SkipAllCache,
				Entries: []IOEntry{{Size: size, Data: data}}}))
			if tc.readOnly {
				path := fs.indexFileCache.disk.path
				require.NoError(t, os.Chmod(path, 0o500))
				defer func() { require.NoError(t, os.Chmod(path, 0o755)) }()
			}
			vec := &IOVector{FilePath: "object", Policy: IndexFileObject | SkipMemoryCache,
				Entries: []IOEntry{{Size: size}}}
			defer vec.Release()
			require.NoError(t, fs.Read(ctx, vec))
			require.True(t, bytes.Equal(data, vec.Entries[0].Data), "the index read should return the whole object")
			fs.diskCache.prefetchObjects.reads.Wait()
			require.Zero(t, fs.diskCache.cache.Used(), "an index-object read should take no space in the table-data cache")
		})
	}
}

type reviewFailOnceWriter struct {
	err   error
	calls int
}

func (w *reviewFailOnceWriter) Write(data []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return 0, w.err
	}
	return len(data), nil
}

func TestSplitCacheReadReturnsWriterError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy Policy
	}{
		{name: "baseline_source_read", policy: SkipDiskCache},
		{name: "read_across_two_cached_ranges"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const part = 64 << 10
			fs, storage, data := newPrefetchRangeFS(t, 0, 4*part)
			d := fs.diskCache
			<-d.load.done
			ctx := context.Background()
			require.NoError(t, d.Update(ctx, &IOVector{FilePath: "object", Entries: []IOEntry{
				{Size: part, Data: data[:part]},
				{Offset: part, Size: part, Data: data[part : 2*part]},
			}}, false))
			const offset, size = part - 16, 32
			rs, ok := d.ranges.covering(d.objectKey("object"), offset, size)
			require.True(t, ok)
			require.Len(t, rs, 2)
			wantErr := errors.New("review: caller's writer rejected output")
			writer := &reviewFailOnceWriter{err: wantErr}
			vec := &IOVector{FilePath: "object", Policy: ExactColumnsRead | SkipMemoryCache | tc.policy,
				Entries: []IOEntry{{Offset: offset, Size: size, WriterForRead: writer}}}
			defer vec.Release()
			err := fs.Read(ctx, vec)
			require.ErrorIs(t, err, wantErr, "a caller writer failure should be returned; writer calls=%d, source opens=%d", writer.calls, storage.reads.Load())
			require.Equal(t, 1, writer.calls)
		})
	}
}
