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
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeleteInvalidatesPrefetchedRangeForNestedPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "baseline_flat_object_path", path: "flat-object"},
		{name: "nested_object_path", path: "prefix/nested-object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const size = 4 << 10
			fs, storage, _ := newPrefetchRangeFS(t, 0, size)
			<-fs.diskCache.load.done
			ctx := context.Background()
			write := func(value byte) {
				t.Helper()
				require.NoError(t, fs.Write(ctx, IOVector{FilePath: tc.path, Policy: SkipAllCache,
					Entries: []IOEntry{{Size: size, Data: bytes.Repeat([]byte{value}, size)}}}))
			}
			read := func() byte {
				t.Helper()
				vec := &IOVector{FilePath: tc.path, Policy: ExactColumnsRead | SkipMemoryCache | SkipDiskCacheWrites,
					Entries: []IOEntry{{Size: size}}}
				defer vec.Release()
				require.NoError(t, fs.Read(ctx, vec))
				require.Len(t, vec.Entries[0].Data, size)
				return vec.Entries[0].Data[0]
			}

			write(1)
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
				FilePath: tc.path, Size: size, Ranges: []PrefetchRange{{Size: size}},
			}))
			require.Eventually(t, func() bool { return !fs.prefetcher.isRunning(tc.path) }, time.Second, time.Millisecond)
			require.EqualValues(t, 1, storage.reads.Load(), "the prefetch should read the former object once")
			require.Equal(t, byte(1), read())
			require.EqualValues(t, 1, storage.reads.Load(), "the former data should be read from the prefetched cache range")

			require.NoError(t, fs.Delete(ctx, tc.path))
			write(2)
			require.Equal(t, byte(2), read(), "a read after delete and recreate should return the new object")
		})
	}
}

func TestAsyncWholeFillDoesNotPublishAfterDeleteMark(t *testing.T) {
	for _, tc := range []struct {
		name   string
		delete bool
	}{
		{name: "baseline_no_delete"},
		{name: "delete_marks_object_before_publication", delete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, _ := newPrefetchRangeFS(t, 0, 64<<10)
			d := fs.diskCache
			<-d.load.done
			fs.SetAsyncUpdate(true)
			reached, release := make(chan struct{}), make(chan struct{})
			allowPublish := sync.OnceFunc(func() { close(release) })
			defer allowPublish()
			d.hooks.beforeFilePublication = func() {
				close(reached)
				<-release
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 64 << 10}))
			select {
			case <-reached:
			case <-ctx.Done():
				t.Fatal("whole-object fill did not reach the publication gate", ctx.Err())
			}

			diskPath := d.pathForFile("object")
			if tc.delete {
				mark := d.deletes.now()
				deleteCtx, cancelDelete := context.WithCancel(ctx)
				defer cancelDelete()
				deleted := make(chan error, 1)
				go func() { deleted <- fs.Delete(deleteCtx, "object") }()
				require.Eventually(t, func() bool {
					return d.deletes.since(diskPath, mark)
				}, time.Second, time.Millisecond, "the source delete completed and the cache delete was counted")
				_, err := fs.StatFile(ctx, "object")
				require.Error(t, err, "object storage no longer holds the object")
				cancelDelete()
				require.ErrorIs(t, <-deleted, context.Canceled, "the caller stops waiting for the cache path owner")
			}
			allowPublish()
			d.Flush(ctx)
			require.NoError(t, ctx.Err())
			if !tc.delete {
				require.FileExists(t, diskPath, "the staged whole object normally publishes")
				return
			}
			require.NoFileExists(t, diskPath, "the old whole object must not appear once its delete has been counted")
		})
	}
}

func TestMemoryPrefetchCallbackPanicIsContained(t *testing.T) {
	const childCase = "MATRIXONE_REVIEW_PREFETCH_CALLBACK_CHILD"
	if mode := os.Getenv(childCase); mode != "" {
		const unit = 64 << 10
		fs, _, data := newPrefetchRangeFS(t, unit, 2*unit)
		<-fs.diskCache.load.done
		fs.SetAsyncUpdate(true)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		wantOffset := int64(unit + 16)
		wantSize := int64(64)
		callbackAfterPanic := make(chan struct{})
		ctx = OnDiskCacheWritten(ctx, func(string, IOEntry) {
			panic("review: disk cache callback panicked")
		})
		ctx = OnDiskCacheWritten(ctx, func(string, IOEntry) { close(callbackAfterPanic) })
		policy := Policy(SkipFullFilePreloads | SkipMemoryCache)
		if mode == "baseline_exact_entry" {
			policy |= ExactRead
		}
		vec := &IOVector{FilePath: "object", Policy: policy,
			Entries: []IOEntry{{Offset: wantOffset, Size: wantSize}}}
		defer vec.Release()
		require.NoError(t, fs.Read(ctx, vec))
		require.Equal(t, data[wantOffset:wantOffset+wantSize], vec.Entries[0].Data)
		select {
		case <-callbackAfterPanic:
		case <-ctx.Done():
			t.Fatal("the callback after the panic was never called")
		}
		return
	}
	for _, mode := range []string{"baseline_exact_entry", "aligned_memory_prefetch"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMemoryPrefetchCallbackPanicIsContained$",
				"-test.cpu="+strconv.Itoa(runtime.GOMAXPROCS(0)), "-test.count=1")
			cmd.Env = append(os.Environ(), childCase+"="+mode)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("the callback panic escaped the fileservice background worker: %v\n%s", err, output)
			}
		})
	}
}

func TestSyncWholeFillDoesNotPublishAfterDeleteMark(t *testing.T) {
	for _, tc := range []struct {
		name   string
		delete bool
	}{
		{name: "baseline_no_delete"},
		{name: "delete_during_publication", delete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, _ := newPrefetchRangeFS(t, 0, 64<<10)
			d := fs.diskCache
			<-d.load.done
			reached, release := make(chan struct{}), make(chan struct{})
			allowPublish := sync.OnceFunc(func() { close(release) })
			defer allowPublish()
			d.hooks.afterPublicationClaim = func() {
				close(reached)
				<-release
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: 64 << 10}))
			select {
			case <-reached:
			case <-ctx.Done():
				t.Fatal("whole-object fill did not reach the publication claim", ctx.Err())
			}

			diskPath := d.pathForFile("object")
			deleted := make(chan error, 1)
			if tc.delete {
				mark := d.deletes.now()
				deleteCtx, cancelDelete := context.WithCancel(ctx)
				defer cancelDelete()
				go func() { deleted <- fs.Delete(deleteCtx, "object") }()
				counted := false
				for i := 0; i < 100 && !counted; i++ {
					time.Sleep(time.Millisecond)
					counted = d.deletes.since(diskPath, mark)
				}
				if counted {
					cancelDelete()
					require.ErrorIs(t, <-deleted, context.Canceled)
					deleted <- nil
				}
			}
			allowPublish()
			if tc.delete {
				require.NoError(t, <-deleted)
			}
			require.Eventually(t, func() bool { return !fs.prefetcher.isRunning("object") }, time.Second, time.Millisecond)
			if !tc.delete {
				require.FileExists(t, diskPath)
				return
			}
			require.NoFileExists(t, diskPath, "a delete counted during the publication of the whole object removes it")
		})
	}
}

func TestDeleteInvalidatesCachedEntryForNestedPath(t *testing.T) {
	for _, path := range []string{"flat-object", "prefix/nested-object"} {
		t.Run(path, func(t *testing.T) {
			const size = 4 << 10
			fs, storage, _ := newPrefetchRangeFS(t, 0, size)
			<-fs.diskCache.load.done
			ctx := context.Background()
			write := func(value byte) {
				t.Helper()
				require.NoError(t, fs.Write(ctx, IOVector{FilePath: path, Policy: SkipAllCache,
					Entries: []IOEntry{{Size: size, Data: bytes.Repeat([]byte{value}, size)}}}))
			}
			read := func() byte {
				t.Helper()
				vec := &IOVector{FilePath: path, Policy: SkipMemoryCache | SkipFullFilePreloads,
					Entries: []IOEntry{{Offset: 16, Size: 64}}}
				defer vec.Release()
				require.NoError(t, fs.Read(ctx, vec))
				return vec.Entries[0].Data[0]
			}

			write(1)
			require.Equal(t, byte(1), read())
			reads := storage.reads.Load()
			require.Equal(t, byte(1), read())
			require.Equal(t, reads, storage.reads.Load(), "the entry is read from the disk cache")

			require.NoError(t, fs.Delete(ctx, path))
			write(2)
			require.Equal(t, byte(2), read())
		})
	}
}
