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
			require.Eventually(t, func() bool { return !fs.diskCache.fillsUnderWay(tc.path) }, time.Second, time.Millisecond)
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

			diskPath := d.objectKey("object")
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
				require.NoError(t, <-deleted, "the delete waits for no fill whose publication it prevents")
			}
			allowPublish()
			d.Flush(ctx)
			require.NoError(t, ctx.Err())
			if !tc.delete {
				require.FileExists(t, d.wholePath("object", 64<<10), "the staged whole object normally publishes")
				return
			}
			require.NoFileExists(t, d.wholePath("object", 64<<10), "the old whole object must not appear once its delete has been counted")
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

			diskPath := d.objectKey("object")
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
			require.Eventually(t, func() bool { return !fs.diskCache.fillsUnderWay("object") }, time.Second, time.Millisecond)
			if !tc.delete {
				require.FileExists(t, d.wholePath("object", 64<<10))
				return
			}
			require.NoFileExists(t, d.wholePath("object", 64<<10), "a delete counted during the publication of the whole object removes it")
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
