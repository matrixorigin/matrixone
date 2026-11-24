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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestJoinedColumnReadReleasesBufferOnDecoderPanic(t *testing.T) {
	for _, tc := range []struct {
		name   string
		panics bool
	}{
		{name: "decoder_returns_error"},
		{name: "decoder_panics", panics: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, _, _ := newPrefetchRangeFS(t, 0, 4096)
			<-fs.diskCache.load.done
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			publishing := make(chan struct{})
			resume := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			fs.diskCache.hooks.beforeFilePublication = func() {
				close(publishing)
				<-resume
			}
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
				FilePath: "object", Size: 128, Ranges: []PrefetchRange{{Size: 128}},
			}))
			select {
			case <-publishing:
			case <-ctx.Done():
				t.Fatal("prefetch did not reach publication")
			}
			decoderError := errors.New("decoder rejected input")
			vector := &IOVector{
				FilePath: "object",
				Policy:   ColumnPrefetch | SkipFullFilePreloads | SkipMemoryCache,
				Entries: []IOEntry{{Offset: 8, Size: 8, ToCacheData: func(context.Context, io.Reader, []byte, CacheDataAllocator) (fscache.Data, error) {
					if tc.panics {
						panic(decoderError)
					}
					return nil, decoderError
				}}},
			}
			defer vector.Release()
			if tc.panics {
				require.PanicsWithValue(t, decoderError, func() { _ = fs.Read(ctx, vector) })
			} else {
				require.ErrorIs(t, fs.Read(ctx, vector), decoderError)
			}
			unblock()
			fs.prefetcher.wg.Wait()
			fs.diskCache.prefetchObjects.reads.Wait()
			flights := &fs.diskCache.prefetchObjects
			flights.mu.Lock()
			charged := flights.bytes
			flights.mu.Unlock()
			require.Zero(t, charged, "a finished prefetch must return its buffer charge after the reading decoder exits")
		})
	}
}

func TestReadStartedSplitHonorsConfiguredPartCount(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset int64
	}{
		{name: "read_starts_at_object_start"},
		{name: "read_starts_at_interior_block", offset: 64 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, storage, data := newPrefetchRangeFS(t, 0, 256<<10)
			<-fs.diskCache.load.done
			fs.diskCache.objectSplit = (&CacheConfig{
				DiskPrefetchObjectSplit:    ptrTo(4),
				DiskPrefetchObjectSplitMin: ptrTo[toml.ByteSize](0),
			}).diskObjectSplit()
			fs.diskCache.storePrefetchedSize(fs.diskCache.pathForFile("object"), int64(len(data)), fs.diskCache.deletes.now())
			vector := &IOVector{FilePath: "object", Entries: []IOEntry{{Offset: tc.offset, Size: 16}}}
			defer vector.Release()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, fs.Read(ctx, vector))
			require.Equal(t, data[tc.offset:tc.offset+16], vector.Entries[0].Data)
			require.EqualValues(t, 4, storage.reads.Load(), "the known-size object fits four aligned parts, including a boundary at the read's first needed block")
		})
	}
}

func TestFileRangeReadUsesCachePublishedWhileAdmitted(t *testing.T) {
	for _, tc := range []struct {
		name    string
		overlap bool
	}{
		{name: "read_after_publication"},
		{name: "read_admitted_before_publication", overlap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const size = minDiskPrefetchMemoryMax + 64
			fs, storage, data := newPrefetchRangeFS(t, 0, size)
			fs.diskCache.memoryPrefetchMax = minDiskPrefetchMemoryMax
			<-fs.diskCache.load.done
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			creating := make(chan struct{})
			proceed := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(proceed) })
			defer unblock()
			fs.diskCache.hooks.beforeCacheTempFileCreate = sync.OnceFunc(func() {
				close(creating)
				<-proceed
			})
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
				FilePath: "object", Size: size, Ranges: []PrefetchRange{{Size: size}},
			}))
			select {
			case <-creating:
			case <-ctx.Done():
				t.Fatal("background range did not acquire the cache path")
			}
			if !tc.overlap {
				unblock()
				fs.prefetcher.wg.Wait()
			}
			vector := &IOVector{FilePath: "object", Policy: ExactColumnsRead | SkipMemoryCache, Entries: []IOEntry{{Size: size}}}
			readDone := make(chan struct{})
			var readErr error
			go func() {
				defer close(readDone)
				readErr = fs.Read(ctx, vector)
			}()
			defer func() { cancel(); unblock(); <-readDone; vector.Release() }()
			if tc.overlap {
				path := fs.diskCache.pathForFile("object")
				objects := &fs.diskCache.prefetchObjects
				require.Eventually(t, func() bool {
					objects.mu.Lock()
					defer objects.mu.Unlock()
					for _, object := range objects.m[path] {
						file, ok := object.(*filePrefetchObject)
						if !ok {
							continue
						}
						file.mu.Lock()
						foregroundAttached := file.readStarted || file.holds > 1
						file.mu.Unlock()
						if foregroundAttached {
							return true
						}
					}
					return false
				}, 3*time.Second, time.Millisecond, "foreground read should reach range admission while the background range owns the path")
				unblock()
			}
			select {
			case <-readDone:
				require.NoError(t, readErr)
			case <-ctx.Done():
				t.Fatal("foreground read did not finish")
			}
			require.True(t, bytes.Equal(data, vector.Entries[0].Data))
			require.EqualValues(t, 1, storage.reads.Load(), "publication of the same range should avoid a second storage read")
		})
	}
}
