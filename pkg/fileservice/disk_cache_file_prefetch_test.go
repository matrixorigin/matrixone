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

	"github.com/stretchr/testify/require"
)

func filePrefetchOpener(fs *S3FS, file string) rangeOpener {
	key := fs.pathToKey(file)
	return func(ctx context.Context, start, end int64) (io.ReadCloser, error) {
		return fs.storage.Read(ctx, key, &start, &end)
	}
}

// A read of bytes a FilePrefetchObject has written is served from its download, while
// the download still runs.
func TestFilePrefetchServesReadsWhileItDownloads(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 256<<10)
	storage := &stallingStorage{ObjectStorage: fs.storage, after: 64 << 10, stalled: make(chan struct{}), release: make(chan struct{})}
	fs.storage = storage
	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		_, err := fs.diskCache.prefetchFile(ctx, "object", 1000, 200<<10, filePrefetchOpener(fs, "object"))
		done <- err
	}()
	<-storage.stalled

	require.Equal(t, data[1100:1200], readRangeOf(t, fs, ColumnPrefetch, 1100, 100))
	require.Equal(t, int64(1), storage.reads.Load(), "the read is served from the download")
	close(storage.release)
	require.NoError(t, <-done)
	_, cached := fs.diskCache.ranges.covering(fs.diskCache.rangeObjectKey("object"), 1000, 200<<10-1000)
	require.True(t, cached)
}

// A delete of an object cancels its FilePrefetchObject under way, which caches nothing: a
// read after the object is recreated returns the new object.
func TestDeleteCancelsFilePrefetch(t *testing.T) {
	fs, _, data := newPrefetchRangeFS(t, 0, 256<<10)
	storage := &stallingStorage{ObjectStorage: fs.storage, after: 4096, stalled: make(chan struct{}), release: make(chan struct{})}
	fs.storage = storage
	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		_, err := fs.diskCache.prefetchFile(ctx, "object", 0, 200<<10, filePrefetchOpener(fs, "object"))
		done <- err
	}()
	<-storage.stalled

	require.NoError(t, fs.Delete(ctx, "object"))
	require.True(t, storage.canceled.Load(), "the delete cancels the download")
	close(storage.release)
	<-done
	recreated := slices.Clone(data)
	for i := range recreated {
		recreated[i] ^= 0xff
	}
	require.NoError(t, fs.Write(ctx, IOVector{FilePath: "object", Entries: []IOEntry{{Size: int64(len(recreated)), Data: recreated}}, Policy: SkipAllCache}))
	_, cached := fs.diskCache.ranges.covering(fs.diskCache.rangeObjectKey("object"), 0, 200<<10)
	require.False(t, cached)
	require.Equal(t, recreated[100:110], readRangeOf(t, fs, ColumnPrefetch, 100, 10))
}
