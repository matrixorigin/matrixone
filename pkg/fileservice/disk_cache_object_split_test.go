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
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

type failingRangeStorage struct {
	ObjectStorage
	fail atomic.Bool
}

func (f *failingRangeStorage) Read(ctx context.Context, key string, min, max *int64) (io.ReadCloser, error) {
	if f.fail.Load() && min != nil && *min > 0 {
		return nil, errors.New("injected range read failure")
	}
	return f.ObjectStorage.Read(ctx, key, min, max)
}

type statCountingStorage struct {
	ObjectStorage
	stats atomic.Int64
}

func (s *statCountingStorage) Stat(ctx context.Context, key string) (int64, error) {
	s.stats.Add(1)
	return s.ObjectStorage.Stat(ctx, key)
}

func newObjectSplitFS(t *testing.T, split objectSplitConfig, size int) (*S3FS, *countingStorage, []byte) {
	fs, storage, data := newPrefetchRangeFS(t, 0, false, size)
	fs.diskCache.objectSplit = split
	return fs, storage, data
}

func readFullObjectOf(fs *S3FS, offset, size int64) ([]byte, error) {
	vec := &IOVector{FilePath: "object", Policy: SkipMemoryCache, Entries: []IOEntry{{Offset: offset, Size: size}}}
	err := fs.Read(context.Background(), vec)
	return vec.Entries[0].Data, err
}

// The download of an object of at least the split size reads its 64KiB-aligned halves
// at once and caches the whole object.
func TestS3FSObjectSplitDownloadReadsParts(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10+17)
	got, err := readFullObjectOf(fs, 250<<10, 10)
	require.NoError(t, err)
	require.Equal(t, data[250<<10:250<<10+10], got)
	require.Equal(t, int64(2), storage.reads.Load())
	require.ElementsMatch(t, []int64{0, 192 << 10}, storage.starts)
	require.True(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))

	got, err = readFullObjectOf(fs, 0, int64(len(data)))
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.Equal(t, int64(2), storage.reads.Load(), "the cached whole object serves later reads")
}

// An object smaller than the split size downloads as one range.
func TestS3FSObjectSplitSkipsSmallObjects(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 1 << 20}, 300<<10)
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Empty(t, storage.starts, "an open-ended read of the object")
}

// With the most downloads in flight that a split allows, a large object downloads as one
// range.
func TestS3FSObjectSplitSkipsSharedLink(t *testing.T) {
	fs, storage, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	d := fs.diskCache
	d.downloads.mu.Lock()
	d.downloads.m = make(map[string]*objectDownload)
	for i := 0; i < objectSplitMaxDownloads; i++ {
		d.downloads.m[fmt.Sprintf("other-%d", i)] = newObjectDownload("", false, nil)
	}
	d.downloads.mu.Unlock()
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Equal(t, int64(1), storage.reads.Load())
	require.Empty(t, storage.starts, "an open-ended read of the object")
}

// A download that is not split never asks object storage for the object's size.
func TestS3FSObjectSplitOffMakesNoStat(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 1, min: 0}, 300<<10)
	storage := &statCountingStorage{ObjectStorage: fs.storage}
	fs.storage = storage
	got, err := readFullObjectOf(fs, 100, 10)
	require.NoError(t, err)
	require.Equal(t, data[100:110], got)
	require.Zero(t, storage.stats.Load())
}

// A failed part fails the download, which publishes no whole object.
func TestS3FSObjectSplitFailedPartPublishesNothing(t *testing.T) {
	fs, _, data := newObjectSplitFS(t, objectSplitConfig{parts: 2, min: 128 << 10}, 300<<10)
	failing := &failingRangeStorage{ObjectStorage: fs.storage}
	failing.fail.Store(true)
	fs.storage = failing
	_, err := readFullObjectOf(fs, 10, 10)
	require.Error(t, err)
	require.False(t, fs.diskCache.cache.Contains(fs.diskCache.pathForFile("object")))

	failing.fail.Store(false)
	got, err := readFullObjectOf(fs, 10, 10)
	require.NoError(t, err)
	require.Equal(t, data[10:20], got)
}

// A reader of a part waits only for that part.
func TestObjectDownloadWaitsForItsPart(t *testing.T) {
	ctx := context.Background()
	o := newObjectDownload("", false, []int64{0, 4, 8})
	_, wait := o.estimate(0, 1)
	require.GreaterOrEqual(t, wait, rangeReadCosts.firstByte(), "a part not yet read is a latency away")
	o.advance(1, 4)
	written, ok := o.waitFor(ctx, 4, 8, time.Millisecond)
	require.True(t, ok)
	require.Equal(t, int64(8), written)
	behind, wait := o.estimate(4, 8)
	require.Zero(t, behind)
	require.Zero(t, wait)

	_, ok = o.waitFor(ctx, 0, 1, time.Millisecond)
	require.False(t, ok, "the first part has no byte")
	behind, _ = o.estimate(0, 8)
	require.Equal(t, int64(4), behind)

	o.advance(0, 2)
	written, ok = o.waitFor(ctx, 0, 2, time.Millisecond)
	require.True(t, ok)
	require.Equal(t, int64(2), written, "the bytes written from the start stop at the gap")
	o.advance(0, 2)
	o.end(true)
	written, ok = o.waitFor(ctx, 0, -1, time.Millisecond)
	require.True(t, ok)
	require.Equal(t, int64(8), written)
}

func TestObjectSplitBounds(t *testing.T) {
	split := objectSplitConfig{parts: 2, min: 1 << 20}
	require.Nil(t, split.bounds(1<<20-1))
	require.Equal(t, []int64{0, 704 << 10, 1<<20 + 300<<10}, split.bounds(1<<20+300<<10))
	require.Equal(t, []int64{0, 384 << 10, 768 << 10, 1152 << 10, 1<<20 + 300<<10},
		objectSplitConfig{parts: 4, min: 1 << 20}.bounds(1<<20+300<<10))
	require.Nil(t, objectSplitConfig{parts: 1, min: 0}.bounds(64<<20))
}

func TestDiskObjectSplitConfig(t *testing.T) {
	require.Equal(t, objectSplitConfig{parts: 2, min: 16 << 20}, (&CacheConfig{}).diskObjectSplit())
	require.Equal(t, objectSplitConfig{parts: 1, min: 16 << 20}, (&CacheConfig{DiskPrefetchObjectSplit: ptrTo(0)}).diskObjectSplit())
	require.Equal(t, objectSplitConfig{parts: 4, min: 1 << 20}, (&CacheConfig{
		DiskPrefetchObjectSplit:    ptrTo(9),
		DiskPrefetchObjectSplitMin: ptrTo[toml.ByteSize](1 << 20),
	}).diskObjectSplit())
}
