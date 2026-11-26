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
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/require"
)

func TestNegativeConcurrencyIsRejected(t *testing.T) {
	noProbe := func(context.Context, string) (string, error) { return "", nil }
	for _, tc := range []struct {
		concurrency int64
		ok          bool
	}{{0, true}, {1, true}, {minRequestRoom - 1, true}, {minRequestRoom, true}, {1024, true}, {-1, false}} {
		args := ObjectStorageArguments{Endpoint: "disk", Bucket: t.TempDir(), Concurrency: tc.concurrency}
		err := args.validateWithRegionProbe(noProbe)
		if tc.ok {
			require.NoError(t, err, "concurrency=%d", tc.concurrency)
		} else {
			require.ErrorContains(t, err, fmt.Sprintf("concurrency must not be negative, got %d", tc.concurrency))
		}
		fs, err := NewS3FS(context.Background(), ObjectStorageArguments{Name: "s3", Endpoint: "disk", Bucket: t.TempDir(), Concurrency: tc.concurrency},
			CacheConfig{MemoryCapacity: ptrTo[toml.ByteSize](0)}, nil, false, true)
		if tc.ok {
			require.NoError(t, err, "concurrency=%d", tc.concurrency)
			fs.Close(context.Background())
		} else {
			require.ErrorContains(t, err, "concurrency must not be negative")
		}
	}
}

func TestDownloadConnsAreHalfTheStorageConcurrency(t *testing.T) {
	for _, tc := range []struct {
		conns       int
		concurrency int64
		want        int
	}{
		{0, 0, maxConnsPerHost / 2},
		{1, 0, 32},
		{0, 1, 32},
		{2, 3, 32},
		{64, 0, 32},
		{256, 0, 128},
		{0, 64, 32},
		{64, 64, 32},
		{256, 100, 50},
		{64, 1 << 20, 32},
		{4096, 0, 512},
	} {
		ctx := context.Background()
		fs, err := NewS3FS(ctx, ObjectStorageArguments{
			Name: "s3", Endpoint: "disk", Bucket: t.TempDir(),
			Concurrency: tc.concurrency, MaxConnsPerHost: tc.conns,
		}, CacheConfig{DiskPath: ptrTo(t.TempDir()), DiskCapacity: ptrTo[toml.ByteSize](64 << 20), MemoryCapacity: ptrTo[toml.ByteSize](0)}, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, tc.want, fs.governor.backgroundMax, "conns %d concurrency %d", tc.conns, tc.concurrency)
		require.Equal(t, 2*tc.want, fs.governor.capacity, "conns %d concurrency %d", tc.conns, tc.concurrency)
		fs.Close(ctx)
	}
}

func newStorageSlotFS(t *testing.T, concurrency int64, conns int, paths []string, data []byte, stalled func(string) bool) (*S3FS, *blackBoxStorage) {
	ctx := context.Background()
	config := indexCacheConfig(t, 64<<20, 64<<20)
	config.IndexFileCacheEnabled = false
	config.MemoryCapacity = DisabledCacheConfig.MemoryCapacity
	fs, err := NewS3FS(ctx, ObjectStorageArguments{
		Name: "s3", Endpoint: "disk", Bucket: t.TempDir(),
		Concurrency: concurrency, MaxConnsPerHost: conns,
	}, config, nil, false, false)
	require.NoError(t, err)
	<-fs.diskCache.load.done
	for _, path := range paths {
		require.NoError(t, fs.Write(ctx, IOVector{
			FilePath: path, Policy: SkipAllCache,
			Entries: []IOEntry{{Size: int64(len(data)), Data: data}},
		}))
	}
	// Stall prefetch opens after the governor admits them.
	gated := newBlackBoxStorage(fs.governor.raw, func(key string, _ int64) bool { return stalled(key) })
	fs.governor.raw = gated
	return fs, gated
}

func requireForegroundReads(t *testing.T, fs *S3FS, gated *blackBoxStorage, data []byte, policies ...Policy) {
	for _, policy := range policies {
		readCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		vec := &IOVector{FilePath: "foreground", Policy: policy, Entries: []IOEntry{{Offset: 64, Size: 16}}}
		readErr := fs.Read(readCtx, vec)
		_, _, opens := gated.counts("foreground")
		require.NoError(t, readErr, "policy %v, foreground opens: %d", policy, opens)
		require.Equal(t, data[64:80], vec.Entries[0].Data)
		vec.Release()
		cancel()
	}
}

// Two stalled whole-object prefetches leave storage slots and download connections for
// foreground reads, cache-filling ones included.
func TestPrefetchLeavesStorageSlotForForegroundRead(t *testing.T) {
	for _, tc := range []struct {
		name        string
		concurrency int64
	}{
		{name: "default_storage_concurrency"},
		{name: "storage_concurrency_64", concurrency: minRequestRoom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				data := bytes.Repeat([]byte("data"), 64<<10)
				fs, gated := newStorageSlotFS(t, tc.concurrency, 64, []string{"prefetch-a", "prefetch-b", "foreground"}, data,
					func(key string) bool { return key == "prefetch-a" || key == "prefetch-b" })
				defer fs.Close(ctx)
				defer close(gated.gate)
				for _, path := range []string{"prefetch-a", "prefetch-b"} {
					require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: path, Size: int64(len(data))}))
				}
				synctest.Wait()
				first, _, _ := gated.counts("prefetch-a")
				second, _, _ := gated.counts("prefetch-b")
				require.Positive(t, first+second, "at least one prefetch must still be in flight")
				requireForegroundReads(t, fs, gated, data, SkipAllCache|ExactColumnsRead, SkipMemoryCache)
			})
		})
	}
}

// Stalled column prefetches holding every connection downloads may hold leave storage
// slots for a foreground read when the storage concurrency is below the connections per host.
func TestStalledDownloadsLeaveStorageConcurrencyForForegroundRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		data := bytes.Repeat([]byte("data"), 64<<10)
		paths := make([]string, 0, 33)
		paths = append(paths, "foreground")
		for i := range 32 {
			paths = append(paths, fmt.Sprintf("prefetch-%d", i))
		}
		fs, gated := newStorageSlotFS(t, minRequestRoom, 0, paths, data,
			func(key string) bool { return strings.HasPrefix(key, "prefetch-") })
		defer fs.Close(ctx)
		defer close(gated.gate)
		for _, path := range paths[1:] {
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{
				FilePath: path, Size: int64(len(data)), Ranges: columnRanges(8, 20<<10, 1<<10), Scan: &PrefetchScan{},
			}))
		}
		synctest.Wait()
		inFlight := 0
		for _, path := range paths[1:] {
			cur, _, _ := gated.counts(path)
			inFlight += cur
		}
		require.EqualValues(t, fs.governor.backgroundMax, inFlight, "stalled downloads hold every background room")
		requireForegroundReads(t, fs, gated, data, SkipAllCache|ExactColumnsRead)
	})
}
