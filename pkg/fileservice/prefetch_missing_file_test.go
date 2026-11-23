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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitPrefetcherIdle(t *testing.T, fs *S3FS) {
	t.Helper()
	require.Eventually(t, func() bool { return fs.prefetcher.known.Load() == 0 }, 5*time.Second, time.Millisecond)
	fs.diskCache.prefetchObjects.reads.Wait()
}

// A prefetch of an object or a column range whose cached file is gone downloads it
// again, and later reads are served from the disk cache.
func TestPrefetchObjectRefillsMissingCachedFile(t *testing.T) {
	for _, whole := range []bool{true, false} {
		for _, removed := range []bool{true, false} {
			name := map[bool]string{true: "whole", false: "range"}[whole] + "/" + map[bool]string{true: "removed", false: "control"}[removed]
			t.Run(name, func(t *testing.T) {
				fs, storage, data := newPrefetchRangeFS(t, 0, 300<<10)
				ctx := context.Background()
				r := PrefetchRange{Offset: 40 << 10, Size: 4 << 10}
				req := func() ObjectPrefetch {
					if whole {
						return ObjectPrefetch{FilePath: "object", Size: int64(len(data)), Scan: &PrefetchScan{}}
					}
					return ObjectPrefetch{FilePath: "object", Size: r.Size, Ranges: []PrefetchRange{r}, Scan: &PrefetchScan{}}
				}
				read := func(i int) {
					b := PrefetchRange{Offset: r.Offset + int64(i)<<10, Size: 1 << 10}
					var got []byte
					if whole {
						vec := &IOVector{FilePath: "object", Policy: SkipFullFilePreloads | SkipMemoryCache,
							Entries: []IOEntry{{Offset: b.Offset, Size: b.Size}}}
						require.NoError(t, fs.Read(ctx, vec))
						got = append(got, vec.Entries[0].Data...)
						vec.Release()
					} else {
						var err error
						got, err = readColumnRange(ctx, fs, b)
						require.NoError(t, err)
					}
					require.Equal(t, data[b.Offset:b.Offset+b.Size], got)
				}

				require.NoError(t, fs.PrefetchObject(ctx, req()))
				waitPrefetcherIdle(t, fs)
				diskPath := fs.diskCache.pathForFile("object")
				if !whole {
					rs := fs.diskCache.ranges.of(fs.diskCache.rangeObjectKey("object"))
					require.Len(t, rs, 1)
					diskPath = rs[0].diskPath
				}
				require.Eventually(t, func() bool { return fs.diskCache.cache.Contains(diskPath) }, 5*time.Second, time.Millisecond)
				if removed {
					require.NoError(t, os.Remove(diskPath))
				}
				fs.diskCache.paths.closeKept()

				require.NoError(t, fs.PrefetchObject(ctx, req()))
				waitPrefetcherIdle(t, fs)
				_, err := os.Stat(diskPath)
				require.NoError(t, err, "the prefetch downloads the file again")
				base := storage.reads.Load()
				for i := 1; i <= 3; i++ {
					read(i)
				}
				require.Equal(t, base, storage.reads.Load(), "the reads are served from the disk cache")
			})
		}
	}
}
