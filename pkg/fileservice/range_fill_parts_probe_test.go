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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A range prefetch of at least the split size, memory- or file-backed, reads its range in
// one request and caches it as one range. Control: a whole object of the same size,
// prefetched first, splits into its parts.
func TestRangeFillProbeOneRequestWholeObjectSplits(t *testing.T) {
	const size = 1 << 20
	for _, backing := range []string{"memory", "file"} {
		t.Run(backing, func(t *testing.T) {
			fs, _, data := newPrefetchRangeFS(t, 0, 2<<20)
			d := fs.diskCache
			d.objectSplit = objectSplitConfig{parts: 2, min: 512 << 10}
			if backing == "file" {
				d.memoryPrefetchMax = 64 << 10
			}
			ctx := context.Background()
			require.NoError(t, fs.Write(ctx, IOVector{FilePath: "whole", Entries: []IOEntry{{Size: size, Data: data[:size]}},
				Policy: SkipDiskCache | SkipMemoryCache}))
			d.prefetched.store(d.objectKey("whole"), size)
			starts := &startRecorder{ObjectStorage: fs.governor.raw}
			fs.governor.raw = starts

			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "whole", Size: size, Scan: &PrefetchScan{}}))
			require.Eventually(t, func() bool { return d.cache.Contains(d.wholePath("whole", size)) },
				10*time.Second, time.Millisecond)
			probeWaitIdle(t, fs)
			require.ElementsMatch(t, []int64{0, 512 << 10}, starts.offsets(), "the whole object splits")

			r := PrefetchRange{Offset: 64 << 10, Size: size}
			require.NoError(t, fs.PrefetchObject(ctx, ObjectPrefetch{FilePath: "object", Size: r.Size,
				Ranges: []PrefetchRange{r}, Scan: &PrefetchScan{}}))
			require.Eventually(t, func() bool {
				_, ok := d.ranges.covering(d.objectKey("object"), r.Offset, r.Size)
				return ok
			}, 10*time.Second, time.Millisecond)
			probeWaitIdle(t, fs)
			require.Equal(t, []int64{r.Offset}, starts.offsets()[2:], "the range is read in one request")
			cached, _ := d.ranges.covering(d.objectKey("object"), r.Offset, r.Size)
			require.Len(t, cached, 1, "the range is one cached range")
			got, err := readColumnRange(ctx, fs, PrefetchRange{Offset: r.Offset + 512<<10 - 100, Size: 200})
			require.NoError(t, err)
			require.Equal(t, data[r.Offset+512<<10-100:r.Offset+512<<10+100], got)
			require.Len(t, starts.offsets(), 3, "the read is served from the cache")
		})
	}
}
