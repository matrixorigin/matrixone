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
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

// The size of a range file is the prefix of the digits after its offset that equals the
// file's length, also when the object name starts with digits. Only files in the shard
// directory of their object are range files.
func TestParseRangeFile(t *testing.T) {
	root := "cache"
	in := func(object, name string) string {
		return filepath.Join(root, diskCacheShard(object), name+cacheFileSuffix)
	}
	require.NotEqual(t, diskCacheShard("123obj"), diskCacheShard("23obj"))

	key, offset, size, ok := parseRangeFile(root, in("123obj", "10-100123obj"), 100)
	require.True(t, ok)
	require.Equal(t, filepath.Join(root, diskCacheShard("123obj"), "123obj"+cacheFileSuffix), key)
	require.Equal(t, int64(10), offset)
	require.Equal(t, int64(100), size)

	key, _, size, ok = parseRangeFile(root, in("23obj", "10-100123obj"), 1001)
	require.True(t, ok)
	require.Equal(t, filepath.Join(root, diskCacheShard("23obj"), "23obj"+cacheFileSuffix), key)
	require.Equal(t, int64(1001), size)

	for name, path := range map[string]string{
		"no size prefix equals the length": in("123obj", "10-100123obj"),
		"another object's shard":           in("23obj", "10-7obj"),
		"whole file":                       in("full123obj", "full123obj"),
		"no object name":                   in("", "10-7"),
		"no offset":                        in("obj", "x-7obj"),
		"temporary file":                   filepath.Join(root, diskCacheShard("obj"), "10-7obj.tmp"),
		"object path with a separator":     filepath.Join(root, diskCacheShard("dir/obj"), "10-7dir", "obj"+cacheFileSuffix),
		"outside a shard":                  filepath.Join(root, "10-7obj"+cacheFileSuffix),
	} {
		_, _, _, ok := parseRangeFile(root, path, 7)
		require.False(t, ok, name)
	}
}

func rangeTestData(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	return data
}

func readEntry(t *testing.T, cache *DiskCache, offset, size int64) IOEntry {
	vec := &IOVector{FilePath: "123obj", Entries: []IOEntry{{Offset: offset, Size: size}}}
	require.NoError(t, cache.Read(context.Background(), vec))
	return vec.Entries[0]
}

// A read inside a cached range is served from it; a read beyond it is not, nor is one
// after the range left the cache. A restarted cache finds the range again.
func TestDiskCacheReadWithinCachedRange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cache, err := NewDiskCache(ctx, dir, fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	data := rangeTestData(200)
	require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "123obj", Entries: []IOEntry{
		{Offset: 100, Size: 100, Data: data[100:]},
		{Offset: 120, Size: 50, Data: data[120:170]},
	}}, false))

	e := readEntry(t, cache, 130, 30)
	require.True(t, e.done)
	require.Same(t, cache, e.fromCache)
	require.Equal(t, data[130:160], e.Data)
	e = readEntry(t, cache, 100, 100)
	require.True(t, e.done, "the exact range")
	require.Equal(t, data[100:], e.Data)
	require.False(t, readEntry(t, cache, 90, 20).done, "starts before the range")
	require.False(t, readEntry(t, cache, 190, 20).done, "ends after the range")

	cache.Close(ctx)
	cache, err = NewDiskCache(ctx, dir, fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	e = readEntry(t, cache, 101, 10)
	require.True(t, e.done, "after restart")
	require.Equal(t, data[101:111], e.Data)

	cache.cache.Delete(ctx, cache.pathForIOEntry("123obj", IOEntry{Offset: 100, Size: 100}))
	e = readEntry(t, cache, 101, 10)
	require.False(t, e.done, "the range left the cache")
	e = readEntry(t, cache, 125, 10)
	require.True(t, e.done, "the other range still contains it")
	require.Equal(t, data[125:135], e.Data)
}

// A read passes over an indexed range file the cache no longer holds, forgets it, and is
// served by the next range that contains it.
func TestDiskCacheReadPassesOverStaleRange(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	data := rangeTestData(200)
	require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "123obj", Entries: []IOEntry{
		{Offset: 120, Size: 50, Data: data[120:170]},
	}}, false))
	stale := cache.pathForIOEntry("123obj", IOEntry{Offset: 125, Size: 10})
	require.NoError(t, os.WriteFile(stale, make([]byte, 10), 0644))
	cache.ranges.add(cache.path, stale)

	e := readEntry(t, cache, 126, 4)
	require.True(t, e.done)
	require.Equal(t, data[126:130], e.Data)
	cache.ranges.mu.Lock()
	_, indexed := cache.ranges.objectOf[stale]
	cache.ranges.mu.Unlock()
	require.False(t, indexed)
}

// A read inside a cached range of an object whose whole file is cached reads the whole
// file, which counts the bytes toward the file's read share.
func TestDiskCacheReadPrefersWholeFileOverContainingRange(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	data := rangeTestData(200)
	require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "123obj", Entries: []IOEntry{
		{Offset: 100, Size: 100, Data: data[100:]},
	}}, false))
	require.NoError(t, cache.SetFile(ctx, "123obj", func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}))

	e := readEntry(t, cache, 130, 30)
	require.True(t, e.done)
	require.Equal(t, data[130:160], e.Data)
	cache.preloads.mu.Lock()
	served := cache.preloads.served[cache.pathForFile("123obj")]
	cache.preloads.mu.Unlock()
	require.Equal(t, int64(30), served)
}

// A read that no single cached range contains is served from cached ranges that hold it
// together, and not when they leave a gap or one of them left the cache.
func TestDiskCacheReadAcrossCachedRanges(t *testing.T) {
	ctx := context.Background()
	cache, err := NewDiskCache(ctx, t.TempDir(), fscache.ConstCapacity(1<<20), nil, false, nil, "")
	require.NoError(t, err)
	defer cache.Close(ctx)
	data := rangeTestData(400)
	require.NoError(t, cache.Update(ctx, &IOVector{FilePath: "123obj", Entries: []IOEntry{
		{Offset: 0, Size: 100, Data: data[:100]},
		{Offset: 80, Size: 120, Data: data[80:200]},
		{Offset: 200, Size: 100, Data: data[200:300]},
		{Offset: 350, Size: 50, Data: data[350:]},
	}}, false))

	e := readEntry(t, cache, 50, 220)
	require.True(t, e.done)
	require.Same(t, cache, e.fromCache)
	require.Equal(t, data[50:270], e.Data)
	e = readEntry(t, cache, 0, 300)
	require.True(t, e.done, "the whole span")
	require.Equal(t, data[:300], e.Data)
	require.False(t, readEntry(t, cache, 250, 120).done, "a gap")

	cache.cache.Delete(ctx, cache.pathForIOEntry("123obj", IOEntry{Offset: 200, Size: 100}))
	require.False(t, readEntry(t, cache, 150, 100).done, "a range left the cache")
}

// covering and containing on the sorted snapshots return what a scan of every range of
// the object returns, as ranges are added and removed.
func TestCachedRangesMatchFullScan(t *testing.T) {
	scanCovering := func(ranges []cachedRange, offset, size int64) ([]cachedRange, bool) {
		var out []cachedRange
		for pos, end := offset, offset+size; pos < end; {
			var best cachedRange
			ok := false
			for _, r := range ranges {
				if r.offset <= pos && pos < r.offset+r.size && (!ok || r.offset+r.size > best.offset+best.size) {
					best, ok = r, true
				}
			}
			if !ok {
				return nil, false
			}
			out = append(out, best)
			pos = best.offset + best.size
		}
		return out, true
	}
	ends := func(rs []cachedRange) []int64 {
		out := make([]int64, 0, len(rs))
		for _, r := range rs {
			out = append(out, r.offset+r.size)
		}
		return out
	}
	rnd := rand.New(rand.NewSource(1))
	for round := 0; round < 50; round++ {
		var c cachedRanges
		var all []cachedRange
		for i := 0; i < 1+rnd.Intn(40); i++ {
			r := cachedRange{diskPath: strconv.Itoa(i), offset: int64(rnd.Intn(1000)), size: int64(1 + rnd.Intn(200))}
			all = append(all, r)
			c.mu.Lock()
			if c.objectOf == nil {
				c.objectOf = make(map[string]string)
			}
			c.objectOf[r.diskPath] = "k"
			var rs []cachedRange
			if s := c.snapshot("k"); s != nil {
				rs = slices.Clone(s.ranges)
			}
			c.byObject.Store("k", newRangeSnapshot(append(rs, r)))
			c.mu.Unlock()
		}
		check := func() {
			for q := 0; q < 200; q++ {
				offset, size := int64(rnd.Intn(1200)), int64(1+rnd.Intn(300))
				got, ok := c.covering("k", offset, size)
				want, wantOK := scanCovering(all, offset, size)
				require.Equal(t, wantOK, ok)
				// Ties between ranges reaching as far may pick either; the ends are the same.
				require.Equal(t, ends(want), ends(got))
				gotC, okC := c.containing("k", offset, size, nil)
				wantC, wantOKC := smallestContaining(all, offset, size, nil)
				require.Equal(t, wantOKC, okC)
				require.Equal(t, wantC.size, gotC.size)
			}
		}
		check()
		for len(all) > 0 && rnd.Intn(3) > 0 {
			i := rnd.Intn(len(all))
			c.remove(all[i].diskPath)
			all = slices.Delete(all, i, i+1)
			check()
		}
	}
}
