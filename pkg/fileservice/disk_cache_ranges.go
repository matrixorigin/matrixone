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
	"cmp"
	"os"
	pathpkg "path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// cachedRange is a range of an object cached in a file of its own.
type cachedRange struct {
	diskPath string
	offset   int64
	size     int64
}

// cachedRanges indexes the cached range files by object, so a read can find a cached
// range that contains its own. Reads take no lock: each object's ranges are a snapshot
// that add and remove replace.
type cachedRanges struct {
	mu       sync.Mutex        // held by add and remove
	byObject sync.Map          // object key -> *rangeSnapshot
	objectOf map[string]string // diskPath -> object key, under mu
}

// rangeSnapshot is the cached ranges of an object, sorted by offset. reach[i] is the index
// of the range among the first i+1 that reaches furthest.
type rangeSnapshot struct {
	ranges []cachedRange
	reach  []int
}

func newRangeSnapshot(ranges []cachedRange) *rangeSnapshot {
	slices.SortFunc(ranges, func(a, b cachedRange) int { return cmp.Compare(a.offset, b.offset) })
	reach := make([]int, len(ranges))
	for i := range ranges {
		reach[i] = i
		if i > 0 && ranges[reach[i-1]].offset+ranges[reach[i-1]].size >= ranges[i].offset+ranges[i].size {
			reach[i] = reach[i-1]
		}
	}
	return &rangeSnapshot{ranges: ranges, reach: reach}
}

func (c *cachedRanges) snapshot(key string) *rangeSnapshot {
	if v, ok := c.byObject.Load(key); ok {
		return v.(*rangeSnapshot)
	}
	return nil
}

// startingBy returns the number of ranges that start at or before pos.
func (s *rangeSnapshot) startingBy(pos int64) int {
	n, _ := slices.BinarySearchFunc(s.ranges, pos+1, func(r cachedRange, t int64) int { return cmp.Compare(r.offset, t) })
	return n
}

// rangeObjectKey returns the key of the range files of filePath: their disk path without
// the offset and size.
func (d *DiskCache) rangeObjectKey(filePath string) string {
	filePath = pathpkg.Clean(filePath)
	return filepath.Join(d.path, diskCacheShard(filePath), toOSPath(filePath)+cacheFileSuffix)
}

// parseRangeFile returns the object key, offset and size of the range file at diskPath
// under the cache directory root. The size digits run into the object name, so the size
// is the prefix of them that equals the file's length. Only files in the shard
// directory of their object are range files of objects whose paths have no separator,
// the only ones indexed.
func parseRangeFile(root, diskPath string, length int64) (key string, offset, size int64, ok bool) {
	dir, name := filepath.Split(diskPath)
	if !strings.HasSuffix(name, cacheFileSuffix) || filepath.Dir(filepath.Clean(dir)) != filepath.Clean(root) {
		return "", 0, 0, false
	}
	dash := strings.IndexByte(name, '-')
	if dash <= 0 {
		return "", 0, 0, false
	}
	offset, err := strconv.ParseInt(name[:dash], 10, 64)
	if err != nil {
		return "", 0, 0, false
	}
	digits := name[dash+1:]
	for i := 1; i <= len(digits) && digits[i-1] >= '0' && digits[i-1] <= '9'; i++ {
		size, err = strconv.ParseInt(digits[:i], 10, 64)
		if err != nil || size > length {
			break
		}
		object := strings.TrimSuffix(digits[i:], cacheFileSuffix)
		if size == length && object != "" && diskCacheShard(fromOSPath(object)) == filepath.Base(dir) {
			return filepath.Join(dir, digits[i:]), offset, size, true
		}
	}
	return "", 0, 0, false
}

// add indexes the range file at diskPath under root, if it is one.
func (c *cachedRanges) add(root, diskPath string) {
	if isFullFileCachePath(diskPath) || !strings.HasSuffix(diskPath, cacheFileSuffix) {
		return
	}
	info, err := os.Stat(diskPath)
	if err != nil {
		return
	}
	key, offset, size, ok := parseRangeFile(root, diskPath, info.Size())
	if !ok {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.objectOf[diskPath]; ok {
		return
	}
	if c.objectOf == nil {
		c.objectOf = make(map[string]string)
	}
	c.objectOf[diskPath] = key
	var ranges []cachedRange
	if old := c.snapshot(key); old != nil {
		ranges = slices.Clone(old.ranges)
	}
	ranges = append(ranges, cachedRange{diskPath: diskPath, offset: offset, size: size})
	c.byObject.Store(key, newRangeSnapshot(ranges))
}

// remove forgets the range file at diskPath.
func (c *cachedRanges) remove(diskPath string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key, ok := c.objectOf[diskPath]
	if !ok {
		return
	}
	delete(c.objectOf, diskPath)
	old := c.snapshot(key)
	if old == nil {
		return
	}
	ranges := slices.DeleteFunc(slices.Clone(old.ranges), func(r cachedRange) bool { return r.diskPath == diskPath })
	if len(ranges) == 0 {
		c.byObject.Delete(key)
		return
	}
	c.byObject.Store(key, newRangeSnapshot(ranges))
}

// covering returns cached ranges of the object with key that together hold size bytes
// at offset, in order, each starting where the one before it ends or earlier, or false
// if the cached ranges leave a gap. Each range is the one that reaches furthest of those
// holding the byte where it starts.
func (c *cachedRanges) covering(key string, offset, size int64) ([]cachedRange, bool) {
	s := c.snapshot(key)
	if s == nil {
		return nil, false
	}
	var out []cachedRange
	end := offset + size
	for pos := offset; pos < end; {
		n := s.startingBy(pos)
		if n == 0 {
			return nil, false
		}
		best := s.ranges[s.reach[n-1]]
		if best.offset+best.size <= pos {
			return nil, false
		}
		out = append(out, best)
		pos = best.offset + best.size
	}
	return out, true
}

// containing returns the smallest cached range of the object with key that contains
// size bytes at offset, passing over the range files in tried.
func (c *cachedRanges) containing(key string, offset, size int64, tried []string) (cachedRange, bool) {
	s := c.snapshot(key)
	if s == nil {
		return cachedRange{}, false
	}
	return smallestContaining(s.ranges[:s.startingBy(offset)], offset, size, tried)
}

// of returns a copy of the cached ranges of the object with key.
func (c *cachedRanges) of(key string) []cachedRange {
	if s := c.snapshot(key); s != nil {
		return slices.Clone(s.ranges)
	}
	return nil
}

// smallestContaining returns the smallest of ranges that contains size bytes at offset,
// passing over the range files in tried.
func smallestContaining(ranges []cachedRange, offset, size int64, tried []string) (cachedRange, bool) {
	var found cachedRange
	ok := false
	for _, r := range ranges {
		if r.offset <= offset && offset+size <= r.offset+r.size && (!ok || r.size < found.size) &&
			!slices.Contains(tried, r.diskPath) {
			found, ok = r, true
		}
	}
	return found, ok
}
