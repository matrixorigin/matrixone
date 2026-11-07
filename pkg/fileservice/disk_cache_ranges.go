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
// range that contains its own.
type cachedRanges struct {
	mu       sync.Mutex
	byObject map[string][]cachedRange
	objectOf map[string]string // diskPath -> object key
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
	if c.byObject == nil {
		c.byObject = make(map[string][]cachedRange)
		c.objectOf = make(map[string]string)
	}
	c.objectOf[diskPath] = key
	c.byObject[key] = append(c.byObject[key], cachedRange{diskPath: diskPath, offset: offset, size: size})
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
	ranges := c.byObject[key]
	for i := range ranges {
		if ranges[i].diskPath == diskPath {
			ranges[i] = ranges[len(ranges)-1]
			ranges = ranges[:len(ranges)-1]
			break
		}
	}
	if len(ranges) == 0 {
		delete(c.byObject, key)
	} else {
		c.byObject[key] = ranges
	}
}

// containing returns the smallest cached range of the object with key that contains
// size bytes at offset, passing over the range files in tried.
func (c *cachedRanges) containing(key string, offset, size int64, tried []string) (cachedRange, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var found cachedRange
	ok := false
	for _, r := range c.byObject[key] {
		if r.offset <= offset && offset+size <= r.offset+r.size && (!ok || r.size < found.size) &&
			!slices.Contains(tried, r.diskPath) {
			found, ok = r, true
		}
	}
	return found, ok
}
