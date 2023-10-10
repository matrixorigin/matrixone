// Copyright 2022 Matrix Origin
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
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

type DiskCache struct {
	capacity        int64
	evictInterval   time.Duration
	evictTarget     float64
	path            string
	fileExists      sync.Map
	perfCounterSets []*perfcounter.CounterSet

	evictState struct {
		sync.Mutex
		timer        *time.Timer
		newlyWritten int64
	}
	noAutoEviction bool

	updatingPaths struct {
		*sync.Cond
		m map[string]bool
	}
}

func NewDiskCache(
	ctx context.Context,
	path string,
	capacity int64,
	evictInterval time.Duration,
	evictTarget float64,
	perfCounterSets []*perfcounter.CounterSet,
) (*DiskCache, error) {

	if evictInterval == 0 {
		panic("zero evict interval")
	}
	if evictTarget == 0 {
		panic("zero evict target")
	}
	if evictTarget > 1 {
		panic("evict target greater than 1")
	}

	err := os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}
	ret := &DiskCache{
		capacity:        capacity,
		evictInterval:   evictInterval,
		evictTarget:     evictTarget,
		path:            path,
		perfCounterSets: perfCounterSets,
	}
	ret.triggerEvict(ctx, 0)
	ret.updatingPaths.Cond = sync.NewCond(new(sync.Mutex))
	ret.updatingPaths.m = make(map[string]bool)
	return ret, nil
}

var _ IOVectorCache = new(DiskCache)

func (d *DiskCache) Read(
	ctx context.Context,
	vector *IOVector,
) (
	err error,
) {

	if vector.CachePolicy.Any(SkipDiskReads) {
		return nil
	}

	var numHit, numRead, numOpen, numError int64
	defer func() {
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Disk.Read.Add(numRead)
			c.FileService.Cache.Disk.Hit.Add(numHit)
			c.FileService.Cache.Disk.Error.Add(numError)
			c.FileService.Cache.Disk.OpenFile.Add(numOpen)
		}, d.perfCounterSets...)
	}()

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	for i, entry := range vector.Entries {
		if entry.done {
			continue
		}
		if entry.Size < 0 {
			// ignore size unknown entry
			continue
		}

		t0 := time.Now()
		numRead++

		var file *os.File
		// open entry file
		entryPath := d.entryDataFilePath(path.File, entry)
		d.waitUpdateComplete(entryPath)
		entryFile, err := os.Open(entryPath)
		if err == nil {
			file = entryFile
			defer entryFile.Close()
			numOpen++
		}
		if file == nil {
			// no file available
			continue
		}

		if err := entry.ReadFromOSFile(file); err != nil {
			// ignore error
			numError++
			continue
		}

		entry.done = true
		entry.fromCache = d
		vector.Entries[i] = entry
		numHit++
		d.cacheHit(time.Since(t0))
	}

	return nil
}

func (d *DiskCache) cacheHit(duration time.Duration) {
	FSProfileHandler.AddSample(duration)
}

func (d *DiskCache) Update(
	ctx context.Context,
	vector *IOVector,
	async bool,
) (
	err error,
) {

	if vector.CachePolicy.Any(SkipDiskWrites) {
		return nil
	}

	var numOpen, numStat, numError, numWrite int64
	defer func() {
		perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
			set.FileService.Cache.Disk.OpenFile.Add(numOpen)
			set.FileService.Cache.Disk.StatFile.Add(numStat)
			set.FileService.Cache.Disk.WriteFile.Add(numWrite)
			set.FileService.Cache.Disk.Error.Add(numError)
		})
	}()

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	// callback
	var onWritten []OnDiskCacheWrittenFunc
	if v := ctx.Value(CtxKeyDiskCacheCallbacks); v != nil {
		onWritten = v.(*DiskCacheCallbacks).OnWritten
	}

	for _, entry := range vector.Entries {
		if len(entry.Data) == 0 {
			// no data
			continue
		}
		if entry.Size < 0 {
			// ignore size unknown entry
			continue
		}
		if entry.fromCache == d {
			// no need to update
			continue
		}

		entryFilePath := d.entryDataFilePath(path.File, entry)

		func() {
			doneUpdate := d.startUpdate(entryFilePath)
			defer doneUpdate()

			if _, ok := d.fileExists.Load(entryFilePath); ok {
				// already exists
				return
			}
			_, err := os.Stat(entryFilePath)
			if err == nil {
				// file exists
				d.fileExists.Store(entryFilePath, true)
				numStat++
				return
			}

			// write data
			dir := filepath.Dir(entryFilePath)
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				numError++
				return // ignore error
			}
			f, err := os.CreateTemp(dir, "*")
			if err != nil {
				numError++
				return // ignore error
			}
			numOpen++
			n, err := f.Write(entry.Data)
			if err != nil {
				f.Close()
				os.Remove(f.Name())
				numError++
				return // ignore error
			}
			if err := f.Close(); err != nil {
				numError++
				return // ignore error
			}
			if err := os.Rename(f.Name(), entryFilePath); err != nil {
				numError++
				return // ignore error
			}

			numWrite++
			d.triggerEvict(ctx, int64(n))
			d.fileExists.Store(entryFilePath, true)

			for _, fn := range onWritten {
				fn(vector.FilePath, entry)
			}

		}()

	}

	return nil
}

func (d *DiskCache) Flush() {
	//TODO
}

func (d *DiskCache) triggerEvict(ctx context.Context, bytesWritten int64) {
	if d.noAutoEviction {
		return
	}

	d.evictState.Lock()
	defer d.evictState.Unlock()

	d.evictState.newlyWritten += bytesWritten

	if d.evictState.timer != nil {
		// has pending eviction

		newlyWrittenThreshold := int64(math.Ceil(float64(d.capacity) * (1 - d.evictTarget)))
		if newlyWrittenThreshold > 0 && d.evictState.newlyWritten >= newlyWrittenThreshold {
			if d.evictState.timer.Stop() {
				// evict immediately
				logutil.Debug("disk cache: newly written bytes may excceeds eviction target, start immediately",
					zap.Any("newly-written", d.evictState.newlyWritten),
					zap.Any("newly-written-threshold", newlyWrittenThreshold),
				)
				// timer stopped, start eviction
				// before the following eviction is end, further calls of triggerEvict will not go this path, since timer.Stop will return false
				perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
					set.FileService.Cache.Disk.EvictImmediately.Add(1)
				}, d.perfCounterSets...)
				go func() {
					defer func() {
						d.evictState.Lock()
						d.evictState.timer = nil
						d.evictState.newlyWritten = 0
						d.evictState.Unlock()
					}()
					d.evict(ctx)
				}()
			}
			return
		}

		return
	}

	perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
		set.FileService.Cache.Disk.EvictPending.Add(1)
	}, d.perfCounterSets...)
	d.evictState.timer = time.AfterFunc(
		d.evictInterval,
		func() {
			defer func() {
				d.evictState.Lock()
				d.evictState.timer = nil
				d.evictState.newlyWritten = 0
				d.evictState.Unlock()
			}()
			d.evict(ctx)
		},
	)
}

func (d *DiskCache) evict(ctx context.Context) {
	paths := make(map[string]int64)
	var sumSize int64

	_ = filepath.WalkDir(d.path, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil //ignore
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(entry.Name(), cacheFileSuffix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil // ignore
		}
		size := info.Size()
		if _, ok := paths[path]; !ok {
			paths[path] = size
			sumSize += size
		}
		return nil
	})

	var numDeleted int64
	var bytesDeleted int64
	defer func() {
		if numDeleted > 0 {
			perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
				set.FileService.Cache.Disk.Evict.Add(numDeleted)
			}, d.perfCounterSets...)
			logutil.Debug("disk cache: eviction finished",
				zap.Any("files", numDeleted),
				zap.Any("bytes", bytesDeleted),
			)
		}
	}()
	target := int64(float64(d.capacity) * d.evictTarget)

	var onEvict []OnDiskCacheEvictFunc
	if v := ctx.Value(CtxKeyDiskCacheCallbacks); v != nil {
		onEvict = v.(*DiskCacheCallbacks).OnEvict
	}

	for path, size := range paths {
		if sumSize <= target {
			break
		}
		if err := os.Remove(path); err != nil {
			continue // ignore
		}
		d.fileExists.Delete(path)
		sumSize -= size
		numDeleted++
		bytesDeleted += size

		for _, fn := range onEvict {
			fn(path)
		}

	}
}

func (d *DiskCache) newFileContentWriter(contentPath string) (w io.Writer, done func(context.Context) error, closeFunc func() error, err error) {
	dir := filepath.Dir(contentPath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, nil, nil, err
	}
	f, err := os.CreateTemp(dir, "*")
	if err != nil {
		return nil, nil, nil, err
	}
	var doneOnce sync.Once
	return f, func(ctx context.Context) (err error) {
		doneOnce.Do(func() {
			var info fs.FileInfo
			info, err = f.Stat()
			if err != nil {
				return
			}
			name := f.Name()
			if err = f.Close(); err != nil {
				return
			}
			if err = os.Rename(name, contentPath); err != nil {
				return
			}
			d.triggerEvict(ctx, info.Size())
		})
		return
	}, f.Close, nil
}

const cacheFileSuffix = ".mofscache"

func (d *DiskCache) entryDataFilePath(path string, entry IOEntry) string {
	if entry.Size < 0 {
		panic("should not cache size -1 entry")
	}
	return filepath.Join(
		d.path,
		toOSPath(path),
		fmt.Sprintf("%d-%d%s", entry.Offset, entry.Size, cacheFileSuffix),
	)
}

func (d *DiskCache) waitUpdateComplete(path string) {
	d.updatingPaths.L.Lock()
	for d.updatingPaths.m[path] {
		d.updatingPaths.Wait()
	}
	d.updatingPaths.L.Unlock()
}

func (d *DiskCache) startUpdate(path string) (done func()) {
	d.updatingPaths.L.Lock()
	for d.updatingPaths.m[path] {
		d.updatingPaths.Wait()
	}
	d.updatingPaths.m[path] = true
	d.updatingPaths.L.Unlock()
	done = func() {
		d.updatingPaths.L.Lock()
		delete(d.updatingPaths.m, path)
		d.updatingPaths.Broadcast()
		d.updatingPaths.L.Unlock()
	}
	return
}
