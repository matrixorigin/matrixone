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
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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
	ret.triggerEvict(ctx, capacity) // trigger an immediately eviction
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

	if vector.Policy.Any(SkipDiskCacheReads) {
		return nil
	}

	var numHit, numRead, numOpenIOEntry, numOpenFull, numError int64
	defer func() {
		metric.FSReadHitDiskCounter.Add(float64(numHit))
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Disk.Read.Add(numRead)
			c.FileService.Cache.Disk.Hit.Add(numHit)
			c.FileService.Cache.Disk.Error.Add(numError)
			c.FileService.Cache.Disk.OpenIOEntryFile.Add(numOpenIOEntry)
			c.FileService.Cache.Disk.OpenFullFile.Add(numOpenFull)
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

		// entry file
		diskPath := d.pathForIOEntry(path.File, entry)
		d.waitUpdateComplete(diskPath)
		diskFile, err := os.Open(diskPath)
		if err == nil {
			file = diskFile
			defer diskFile.Close()
			numOpenIOEntry++
		}

		if file == nil {
			// full file
			diskPath := d.pathForFile(path.File)
			d.waitUpdateComplete(diskPath)
			diskFile, err := os.Open(diskPath)
			if err == nil {
				defer diskFile.Close()
				numOpenFull++
				// seek
				_, err = diskFile.Seek(entry.Offset, io.SeekStart)
				if err == nil {
					file = diskFile
				}
			}
		}

		if file == nil {
			// no file available
			continue
		}

		if err := entry.ReadFromOSFile(file); err != nil {
			// ignore error
			numError++
			logutil.Warn("read disk cache error", zap.Any("error", err))
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

	if vector.Policy.Any(SkipDiskCacheWrites) {
		return nil
	}

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

		diskPath := d.pathForIOEntry(path.File, entry)
		written, err := d.writeFile(ctx, diskPath, func(context.Context) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(entry.Data)), nil
		})
		if err != nil {
			return err
		}
		if written {
			for _, fn := range onWritten {
				fn(vector.FilePath, entry)
			}
		}

	}

	return nil
}

func (d *DiskCache) writeFile(
	ctx context.Context,
	diskPath string,
	openReader func(context.Context) (io.ReadCloser, error),
) (bool, error) {
	var numCreate, numStat, numError, numWrite int64
	defer func() {
		perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
			set.FileService.Cache.Disk.CreateFile.Add(numCreate)
			set.FileService.Cache.Disk.StatFile.Add(numStat)
			set.FileService.Cache.Disk.WriteFile.Add(numWrite)
			set.FileService.Cache.Disk.Error.Add(numError)
		})
	}()

	doneUpdate := d.startUpdate(diskPath)
	defer doneUpdate()

	if _, ok := d.fileExists.Load(diskPath); ok {
		// already exists
		return false, nil
	}
	_, err := os.Stat(diskPath)
	if err == nil {
		// file exists
		d.fileExists.Store(diskPath, true)
		numStat++
		return false, nil
	}

	// write data
	dir := filepath.Dir(diskPath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}
	f, err := os.CreateTemp(dir, "*")
	if err != nil {
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}
	numCreate++
	from, err := openReader(ctx)
	if err != nil {
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}
	defer from.Close()
	var buf []byte
	put := ioBufferPool.Get(&buf)
	defer put.Put()
	n, err := io.CopyBuffer(f, from, buf)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}
	if err := f.Close(); err != nil {
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}
	if err := os.Rename(f.Name(), diskPath); err != nil {
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}
	logutil.Debug("disk cache file written",
		zap.Any("path", diskPath),
	)

	numWrite++
	d.triggerEvict(ctx, int64(n))
	d.fileExists.Store(diskPath, true)

	return true, nil
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
				logutil.Info("disk cache: newly written bytes may excceeds eviction target, start immediately",
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
			// try remove if empty. for cleaning old structure
			if path != d.path {
				os.Remove(path)
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), cacheFileSuffix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil // ignore
		}

		var size int64
		if sys, ok := info.Sys().(*syscall.Stat_t); ok {
			size = int64(sys.Blocks) * 512 // it's always 512, not sys.Blksize
		} else {
			size = info.Size()
		}

		if _, ok := paths[path]; !ok {
			paths[path] = size
			sumSize += size
		}
		return nil
	})

	target := int64(float64(d.capacity) * d.evictTarget)

	logutil.Info("disk cache eviction check",
		zap.Any("used bytes", sumSize),
		zap.Any("target", target),
	)

	var numDeleted int64
	var bytesDeleted int64
	defer func() {
		if numDeleted > 0 {
			perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
				set.FileService.Cache.Disk.Evict.Add(numDeleted)
			}, d.perfCounterSets...)
			logutil.Info("disk cache eviction finished",
				zap.Any("deleted files", numDeleted),
				zap.Any("deleted bytes", bytesDeleted),
			)
		}
	}()

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
		} else {
			logutil.Debug("disk cache file removed",
				zap.Any("path", path),
			)
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

const cacheFileSuffix = ".mofscache"

func (d *DiskCache) pathForIOEntry(path string, entry IOEntry) string {
	if entry.Size < 0 {
		panic("should not cache size -1 entry")
	}
	return filepath.Join(
		d.path,
		fmt.Sprintf("%d-%d%s%s", entry.Offset, entry.Size, toOSPath(path), cacheFileSuffix),
	)
}

func (d *DiskCache) pathForFile(path string) string {
	return filepath.Join(
		d.path,
		fmt.Sprintf("full%s%s", toOSPath(path), cacheFileSuffix),
	)
}

var ErrNotCacheFile = errorStr("not a cache file")

func (d *DiskCache) decodeFilePath(diskPath string) (string, error) {
	path, err := filepath.Rel(d.path, diskPath)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(path, "full") {
		return "", ErrNotCacheFile
	}
	path = strings.TrimPrefix(path, "full")
	path = strings.TrimSuffix(path, cacheFileSuffix)
	return fromOSPath(path), nil
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

var _ FileCache = new(DiskCache)

func (d *DiskCache) SetFile(
	ctx context.Context,
	path string,
	openReader func(context.Context) (io.ReadCloser, error),
) error {
	diskPath := d.pathForFile(path)
	_, err := d.writeFile(ctx, diskPath, openReader)
	if err != nil {
		return err
	}
	return nil
}

func (d *DiskCache) DeletePaths(
	ctx context.Context,
	paths []string,
) error {

	for _, path := range paths {
		diskPath := d.pathForFile(path)
		//TODO also delete IOEntry files

		doneUpdate := d.startUpdate(diskPath)
		defer doneUpdate()

		d.fileExists.Delete(diskPath)

		if err := os.Remove(diskPath); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
	}

	return nil
}
