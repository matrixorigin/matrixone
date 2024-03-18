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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fifocache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/ncw/directio"
	"go.uber.org/zap"
)

type DiskCache struct {
	path            string
	perfCounterSets []*perfcounter.CounterSet
	useDirectIO     bool

	updatingPaths struct {
		*sync.Cond
		m map[string]bool
	}

	cache *fifocache.Cache[string, struct{}]
}

func NewDiskCache(
	ctx context.Context,
	path string,
	capacity int,
	perfCounterSets []*perfcounter.CounterSet,
	useDirectIO bool,
) (ret *DiskCache, err error) {

	err = os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}

	ret = &DiskCache{
		path:            path,
		perfCounterSets: perfCounterSets,

		cache: fifocache.New[string, struct{}](
			capacity,
			func(path string, _ struct{}) {
				err := os.Remove(path)
				if err == nil {
					perfcounter.Update(ctx, func(set *perfcounter.CounterSet) {
						set.FileService.Cache.Disk.Evict.Add(1)
					}, perfCounterSets...)
				}
			},
			func(key string) uint8 {
				return uint8(xxhash.Sum64String(key))
			},
		),

		useDirectIO: useDirectIO,
	}
	ret.updatingPaths.Cond = sync.NewCond(new(sync.Mutex))
	ret.updatingPaths.m = make(map[string]bool)

	if err := ret.checkDirectIO(); err != nil {
		return nil, err
	}

	ret.loadCache()

	return ret, nil
}

func (d *DiskCache) checkDirectIO() error {
	if !d.useDirectIO {
		return nil
	}

	// check block size
	stat, err := os.Stat(d.path)
	if err != nil {
		return err
	}
	sys, ok := stat.Sys().(*syscall.Stat_t)
	if ok {
		logutil.Info("using direct I/O",
			zap.Any("block size", sys.Blksize),
			zap.Any("align size", directIOAlignSize),
		)
		if directIOAlignSize%sys.Blksize != 0 ||
			directIOAlignSize/sys.Blksize == 0 {
			panic("bad align size")
		}

	} else {
		// cannot get block size, do not use direct I/O
		d.useDirectIO = false
		return nil
	}

	// write checks
	// if direct I/O doesn't work due to unknown reason, we just disable it
	f, err := d.createTemp(d.path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	var buf []byte
	put := directioBufferPool.Get(&buf)
	defer put.Put()
	_, err = CopyBuffer(f, bytes.NewReader(bytes.Repeat([]byte("test"), 8192)), buf)
	if err != nil {
		logutil.Warn("not using direct I/O",
			zap.Error(err),
		)
		d.useDirectIO = false
		return nil
	}

	return nil
}

func (d *DiskCache) loadCache() {

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

		d.cache.Set(path, struct{}{}, int(fileSize(info)))

		return nil
	})

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
		v2.FSReadHitDiskCounter.Add(float64(numHit))
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
		var diskFile *os.File
		if d.useDirectIO {
			diskFile, err = directio.OpenFile(diskPath, os.O_RDONLY, 0644)
		} else {
			diskFile, err = os.OpenFile(diskPath, os.O_RDONLY, 0644)
		}
		if err == nil {
			file = diskFile
			defer diskFile.Close()
			numOpenIOEntry++
		}

		if file == nil {
			// full file
			diskPath = d.pathForFile(path.File)
			d.waitUpdateComplete(diskPath)
			var diskFile *os.File
			if d.useDirectIO {
				diskFile, err = directio.OpenFile(diskPath, os.O_RDONLY, 0644)
			} else {
				diskFile, err = os.OpenFile(diskPath, os.O_RDONLY, 0644)
			}
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

		if _, ok := d.cache.Get(diskPath); !ok {
			// set cache
			stat, err := file.Stat()
			if err != nil {
				return err
			}
			d.cache.Set(diskPath, struct{}{}, int(fileSize(stat)))
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

	if _, ok := d.cache.Get(diskPath); ok {
		// already exists
		return false, nil
	}
	stat, err := os.Stat(diskPath)
	if err == nil {
		// file exists
		d.cache.Set(diskPath, struct{}{}, int(fileSize(stat)))
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

	f, err := d.createTemp(dir)
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
	put := directioBufferPool.Get(&buf)
	defer put.Put()
	_, err = CopyBuffer(f, from, buf)
	if err != nil {
		f.Close()
		os.Remove(f.Name())
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}

	if err := f.Sync(); err != nil {
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}

	// set cache
	stat, err = f.Stat()
	if err != nil {
		numError++
		logutil.Warn("write disk cache error", zap.Any("error", err))
		return false, nil // ignore error
	}
	d.cache.Set(diskPath, struct{}{}, int(fileSize(stat)))

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

	return true, nil
}

func (d *DiskCache) Flush() {
}

func (d *DiskCache) createTemp(dir string) (*os.File, error) {
	try := 0
	var f *os.File
	var err error
	for {
		tempFilePath := filepath.Join(
			dir,
			fmt.Sprintf("%d.tmp", fastrand()),
		)
		if d.useDirectIO {
			f, err = directio.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		} else {
			f, err = os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
		}
		if err != nil {
			if os.IsExist(err) {
				if try++; try < 10000 {
					continue
				}
				err = &os.PathError{Op: "createtemp", Path: tempFilePath, Err: os.ErrExist}
				return nil, err
			}
			err = &os.PathError{Op: "createtemp", Path: tempFilePath, Err: os.ErrExist}
			return nil, err
		}
		break
	}
	return f, err
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

		if err := os.Remove(diskPath); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		}
		d.cache.Delete(diskPath)
	}

	return nil
}

func fileSize(info fs.FileInfo) int64 {
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		return int64(sys.Blocks) * 512 // it's always 512, not sys.Blksize
	}
	return info.Size()
}
