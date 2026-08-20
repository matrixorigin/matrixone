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
	"hash/maphash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fifocache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type DiskCache struct {
	path               string
	cacheDataAllocator CacheDataAllocator
	memoryCache        fscache.DataCache
	perfCounterSets    []*perfcounter.CounterSet

	updatingPaths struct {
		*sync.Cond
		m map[string]*diskCachePathUpdate
	}

	cache        *fifocache.Cache[string, struct{}]
	capacityFunc fscache.CapacityFunc

	async         diskCacheAsyncState
	writeFailures diskCacheWriteFailureState
	fileSync      func(*os.File) error
}

// Keep this type non-zero-sized: pointers to distinct zero-sized allocations
// are not required to have distinct addresses in Go, but the path owner uses
// pointer identity as its generation token.
type diskCachePathUpdate struct{ _ byte }

func NewDiskCache(
	ctx context.Context,
	path string,
	capacity fscache.CapacityFunc,
	perfCounterSets []*perfcounter.CounterSet,
	asyncLoad bool,
	cacheDataAllocator CacheDataAllocator,
	name string,
) (ret *DiskCache, err error) {
	return newDiskCacheWithMetricScope(ctx, path, capacity, perfCounterSets, asyncLoad, cacheDataAllocator, name, "")
}

func newDiskCacheWithMetricScope(
	ctx context.Context,
	path string,
	capacity fscache.CapacityFunc,
	perfCounterSets []*perfcounter.CounterSet,
	asyncLoad bool,
	cacheDataAllocator CacheDataAllocator,
	name string,
	metricScope string,
) (ret *DiskCache, err error) {

	err = os.MkdirAll(path, 0755)
	if err != nil {
		return nil, err
	}

	if cacheDataAllocator == nil {
		cacheDataAllocator = DefaultCacheDataAllocator()
	}

	seed := maphash.MakeSeed()

	inuseBytes, capacityBytes := metric.GetFsCacheBytesGaugeWithScope(metricScope, name, "disk")
	capacityBytes.Set(float64(capacity()))

	capacityFunc := func() int64 {
		// read from global size hint
		if n := GlobalDiskCacheSizeHint.Load(); n > 0 {
			return n
		}
		// fallback
		return capacity()
	}

	var cache *fifocache.Cache[string, struct{}]
	ret = &DiskCache{
		path:               path,
		cacheDataAllocator: cacheDataAllocator,
		perfCounterSets:    perfCounterSets,
		fileSync:           func(file *os.File) error { return file.Sync() },

		capacityFunc: capacityFunc,
	}
	cache = fifocache.New(

		capacityFunc,

		func(key string) uint64 {
			return maphash.String(seed, key)
		},

		func(_ context.Context, _ string, _ struct{}, size int64, _ uint64) { // postSet
			inuseBytes.Add(float64(size))
			capacityBytes.Set(float64(capacityFunc()))
		},

		nil,
		func(ctx context.Context, path string, _ struct{}, size int64, _ uint64) {
			inuseBytes.Add(float64(-size))
			capacityBytes.Set(float64(capacityFunc()))
			doneUpdate, ok := ret.tryStartUpdate(path)
			if !ok {
				return
			}
			defer doneUpdate()
			if ret.cache.Contains(path) {
				return
			}
			err := os.Remove(path)
			if err == nil {
				metric.FSDiskCacheEvictCounter.Add(1)
			} else if !os.IsNotExist(err) {
				logutil.Error("delete disk cache file",
					zap.Any("error", err),
				)
			}
		},
	)
	ret.cache = cache
	ret.updatingPaths.Cond = sync.NewCond(new(sync.Mutex))
	ret.updatingPaths.m = make(map[string]*diskCachePathUpdate)
	ret.initAsyncUpdates()
	ret.initWriteFailureState()

	if asyncLoad {
		go ret.loadCache(ctx)
	} else {
		ret.loadCache(ctx)
	}

	if name != "" {
		allDiskCaches.Store(ret, name)
	}

	return ret, nil
}

func (d *DiskCache) loadCache(ctx context.Context) {
	t0 := time.Now()

	type Info struct {
		Path  string
		Entry os.DirEntry
	}
	works := make(chan Info)

	numWorkers := runtime.NumCPU()
	wg := new(sync.WaitGroup)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range works {

				info, err := work.Entry.Info()
				if err != nil {
					continue // ignore
				}

				d.cache.Set(ctx, work.Path, struct{}{}, int64(fileSize(info)))
			}
		}()
	}

	var numFiles, numCacheFiles, numTempFiles, numDeleted int

	_ = filepath.WalkDir(d.path, func(path string, entry os.DirEntry, err error) error {
		numFiles++
		if err != nil {
			return nil //ignore
		}

		if entry.IsDir() {
			// try remove if empty. for cleaning old structure
			if path != d.path {
				// os.Remove will not delete non-empty directory
				_ = os.Remove(path)
			}
			return nil

		} else {
			// plain files
			if !strings.HasSuffix(entry.Name(), cacheFileSuffix) {
				// not cache file
				if strings.HasSuffix(entry.Name(), cacheFileTempSuffix) {
					numTempFiles++
					// temp file
					info, err := entry.Info()
					if err == nil && time.Since(info.ModTime()) > time.Hour*8 {
						// old temp file
						_ = os.Remove(path)
						numDeleted++
					}
				} else {
					// unknown file
					_ = os.Remove(path)
					numDeleted++
				}
				return nil
			}
		}

		numCacheFiles++
		works <- Info{
			Path:  path,
			Entry: entry,
		}

		return nil
	})

	close(works)
	wg.Wait()

	logutil.Info("disk cache info loaded",
		zap.Any("all files", numFiles),
		zap.Any("cache files", numCacheFiles),
		zap.Any("temp files", numTempFiles),
		zap.Any("deleted files", numDeleted),
		zap.Any("time", time.Since(t0)),
	)

	done := make(chan int64, 1)
	d.cache.Evict(ctx, done, 0)
	target := <-done
	logutil.Info("disk cache evict done",
		zap.Any("target", target),
	)

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
		LogEvent(ctx, str_update_metrics_begin)

		metric.FSReadHitDiskCounter.Add(float64(numHit))
		metric.FSReadReadDiskCounter.Add(float64(numRead))
		if numError > 0 {
			metric.FSDiskCacheErrorCounter.Add(float64(numError))
		}
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.FileService.Cache.Read.Add(numRead)
			c.FileService.Cache.Hit.Add(numHit)
			c.FileService.Cache.Disk.Read.Add(numRead)
			c.FileService.Cache.Disk.Hit.Add(numHit)
		}, d.perfCounterSets...)

		LogEvent(ctx, str_update_metrics_end)
	}()

	path, err := ParsePath(vector.FilePath)
	if err != nil {
		return err
	}

	openedFiles := make(map[string]*os.File)
	defer func() {
		LogEvent(ctx, str_close_disk_files_begin)
		for _, file := range openedFiles {
			_ = file.Close()
		}
		LogEvent(ctx, str_close_disk_files_end)
	}()

	fillEntry := func(entry *IOEntry) error {
		LogEvent(ctx, str_disk_cache_fill_entry_begin)
		defer LogEvent(ctx, str_disk_cache_fill_entry_end)

		if entry.done {
			return nil
		}
		if entry.Size < 0 {
			// ignore size unknown entry
			return nil
		}

		numRead++

		var file *os.File

		// entry file
		diskPath := d.pathForIOEntry(path.File, *entry)
		if f, ok := openedFiles[diskPath]; ok {
			// use opened file
			LogEvent(ctx, str_disk_cache_file_seek_begin)
			// An IOEntry cache file contains only this range, so its content
			// always starts at file offset zero. entry.Offset is only valid for
			// a full-object cache file.
			_, err = f.Seek(0, io.SeekStart)
			LogEvent(ctx, str_disk_cache_file_seek_end)
			if err == nil {
				file = f
			}
		} else {
			// open file
			if d.waitUpdateComplete(ctx, diskPath) {
				LogEvent(ctx, str_disk_cache_file_open_begin)
				diskFile, err := os.Open(diskPath)
				LogEvent(ctx, str_disk_cache_file_open_end)
				if err == nil {
					file = diskFile
					defer func() {
						openedFiles[diskPath] = diskFile
					}()
					numOpenIOEntry++
				}
			}
		}

		if file == nil {
			// try full file
			diskPath = d.pathForFile(path.File)
			if f, ok := openedFiles[diskPath]; ok {
				// use opened file
				LogEvent(ctx, str_disk_cache_file_seek_begin)
				_, err = f.Seek(entry.Offset, io.SeekStart)
				LogEvent(ctx, str_disk_cache_file_seek_end)
				if err == nil {
					file = f
				}
			} else {
				// open file
				if d.waitUpdateCompleteFor(ctx, diskPath, shortIOWaitDuration) {
					LogEvent(ctx, str_disk_cache_file_open_begin)
					diskFile, err := os.Open(diskPath)
					LogEvent(ctx, str_disk_cache_file_open_end)
					if err == nil {
						defer func() {
							openedFiles[diskPath] = diskFile
						}()
						numOpenFull++
						// seek
						LogEvent(ctx, str_disk_cache_file_seek_begin)
						_, err = diskFile.Seek(entry.Offset, io.SeekStart)
						LogEvent(ctx, str_disk_cache_file_seek_end)
						if err == nil {
							file = diskFile
						}
					}
				}
			}
		}

		if file == nil {
			// no file available
			return nil
		}

		LogEvent(ctx, str_disk_cache_update_states_begin)
		if _, ok := d.cache.Get(ctx, diskPath); !ok {
			// set cache
			LogEvent(ctx, str_disk_cache_file_stat_begin)
			stat, err := file.Stat()
			LogEvent(ctx, str_disk_cache_file_stat_end)
			if err != nil {
				return err
			}
			d.cache.Set(ctx, diskPath, struct{}{}, fileSize(stat))
		}
		LogEvent(ctx, str_disk_cache_update_states_end)

		allocator := d.cacheDataAllocator
		if entry.ToCacheData != nil && d.memoryCache != nil {
			if _, ok := allocator.(capacityGuardedCacheDataAllocator); !ok {
				allocator = cacheCapacityGuardedAllocator{
					cache:     d.memoryCache,
					allocator: allocator,
				}
			}
		}
		readOffset, readSize := int64(0), entry.Size
		if diskPath == d.pathForFile(path.File) {
			readOffset = entry.Offset
		}
		if err := entry.ReadFromOSFile(ctx, file, allocator); err != nil {
			return err
		}
		fadviseDontNeed(file, readOffset, readSize)

		entry.done = true
		entry.fromCache = d
		numHit++

		return nil
	}

	for i := range vector.Entries {
		if err := fillEntry(&vector.Entries[i]); err != nil {
			// ignore error
			numError++
			logutil.Warn(
				"read disk cache error",
				zap.Any("error", err),
				zap.Any("path", vector.FilePath),
				zap.Any("entry", vector.Entries[i]),
			)
		}
	}

	return nil
}

func (d *DiskCache) Update(
	ctx context.Context,
	vector *IOVector,
	async bool,
) (
	err error,
) {
	if err := ctx.Err(); err != nil {
		return err
	}

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
		onWritten = append(onWritten, v.(*DiskCacheCallbacks).OnWritten...)
	}

	for _, entry := range vector.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
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
		if async {
			d.scheduleAsyncUpdate(vector.FilePath, diskPath, entry, onWritten)
			continue
		}
		if err := d.updateEntry(ctx, vector.FilePath, diskPath, entry, onWritten); err != nil {
			return err
		}
	}

	return nil
}

func (d *DiskCache) updateEntry(
	ctx context.Context,
	filePath string,
	diskPath string,
	entry IOEntry,
	onWritten []OnDiskCacheWrittenFunc,
) error {
	written, writeErr := d.writeFile(ctx, diskPath, func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(entry.Data)), nil
	})
	if writeErr != nil {
		return d.observeWriteResult(diskPath, writeErr)
	}
	if !written {
		return nil
	}
	if err := d.observeWriteResult(diskPath, nil); err != nil {
		return err
	}
	for _, fn := range onWritten {
		fn(filePath, entry)
	}
	return nil
}

func (d *DiskCache) writeFile(
	ctx context.Context,
	diskPath string,
	openReader func(context.Context) (io.ReadCloser, error),
) (written bool, err error) {
	return d.writeFileWithFinalizeMode(ctx, diskPath, openReader, false)
}

func (d *DiskCache) writeFileWithFinalizeMode(
	ctx context.Context,
	diskPath string,
	openReader func(context.Context) (io.ReadCloser, error),
	asyncFinalize bool,
) (written bool, err error) {
	// evict if disk is full
	defer func() {
		if isDiskFull(err) {
			d.cache.ForceEvict(ctx, d.capacityFunc()/10)
		}
	}()

	cleanup := func() error { return d.removeUnindexedFile(diskPath) }
	var doneUpdate func() error
	if asyncFinalize {
		var ok bool
		doneUpdate, ok = d.tryStartUpdateWithCleanup(diskPath, cleanup)
		if !ok {
			return false, nil
		}
	} else {
		doneUpdate, err = d.startUpdateWithCleanupContext(ctx, diskPath, cleanup)
		if err != nil {
			return false, err
		}
	}
	updateOwned := true
	defer func() {
		if updateOwned {
			d.mergeWriteCleanupError(diskPath, &err, doneUpdate())
		}
	}()
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if _, ok := d.cache.Get(ctx, diskPath); ok {
		if _, err := os.Stat(diskPath); err == nil {
			// already exists
			return false, nil
		} else if os.IsNotExist(err) {
			// Repair the missing physical file and replace the existing index
			// after the rewrite so FIFO accounting uses the repaired file size.
		} else {
			return false, err
		}
	}
	stat, err := os.Stat(diskPath)
	if err == nil {
		// file exists
		d.cache.Set(ctx, diskPath, struct{}{}, fileSize(stat))
		return false, nil
	}
	asyncSlotOwned := false
	if asyncFinalize {
		if !d.tryReserveAsyncFileFinalize() {
			return false, nil
		}
		asyncSlotOwned = true
		defer func() {
			if asyncSlotOwned {
				d.releaseAsyncFileFinalizeReservation()
			}
		}()
	}

	// write data
	dir := filepath.Dir(diskPath)
	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return false, err
	}
	f, err := os.CreateTemp(dir, "*"+cacheFileTempSuffix)
	if err != nil {
		return false, err
	}
	fileOwned := true
	defer func() {
		if fileOwned {
			d.mergeWriteCleanupError(diskPath, &err, cleanupDiskCacheTempFile(f))
		}
	}()

	from, err := openReader(ctx)
	if err != nil {
		return false, &diskCacheSourceError{err: err}
	}
	defer from.Close()
	source := &diskCacheSourceReader{Reader: from}

	// do eviction before write
	forceEvict := int64(0)
	if file, ok := from.(*os.File); ok {
		// get file size
		info, err := file.Stat()
		if err == nil {
			forceEvict = fileSize(info)
		}
	}
	d.cache.Evict(ctx, nil, forceEvict)

	var buf []byte
	put := ioBufferPool.Get(&buf)
	defer put.Put()
	_, err = io.CopyBuffer(f, source, buf)
	if err != nil {
		if source.err != nil {
			return false, &diskCacheSourceError{err: source.err}
		}
		return false, err
	}

	if asyncFinalize {
		if d.scheduleReservedAsyncFileFinalize(diskPath, f, doneUpdate) {
			asyncSlotOwned = false
			fileOwned = false
			updateOwned = false
			return true, nil
		}
		// Source data has already been consumed for the foreground operation.
		// If the bounded finalization queue is full or closing, discard the
		// optional temp artifact instead of exposing disk latency to the caller.
		return false, nil
	}

	if err = d.finalizeFile(ctx, diskPath, f); err != nil {
		return false, err
	}
	fileOwned = false
	return true, nil
}

func (d *DiskCache) finalizeFile(
	ctx context.Context,
	diskPath string,
	f *os.File,
) error {
	if err := d.fileSync(f); err != nil {
		return err
	}
	fadviseDontNeed(f, 0, 0)

	stat, err := f.Stat()
	if err != nil {
		return err
	}
	size := fileSize(stat)
	tempPath := f.Name()

	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, diskPath); err != nil {
		return err
	}
	logutil.Debug("disk cache file written",
		zap.Any("path", diskPath),
	)

	if !d.cache.Replace(ctx, diskPath, struct{}{}, size) {
		d.cache.Set(ctx, diskPath, struct{}{}, size)
	}
	if !d.cache.Contains(diskPath) {
		if err := os.Remove(diskPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func (d *DiskCache) Flush(ctx context.Context) {
	d.flushAsyncUpdates(ctx)
}

const (
	cacheFileSuffix     = ".mofscache"
	cacheFileTempSuffix = cacheFileSuffix + ".tmp"
)

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

func (d *DiskCache) waitUpdateComplete(ctx context.Context, path string) bool {
	LogEvent(ctx, str_disk_cache_wait_update_complete_begin)
	defer LogEvent(ctx, str_disk_cache_wait_update_complete_end)
	d.updatingPaths.L.Lock()
	if d.updatingPaths.m[path] == nil {
		completed := ctx.Err() == nil
		d.updatingPaths.L.Unlock()
		return completed
	}
	stopWakeup := context.AfterFunc(ctx, func() {
		d.updatingPaths.L.Lock()
		d.updatingPaths.Broadcast()
		d.updatingPaths.L.Unlock()
	})
	for d.updatingPaths.m[path] != nil && ctx.Err() == nil {
		d.updatingPaths.Wait()
	}
	completed := d.updatingPaths.m[path] == nil
	d.updatingPaths.L.Unlock()
	stopWakeup()
	return completed
}

func (d *DiskCache) waitUpdateCompleteFor(ctx context.Context, path string, timeout time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if !d.isUpdating(path) {
		return true
	}
	if timeout <= 0 {
		return false
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return d.waitUpdateComplete(waitCtx, path)
}

func (d *DiskCache) isUpdating(path string) bool {
	d.updatingPaths.L.Lock()
	defer d.updatingPaths.L.Unlock()
	return d.updatingPaths.m[path] != nil
}

func (d *DiskCache) startUpdate(path string) (done func()) {
	doneWithError := d.startUpdateWithCleanup(path, nil)
	return func() {
		_ = doneWithError()
	}
}

func (d *DiskCache) startUpdateWithCleanup(path string, cleanup func() error) (done func() error) {
	done, err := d.startUpdateWithCleanupContext(context.Background(), path, cleanup)
	if err != nil {
		panic(err)
	}
	return done
}

func (d *DiskCache) startUpdateContext(ctx context.Context, path string) (done func(), err error) {
	doneWithError, err := d.startUpdateWithCleanupContext(ctx, path, nil)
	if err != nil {
		return nil, err
	}
	return func() {
		_ = doneWithError()
	}, nil
}

func (d *DiskCache) startUpdateWithCleanupContext(
	ctx context.Context,
	path string,
	cleanup func() error,
) (done func() error, err error) {
	d.updatingPaths.L.Lock()
	var stopWakeup func() bool
	if d.updatingPaths.m[path] != nil {
		stopWakeup = context.AfterFunc(ctx, func() {
			d.updatingPaths.L.Lock()
			d.updatingPaths.Broadcast()
			d.updatingPaths.L.Unlock()
		})
	}
	for d.updatingPaths.m[path] != nil && ctx.Err() == nil {
		d.updatingPaths.Wait()
	}
	if err := ctx.Err(); err != nil {
		d.updatingPaths.L.Unlock()
		if stopWakeup != nil {
			stopWakeup()
		}
		return nil, err
	}
	update := new(diskCachePathUpdate)
	d.updatingPaths.m[path] = update
	d.updatingPaths.L.Unlock()
	if stopWakeup != nil {
		stopWakeup()
	}
	var once sync.Once
	var cleanupErr error
	done = func() error {
		once.Do(func() {
			defer func() {
				d.updatingPaths.L.Lock()
				if d.updatingPaths.m[path] == update {
					delete(d.updatingPaths.m, path)
					d.updatingPaths.Broadcast()
				}
				d.updatingPaths.L.Unlock()
			}()
			if cleanup != nil {
				cleanupErr = cleanup()
			}
		})
		return cleanupErr
	}
	return done, nil
}

func (d *DiskCache) tryStartUpdate(path string) (done func(), ok bool) {
	d.updatingPaths.L.Lock()
	if d.updatingPaths.m[path] != nil {
		d.updatingPaths.L.Unlock()
		return nil, false
	}
	update := new(diskCachePathUpdate)
	d.updatingPaths.m[path] = update
	d.updatingPaths.L.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			d.updatingPaths.L.Lock()
			if d.updatingPaths.m[path] == update {
				delete(d.updatingPaths.m, path)
				d.updatingPaths.Broadcast()
			}
			d.updatingPaths.L.Unlock()
		})
	}, true
}

func (d *DiskCache) tryStartUpdateWithCleanup(
	path string,
	cleanup func() error,
) (done func() error, ok bool) {
	release, ok := d.tryStartUpdate(path)
	if !ok {
		return nil, false
	}
	var once sync.Once
	var cleanupErr error
	return func() error {
		once.Do(func() {
			if cleanup != nil {
				cleanupErr = cleanup()
			}
			release()
		})
		return cleanupErr
	}, true
}

func (d *DiskCache) removeUnindexedFile(path string) error {
	if d.cache.Contains(path) {
		return nil
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

var _ FileCache = new(DiskCache)

func (d *DiskCache) SetFile(
	ctx context.Context,
	filePath string,
	openReader func(context.Context) (io.ReadCloser, error),
) error {
	return d.setFile(ctx, filePath, openReader, false)
}

func (d *DiskCache) setFile(
	ctx context.Context,
	filePath string,
	openReader func(context.Context) (io.ReadCloser, error),
	asyncFinalize bool,
) error {
	path, err := ParsePath(filePath)
	if err != nil {
		return err
	}
	diskPath := d.pathForFile(path.File)
	written, writeErr := d.writeFileWithFinalizeMode(ctx, diskPath, openReader, asyncFinalize)
	if writeErr != nil {
		return d.observeWriteResult(diskPath, writeErr)
	}
	if !written {
		return nil
	}
	if asyncFinalize {
		// Admission is not a successful disk write. The worker that owns the
		// finalizer is the only authority allowed to report failure or recovery.
		return nil
	}
	return d.observeWriteResult(diskPath, nil)
}

func (d *DiskCache) DeletePaths(
	ctx context.Context,
	paths []string,
) (err error) {
	canonical, err := canonicalFilePaths(paths)
	if err != nil {
		return err
	}
	for _, path := range canonical {
		//TODO also delete IOEntry files
		if err = d.removeOnePath(ctx, path); err != nil {
			return
		}
	}

	return
}

func (d *DiskCache) removeOnePath(ctx context.Context, path string) (err error) {
	diskPath := d.pathForFile(path)
	doneUpdate, err := d.startUpdateContext(ctx, diskPath)
	if err != nil {
		return err
	}
	defer doneUpdate()
	if err = os.Remove(diskPath); err != nil {
		if !os.IsNotExist(err) {
			return
		}
		err = nil
	}
	d.cache.Delete(ctx, diskPath)
	return
}

func (d *DiskCache) Evict(ctx context.Context, done chan int64) {
	d.cache.Evict(ctx, done, 0)
}

func fileSize(info fs.FileInfo) int64 {
	if sys, ok := info.Sys().(*syscall.Stat_t); ok {
		return int64(sys.Blocks) * 512 // it's always 512, not sys.Blksize
	}
	return info.Size()
}

func (d *DiskCache) Close(ctx context.Context) {
	d.closeAsyncUpdates(ctx)
	allDiskCaches.Delete(d)
}
