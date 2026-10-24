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
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
	perfCounterSets    []*perfcounter.CounterSet

	updatingPaths struct {
		*sync.Cond
		m                  map[string]*diskCachePathUpdate
		readGenerations    map[string]diskCachePathReadGeneration
		nextReadGeneration uint64
	}

	cache        *fifocache.Cache[string, struct{}]
	capacityFunc fscache.CapacityFunc
	dirEntries   bool
	directoryMu  sync.RWMutex
	// diskFullEviction is held by the single eviction that responds to
	// disk-full write failures.
	diskFullEviction atomic.Bool
	// statFilesystem overrides statDiskCacheFilesystem in tests.
	statFilesystem func(path string) (diskCacheFilesystemStat, bool)

	async         diskCacheAsyncState
	writeFailures diskCacheWriteFailureState
	fileSync      func(*os.File) error
	load          struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	// beforeLoadDirectoryCleanup and beforeCacheTempFileCreate are test-only
	// phase barriers. Production instances leave them nil.
	beforeLoadDirectoryCleanup func(path string)
	beforeCacheTempFileCreate  func()
	// beforeFilePublication is a test-only phase barrier. Production instances
	// leave it nil, so the publication hot path pays only one predictable branch.
	beforeFilePublication func()
	// beforeCacheDirFileOpen is a test-only hook between the files of one entry open.
	beforeCacheDirFileOpen func()
}

// Keep this type non-zero-sized: pointers to distinct zero-sized allocations
// are not required to have distinct addresses in Go, but the path owner uses
// pointer identity as its generation token.
type diskCachePathUpdate struct {
	_               byte
	evictionPending bool
}

// readGenerations contains only the current generation while at least one Read
// owns an open file for the path. Path mutation removes it before changing the
// pathname; stale readers retain only the scalar id and cannot re-index it.
type diskCachePathReadGeneration struct {
	id      uint64
	readers int
}

type openedDiskCacheFile struct {
	file       *os.File
	generation uint64
}

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
	return newDiskCache(ctx, path, capacity, perfCounterSets, asyncLoad, cacheDataAllocator, name, metricScope, diskCacheOptions{})
}

type diskCacheOptions struct {
	// dirEntries makes directories named with cacheDirSuffix cache entries (IndexFileCache).
	dirEntries bool
	// sizeHintShare scales GlobalDiskCacheSizeHint for this cache; 0 means the whole hint.
	sizeHintShare float64
}

func newDiskCache(
	ctx context.Context,
	path string,
	capacity fscache.CapacityFunc,
	perfCounterSets []*perfcounter.CounterSet,
	asyncLoad bool,
	cacheDataAllocator CacheDataAllocator,
	name string,
	metricScope string,
	opts diskCacheOptions,
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
			if opts.sizeHintShare > 0 {
				return int64(float64(n) * opts.sizeHintShare)
			}
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
		dirEntries:   opts.dirEntries,
	}
	cache = fifocache.NewWithPrepareEvict(

		capacityFunc,

		func(key string) uint64 {
			return maphash.String(seed, key)
		},

		func(_ context.Context, _ string, _ struct{}, size int64, _ uint64) { // postSet
			inuseBytes.Add(float64(size))
			capacityBytes.Set(float64(capacityFunc()))
		},

		nil,
		func(path string, _ struct{}, _ int64, _ uint64) func() {
			doneUpdate, ok := ret.startEviction(path)
			if !ok {
				return nil
			}
			return func() {
				defer func() {
					if err := doneUpdate(); err != nil {
						logutil.Error("finish disk cache eviction",
							zap.Any("error", err),
						)
					}
				}()
				if err := ret.removeEvictedFile(path); err != nil {
					logutil.Error("delete disk cache file",
						zap.Any("error", err),
					)
				}
			}
		},
		func(_ context.Context, _ string, _ struct{}, size int64, _ uint64) {
			inuseBytes.Add(float64(-size))
			capacityBytes.Set(float64(capacityFunc()))
		},
	)
	ret.cache = cache
	ret.updatingPaths.Cond = sync.NewCond(new(sync.Mutex))
	ret.updatingPaths.m = make(map[string]*diskCachePathUpdate)
	ret.updatingPaths.readGenerations = make(map[string]diskCachePathReadGeneration)
	ret.initAsyncUpdates()
	ret.initWriteFailureState()
	loadCtx, loadCancel := context.WithCancel(ctx)
	ret.load.cancel = loadCancel
	ret.load.done = make(chan struct{})
	runLoad := func() {
		defer func() {
			loadCancel()
			close(ret.load.done)
		}()
		ret.loadCache(loadCtx)
	}

	if asyncLoad {
		go runLoad()
	} else {
		runLoad()
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
		Dir   bool
	}
	works := make(chan Info)

	numWorkers := runtime.NumCPU()
	wg := new(sync.WaitGroup)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case work, ok := <-works:
					if !ok {
						return
					}
					if work.Dir {
						size, err := cacheDirSize(work.Path)
						if err != nil {
							continue // ignore
						}
						d.cache.Set(ctx, work.Path, struct{}{}, size)
						continue
					}
					info, err := work.Entry.Info()
					if err != nil {
						continue // ignore
					}

					d.cache.Set(ctx, work.Path, struct{}{}, int64(fileSize(info)))
				}
			}
		}()
	}

	var numFiles, numCacheFiles, numTempFiles, numDeleted int

	_ = filepath.WalkDir(d.path, func(path string, entry os.DirEntry, err error) error {
		if ctx.Err() != nil {
			return fs.SkipAll
		}
		numFiles++
		if err != nil {
			return nil //ignore
		}

		if entry.IsDir() {
			if path == d.path {
				return nil
			}
			switch name := entry.Name(); {
			case !d.dirEntries:
			case strings.HasSuffix(name, cacheDirTrashSuffix):
				_ = os.RemoveAll(path)
				numDeleted++
				return fs.SkipDir
			case strings.HasSuffix(name, cacheDirTempSuffix):
				numTempFiles++
				info, err := entry.Info()
				if err == nil && time.Since(info.ModTime()) > time.Hour*8 {
					_ = os.RemoveAll(path)
					numDeleted++
				}
				return fs.SkipDir
			case strings.HasSuffix(name, cacheDirSuffix):
				numCacheFiles++
				select {
				case works <- Info{Path: path, Entry: entry, Dir: true}:
				case <-ctx.Done():
					return fs.SkipAll
				}
				return fs.SkipDir
			}
			d.removeEmptyDirectory(path)
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
		select {
		case works <- Info{
			Path: path, Entry: entry,
		}:
		case <-ctx.Done():
			return fs.SkipAll
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
	if ctx.Err() != nil {
		return
	}

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

	openedFiles := make(map[string]openedDiskCacheFile)
	// All path waits in one Read share a single budget. Start it lazily on the
	// first contended path so uncontended reads do not pay for a timer.
	var waitDeadline time.Time
	defer func() {
		LogEvent(ctx, str_close_disk_files_begin)
		for diskPath, opened := range openedFiles {
			_ = opened.file.Close()
			d.finishPathRead(diskPath, opened.generation)
		}
		LogEvent(ctx, str_close_disk_files_end)
	}()
	openPath := func(diskPath string) (opened openedDiskCacheFile, newlyOpened bool, ok bool) {
		if opened, ok = openedFiles[diskPath]; ok {
			return opened, false, true
		}
		generation, ok := d.startPathReadWithin(
			ctx,
			diskPath,
			&waitDeadline,
			shortIOWaitDuration,
		)
		if !ok {
			return openedDiskCacheFile{}, false, false
		}
		LogEvent(ctx, str_disk_cache_file_open_begin)
		diskFile, err := os.Open(diskPath)
		LogEvent(ctx, str_disk_cache_file_open_end)
		if err != nil {
			d.finishPathRead(diskPath, generation)
			return openedDiskCacheFile{}, false, false
		}
		opened = openedDiskCacheFile{
			file:       diskFile,
			generation: generation,
		}
		openedFiles[diskPath] = opened
		return opened, true, true
	}

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

		var opened openedDiskCacheFile
		var openedOK bool

		// entry file
		diskPath := d.pathForIOEntry(path.File, *entry)
		if f, newlyOpened, ok := openPath(diskPath); ok {
			if newlyOpened {
				opened = f
				openedOK = true
				numOpenIOEntry++
			} else {
				LogEvent(ctx, str_disk_cache_file_seek_begin)
				// An IOEntry cache file contains only this range, so its content
				// always starts at file offset zero. entry.Offset is only valid for
				// a full-object cache file.
				_, err = f.file.Seek(0, io.SeekStart)
				LogEvent(ctx, str_disk_cache_file_seek_end)
				if err == nil {
					opened = f
					openedOK = true
				}
			}
		}

		if !openedOK {
			// try full file
			diskPath = d.pathForFile(path.File)
			if f, newlyOpened, ok := openPath(diskPath); ok {
				if newlyOpened {
					numOpenFull++
				}
				LogEvent(ctx, str_disk_cache_file_seek_begin)
				_, err = f.file.Seek(entry.Offset, io.SeekStart)
				LogEvent(ctx, str_disk_cache_file_seek_end)
				if err == nil {
					opened = f
					openedOK = true
				}
			}
		}

		if !openedOK {
			// no file available
			return nil
		}
		file := opened.file

		LogEvent(ctx, str_disk_cache_update_states_begin)
		if _, ok := d.cache.Get(ctx, diskPath); !ok {
			// set cache
			LogEvent(ctx, str_disk_cache_file_stat_begin)
			stat, err := file.Stat()
			LogEvent(ctx, str_disk_cache_file_stat_end)
			if err != nil {
				return err
			}
			if doneReindex, ok := d.tryStartReadReindex(diskPath, opened.generation); ok {
				var reindexErr error
				func() {
					defer func() { reindexErr = doneReindex() }()
					d.cache.Set(ctx, diskPath, struct{}{}, fileSize(stat))
				}()
				if reindexErr != nil {
					return reindexErr
				}
			}
		}
		LogEvent(ctx, str_disk_cache_update_states_end)

		readOffset, readSize := int64(0), entry.Size
		if diskPath == d.pathForFile(path.File) {
			readOffset = entry.Offset
		}
		if err := entry.ReadFromOSFile(ctx, file, d.cacheDataAllocator); err != nil {
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
	written, err := d.writeEntry(ctx, diskPath, entry, nil)
	if err != nil || !written {
		return err
	}
	for _, fn := range onWritten {
		fn(filePath, entry)
	}
	return nil
}

func (d *DiskCache) writeEntry(
	ctx context.Context,
	diskPath string,
	entry IOEntry,
	claimPublication func() bool,
) (bool, error) {
	written, writeErr := d.writeFileWithFinalizeMode(ctx, diskPath, func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(entry.Data)), nil
	}, false, claimPublication)
	if writeErr != nil {
		return false, d.observeWriteResult(diskPath, writeErr)
	}
	if !written {
		return false, nil
	}
	if err := d.observeWriteResult(diskPath, nil); err != nil {
		return false, err
	}
	return true, nil
}

func (d *DiskCache) writeFile(
	ctx context.Context,
	diskPath string,
	openReader func(context.Context) (io.ReadCloser, error),
) (written bool, err error) {
	return d.writeFileWithFinalizeMode(ctx, diskPath, openReader, false, nil)
}

func (d *DiskCache) removeEmptyDirectory(path string) {
	if d.beforeLoadDirectoryCleanup != nil {
		d.beforeLoadDirectoryCleanup(path)
	}
	d.directoryMu.Lock()
	defer d.directoryMu.Unlock()
	// os.Remove will not delete a non-empty directory.
	_ = os.Remove(path)
}

type diskCacheFilesystemStat struct {
	availableBytes uint64
	totalInodes    uint64
	freeInodes     uint64
}

// evictForDiskFull frees cache space after a cache write failed because the
// disk is full.
func (d *DiskCache) evictForDiskFull(ctx context.Context, err error) {
	var sourceErr *diskCacheSourceError
	if errors.As(err, &sourceErr) {
		// Reading the source failed; the cache filesystem is not involved.
		return
	}
	kind := classifyDiskFull(err)
	if kind == notDiskFull {
		return
	}
	n := d.capacityFunc() / 10
	if n <= 0 {
		return
	}
	// Concurrent failures observe the same shortage. Each additional eviction
	// would cut another n bytes before the first one has freed anything, so
	// only one runs at a time, and the shortage is checked inside it so that a
	// failure observed before a completed eviction does not evict again.
	if !d.diskFullEviction.CompareAndSwap(false, true) {
		return
	}
	defer d.diskFullEviction.Store(false)
	if kind == diskNoSpace && !d.filesystemExhausted(n) {
		// ENOSPC while blocks and inodes are available comes from a structural
		// limit such as a full directory index. Evicting cannot fix that and
		// would only discard cached data on every failed write.
		return
	}
	d.cache.ForceEvictWithWait(ctx, n)
}

func (d *DiskCache) filesystemExhausted(wantBytes int64) bool {
	statFilesystem := statDiskCacheFilesystem
	if d.statFilesystem != nil {
		statFilesystem = d.statFilesystem
	}
	stat, ok := statFilesystem(d.path)
	if !ok {
		// Unknown: keep evicting, as before ENOSPC was distinguished.
		return true
	}
	if stat.availableBytes < uint64(wantBytes) {
		return true
	}
	// Filesystems with dynamic inode allocation report zero total inodes.
	// Cross-multiply instead of dividing totalInodes by 100: on a filesystem
	// with under 100 inodes total, that division truncates to zero and
	// freeInodes < 0 can never be true, so zero free inodes would never be
	// recognized as exhaustion.
	return stat.totalInodes > 0 && stat.freeInodes*100 < stat.totalInodes
}

func (d *DiskCache) createCacheTempFile(dir string) (*os.File, error) {
	d.directoryMu.RLock()
	defer d.directoryMu.RUnlock()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	if d.beforeCacheTempFileCreate != nil {
		d.beforeCacheTempFileCreate()
	}
	return os.CreateTemp(dir, "*"+cacheFileTempSuffix)
}

func (d *DiskCache) writeFileWithFinalizeMode(
	ctx context.Context,
	diskPath string,
	openReader func(context.Context) (io.ReadCloser, error),
	asyncFinalize bool,
	claimPublication func() bool,
) (written bool, err error) {
	defer func() {
		d.evictForDiskFull(ctx, err)
	}()
	asyncSlotOwned := false
	if asyncFinalize {
		if !d.tryReserveAsyncFileFinalize(diskPath) {
			return false, nil
		}
		asyncSlotOwned = true
		defer func() {
			if asyncSlotOwned {
				d.releaseAsyncFileFinalizeReservation(diskPath)
			}
		}()
	}

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

	// write data
	f, err := d.createCacheTempFile(filepath.Dir(diskPath))
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
		d.scheduleReservedAsyncFileFinalize(diskPath, f, doneUpdate)
		asyncSlotOwned = false
		fileOwned = false
		updateOwned = false
		return true, nil
	}

	if err = d.finalizeFile(ctx, diskPath, f, claimPublication); err != nil {
		return false, err
	}
	fileOwned = false
	return true, nil
}

func (d *DiskCache) finalizeFile(
	ctx context.Context,
	diskPath string,
	f *os.File,
	claimPublication func() bool,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.fileSync(f); err != nil {
		return err
	}
	// Sync is an uninterruptible syscall on the supported platforms. Recheck
	// cancellation after it returns so Close cannot publish an async cache file
	// after the cache has entered its terminal generation.
	if err := ctx.Err(); err != nil {
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
	if d.beforeFilePublication != nil {
		d.beforeFilePublication()
	}
	if claimPublication != nil {
		if !claimPublication() {
			return context.Canceled
		}
	} else if err := ctx.Err(); err != nil {
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
	// OnWritten callbacks are post-publication notifications and deliberately
	// outside this barrier, so a callback may safely reenter Flush or Close.
	d.flushAsyncUpdates(ctx)
}

const (
	cacheFileSuffix     = ".mofscache"
	cacheFileTempSuffix = cacheFileSuffix + ".tmp"
)

// Cache files are spread over diskCacheShardCount sub-directories so that no
// single directory accumulates every cached object: filesystems cap the
// entries of one directory independently of free space (ext4 without
// large_dir fails with ENOSPC once a directory's htree index is full).
//
// The shard must be stable across processes and versions, otherwise a restart
// or upgrade would no longer find existing cache files. Do not change the hash
// or the shard count without handling the previous layout.
const (
	diskCacheShardHexDigits = 3
	diskCacheShardCount     = 1 << (4 * diskCacheShardHexDigits)
)

func diskCacheShard(path string) string {
	// FNV-1a, 32 bit.
	h := uint32(2166136261)
	for i := 0; i < len(path); i++ {
		h ^= uint32(path[i])
		h *= 16777619
	}
	h %= diskCacheShardCount
	const digits = "0123456789abcdef"
	var shard [diskCacheShardHexDigits]byte
	for i := len(shard) - 1; i >= 0; i-- {
		shard[i] = digits[h&0xf]
		h >>= 4
	}
	return string(shard[:])
}

func isDiskCacheShard(name string) bool {
	if len(name) != diskCacheShardHexDigits {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func (d *DiskCache) pathForIOEntry(path string, entry IOEntry) string {
	if entry.Size < 0 {
		panic("should not cache size -1 entry")
	}
	// Different spellings of the same object path (for example "foo/./bar"
	// and "foo/bar") must land on the same disk path, or a delete issued
	// with one spelling leaves a stale entry reachable under another.
	// filepath.Join below already cleans the "full..."/"N-N..." component,
	// so the shard has to be derived from the same cleaned form or the two
	// spellings would be sharded apart even though the filename collides.
	path = pathpkg.Clean(path)
	return filepath.Join(
		d.path,
		diskCacheShard(path),
		fmt.Sprintf("%d-%d%s%s", entry.Offset, entry.Size, toOSPath(path), cacheFileSuffix),
	)
}

func (d *DiskCache) pathForFile(path string) string {
	path = pathpkg.Clean(path)
	return filepath.Join(
		d.path,
		diskCacheShard(path),
		fmt.Sprintf("full%s%s", toOSPath(path), cacheFileSuffix),
	)
}

// legacyPathForFile reproduces the pre-sharding on-disk name for path, i.e.
// what pathForFile returned before shard directories were introduced. It is
// never written to going forward; it exists solely so removeOnePath can find
// and invalidate a leftover legacy-layout cache file on delete.
func (d *DiskCache) legacyPathForFile(path string) string {
	return filepath.Join(
		d.path,
		fmt.Sprintf("full%s%s", toOSPath(path), cacheFileSuffix),
	)
}

var ErrNotCacheFile = errorStr("not a cache file")

// decodeFilePath accepts both the sharded layout and the unsharded layout
// written by earlier versions, which may still be present after an upgrade.
func (d *DiskCache) decodeFilePath(diskPath string) (string, error) {
	path, err := filepath.Rel(d.path, diskPath)
	if err != nil {
		return "", err
	}
	if shard, rest, ok := strings.Cut(path, string(filepath.Separator)); ok && isDiskCacheShard(shard) {
		path = rest
	}
	if !strings.HasPrefix(path, "full") {
		return "", ErrNotCacheFile
	}
	path = strings.TrimPrefix(path, "full")
	path = strings.TrimSuffix(path, cacheFileSuffix)
	return fromOSPath(path), nil
}

func (d *DiskCache) startPathReadWithin(
	ctx context.Context,
	path string,
	deadline *time.Time,
	timeout time.Duration,
) (
	generation uint64,
	ok bool,
) {
	LogEvent(ctx, str_disk_cache_wait_update_complete_begin)
	defer LogEvent(ctx, str_disk_cache_wait_update_complete_end)
	d.updatingPaths.L.Lock()
	if d.updatingPaths.m[path] == nil {
		completed := ctx.Err() == nil
		if completed {
			generation = d.addPathReaderLocked(path)
		}
		d.updatingPaths.L.Unlock()
		if !completed {
			return 0, false
		}
		return generation, true
	}
	if timeout <= 0 {
		d.updatingPaths.L.Unlock()
		return 0, false
	}
	if deadline.IsZero() {
		*deadline = time.Now().Add(timeout)
	}
	if !time.Now().Before(*deadline) {
		d.updatingPaths.L.Unlock()
		return 0, false
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithDeadline(ctx, *deadline)
	defer cancel()
	stopWakeup := context.AfterFunc(ctx, func() {
		d.updatingPaths.L.Lock()
		d.updatingPaths.Broadcast()
		d.updatingPaths.L.Unlock()
	})
	for d.updatingPaths.m[path] != nil && ctx.Err() == nil {
		d.updatingPaths.Wait()
	}
	completed := d.updatingPaths.m[path] == nil && ctx.Err() == nil
	if completed {
		generation = d.addPathReaderLocked(path)
	}
	d.updatingPaths.L.Unlock()
	stopWakeup()
	if !completed {
		return 0, false
	}
	return generation, true
}

func (d *DiskCache) addPathReaderLocked(path string) uint64 {
	generation, ok := d.updatingPaths.readGenerations[path]
	if !ok {
		d.updatingPaths.nextReadGeneration++
		generation.id = d.updatingPaths.nextReadGeneration
	}
	generation.readers++
	d.updatingPaths.readGenerations[path] = generation
	return generation.id
}

func (d *DiskCache) finishPathRead(
	path string,
	generationID uint64,
) {
	d.updatingPaths.L.Lock()
	if generation, ok := d.updatingPaths.readGenerations[path]; ok && generation.id == generationID {
		generation.readers--
		if generation.readers == 0 {
			delete(d.updatingPaths.readGenerations, path)
		} else {
			d.updatingPaths.readGenerations[path] = generation
		}
	}
	d.updatingPaths.L.Unlock()
}

func (d *DiskCache) invalidatePathReadsLocked(path string) {
	delete(d.updatingPaths.readGenerations, path)
}

func (d *DiskCache) tryStartReadReindex(
	path string,
	generationID uint64,
) (done func() error, ok bool) {
	d.updatingPaths.L.Lock()
	generation, generationOK := d.updatingPaths.readGenerations[path]
	if !generationOK || generation.id != generationID || d.updatingPaths.m[path] != nil {
		d.updatingPaths.L.Unlock()
		return nil, false
	}
	update := new(diskCachePathUpdate)
	d.updatingPaths.m[path] = update
	d.updatingPaths.L.Unlock()
	return d.newPathUpdateDone(path, update, nil), true
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
	d.invalidatePathReadsLocked(path)
	update := new(diskCachePathUpdate)
	d.updatingPaths.m[path] = update
	d.updatingPaths.L.Unlock()
	if stopWakeup != nil {
		stopWakeup()
	}
	return d.newPathUpdateDone(path, update, cleanup), nil
}

func (d *DiskCache) tryStartUpdateWithCleanup(
	path string,
	cleanup func() error,
) (done func() error, ok bool) {
	d.updatingPaths.L.Lock()
	if d.updatingPaths.m[path] != nil {
		d.updatingPaths.L.Unlock()
		return nil, false
	}
	d.invalidatePathReadsLocked(path)
	update := new(diskCachePathUpdate)
	d.updatingPaths.m[path] = update
	d.updatingPaths.L.Unlock()
	return d.newPathUpdateDone(path, update, cleanup), true
}

// startEviction either owns the path cleanup or transfers that responsibility
// to the current path owner. Both decisions happen while holding the same lock,
// so eviction cannot be lost between observing and releasing a generation.
func (d *DiskCache) startEviction(path string) (done func() error, ok bool) {
	d.updatingPaths.L.Lock()
	d.invalidatePathReadsLocked(path)
	if update := d.updatingPaths.m[path]; update != nil {
		update.evictionPending = true
		d.updatingPaths.L.Unlock()
		return nil, false
	}
	update := new(diskCachePathUpdate)
	d.updatingPaths.m[path] = update
	d.updatingPaths.L.Unlock()
	return d.newPathUpdateDone(path, update, nil), true
}

func (d *DiskCache) newPathUpdateDone(
	path string,
	update *diskCachePathUpdate,
	cleanup func() error,
) func() error {
	var once sync.Once
	var cleanupErr error
	return func() error {
		once.Do(func() {
			defer func() {
				cleanupErr = errors.Join(cleanupErr, d.finishPathUpdate(path, update))
			}()
			if cleanup != nil {
				cleanupErr = cleanup()
			}
		})
		return cleanupErr
	}
}

func (d *DiskCache) finishPathUpdate(path string, update *diskCachePathUpdate) (err error) {
	for {
		d.updatingPaths.L.Lock()
		if d.updatingPaths.m[path] != update {
			d.updatingPaths.L.Unlock()
			return err
		}
		if !update.evictionPending {
			delete(d.updatingPaths.m, path)
			d.updatingPaths.Broadcast()
			d.updatingPaths.L.Unlock()
			return err
		}
		update.evictionPending = false
		d.updatingPaths.L.Unlock()

		err = errors.Join(err, d.removeEvictedFile(path))
	}
}

func (d *DiskCache) removeEvictedFile(path string) error {
	if d.cache.Contains(path) {
		return nil
	}
	if err := d.removeCachePath(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	metric.FSDiskCacheEvictCounter.Add(1)
	return nil
}

func (d *DiskCache) removeUnindexedFile(path string) error {
	if d.cache.Contains(path) {
		return nil
	}
	if err := d.removeCachePath(path); err != nil && !os.IsNotExist(err) {
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
	written, writeErr := d.writeFileWithFinalizeMode(ctx, diskPath, openReader, asyncFinalize, nil)
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

	// A pre-upgrade binary may have left a legacy, unsharded cache file for this
	// path (see the sharding comment on pathForFile). Legacy files are never
	// migrated, but they must still be invalidated on delete: otherwise
	// restoreFromDiskCache's directory walk (triggered by -fs-fix-missing /
	// MO_FS_FIX_MISSING) can later read the orphaned legacy file back and
	// resurrect data that was explicitly deleted.
	if legacyPath := d.legacyPathForFile(path); legacyPath != diskPath {
		if err = os.Remove(legacyPath); err != nil {
			if !os.IsNotExist(err) {
				return
			}
			err = nil
		}
		d.cache.Delete(ctx, legacyPath)
	}

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
	// A canceled ctx means terminal draining is incomplete. Callers must not
	// reuse the cache directory for a new DiskCache generation until a later
	// Close returns with a live context.
	d.load.cancel()
	d.closeAsyncUpdates(ctx)
	select {
	case <-d.load.done:
	case <-ctx.Done():
	}
	allDiskCaches.Delete(d)
}
