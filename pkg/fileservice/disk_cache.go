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
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/lrucache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type DiskCache struct {
	path               string
	cacheDataAllocator CacheDataAllocator
	perfCounterSets    []*perfcounter.CounterSet

	paths pathOwners

	cache        *lrucache.Cache[string, struct{}]
	capacityFunc fscache.CapacityFunc
	preloads     *preloadUsage
	ranges       cachedRanges
	dirEntries   bool
	// held counts the holds on entry directories.
	held dirHolds
	// trash holds the entry directories whose removal failed.
	trash       cacheTrash
	directoryMu sync.RWMutex
	// diskFullEviction is held by the single eviction that responds to
	// disk-full write failures.
	diskFullEviction atomic.Bool
	prefetchRange    prefetchRangeConfig
	objectSplit      objectSplitConfig
	// memoryPrefetchMax is the most bytes of a MemoryPrefetchObject.
	memoryPrefetchMax int64
	prefetched        prefetchedObjects
	// server makes the requests of fills; nil when the cache has no object storage.
	server          *objectStorageServer
	prefetchObjects prefetchObjects
	// wholeFills counts whole-object fills in progress; wholeFileMax is the size of
	// the largest whole-object file written.
	wholeFills   atomic.Int64
	wholeFileMax atomic.Int64
	// wholePublications holds, by range file, the whole-object fills publishing it.
	wholePublications sync.Map
	// statFilesystem overrides statDiskCacheFilesystem in tests.
	statFilesystem func(path string) (diskCacheFilesystemStat, bool)

	async         diskCacheAsyncState
	writeFailures diskCacheWriteFailureState
	fileSync      func(*os.File) error
	load          struct {
		cancel context.CancelFunc
		done   chan struct{}
	}
	hooks diskCacheHooks
	// deletes counts the deletes of objects; see objectDeletes.
	deletes objectDeletes
}

// diskCacheHooks are test-only phase barriers; production instances leave them nil.
type diskCacheHooks struct {
	beforeLoadDirectoryCleanup func(path string)
	beforeCacheTempFileCreate  func()
	// beforeIndexLoadedDirs runs before the entry directories found by the load are
	// indexed, beforeLoadFile before a file found by the load is, and
	// beforeRemoveUnlistedDir before an entry directory the load cannot list is removed.
	beforeIndexLoadedDirs   func()
	beforeLoadFile          func(diskPath string)
	beforeRemoveUnlistedDir func(path string)
	// beforeFilePublication, afterPublicationClaim and afterFilePublication run before a
	// file's publication claim, between it and its caching, and after its caching.
	beforeFilePublication func()
	afterPublicationClaim func()
	afterFilePublication  func()
	// beforeCacheDirFileOpen runs between the files of one entry open.
	beforeCacheDirFileOpen func()
	// beforeEvictionStart runs between an entry leaving the index and its eviction
	// starting.
	beforeEvictionStart func(path string)
}

func runHook(hook func()) {
	if hook != nil {
		hook()
	}
}

func runPathHook(hook func(path string), path string) {
	if hook != nil {
		hook(path)
	}
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
	return newDiskCache(ctx, path, capacity, perfCounterSets, asyncLoad, cacheDataAllocator, name, metricScope,
		diskCacheOptions{})
}

type diskCacheOptions struct {
	// dirEntries makes directories named with cacheDirSuffix cache entries
	// (IndexFileCache), never evicted while held, and whose
	// capacity does not follow GlobalDiskCacheSizeHint.
	dirEntries bool
	// prefetchRange sets the prefetch ranges of range reads.
	prefetchRange prefetchRangeConfig
	// objectSplit sets the parts a whole-object download reads at once.
	objectSplit objectSplitConfig
	// memoryPrefetchMax is the most bytes of a MemoryPrefetchObject; 0 is
	// DefaultDiskPrefetchMemoryMax.
	memoryPrefetchMax int64
	// openFilesMax is the most cache files kept open for later reads; 0 is
	// DefaultDiskOpenFilesMax, and less keeps none.
	openFilesMax int
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

	inuseBytes, capacityBytes := metric.GetFsCacheBytesGaugeWithScope(metricScope, name, "disk")
	capacityBytes.Set(float64(capacity()))

	capacityFunc := func() int64 {
		// read from global size hint
		if n := GlobalDiskCacheSizeHint.Load(); n > 0 && !opts.dirEntries {
			return n
		}
		// fallback
		c := max(capacity()-ret.trash.bytes.Load(), 0)
		if l := ret.trash.limit.Load(); l >= 0 {
			c = min(c, l)
		}
		return c
	}

	ret = &DiskCache{
		path:               path,
		cacheDataAllocator: cacheDataAllocator,
		perfCounterSets:    perfCounterSets,
		fileSync:           func(file *os.File) error { return file.Sync() },

		capacityFunc:      capacityFunc,
		dirEntries:        opts.dirEntries,
		prefetchRange:     opts.prefetchRange,
		objectSplit:       opts.objectSplit,
		memoryPrefetchMax: cmp.Or(opts.memoryPrefetchMax, DefaultDiskPrefetchMemoryMax),
		preloads:          newPreloadUsage(),
	}
	ret.trash.limit.Store(-1)

	postSet := func(_ context.Context, path string, _ struct{}, size int64, _ uint64) {
		// The file of a whole object is the one its fill publishes.
		if v, ok := ret.wholePublications.Load(path); ok {
			pub := v.(wholePublication)
			o := pub.download
			ret.preloads.cachedDownload(path, pub.key, o)
			if n := o.creditTo(func(n int64) { ret.preloads.readDownload(path, o, n) }); n > 0 {
				ret.preloads.readDownload(path, o, n)
			}
		}
		ret.ranges.add(ret.path, path)
		inuseBytes.Add(float64(size))
		capacityBytes.Set(float64(capacityFunc()))
	}
	prepareEvict := func(path string, _ struct{}, size int64, _ uint64) func() {
		runPathHook(ret.hooks.beforeEvictionStart, path)
		owner, ok := ret.paths.evict(path, size)
		if !ok {
			return nil
		}
		return func() {
			defer func() {
				if err := owner.release(); err != nil {
					logutil.Error("finish disk cache eviction",
						zap.Any("error", err),
					)
				}
			}()
			if err := ret.removeEvictedFile(path, size); err != nil {
				logutil.Error("delete disk cache file",
					zap.Any("error", err),
				)
			}
		}
	}
	postEvict := func(_ context.Context, path string, _ struct{}, size int64, _ uint64) {
		ret.preloads.removed(path, size)
		ret.ranges.remove(path)
		// A read may have cached the file again since it was evicted.
		if ret.cache.Contains(path) {
			ret.ranges.add(ret.path, path)
		}
		inuseBytes.Add(float64(-size))
		capacityBytes.Set(float64(capacityFunc()))
	}
	ret.cache = lrucache.NewWithPrepareEvict(capacityFunc, postSet, nil, prepareEvict, postEvict)
	if opts.dirEntries {
		ret.cache.SetEvictable(func(path string) bool { return !ret.dirHeld(path) })
	}
	ret.paths.init(opts.openFilesMax, ret.removeEvictedFile)
	ret.initAsyncUpdates()
	ret.initWriteFailureState()
	loadCtx, loadCancel := context.WithCancel(ctx)
	ret.load.cancel = loadCancel
	ret.load.done = make(chan struct{})
	ret.deletes.setLoading(true)
	runLoad := func() {
		defer func() {
			ret.deletes.setLoading(false)
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

// loadFile indexes the cache file at diskPath found at startup, unless its object was
// deleted since the cache was created: the delete may have missed a file not yet
// indexed, so the file is removed instead (see removeLoadedFile). As for the publication
// of an entry, a delete counted later finds the file indexed.
func (d *DiskCache) loadFile(ctx context.Context, diskPath string, info fs.FileInfo) {
	objectPath := diskPath
	if key, _, _, ok := parseRangeFile(d.path, diskPath, info.Size()); ok {
		objectPath = key
	}
	runPathHook(d.hooks.beforeLoadFile, diskPath)
	if !d.indexLoadedFile(ctx, objectPath, diskPath, info) {
		d.removeLoadedFile(ctx, diskPath, info)
	}
}

// indexLoadedFile indexes the file at diskPath found at startup and reports true, unless
// the object at objectPath was deleted since the cache was created.
func (d *DiskCache) indexLoadedFile(ctx context.Context, objectPath, diskPath string, info fs.FileInfo) bool {
	stripe := d.deletes.stripe(objectPath)
	stripe.RLock()
	defer stripe.RUnlock()
	if d.deletes.since(objectPath, 0) {
		return false
	}
	d.cache.SetOldest(ctx, diskPath, struct{}{}, int64(fileSize(info)))
	return true
}

// indexFoundFile indexes the file at diskPath the cache does not hold and reports true,
// unless its object was deleted since the cache was created.
func (d *DiskCache) indexFoundFile(ctx context.Context, diskPath string, info fs.FileInfo) bool {
	objectPath := diskPath
	if key, _, _, ok := parseRangeFile(d.path, diskPath, info.Size()); ok {
		objectPath = key
	}
	stripe := d.deletes.stripe(objectPath)
	stripe.RLock()
	defer stripe.RUnlock()
	if d.deletes.since(objectPath, 0) {
		return false
	}
	d.cache.Set(ctx, diskPath, struct{}{}, fileSize(info))
	return true
}

// removeLoadedFile removes the file at diskPath found at startup while, under the owner of
// the path, it is not indexed and is still the file info describes.
func (d *DiskCache) removeLoadedFile(ctx context.Context, diskPath string, info fs.FileInfo) {
	owner, err := d.paths.acquire(ctx, diskPath, nil)
	if err != nil {
		return
	}
	defer owner.release()
	if d.cache.Contains(diskPath) {
		return
	}
	if cur, err := os.Lstat(diskPath); err != nil || !os.SameFile(info, cur) {
		return
	}
	_ = os.Remove(diskPath)
}

// reindexRead indexes the cache file at diskPath that a read opened and the cache does
// not hold. As in loadFile, a file of an object deleted since the cache was created is
// neither read nor indexed: a delete during the load may have missed it. It reports
// whether the file may be read.
func (d *DiskCache) reindexRead(
	ctx context.Context,
	diskPath string,
	objectPath string,
	generation uint64,
	file *os.File,
) (readable bool, err error) {
	stripe := d.deletes.stripe(objectPath)
	stripe.RLock()
	defer stripe.RUnlock()
	if d.deletes.since(objectPath, 0) {
		return false, nil
	}
	LogEvent(ctx, str_disk_cache_file_stat_begin)
	stat, err := file.Stat()
	LogEvent(ctx, str_disk_cache_file_stat_end)
	if err != nil {
		return false, err
	}
	owner, ok := d.paths.reindex(diskPath, generation)
	if !ok {
		return true, nil
	}
	defer func() {
		if doneErr := owner.release(); err == nil {
			err = doneErr
		}
	}()
	d.cache.Set(ctx, diskPath, struct{}{}, fileSize(stat))
	return true, nil
}

func (d *DiskCache) loadCache(ctx context.Context) {
	t0 := time.Now()
	d.startLoading()

	type Info struct {
		Path  string
		Entry os.DirEntry
		Dir   bool
	}
	works := make(chan Info)

	var (
		dirsMu sync.Mutex
		dirs   []loadedDir
	)

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
						if os.IsPermission(err) {
							d.removeUnlistedDir(ctx, work.Path)
							continue
						}
						if err != nil {
							continue // ignore
						}
						info, err := work.Entry.Info()
						if err != nil {
							continue // ignore
						}
						d.loadingDir(work.Path, size)
						dirsMu.Lock()
						dirs = append(dirs, loadedDir{path: work.Path, size: size, lastUse: info.ModTime()})
						dirsMu.Unlock()
						continue
					}
					info, err := work.Entry.Info()
					if err != nil {
						continue // ignore
					}
					d.loadFile(ctx, work.Path, info)
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
				_ = d.removeTrash(path, trashSize(path))
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
			if d.isOldWholeFile(path) {
				_ = os.Remove(path)
				numDeleted++
				return nil
			}
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
	if len(dirs) > 0 {
		d.indexLoadedDirs(ctx, dirs)
	}
	d.loadingDir("", 0)

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

// isOldWholeFile reports whether the file at diskPath is a whole-object cache file of an
// earlier version: "full" and the object path, in a shard directory or the cache's.
func (d *DiskCache) isOldWholeFile(diskPath string) bool {
	rel, err := filepath.Rel(d.path, diskPath)
	if err != nil {
		return false
	}
	if shard, rest, ok := strings.Cut(rel, string(filepath.Separator)); ok && isDiskCacheShard(shard) {
		rel = rest
	}
	return strings.HasPrefix(rel, "full") && strings.HasSuffix(rel, cacheFileSuffix)
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

	var numHit, numRead, numOpenIOEntry, numError int64
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
			d.paths.close(diskPath, opened)
		}
		LogEvent(ctx, str_close_disk_files_end)
	}()
	openPath := func(diskPath string) (opened openedDiskCacheFile, newlyOpened bool, ok bool) {
		if opened, ok = openedFiles[diskPath]; ok {
			return opened, false, true
		}
		generation, ok := d.paths.startRead(ctx, diskPath, &waitDeadline)
		if !ok {
			return openedDiskCacheFile{}, false, false
		}
		opened, ok = d.paths.open(ctx, diskPath, generation)
		if !ok {
			d.paths.endRead(diskPath, generation)
			if d.cache.Contains(diskPath) {
				d.cachedFileExists(ctx, diskPath)
			}
			return openedDiskCacheFile{}, false, false
		}
		openedFiles[diskPath] = opened
		return opened, true, true
	}

	var key string
	objectKey := func() string {
		if key == "" {
			key = d.objectKey(path.File)
		}
		return key
	}

	fillEntry := func(entry *IOEntry) error {
		LogEvent(ctx, str_disk_cache_fill_entry_begin)
		defer LogEvent(ctx, str_disk_cache_fill_entry_end)

		if entry.done {
			return nil
		}
		if entry.Size == 0 {
			return &failingReadError{err: moerr.NewEmptyRangeNoCtx(path.File)}
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
				numOpenIOEntry++
			}
			opened = f
			openedOK = true
		}

		var downloading bool
		// The reads of a download join no other, nor does a large streamed entry.
		if !openedOK && !isDownload(ctx) && entry.materializes(d.memoryPrefetchMax) {
			var filled bool
			downloading, filled, err = d.readFromDownload(ctx, objectKey(), vector.indexTier, entry, vector)
			if err != nil {
				return err
			}
			if filled {
				entry.done = true
				entry.fromCache = d
				numHit++
				return nil
			}
		}

		// try a cached range that contains the entry; rangeOffset is where the entry
		// starts in it
		var rangeOffset int64
		fromRange := false
		for tried := []string(nil); !openedOK; {
			r, ok := d.ranges.containing(objectKey(), entry.Offset, entry.Size, tried)
			if !ok {
				break
			}
			tried = append(tried, r.diskPath)
			if !d.cache.Contains(r.diskPath) {
				d.ranges.remove(r.diskPath)
				continue
			}
			f, newlyOpened, ok := openPath(r.diskPath)
			if !ok {
				continue
			}
			if newlyOpened {
				numOpenIOEntry++
			}
			diskPath, opened, openedOK, fromRange = r.diskPath, f, true, true
			rangeOffset = entry.Offset - r.offset
		}

		// try cached ranges that hold the entry together
		if !openedOK {
			if rs, ok := d.ranges.covering(objectKey(), entry.Offset, entry.Size); ok && len(rs) > 1 {
				readers := make([]io.Reader, 0, len(rs))
				pos, end := entry.Offset, entry.Offset+entry.Size
				for _, r := range rs {
					if !d.cache.Contains(r.diskPath) {
						d.ranges.remove(r.diskPath)
						readers = nil
						break
					}
					f, newlyOpened, ok := openPath(r.diskPath)
					if !ok {
						readers = nil
						break
					}
					if newlyOpened {
						numOpenIOEntry++
					}
					next := min(r.offset+r.size, end)
					readers = append(readers, io.NewSectionReader(f.file, pos-r.offset, next-pos))
					pos = next
				}
				if readers != nil {
					if local, err := d.readEntryFrom(ctx, entry, io.MultiReader(readers...)); err != nil {
						if local != nil {
							for _, r := range rs {
								d.paths.close(r.diskPath, openedFiles[r.diskPath])
								delete(openedFiles, r.diskPath)
								d.dropCachedRange(ctx, r.diskPath)
							}
						}
						return err
					}
					for _, r := range rs {
						d.cache.Get(ctx, r.diskPath)
					}
					entry.done = true
					entry.fromCache = d
					numHit++
					return nil
				}
			}
		}

		if !openedOK && downloading {
			// The fill the read joined stopped without these bytes.
			vector.fillStopped = true
			return nil
		}

		if !openedOK {
			// no file available
			return nil
		}
		file := opened.file

		LogEvent(ctx, str_disk_cache_update_states_begin)
		if _, ok := d.cache.Get(ctx, diskPath); !ok {
			readable, err := d.reindexRead(ctx, diskPath, objectKey(), opened.generation, file)
			if err != nil {
				return err
			}
			if !readable {
				return nil
			}
		}
		LogEvent(ctx, str_disk_cache_update_states_end)

		readOffset, readSize := rangeOffset, entry.Size
		// Only a file from the object's start can be its whole object.
		if readOffset == entry.Offset {
			d.preloads.read(diskPath, entry.Size)
		}
		// Reads of a file kept open share it, so each reads at its own offset.
		if local, err := d.readEntryFrom(ctx, entry, io.NewSectionReader(file, readOffset, readSize)); err != nil {
			if local != nil && fromRange {
				d.paths.close(diskPath, opened)
				delete(openedFiles, diskPath)
				d.dropCachedRange(ctx, diskPath)
			}
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
			// A failed read of object storage by a download the read joined, and an error
			// of the entry, fail the read; an error of the cache itself is ignored.
			var failing *failingReadError
			if errors.As(err, &failing) {
				return failing.err
			}
			numError++
			logutil.Warn(
				"read disk cache error",
				zap.Any("error", err),
				zap.Any("path", vector.FilePath),
				zap.Any("entry", vector.Entries[i]),
			)
			// A process allocation failure is not a cache miss; falling back
			// can immediately fail another allocation in the storage path.
			if errors.Is(err, syscall.ENOMEM) {
				return err
			}
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
	if d.wholeFill(d.objectKey(path.File), false) != nil {
		// The fill of the whole object caches it.
		return nil
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
		if entry.fromCache == d || entry.inPrefetchRange {
			// no need to update
			continue
		}

		diskPath := d.pathForIOEntry(path.File, entry)
		if async {
			d.writeEntryAsync(ctx, vector.FilePath, diskPath, entry)
			continue
		}
		if err := d.updateEntry(ctx, vector.FilePath, diskPath, entry); err != nil {
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
) error {
	live := func() bool { return ctx.Err() == nil }
	_, err := d.publishUnlessDeleted(filePath, entry, live, func(claim func() bool) (bool, error) {
		return d.writeEntry(ctx, diskPath, entry, claim)
	})
	return err
}

func (d *DiskCache) writeEntry(
	ctx context.Context,
	diskPath string,
	entry IOEntry,
	claimPublication func() bool,
) (bool, error) {
	written, writeErr := d.writeFileWithFinalizeMode(ctx, diskPath, func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(entry.Data)), nil
	}, false, claimPublication, notWholeFill, nil)
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

// writeEntryAsync writes entry to a file now and leaves its publication to the async
// workers, so entry's data is not kept until then. It waits for an async slot first.
func (d *DiskCache) writeEntryAsync(ctx context.Context, filePath, diskPath string, entry IOEntry) {
	target := entryFillTarget{diskFillTarget: diskFillTarget{d: d, diskPath: diskPath}, filePath: filePath, readDeletes: entry.readDeletes}
	_, err := d.writeFileTo(ctx, diskPath, func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(entry.Data)), nil
	}, true, nil, notWholeFill, nil, target)
	if err != nil {
		_ = d.observeWriteResult(diskPath, err)
	}
}

func (d *DiskCache) writeFile(
	ctx context.Context,
	diskPath string,
	openReader func(context.Context) (io.ReadCloser, error),
) (written bool, err error) {
	return d.writeFileWithFinalizeMode(ctx, diskPath, openReader, false, nil, notWholeFill, nil)
}

func (d *DiskCache) removeEmptyDirectory(path string) {
	runPathHook(d.hooks.beforeLoadDirectoryCleanup, path)
	d.directoryMu.Lock()
	defer d.directoryMu.Unlock()
	// os.Remove will not delete a non-empty directory.
	_ = os.Remove(path)
}

type diskCacheFilesystemStat struct {
	totalBytes     uint64
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

func (d *DiskCache) statFS() (diskCacheFilesystemStat, bool) {
	if d.statFilesystem != nil {
		return d.statFilesystem(d.path)
	}
	return statDiskCacheFilesystem(d.path)
}

func (d *DiskCache) filesystemExhausted(wantBytes int64) bool {
	stat, ok := d.statFS()
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
	runHook(d.hooks.beforeCacheTempFileCreate)
	return os.CreateTemp(dir, "*"+cacheFileTempSuffix)
}

func (d *DiskCache) writeFileWithFinalizeMode(
	ctx context.Context,
	diskPath string,
	openReader func(context.Context) (io.ReadCloser, error),
	asyncFinalize bool,
	claimPublication func() bool,
	fill wholeFill,
	objectSize objectLayout,
) (written bool, err error) {
	return d.writeFileTo(ctx, diskPath, openReader, asyncFinalize, claimPublication, fill, objectSize, nil)
}

// writeFileTo writes the file of the object at diskPath and publishes it into target, the
// disk cache when nil. copied, when set, is called once the bytes are read.
type prefetchFillKey struct{}

// withPrefetchFill returns ctx under which a whole-object fill is a prefetch's.
func withPrefetchFill(ctx context.Context) context.Context {
	return context.WithValue(ctx, prefetchFillKey{}, true)
}

func prefetchFill(ctx context.Context) bool {
	_, ok := ctx.Value(prefetchFillKey{}).(bool)
	return ok
}

func (d *DiskCache) writeFileTo(
	ctx context.Context,
	diskPath string,
	openReader func(context.Context) (io.ReadCloser, error),
	asyncFinalize bool,
	claimPublication func() bool,
	fill wholeFill,
	objectSize objectLayout,
	target fillTarget,
) (written bool, err error) {
	if target == nil {
		target = diskFillTarget{d: d, diskPath: diskPath}
	}
	callerCtx := ctx
	defer func() {
		target.cache().evictForDiskFull(callerCtx, err)
	}()
	// A whole-object download is a FilePrefetchObject, not registered after a delete of
	// the object since the mark of ctx.
	var whole *filePrefetchObject
	if fill == storageFill {
		_, index := target.(indexFillTarget)
		whole = d.newWholeObject(ctx, diskPath, index)
		defer func() { whole.endWhole(err) }()
	}
	if target.heldBeforeLock(ctx) {
		return false, nil
	}
	var w fileWrite
	defer func() { d.endFileWrite(diskPath, &w, &err) }()
	if asyncFinalize {
		var deleted func() bool
		if whole != nil {
			deleted = func() bool { return d.deletes.since(diskPath, *whole.needs.deletes) }
		} else if t, ok := target.(entryFillTarget); ok {
			deleted = t.deletedSinceRead
		}
		w.slot = pendingKey(target, diskPath)
		if !d.reserveAsyncFileFinalize(ctx, w.slot, deleted) {
			w.slot = ""
			return false, nil
		}
	}
	// A whole-object download registers before it holds the path; each of its parts is
	// its own request.
	var bounds []int64
	lead, taken := 0, false
	if fill == storageFill {
		var ok bool
		if ok, err = whole.admitWhole(); !ok {
			return false, err
		}
		ctx = whole.ctx
		bounds, lead, taken = whole.splitParts(objectSize)
		if t, ok := target.(*wholeFillTarget); ok && whole.end != math.MaxInt64 {
			t.sizeAt(whole.end)
		}
		if taken {
			defer func() {
				if taken {
					d.prefetchObjects.downloads.n.Add(-1)
				}
			}()
		}
	}

	cleanup := func() error { return d.removeUnindexedFile(diskPath) }
	p, _ := ctx.Value(fileDownloadKey{}).(*filePrefetchObject)
	// A prefetch's fill waits for the path a fill of the object into either cache holds,
	// and fills its own cache after it; a read's async fill does not wait for it.
	if asyncFinalize && !prefetchFill(ctx) {
		var ok bool
		if w.owner, ok = d.paths.tryAcquire(diskPath, cleanup); !ok {
			return false, nil
		}
	} else if w.owner, err = d.paths.acquire(ctx, diskPath, cleanup); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if held, err := target.held(ctx); err != nil {
		return false, err
	} else if held {
		return false, nil
	}

	// write data
	f, err := target.createTemp()
	if err != nil {
		return false, err
	}
	w.file = f
	var download *objectDownload
	var dst io.Writer = f
	if fill != notWholeFill {
		// The fill of ctx, or one of the bytes the write path holds.
		own := p
		if own == nil {
			own = d.newContentFill(diskPath)
		}
		download = newObjectDownload(f.Name(), bounds)
		if t, ok := target.(*wholeFillTarget); ok {
			t.download = download
		}
		d.startDownload(own, download, taken)
		taken = false
		w.owner.first(func() { d.finishDownload(download, own) })
		own.started(download)
		downloadStarted(ctx)
		dst = downloadProgressWriter{w: f, download: download}
	}

	readCtx, idle := newIdleReader(ctx)
	defer idle.Close()
	if download != nil {
		defer download.idle.add(idle)()
	}
	from, err := idle.open(func() (io.ReadCloser, error) { return openReader(readCtx) })
	if err != nil {
		err = &diskCacheSourceError{err: err}
		if download != nil {
			download.endWith(false, sourceFailure(readCtx, err))
		}
		return false, err
	}
	defer from.Close()
	source := &diskCacheSourceReader{Reader: idle}

	// do eviction before write
	forceEvict := int64(0)
	if file, ok := from.(*os.File); ok {
		// get file size
		info, err := file.Stat()
		if err == nil {
			forceEvict = fileSize(info)
		}
	}
	target.reserve(ctx, forceEvict)

	var n int64
	if src, ok := from.(*objectSource); ok && download != nil {
		n, err = copyParts(readCtx, f, download, src, bounds, lead)
	} else {
		if download != nil {
			download.begin(0)
		}
		var buf []byte
		put := ioBufferPool.Get(&buf)
		defer put.Put()
		n, err = io.CopyBuffer(dst, source, buf)
		if err != nil && source.err != nil {
			err = &diskCacheSourceError{err: source.err}
		}
	}
	if download != nil {
		download.endWith(err == nil, sourceFailure(readCtx, err))
	}
	if err != nil {
		return false, err
	}

	if t, ok := target.(*wholeFillTarget); ok {
		if placed, err := t.place(ctx, n, &w, asyncFinalize && !prefetchFill(ctx)); !placed {
			return false, err
		}
	}
	if download != nil && fill != rangeFill {
		target.noteWritten(n)
	}

	var deletes *uint64
	if whole != nil {
		deletes = whole.needs.deletes
	}
	if asyncFinalize {
		if whole != nil {
			whole.handedOff = true
		}
		d.scheduleReservedAsyncFileFinalize(diskPath, f, w.owner, target, deletes)
		w = fileWrite{}
		return true, nil
	}

	if deletes != nil {
		claim := claimPublication
		if claim == nil {
			claim = func() bool { return ctx.Err() == nil }
		}
		if written, err = d.publishUnlessDeletedSince(diskPath, *deletes, claim, func(claim func() bool) (bool, error) {
			err := d.finalizeFileTo(ctx, diskPath, f, claim, target)
			if err != nil && claimPublication == nil && ctx.Err() != nil {
				err = ctx.Err()
			}
			return err == nil, err
		}); !written {
			return false, err
		}
	} else if err = d.finalizeFileTo(ctx, diskPath, f, claimPublication, target); err != nil {
		return false, err
	}
	w.file = nil
	return true, nil
}

// sourceFailure returns err when it is a failed read of object storage by a fill whose
// context readCtx did not end: it fails the reads waiting for the fill with the error of
// object storage itself, as a read of their own would. A fill stopped (idle, delete,
// close, its query) leaves them to read their own bytes.
func sourceFailure(readCtx context.Context, err error) error {
	var source *diskCacheSourceError
	if err == nil || readCtx.Err() != nil || !errors.As(err, &source) || sourceStopped(err) {
		return nil
	}
	return source.err
}

// sourceStopped reports whether err ends a source without failing the reads of its
// bytes: it was canceled, sent no bytes within the idle timeout, or ended short of its
// range, so the reads read their own bytes.
func sourceStopped(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, errSourceIdle) || errors.Is(err, errSourceShort)
}

// fileWrite is what a write of a cache file holds until it publishes the file or hands it
// to an async finalize.
type fileWrite struct {
	slot  string // the pending key of an async finalize slot
	owner *pathOwner
	file  *os.File
}

// endFileWrite releases what w holds, joining the errors into err.
func (d *DiskCache) endFileWrite(diskPath string, w *fileWrite, err *error) {
	if w.file != nil {
		d.mergeWriteCleanupError(diskPath, err, cleanupDiskCacheTempFile(w.file))
	}
	if w.owner != nil {
		d.mergeWriteCleanupError(diskPath, err, w.owner.release())
	}
	if w.slot != "" {
		d.releaseAsyncFileFinalizeReservation(w.slot)
	}
}

func (d *DiskCache) finalizeFile(
	ctx context.Context,
	diskPath string,
	f *os.File,
	claimPublication func() bool,
) error {
	return d.finalizeFileTo(ctx, diskPath, f, claimPublication, diskFillTarget{d: d, diskPath: diskPath})
}

// finalizeFileTo makes f durable and publishes it into target.
func (d *DiskCache) finalizeFileTo(
	ctx context.Context,
	diskPath string,
	f *os.File,
	claimPublication func() bool,
	target fillTarget,
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
	runHook(d.hooks.beforeFilePublication)
	if claimPublication != nil {
		if !claimPublication() {
			return context.Canceled
		}
	} else if err := ctx.Err(); err != nil {
		return err
	}
	runHook(d.hooks.afterPublicationClaim)
	if err := target.publish(ctx, tempPath, size); err != nil {
		return err
	}
	runHook(d.hooks.afterFilePublication)

	return nil
}

func (d *DiskCache) Flush(ctx context.Context) {
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
	// filepath.Join below already cleans the "N-N..." component,
	// so the shard has to be derived from the same cleaned form or the two
	// spellings would be sharded apart even though the filename collides.
	path = pathpkg.Clean(path)
	return filepath.Join(
		d.path,
		diskCacheShard(path),
		fmt.Sprintf("%d-%d%s%s", entry.Offset, entry.Size, toOSPath(path), cacheFileSuffix),
	)
}

func (d *DiskCache) removeEvictedFile(path string, size int64) error {
	if d.cache.Contains(path) {
		return nil
	}
	if d.dirEntries && strings.HasSuffix(path, cacheDirSuffix) {
		return d.removeEvictedDir(path, size)
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
	return d.fillFile(ctx, filePath, openReader, asyncFinalize, storageFill, nil)
}

// setFileContent caches content already in memory as the whole object of filePath.
func (d *DiskCache) setFileContent(
	ctx context.Context,
	filePath string,
	content []byte,
	asyncFinalize bool,
) error {
	return d.fillFile(ctx, filePath, func(context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(content)), nil
	}, asyncFinalize, contentFill, nil)
}

// wholeFill is how a write fills a disk-cache file.
type wholeFill int

const (
	// notWholeFill writes a range or an index file.
	notWholeFill wholeFill = iota
	// contentFill writes a whole object from bytes in memory.
	contentFill
	// storageFill downloads a whole object from object storage.
	storageFill
	// rangeFill downloads a range of an object from object storage.
	rangeFill
)

// fillFile caches the whole object of filePath read through openReader; objectSize, when
// set, returns the object's size before a download starts, so a large object downloads
// as parts.
func (d *DiskCache) fillFile(
	ctx context.Context,
	filePath string,
	openReader func(context.Context) (io.ReadCloser, error),
	asyncFinalize bool,
	fill wholeFill,
	objectSize objectLayout,
) error {
	return d.fillFileTo(ctx, filePath, openReader, asyncFinalize, fill, objectSize, nil)
}

// fillFileTo fills the file of the object at filePath and publishes it into target, the
// disk cache when nil.
func (d *DiskCache) fillFileTo(
	ctx context.Context,
	filePath string,
	openReader func(context.Context) (io.ReadCloser, error),
	asyncFinalize bool,
	fill wholeFill,
	objectSize objectLayout,
	target fillTarget,
) error {
	path, err := ParsePath(filePath)
	if err != nil {
		return err
	}
	diskPath := d.objectKey(path.File)
	// Only a fill published into the disk cache takes its room.
	if target == nil {
		target = d.newWholeFillTarget(ctx, path.File)
		d.wholeFills.Add(1)
		defer d.wholeFills.Add(-1)
	}
	written, writeErr := d.writeFileTo(ctx, diskPath, openReader, asyncFinalize, nil, fill, objectSize, target)
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

// DeletePaths deletes the entries of every path of paths, whatever the others return,
// and returns their errors joined.
func (d *DiskCache) DeletePaths(
	ctx context.Context,
	paths []string,
) error {
	canonical, err := canonicalFilePaths(paths)
	if err != nil {
		return err
	}
	errs := make([]error, 0, len(canonical))
	for _, path := range canonical {
		errs = append(errs, d.deletePath(ctx, path))
	}
	return errors.Join(errs...)
}

func (d *DiskCache) deletePath(ctx context.Context, path string) error {
	key := d.objectKey(path)
	// Counted first: an entry read before it and cached after it is retracted.
	d.deletes.deleted(key)
	d.async.mu.Lock()
	d.async.wakeFreedLocked()
	d.async.mu.Unlock()
	// No prefetch object of the object caches its bytes after the delete; one under
	// way is canceled rather than waited for.
	if err := d.dropPrefetches(ctx, key); err != nil {
		return err
	}
	d.prefetched.forget(key)
	return d.removeRangeFiles(ctx, path)
}

// removeRangeFiles deletes the cached range files of the object at path, each one
// whatever the others return, and returns their errors joined.
func (d *DiskCache) removeRangeFiles(ctx context.Context, path string) error {
	ranges := d.ranges.of(d.objectKey(path))
	errs := make([]error, 0, len(ranges))
	for _, r := range ranges {
		errs = append(errs, d.removeRangeFile(ctx, r.diskPath))
	}
	return errors.Join(errs...)
}

func (d *DiskCache) removeRangeFile(ctx context.Context, diskPath string) error {
	owner, err := d.paths.acquire(ctx, diskPath, nil)
	if err != nil {
		return err
	}
	defer owner.release()
	if err := os.Remove(diskPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if d.cache.Contains(diskPath) {
		d.preloads.deleted(diskPath)
	}
	d.ranges.remove(diskPath)
	d.cache.Delete(ctx, diskPath)
	return nil
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
	d.closePrefetches(ctx)
	d.closeAsyncUpdates(ctx)
	d.paths.closeKept()
	select {
	case <-d.load.done:
	case <-ctx.Done():
	}
	allDiskCaches.Delete(d)
}
