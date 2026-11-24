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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	cacheDirSuffix      = ".mofsdir"
	cacheDirTempSuffix  = cacheDirSuffix + ".tmp"
	cacheDirTrashSuffix = cacheDirSuffix + ".trash"
)

var cacheDirTrashSeq atomic.Uint64

const (
	// DefaultIndexFileDiskCapacity is the default capacity of the index-file cache.
	DefaultIndexFileDiskCapacity = 8 << 30

	// DefaultIndexFileIdleTTL is how long an index-file cache entry with no holds stays
	// after its last use; 0 turns idle collection off.
	DefaultIndexFileIdleTTL time.Duration = 0

	maxIndexFileGCInterval = 10 * time.Minute

	// indexFileLowFree and indexFileTargetFree are shares of the cache filesystem's size:
	// below the first, collection removes entries with no holds, least recently used first,
	// until free space reaches the second, and runs every indexFilePressureGCInterval
	// until it does.
	indexFileLowFree            = 0.10
	indexFileTargetFree         = 0.15
	indexFilePressureGCInterval = 30 * time.Second

	IndexFileRebuildCacheRead      = "read"
	IndexFileRebuildCacheNone      = "none"
	IndexFileRebuildCacheReadWrite = "read-write"

	// IndexFileRebuildPolicyDefault reads and fills the memory and disk caches like an
	// ordinary scan.
	IndexFileRebuildPolicyDefault Policy = 0
)

// ParseIndexFileRebuildCache maps an index-file-rebuild-cache setting to a cache policy.
func ParseIndexFileRebuildCache(s string) (Policy, error) {
	switch s {
	case "", IndexFileRebuildCacheReadWrite:
		return IndexFileRebuildPolicyDefault, nil
	case IndexFileRebuildCacheRead:
		return SkipMemoryCacheWrites | SkipDiskCacheWrites | SkipFullFilePreloads, nil
	case IndexFileRebuildCacheNone:
		return SkipAllCache | SkipFullFilePreloads, nil
	}
	return 0, moerr.NewInvalidInputNoCtxf(
		"invalid index-file-rebuild-cache %q, want %q, %q or %q",
		s, IndexFileRebuildCacheReadWrite, IndexFileRebuildCacheRead, IndexFileRebuildCacheNone)
}

// IndexFileCache keeps whole index files on local disk. An entry is a directory of named
// files that is published, evicted and deleted as a unit. Capacity, eviction, per-path
// coordination and reuse after restart are those of DiskCache. An entry with no holds
// whose last use, its publication or the release of its last hold, is older than the
// idle TTL is deleted.
type IndexFileCache struct {
	disk           *DiskCache
	rebuildPolicy  Policy
	checkOnStartup atomic.Bool
	idleTTL        time.Duration
	stopGC         context.CancelFunc
	gcDone         chan struct{}
}

// IndexCacheFile is one file of an entry written by IndexFileCache.Put. Its content
// comes from Open, or from Path: a local file that Put moves into the entry, created by
// CreateTempFile so it is on the cache's filesystem.
type IndexCacheFile struct {
	Name string
	Open func(ctx context.Context) (io.ReadCloser, error)
	Path string
}

// NewIndexFileCache returns an index file cache rooted at path whose rebuilds read with
// rebuildPolicy.
func NewIndexFileCache(
	ctx context.Context,
	path string,
	capacity fscache.CapacityFunc,
	name string,
	rebuildPolicy Policy,
) (*IndexFileCache, error) {
	return newIndexFileCache(ctx, path, capacity, name, "", rebuildPolicy, 0, 0)
}

func newIndexFileCache(
	ctx context.Context,
	path string,
	capacity fscache.CapacityFunc,
	name string,
	metricScope string,
	rebuildPolicy Policy,
	idleTTL time.Duration,
	openFilesMax int,
) (*IndexFileCache, error) {
	disk, err := newDiskCache(ctx, path, capacity, nil, true, nil, name, metricScope,
		diskCacheOptions{dirEntries: true, openFilesMax: openFilesMax})
	if err != nil {
		return nil, err
	}
	c := &IndexFileCache{disk: disk, rebuildPolicy: rebuildPolicy, idleTTL: idleTTL}
	interval := maxIndexFileGCInterval
	if idleTTL > 0 {
		interval = min(max(idleTTL/4, time.Second), maxIndexFileGCInterval)
	}
	c.startGC(interval)
	return c, nil
}

// indexFileCacheSettings are the settings of an index-file cache, taken from the
// configuration of the CN that creates it.
type indexFileCacheSettings struct {
	path           string
	capacity       int64
	rebuildPolicy  Policy
	idleTTL        time.Duration
	checkOnStartup bool
	openFilesMax   int
}

type sharedIndexFileCache struct {
	cache    *IndexFileCache
	settings indexFileCacheSettings
	refs     int
}

// sharedIndexFileCaches holds one index-file cache per directory in the process, shared
// by the CNs configured with that directory. The first CN to use a directory creates its
// cache with its settings; the last to stop closes it.
var sharedIndexFileCaches struct {
	mu sync.Mutex
	m  map[string]*sharedIndexFileCache
}

// acquireIndexFileCache returns the cache of settings.path, creating it with settings if
// the process has none. diskPath is the table-data disk cache directory of the CN.
func acquireIndexFileCache(
	ctx context.Context,
	diskPath string,
	settings indexFileCacheSettings,
	name string,
	metricScope string,
) (*IndexFileCache, error) {
	if err := checkIndexFileDiskPath(diskPath, settings.path); err != nil {
		return nil, err
	}
	key, err := filepath.Abs(settings.path)
	if err != nil {
		return nil, err
	}
	p := &sharedIndexFileCaches
	p.mu.Lock()
	defer p.mu.Unlock()
	if e := p.m[key]; e != nil {
		if settings != e.settings {
			logutil.Warn("index file cache: directory already in use in this process; its first settings are kept",
				zap.String("path", key),
				zap.Int64("capacity", e.settings.capacity),
				zap.Duration("idle-ttl", e.settings.idleTTL),
				zap.Bool("check-on-startup", e.settings.checkOnStartup),
				zap.Int64("ignored-capacity", settings.capacity),
			)
		}
		e.refs++
		return e.cache, nil
	}
	c, err := newIndexFileCache(ctx, settings.path, fscache.ConstCapacity(settings.capacity),
		name, metricScope, settings.rebuildPolicy, settings.idleTTL, settings.openFilesMax)
	if err != nil {
		return nil, err
	}
	c.SetCheckOnStartup(settings.checkOnStartup)
	if p.m == nil {
		p.m = make(map[string]*sharedIndexFileCache)
	}
	p.m[key] = &sharedIndexFileCache{cache: c, settings: settings, refs: 1}
	logutil.Info("fileservice: index file cache initialized",
		zap.String("path", key),
		zap.Int64("capacity", settings.capacity),
		zap.Duration("idle-ttl", settings.idleTTL),
		zap.Bool("check-on-startup", settings.checkOnStartup),
	)
	return c, nil
}

// releaseIndexFileCache releases a reference from acquireIndexFileCache and closes the
// cache with the last one.
func releaseIndexFileCache(ctx context.Context, c *IndexFileCache) {
	p := &sharedIndexFileCaches
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, e := range p.m {
		if e.cache != c {
			continue
		}
		if e.refs--; e.refs == 0 {
			delete(p.m, key)
			c.Close(ctx)
		}
		return
	}
}

// startGC collects entries every interval until Close: those idle past the idle TTL, and
// while the filesystem is short of free space those with no holds, every
// indexFilePressureGCInterval until it is not.
func (c *IndexFileCache) startGC(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	c.stopGC, c.gcDone = cancel, make(chan struct{})
	go func() {
		defer close(c.gcDone)
		timer := time.NewTimer(interval)
		defer timer.Stop()
		stuck := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			c.disk.retryTrash()
			if c.idleTTL > 0 {
				if n, err := c.collectIdle(ctx, time.Now()); err != nil && ctx.Err() == nil {
					logutil.Warn("index file cache: collect idle entries", zap.String("path", c.disk.path), zap.Error(err))
				} else if n > 0 {
					logutil.Info("index file cache: collected idle entries", zap.String("path", c.disk.path), zap.Int("entries", n))
				}
			}
			short, err := c.relieveDiskPressure(ctx)
			if err != nil && ctx.Err() == nil {
				logutil.Warn("index file cache: free disk space", zap.String("path", c.disk.path), zap.Error(err))
			}
			if short && !stuck {
				logutil.Warn("index file cache: filesystem still short of free space; the remaining entries are in use",
					zap.String("path", c.disk.path))
			}
			stuck = short
			next := interval
			if short {
				next = min(interval, indexFilePressureGCInterval)
			}
			timer.Reset(next)
		}
	}()
}

// relieveDiskPressure removes entries with no holds, least recently used first, while the
// cache filesystem's free space is below indexFileLowFree of its size, until it reaches
// indexFileTargetFree. It reports whether free space is still below the target.
func (c *IndexFileCache) relieveDiskPressure(ctx context.Context) (short bool, err error) {
	stat, ok := c.disk.statFS()
	if !ok || stat.totalBytes == 0 {
		return false, nil
	}
	low := uint64(float64(stat.totalBytes) * indexFileLowFree)
	target := uint64(float64(stat.totalBytes) * indexFileTargetFree)
	if stat.availableBytes >= low {
		return false, nil
	}
	paths, err := c.entryDirs()
	if err != nil {
		return true, err
	}
	type entry struct {
		path string
		used time.Time
		size uint64
	}
	entries := make([]entry, 0, len(paths))
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			continue
		}
		entries = append(entries, entry{path: path, used: info.ModTime(), size: uint64(entrySize(path))})
	}
	slices.SortFunc(entries, func(a, b entry) int { return a.used.Compare(b.used) })
	need := target - stat.availableBytes
	var freed uint64
	n := 0
	for _, e := range entries {
		if freed >= need {
			break
		}
		removed, err := c.disk.removeIdleDir(ctx, e.path, anyLastUse)
		if err != nil {
			return true, err
		}
		if removed {
			freed += e.size
			n++
		}
	}
	if n > 0 {
		logutil.Warn("index file cache: removed entries for free disk space",
			zap.String("path", c.disk.path), zap.Int("entries", n), zap.Uint64("bytes", freed),
			zap.Uint64("available", stat.availableBytes), zap.Uint64("total", stat.totalBytes))
	}
	return freed < need, nil
}

func entrySize(path string) int64 {
	files, err := os.ReadDir(path)
	if err != nil {
		return 0
	}
	var n int64
	for _, f := range files {
		if info, err := f.Info(); err == nil {
			n += info.Size()
		}
	}
	return n
}

// collectIdle deletes the entries with no holds last used before now minus the idle TTL,
// and returns how many it deleted.
func (c *IndexFileCache) collectIdle(ctx context.Context, now time.Time) (int, error) {
	before := now.Add(-c.idleTTL)
	paths, err := c.entryDirs()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, path := range paths {
		removed, err := c.disk.removeIdleDir(ctx, path, before)
		if err != nil {
			return n, err
		}
		if removed {
			n++
		}
	}
	return n, nil
}

// entryDirs returns the entry directories in the cache.
func (c *IndexFileCache) entryDirs() ([]string, error) {
	var paths []string
	err := filepath.WalkDir(c.disk.path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !d.IsDir() {
			return nil
		}
		switch name := d.Name(); {
		case strings.HasSuffix(name, cacheDirSuffix):
			paths = append(paths, path)
			return fs.SkipDir
		case strings.HasSuffix(name, cacheDirTempSuffix), strings.HasSuffix(name, cacheDirTrashSuffix):
			return fs.SkipDir
		}
		return nil
	})
	return paths, err
}

// Path returns the cache directory.
func (c *IndexFileCache) Path() string {
	return c.disk.path
}

// RebuildPolicy is the cache policy for reading chunk rows when an entry is rebuilt.
func (c *IndexFileCache) RebuildPolicy() Policy {
	return c.rebuildPolicy
}

// CheckOnStartup reports whether each cached file's full checksum is checked on its first
// open since the process started.
func (c *IndexFileCache) CheckOnStartup() bool {
	return c.checkOnStartup.Load()
}

// SetCheckOnStartup sets CheckOnStartup.
func (c *IndexFileCache) SetCheckOnStartup(on bool) {
	c.checkOnStartup.Store(on)
}

// IndexFileCacheProvider is a file service that owns an index-file cache.
type IndexFileCacheProvider interface {
	IndexFileCache() *IndexFileCache
}

// GetIndexFileCache returns the index-file cache of fs's SHARED file service, or nil.
func GetIndexFileCache(fs FileService) *IndexFileCache {
	if fs == nil {
		return nil
	}
	shared, err := Get[FileService](fs, defines.SharedFileServiceName)
	if err != nil {
		return nil
	}
	if p, ok := shared.(IndexFileCacheProvider); ok {
		return p.IndexFileCache()
	}
	return nil
}

// Close stops the cache. Entries stay on disk.
func (c *IndexFileCache) Close(ctx context.Context) {
	if c.stopGC != nil {
		c.stopGC()
		<-c.gcDone
	}
	c.disk.Close(ctx)
}

// CreateTempFile creates a file on the cache's filesystem, for Put to move into an
// entry. remove deletes whatever is left of it; call it after Put.
func (c *IndexFileCache) CreateTempFile() (f *os.File, remove func(), err error) {
	dir, err := c.disk.createCacheTempDir(c.disk.path)
	if err != nil {
		return nil, nil, err
	}
	f, err = os.Create(filepath.Join(dir, "data"))
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, nil, err
	}
	return f, func() { _ = os.RemoveAll(dir) }, nil
}

// Put publishes entry name with files. It returns false, with no error, when the entry
// already exists or the cache could not be written; a source error is returned.
func (c *IndexFileCache) Put(ctx context.Context, name string, files []IndexCacheFile) (bool, error) {
	return c.put(ctx, name, 0, files)
}

// DeleteMark returns the mark of content read now, for PutUnlessDeleted.
func (c *IndexFileCache) DeleteMark() uint64 {
	return c.disk.readDeletes()
}

// PutUnlessDeleted is Put, except that it publishes nothing, and returns false, when entry
// name was deleted after mark was taken.
func (c *IndexFileCache) PutUnlessDeleted(ctx context.Context, name string, mark uint64, files []IndexCacheFile) (bool, error) {
	return c.put(ctx, name, mark, files)
}

func (c *IndexFileCache) put(ctx context.Context, name string, mark uint64, files []IndexCacheFile) (bool, error) {
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return false, err
	}
	if err := checkCacheDirFiles(files); err != nil {
		return false, err
	}
	if mark > 0 {
		stripe := c.disk.deletes.stripe(diskPath)
		stripe.RLock()
		defer stripe.RUnlock()
		if c.disk.deletes.since(diskPath, mark-1) {
			return false, nil
		}
	}
	if c.disk.trashHoldsPublications() {
		return false, nil
	}
	written, err := c.disk.putDir(ctx, diskPath, files)
	if err != nil {
		return false, c.disk.observeWriteResult(diskPath, err)
	}
	if !written {
		return false, nil
	}
	return true, c.disk.observeWriteResult(diskPath, nil)
}

// Open opens files of entry name. A missing entry or file returns a file-not-found
// error. The returned files stay readable after the entry is evicted; the caller closes
// them.
func (c *IndexFileCache) Open(ctx context.Context, name string, files ...string) ([]*os.File, error) {
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if err := checkCacheDirFileName(f); err != nil {
			return nil, err
		}
	}
	var deadline time.Time
	return c.disk.openDir(ctx, name, diskPath, files, &deadline)
}

// entryRead is a read of generation gen of an entry; done ends it. dir is the entry the
// read opened, kept open until done so that no other entry takes its identity, or nil;
// absent is set when there was no entry to open.
type entryRead struct {
	gen    uint64
	dir    os.FileInfo
	absent bool
	done   func()
}

// openEntryFile opens file of entry name for a read, through the file kept open for the
// entry if any, and keeps a file it opens for later reads. When the file cannot be
// opened, the read is still started unless read is nil. The file is kept by entry, so
// reads of an entry through it read the same file of it.
func (c *IndexFileCache) openEntryFile(ctx context.Context, name string, file string) (f *os.File, read *entryRead, err error) {
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return nil, nil, err
	}
	if err := checkCacheDirFileName(file); err != nil {
		return nil, nil, err
	}
	d := c.disk
	var deadline time.Time
	generation, ok := d.paths.startRead(ctx, diskPath, &deadline)
	if !ok {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, moerr.NewFileNotFoundNoCtx(name)
	}
	read = &entryRead{gen: generation}
	kept := d.paths.takeKept(diskPath)
	opened := openedDiskCacheFile{generation: generation, kept: kept}
	if kept != nil {
		d.cache.Get(ctx, diskPath)
		opened.file = kept.file
	} else {
		dir, dirErr := os.Open(diskPath)
		files, err := d.openDir(ctx, name, diskPath, []string{file}, &deadline)
		if err != nil {
			if dirErr == nil {
				read.dir, _ = dir.Stat()
			}
			read.absent = os.IsNotExist(dirErr)
			read.done = func() {
				if dirErr == nil {
					_ = dir.Close()
				}
				d.paths.endRead(diskPath, generation)
			}
			return nil, read, err
		}
		if dirErr == nil {
			_ = dir.Close()
		}
		opened.file = files[0]
		opened.kept = d.paths.keep(diskPath, generation, files[0])
	}
	read.done = func() { d.paths.close(diskPath, opened) }
	return opened.file, read, nil
}

// IndexFileHold keeps a cache entry on disk until it is released. Only the first release
// takes effect; a nil hold holds nothing.
type IndexFileHold struct {
	once    sync.Once
	release func(used bool)
}

// Release drops the hold. The release of an entry's last hold is its last use.
func (h *IndexFileHold) Release() {
	if h != nil {
		h.once.Do(func() { h.release(true) })
	}
}

// ReleaseSuperseded drops the hold of an entry a newer version has replaced, without
// counting it as a use: the entry keeps its last use and is evicted ahead of entries used
// since.
func (h *IndexFileHold) ReleaseSuperseded() {
	if h != nil {
		h.once.Do(func() { h.release(false) })
	}
}

// NewIndexFileHold returns a hold that calls release once, with whether the release counts
// as a use, for holds kept by something other than the cache.
func NewIndexFileHold(release func(used bool)) *IndexFileHold {
	return &IndexFileHold{release: release}
}

// Hold keeps entry name on disk until the hold is released: evicting or collecting the
// entry is deferred until its last hold is released. Take the hold before Open.
func (c *IndexFileCache) Hold(name string) (*IndexFileHold, error) {
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return nil, err
	}
	c.disk.holdDir(diskPath)
	return &IndexFileHold{release: func(used bool) { c.disk.releaseDir(diskPath, used) }}, nil
}

// HoldCached holds entry name, as Hold does, when the cache holds the entry, and returns
// nil when it does not.
func (c *IndexFileCache) HoldCached(name string) *IndexFileHold {
	hold, err := c.Hold(name)
	if err != nil {
		return nil
	}
	if diskPath, err := c.disk.pathForDir(name); err != nil || !c.disk.dirCached(diskPath) {
		hold.ReleaseSuperseded()
		return nil
	}
	return hold
}

// dirCached reports whether the entry directory at diskPath is cached: indexed, or on
// disk while the index is loading.
func (d *DiskCache) dirCached(diskPath string) bool {
	if d.cache.Contains(diskPath) {
		return true
	}
	select {
	case <-d.load.done:
		return false
	default:
	}
	_, err := os.Stat(diskPath)
	return err == nil
}

// Delete removes entry name at once, held or not; files already open stay readable. A
// PutUnlessDeleted of content read before the delete publishes nothing.
func (c *IndexFileCache) Delete(ctx context.Context, name string) error {
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return err
	}
	c.disk.deletes.deleted(diskPath)
	owner, err := c.disk.paths.acquire(ctx, diskPath, nil)
	if err != nil {
		return err
	}
	defer owner.release()
	if err := c.disk.removeCacheDir(diskPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	c.disk.cache.Delete(ctx, diskPath)
	return nil
}

// dropBroken removes entry name while gen, the generation of the read that found it
// broken, is its current one, and seen, when set, is still what is at file of the entry,
// or at the entry when file is empty: another cache instance on the directory may have
// replaced it. When missing is set, it removes it only while the entry is indexed and its
// file missing does not exist. It is no delete of the object: a fill of it is not
// canceled.
func (c *IndexFileCache) dropBroken(ctx context.Context, name string, missing string, gen uint64, seen os.FileInfo, file string) {
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return
	}
	owner, ok := c.disk.paths.acquireCurrent(diskPath, gen)
	if !ok {
		return
	}
	defer owner.release()
	if seen != nil {
		if cur, err := os.Lstat(filepath.Join(diskPath, file)); err != nil || !os.SameFile(seen, cur) {
			return
		}
	}
	if missing != "" {
		if !c.disk.cache.Contains(diskPath) {
			return
		}
		if _, err := os.Stat(filepath.Join(diskPath, missing)); !os.IsNotExist(err) {
			return
		}
	}
	if err := c.disk.removeCacheDir(diskPath); err != nil && !os.IsNotExist(err) {
		return
	}
	c.disk.cache.Delete(ctx, diskPath)
}

// dropUnopened drops entry name after read failed to open it, unless the entry is there
// and read could not open it: its identity is unknown.
func (c *IndexFileCache) dropUnopened(ctx context.Context, name string, missing string, read *entryRead) {
	if read.dir == nil && !read.absent {
		return
	}
	c.dropBroken(ctx, name, missing, read.gen, read.dir, "")
}

type dirHolds struct {
	mu sync.Mutex
	m  map[string]int
}

// loadedDir is an entry directory found when the cache loads.
type loadedDir struct {
	path    string
	size    int64
	lastUse time.Time
}

// indexLoadedDirs indexes the entry directories found when the cache loads behind every
// entry published or opened meanwhile, in their order of last use, skipping those removed
// meanwhile.
func (d *DiskCache) indexLoadedDirs(ctx context.Context, dirs []loadedDir) {
	lru := d.cache
	slices.SortFunc(dirs, func(a, b loadedDir) int { return b.lastUse.Compare(a.lastUse) })
	runHook(d.hooks.beforeIndexLoadedDirs)
	for _, e := range dirs {
		owner, _ := d.paths.acquireKeeping(context.Background(), e.path)
		if _, err := os.Stat(e.path); err == nil {
			lru.SetOldest(ctx, e.path, struct{}{}, e.size)
		}
		_ = owner.release()
	}
}

// dirHeld reports whether entry directory path has a hold.
func (d *DiskCache) dirHeld(path string) bool {
	d.held.mu.Lock()
	defer d.held.mu.Unlock()
	return d.held.m[path] > 0
}

func (d *DiskCache) holdDir(path string) {
	d.held.mu.Lock()
	defer d.held.mu.Unlock()
	if d.held.m == nil {
		d.held.m = make(map[string]int)
	}
	d.held.m[path]++
}

// releaseDir drops a hold; with used, the release of the last one is the entry's last use.
func (d *DiskCache) releaseDir(path string, used bool) {
	d.held.mu.Lock()
	d.held.m[path]--
	if d.held.m[path] > 0 {
		d.held.mu.Unlock()
		return
	}
	if used {
		now := time.Now()
		_ = os.Chtimes(path, now, now)
	}
	delete(d.held.m, path)
	d.held.mu.Unlock()
	if used {
		_, _ = d.cache.Get(context.Background(), path)
	}
}

// removeEvictedDir removes an evicted entry directory, or puts it back in the cache with
// size when it has a hold.
func (d *DiskCache) removeEvictedDir(path string, size int64) error {
	d.held.mu.Lock()
	if d.held.m[path] > 0 {
		d.held.mu.Unlock()
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		d.cache.Set(context.Background(), path, struct{}{}, size)
		return nil
	}
	trash, err := trashCacheDir(path, size)
	d.held.mu.Unlock()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	metric.FSDiskCacheEvictCounter.Add(1)
	return d.removeTrash(trash, size)
}

// removeIdleDir removes an entry directory that has no holds and was last used before
// before.
func (d *DiskCache) removeIdleDir(ctx context.Context, path string, before time.Time) (bool, error) {
	d.held.mu.Lock()
	idle, err := d.idleDirLocked(path, before)
	d.held.mu.Unlock()
	if !idle {
		return false, err
	}
	owner, err := d.paths.acquire(ctx, path, nil)
	if err != nil {
		return false, err
	}
	defer owner.release()
	// The size is read before d.held.mu: eviction takes d.held.mu under the cache's lock.
	size, _ := d.cache.Size(path)
	d.held.mu.Lock()
	if idle, err = d.idleDirLocked(path, before); !idle {
		d.held.mu.Unlock()
		return false, err
	}
	trash, err := trashCacheDir(path, size)
	d.held.mu.Unlock()
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	d.cache.Delete(ctx, path)
	return true, d.removeTrash(trash, size)
}

// anyLastUse is a cutoff later than any entry's last use, even one recorded before the
// clock was set back.
var anyLastUse = time.Unix(1<<62, 0)

// idleDirLocked reports whether an entry directory has no holds and was last used before
// before. The caller holds d.held.mu.
func (d *DiskCache) idleDirLocked(path string, before time.Time) (bool, error) {
	if d.held.m[path] > 0 {
		return false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.ModTime().Before(before), nil
}

func (d *DiskCache) pathForDir(name string) (string, error) {
	clean := pathpkg.Clean(name)
	if name == "" || clean == "." || pathpkg.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", moerr.NewInvalidInputNoCtxf("invalid index cache entry name %q", name)
	}
	for _, part := range strings.Split(clean, "/") {
		if strings.HasSuffix(part, cacheDirSuffix) ||
			strings.HasSuffix(part, cacheDirTempSuffix) ||
			strings.HasSuffix(part, cacheDirTrashSuffix) ||
			strings.HasSuffix(part, cacheFileSuffix) ||
			strings.HasSuffix(part, cacheFileTempSuffix) {
			return "", moerr.NewInvalidInputNoCtxf("invalid index cache entry name %q", name)
		}
	}
	return filepath.Join(d.path, diskCacheShard(clean), cacheDirName(clean)), nil
}

const maxCacheDirNameLen = 200

// cacheDirName flattens an entry name into one directory name, so removing an entry
// leaves no directory of its own behind.
func cacheDirName(clean string) string {
	name := url.PathEscape(clean)
	if len(name) > maxCacheDirNameLen {
		sum := sha256.Sum256([]byte(clean))
		name = "%sha256-" + hex.EncodeToString(sum[:])
	}
	return name + cacheDirSuffix
}

func checkCacheDirFileName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return moerr.NewInvalidInputNoCtxf("invalid index cache file name %q", name)
	}
	return nil
}

func checkCacheDirFiles(files []IndexCacheFile) error {
	if len(files) == 0 {
		return moerr.NewInvalidInputNoCtx("index cache entry has no files")
	}
	seen := make(map[string]struct{}, len(files))
	for _, f := range files {
		if err := checkCacheDirFileName(f.Name); err != nil {
			return err
		}
		if (f.Open == nil) == (f.Path == "") {
			return moerr.NewInvalidInputNoCtxf("index cache file %q needs exactly one of Open or Path", f.Name)
		}
		if _, ok := seen[f.Name]; ok {
			return moerr.NewInvalidInputNoCtxf("duplicate index cache file name %q", f.Name)
		}
		seen[f.Name] = struct{}{}
	}
	return nil
}

func (d *DiskCache) putDir(ctx context.Context, diskPath string, files []IndexCacheFile) (written bool, err error) {
	defer func() {
		d.evictForDiskFull(ctx, err)
	}()
	owner, err := d.paths.acquire(ctx, diskPath, func() error {
		return d.removeUnindexedFile(diskPath)
	})
	if err != nil {
		return false, err
	}
	defer func() {
		d.mergeWriteCleanupError(diskPath, &err, owner.release())
	}()
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if size, err := cacheDirSize(diskPath); err == nil {
		if cacheDirHasFiles(diskPath, files) {
			if _, ok := d.cache.Get(ctx, diskPath); !ok {
				d.cache.Set(ctx, diskPath, struct{}{}, size)
			}
			return false, nil
		}
		// an incomplete entry is replaced
		if err := d.removeCacheDir(diskPath); err != nil && !os.IsNotExist(err) {
			return false, err
		}
	} else if !os.IsNotExist(err) {
		return false, err
	}

	tmp, err := d.createCacheTempDir(filepath.Dir(diskPath))
	if err != nil {
		return false, err
	}
	tmpOwned := true
	defer func() {
		if tmpOwned {
			d.mergeWriteCleanupError(diskPath, &err, os.RemoveAll(tmp))
		}
	}()

	var size int64
	for _, f := range files {
		var n int64
		if f.Path != "" {
			n, err = d.moveCacheDirFile(ctx, f.Path, filepath.Join(tmp, f.Name))
		} else {
			n, err = d.writeCacheDirFile(ctx, filepath.Join(tmp, f.Name), f.Open)
		}
		if err != nil {
			return false, err
		}
		size += n
	}
	d.cache.Evict(ctx, nil, size)

	runHook(d.hooks.beforeFilePublication)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := syncDir(tmp); err != nil {
		return false, err
	}
	if err := os.Rename(tmp, diskPath); err != nil {
		if existing, statErr := cacheDirSize(diskPath); statErr == nil {
			// published by another cache instance on the same directory
			if !d.cache.Contains(diskPath) {
				d.cache.Set(ctx, diskPath, struct{}{}, existing)
			}
			return false, nil
		}
		return false, err
	}
	tmpOwned = false

	// a republished entry is the most recently used before its new size is accounted
	if _, ok := d.cache.Get(ctx, diskPath); !ok || !d.cache.Replace(ctx, diskPath, struct{}{}, size) {
		d.cache.Set(ctx, diskPath, struct{}{}, size)
	}
	if !d.cache.Contains(diskPath) {
		if err := d.removeCacheDir(diskPath); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

func (d *DiskCache) createCacheTempDir(dir string) (string, error) {
	d.directoryMu.RLock()
	defer d.directoryMu.RUnlock()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	runHook(d.hooks.beforeCacheTempFileCreate)
	return os.MkdirTemp(dir, "*"+cacheDirTempSuffix)
}

func (d *DiskCache) writeCacheDirFile(
	ctx context.Context,
	path string,
	openReader func(context.Context) (io.ReadCloser, error),
) (size int64, err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return 0, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && !errors.Is(closeErr, os.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}()

	from, err := openReader(ctx)
	if err != nil {
		return 0, &diskCacheSourceError{err: err}
	}
	defer from.Close()
	source := &diskCacheSourceReader{Reader: from}

	var buf []byte
	put := ioBufferPool.Get(&buf)
	defer put.Put()
	if _, err := io.CopyBuffer(f, source, buf); err != nil {
		if source.err != nil {
			return 0, &diskCacheSourceError{err: source.err}
		}
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := d.fileSync(f); err != nil {
		return 0, err
	}
	fadviseDontNeed(f, 0, 0)
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fileSize(stat), nil
}

// moveCacheDirFile moves a caller's file into an entry being written, copying it when it
// is on another filesystem.
func (d *DiskCache) moveCacheDirFile(ctx context.Context, from, to string) (int64, error) {
	if err := os.Rename(from, to); err != nil {
		var linkErr *os.LinkError
		if !errors.As(err, &linkErr) || os.IsNotExist(err) {
			return 0, &diskCacheSourceError{err: err}
		}
		return d.writeCacheDirFile(ctx, to, func(context.Context) (io.ReadCloser, error) {
			return os.Open(from)
		})
	}
	f, err := os.Open(to)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if err := d.fileSync(f); err != nil {
		return 0, err
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fileSize(stat), nil
}

// openDir opens files of the entry directory diskPath; its wait for the path shares
// deadline with the caller's.
func (d *DiskCache) openDir(ctx context.Context, name string, diskPath string, files []string, deadline *time.Time) ([]*os.File, error) {
	generation, ok := d.paths.startRead(ctx, diskPath, deadline)
	if !ok {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, moerr.NewFileNotFoundNoCtx(name)
	}
	defer d.paths.endRead(diskPath, generation)

	// Files are opened relative to one directory handle, so all of them come from the
	// same entry even if it is evicted and published again meanwhile.
	root, err := os.OpenRoot(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, moerr.NewFileNotFoundNoCtx(name)
		}
		return nil, err
	}
	defer root.Close()
	opened := make([]*os.File, 0, len(files))
	closeOpened := func() {
		for _, f := range opened {
			_ = f.Close()
		}
	}
	for i, file := range files {
		if i > 0 {
			runHook(d.hooks.beforeCacheDirFileOpen)
		}
		f, err := root.Open(file)
		if err != nil {
			closeOpened()
			if os.IsNotExist(err) {
				return nil, moerr.NewFileNotFoundNoCtx(name)
			}
			return nil, err
		}
		opened = append(opened, f)
	}

	if _, ok := d.cache.Get(ctx, diskPath); !ok {
		size, err := cacheDirSize(diskPath)
		if err != nil {
			closeOpened()
			if os.IsNotExist(err) {
				return nil, moerr.NewFileNotFoundNoCtx(name)
			}
			return nil, err
		}
		if owner, ok := d.paths.reindex(diskPath, generation); ok {
			var reindexErr error
			func() {
				defer func() { reindexErr = owner.release() }()
				d.cache.Set(ctx, diskPath, struct{}{}, size)
			}()
			if reindexErr != nil {
				closeOpened()
				return nil, reindexErr
			}
		}
	}
	return opened, nil
}

func cacheDirHasFiles(dir string, files []IndexCacheFile) bool {
	for _, f := range files {
		if info, err := os.Stat(filepath.Join(dir, f.Name)); err != nil || !info.Mode().IsRegular() {
			return false
		}
	}
	return true
}

// syncDir makes the names in dir durable before dir is published.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) && !errors.Is(err, syscall.ENOTSUP) {
		return err
	}
	return nil
}

// cacheDirSize returns the disk usage of the files in an entry directory.
func cacheDirSize(path string) (int64, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, err
	}
	var size int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			return 0, err
		}
		if info.Mode().IsRegular() {
			size += fileSize(info)
		}
	}
	return size, nil
}

func (d *DiskCache) removeCachePath(path string) error {
	if d.dirEntries && strings.HasSuffix(path, cacheDirSuffix) {
		return d.removeCacheDir(path)
	}
	return os.Remove(path)
}

// removeCacheDir renames the entry out of its name in one step, then removes it. Once
// renamed the entry is gone; files it fails to remove are kept as trash (see removeTrash).
func (d *DiskCache) removeCacheDir(path string) error {
	size := d.cacheDirBytes(path)
	trash, err := trashCacheDir(path, size)
	if err != nil {
		return err
	}
	if err := d.removeTrash(trash, size); err != nil {
		logutil.Error("index file cache: remove entry files", zap.String("path", trash), zap.Error(err))
	}
	return nil
}

// removeUnlistedDir removes an entry directory the load cannot list, as trash of unknown
// bytes.
func (d *DiskCache) removeUnlistedDir(ctx context.Context, path string) {
	runPathHook(d.hooks.beforeRemoveUnlistedDir, path)
	owner, err := d.paths.acquire(ctx, path, nil)
	if err != nil {
		return
	}
	defer owner.release()
	if d.cache.Contains(path) {
		return
	}
	if _, err := cacheDirSize(path); !os.IsPermission(err) {
		return
	}
	trash, err := trashCacheDir(path, -1)
	if err != nil {
		return
	}
	d.cache.Delete(ctx, path)
	if err := d.removeTrash(trash, -1); err != nil {
		logutil.Error("index file cache: remove entry files", zap.String("path", trash), zap.Error(err))
	}
}

// cacheDirBytes returns the bytes of entry directory path: those it is indexed with, those
// the load measured, or those it holds; -1 when none is known.
func (d *DiskCache) cacheDirBytes(path string) int64 {
	if size, ok := d.cache.Size(path); ok {
		return size
	}
	d.trash.mu.Lock()
	size, ok := d.trash.loading[path]
	d.trash.mu.Unlock()
	if ok {
		return size
	}
	if size, err := cacheDirSize(path); err == nil {
		return size
	}
	return -1
}

// cacheTrash holds the entry directories renamed for removal whose files are not all
// removed, with their bytes, -1 when unknown.
type cacheTrash struct {
	mu sync.Mutex
	m  map[string]int64
	// loading holds the bytes the load measured of the entry directories it has not
	// indexed yet.
	loading map[string]int64
	unknown int
	loaded  bool
	bytes   atomic.Int64
	// limit caps the capacity while trash of unknown bytes is kept: the bytes the cache
	// used when such trash was first kept after the load, or -1.
	limit atomic.Int64
}

// removeTrash removes trash, an entry directory of size bytes renamed for removal, -1
// when unknown. Trash it fails to remove is kept with its bytes, which count against the
// capacity until retryTrash removes it; its bytes are size, or those it was kept with,
// when it cannot be listed. While trash of unknown bytes is kept, the cache holds no more
// than it did once it was kept. Trash left when the process ends is removed when the
// cache loads, with the bytes its name records.
func (d *DiskCache) removeTrash(trash string, size int64) error {
	err := os.RemoveAll(trash)
	t := &d.trash
	t.mu.Lock()
	defer t.mu.Unlock()
	old, had := t.m[trash]
	if !had {
		old = -1
	}
	cur := int64(0)
	if err != nil {
		if n, sizeErr := cacheDirSize(trash); sizeErr == nil {
			cur = n
		} else {
			cur = max(size, old)
		}
		if t.m == nil {
			t.m = make(map[string]int64)
		}
		t.m[trash] = cur
	} else {
		delete(t.m, trash)
	}
	t.bytes.Add(max(cur, 0) - max(old, 0))
	if wasUnknown, isUnknown := had && old < 0, err != nil && cur < 0; wasUnknown != isUnknown {
		if isUnknown {
			t.unknown++
		} else {
			t.unknown--
		}
		d.limitTrashLocked()
	}
	return err
}

// limitTrashLocked sets the capacity limit of trash of unknown bytes.
func (d *DiskCache) limitTrashLocked() {
	t := &d.trash
	switch {
	case t.unknown == 0:
		t.limit.Store(-1)
	case t.loaded && t.limit.Load() < 0:
		used := d.cache.Used()
		t.limit.Store(used)
		logutil.Error("index file cache: trash of unknown size holds the cache at its bytes used",
			zap.String("path", d.path), zap.Int64("used", used))
	}
}

// trashHoldsPublications reports whether trash of unknown bytes is kept while the cache
// loads: nothing is published until the load has indexed the entries it found.
func (d *DiskCache) trashHoldsPublications() bool {
	t := &d.trash
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.unknown > 0 && !t.loaded
}

// startLoading starts the records of a load.
func (d *DiskCache) startLoading() {
	d.trash.mu.Lock()
	defer d.trash.mu.Unlock()
	d.trash.loaded = false
}

// loadingDir records the bytes the load measured of entry directory path, or ends the
// records when path is empty.
func (d *DiskCache) loadingDir(path string, size int64) {
	t := &d.trash
	t.mu.Lock()
	defer t.mu.Unlock()
	if path == "" {
		t.loading = nil
		t.loaded = true
		d.limitTrashLocked()
		return
	}
	if t.loading == nil {
		t.loading = make(map[string]int64)
	}
	t.loading[path] = size
}

// retryTrash removes the trash removeTrash failed to remove.
func (d *DiskCache) retryTrash() {
	d.trash.mu.Lock()
	paths := make([]string, 0, len(d.trash.m))
	for p := range d.trash.m {
		paths = append(paths, p)
	}
	d.trash.mu.Unlock()
	for _, p := range paths {
		_ = d.removeTrash(p, -1)
	}
}

// trashCacheDir renames entry directory path of size bytes, -1 when unknown, to a trash
// name that records a known size.
func trashCacheDir(path string, size int64) (string, error) {
	trash := fmt.Sprintf("%s.%d-%d", path, os.Getpid(), cacheDirTrashSeq.Add(1))
	if size >= 0 {
		trash += "." + strconv.FormatInt(size, 10)
	}
	trash += cacheDirTrashSuffix
	return trash, os.Rename(path, trash)
}

// trashSize returns the bytes trash name records, or -1.
func trashSize(trash string) int64 {
	s := strings.TrimSuffix(trash, cacheDirTrashSuffix)
	size, err := strconv.ParseUint(s[strings.LastIndexByte(s, '.')+1:], 10, 63)
	if err != nil {
		return -1
	}
	return int64(size)
}
