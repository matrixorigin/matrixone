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
	"errors"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

const (
	cacheDirSuffix      = ".mofsdir"
	cacheDirTempSuffix  = cacheDirSuffix + ".tmp"
	cacheDirTrashSuffix = cacheDirSuffix + ".trash"
)

var cacheDirTrashSeq atomic.Uint64

// IndexFileCache keeps whole index files on local disk. An entry is a directory of named
// files that is published, evicted and deleted as a unit. Capacity, eviction, per-path
// coordination and reuse after restart are those of DiskCache.
type IndexFileCache struct {
	disk *DiskCache
}

// IndexCacheFile is one file of an entry written by IndexFileCache.Put.
type IndexCacheFile struct {
	Name string
	Open func(ctx context.Context) (io.ReadCloser, error)
}

// NewIndexFileCache returns an index file cache rooted at path.
func NewIndexFileCache(
	ctx context.Context,
	path string,
	capacity fscache.CapacityFunc,
	name string,
) (*IndexFileCache, error) {
	disk, err := newDiskCache(ctx, path, capacity, nil, true, nil, name, "", true)
	if err != nil {
		return nil, err
	}
	return &IndexFileCache{disk: disk}, nil
}

// Close stops the cache. Entries stay on disk.
func (c *IndexFileCache) Close(ctx context.Context) {
	c.disk.Close(ctx)
}

// Put publishes entry name with files. It returns false, with no error, when the entry
// already exists or the cache could not be written; a source error is returned.
func (c *IndexFileCache) Put(ctx context.Context, name string, files []IndexCacheFile) (bool, error) {
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return false, err
	}
	if err := checkCacheDirFiles(files); err != nil {
		return false, err
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
	return c.disk.openDir(ctx, name, diskPath, files)
}

// Delete removes entry name.
func (c *IndexFileCache) Delete(ctx context.Context, name string) error {
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return err
	}
	done, err := c.disk.startUpdateContext(ctx, diskPath)
	if err != nil {
		return err
	}
	defer done()
	if err := removeCacheDir(diskPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	c.disk.cache.Delete(ctx, diskPath)
	return nil
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
	return filepath.Join(d.path, diskCacheShard(clean), toOSPath(clean)+cacheDirSuffix), nil
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
	doneUpdate, err := d.startUpdateWithCleanupContext(ctx, diskPath, func() error {
		return d.removeUnindexedFile(diskPath)
	})
	if err != nil {
		return false, err
	}
	defer func() {
		d.mergeWriteCleanupError(diskPath, &err, doneUpdate())
	}()
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if size, err := cacheDirSize(diskPath); err == nil {
		if _, ok := d.cache.Get(ctx, diskPath); !ok {
			d.cache.Set(ctx, diskPath, struct{}{}, size)
		}
		return false, nil
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
		n, err := d.writeCacheDirFile(ctx, filepath.Join(tmp, f.Name), f.Open)
		if err != nil {
			return false, err
		}
		size += n
	}
	d.cache.Evict(ctx, nil, size)

	if d.beforeFilePublication != nil {
		d.beforeFilePublication()
	}
	if err := ctx.Err(); err != nil {
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

	if !d.cache.Replace(ctx, diskPath, struct{}{}, size) {
		d.cache.Set(ctx, diskPath, struct{}{}, size)
	}
	if !d.cache.Contains(diskPath) {
		if err := removeCacheDir(diskPath); err != nil && !os.IsNotExist(err) {
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
	if d.beforeCacheTempFileCreate != nil {
		d.beforeCacheTempFileCreate()
	}
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

func (d *DiskCache) openDir(ctx context.Context, name string, diskPath string, files []string) ([]*os.File, error) {
	var deadline time.Time
	generation, ok := d.startPathReadWithin(ctx, diskPath, &deadline, shortIOWaitDuration)
	if !ok {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, moerr.NewFileNotFoundNoCtx(name)
	}
	defer d.finishPathRead(diskPath, generation)

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
		if i > 0 && d.beforeCacheDirFileOpen != nil {
			d.beforeCacheDirFileOpen()
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
		if doneReindex, ok := d.tryStartReadReindex(diskPath, generation); ok {
			var reindexErr error
			func() {
				defer func() { reindexErr = doneReindex() }()
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
		return removeCacheDir(path)
	}
	return os.Remove(path)
}

// removeCacheDir renames the entry out of its name in one step, then removes it.
func removeCacheDir(path string) error {
	trash := fmt.Sprintf("%s.%d-%d%s", path, os.Getpid(), cacheDirTrashSeq.Add(1), cacheDirTrashSuffix)
	if err := os.Rename(path, trash); err != nil {
		return err
	}
	return os.RemoveAll(trash)
}
