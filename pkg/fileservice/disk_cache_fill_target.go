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
	"os"
	pathpkg "path"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// fillTarget is the cache a fill of the disk cache publishes the file it writes into: the
// disk cache itself, or the index-file cache for the objects it keeps. The download, the
// path lock, the file written and the reads of it while it is written are the disk
// cache's either way.
type fillTarget interface {
	// heldBeforeLock reports whether the target already holds the file, before the fill
	// holds the path.
	heldBeforeLock(ctx context.Context) bool
	// held reports whether the target holds the file, while the fill holds the path.
	held(ctx context.Context) (bool, error)
	// reserve makes room for a file of size bytes about to be written.
	reserve(ctx context.Context, size int64)
	// noteWritten tells the target the size of a whole object written.
	noteWritten(size int64)
	// createTemp creates the file the fill writes, on the file system publish renames it
	// within.
	createTemp() (*os.File, error)
	// publish makes the file written at tempPath, of size bytes, the target's file.
	publish(ctx context.Context, tempPath string, size int64) error
	// cache returns the cache whose files the target holds, which frees room when a write
	// of its files finds the disk full.
	cache() *DiskCache
}

func (t diskFillTarget) cache() *DiskCache { return t.d }

// diskFillTarget publishes into the disk cache, at diskPath.
type diskFillTarget struct {
	d        *DiskCache
	diskPath string
}

func (t diskFillTarget) heldBeforeLock(ctx context.Context) bool {
	if _, ok := t.d.cache.Get(ctx, t.diskPath); ok {
		if _, err := os.Stat(t.diskPath); err == nil {
			return true
		}
	}
	return false
}

func (t diskFillTarget) held(ctx context.Context) (bool, error) {
	if _, ok := t.d.cache.Get(ctx, t.diskPath); ok {
		if _, err := os.Stat(t.diskPath); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, err
		}
		// Repair the missing physical file and replace the existing index after the
		// rewrite so FIFO accounting uses the repaired file size.
	}
	if stat, err := os.Stat(t.diskPath); err == nil {
		if t.d.indexFoundFile(ctx, t.diskPath, stat) {
			return true, nil
		}
		// A file of an object deleted since the cache was created may be stale.
		if err := os.Remove(t.diskPath); err != nil && !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}

func (t diskFillTarget) reserve(ctx context.Context, size int64) {
	t.d.cache.Evict(ctx, nil, size)
}

func (t diskFillTarget) noteWritten(size int64) {
	t.d.noteWholeFile(size)
}

func (t diskFillTarget) createTemp() (*os.File, error) {
	return t.d.createCacheTempFile(filepath.Dir(t.diskPath))
}

func (t diskFillTarget) publish(ctx context.Context, tempPath string, size int64) error {
	if err := os.Rename(tempPath, t.diskPath); err != nil {
		return err
	}
	return t.index(ctx, size)
}

// index indexes the file published at diskPath, of size bytes.
func (t diskFillTarget) index(ctx context.Context, size int64) error {
	logutil.Debug("disk cache file written",
		zap.Any("path", t.diskPath),
	)
	if !t.d.cache.Replace(ctx, t.diskPath, struct{}{}, size) {
		t.d.cache.Set(ctx, t.diskPath, struct{}{}, size)
	}
	if !t.d.cache.Contains(t.diskPath) {
		if err := os.Remove(t.diskPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// entryFillTarget publishes the file of an entry into the disk cache, at diskPath, unless
// the entry's object was deleted after the entry was read.
type entryFillTarget struct {
	diskFillTarget
	filePath    string
	readDeletes uint64
}

// deletedSinceRead reports whether the entry's object was deleted after the entry was read.
func (t entryFillTarget) deletedSinceRead() bool {
	if t.readDeletes == 0 {
		return false
	}
	path, err := ParsePath(t.filePath)
	if err != nil {
		return false
	}
	return t.d.deletes.since(t.d.objectKey(path.File), t.readDeletes-1)
}

// wholeFillTarget publishes a whole object into the disk cache as the range [0, size) of
// filePath, at the size its fill finds.
type wholeFillTarget struct {
	d        *DiskCache
	filePath string
	key      string
	// mark is the delete count the size of the object is stored at.
	mark uint64
	// size is the object's size, once known; path is the range file of [0, size), once
	// the fill holds it.
	size     int64
	sized    bool
	path     string
	download *objectDownload
}

// wholePublication is a whole-object fill publishing its range file.
type wholePublication struct {
	key      string
	download *objectDownload
}

func (d *DiskCache) newWholeFillTarget(ctx context.Context, filePath string) *wholeFillTarget {
	mark, ok := ctx.Value(deletesMarkKey{}).(uint64)
	if !ok {
		mark = d.deletes.now()
	}
	t := &wholeFillTarget{d: d, filePath: filePath, key: d.objectKey(filePath), mark: mark}
	t.size, t.sized = d.prefetched.size(t.key)
	return t
}

// sizeAt sets the object's size.
func (t *wholeFillTarget) sizeAt(size int64) {
	t.size, t.sized = size, true
}

func (t *wholeFillTarget) cached(ctx context.Context) bool {
	if !t.sized {
		return false
	}
	path := t.d.pathForIOEntry(t.filePath, IOEntry{Size: t.size})
	if !(diskFillTarget{d: t.d, diskPath: path}).heldBeforeLock(ctx) {
		return false
	}
	t.d.storePrefetchedSize(t.key, t.size, t.mark)
	return true
}

func (t *wholeFillTarget) heldBeforeLock(ctx context.Context) bool { return t.cached(ctx) }

func (t *wholeFillTarget) held(ctx context.Context) (bool, error) { return t.cached(ctx), nil }

func (t *wholeFillTarget) reserve(ctx context.Context, size int64) {
	t.d.cache.Evict(ctx, nil, size)
}

func (t *wholeFillTarget) noteWritten(size int64) {
	t.d.noteWholeFile(size)
}

func (t *wholeFillTarget) createTemp() (*os.File, error) {
	return t.d.createCacheTempFile(filepath.Join(t.d.path, diskCacheShard(pathpkg.Clean(t.filePath))))
}

func (t *wholeFillTarget) cache() *DiskCache { return t.d }

// place makes the file of the fill holding w the range [0, n): it owns the range file,
// released with w's owner, waiting for it unless try is set. It reports false when the
// range file is not free under try, or is cached.
func (t *wholeFillTarget) place(ctx context.Context, n int64, w *fileWrite, try bool) (bool, error) {
	t.sizeAt(n)
	t.path = t.d.pathForIOEntry(t.filePath, IOEntry{Size: n})
	t.d.storePrefetchedSize(t.key, n, t.mark)
	cleanup := func() error { return t.d.removeUnindexedFile(t.path) }
	var owner *pathOwner
	if try {
		var ok bool
		if owner, ok = t.d.paths.tryAcquire(t.path, cleanup); !ok {
			return false, nil
		}
	} else {
		var err error
		if owner, err = t.d.paths.acquire(ctx, t.path, cleanup); err != nil {
			return false, err
		}
	}
	w.owner.releasesFirst(owner)
	held, err := diskFillTarget{d: t.d, diskPath: t.path}.held(ctx)
	return err == nil && !held, err
}

func (t *wholeFillTarget) publish(ctx context.Context, tempPath string, size int64) error {
	if t.download != nil {
		t.d.wholePublications.Store(t.path, wholePublication{key: t.key, download: t.download})
		defer t.d.wholePublications.Delete(t.path)
	}
	if err := t.rename(tempPath); err != nil {
		return err
	}
	return diskFillTarget{d: t.d, diskPath: t.path}.index(ctx, size)
}

// rename moves the file at tempPath to the range file, in its directory, which stays
// while the file is moved into it.
func (t *wholeFillTarget) rename(tempPath string) error {
	t.d.directoryMu.RLock()
	defer t.d.directoryMu.RUnlock()
	if err := os.MkdirAll(filepath.Dir(t.path), 0755); err != nil {
		return err
	}
	return os.Rename(tempPath, t.path)
}
