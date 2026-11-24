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
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

const (
	indexObjectPrefix = "obj"
	// IndexObjectFile is the file of an index object's entry in the index-file cache.
	IndexObjectFile = "data"
	// indexObjectTouch is the longest an index object read here goes without being marked
	// used, so idle collection keeps it.
	indexObjectTouch = time.Minute
	// indexObjectsTouchedMax is the most index objects remembered as marked used.
	indexObjectsTouchedMax = 1 << 14
)

// indexObjects is the state of the index objects an S3FS reads from its index-file cache.
type indexObjects struct {
	mu      sync.Mutex
	touched map[string]time.Time // entries by when they were last marked used
}

// IndexObjectEntry returns the index-file cache entry of the object at file: the whole
// object, as its file IndexObjectFile.
func IndexObjectEntry(file string) string {
	return path.Join(indexObjectPrefix, path.Clean(file))
}

// IndexObjectCache returns fs's SHARED file service, which reads the objects of tables,
// and its index-file cache, which keeps the objects its IndexFileObject reads ask for;
// or nil, nil when it has none.
func IndexObjectCache(fs FileService) (*IndexFileCache, FileService) {
	if fs == nil {
		return nil, nil
	}
	shared, err := Get[FileService](fs, defines.SharedFileServiceName)
	if err != nil {
		return nil, nil
	}
	if s, ok := shared.(*S3FS); ok && s.cachesIndexObjects() {
		return s.indexFileCache, s
	}
	return nil, nil
}

func (s *S3FS) cachesIndexObjects() bool {
	// Index objects enter the index-file cache through the disk cache's download.
	return s.indexFileCache != nil && s.diskCache != nil
}

// indexObject reports whether policy marks a read of an object the index-file cache keeps.
func (s *S3FS) indexObject(policy Policy) bool {
	return policy.Any(IndexFileObject) && s.cachesIndexObjects()
}

// indexObjectReadable reports whether a read under policy reads the index-file cache: a
// read that skips every cache read, not only the disk cache's, skips it.
func (s *S3FS) indexObjectReadable(policy Policy) bool {
	return s.indexObject(policy) && policy&SkipCacheReads != SkipCacheReads
}

// indexObjectFillable reports whether a fill under policy fills the index-file cache: one
// that skips every cache write skips it. A fill of an index object never fills the disk
// cache.
func (s *S3FS) indexObjectFillable(policy Policy) bool {
	return s.indexObject(policy) && policy&SkipCacheWrites != SkipCacheWrites
}

// cacheTier is the cache on disk the bytes a read takes from object storage are cached in.
type cacheTier int

const (
	noTier cacheTier = iota
	diskTier
	indexTier
)

// tierOf returns the cache on disk of a read under policy: the index-file cache for an
// index object, whatever the skips of the disk cache, otherwise the disk cache; none when
// the read skips its writes.
func (s *S3FS) tierOf(policy Policy) cacheTier {
	switch {
	case s.indexObject(policy):
		if s.indexObjectFillable(policy) {
			return indexTier
		}
	case s.diskCache != nil && !policy.Any(SkipDiskCacheWrites):
		return diskTier
	}
	return noTier
}

// readRange returns the range a read of vector reads from object storage, and whether it
// reads the whole object to fill its tier: an index object whenever its policy caches
// whole files, any other object when it also uses the disk cache.
func (s *S3FS) readRange(vector *IOVector) (min, max *int64, readFull bool) {
	p := vector.Policy
	if !s.indexObject(p) {
		return vector.readRange()
	}
	if p.wholeRead() && s.tierOf(p) == indexTier {
		return ptrTo[int64](0), nil, true
	}
	min, max = vector.readMinimalRange()
	return min, max, false
}

// fillTarget returns where a whole-object fill of the object at filePath for a read
// under policy publishes: the index-file cache for an IndexFileObject read of an object it
// keeps, otherwise the disk cache (nil).
func (s *S3FS) fillTarget(policy Policy, filePath string) fillTarget {
	if !s.indexObjectFillable(policy) {
		return nil
	}
	p, err := parseFilePathAtService(filePath, s.name)
	if err != nil {
		return nil
	}
	return indexFillTarget{s: s, filePath: filePath, name: IndexObjectEntry(p.File), mark: s.indexFileCache.DeleteMark()}
}

// indexFillTarget publishes the object at filePath into the index-file cache, as entry
// name, unless the object was deleted after mark.
type indexFillTarget struct {
	s        *S3FS
	filePath string
	name     string
	mark     uint64
}

func (t indexFillTarget) heldBeforeLock(context.Context) bool {
	return t.s.indexObjectHeld(t.filePath)
}

func (t indexFillTarget) held(context.Context) (bool, error) {
	return t.s.indexObjectHeld(t.filePath), nil
}

// reserve makes no room: the index-file cache evicts when the entry is put.
func (t indexFillTarget) reserve(context.Context, int64) {}

func (t indexFillTarget) noteWritten(int64) {}

func (t indexFillTarget) cache() *DiskCache { return t.s.indexFileCache.disk }

// createTemp creates the file in the index-file cache's directory, so publishing it is a
// rename on one file system.
func (t indexFillTarget) createTemp() (*os.File, error) {
	return t.s.indexFileCache.disk.createCacheTempFile(t.s.indexFileCache.disk.path)
}

func (t indexFillTarget) publish(ctx context.Context, tempPath string, _ int64) error {
	_, err := t.s.indexFileCache.PutUnlessDeleted(ctx, t.name, t.mark,
		[]IndexCacheFile{{Name: IndexObjectFile, Path: tempPath}})
	// The put moves the file or does not take it.
	if rmErr := os.Remove(tempPath); rmErr != nil && !os.IsNotExist(rmErr) {
		return rmErr
	}
	// A failed put is the index-file cache's, not a failed write of the disk cache.
	if err != nil && ctx.Err() == nil {
		logutil.Warn("fileservice: put index object into the index-file cache",
			zap.String("path", t.filePath), zap.Error(err))
		return nil
	}
	return err
}

// openResourcesExhausted reports whether err is an open failing for lack of file
// descriptors or memory.
func openResourcesExhausted(err error) bool {
	return errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ENOMEM) || errors.Is(err, syscall.EAGAIN)
}

// indexObjectCached returns the disk path of the entry of the object at filePath, and
// whether the index-file cache holds it. While the cache loads at startup, an entry on
// disk is not yet indexed, so its directory decides.
func (s *S3FS) indexObjectCached(filePath string) (string, bool) {
	p, err := parseFilePathAtService(filePath, s.name)
	if err != nil {
		return "", false
	}
	d := s.indexFileCache.disk
	diskPath, err := d.pathForDir(IndexObjectEntry(p.File))
	if err != nil {
		return "", false
	}
	return diskPath, d.dirCached(diskPath)
}

// indexObjectHeld reports whether the index-file cache holds the object at filePath with
// its file, so that a fill or prefetch of it has nothing to do.
func (s *S3FS) indexObjectHeld(filePath string) bool {
	diskPath, ok := s.indexObjectCached(filePath)
	if !ok {
		return false
	}
	_, err := os.Stat(filepath.Join(diskPath, IndexObjectFile))
	return err == nil
}

// readIndexObject fills the entries of vector not done from the object's entry in the
// index-file cache, when it holds the object.
func (s *S3FS) readIndexObject(ctx context.Context, vector *IOVector) error {
	// The download registry is read before the cache, so an entry its download published
	// in between is found in the cache; the entries a download no longer serves are read
	// from the file it published.
	if err := s.readIndexDownload(ctx, vector); err != nil || vector.allDone() {
		return err
	}
	diskPath, ok := s.indexObjectCached(vector.FilePath)
	if !ok {
		return nil
	}
	p, err := parseFilePathAtService(vector.FilePath, s.name)
	if err != nil {
		return nil
	}
	name := IndexObjectEntry(p.File)
	// The read holds the entry, so it is not evicted while the read uses it.
	d := s.indexFileCache.disk
	d.holdDir(diskPath)
	defer d.releaseDir(diskPath, false)
	file, entry, err := s.indexFileCache.openEntryFile(ctx, name, IndexObjectFile)
	if err != nil {
		if entry != nil {
			defer entry.done()
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// The entries are read from the other caches or storage. The entry read is
		// dropped when it cannot be opened, or is indexed without its file; a lack of
		// resources to open it keeps it.
		if entry == nil || openResourcesExhausted(err) {
			return nil
		}
		missing := ""
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			missing = IndexObjectFile
		}
		s.indexFileCache.dropUnopened(ctx, name, missing, entry)
		return nil
	}
	defer entry.done()
	s.touchIndexObject(diskPath)
	var read int64
	for i := range vector.Entries {
		e := &vector.Entries[i]
		if e.done {
			continue
		}
		if e.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(p.File)
		}
		if e.Size < 0 {
			// The entry's file cannot tell its end from the object's: storage reads it.
			continue
		}
		from := &sourceErrReader{Reader: io.NewSectionReader(file, e.Offset, e.Size)}
		if err := e.readFrom(ctx, from, s); err != nil {
			if from.err == nil {
				return err
			}
			// The entry's file is shorter than the object or unreadable: the entry read
			// is dropped and the entries not done are read from the other caches or
			// storage.
			seen, statErr := file.Stat()
			if statErr != nil {
				return nil
			}
			s.indexFileCache.dropBroken(ctx, name, "", entry.gen, seen, IndexObjectFile)
			return nil
		}
		e.done = true
		e.fromCache = s.diskCache
		read += e.Size
	}
	if read > 0 {
		perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
			counter.FileService.DiskReadSize.Add(read)
		})
	}
	return nil
}

// readIndexDownload fills the entries of vector not done from the download of the object
// into the index-file cache under way, as a read of the disk cache does, whatever the
// read's skip of disk-cache reads.
func (s *S3FS) readIndexDownload(ctx context.Context, vector *IOVector) error {
	p, err := parseFilePathAtService(vector.FilePath, s.name)
	if err != nil || isDownload(ctx) {
		return nil
	}
	d := s.diskCache
	diskPath := d.pathForFile(p.File)
	if o := d.download(diskPath); o == nil || !o.indexFile {
		return nil
	}
	for i := range vector.Entries {
		e := &vector.Entries[i]
		if !e.ranged() || !e.materializes(d.memoryPrefetchMax) {
			continue
		}
		_, filled, err := d.readFromDownload(ctx, diskPath, e, vector)
		var failing *failingReadError
		if errors.As(err, &failing) {
			return failing.err
		}
		if filled {
			e.done, e.fromCache = true, d
		}
	}
	return nil
}

// touchIndexObject marks the entry at diskPath used, at most once per indexObjectTouch,
// or per half the idle TTL when that is shorter.
func (s *S3FS) touchIndexObject(diskPath string) {
	interval := indexObjectTouch
	if ttl := s.indexFileCache.idleTTL; ttl > 0 {
		interval = min(interval, ttl/2)
	}
	o := &s.indexObjects
	now := time.Now()
	o.mu.Lock()
	if o.touched == nil {
		o.touched = make(map[string]time.Time)
	}
	if now.Sub(o.touched[diskPath]) < interval {
		o.mu.Unlock()
		return
	}
	if len(o.touched) >= indexObjectsTouchedMax {
		clear(o.touched)
	}
	o.touched[diskPath] = now
	o.mu.Unlock()
	_ = os.Chtimes(diskPath, now, now)
}

// deleteIndexObjects deletes the index-file cache's entries of the objects of filePaths.
func (s *S3FS) deleteIndexObjects(ctx context.Context, filePaths []string) error {
	if !s.cachesIndexObjects() {
		return nil
	}
	for _, filePath := range filePaths {
		p, err := parseFilePathAtService(filePath, s.name)
		if err != nil {
			return err
		}
		if err := s.indexFileCache.Delete(ctx, IndexObjectEntry(p.File)); err != nil {
			return err
		}
	}
	return nil
}

// sourceErrReader records the error, io.EOF included, a read of its Reader returns.
type sourceErrReader struct {
	io.Reader
	err error
}

func (r *sourceErrReader) Read(buf []byte) (int, error) {
	n, err := r.Reader.Read(buf)
	if err != nil {
		r.err = err
	}
	return n, err
}
