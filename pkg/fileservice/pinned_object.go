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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

const (
	pinnedObjectPrefix = "obj"
	pinnedObjectFile   = "data"
	// pinnedObjectParts is how many ranges of an object of at least pinnedObjectPartMin
	// bytes its download reads at once.
	pinnedObjectParts   = 4
	pinnedObjectPartMin = 16 << 20
	// pinnedObjectRetry is how long an object not pinned, as the cache had no room for it
	// or its download failed, is not queued again.
	pinnedObjectRetry = time.Minute
	// pinnedObjectRetriesMax is the most objects remembered as not pinned, or as used.
	pinnedObjectRetriesMax = 1 << 14
	// pinnedObjectTouch is the longest a read pinned object goes without being marked
	// used, so idle collection keeps it.
	pinnedObjectTouch = time.Minute
)

// pinnedObjects tracks the downloads of objects into the index-file cache.
type pinnedObjects struct {
	mu       sync.Mutex
	pending  map[string]*pinJob
	retry    map[string]time.Time // objects not pinned, by when they may be queued again
	touched  map[string]time.Time // pinned objects, by when they were last marked used
	reserved int64                // bytes of downloads under way
	queue    chan string
	start    sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// pinJob is an object queued for download; done is closed when its pin ends or is
// dropped before it starts.
type pinJob struct {
	started bool
	done    chan struct{}
}

// pinnedObjectEntry returns the index-file cache entry of the object at file.
func pinnedObjectEntry(file string) string {
	return path.Join(pinnedObjectPrefix, path.Clean(file))
}

// pinsObjects reports whether reads of IndexFileObject objects use the index-file cache.
func (s *S3FS) pinsObjects() bool {
	return s.pinObjectsOn && s.indexFileCache != nil
}

// holdsPinned reports whether the index-file cache holds the entry at diskPath. While the
// cache loads at startup, an entry on disk is not yet indexed, so its directory decides.
func (s *S3FS) holdsPinned(diskPath string) bool {
	d := s.indexFileCache.disk
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

// readPinnedObject fills the entries of vector not done from the object's file in the
// index-file cache, when it holds the object.
func (s *S3FS) readPinnedObject(ctx context.Context, vector *IOVector) error {
	p, err := parseFilePathAtService(vector.FilePath, s.name)
	if err != nil {
		return nil
	}
	c := s.indexFileCache
	name := pinnedObjectEntry(p.File)
	diskPath, err := c.disk.pathForDir(name)
	if err != nil || !s.holdsPinned(diskPath) {
		return nil
	}
	files, err := c.Open(ctx, name, pinnedObjectFile)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil
		}
		return err
	}
	file := files[0]
	defer file.Close()
	s.touchPinned(diskPath)
	var allocator CacheDataAllocator
	if s.memCache != nil {
		allocator = s.memCache
	}
	var read int64
	for i := range vector.Entries {
		e := &vector.Entries[i]
		if e.done {
			continue
		}
		if e.Size < 0 {
			return nil
		}
		if err := e.readFrom(ctx, io.NewSectionReader(file, e.Offset, e.Size), allocator); err != nil {
			return err
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

// touchPinned marks the pinned entry at diskPath used, at most once per pinnedObjectTouch.
func (s *S3FS) touchPinned(diskPath string) {
	o := &s.pinned
	now := time.Now()
	o.mu.Lock()
	if o.touched == nil {
		o.touched = make(map[string]time.Time)
	}
	if now.Sub(o.touched[diskPath]) < pinnedObjectTouch {
		o.mu.Unlock()
		return
	}
	if len(o.touched) >= pinnedObjectRetriesMax {
		clear(o.touched)
	}
	o.touched[diskPath] = now
	o.mu.Unlock()
	_ = os.Chtimes(diskPath, now, now)
}

// pinRoute queues the object at filePath, of size bytes or 0 when unknown, for download
// into the index-file cache, and reports whether the object is pinned or being pinned:
// then its reads take their ranges alone and its prefetches are not downloaded into the
// disk cache. An object the cache has no room for is left to the disk cache.
func (s *S3FS) pinRoute(filePath string, size int64) bool {
	if !s.pinsObjects() {
		return false
	}
	o := &s.pinned
	o.start.Do(func() { s.startPins() })
	p, err := parseFilePathAtService(filePath, s.name)
	if err != nil {
		return false
	}
	c := s.indexFileCache
	diskPath, err := c.disk.pathForDir(pinnedObjectEntry(p.File))
	if err != nil {
		return false
	}
	if s.holdsPinned(diskPath) {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ctx.Err() != nil {
		return false
	}
	if _, ok := o.pending[p.File]; ok {
		return true
	}
	if time.Now().Before(o.retry[p.File]) {
		return false
	}
	if size > 0 && c.disk.cache.Used()+o.reserved+size > c.disk.capacityFunc() {
		return false
	}
	select {
	case o.queue <- p.File:
		o.pending[p.File] = &pinJob{done: make(chan struct{})}
		return true
	default:
		return false
	}
}

// startPins starts the workers that download queued objects.
func (s *S3FS) startPins() {
	o := &s.pinned
	o.mu.Lock()
	o.ctx, o.cancel = context.WithCancel(context.Background())
	o.pending = make(map[string]*pinJob)
	o.retry = make(map[string]time.Time)
	o.queue = make(chan string, 1024)
	o.mu.Unlock()
	workers := 4
	if s.diskCache != nil {
		workers = s.diskCache.prefetchWorkers()
	}
	for range workers {
		o.wg.Add(1)
		go func() {
			defer o.wg.Done()
			for {
				select {
				case <-o.ctx.Done():
					return
				case file := <-o.queue:
					o.mu.Lock()
					job, ok := o.pending[file]
					if ok {
						job.started = true
					}
					o.mu.Unlock()
					if !ok {
						continue
					}
					pinned := s.pinObject(o.ctx, file)
					o.mu.Lock()
					close(job.done)
					delete(o.pending, file)
					if !pinned {
						if len(o.retry) >= pinnedObjectRetriesMax {
							clear(o.retry)
						}
						o.retry[file] = time.Now().Add(pinnedObjectRetry)
					}
					o.mu.Unlock()
				}
			}
		}()
	}
}

// pinObject downloads the object at file into the index-file cache when the cache has
// free room for it, so pinning itself evicts nothing. It reports whether the cache holds
// the object.
func (s *S3FS) pinObject(ctx context.Context, file string) bool {
	c := s.indexFileCache
	name := pinnedObjectEntry(file)
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return false
	}
	if s.holdsPinned(diskPath) {
		return true
	}
	if _, err := os.Stat(diskPath); err == nil {
		return true
	}
	size, ok := int64(0), false
	if s.diskCache != nil {
		size, ok = s.diskCache.prefetched.size(s.diskCache.pathForFile(file))
	}
	if !ok {
		if size, err = s.storage.Stat(ctx, s.pathToKey(file)); err != nil {
			return false
		}
	}
	o := &s.pinned
	o.mu.Lock()
	if c.disk.cache.Used()+o.reserved+size > c.disk.capacityFunc() {
		o.mu.Unlock()
		return false
	}
	o.reserved += size
	o.mu.Unlock()
	defer func() {
		o.mu.Lock()
		o.reserved -= size
		o.mu.Unlock()
	}()

	// The download holds connections of the disk cache's download budget.
	parts := int64(1)
	if size >= pinnedObjectPartMin {
		parts = pinnedObjectParts
	}
	if d := s.diskCache; d != nil {
		if !d.reserveDownloadConns(parts) {
			if parts == 1 || !d.reserveDownloadConns(1) {
				return false
			}
			parts = 1
		}
		defer d.downloadConns.Add(-parts)
	}

	tmp, remove, err := c.CreateTempFile()
	if err != nil {
		return false
	}
	defer remove()
	if err := s.downloadObject(ctx, file, size, parts, tmp); err != nil {
		_ = tmp.Close()
		logutil.Debug("fileservice: pin object", zap.String("file", file), zap.Error(err))
		return false
	}
	if err := tmp.Close(); err != nil {
		return false
	}
	if _, err := c.Put(ctx, name, []IndexCacheFile{{Name: pinnedObjectFile, Path: tmp.Name()}}); err != nil {
		logutil.Debug("fileservice: pin object", zap.String("file", file), zap.Error(err))
		return false
	}
	return c.disk.cache.Contains(diskPath)
}

// downloadObject writes the size bytes of the object at file to dst, reading it as parts
// ranges at once.
func (s *S3FS) downloadObject(ctx context.Context, file string, size, parts int64, dst *os.File) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	part := (size + parts - 1) / parts
	errs := make(chan error, parts)
	n := 0
	for start := int64(0); start < size; start += part {
		n++
		go func(start, end int64) {
			err := s.downloadRange(ctx, file, start, end, dst)
			if err != nil {
				cancel()
			}
			errs <- err
		}(start, min(start+part, size))
	}
	var err error
	for range n {
		err = errors.Join(err, <-errs)
	}
	return err
}

// downloadRange writes the bytes [start, end) of the object at file to dst at start.
func (s *S3FS) downloadRange(ctx context.Context, file string, start, end int64, dst *os.File) error {
	r, err := s.storage.Read(ctx, s.pathToKey(file), ptrTo(start), ptrTo(end))
	if err != nil {
		return err
	}
	defer r.Close()
	n, err := io.Copy(io.NewOffsetWriter(dst, start), r)
	if err != nil {
		return err
	}
	if n != end-start {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// unpinObjects deletes the index-file cache's copies of the objects of filePaths. A pin
// queued and not started is dropped; one under way is waited for, so none is pinned
// after it returns.
func (s *S3FS) unpinObjects(ctx context.Context, filePaths []string) error {
	if !s.pinsObjects() {
		return nil
	}
	c := s.indexFileCache
	o := &s.pinned
	for _, filePath := range filePaths {
		p, err := parseFilePathAtService(filePath, s.name)
		if err != nil {
			return err
		}
		o.mu.Lock()
		job := o.pending[p.File]
		if job != nil && !job.started {
			delete(o.pending, p.File)
			close(job.done)
			job = nil
		}
		o.mu.Unlock()
		if job != nil {
			select {
			case <-job.done:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := c.Delete(ctx, pinnedObjectEntry(p.File)); err != nil {
			return err
		}
	}
	return nil
}

func (o *pinnedObjects) close() {
	o.mu.Lock()
	cancel := o.cancel
	o.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	o.wg.Wait()
	o.mu.Lock()
	for file, job := range o.pending {
		close(job.done)
		delete(o.pending, file)
	}
	o.mu.Unlock()
}
