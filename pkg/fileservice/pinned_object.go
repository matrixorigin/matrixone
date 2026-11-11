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
	// pinnedObjectRetriesMax is the most objects remembered as not pinned.
	pinnedObjectRetriesMax = 1 << 14
)

// pinnedObjects tracks the downloads of objects into the index-file cache.
type pinnedObjects struct {
	mu       sync.Mutex
	pending  map[string]chan struct{} // closed when the object's pin ends
	retry    map[string]time.Time     // objects not pinned, by when they may be queued again
	reserved int64                    // bytes of downloads under way
	queue    chan string
	start    sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// pinnedObjectEntry returns the index-file cache entry of the object at file.
func pinnedObjectEntry(file string) string {
	return path.Join(pinnedObjectPrefix, path.Clean(file))
}

// pinsObjects reports whether reads of IndexFileObject objects use the index-file cache.
func (s *S3FS) pinsObjects() bool {
	return s.pinObjectsOn && s.indexFileCache != nil
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
	if err != nil || !c.disk.cache.Contains(diskPath) {
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

// pinObjects queues the objects of filePaths for download into the index-file cache.
// The downloads run in the background; a read never waits for them.
func (s *S3FS) pinObjects(filePaths ...string) {
	if !s.pinsObjects() {
		return
	}
	o := &s.pinned
	o.start.Do(func() {
		o.mu.Lock()
		o.ctx, o.cancel = context.WithCancel(context.Background())
		o.pending = make(map[string]chan struct{})
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
						pinned := s.pinObject(o.ctx, file)
						o.mu.Lock()
						close(o.pending[file])
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
	})
	for _, filePath := range filePaths {
		p, err := parseFilePathAtService(filePath, s.name)
		if err != nil {
			continue
		}
		diskPath, err := s.indexFileCache.disk.pathForDir(pinnedObjectEntry(p.File))
		if err != nil || s.indexFileCache.disk.cache.Contains(diskPath) {
			continue
		}
		o.mu.Lock()
		if _, ok := o.pending[p.File]; ok || o.ctx.Err() != nil || time.Now().Before(o.retry[p.File]) {
			o.mu.Unlock()
			continue
		}
		select {
		case o.queue <- p.File:
			o.pending[p.File] = make(chan struct{})
		default:
		}
		o.mu.Unlock()
	}
}

// pinObject downloads the object at file into the index-file cache when the cache has
// free room for it: pinning never evicts. It reports whether the cache holds the object.
func (s *S3FS) pinObject(ctx context.Context, file string) bool {
	c := s.indexFileCache
	name := pinnedObjectEntry(file)
	diskPath, err := c.disk.pathForDir(name)
	if err != nil {
		return false
	}
	if c.disk.cache.Contains(diskPath) {
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

	tmp, remove, err := c.CreateTempFile()
	if err != nil {
		return false
	}
	defer remove()
	if err := s.downloadObject(ctx, file, size, tmp); err != nil {
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

// downloadObject writes the size bytes of the object at file to dst, reading the parts
// of a large object at once.
func (s *S3FS) downloadObject(ctx context.Context, file string, size int64, dst *os.File) error {
	parts := int64(1)
	if size >= pinnedObjectPartMin {
		parts = pinnedObjectParts
	}
	part := (size + parts - 1) / parts
	errs := make(chan error, parts)
	for start := int64(0); start < size; start += part {
		go func(start, end int64) {
			errs <- s.downloadRange(ctx, file, start, end, dst)
		}(start, min(start+part, size))
	}
	var err error
	for start := int64(0); start < size; start += part {
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

// unpinObjects deletes the index-file cache's copies of the objects of filePaths after
// their pins under way end, so none is pinned after it returns.
func (s *S3FS) unpinObjects(ctx context.Context, filePaths []string) error {
	c := s.indexFileCache
	if c == nil {
		return nil
	}
	o := &s.pinned
	for _, filePath := range filePaths {
		p, err := parseFilePathAtService(filePath, s.name)
		if err != nil {
			return err
		}
		o.mu.Lock()
		done := o.pending[p.File]
		o.mu.Unlock()
		if done != nil {
			select {
			case <-done:
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
	if cancel != nil {
		cancel()
		o.wg.Wait()
	}
}
