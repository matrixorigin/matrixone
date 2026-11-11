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
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
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
	on      bool
	mu      sync.Mutex
	touched map[string]time.Time // entries by when they were last marked used
}

// IndexObjectEntry returns the index-file cache entry of the object at file: the whole
// object, as its file IndexObjectFile.
func IndexObjectEntry(file string) string {
	return path.Join(indexObjectPrefix, path.Clean(file))
}

// IndexObjectCache returns fs's SHARED file service, which reads the objects of tables,
// and its index-file cache when it keeps index objects, those its IndexFileObject reads
// ask for; or nil, nil.
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
	return s.indexObjects.on && s.indexFileCache != nil
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
	if d.cache.Contains(diskPath) {
		return diskPath, true
	}
	select {
	case <-d.load.done:
		return diskPath, false
	default:
	}
	_, err = os.Stat(diskPath)
	return diskPath, err == nil
}

// readIndexObject fills the entries of vector not done from the object's entry in the
// index-file cache, when it holds the object.
func (s *S3FS) readIndexObject(ctx context.Context, vector *IOVector) error {
	diskPath, ok := s.indexObjectCached(vector.FilePath)
	if !ok {
		return nil
	}
	p, err := parseFilePathAtService(vector.FilePath, s.name)
	if err != nil {
		return nil
	}
	files, err := s.indexFileCache.Open(ctx, IndexObjectEntry(p.File), IndexObjectFile)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return nil
		}
		return err
	}
	file := files[0]
	defer file.Close()
	s.touchIndexObject(diskPath)
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

// touchIndexObject marks the entry at diskPath used, at most once per indexObjectTouch.
func (s *S3FS) touchIndexObject(diskPath string) {
	o := &s.indexObjects
	now := time.Now()
	o.mu.Lock()
	if o.touched == nil {
		o.touched = make(map[string]time.Time)
	}
	if now.Sub(o.touched[diskPath]) < indexObjectTouch {
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
