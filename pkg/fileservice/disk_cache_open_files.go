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
	"cmp"
	"os"
)

const (
	// diskCacheOpenFilesMax is the most cache files kept open for later reads.
	diskCacheOpenFilesMax = 1024
	// diskCacheMissingPathsMax is the most paths noted missing; past it they are forgotten.
	diskCacheMissingPathsMax = 1 << 16
)

// openCacheFile is a cache file kept open for the reads of its path.
type openCacheFile struct {
	file *os.File
	// refs counts the reads holding the file.
	refs int
	// dropped marks a file whose path changed; it is closed once no read holds it.
	dropped bool
	// used is when a read last took the file, in takes.
	used uint64
}

// openFilePool keeps, by disk path, the cache files reads opened, so the reads after
// them do not open them again. A change of a path drops its file. It is guarded by the
// disk cache's updatingPaths lock.
type openFilePool struct {
	m map[string]*openCacheFile
	// missing are the paths a read found no file at, until they change.
	missing map[string]struct{}
	takes   uint64
	// limit, when set, is the most files kept instead of diskCacheOpenFilesMax.
	limit int
}

// takeLocked returns the file kept open for path, held for a read, or nil.
func (o *openFilePool) takeLocked(path string) *openCacheFile {
	f := o.m[path]
	if f == nil {
		return nil
	}
	f.refs++
	o.takes++
	f.used = o.takes
	return f
}

// keepLocked keeps file open for path, held for the read that opened it. With the most
// files kept and all held, it keeps nothing and returns nil.
func (o *openFilePool) keepLocked(path string, file *os.File) (kept *openCacheFile, closeFile *os.File) {
	if o.m[path] != nil {
		return nil, nil
	}
	if len(o.m) >= cmp.Or(o.limit, diskCacheOpenFilesMax) {
		var lru string
		var oldest *openCacheFile
		for p, f := range o.m {
			if f.refs == 0 && (oldest == nil || f.used < oldest.used) {
				lru, oldest = p, f
			}
		}
		if oldest == nil {
			return nil, nil
		}
		delete(o.m, lru)
		closeFile = oldest.file
	}
	if o.m == nil {
		o.m = make(map[string]*openCacheFile)
	}
	o.takes++
	kept = &openCacheFile{file: file, refs: 1, used: o.takes}
	o.m[path] = kept
	return kept, closeFile
}

// releaseLocked ends a read's hold of f, and returns the file to close when f was
// dropped and no read holds it any more.
func (o *openFilePool) releaseLocked(f *openCacheFile) *os.File {
	f.refs--
	if f.dropped && f.refs == 0 {
		return f.file
	}
	return nil
}

// missingLocked reports whether path was noted missing.
func (o *openFilePool) missingLocked(path string) bool {
	_, ok := o.missing[path]
	return ok
}

// noteMissingLocked notes that path has no file.
func (o *openFilePool) noteMissingLocked(path string) {
	if o.missing == nil || len(o.missing) >= diskCacheMissingPathsMax {
		o.missing = make(map[string]struct{})
	}
	o.missing[path] = struct{}{}
}

// dropLocked forgets the file of path, whose path changes, and returns it to close when
// no read holds it.
func (o *openFilePool) dropLocked(path string) *os.File {
	delete(o.missing, path)
	f := o.m[path]
	if f == nil {
		return nil
	}
	delete(o.m, path)
	f.dropped = true
	if f.refs == 0 {
		return f.file
	}
	return nil
}

// dropAllLocked forgets every file, and returns those no read holds to close.
func (o *openFilePool) dropAllLocked() (files []*os.File) {
	o.missing = nil
	for p := range o.m {
		if f := o.dropLocked(p); f != nil {
			files = append(files, f)
		}
	}
	return files
}
