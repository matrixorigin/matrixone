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
	"os"
	"sync"
	"time"
)

// pathOwners serializes the changes of each cache path: one owner at a time changes a
// path, and a read of a path starts only while it has no owner. A read is of one
// generation of the path, which ends when an owner takes the path.
type pathOwners struct {
	*sync.Cond
	owners map[string]*pathOwner
	// reads holds the current generation of each path a read has open.
	reads   map[string]pathReads
	lastGen uint64
	// openFiles are the cache files kept open for later reads.
	openFiles openFilePool
	// removeEvicted removes the file of a path evicted while it had an owner.
	removeEvicted func(path string, size int64) error
}

type pathReads struct {
	gen     uint64
	readers int
}

// pathOwner owns a path until released.
type pathOwner struct {
	paths   *pathOwners
	path    string
	cleanup func() error
	// evictionSize is the size of the file to remove on release, when evictionPending.
	evictionPending bool
	evictionSize    int64
	once            sync.Once
	err             error
}

type openedDiskCacheFile struct {
	file       *os.File
	generation uint64
	// kept is the file kept open for later reads, when the read holds one.
	kept *openCacheFile
}

func (p *pathOwners) init(openFilesMax int, removeEvicted func(path string, size int64) error) {
	p.Cond = sync.NewCond(new(sync.Mutex))
	p.owners = make(map[string]*pathOwner)
	p.reads = make(map[string]pathReads)
	p.openFiles.limit = openFilesMax
	p.removeEvicted = removeEvicted
}

// waitFreeLocked waits until path has no owner, or ctx ends.
func (p *pathOwners) waitFreeLocked(ctx context.Context, path string) error {
	if p.owners[path] != nil {
		stop := context.AfterFunc(ctx, func() {
			p.L.Lock()
			p.Broadcast()
			p.L.Unlock()
		})
		defer stop()
		for p.owners[path] != nil && ctx.Err() == nil {
			p.Wait()
		}
	}
	return ctx.Err()
}

func (p *pathOwners) ownLocked(path string, cleanup func() error) *pathOwner {
	o := &pathOwner{paths: p, path: path, cleanup: cleanup}
	p.owners[path] = o
	return o
}

// acquire waits until path has no owner, or ctx ends, and owns it; cleanup runs on
// release.
func (p *pathOwners) acquire(ctx context.Context, path string, cleanup func() error) (*pathOwner, error) {
	p.L.Lock()
	defer p.L.Unlock()
	if err := p.waitFreeLocked(ctx, path); err != nil {
		return nil, err
	}
	p.endReadsLocked(path)
	return p.ownLocked(path, cleanup), nil
}

// tryAcquire owns path when it has no owner.
func (p *pathOwners) tryAcquire(path string, cleanup func() error) (*pathOwner, bool) {
	p.L.Lock()
	defer p.L.Unlock()
	if p.owners[path] != nil {
		return nil, false
	}
	p.endReadsLocked(path)
	return p.ownLocked(path, cleanup), true
}

// reindex owns path when gen is its current read generation, keeping the generation.
func (p *pathOwners) reindex(path string, gen uint64) (*pathOwner, bool) {
	p.L.Lock()
	defer p.L.Unlock()
	if !p.currentLocked(path, gen) {
		return nil, false
	}
	return p.ownLocked(path, nil), true
}

// evict ends the generation of path and owns it, or, when path has an owner, has the
// owner remove its file of size on release. The decision is made under the lock that
// releases owners, so the eviction is not lost.
func (p *pathOwners) evict(path string, size int64) (*pathOwner, bool) {
	p.L.Lock()
	defer p.L.Unlock()
	p.endReadsLocked(path)
	if o := p.owners[path]; o != nil {
		o.evictionPending = true
		o.evictionSize = size
		return nil, false
	}
	return p.ownLocked(path, nil), true
}

// first has f run first on release.
func (o *pathOwner) first(f func()) {
	cleanup := o.cleanup
	o.cleanup = func() error {
		f()
		if cleanup == nil {
			return nil
		}
		return cleanup()
	}
}

// release runs the cleanup, removes the files evicted meanwhile and frees the path. Only
// the first call takes effect.
func (o *pathOwner) release() error {
	o.once.Do(func() {
		defer func() { o.err = errors.Join(o.err, o.paths.free(o)) }()
		if o.cleanup != nil {
			o.err = o.cleanup()
		}
	})
	return o.err
}

func (p *pathOwners) free(o *pathOwner) (err error) {
	for {
		p.L.Lock()
		if p.owners[o.path] != o {
			p.L.Unlock()
			return err
		}
		if !o.evictionPending {
			delete(p.owners, o.path)
			p.Broadcast()
			p.L.Unlock()
			return err
		}
		o.evictionPending = false
		size := o.evictionSize
		p.L.Unlock()
		err = errors.Join(err, p.removeEvicted(o.path, size))
	}
}

// startRead waits until path has no owner and starts a read of its current generation.
// The waits of one caller share deadline, set at its first wait to shortIOWaitDuration
// away.
func (p *pathOwners) startRead(ctx context.Context, path string, deadline *time.Time) (gen uint64, ok bool) {
	LogEvent(ctx, str_disk_cache_wait_update_complete_begin)
	defer LogEvent(ctx, str_disk_cache_wait_update_complete_end)
	p.L.Lock()
	defer p.L.Unlock()
	if p.owners[path] != nil {
		if deadline.IsZero() {
			*deadline = time.Now().Add(shortIOWaitDuration)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, *deadline)
		defer cancel()
	}
	if p.waitFreeLocked(ctx, path) != nil {
		return 0, false
	}
	r, ok := p.reads[path]
	if !ok {
		p.lastGen++
		r.gen = p.lastGen
	}
	r.readers++
	p.reads[path] = r
	return r.gen, true
}

// endRead ends a read of generation gen of path.
func (p *pathOwners) endRead(path string, gen uint64) {
	p.L.Lock()
	defer p.L.Unlock()
	if r, ok := p.reads[path]; ok && r.gen == gen {
		r.readers--
		if r.readers == 0 {
			delete(p.reads, path)
		} else {
			p.reads[path] = r
		}
	}
}

// endReadsLocked ends the generation of path and drops its kept file.
func (p *pathOwners) endReadsLocked(path string) {
	delete(p.reads, path)
	if f := p.openFiles.dropLocked(path); f != nil {
		_ = f.Close()
	}
}

// currentLocked reports whether path has not changed since a read of generation gen
// started.
func (p *pathOwners) currentLocked(path string, gen uint64) bool {
	r, ok := p.reads[path]
	return ok && r.gen == gen && p.owners[path] == nil
}

// takeKept returns the file kept open for path, held for a read, or nil.
func (p *pathOwners) takeKept(path string) *openCacheFile {
	p.L.Lock()
	defer p.L.Unlock()
	return p.openFiles.takeLocked(path)
}

// keep keeps file open for later reads, held for the read of generation gen of path,
// while path has not changed since; it returns the kept file, or nil.
func (p *pathOwners) keep(path string, gen uint64, file *os.File) *openCacheFile {
	var kept *openCacheFile
	var closeFile *os.File
	p.L.Lock()
	if p.currentLocked(path, gen) {
		kept, closeFile = p.openFiles.keepLocked(path, file)
	}
	p.L.Unlock()
	if closeFile != nil {
		_ = closeFile.Close()
	}
	return kept
}

// open opens the cache file at path for a read of generation gen: the file kept open for
// path, or a new one, kept open for later reads. A path noted missing is not opened again
// until it changes.
func (p *pathOwners) open(ctx context.Context, path string, gen uint64) (openedDiskCacheFile, bool) {
	p.L.Lock()
	kept := p.openFiles.takeLocked(path)
	missing := kept == nil && p.openFiles.missingLocked(path)
	p.L.Unlock()
	if kept != nil {
		return openedDiskCacheFile{file: kept.file, generation: gen, kept: kept}, true
	}
	if missing {
		return openedDiskCacheFile{}, false
	}
	LogEvent(ctx, str_disk_cache_file_open_begin)
	file, err := os.Open(path)
	LogEvent(ctx, str_disk_cache_file_open_end)
	if err != nil {
		if os.IsNotExist(err) {
			p.L.Lock()
			if p.currentLocked(path, gen) {
				p.openFiles.noteMissingLocked(path)
			}
			p.L.Unlock()
		}
		return openedDiskCacheFile{}, false
	}
	return openedDiskCacheFile{file: file, generation: gen, kept: p.keep(path, gen, file)}, true
}

// close ends the read of opened at path.
func (p *pathOwners) close(path string, opened openedDiskCacheFile) {
	if opened.kept == nil {
		_ = opened.file.Close()
	} else {
		p.L.Lock()
		f := p.openFiles.releaseLocked(opened.kept)
		p.L.Unlock()
		if f != nil {
			_ = f.Close()
		}
	}
	p.endRead(path, opened.generation)
}

// closeKept closes the files kept open.
func (p *pathOwners) closeKept() {
	p.L.Lock()
	files := p.openFiles.dropAllLocked()
	p.L.Unlock()
	for _, f := range files {
		_ = f.Close()
	}
}
