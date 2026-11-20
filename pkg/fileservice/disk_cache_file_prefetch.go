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
	"sync"
	"sync/atomic"
	"time"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// filePrefetchObject is a FilePrefetchObject of the bytes [start, end) of an object: they
// download to their cache file, which reads of them join while it downloads. It is
// created unregistered, waits for one of the connections downloads may hold, holding
// nothing, then registers and downloads; or ends joining the prefetch object already
// holding its bytes.
type filePrefetchObject struct {
	start, end int64
	ctx        context.Context // of its download
	cancelRead context.CancelFunc
	endedCh    chan struct{}
	ready      chan struct{} // closed once reads can join its download, or it ended
	readyOnce  sync.Once

	mu       sync.Mutex
	download *objectDownload // set once the download is registered
	endErr   error           // why it cached nothing, once ended
	// joined is the prefetch object holding its bytes it joined instead of downloading
	// them, held until no read uses it.
	joined prefetchObject
	// holds counts the reads using one a read started, which is stopped when none uses it
	// before it is registered.
	holds       int
	readStarted bool
	registered  bool

	// d downloads the bytes of the object at filePath, at diskPath in d, through open,
	// with what needs asks for; closing is done when d closes.
	d                  *DiskCache
	filePath, diskPath string
	open               rangeOpener
	needs              flightNeeds
	closing            context.Context
}

// newFilePrefetchObject returns a FilePrefetchObject of the bytes [start, end), whose
// download runs under ctx without its cancellation.
func newFilePrefetchObject(ctx context.Context, start, end int64) *filePrefetchObject {
	p := &filePrefetchObject{start: start, end: end, endedCh: make(chan struct{}), ready: make(chan struct{})}
	ctx, p.cancelRead = context.WithCancel(ctx)
	p.ctx = context.WithValue(ctx, fileDownloadKey{}, p)
	return p
}

// newFileObject returns an unregistered FilePrefetchObject of the bytes [start, end) of
// the object at filePath, downloaded through open under ctx with what needs asks for.
func (d *DiskCache) newFileObject(ctx context.Context, filePath string, start, end int64, open rangeOpener, needs flightNeeds) *filePrefetchObject {
	p := newFilePrefetchObject(ctx, start, end)
	p.d, p.filePath, p.diskPath, p.open, p.needs = d, filePath, d.pathForFile(filePath), open, needs
	return p
}

func (p *filePrefetchObject) joinableDownload() *objectDownload {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.download
}

func (p *filePrefetchObject) markReady() { p.readyOnce.Do(func() { close(p.ready) }) }

type fileDownloadKey struct{}

// fileDownloadStarted gives the download of ctx's FilePrefetchObject to it.
func fileDownloadStarted(ctx context.Context, o *objectDownload) {
	if p, ok := ctx.Value(fileDownloadKey{}).(*filePrefetchObject); ok {
		p.mu.Lock()
		p.download = o
		p.mu.Unlock()
		p.markReady()
	}
}

// admit waits until p holds one of the connections downloads may hold, then registers p
// and returns it, for the caller to run. It returns instead, held, the prefetch object
// reads can join holding the bytes of p; neither once they are cached, or when downloads
// may hold no connection; errPrefetchClosed once the cache is closed, and errPrefetchDeleted
// after a delete of the object counted since its needs' deletes.
func (p *filePrefetchObject) admit() (prefetchObject, error) {
	d, needs := p.d, p.needs
	key := d.rangeObjectKey(p.filePath)
	f := &d.prefetchObjects
	for {
		f.mu.Lock()
		closing, ok := f.openLocked()
		if !ok {
			f.mu.Unlock()
			return nil, errPrefetchClosed
		}
		if needs.deletes != nil && d.deletes.since(p.diskPath, *needs.deletes) {
			f.mu.Unlock()
			return nil, errPrefetchDeleted
		}
		if key != "" {
			if _, cached := d.ranges.covering(key, p.start, p.end-p.start); cached {
				f.mu.Unlock()
				return nil, nil
			}
		}
		if o := f.holdingLocked(p.diskPath, p.start, p.end, time.Now(), d.downloadMaxExtraWait); o != nil {
			o.hold()
			f.mu.Unlock()
			return o, nil
		}
		if d.maxDownloadConns < 1 {
			f.mu.Unlock()
			metric.FSDiskCacheDownloadSkippedCounter.Inc()
			return nil, nil
		}
		connsFreed := d.downloadConnsFreed()
		if d.reserveDownloadConns(1) {
			p.closing = closing
			p.mu.Lock()
			p.registered = true
			p.mu.Unlock()
			f.registerLocked(p.diskPath, p)
			f.reads.Add(1)
			f.mu.Unlock()
			return p, nil
		}
		// A delete or the closing of the cache wakes the objects waiting for room.
		if f.freed == nil {
			f.freed = make(chan struct{})
		}
		freed := f.freed
		f.mu.Unlock()
		if needs.waiting != nil {
			needs.waiting()
		}
		select {
		case <-freed:
		case <-connsFreed:
		case <-p.ctx.Done():
			return nil, p.ctx.Err()
		}
	}
}

// run downloads the bytes of p, registered, to their cache file, frees the connection p
// holds, and ends p. The closing of the cache or a delete of the object cancels it.
func (p *filePrefetchObject) run() (bool, error) {
	d := p.d
	defer d.prefetchObjects.reads.Done()
	defer context.AfterFunc(p.closing, p.cancelRead)()
	defer d.releaseDownloadConns(1)
	return d.downloadFile(p.ctx, p.filePath, p.start, p.end, p.open, d.endFilePrefetch(p.diskPath, p))
}

// endJoined ends p, never registered, having joined o, which may be nil, or with why it
// cached nothing.
func (p *filePrefetchObject) endJoined(o prefetchObject, err error) {
	p.mu.Lock()
	p.joined, p.endErr = o, err
	drop := o != nil && p.holds == 0
	if drop {
		p.joined = nil
	}
	p.mu.Unlock()
	if drop {
		o.release()
	}
	p.cancelRead()
	close(p.endedCh)
	p.markReady()
}

// prefetch admits p and, once registered, runs it; it ends p otherwise.
func (p *filePrefetchObject) prefetch() (bool, error) {
	o, err := p.admit()
	if o == prefetchObject(p) {
		return p.run()
	}
	p.endJoined(o, err)
	return false, err
}

// startFilePrefetch registers a FilePrefetchObject of the bytes [start, end) of the
// object at objectPath for a whole-object download, which reads join once the download,
// holding its connections, is registered. It returns the context of its download, which
// a delete of the object cancels, and the function ending it with why it cached nothing.
func (d *DiskCache) startFilePrefetch(ctx context.Context, objectPath string, start, end int64) (context.Context, func(error)) {
	p := newFilePrefetchObject(ctx, start, end)
	f := &d.prefetchObjects
	f.mu.Lock()
	f.registerLocked(objectPath, p)
	f.mu.Unlock()
	return p.ctx, d.endFilePrefetch(objectPath, p)
}

// endFilePrefetch returns the function ending the registered FilePrefetchObject p of the
// object at objectPath with why it cached nothing.
func (d *DiskCache) endFilePrefetch(objectPath string, p *filePrefetchObject) func(error) {
	return func(err error) {
		f := &d.prefetchObjects
		f.mu.Lock()
		f.forgetLocked(objectPath, p)
		f.mu.Unlock()
		p.cancelRead()
		p.mu.Lock()
		p.endErr = err
		p.mu.Unlock()
		close(p.endedCh)
		p.markReady()
	}
}

// holdingLocked returns the prefetch object of the object at objectPath holding the bytes
// [start, end) that a FilePrefetchObject joins instead of downloading them: the fastest
// one reads can join that caches them, or a registered FilePrefetchObject a read started.
func (f *prefetchObjects) holdingLocked(objectPath string, start, end int64, now time.Time, extraWait time.Duration) prefetchObject {
	if o := f.fastestLocked(objectPath, start, end, now, extraWait, true); o != nil {
		return o
	}
	for _, o := range f.m[objectPath] {
		if p, ok := o.(*filePrefetchObject); ok && p.readStarted && p.start <= start && end <= p.end {
			return p
		}
	}
	return nil
}

// prefetchFile downloads, as a FilePrefetchObject, the bytes [start, end) of the object
// at filePath through open to their cache file, once it holds a connection downloads may
// hold. A delete of the object cancels it, and it is not cached after a delete counted
// since it began.
func (d *DiskCache) prefetchFile(ctx context.Context, filePath string, start, end int64, open rangeOpener) (bool, error) {
	p := d.newFileObject(ctx, filePath, start, end, open, flightNeeds{})
	return p.prefetch()
}

// downloadFile downloads under ctx, the context of a FilePrefetchObject, its bytes [start,
// end) of the object at filePath through open, then ends it through done.
func (d *DiskCache) downloadFile(ctx context.Context, filePath string, start, end int64, open rangeOpener, done func(error)) (bool, error) {
	var written bool
	var err error
	defer func() { done(err) }()
	readDeletes := d.readDeletes()
	diskPath := d.pathForIOEntry(filePath, IOEntry{Offset: start, Size: end - start})
	// objectEnd is where a source ended before its range did: the object's size.
	var objectEnd atomic.Int64
	openReader := func(ctx context.Context) (io.ReadCloser, error) {
		return &lazyObjectReader{ctx: ctx, read: func(ctx context.Context, min, max *int64) (io.ReadCloser, error) {
			from, to := start+*min, end
			if max != nil {
				to = start + *max
			}
			r, err := open(ctx, from, to)
			if err != nil {
				return nil, err
			}
			return &objectEndReader{ReadCloser: r, at: from, end: to, objectEnd: &objectEnd}, nil
		}}, nil
	}
	layout := func(context.Context) (int64, int64, error) { return end - start, 0, nil }
	written, err = d.publishUnlessDeleted(filePath, IOEntry{readDeletes: readDeletes}, nil, func(claim func() bool) (bool, error) {
		return d.writeFileWithFinalizeMode(ctx, diskPath, openReader, false, claim, rangeFill, layout)
	})
	if err != nil {
		if size := objectEnd.Load(); size > start {
			// A later run of the object ends at its size.
			d.storePrefetchedSize(d.pathForFile(filePath), size, readDeletes-1)
		}
		err = d.observeWriteResult(diskPath, err)
		return false, err
	}
	if written {
		err = d.observeWriteResult(diskPath, nil)
	}
	return written, err
}

// objectEndReader reads a source of the bytes [at, end) of an object. A source that ends
// before end fails the read, and objectEnd holds where it ended.
type objectEndReader struct {
	io.ReadCloser
	at, end   int64
	objectEnd *atomic.Int64
}

func (r *objectEndReader) Read(b []byte) (int, error) {
	n, err := r.ReadCloser.Read(b)
	r.at += int64(n)
	if err == io.EOF && r.at < r.end {
		r.objectEnd.Store(r.at)
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

// fileDownload returns the download of a FilePrefetchObject of the object at objectPath
// holding the bytes [lo, hi), and where its bytes start in the object.
func (d *DiskCache) fileDownload(objectPath string, lo, hi int64) (*objectDownload, int64) {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, o := range f.m[objectPath] {
		if p, ok := o.(*filePrefetchObject); ok && p.start <= lo && hi <= p.end {
			if download := p.joinableDownload(); download != nil {
				return download, p.start
			}
		}
	}
	return nil, 0
}
