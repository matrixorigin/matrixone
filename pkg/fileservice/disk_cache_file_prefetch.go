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
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// filePrefetchObject is a FilePrefetchObject of the bytes [start, end) of an object: they
// download to their cache file, which reads of them join while it downloads. One of a run
// is created unregistered, waits for one of the connections downloads may hold, holding
// nothing, then registers and downloads.
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
	// holds counts the reads using one a read started, which is stopped when none uses it
	// before its download is registered.
	holds       int
	readStarted bool
	// declined is set on one a read started past the download of its range it declined.
	declined bool
	// holdStarter is set when the download holds a reader for the read that started p,
	// which reads it through readOwnAt, so that its file is not closed before that read
	// copies its bytes: starterHeld while it holds one, starterDone once the read no longer
	// needs it.
	holdStarter, starterHeld, starterDone bool
	// connOnce frees the connection it holds once registered.
	connOnce sync.Once
	// whole is set on the download of a whole object, [0, size) or [0, end of object);
	// stopOnClose, set once it is registered, ends its stop on the closing of the cache.
	whole       bool
	stopOnClose func() bool
	// content is set on a fill of bytes the write path holds, which is registered with
	// its download and ends once its file is published.
	content bool
	// handedOff is set once its file is handed to an async finalize: it stays registered
	// until the file is published.
	handedOff bool

	prefetchTarget
}

// newFilePrefetchObject returns a FilePrefetchObject of the bytes [start, end), whose
// download runs under ctx.
func newFilePrefetchObject(ctx context.Context, start, end int64) *filePrefetchObject {
	p := &filePrefetchObject{start: start, end: end, endedCh: make(chan struct{}), ready: make(chan struct{})}
	ctx, p.cancelRead = context.WithCancel(ctx)
	p.ctx = context.WithValue(ctx, fileDownloadKey{}, p)
	return p
}

// newFileObject returns an unregistered FilePrefetchObject of the bytes [start, end) of
// the object at filePath, downloaded through open under ctx with what needs asks for, a
// connection downloads may hold and caching included.
func (d *DiskCache) newFileObject(ctx context.Context, filePath string, start, end int64, open rangeOpener, needs flightNeeds) *filePrefetchObject {
	p := newFilePrefetchObject(ctx, start, end)
	needs.conn, needs.caching = true, true
	p.prefetchTarget = prefetchTarget{d: d, filePath: filePath, diskPath: d.pathForFile(filePath), open: open, needs: needs}
	return p
}

type deletesMarkKey struct{}

// withDeletesMark returns ctx under which a whole-object download is not registered after
// a delete of its object counted since mark.
func withDeletesMark(ctx context.Context, mark uint64) context.Context {
	return context.WithValue(ctx, deletesMarkKey{}, mark)
}

// newWholeObject returns an unregistered FilePrefetchObject of the whole object at
// diskPath, downloaded under ctx into the index-file cache when index is set. It needs a
// connection downloads may hold, is not registered after a delete of the object since the
// mark of ctx, or since now, and, while it waits, tells the prefetcher whose download ctx
// is for that reads need not wait for it.
func (d *DiskCache) newWholeObject(ctx context.Context, diskPath string, index bool) *filePrefetchObject {
	p := newFilePrefetchObject(ctx, 0, math.MaxInt64)
	deletes, ok := ctx.Value(deletesMarkKey{}).(uint64)
	if !ok {
		deletes = d.deletes.now()
	}
	needs := flightNeeds{conn: true, deletes: &deletes, waiting: func() { downloadStarted(ctx) }}
	p.prefetchTarget = prefetchTarget{d: d, diskPath: diskPath, needs: needs, index: index}
	p.whole = true
	return p
}

// newContentFill registers and returns the fill of the whole object at diskPath from the
// bytes the write path holds, whose download is o.
func (d *DiskCache) newContentFill(diskPath string, o *objectDownload) *filePrefetchObject {
	p := newFilePrefetchObject(context.Background(), 0, math.MaxInt64)
	p.prefetchTarget = prefetchTarget{d: d, diskPath: diskPath}
	p.whole, p.content = true, true
	f := &d.prefetchObjects
	f.mu.Lock()
	f.registerLocked(diskPath, p)
	f.mu.Unlock()
	p.started(o)
	return p
}

// wholeFill returns the registered fill of the whole object at diskPath whose download
// started, of the index-file cache when index is set, or nil.
func (d *DiskCache) wholeFill(diskPath string, index bool) *filePrefetchObject {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.wholeFillLocked(diskPath, index)
}

func (f *prefetchObjects) wholeFillLocked(diskPath string, index bool) *filePrefetchObject {
	for _, o := range f.m[diskPath] {
		if p, ok := o.(*filePrefetchObject); ok && p.whole && p.index == index && p.joinableDownload() != nil {
			return p
		}
	}
	return nil
}

// endAt sets the end of p, the fill of a whole object, at the object's size.
func (p *filePrefetchObject) endAt(size int64) {
	if size <= 0 {
		return
	}
	f := &p.d.prefetchObjects
	f.mu.Lock()
	p.end = size
	f.mu.Unlock()
}

// admitWhole waits until p, a whole-object download, holds its connection, then registers
// it. It reports false when the download is skipped, or with why it stopped; a delete of
// the object or the closing of the cache stops it as a cancel does.
func (p *filePrefetchObject) admitWhole() (bool, error) {
	o, err := admit(p.ctx, p)
	if errors.Is(err, errPrefetchDeleted) || errors.Is(err, errPrefetchClosed) {
		err = context.Canceled
	}
	if o == nil || err != nil {
		return false, err
	}
	p.stopOnClose = context.AfterFunc(p.closing, p.cancelRead)
	return true, nil
}

// endWhole ends p, a whole-object download, with why it cached nothing.
func (p *filePrefetchObject) endWhole(err error) {
	if p.stopOnClose == nil {
		p.abandon()
		return
	}
	p.stopOnClose()
	p.releaseConn()
	p.d.endFilePrefetch(p.diskPath, p)(err)
	p.release()
	p.d.prefetchObjects.reads.Done()
}

func (p *filePrefetchObject) joinableDownload() *objectDownload {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.download
}

func (p *filePrefetchObject) markReady() { p.readyOnce.Do(func() { close(p.ready) }) }

type fileDownloadKey struct{}

// fileDownloadStarted gives the download of ctx's FilePrefetchObject to it, and returns
// that FilePrefetchObject, or nil.
func fileDownloadStarted(ctx context.Context, o *objectDownload) *filePrefetchObject {
	p, ok := ctx.Value(fileDownloadKey{}).(*filePrefetchObject)
	if !ok {
		return nil
	}
	p.started(o)
	return p
}

// started gives p its download o, which reads can join from then.
func (p *filePrefetchObject) started(o *objectDownload) {
	p.mu.Lock()
	p.download = o
	if p.holdStarter && !p.starterDone && o.hold() {
		p.starterHeld = true
	}
	p.mu.Unlock()
	p.markReady()
}

// joinLocked returns the prefetch object holding the bytes of p that p joins instead,
// none for a whole object.
func (p *filePrefetchObject) joinLocked(now time.Time) prefetchObject {
	if p.whole {
		return nil
	}
	return p.d.prefetchObjects.holdingLocked(p.diskPath, p.start, p.end, now, p.d.downloadMaxExtraWait)
}

// fitsLocked reports true: p needs only a connection.
func (p *filePrefetchObject) fitsLocked() bool { return true }

// registerLocked registers p, joinable once its download is registered.
func (p *filePrefetchObject) registerLocked(_, closing context.Context, _ time.Time) {
	f := &p.d.prefetchObjects
	p.closing = closing
	f.registerLocked(p.diskPath, p)
	f.reads.Add(1)
}

// run downloads the bytes of p, registered, to their cache file, frees the connection p
// holds once its bytes are read, and ends p. The closing of the cache or a delete of the
// object cancels it.
func (p *filePrefetchObject) run() {
	d := p.d
	defer d.prefetchObjects.reads.Done()
	defer context.AfterFunc(p.closing, p.cancelRead)()
	defer p.releaseConn()
	_, _ = d.downloadFile(p.ctx, p.closing, p.filePath, p.start, p.end, p.open, p.releaseConn, d.endFilePrefetch(p.diskPath, p))
}

// abandon ends p, never registered.
func (p *filePrefetchObject) abandon() { p.cancelRead() }

// releaseConn frees the connection p holds, once.
func (p *filePrefetchObject) releaseConn() {
	p.connOnce.Do(func() { p.d.conns.release() })
}

// endFilePrefetch returns the function ending the registered FilePrefetchObject p of the
// object at objectPath with why it cached nothing. One handed to an async finalize is
// forgotten once its file is published.
func (d *DiskCache) endFilePrefetch(objectPath string, p *filePrefetchObject) func(error) {
	return func(err error) {
		if !p.handedOff {
			d.forgetFill(objectPath, p)
		}
		p.mu.Lock()
		p.endErr = err
		p.mu.Unlock()
		close(p.endedCh)
		p.markReady()
	}
}

// forgetFill forgets the fill p of the object at objectPath and ends its context.
func (d *DiskCache) forgetFill(objectPath string, p *filePrefetchObject) {
	f := &d.prefetchObjects
	f.mu.Lock()
	f.forgetLocked(objectPath, p)
	f.mu.Unlock()
	p.cancelRead()
}

// holdingLocked returns the prefetch object of the object at objectPath holding the bytes
// [start, end) that a FilePrefetchObject joins instead of downloading them: the fastest
// one reads can join that caches them, or a registered FilePrefetchObject a read started.
func (f *prefetchObjects) holdingLocked(objectPath string, start, end int64, now time.Time, extraWait time.Duration) prefetchObject {
	if o := f.fastestLocked(objectPath, start, end, now, extraWait, true); o != nil {
		return o
	}
	for _, o := range f.m[objectPath] {
		if p, ok := o.(*filePrefetchObject); ok && p.readStarted && p.ctx.Err() == nil && p.start <= start && end <= p.end {
			return p
		}
	}
	return nil
}

// downloadFile downloads under ctx, the context of a FilePrefetchObject, its bytes [start,
// end) of the object at filePath through open, calls copied once they are read, and ends
// it through done. It publishes nothing once closing is done.
func (d *DiskCache) downloadFile(ctx, closing context.Context, filePath string, start, end int64, open rangeOpener, copied func(), done func(error)) (bool, error) {
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
	live := func() bool { return closing.Err() == nil }
	written, err = d.publishUnlessDeleted(filePath, IOEntry{readDeletes: readDeletes}, live, func(claim func() bool) (bool, error) {
		return d.writeFileTo(ctx, diskPath, openReader, false, claim, rangeFill, layout, nil, copied)
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
		err = errSourceShort
	}
	return n, err
}

// fileFill returns the live file fill of the object at objectPath, of the index-file cache
// when index is set, holding the bytes [lo, hi) whose bytes arrive first, and its
// download.
func (d *DiskCache) fileFill(objectPath string, index bool, lo, hi int64) (*filePrefetchObject, *objectDownload) {
	f := &d.prefetchObjects
	f.mu.Lock()
	defer f.mu.Unlock()
	var best *filePrefetchObject
	var bestDownload *objectDownload
	var bestWait time.Duration
	for _, o := range f.m[objectPath] {
		p, ok := o.(*filePrefetchObject)
		if !ok || p.index != index || p.start > lo || hi > p.end || p.ctx.Err() != nil {
			continue
		}
		if download := p.joinableDownload(); download != nil {
			if _, wait := download.estimate(lo-p.start, hi-p.start); best == nil || wait < bestWait {
				best, bestDownload, bestWait = p, download, wait
			}
		}
	}
	return best, bestDownload
}

// fileDownload returns the download of the file fill of the disk cache fileFill returns,
// and where its bytes start in the object.
func (d *DiskCache) fileDownload(objectPath string, lo, hi int64) (*objectDownload, int64) {
	p, o := d.fileFill(objectPath, false, lo, hi)
	if p == nil {
		return nil, 0
	}
	return o, p.start
}
