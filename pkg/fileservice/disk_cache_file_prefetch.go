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
)

// filePrefetchObject is a FilePrefetchObject of the bytes [start, end) of an object: they
// download to their cache file, which reads of them join while it downloads.
type filePrefetchObject struct {
	start, end int64
	cancelRead context.CancelFunc
	endedCh    chan struct{}

	mu       sync.Mutex
	download *objectDownload // set once the download is registered
	endErr   error           // why it cached nothing, once ended
}

func (p *filePrefetchObject) joinableDownload() *objectDownload {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.download
}

type fileDownloadKey struct{}

// fileDownloadStarted gives the download of ctx's FilePrefetchObject to it.
func fileDownloadStarted(ctx context.Context, o *objectDownload) {
	if p, ok := ctx.Value(fileDownloadKey{}).(*filePrefetchObject); ok {
		p.mu.Lock()
		p.download = o
		p.mu.Unlock()
	}
}

// startFilePrefetch registers a FilePrefetchObject of the bytes [start, end) of the
// object at objectPath. It returns the context of its download, which a delete of the
// object cancels, and the function ending it with why it cached nothing.
func (d *DiskCache) startFilePrefetch(ctx context.Context, objectPath string, start, end int64) (context.Context, func(error)) {
	ctx, cancel := context.WithCancel(ctx)
	p := &filePrefetchObject{start: start, end: end, cancelRead: cancel, endedCh: make(chan struct{})}
	f := &d.prefetchObjects
	f.mu.Lock()
	f.registerLocked(objectPath, p)
	f.mu.Unlock()
	return context.WithValue(ctx, fileDownloadKey{}, p), func(err error) {
		f.mu.Lock()
		f.forgetLocked(objectPath, p)
		f.mu.Unlock()
		cancel()
		p.endErr = err
		close(p.endedCh)
	}
}

// prefetchFile downloads, as a FilePrefetchObject, the bytes [start, end) of the object
// at filePath through open to their cache file. A delete of the object cancels it, and
// it is not cached after a delete counted since it began.
func (d *DiskCache) prefetchFile(ctx context.Context, filePath string, start, end int64, open rangeOpener) (bool, error) {
	ctx, done := d.startFilePrefetch(ctx, d.pathForFile(filePath), start, end)
	var written bool
	var err error
	defer func() { done(err) }()
	readDeletes := d.readDeletes()
	diskPath := d.pathForIOEntry(filePath, IOEntry{Offset: start, Size: end - start})
	openReader := func(ctx context.Context) (io.ReadCloser, error) {
		return &lazyObjectReader{ctx: ctx, read: func(ctx context.Context, min, max *int64) (io.ReadCloser, error) {
			from, to := start+*min, end
			if max != nil {
				to = start + *max
			}
			return open(ctx, from, to)
		}}, nil
	}
	layout := func(context.Context) (int64, int64, error) { return end - start, 0, nil }
	written, err = d.publishUnlessDeleted(filePath, IOEntry{readDeletes: readDeletes}, nil, func(claim func() bool) (bool, error) {
		return d.writeFileWithFinalizeMode(ctx, diskPath, openReader, false, claim, rangeFill, layout)
	})
	if err != nil {
		err = d.observeWriteResult(diskPath, err)
		return false, err
	}
	if written {
		err = d.observeWriteResult(diskPath, nil)
	}
	return written, err
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
