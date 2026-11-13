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
	"slices"
	"sync"
)

// filePrefetch is a FilePrefetchObject of the bytes [start, end) of an object: they
// download to their cache file, which reads of them join while it downloads.
type filePrefetch struct {
	start, end int64
	cancel     context.CancelFunc
	ended      chan struct{}

	mu       sync.Mutex
	download *objectDownload // set once the download is registered
}

func (p *filePrefetch) joinable() *objectDownload {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.download
}

// filePrefetches indexes the FilePrefetchObjects under way by the disk path of their
// object.
type filePrefetches struct {
	mu sync.Mutex
	m  map[string][]*filePrefetch
}

type fileDownloadKey struct{}

// fileDownloadStarted gives the download of ctx's FilePrefetchObject to it.
func fileDownloadStarted(ctx context.Context, o *objectDownload) {
	if p, ok := ctx.Value(fileDownloadKey{}).(*filePrefetch); ok {
		p.mu.Lock()
		p.download = o
		p.mu.Unlock()
	}
}

// startFilePrefetch registers a FilePrefetchObject of the bytes [start, end) of the
// object at objectPath. It returns the context of its download, which a delete of the
// object cancels, and the function ending it.
func (d *DiskCache) startFilePrefetch(ctx context.Context, objectPath string, start, end int64) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	p := &filePrefetch{start: start, end: end, cancel: cancel, ended: make(chan struct{})}
	f := &d.filePrefetches
	f.mu.Lock()
	if f.m == nil {
		f.m = make(map[string][]*filePrefetch)
	}
	f.m[objectPath] = append(f.m[objectPath], p)
	f.mu.Unlock()
	return context.WithValue(ctx, fileDownloadKey{}, p), func() {
		f.mu.Lock()
		f.m[objectPath] = slices.DeleteFunc(f.m[objectPath], func(o *filePrefetch) bool { return o == p })
		if len(f.m[objectPath]) == 0 {
			delete(f.m, objectPath)
		}
		f.mu.Unlock()
		cancel()
		close(p.ended)
	}
}

// prefetchFile downloads, as a FilePrefetchObject, the bytes [start, end) of the object
// at filePath through open to their cache file. A delete of the object cancels it, and
// it is not cached after a delete counted since it began.
func (d *DiskCache) prefetchFile(ctx context.Context, filePath string, start, end int64, open rangeOpener) (bool, error) {
	ctx, done := d.startFilePrefetch(ctx, d.pathForFile(filePath), start, end)
	defer done()
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
	written, err := d.publishUnlessDeleted(filePath, IOEntry{readDeletes: readDeletes}, nil, func(claim func() bool) (bool, error) {
		return d.writeFileWithFinalizeMode(ctx, diskPath, openReader, false, claim, rangeFill, layout)
	})
	if err != nil {
		return false, d.observeWriteResult(diskPath, err)
	}
	if written {
		err = d.observeWriteResult(diskPath, nil)
	}
	return written, err
}

// fileDownload returns the download of a FilePrefetchObject of the object at objectPath
// holding the bytes [lo, hi), and where its bytes start in the object.
func (d *DiskCache) fileDownload(objectPath string, lo, hi int64) (*objectDownload, int64) {
	f := &d.filePrefetches
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range f.m[objectPath] {
		if p.start <= lo && hi <= p.end {
			if o := p.joinable(); o != nil {
				return o, p.start
			}
		}
	}
	return nil, 0
}

// dropFilePrefetches cancels the FilePrefetchObjects of the object at objectPath and
// waits for them to end, or ctx.
func (d *DiskCache) dropFilePrefetches(ctx context.Context, objectPath string) error {
	f := &d.filePrefetches
	f.mu.Lock()
	prefetches := slices.Clone(f.m[objectPath])
	f.mu.Unlock()
	for _, p := range prefetches {
		p.cancel()
	}
	for _, p := range prefetches {
		select {
		case <-p.ended:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
