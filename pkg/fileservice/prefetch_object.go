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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// prefetchObject is a download of the bytes [start, end) of an object, which reads of
// them join while it runs: a memoryPrefetchObject, into a buffer, or a
// filePrefetchObject, into its cache file.
type prefetchObject interface {
	// span returns the bytes of the object it downloads.
	span() (start, end int64)
	// joinable reports whether reads can join it yet.
	joinable() bool
	// eta estimates how long after now its bytes [lo, hi) arrive.
	eta(now time.Time, lo, hi int64) time.Duration
	// caches reports whether it caches its bytes.
	caches() bool
	// hold keeps its bytes readable for a read until release. Holds are taken while it
	// is registered.
	hold()
	release()
	// use calls fn with its bytes [off, off+n) of the object once they are written. It
	// returns fn's error, the error it failed with, ctx's, or errPrefetchMissed when it
	// ended without them.
	use(ctx context.Context, off, n int64, fn func([]byte) error) error
	// readAt reads its bytes [off, off+len(dst)) of the object into dst, as use does.
	readAt(ctx context.Context, off int64, dst []byte) error
	// stop cancels it. ended is closed once it ended, when err is why it cached nothing.
	stop()
	ended() <-chan struct{}
	err() error
}

// errPrefetchMissed is returned by use of a prefetch object that ended without the bytes.
var errPrefetchMissed = moerr.NewInternalErrorNoCtx("fileservice: prefetch object ended without the bytes")

var (
	_ prefetchObject = (*memoryPrefetchObject)(nil)
	_ prefetchObject = (*filePrefetchObject)(nil)
)

func (f *memoryPrefetchObject) span() (int64, int64) { return f.start, f.end }

func (f *memoryPrefetchObject) joinable() bool { return true }

func (f *memoryPrefetchObject) caches() bool { return !f.skipCache }

func (f *memoryPrefetchObject) use(ctx context.Context, off, n int64, fn func([]byte) error) error {
	content, ok := f.waitFor(ctx, off+n)
	if !ok {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A stopped object misses the bytes; the error of a failed read is the read's.
		if err := f.failure(); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return errPrefetchMissed
	}
	return fn(content[off-f.start : off-f.start+n])
}

func (f *memoryPrefetchObject) readAt(ctx context.Context, off int64, dst []byte) error {
	return f.use(ctx, off, int64(len(dst)), func(bs []byte) error {
		copy(dst, bs)
		return nil
	})
}

func (f *memoryPrefetchObject) stop() { f.cancelRead() }

func (f *memoryPrefetchObject) ended() <-chan struct{} { return f.endedCh }

func (f *memoryPrefetchObject) err() error { return f.cacheErr }

func (p *filePrefetchObject) span() (int64, int64) { return p.start, p.end }

func (p *filePrefetchObject) joinable() bool { return p.joinableDownload() != nil }

func (p *filePrefetchObject) eta(_ time.Time, lo, hi int64) time.Duration {
	o := p.joinableDownload()
	if o == nil {
		return time.Duration(1<<63 - 1)
	}
	_, wait := o.estimate(lo-p.start, hi-p.start)
	return wait
}

func (p *filePrefetchObject) caches() bool { return true }

func (p *filePrefetchObject) hold() {
	p.mu.Lock()
	p.holds++
	p.mu.Unlock()
}

// release stops one a read started when no read uses it and its download is not
// registered yet.
func (p *filePrefetchObject) release() {
	p.mu.Lock()
	p.holds--
	stop := p.readStarted && p.holds == 0 && p.download == nil
	p.mu.Unlock()
	if stop {
		p.cancelRead()
	}
}

func (p *filePrefetchObject) use(ctx context.Context, off, n int64, fn func([]byte) error) error {
	buf := make([]byte, n)
	if err := p.readAt(ctx, off, buf); err != nil {
		return err
	}
	return fn(buf)
}

// readAt waits until reads can join its download, then reads from its file as the bytes
// arrive.
func (p *filePrefetchObject) readAt(ctx context.Context, off int64, dst []byte) error {
	select {
	case <-p.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	o := p.joinableDownload()
	if o == nil {
		return errPrefetchMissed
	}
	if !readDownload(ctx, o, off-p.start, dst) {
		if err := ctx.Err(); err != nil {
			return err
		}
		return errPrefetchMissed
	}
	return nil
}

func (p *filePrefetchObject) stop() { p.cancelRead() }

func (p *filePrefetchObject) ended() <-chan struct{} { return p.endedCh }

func (p *filePrefetchObject) err() error { return p.endErr }

// registerLocked registers o as a prefetch object of the object at diskPath.
func (f *prefetchObjects) registerLocked(diskPath string, o prefetchObject) {
	if f.m == nil {
		f.m = make(map[string][]prefetchObject)
	}
	f.m[diskPath] = append(f.m[diskPath], o)
}

// forgetLocked forgets o as a prefetch object of the object at diskPath.
func (f *prefetchObjects) forgetLocked(diskPath string, o prefetchObject) {
	objects := f.m[diskPath]
	for i := range objects {
		if objects[i] == o {
			objects = append(objects[:i], objects[i+1:]...)
			break
		}
	}
	if len(objects) == 0 {
		delete(f.m, diskPath)
	} else {
		f.m[diskPath] = objects
	}
}
