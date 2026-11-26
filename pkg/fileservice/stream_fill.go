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
	"runtime"
	"sync"
)

// streamDownload reads a range of an object in one request on its own goroutine: into a
// buffer when its size is known and fits the flight memory of the disk cache, otherwise
// into an unlinked temporary file. Its reader reads the bytes as they arrive.
type streamDownload struct {
	cancel context.CancelFunc
	// done is closed once the request ends.
	done chan struct{}
	buf  []byte
	file *os.File
	free func()

	mu sync.Mutex
	// arrived is closed and replaced once more bytes are written or the fill ends.
	arrived chan struct{}
	written int64
	open    bool
	ended   bool
	err     error
	// closeErr is the error closing the body of the read, which Close returns.
	closeErr error
	closed   bool
}

// openStream starts the read rr, which sets neither buf nor file, under ctx and returns
// the reader of its bytes once its request is open, or the error that ended it before.
func (s *S3FS) openStream(ctx context.Context, rr rangeRead) (*streamReader, error) {
	f := &streamDownload{arrived: make(chan struct{}), done: make(chan struct{}), free: func() {}}
	if rr.end >= 0 {
		if buf, free := s.streamBuffer(rr.end - rr.start); buf != nil {
			f.buf, f.free = buf, free
		}
	}
	if f.buf == nil {
		file, err := s.streamFile()
		if err != nil {
			return nil, err
		}
		f.file = file
	}
	rr.buf, rr.file, rr.progress = f.buf, f.file, fillProgress{stream: f}
	ctx, f.cancel = context.WithCancel(ctx)
	go func() {
		_, err := s.governor.Do(ctx, rr)
		var source *diskCacheSourceError
		if errors.As(err, &source) {
			err = source.err
		}
		var closeErr *bodyCloseError
		if errors.As(err, &closeErr) {
			f.closeErr, err = closeErr.err, nil
		}
		f.end(err)
	}()
	r := &streamReader{f: f}
	if err := r.wait(ctx, false); err != nil {
		_ = r.Close()
		return nil, err
	}
	runtime.SetFinalizer(r, func(r *streamReader) { _ = r.Close() })
	return r, nil
}

// streamBuffer returns a buffer of size bytes charged to the flight memory of the disk
// cache, and the function freeing it, or nil when there is no disk cache or it does not
// fit.
func (s *S3FS) streamBuffer(size int64) ([]byte, func()) {
	d := s.diskCache
	if d == nil || size > d.memoryPrefetchMax {
		return nil, nil
	}
	f := &d.prefetchObjects
	charge := flightBufSize(size)
	f.mu.Lock()
	if f.bytes > 0 && f.bytes+charge > d.flightBytes() {
		f.mu.Unlock()
		return nil, nil
	}
	f.bytes += charge
	f.mu.Unlock()
	buf := getFlightBuf(size)
	return buf, func() {
		putFlightBuf(buf)
		f.uncharge(charge, false)
	}
}

// streamFile returns an unlinked temporary file in the directory of the disk cache, or in
// the temporary directory of the process without one.
func (s *S3FS) streamFile() (*os.File, error) {
	var file *os.File
	var err error
	if s.diskCache != nil {
		file, err = s.diskCache.createCacheTempFile(s.diskCache.path)
	} else {
		file, err = os.CreateTemp("", "mo-fs-stream-*")
	}
	if err != nil {
		return nil, err
	}
	if err := os.Remove(file.Name()); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (f *streamDownload) opened() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open = true
	close(f.arrived)
	f.arrived = make(chan struct{})
}

func (f *streamDownload) wrote(n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written += n
	close(f.arrived)
	f.arrived = make(chan struct{})
}

// end ends the fill with err.
func (f *streamDownload) end(err error) {
	f.mu.Lock()
	f.ended, f.err = true, err
	close(f.arrived)
	f.mu.Unlock()
	f.cancel()
	close(f.done)
}

// close stops the fill, waits for its request to end, frees its bytes, and returns the
// error closing the body of its read.
func (f *streamDownload) close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.cancel()
	<-f.done
	f.drop()
	return f.closeErr
}

func (f *streamDownload) drop() {
	if f.file != nil {
		_ = f.file.Close()
	}
	f.free()
}

// streamReader reads the bytes of a streamDownload from its start.
type streamReader struct {
	f         *streamDownload
	off       int64
	closeOnce sync.Once
	closeErr  error
}

func (r *streamReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	f := r.f
	for {
		f.mu.Lock()
		written, ended, err, arrived, closed := f.written, f.ended, f.err, f.arrived, f.closed
		f.mu.Unlock()
		if closed {
			return 0, os.ErrClosed
		}
		if r.off < written {
			n := int(min(int64(len(b)), written-r.off))
			if f.buf != nil {
				copy(b, f.buf[r.off:r.off+int64(n)])
			} else if n, err = f.file.ReadAt(b[:n], r.off); err != nil {
				return n, err
			}
			r.off += int64(n)
			return n, nil
		}
		if ended {
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		<-arrived
	}
}

// wait waits until the fill ends, or, unless toEnd, until its request is open, or until
// ctx is done, and returns the error that ended the fill.
func (r *streamReader) wait(ctx context.Context, toEnd bool) error {
	f := r.f
	for {
		f.mu.Lock()
		ended, err, arrived, open := f.ended, f.err, f.arrived, f.open
		f.mu.Unlock()
		if ended {
			return err
		}
		if !toEnd && open {
			return nil
		}
		select {
		case <-arrived:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close stops the fill, and returns the error closing the body of its read, once ended.
func (r *streamReader) Close() error {
	r.closeOnce.Do(func() {
		runtime.SetFinalizer(r, nil)
		r.closeErr = r.f.close()
	})
	return r.closeErr
}
