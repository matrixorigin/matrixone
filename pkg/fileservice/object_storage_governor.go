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
	"bytes"
	"container/list"
	"context"
	"io"
	"iter"
	"os"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// requestClass is the priority class of a request to object storage.
type requestClass uint8

const (
	foregroundRequest requestClass = iota
	backgroundRequest
)

type backgroundRequestKey struct{}

// withBackgroundRequests returns ctx under which requests to object storage are background.
func withBackgroundRequests(ctx context.Context) context.Context {
	return context.WithValue(ctx, backgroundRequestKey{}, true)
}

func requestClassOf(ctx context.Context) requestClass {
	if ctx.Value(backgroundRequestKey{}) == true {
		return backgroundRequest
	}
	return foregroundRequest
}

var errGovernorClosed = moerr.NewInternalErrorNoCtx("fileservice: object storage governor closed")

// minRequestRoom is the least room of a governor.
const minRequestRoom = 64

// governor makes every request to the object storage raw, at most capacity at once.
// Waiting requests are admitted foreground first, then background, each in arrival
// order; background requests run at most backgroundMax at once. A running request is
// never stopped.
type governor struct {
	raw ObjectStorage
	ops [opCount]prometheus.Gauge

	mu            sync.Mutex
	capacity      int
	backgroundMax int
	running       [2]int
	waiting       [2]list.List
	closed        bool
	closedCh      chan struct{}
}

type roomWaiter struct {
	class   requestClass
	granted chan struct{}
	elem    *list.Element
}

// newGovernor returns a governor of raw with room for capacity requests, at least
// minRequestRoom, half of it for background requests.
func newGovernor(raw ObjectStorage, capacity int) *governor {
	g := &governor{raw: raw, closedCh: make(chan struct{})}
	for op, name := range opNames {
		g.ops[op] = metric.FSObjectStorageOperations.WithLabelValues("s3", name)
	}
	g.capacity = max(capacity, minRequestRoom)
	g.backgroundMax = g.capacity / 2
	return g
}

func (g *governor) canRunLocked(c requestClass) bool {
	if g.running[foregroundRequest]+g.running[backgroundRequest] >= g.capacity {
		return false
	}
	return c == foregroundRequest || g.running[backgroundRequest] < g.backgroundMax
}

// wait waits for room for a request of class c until ctx ends or g closes.
func (g *governor) wait(ctx context.Context, c requestClass) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return errGovernorClosed
	}
	if g.waiting[foregroundRequest].Len() == 0 &&
		(c == foregroundRequest || g.waiting[backgroundRequest].Len() == 0) && g.canRunLocked(c) {
		g.startLocked(c)
		g.mu.Unlock()
		return nil
	}
	w := &roomWaiter{class: c, granted: make(chan struct{})}
	w.elem = g.waiting[c].PushBack(w)
	waitingGauge(c).Inc()
	g.mu.Unlock()
	select {
	case <-w.granted:
		return nil
	case <-ctx.Done():
		return g.abandon(w, ctx.Err())
	case <-g.closedCh:
		return g.abandon(w, errGovernorClosed)
	}
}

// abandon ends the wait of w with err, freeing the room it was granted meanwhile.
func (g *governor) abandon(w *roomWaiter, err error) error {
	g.mu.Lock()
	if w.elem == nil {
		g.mu.Unlock()
		g.done(w.class)
		return err
	}
	g.waiting[w.class].Remove(w.elem)
	w.elem = nil
	waitingGauge(w.class).Dec()
	g.grantLocked()
	g.mu.Unlock()
	return err
}

func (g *governor) startLocked(c requestClass) {
	g.running[c]++
	runningGauge(c).Inc()
}

func (g *governor) done(c requestClass) {
	g.mu.Lock()
	g.running[c]--
	runningGauge(c).Dec()
	g.grantLocked()
	g.mu.Unlock()
}

func (g *governor) grantLocked() {
	for _, c := range []requestClass{foregroundRequest, backgroundRequest} {
		for g.waiting[c].Len() > 0 && g.canRunLocked(c) {
			w := g.waiting[c].Remove(g.waiting[c].Front()).(*roomWaiter)
			w.elem = nil
			waitingGauge(c).Dec()
			g.startLocked(c)
			close(w.granted)
		}
		if g.waiting[foregroundRequest].Len() > 0 {
			return
		}
	}
}

// hasRoom reports whether a request of class c would be admitted now.
func (g *governor) hasRoom(c requestClass) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.waiting[foregroundRequest].Len() == 0 &&
		(c == foregroundRequest || g.waiting[backgroundRequest].Len() == 0) && g.canRunLocked(c)
}

// close ends the waits for room; running requests run on.
func (g *governor) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		close(g.closedCh)
	}
}

// counts returns the requests running and waiting of each class.
func (g *governor) counts() (running, waiting [2]int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for c := range running {
		running[c], waiting[c] = g.running[c], g.waiting[c].Len()
	}
	return
}

func runningGauge(c requestClass) prometheus.Gauge {
	if c == backgroundRequest {
		return metric.FSObjectStorageRequestsBackgroundRunning
	}
	return metric.FSObjectStorageRequestsForegroundRunning
}

func waitingGauge(c requestClass) prometheus.Gauge {
	if c == backgroundRequest {
		return metric.FSObjectStorageRequestsBackgroundWaiting
	}
	return metric.FSObjectStorageRequestsForegroundWaiting
}

// request is one request of a governor: it holds room while it runs and while a retry of
// it does not wait.
type request struct {
	g     *governor
	class requestClass

	mu   sync.Mutex
	held bool
}

type requestKey struct{}

// begin waits for room for a request under ctx and returns the context the request runs
// under, and the request, which end ends.
func (g *governor) begin(ctx context.Context, op objectStorageOp) (context.Context, *request, error) {
	r := &request{g: g, class: requestClassOf(ctx)}
	if err := r.resume(ctx); err != nil {
		return nil, nil, err
	}
	g.ops[op].Inc()
	return context.WithValue(withHTTPTrace(ctx), requestKey{}, r), r, nil
}

// resume waits for room for r, unless r holds room.
func (r *request) resume(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	held := r.held
	r.mu.Unlock()
	if held {
		return nil
	}
	if err := r.g.wait(ctx, r.class); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held {
		r.g.done(r.class)
	}
	r.held = true
	return nil
}

// end frees the room r holds.
func (r *request) end() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held {
		r.held = false
		r.g.done(r.class)
	}
}

// pauseRequest frees the room of the request ctx runs under, for a retry to wait, and
// returns the request to resume, or nil.
func pauseRequest(ctx context.Context) *request {
	r, _ := ctx.Value(requestKey{}).(*request)
	r.end()
	return r
}

type objectStorageOp int

const (
	opRead objectStorageOp = iota
	opWrite
	opDelete
	opList
	opExists
	opStat
	opCount
)

var opNames = [opCount]string{"read", "write", "delete", "list", "exists", "stat"}

func (g *governor) stat(ctx context.Context, key string) (int64, error) {
	ctx, r, err := g.begin(ctx, opStat)
	if err != nil {
		return 0, err
	}
	defer r.end()
	return g.raw.Stat(ctx, key)
}

func (g *governor) exists(ctx context.Context, key string) (bool, error) {
	ctx, r, err := g.begin(ctx, opExists)
	if err != nil {
		return false, err
	}
	defer r.end()
	return g.raw.Exists(ctx, key)
}

func (g *governor) delete(ctx context.Context, keys ...string) error {
	ctx, r, err := g.begin(ctx, opDelete)
	if err != nil {
		return err
	}
	defer r.end()
	return g.raw.Delete(ctx, keys...)
}

// copyObject copies srcKey of src into dstKey of raw on the provider, reporting false when
// raw cannot.
func (g *governor) copyObject(ctx context.Context, src ObjectStorage, srcKey, dstKey string) (bool, error) {
	copier, ok := g.raw.(objectStorageCopier)
	if !ok {
		return false, nil
	}
	ctx, r, err := g.begin(ctx, opWrite)
	if err != nil {
		return false, err
	}
	defer r.end()
	return copier.CopyObject(ctx, src, srcKey, dstKey)
}

func (g *governor) statIdentity(ctx context.Context, key string) (ObjectIdentity, error) {
	reader, ok := g.raw.(objectStorageIdentityReader)
	if !ok {
		return ObjectIdentity{}, moerr.NewNotSupported(ctx, "object storage identity")
	}
	ctx, r, err := g.begin(ctx, opStat)
	if err != nil {
		return ObjectIdentity{}, err
	}
	defer r.end()
	return reader.StatObjectIdentity(ctx, key)
}

// list lists prefix. Each entry is taken from the listing holding room, so the request
// that reads a page holds room while it runs, and the entry is yielded with no room held.
func (g *governor) list(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		r := &request{g: g, class: requestClassOf(ctx)}
		listCtx := context.WithValue(withHTTPTrace(ctx), requestKey{}, r)
		next, stop := iter.Pull2(g.raw.List(listCtx, prefix))
		defer stop()
		g.ops[opList].Inc()
		for {
			entry, err, ok := g.listNext(ctx, r, next)
			if !ok || !yield(entry, err) || err != nil {
				return
			}
		}
	}
}

// listNext takes the next entry from next holding the room of r.
func (g *governor) listNext(ctx context.Context, r *request, next func() (*DirEntry, error, bool)) (*DirEntry, error, bool) {
	if err := r.resume(ctx); err != nil {
		return nil, err, true
	}
	defer r.end()
	return next()
}

// rangeRead is a read of the bytes [start, end) of key, to the end of the object when end
// is negative: into buf, filled as far as the bytes go; or into file at offset, a source
// error then wrapped as a diskCacheSourceError; or, with neither, into bytes read returns.
// Each write into buf or file is recorded in progress; idle, when set, times the source.
type rangeRead struct {
	key        string
	start, end int64
	identity   *ObjectIdentity
	buf        []byte
	file       *os.File
	offset     int64
	progress   fillProgress
	idle       *idleReader
}

// read makes the read rr and returns the bytes it read into neither buf nor file, and how
// many it read.
func (g *governor) read(ctx context.Context, rr rangeRead) ([]byte, int64, error) {
	ctx, r, err := g.begin(ctx, opRead)
	if err != nil {
		return nil, 0, err
	}
	defer r.end()
	rr.progress.began()
	t0 := time.Now()
	body, err := g.open(ctx, rr)
	if err != nil {
		if rr.file != nil {
			err = &diskCacheSourceError{err: err}
		}
		return nil, 0, err
	}
	rr.progress.opened()
	var data []byte
	var n int64
	switch {
	case rr.buf != nil:
		n, err = readIntoBuffer(body, rr)
	case rr.file != nil:
		n, err = readIntoFile(body, rr)
	default:
		data, err = io.ReadAll(body)
		n = int64(len(data))
	}
	if closeErr := body.Close(); err == nil && closeErr != nil && rr.progress.stream != nil {
		err = &bodyCloseError{err: closeErr}
	}
	metric.S3ReadIOBytesHistogram.Observe(float64(n))
	if err == nil && rr.end >= 0 && n == rr.end-rr.start {
		rangeReadCosts.observe(n, time.Since(t0))
	}
	return data, n, err
}

func (g *governor) open(ctx context.Context, rr rangeRead) (io.ReadCloser, error) {
	min, max := &rr.start, (*int64)(nil)
	if rr.end >= 0 {
		max = &rr.end
	}
	if rr.idle != nil {
		rr.idle.begin()
	}
	var body io.ReadCloser
	var err error
	if rr.identity != nil {
		reader, ok := g.raw.(objectStorageIdentityReader)
		if !ok {
			return nil, moerr.NewNotSupported(ctx, "conditional object storage read")
		}
		body, err = reader.ReadObjectWithIdentity(ctx, rr.key, min, max, *rr.identity)
	} else {
		body, err = g.raw.Read(ctx, rr.key, min, max)
	}
	if rr.idle == nil {
		return body, err
	}
	if err = rr.idle.end(err); err != nil {
		if body != nil {
			_ = body.Close()
		}
		return nil, err
	}
	rr.idle.r = body
	return &readCloser{r: rr.idle, closeFunc: body.Close}, nil
}

func readIntoBuffer(body io.Reader, rr rangeRead) (int64, error) {
	pos := 0
	for pos < len(rr.buf) {
		n, err := body.Read(rr.buf[pos:min(pos+prefetchChunk, len(rr.buf))])
		if n > 0 {
			pos += n
			rr.progress.wrote(int64(n))
		}
		if err == io.EOF {
			return int64(pos), nil
		}
		if err != nil {
			return int64(pos), err
		}
	}
	_, err := io.Copy(io.Discard, body)
	return int64(pos), err
}

func readIntoFile(body io.Reader, rr rangeRead) (int64, error) {
	var buf []byte
	put := ioBufferPool.Get(&buf)
	defer put.Put()
	var n int64
	for {
		m, err := body.Read(buf)
		if m > 0 {
			if _, werr := rr.file.WriteAt(buf[:m], rr.offset+n); werr != nil {
				return n, werr
			}
			n += int64(m)
			rr.progress.wrote(int64(m))
		}
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, &diskCacheSourceError{err: err}
		}
	}
}

// bodyCloseError is the error closing the body of a stream's read that read its bytes.
type bodyCloseError struct{ err error }

func (e *bodyCloseError) Error() string { return e.err.Error() }
func (e *bodyCloseError) Unwrap() error { return e.err }

// fillProgress is where a read records the bytes it writes into a fill: a part of a
// download, a memory flight or a stream. Recording takes only the fill's own short lock
// and never waits.
type fillProgress struct {
	download *objectDownload
	part     int
	flight   *memoryPrefetchObject
	stream   *streamDownload
}

// began records that the read holds room.
func (p fillProgress) began() {
	switch {
	case p.download != nil:
		p.download.begin(p.part)
	case p.flight != nil:
		p.flight.requested.Store(true)
	}
}

// opened records that the read's request is open.
func (p fillProgress) opened() {
	if p.stream != nil {
		p.stream.opened()
	}
}

func (p fillProgress) wrote(n int64) {
	switch {
	case p.download != nil:
		p.download.advance(p.part, n)
	case p.flight != nil:
		p.flight.wrote(int(n))
	case p.stream != nil:
		p.stream.wrote(n)
	}
}

// put writes data as the object key in one request.
func (g *governor) put(ctx context.Context, key string, data []byte, expire *time.Time) error {
	ctx, r, err := g.begin(ctx, opWrite)
	if err != nil {
		return err
	}
	defer r.end()
	size := int64(len(data))
	return g.raw.Write(ctx, key, bytes.NewReader(data), &size, expire)
}

// putFile writes the size bytes of f as the object key in one request.
func (g *governor) putFile(ctx context.Context, key string, f *os.File, size int64, expire *time.Time) error {
	ctx, r, err := g.begin(ctx, opWrite)
	if err != nil {
		return err
	}
	defer r.end()
	return g.raw.Write(ctx, key, io.NewSectionReader(f, 0, size), &size, expire)
}

// publishStaged publishes the file a disk object storage staged at tempPath as key.
func (g *governor) publishStaged(ctx context.Context, key, tempPath string) error {
	ctx, r, err := g.begin(ctx, opWrite)
	if err != nil {
		return err
	}
	defer r.end()
	return g.raw.(*diskObjectStorage).publish(ctx, key, tempPath)
}

func (g *governor) createMultipart(ctx context.Context, key string, expire *time.Time) (string, error) {
	ctx, r, err := g.begin(ctx, opWrite)
	if err != nil {
		return "", err
	}
	defer r.end()
	return g.raw.(multipartStorage).createMultipart(ctx, key, expire)
}

func (g *governor) uploadPart(ctx context.Context, key, uploadID string, num int32, data []byte) (string, error) {
	ctx, r, err := g.begin(ctx, opWrite)
	if err != nil {
		return "", err
	}
	defer r.end()
	return g.raw.(multipartStorage).uploadPart(ctx, key, uploadID, num, data)
}

func (g *governor) completeMultipart(ctx context.Context, key, uploadID string, parts []multipartPart) error {
	ctx, r, err := g.begin(ctx, opWrite)
	if err != nil {
		return err
	}
	defer r.end()
	return g.raw.(multipartStorage).completeMultipart(ctx, key, uploadID, parts)
}

func (g *governor) abortMultipart(ctx context.Context, key, uploadID string) error {
	ctx, r, err := g.begin(ctx, opWrite)
	if err != nil {
		return err
	}
	defer r.end()
	return g.raw.(multipartStorage).abortMultipart(ctx, key, uploadID)
}

// httpTraceKey marks the context of a request; its HTTP requests are traced at RoundTrip.
type httpTraceKey struct{}

func withHTTPTrace(ctx context.Context) context.Context {
	return context.WithValue(ctx, httpTraceKey{}, true)
}
