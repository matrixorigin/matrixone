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

type promotionKey struct{}

// withPromotion returns ctx under which requests to object storage are background until
// promoted is closed.
func withPromotion(ctx context.Context, promoted <-chan struct{}) context.Context {
	return context.WithValue(ctx, promotionKey{}, promoted)
}

func promotionOf(ctx context.Context) <-chan struct{} {
	ch, _ := ctx.Value(promotionKey{}).(<-chan struct{})
	return ch
}

func requestClassOf(ctx context.Context) requestClass {
	if promoted := promotionOf(ctx); promoted != nil {
		select {
		case <-promoted:
			return foregroundRequest
		default:
			return backgroundRequest
		}
	}
	if ctx.Value(backgroundRequestKey{}) == true {
		return backgroundRequest
	}
	return foregroundRequest
}

var errServerClosed = moerr.NewInternalErrorNoCtx("fileservice: object storage server closed")

// minRequestRoom is the least room of a server.
const minRequestRoom = 64

// server makes every request to the object storage raw, at most capacity at once.
// Waiting requests are admitted foreground first, then background, each in arrival
// order; background requests run at most backgroundMax at once. A running request is
// never stopped.
type objectStorageServer struct {
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

// newObjectStorageServer returns a server of raw with room for capacity requests, at least
// minRequestRoom, half of it for background requests.
func newObjectStorageServer(raw ObjectStorage, capacity int) *objectStorageServer {
	g := &objectStorageServer{raw: raw, closedCh: make(chan struct{})}
	for op, name := range opNames {
		g.ops[op] = metric.FSObjectStorageOperations.WithLabelValues("s3", name)
	}
	g.capacity = max(capacity, minRequestRoom)
	g.backgroundMax = g.capacity / 2
	return g
}

func (g *objectStorageServer) canRunLocked(c requestClass) bool {
	if g.running[foregroundRequest]+g.running[backgroundRequest] >= g.capacity {
		return false
	}
	return c == foregroundRequest || g.running[backgroundRequest] < g.backgroundMax
}

// wait waits for room for a request of class c until ctx ends or g closes, and returns
// the class of the room granted. A background request whose promotion of ctx closes
// waits as a foreground one from then.
func (g *objectStorageServer) wait(ctx context.Context, c requestClass) (requestClass, error) {
	if err := ctx.Err(); err != nil {
		return c, err
	}
	var promoted <-chan struct{}
	if c == backgroundRequest {
		if promoted = promotionOf(ctx); promoted != nil {
			select {
			case <-promoted:
				c, promoted = foregroundRequest, nil
			default:
			}
		}
	}
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return c, errServerClosed
	}
	if g.waiting[foregroundRequest].Len() == 0 &&
		(c == foregroundRequest || g.waiting[backgroundRequest].Len() == 0) && g.canRunLocked(c) {
		g.startLocked(c)
		g.mu.Unlock()
		return c, nil
	}
	w := &roomWaiter{class: c, granted: make(chan struct{})}
	w.elem = g.waiting[c].PushBack(w)
	waitingGauge(c).Inc()
	g.mu.Unlock()
	for {
		select {
		case <-w.granted:
			return w.class, nil
		case <-promoted:
			promoted = nil
			g.promote(w)
		case <-ctx.Done():
			return g.abandon(w, ctx.Err())
		case <-g.closedCh:
			return g.abandon(w, errServerClosed)
		}
	}
}

// promote moves w, while it waits as a background request, to the foreground waiters.
func (g *objectStorageServer) promote(w *roomWaiter) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if w.elem == nil || w.class == foregroundRequest {
		return
	}
	g.waiting[backgroundRequest].Remove(w.elem)
	waitingGauge(backgroundRequest).Dec()
	w.class = foregroundRequest
	w.elem = g.waiting[foregroundRequest].PushBack(w)
	waitingGauge(foregroundRequest).Inc()
	g.grantLocked()
}

// abandon ends the wait of w with err, freeing the room it was granted meanwhile.
func (g *objectStorageServer) abandon(w *roomWaiter, err error) (requestClass, error) {
	g.mu.Lock()
	c := w.class
	if w.elem == nil {
		g.mu.Unlock()
		g.done(c)
		return c, err
	}
	g.waiting[c].Remove(w.elem)
	w.elem = nil
	waitingGauge(c).Dec()
	g.grantLocked()
	g.mu.Unlock()
	return c, err
}

func (g *objectStorageServer) startLocked(c requestClass) {
	g.running[c]++
	runningGauge(c).Inc()
}

func (g *objectStorageServer) done(c requestClass) {
	g.mu.Lock()
	g.running[c]--
	runningGauge(c).Dec()
	g.grantLocked()
	g.mu.Unlock()
}

func (g *objectStorageServer) grantLocked() {
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

// close ends the waits for room; running requests run on.
func (g *objectStorageServer) close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed {
		g.closed = true
		close(g.closedCh)
	}
}

// counts returns the requests running and waiting of each class.
func (g *objectStorageServer) counts() (running, waiting [2]int) {
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

// requestRoom is the room of a request: held while the request runs and while a retry of
// it does not wait.
type requestRoom struct {
	g     *objectStorageServer
	class requestClass

	mu   sync.Mutex
	held bool
}

type requestKey struct{}

// resume waits for room for r, unless r holds room.
func (r *requestRoom) resume(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	held := r.held
	r.mu.Unlock()
	if held {
		return nil
	}
	c, err := r.g.wait(ctx, r.class)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.held {
		r.g.done(r.class)
	}
	r.class, r.held = c, true
	return nil
}

// end frees the room r holds.
func (r *requestRoom) end() {
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
// returns the room to resume, or nil.
func pauseRequest(ctx context.Context) *requestRoom {
	r, _ := ctx.Value(requestKey{}).(*requestRoom)
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

// request is a request Do makes; its unexported method closes it to other packages.
type request interface {
	op() objectStorageOp
}

var requestTypes = []request{statRequest{}, existsRequest{}, deleteRequest{}, copyRequest{}, statIdentityRequest{},
	listRequest{}, rangeRead{}, putRequest{}, putFileRequest{}, publishRequest{}, createMultipartRequest{},
	uploadPartRequest{}, completeMultipartRequest{}, abortMultipartRequest{}}

type (
	statRequest   struct{ key string }
	existsRequest struct{ key string }
	deleteRequest struct{ keys []string }
	// copyRequest copies srcKey of src into dstKey of raw on the provider.
	copyRequest struct {
		src            ObjectStorage
		srcKey, dstKey string
	}
	statIdentityRequest struct{ key string }
	// listRequest takes the next entry of l.
	listRequest struct{ l *listing }
	putRequest  struct {
		key    string
		data   []byte
		expire *time.Time
	}
	// putFileRequest writes the size bytes of file as key.
	putFileRequest struct {
		key    string
		file   *os.File
		size   int64
		expire *time.Time
	}
	// publishRequest publishes the file a disk object storage staged at tempPath as key.
	publishRequest         struct{ key, tempPath string }
	createMultipartRequest struct {
		key    string
		expire *time.Time
	}
	uploadPartRequest struct {
		key, uploadID string
		num           int32
		data          []byte
	}
	completeMultipartRequest struct {
		key, uploadID string
		parts         []multipartPart
	}
	abortMultipartRequest struct{ key, uploadID string }
)

func (statRequest) op() objectStorageOp              { return opStat }
func (existsRequest) op() objectStorageOp            { return opExists }
func (deleteRequest) op() objectStorageOp            { return opDelete }
func (copyRequest) op() objectStorageOp              { return opWrite }
func (statIdentityRequest) op() objectStorageOp      { return opStat }
func (listRequest) op() objectStorageOp              { return opCount }
func (rangeRead) op() objectStorageOp                { return opRead }
func (putRequest) op() objectStorageOp               { return opWrite }
func (putFileRequest) op() objectStorageOp           { return opWrite }
func (publishRequest) op() objectStorageOp           { return opWrite }
func (createMultipartRequest) op() objectStorageOp   { return opWrite }
func (uploadPartRequest) op() objectStorageOp        { return opWrite }
func (completeMultipartRequest) op() objectStorageOp { return opWrite }
func (abortMultipartRequest) op() objectStorageOp    { return opWrite }

// result is what a request returns.
type result struct {
	n        int64          // the size of a stat; the bytes a read read
	ok       bool           // whether a key exists; whether raw copied; false at a listing's end
	identity ObjectIdentity // of a statIdentity
	entry    *DirEntry      // of a list
	data     []byte         // of a read into neither buf nor file
	id       string         // the upload of a createMultipart; the etag of an uploadPart
}

// Do waits for room for req in the class of ctx, makes req on raw, and frees the room.
func (g *objectStorageServer) Do(ctx context.Context, req request) (result, error) {
	switch req.(type) {
	case copyRequest:
		if _, ok := g.raw.(objectStorageCopier); !ok {
			return result{}, nil
		}
	case statIdentityRequest:
		if _, ok := g.raw.(objectStorageIdentityReader); !ok {
			return result{}, moerr.NewNotSupported(ctx, "object storage identity")
		}
	}
	r := &requestRoom{g: g, class: requestClassOf(ctx)}
	if q, ok := req.(listRequest); ok {
		r = q.l.room
	}
	if err := r.resume(ctx); err != nil {
		return result{}, err
	}
	defer r.end()
	if op := req.op(); op < opCount {
		g.ops[op].Inc()
	}
	return makeRequest(context.WithValue(withHTTPTrace(ctx), requestKey{}, r), g.raw, req)
}

// makeRequest makes req on raw.
func makeRequest(ctx context.Context, raw ObjectStorage, req request) (res result, err error) {
	switch q := req.(type) {
	case statRequest:
		res.n, err = raw.Stat(ctx, q.key)
	case existsRequest:
		res.ok, err = raw.Exists(ctx, q.key)
	case deleteRequest:
		err = raw.Delete(ctx, q.keys...)
	case copyRequest:
		res.ok, err = raw.(objectStorageCopier).CopyObject(ctx, q.src, q.srcKey, q.dstKey)
	case statIdentityRequest:
		res.identity, err = raw.(objectStorageIdentityReader).StatObjectIdentity(ctx, q.key)
	case listRequest:
		res.entry, err, res.ok = q.l.next()
	case rangeRead:
		res.data, res.n, err = readRangeBody(ctx, raw, q)
	case putRequest:
		size := int64(len(q.data))
		err = raw.Write(ctx, q.key, bytes.NewReader(q.data), &size, q.expire)
	case putFileRequest:
		err = raw.Write(ctx, q.key, io.NewSectionReader(q.file, 0, q.size), &q.size, q.expire)
	case publishRequest:
		err = raw.(*diskObjectStorage).publish(ctx, q.key, q.tempPath)
	case createMultipartRequest:
		res.id, err = raw.(multipartStorage).createMultipart(ctx, q.key, q.expire)
	case uploadPartRequest:
		res.id, err = raw.(multipartStorage).uploadPart(ctx, q.key, q.uploadID, q.num, q.data)
	case completeMultipartRequest:
		err = raw.(multipartStorage).completeMultipart(ctx, q.key, q.uploadID, q.parts)
	case abortMultipartRequest:
		err = raw.(multipartStorage).abortMultipart(ctx, q.key, q.uploadID)
	}
	return res, err
}

// listing is a listing of raw whose entries listRequests take under room.
type listing struct {
	room *requestRoom
	next func() (*DirEntry, error, bool)
}

// list lists prefix. Each entry is taken by a listRequest, so the request that reads a page
// holds room while it runs, and the entry is yielded with no room held.
func (g *objectStorageServer) list(ctx context.Context, prefix string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		l := &listing{room: &requestRoom{g: g, class: requestClassOf(ctx)}}
		next, stop := iter.Pull2(g.raw.List(context.WithValue(withHTTPTrace(ctx), requestKey{}, l.room), prefix))
		defer stop()
		l.next = next
		g.ops[opList].Inc()
		for {
			res, err := g.Do(ctx, listRequest{l: l})
			if err == nil && !res.ok || !yield(res.entry, err) || err != nil {
				return
			}
		}
	}
}

// rangeRead is a read of the bytes [start, end) of key, to the end of the object when end
// is negative: into buf, filled as far as the bytes go; or into file at offset, a source
// error then wrapped as a diskCacheSourceError; or, with neither, into the bytes returned.
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

// readRangeBody makes rr and returns the bytes it read into neither buf nor file, and how
// many it read.
func readRangeBody(ctx context.Context, raw ObjectStorage, rr rangeRead) ([]byte, int64, error) {
	rr.progress.began()
	t0 := time.Now()
	body, err := open(ctx, raw, rr)
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

func open(ctx context.Context, raw ObjectStorage, rr rangeRead) (io.ReadCloser, error) {
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
		reader, ok := raw.(objectStorageIdentityReader)
		if !ok {
			return nil, moerr.NewNotSupported(ctx, "conditional object storage read")
		}
		body, err = reader.ReadObjectWithIdentity(ctx, rr.key, min, max, *rr.identity)
	} else {
		body, err = raw.Read(ctx, rr.key, min, max)
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
	if p.download != nil {
		p.download.begin(p.part)
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

// httpTraceKey marks the context of a request; its HTTP requests are traced at RoundTrip.
type httpTraceKey struct{}

func withHTTPTrace(ctx context.Context) context.Context {
	return context.WithValue(ctx, httpTraceKey{}, true)
}
