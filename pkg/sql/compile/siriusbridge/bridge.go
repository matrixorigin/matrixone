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

// Package siriusbridge owns the embedded ABI. It has no dependency on compile.
package siriusbridge

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const WindowBytes = 64 << 20

type Config struct {
	ConfigPath string
	GPUStreams uint32
	MaxWaiting uint32
	// Zero selects a 30-second preparation cleanup budget.
	CleanupTimeout time.Duration
}

func (c Config) Validate() error {
	if c.CleanupTimeout < 0 {
		return moerr.NewBadConfigNoCtx("Sirius cleanup timeout must not be negative")
	}
	if c.ConfigPath == "" || len(c.ConfigPath) > 4096 || strings.IndexByte(c.ConfigPath, 0) >= 0 || c.GPUStreams > 128 || c.MaxWaiting > 16 {
		return moerr.NewBadConfigNoCtx("Sirius requires a native configuration path, at most 128 streams and at most 16 waiting queries")
	}
	return nil
}

func (c Config) cleanupBudget() time.Duration {
	if c.CleanupTimeout == 0 {
		return 30 * time.Second
	}
	return c.CleanupTimeout
}

type Column struct {
	OID          uint32
	Width, Scale int32
	Nullable     bool
	Name         string
}

type ReadColumn struct {
	Column
	PhysicalID uint64
	Sequence   uint32
}

type Read struct {
	BindingID               uint64
	Database, Table, Schema string
	Columns                 []ReadColumn
	// Exactly one source must be supplied. Producer is invoked only after
	// native preparation and start succeed; it must stop on ctx cancellation.
	Producer    func(context.Context, *Input) error
	TAEManifest []byte
	DataRoot    string
}

type Request struct {
	AccountID uint64
	QueryID   []byte
	Snapshot  [12]byte
	Plan      []byte
	Columns   []Column
	Reads     []Read
	Deadline  time.Time
	Release   func(context.Context) error
}

type Vector struct {
	Class             uint32
	Data, Area, Nulls []byte
}

// Result contains a bounded copy of the native payload, valid during fill.
// The native lease remains charged until fill returns, including error/panic.
type Result struct {
	Rows    uint32
	Vectors []Vector
}

// driver keeps the native ownership implementation testable without CUDA. A
// production runtime can only be constructed by the build-selected New.
type driver interface {
	prepare(context.Context, Request) (queryDriver, error)
	stop() error
	close(context.Context) error
}

type queryDriver interface {
	start() error
	cancel() error
	next(func(Result) error) error
	close(context.Context) error
	input(uint64) inputDriver
}

type inputDriver interface {
	acquire(context.Context, uint64) (inputLeaseDriver, error)
	finish() error
	fail(error) error
}

type inputLeaseDriver interface {
	publish(uint32, []Vector) error
	release() error
}

type Input struct{ native inputDriver }

type InputLease struct {
	native   inputLeaseDriver
	capacity uint64
	mu       sync.Mutex
	released bool
}

// Acquire reserves native credit before callers allocate, clone, or copy the
// corresponding payload. A zero-byte logical payload still consumes one byte
// of native descriptor ownership but reports zero payload capacity here.
func (i *Input) Acquire(ctx context.Context, payloadBytes uint64) (*InputLease, error) {
	if i == nil || i.native == nil || payloadBytes > WindowBytes {
		return nil, moerr.NewInvalidInputNoCtx("Sirius input exceeds native window; split at row boundaries")
	}
	lease, err := i.native.acquire(ctx, payloadBytes)
	if err != nil {
		return nil, err
	}
	return &InputLease{native: lease, capacity: payloadBytes}, nil
}

func (l *InputLease) Capacity() uint64 {
	if l == nil {
		return 0
	}
	return l.capacity
}

func (l *InputLease) Publish(ctx context.Context, rows uint32, vectors []Vector) error {
	if l == nil || l.native == nil {
		return moerr.NewInvalidInputNoCtx("invalid Sirius input lease")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return moerr.NewInvalidInputNoCtx("Sirius input lease is released")
	}
	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	total, err := vectorPayloadBytes(vectors)
	if err != nil {
		return err
	}
	if total > l.capacity {
		return moerr.NewInvalidInputNoCtx("Sirius input payload exceeds reserved native credit")
	}
	return l.native.publish(rows, vectors)
}

// Release is idempotent. Native publication consumes the handle on success;
// explicit release owns every failure and panic path while a handle remains.
func (l *InputLease) Release() error {
	if l == nil || l.native == nil {
		return nil
	}
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	l.released = true
	l.mu.Unlock()
	return l.native.release()
}

// Push synchronously copies into a capacity-reserved native allocation. The
// producer may reuse its Go buffers when Push returns. Production MO readers
// acquire before materializing payload and do not use this compatibility API.
func (i *Input) Push(ctx context.Context, rows uint32, vectors []Vector) (err error) {
	total, err := vectorPayloadBytes(vectors)
	if err != nil {
		return err
	}
	lease, err := i.Acquire(ctx, total)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lease.Release()) }()
	return lease.Publish(ctx, rows, vectors)
}

func vectorPayloadBytes(vectors []Vector) (uint64, error) {
	if len(vectors) == 0 {
		return 0, moerr.NewInvalidInputNoCtx("empty Sirius input schema")
	}
	var total uint64
	for _, vector := range vectors {
		for _, data := range [][]byte{vector.Data, vector.Area, vector.Nulls} {
			if uint64(len(data)) > WindowBytes-total {
				return 0, moerr.NewInvalidInputNoCtx("Sirius input exceeds native window; split at row boundaries")
			}
			total += uint64(len(data))
		}
	}
	return total, nil
}

type Runtime struct {
	mu             sync.Mutex
	native         driver
	closing        bool
	queries        map[*Query]struct{}
	inflight       int
	inflightDone   chan struct{}
	stopRunning    bool
	stopDone       chan struct{}
	stopped        bool
	closeRunning   bool
	closeDone      chan struct{}
	maxQueries     int
	cleanupTimeout time.Duration
}

func newRuntime(d driver) *Runtime {
	done := make(chan struct{})
	close(done)
	return &Runtime{
		native:         d,
		queries:        make(map[*Query]struct{}),
		inflightDone:   done,
		stopDone:       done,
		closeDone:      done,
		maxQueries:     17,
		cleanupTimeout: (Config{}).cleanupBudget(),
	}
}

// Accepting reports the Go admission gate. A failed cleanup seals admission;
// only process restart may replace a runtime whose quiescence is unproven.
func (r *Runtime) Accepting() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.closing && r.native != nil
}

func (r *Runtime) seal(ctx context.Context) {
	r.mu.Lock()
	r.closing = true
	r.mu.Unlock()
	_ = r.ensureStopped(ctx)
}

func effectiveDeadline(ctx context.Context, requested, now time.Time) time.Time {
	if requested.IsZero() {
		requested = now.Add(15 * time.Minute)
	}
	// ABI timeout_ms is uint32. Go must never outlive a native deadline
	// shortened by its representation or a parent statement deadline.
	maximum := now.Add(time.Duration(^uint32(0)) * time.Millisecond)
	if maximum.Before(requested) {
		requested = maximum
	}
	if parent, ok := ctx.Deadline(); ok && parent.Before(requested) {
		requested = parent
	}
	return requested
}

func (r *Runtime) cleanupPreparation(ctx context.Context, q *Query) error {
	cleanupCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), r.cleanupTimeout,
		moerr.NewInternalErrorNoCtx("timed out cleaning up Sirius preparation"))
	defer cancel()
	return q.Close(cleanupCtx)
}

func panicError(operation string, recovered any) error {
	return moerr.NewInternalErrorNoCtxf("Sirius %s panicked: %v", operation, recovered)
}

func callCleanup(operation string, call func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError(operation, recovered)
		}
	}()
	return call()
}

func callPrepare(d driver, ctx context.Context, req Request) (native queryDriver, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError("native preparation", recovered)
		}
	}()
	return d.prepare(ctx, req)
}

func producerReads(reads []Read) []Read {
	var retained []Read
	for _, read := range reads {
		if read.Producer != nil {
			retained = append(retained, Read{BindingID: read.BindingID, Producer: read.Producer})
		}
	}
	return retained
}

func (r *Runtime) finishInflightLocked() {
	r.inflight--
	if r.inflight == 0 {
		close(r.inflightDone)
	}
}

func (r *Runtime) releaseBeforeNative(ctx context.Context, release func(context.Context) error) error {
	if release == nil {
		return nil
	}
	cleanupCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), r.cleanupTimeout,
		moerr.NewInternalErrorNoCtx("timed out cleaning up Sirius preparation"))
	defer cancel()
	return callCleanup("release callback", func() error { return release(cleanupCtx) })
}

func (r *Runtime) retainFailedReleaseLocked(release func(context.Context) error) {
	q := &Query{runtime: r, release: release, closing: true, idle: make(chan struct{})}
	close(q.idle)
	r.queries[q] = struct{}{}
}

func (r *Runtime) finishPreNative(ctx context.Context, req Request, resultErr error) error {
	releaseErr := r.releaseBeforeNative(ctx, req.Release)
	r.mu.Lock()
	if releaseErr != nil {
		r.retainFailedReleaseLocked(req.Release)
		r.closing = true
	}
	r.finishInflightLocked()
	r.mu.Unlock()
	if releaseErr != nil {
		r.seal(context.Background())
	}
	return errors.Join(resultErr, releaseErr)
}

// Prepare claims ownership before inspecting caller-controlled state. Close can
// therefore wait for every call admitted before its seal, including validation
// failures whose Release callback is still running. The native wait queue is
// bounded and readers are not started here.
func (r *Runtime) Prepare(ctx context.Context, req Request) (*Query, error) {
	r.mu.Lock()
	if r.closing || r.native == nil {
		// The closing check is this call's admission linearization point. A
		// Close already beyond it need not wait for this post-seal request, but
		// Prepare still owns Release by contract: retain a failed callback so a
		// future Close can retry it even after the engine handle is gone.
		r.mu.Unlock()
		var err error = moerr.NewInvalidStateNoCtx("Sirius runtime is closing")
		releaseErr := r.releaseBeforeNative(ctx, req.Release)
		if releaseErr != nil {
			r.mu.Lock()
			r.retainFailedReleaseLocked(req.Release)
			r.mu.Unlock()
		}
		return nil, errors.Join(err, releaseErr)
	}
	if r.inflight == 0 {
		r.inflightDone = make(chan struct{})
	}
	r.inflight++
	overCapacity := r.inflight+len(r.queries) > r.maxQueries
	d := r.native
	r.mu.Unlock()

	req.Deadline = effectiveDeadline(ctx, req.Deadline, time.Now())
	ctx, cancelDeadline := context.WithDeadlineCause(ctx, req.Deadline, moerr.NewInternalErrorNoCtx("Sirius query deadline exceeded"))
	defer cancelDeadline()
	if err := ctx.Err(); err != nil {
		return nil, r.finishPreNative(ctx, req, err)
	}
	if overCapacity {
		return nil, r.finishPreNative(ctx, req, moerr.NewInvalidStateNoCtx("Sirius query capacity reached"))
	}
	if err := validateRequest(req); err != nil {
		return nil, r.finishPreNative(ctx, req, err)
	}
	native, err := callPrepare(d, ctx, req)
	if native == nil && err == nil {
		err = moerr.NewInternalErrorNoCtx("Sirius native preparation returned no query")
	}
	q := &Query{runtime: r, native: native, reads: producerReads(req.Reads), idle: make(chan struct{}), release: req.Release, deadline: req.Deadline}
	close(q.idle)
	r.mu.Lock()
	if native != nil || req.Release != nil {
		r.queries[q] = struct{}{}
	}
	r.finishInflightLocked()
	closing := r.closing
	r.mu.Unlock()
	if closing && err == nil {
		err = moerr.NewInvalidStateNoCtx("Sirius runtime is closing")
	}
	if err != nil {
		if native != nil || req.Release != nil {
			err = errors.Join(err, r.cleanupPreparation(ctx, q))
		}
		return nil, err
	}
	return q, nil
}

func (r *Runtime) ensureStopped(ctx context.Context) error {
	for {
		r.mu.Lock()
		if r.native == nil || r.stopped {
			r.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			r.mu.Unlock()
			return err
		}
		if r.stopRunning {
			done := r.stopDone
			r.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		r.stopRunning = true
		r.stopDone = make(chan struct{})
		d, done := r.native, r.stopDone
		r.mu.Unlock()

		err := callCleanup("engine stop", d.stop)
		r.mu.Lock()
		if err == nil && r.native == d {
			r.stopped = true
		}
		r.stopRunning = false
		close(done)
		r.mu.Unlock()
		return err
	}
}

func (r *Runtime) closeAttempt(ctx context.Context) error {
	if err := r.ensureStopped(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	inflightDone := r.inflightDone
	r.mu.Unlock()
	select {
	case <-inflightDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.mu.Lock()
	queries := make([]*Query, 0, len(r.queries))
	for q := range r.queries {
		queries = append(queries, q)
	}
	r.mu.Unlock()
	var err error
	for _, q := range queries {
		err = errors.Join(err, q.Close(ctx))
	}
	if err != nil {
		return err
	}
	r.mu.Lock()
	if r.inflight != 0 || len(r.queries) != 0 {
		r.mu.Unlock()
		return moerr.NewInvalidStateNoCtx("Sirius runtime cleanup is incomplete")
	}
	d := r.native
	r.mu.Unlock()
	if d == nil {
		return nil
	}
	closeErr := callCleanup("engine close", func() error { return d.close(ctx) })
	if closeErr == nil {
		r.mu.Lock()
		if r.native == d {
			r.native = nil
		}
		r.mu.Unlock()
	}
	return closeErr
}

// Close is retryable. One caller owns each teardown attempt; competitors wait
// with their own contexts and may claim the next attempt after a failure.
func (r *Runtime) Close(ctx context.Context) error {
	for {
		r.mu.Lock()
		r.closing = true
		if r.native == nil && r.inflight == 0 && len(r.queries) == 0 {
			r.mu.Unlock()
			return nil
		}
		if r.closeRunning {
			done := r.closeDone
			r.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		r.closeRunning = true
		r.closeDone = make(chan struct{})
		done := r.closeDone
		r.mu.Unlock()

		err := r.closeAttempt(ctx)
		r.mu.Lock()
		r.closeRunning = false
		close(done)
		r.mu.Unlock()
		return err
	}
}

type Query struct {
	mu               sync.Mutex
	runtime          *Runtime
	native           queryDriver
	reads            []Read
	running, closing bool
	idle             chan struct{}
	cleanupRunning   bool
	cleanupDone      chan struct{}
	release          func(context.Context) error
	deadline         time.Time
}

// Run holds ownership across every native call, producer and borrowed result.
// Cancellation deliberately does not share the native data-path lock.
func (q *Query) Run(ctx context.Context, fill func(Result) error) (err error) {
	ctx, cancelDeadline := context.WithDeadlineCause(ctx, q.deadline, moerr.NewInternalErrorNoCtx("Sirius query deadline exceeded"))
	defer cancelDeadline()
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	if q.running || q.closing || q.native == nil {
		q.mu.Unlock()
		return moerr.NewInvalidStateNoCtx("Sirius query cannot start")
	}
	q.running = true
	q.idle = make(chan struct{})
	d := q.native
	q.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = d.cancel(); close(callbackDone) })
	var producers sync.WaitGroup
	defer func() {
		cancel()
		if !stop() {
			<-callbackDone
		}
		_ = d.cancel()
		producers.Wait()
		q.mu.Lock()
		close(q.idle)
		q.mu.Unlock()
	}()
	if err = d.start(); err != nil {
		return err
	}
	var producerMu sync.Mutex
	var producerErr error
	for _, read := range q.reads {
		if read.Producer == nil {
			continue
		}
		producers.Add(1)
		go func(read Read) {
			defer producers.Done()
			input := d.input(read.BindingID)
			e := func() (callbackErr error) {
				defer func() {
					if recovered := recover(); recovered != nil {
						callbackErr = moerr.NewInternalErrorNoCtxf("Sirius producer callback panicked: %v", recovered)
					}
				}()
				return read.Producer(ctx, &Input{native: input})
			}()
			if IsNotNeeded(e) {
				return
			}
			if e == nil {
				e = input.finish()
			} else {
				e = errors.Join(e, input.fail(e))
			}
			if e != nil && !IsNotNeeded(e) {
				producerMu.Lock()
				producerErr = errors.Join(producerErr, e)
				producerMu.Unlock()
				cancel()
			}
		}(read)
	}
	for {
		if err = ctx.Err(); err != nil {
			break
		}
		err = d.next(fill)
		if errors.Is(err, errEOF) {
			err = nil
			break
		}
		if err != nil {
			break
		}
	}
	cancel()
	producers.Wait()
	producerMu.Lock()
	err = errors.Join(err, producerErr)
	producerMu.Unlock()
	return err
}

func (q *Query) Close(ctx context.Context) (resultErr error) {
	defer func() {
		if resultErr != nil {
			q.runtime.seal(ctx)
		}
	}()
	for {
		q.mu.Lock()
		q.closing = true
		if q.native == nil && q.release == nil {
			q.mu.Unlock()
			q.runtime.mu.Lock()
			delete(q.runtime.queries, q)
			q.runtime.mu.Unlock()
			return nil
		}
		if q.cleanupRunning {
			done := q.cleanupDone
			q.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		native, idle := q.native, q.idle
		q.mu.Unlock()

		var err error
		if native != nil {
			err = callCleanup("native query cancel", native.cancel)
		}
		select {
		case <-idle:
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}

		q.mu.Lock()
		if q.cleanupRunning {
			done := q.cleanupDone
			q.mu.Unlock()
			select {
			case <-done:
				if err != nil {
					return err
				}
				continue
			case <-ctx.Done():
				return errors.Join(err, ctx.Err())
			}
		}
		q.cleanupRunning = true
		q.cleanupDone = make(chan struct{})
		done := q.cleanupDone
		native = q.native
		q.mu.Unlock()

		if native != nil {
			closeErr := callCleanup("native query close", func() error { return native.close(ctx) })
			err = errors.Join(err, closeErr)
			if closeErr == nil {
				q.mu.Lock()
				if q.native == native {
					q.native = nil
				}
				q.mu.Unlock()
			}
		}
		q.mu.Lock()
		release := q.release
		canRelease := q.native == nil
		q.mu.Unlock()
		if canRelease && release != nil {
			releaseErr := callCleanup("release callback", func() error { return release(ctx) })
			err = errors.Join(err, releaseErr)
			if releaseErr == nil {
				q.mu.Lock()
				q.release = nil
				q.mu.Unlock()
			}
		}
		q.mu.Lock()
		closed := q.native == nil && q.release == nil
		q.cleanupRunning = false
		close(done)
		q.mu.Unlock()
		if closed {
			q.runtime.mu.Lock()
			delete(q.runtime.queries, q)
			q.runtime.mu.Unlock()
		}
		return err
	}
}

type terminal string

func (e terminal) Error() string { return string(e) }

const errEOF terminal = "Sirius end of results"
const errNotNeeded terminal = "Sirius input is no longer needed"

func IsNotNeeded(err error) bool { return errors.Is(err, errNotNeeded) }
