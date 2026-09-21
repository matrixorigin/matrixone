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
	push(context.Context, uint32, []Vector) error
	finish() error
	fail(error) error
}

type Input struct{ native inputDriver }

// Push synchronously copies into a capacity-reserved native allocation. The
// producer may reuse its Go buffers when Push returns.
func (i *Input) Push(ctx context.Context, rows uint32, vectors []Vector) error {
	return i.native.push(ctx, rows, vectors)
}

type Runtime struct {
	mu             sync.Mutex
	native         driver
	closing        bool
	queries        map[*Query]struct{}
	preparing      int
	prepared       chan struct{}
	maxQueries     int
	cleanupTimeout time.Duration
}

func newRuntime(d driver) *Runtime {
	done := make(chan struct{})
	close(done)
	return &Runtime{native: d, queries: make(map[*Query]struct{}), prepared: done, maxQueries: 17, cleanupTimeout: (Config{}).cleanupBudget()}
}

// Accepting reports the Go admission gate. A failed cleanup seals admission;
// only process restart may replace a runtime whose quiescence is unproven.
func (r *Runtime) Accepting() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.closing && r.native != nil
}

func (r *Runtime) seal() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closing = true
	if r.native != nil {
		_ = r.native.stop()
	}
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

// Prepare owns native preparation while the engine mutex prevents destruction.
// The native wait queue is bounded; readers are not started here.
func (r *Runtime) Prepare(ctx context.Context, req Request) (result *Query, resultErr error) {
	req.Deadline = effectiveDeadline(ctx, req.Deadline, time.Now())
	ctx, cancelDeadline := context.WithDeadlineCause(ctx, req.Deadline, moerr.NewInternalErrorNoCtx("Sirius query deadline exceeded"))
	defer cancelDeadline()
	transferred := false
	defer func() {
		if !transferred && req.Release != nil {
			q := &Query{runtime: r, release: req.Release, idle: make(chan struct{})}
			close(q.idle)
			r.mu.Lock()
			r.queries[q] = struct{}{}
			r.mu.Unlock()
			resultErr = errors.Join(resultErr, r.cleanupPreparation(ctx, q))
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateRequest(req); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return nil, moerr.NewInvalidStateNoCtx("Sirius runtime is closing")
	}
	// Reserve before entering the native adapter: even a rejected native
	// create would otherwise allocate a C copy of every concurrent plan.
	// Moving preparation into queries below keeps the slot until Close.
	if r.preparing+len(r.queries) >= r.maxQueries {
		r.mu.Unlock()
		return nil, moerr.NewInvalidStateNoCtx("Sirius query capacity reached")
	}
	if r.preparing == 0 {
		r.prepared = make(chan struct{})
	}
	r.preparing++
	d := r.native
	r.mu.Unlock()
	native, err := d.prepare(ctx, req)
	q := &Query{runtime: r, native: native, reads: req.Reads, idle: make(chan struct{}), release: req.Release, deadline: req.Deadline}
	close(q.idle)
	r.mu.Lock()
	if native != nil || req.Release != nil {
		r.queries[q] = struct{}{}
		transferred = true
	}
	r.preparing--
	if r.preparing == 0 {
		close(r.prepared)
	}
	closing := r.closing
	r.mu.Unlock()
	if closing && err == nil {
		err = moerr.NewInvalidStateNoCtx("Sirius runtime is closing")
	}
	if err != nil {
		if transferred {
			err = errors.Join(err, r.cleanupPreparation(ctx, q))
		}
		return nil, err
	}
	return q, nil
}

// Close is retryable. No engine handle is destroyed until every query closes.
func (r *Runtime) Close(ctx context.Context) error {
	r.mu.Lock()
	r.closing = true
	var err error
	if r.native != nil {
		err = r.native.stop()
	}
	prepared := r.prepared
	r.mu.Unlock()
	select {
	case <-prepared:
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
	r.mu.Lock()
	queries := make([]*Query, 0, len(r.queries))
	for q := range r.queries {
		queries = append(queries, q)
	}
	r.mu.Unlock()
	for _, q := range queries {
		err = errors.Join(err, q.Close(ctx))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queries) != 0 {
		return err
	}
	if r.native != nil {
		closeErr := r.native.close(ctx)
		err = errors.Join(err, closeErr)
		if closeErr == nil {
			r.native = nil
		}
	}
	return err
}

type Query struct {
	mu               sync.Mutex
	runtime          *Runtime
	native           queryDriver
	reads            []Read
	running, closing bool
	idle             chan struct{}
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
			q.runtime.seal()
		}
	}()
	q.mu.Lock()
	q.closing = true
	var err error
	if q.native != nil {
		err = q.native.cancel()
	}
	idle := q.idle
	q.mu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
	closed := func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.native != nil {
			closeErr := q.native.close(ctx)
			err = errors.Join(err, closeErr)
			if closeErr == nil {
				q.native = nil
			}
		}
		if q.native == nil && q.release != nil {
			releaseErr := func() (callbackErr error) {
				defer func() {
					if recovered := recover(); recovered != nil {
						callbackErr = moerr.NewInternalErrorNoCtxf("Sirius release callback panicked: %v", recovered)
					}
				}()
				return q.release(ctx)
			}()
			err = errors.Join(err, releaseErr)
			if releaseErr == nil {
				q.release = nil
			}
		}
		return q.native == nil && q.release == nil
	}()
	if closed {
		q.runtime.mu.Lock()
		delete(q.runtime.queries, q)
		q.runtime.mu.Unlock()
	}
	return err
}

type terminal string

func (e terminal) Error() string { return string(e) }

const errEOF terminal = "Sirius end of results"
const errNotNeeded terminal = "Sirius input is no longer needed"

func IsNotNeeded(err error) bool { return errors.Is(err, errNotNeeded) }
