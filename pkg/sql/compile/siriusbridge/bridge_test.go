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

package siriusbridge

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type testDriver struct {
	q         *testQuery
	closed    atomic.Bool
	prepares  atomic.Int32
	prepareFn func(context.Context, Request) (queryDriver, error)
}

func (d *testDriver) prepare(ctx context.Context, req Request) (queryDriver, error) {
	d.prepares.Add(1)
	if d.prepareFn != nil {
		return d.prepareFn(ctx, req)
	}
	return d.q, nil
}
func (d *testDriver) stop() error                 { return d.q.cancel() }
func (d *testDriver) close(context.Context) error { d.closed.Store(true); return nil }

type testQuery struct {
	entered   chan struct{}
	cancelled chan struct{}
	once      sync.Once
	closed    atomic.Bool
	fill      bool
	nextErr   error
	starts    atomic.Int32
	closes    atomic.Int32
	source    inputDriver
	closeFn   func(context.Context) error
}

func (q *testQuery) start() error  { q.starts.Add(1); return nil }
func (q *testQuery) cancel() error { q.once.Do(func() { close(q.cancelled) }); return nil }
func (q *testQuery) next(fill func(Result) error) error {
	close(q.entered)
	if q.nextErr != nil {
		return q.nextErr
	}
	if q.fill {
		return fill(Result{Rows: 1})
	}
	<-q.cancelled
	return context.Canceled
}
func (q *testQuery) close(ctx context.Context) error {
	q.closes.Add(1)
	if q.closeFn != nil {
		if err := q.closeFn(ctx); err != nil {
			return err
		}
	}
	q.closed.Store(true)
	return nil
}
func (q *testQuery) input(uint64) inputDriver { return q.source }
func testRuntime() (*Runtime, *testDriver) {
	d := &testDriver{q: &testQuery{entered: make(chan struct{}), cancelled: make(chan struct{})}}
	return newRuntime(d), d
}
func testRequest() Request {
	return Request{QueryID: []byte("test"), Plan: []byte{1}, Columns: []Column{{OID: 1, Name: "c"}}}
}

func TestCancellationWakesNativeCall(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	q, err := r.Prepare(context.Background(), testRequest())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- q.Run(ctx, func(Result) error { return nil }) }()
	<-d.q.entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if err := q.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !d.q.closed.Load() {
		t.Fatal("query not destroyed after cancellation joined")
	}
}

func TestCloseRetainsBorrowedResultAndRelease(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	d.q.fill = true
	var releases atomic.Int32
	req := testRequest()
	req.Release = func(context.Context) error { releases.Add(1); return nil }
	q, err := r.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	borrowed := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- q.Run(context.Background(), func(Result) error { close(borrowed); <-release; return context.Canceled })
	}()
	<-borrowed
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close: %v", err)
	}
	if d.closed.Load() || d.q.closed.Load() || releases.Load() != 0 {
		t.Fatal("destroyed borrowed result owner")
	}
	close(release)
	<-done
	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !d.closed.Load() || !d.q.closed.Load() || releases.Load() != 1 {
		t.Fatal("cleanup did not release exactly once")
	}
	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 1 {
		t.Fatal("duplicate release")
	}
}

func TestLazyProducerNotStartedByPrepare(t *testing.T) {
	r, _ := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	req := testRequest()
	var started atomic.Bool
	req.Reads = []Read{{BindingID: 1, Columns: []ReadColumn{{Column: Column{Name: "c"}}}, Producer: func(context.Context, *Input) error { started.Store(true); return nil }}}
	q, err := r.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if started.Load() {
		t.Fatal("prepare/close started an MO reader")
	}
}

func TestConfigurationBounds(t *testing.T) {
	for _, c := range []Config{{ConfigPath: "config"}, {ConfigPath: "config", GPUStreams: 128, MaxWaiting: 16, CleanupTimeout: 5 * time.Second}} {
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []Config{{}, {ConfigPath: "config", GPUStreams: 129}, {ConfigPath: "config", MaxWaiting: 17}, {ConfigPath: "config", CleanupTimeout: -time.Nanosecond}} {
		if c.Validate() == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
	if (Config{}).cleanupBudget() != 30*time.Second || (Config{CleanupTimeout: 5 * time.Second}).cleanupBudget() != 5*time.Second {
		t.Fatal("incorrect preparation cleanup budget")
	}
}

func TestCanceledBeforePreparationReleasesWithoutSealing(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	if r.cleanupTimeout != 30*time.Second {
		t.Fatalf("default cleanup timeout: %v", r.cleanupTimeout)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var releases atomic.Int32
	req := testRequest()
	req.Release = func(cleanup context.Context) error {
		releases.Add(1)
		if cleanup.Err() != nil {
			return errors.New("cleanup inherited statement cancellation")
		}
		if _, ok := cleanup.Deadline(); !ok {
			return errors.New("cleanup has no deadline")
		}
		return nil
	}
	if _, err := r.Prepare(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("Prepare: %v", err)
	}
	if releases.Load() != 1 || d.prepares.Load() != 0 || !r.Accepting() {
		t.Fatalf("release/native/admission: %d/%d/%v", releases.Load(), d.prepares.Load(), r.Accepting())
	}
}

func TestCanceledNativePreparationJoinsAndUsesCleanupBudget(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	r.cleanupTimeout = 5 * time.Second
	type valueKey struct{}
	parent, cancelDeadline := context.WithTimeout(context.WithValue(context.Background(), valueKey{}, "statement"), 30*time.Second)
	defer cancelDeadline()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	entered, joined := make(chan struct{}), make(chan struct{})
	d.prepareFn = func(ctx context.Context, _ Request) (queryDriver, error) {
		defer close(joined)
		close(entered)
		<-ctx.Done()
		return d.q, ctx.Err()
	}
	verifyCleanup := func(cleanup context.Context) error {
		if cleanup.Err() != nil || cleanup.Value(valueKey{}) != "statement" {
			return errors.New("invalid independent cleanup context")
		}
		deadline, ok := cleanup.Deadline()
		if !ok || deadline.After(time.Now().Add(r.cleanupTimeout)) {
			return errors.New("configured cleanup budget was not applied")
		}
		select {
		case <-joined:
			return nil
		default:
			return errors.New("native preparation had not joined")
		}
	}
	d.q.closeFn = verifyCleanup
	var releases atomic.Int32
	req := testRequest()
	req.Release = func(cleanup context.Context) error { releases.Add(1); return verifyCleanup(cleanup) }
	done := make(chan error, 1)
	go func() { _, err := r.Prepare(ctx, req); done <- err }()
	select {
	case <-entered:
	case <-parent.Done():
		t.Fatal("native preparation did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Prepare: %v", err)
		}
	case <-parent.Done():
		t.Fatal("canceled preparation did not clean up")
	}
	if releases.Load() != 1 || d.q.closes.Load() != 1 || !r.Accepting() {
		t.Fatalf("release/close/admission: %d/%d/%v", releases.Load(), d.q.closes.Load(), r.Accepting())
	}
}

func TestRejectedPreparationRetainsFailedRelease(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	var releases atomic.Int32
	req := Request{Release: func(context.Context) error {
		if releases.Add(1) == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}}
	if _, err := r.Prepare(context.Background(), req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Prepare: %v", err)
	}
	if r.Accepting() {
		t.Fatal("failed cleanup left native admission open")
	}
	if d.closed.Load() {
		t.Fatal("engine closed before admitted-resource cleanup")
	}
	if err := r.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 2 || !d.closed.Load() {
		t.Fatal("failed release was not retained for retry")
	}
}

func TestEffectiveQueryDeadline(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	parent, cancel := context.WithDeadline(context.Background(), now.Add(time.Minute))
	defer cancel()
	for _, test := range []struct {
		name            string
		ctx             context.Context
		requested, want time.Time
	}{
		{"native default", context.Background(), time.Time{}, now.Add(15 * time.Minute)},
		{"explicit", context.Background(), now.Add(time.Hour), now.Add(time.Hour)},
		{"parent", parent, now.Add(time.Hour), now.Add(time.Minute)},
		{"ABI cap", context.Background(), now.Add(365 * 24 * time.Hour), now.Add(time.Duration(^uint32(0)) * time.Millisecond)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := effectiveDeadline(test.ctx, test.requested, now); !got.Equal(test.want) {
				t.Fatalf("deadline %v, want %v", got, test.want)
			}
		})
	}
}

func TestTerminalTimeoutReleasesQuery(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	d.q.nextErr = context.DeadlineExceeded
	var released atomic.Int32
	req := testRequest()
	req.Release = func(context.Context) error { released.Add(1); return nil }
	q, err := r.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Run(context.Background(), func(Result) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
	if err = q.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !d.q.closed.Load() || released.Load() != 1 {
		t.Fatal("quiesced timed-out query did not release ownership")
	}
}

func TestRunUsesPreparationDeadline(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	q, err := r.Prepare(context.Background(), testRequest())
	if err != nil {
		t.Fatal(err)
	}
	if q.deadline.IsZero() {
		t.Fatal("default native deadline was not persisted")
	}
	// Put the already-prepared owner beyond its deadline without sleeps.
	q.deadline = time.Unix(1, 0)
	if err = q.Run(context.Background(), func(Result) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run: %v", err)
	}
	if d.q.starts.Load() != 0 {
		t.Fatal("expired query started native work")
	}
	if err = q.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAdmissionBoundsPreparingAndOwnedQueries(t *testing.T) {
	r, d := testRuntime()
	if r.maxQueries != 17 {
		t.Fatalf("default query capacity: %d", r.maxQueries)
	}
	r.maxQueries = 2
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entered := make(chan struct{}, 2)
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	defer release()
	d.prepareFn = func(ctx context.Context, _ Request) (queryDriver, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &testQuery{entered: make(chan struct{}), cancelled: make(chan struct{})}, nil
	}
	var acceptedReleases, rejectedReleases atomic.Int32
	request := testRequest()
	request.Release = func(context.Context) error { acceptedReleases.Add(1); return nil }
	type preparation struct {
		query *Query
		err   error
	}
	prepared := make(chan preparation, 2)
	for range 2 {
		go func() { q, err := r.Prepare(ctx, request); prepared <- preparation{q, err} }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("preparation did not enter native driver")
		}
	}
	rejected := testRequest()
	rejected.Release = func(context.Context) error { rejectedReleases.Add(1); return nil }
	if _, err := r.Prepare(ctx, rejected); err == nil {
		t.Fatal("concurrent preparation exceeded admission")
	}
	if d.prepares.Load() != 2 || rejectedReleases.Load() != 1 {
		t.Fatalf("driver/rejected cleanup: %d/%d", d.prepares.Load(), rejectedReleases.Load())
	}
	release()
	queries := make([]*Query, 0, 2)
	for range 2 {
		select {
		case result := <-prepared:
			if result.err != nil {
				t.Fatal(result.err)
			}
			queries = append(queries, result.query)
			t.Cleanup(func() { _ = result.query.Close(context.Background()) })
		case <-ctx.Done():
			t.Fatal("preparation did not complete")
		}
	}
	if _, err := r.Prepare(ctx, rejected); err == nil {
		t.Fatal("unclosed queries stopped counting against admission")
	}
	if d.prepares.Load() != 2 || rejectedReleases.Load() != 2 {
		t.Fatalf("owned-query rejection: %d/%d", d.prepares.Load(), rejectedReleases.Load())
	}
	if err := queries[0].Close(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := r.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = next.Close(context.Background()) })
	if d.prepares.Load() != 3 {
		t.Fatal("closing a query did not return admission")
	}
	if err := queries[1].Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := next.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if acceptedReleases.Load() != 3 || rejectedReleases.Load() != 2 {
		t.Fatalf("release ownership: %d/%d", acceptedReleases.Load(), rejectedReleases.Load())
	}
}

func TestReleasePanicRetainsUnlockedRetryOwner(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var releases atomic.Int32
	req := testRequest()
	req.Release = func(context.Context) error {
		if releases.Add(1) == 1 {
			panic("release boom")
		}
		return nil
	}
	q, err := r.Prepare(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Close(ctx); err == nil || !strings.Contains(err.Error(), "release callback panicked: release boom") {
		t.Fatalf("Close: %v", err)
	}
	var releasePanic *moerr.Error
	if !errors.As(err, &releasePanic) {
		t.Fatalf("release panic was not converted to moerr: %v", err)
	}
	if r.Accepting() {
		t.Fatal("cleanup panic left admission open")
	}
	if d.q.closes.Load() != 1 || releases.Load() != 1 {
		t.Fatalf("native/release calls: %d/%d", d.q.closes.Load(), releases.Load())
	}
	closed := make(chan error, 1)
	go func() { closed <- q.Close(ctx) }()
	select {
	case err = <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("release panic stranded query cleanup mutex")
	}
	if d.q.closes.Load() != 1 || releases.Load() != 2 {
		t.Fatalf("retry ownership: %d/%d", d.q.closes.Load(), releases.Load())
	}
	if err = q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 2 {
		t.Fatal("successful release was repeated")
	}
}

type panicTestInput struct {
	failed   atomic.Int32
	finished atomic.Int32
	failure  error // Read only after Run joins the producer.
}

func (i *panicTestInput) push(context.Context, uint32, []Vector) error { return nil }
func (i *panicTestInput) finish() error                                { i.finished.Add(1); return nil }
func (i *panicTestInput) fail(err error) error                         { i.failure = err; i.failed.Add(1); return nil }

func TestProducerPanicReportsFailureCancelsAndJoins(t *testing.T) {
	r, d := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	source := &panicTestInput{}
	d.q.source = source
	exited := make(chan struct{})
	req := testRequest()
	req.Reads = []Read{{BindingID: 1, Columns: []ReadColumn{{Column: Column{Name: "c"}}}, Producer: func(ctx context.Context, _ *Input) error {
		defer close(exited)
		select {
		case <-d.q.entered:
			panic("producer boom")
		case <-ctx.Done():
			return ctx.Err()
		}
	}}}
	var releases atomic.Int32
	req.Release = func(context.Context) error {
		select {
		case <-exited:
			releases.Add(1)
			return nil
		default:
			return errors.New("producer was not joined before release")
		}
	}
	q, err := r.Prepare(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	err = q.Run(ctx, func(Result) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "producer callback panicked: producer boom") {
		t.Fatalf("Run: %v", err)
	}
	var producerPanic *moerr.Error
	if !errors.As(err, &producerPanic) {
		t.Fatalf("producer panic was not converted to moerr: %v", err)
	}
	if source.failed.Load() != 1 || source.finished.Load() != 0 || source.failure == nil {
		t.Fatalf("producer fail/finish calls: %d/%d", source.failed.Load(), source.finished.Load())
	}
	select {
	case <-exited:
	default:
		t.Fatal("Run retained a producer goroutine")
	}
	if err = q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if d.q.closes.Load() != 1 || releases.Load() != 1 {
		t.Fatalf("native/release calls: %d/%d", d.q.closes.Load(), releases.Load())
	}
}
