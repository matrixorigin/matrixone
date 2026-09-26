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
	stops     atomic.Int32
	closes    atomic.Int32
	prepareFn func(context.Context, Request) (queryDriver, error)
	stopFn    func() error
	closeFn   func(context.Context) error
}

func (d *testDriver) prepare(ctx context.Context, req Request) (queryDriver, error) {
	d.prepares.Add(1)
	if d.prepareFn != nil {
		return d.prepareFn(ctx, req)
	}
	return d.q, nil
}
func (d *testDriver) stop() error {
	d.stops.Add(1)
	if d.stopFn != nil {
		return d.stopFn()
	}
	return d.q.cancel()
}
func (d *testDriver) close(ctx context.Context) error {
	d.closes.Add(1)
	if d.closeFn != nil {
		if err := d.closeFn(ctx); err != nil {
			return err
		}
	}
	d.closed.Store(true)
	return nil
}

type testQuery struct {
	entered   chan struct{}
	cancelled chan struct{}
	once      sync.Once
	closed    atomic.Bool
	fill      bool
	nextErr   error
	starts    atomic.Int32
	cancels   atomic.Int32
	closes    atomic.Int32
	source    inputDriver
	cancelFn  func() error
	closeFn   func(context.Context) error
}

func (q *testQuery) start() error { q.starts.Add(1); return nil }
func (q *testQuery) cancel() error {
	q.cancels.Add(1)
	if q.cancelFn != nil {
		return q.cancelFn()
	}
	q.once.Do(func() { close(q.cancelled) })
	return nil
}
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

func TestRuntimeCloseWaitsForPreValidationRelease(t *testing.T) {
	for _, test := range []struct {
		name         string
		failFirst    bool
		wantReleases int32
	}{
		{name: "release succeeds", wantReleases: 1},
		{name: "failed release is retried", failFirst: true, wantReleases: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, d := testRuntime()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			stopEntered := make(chan struct{})
			d.stopFn = func() error {
				select {
				case <-stopEntered:
				default:
					close(stopEntered)
				}
				return nil
			}
			releaseEntered := make(chan struct{})
			releaseGate := make(chan struct{})
			var releaseGateOnce sync.Once
			openReleaseGate := func() { releaseGateOnce.Do(func() { close(releaseGate) }) }
			t.Cleanup(openReleaseGate)
			var releases atomic.Int32
			d.closeFn = func(context.Context) error {
				if releases.Load() != test.wantReleases {
					return errors.New("engine close overtook release ownership")
				}
				return nil
			}
			request := testRequest()
			request.Release = func(context.Context) error {
				call := releases.Add(1)
				if call == 1 {
					close(releaseEntered)
					<-releaseGate
					if test.failFirst {
						return errors.New("injected release failure")
					}
				}
				return nil
			}
			canceled, cancelPrepare := context.WithCancel(ctx)
			cancelPrepare()
			prepared := make(chan error, 1)
			go func() { _, err := r.Prepare(canceled, request); prepared <- err }()
			select {
			case <-releaseEntered:
			case <-ctx.Done():
				t.Fatal("pre-validation release did not start")
			}
			closed := make(chan error, 1)
			go func() { closed <- r.Close(ctx) }()
			select {
			case <-stopEntered:
			case <-ctx.Done():
				t.Fatal("runtime close did not seal admission")
			}
			select {
			case err := <-closed:
				t.Fatalf("runtime closed ahead of pre-validation ownership: %v", err)
			default:
			}
			openReleaseGate()
			if err := <-prepared; !errors.Is(err, context.Canceled) {
				t.Fatalf("Prepare: %v", err)
			}
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
			if releases.Load() != test.wantReleases || !d.closed.Load() {
				t.Fatalf("release/engine close: %d/%v", releases.Load(), d.closed.Load())
			}
		})
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

func TestClosingRejectionRetainsFailedReleaseForFutureClose(t *testing.T) {
	r, d := testRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	var releases atomic.Int32
	req := testRequest()
	req.Release = func(context.Context) error {
		if releases.Add(1) == 1 {
			return errors.New("injected rejected-release failure")
		}
		return nil
	}
	if _, err := r.Prepare(ctx, req); err == nil {
		t.Fatal("closed runtime admitted a query")
	}
	if releases.Load() != 1 || d.prepares.Load() != 0 {
		t.Fatalf("release/native calls: %d/%d", releases.Load(), d.prepares.Load())
	}
	r.mu.Lock()
	retained := len(r.queries)
	r.mu.Unlock()
	if retained != 1 {
		t.Fatalf("failed release owners: %d", retained)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if releases.Load() != 2 || d.closes.Load() != 1 {
		t.Fatalf("retry/engine close calls: %d/%d", releases.Load(), d.closes.Load())
	}
}

func TestNilNativeQueryIsPreparationFailure(t *testing.T) {
	for _, test := range []struct {
		name        string
		withRelease bool
		failRelease bool
	}{
		{name: "without release"},
		{name: "with release", withRelease: true},
		{name: "failed release retained", withRelease: true, failRelease: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, d := testRuntime()
			t.Cleanup(func() { _ = r.Close(context.Background()) })
			d.prepareFn = func(context.Context, Request) (queryDriver, error) { return nil, nil }
			var releases atomic.Int32
			req := testRequest()
			if test.withRelease {
				req.Release = func(context.Context) error {
					if releases.Add(1) == 1 && test.failRelease {
						return errors.New("injected nil-query release failure")
					}
					return nil
				}
			}
			q, err := r.Prepare(context.Background(), req)
			if q != nil || err == nil || !strings.Contains(err.Error(), "returned no query") {
				t.Fatalf("Prepare: query=%v err=%v", q, err)
			}
			wantReleases := int32(0)
			if test.withRelease {
				wantReleases = 1
			}
			if releases.Load() != wantReleases || d.prepares.Load() != 1 {
				t.Fatalf("release/native calls: %d/%d", releases.Load(), d.prepares.Load())
			}
			if test.failRelease {
				if r.Accepting() {
					t.Fatal("failed nil-query cleanup left admission open")
				}
				if err = r.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if releases.Load() != 2 {
					t.Fatalf("release retry calls: %d", releases.Load())
				}
			}
		})
	}
}

func TestPrepareDoesNotRetainTAEDescriptors(t *testing.T) {
	r, _ := testRuntime()
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	req := testRequest()
	req.Reads = []Read{{
		BindingID:   1,
		Database:    "db",
		Table:       "t",
		Schema:      "schema",
		Columns:     []ReadColumn{{Column: Column{Name: "c"}, PhysicalID: 42, Sequence: 7}},
		TAEManifest: []byte("manifest that must not survive native preparation"),
		DataRoot:    "/data",
	}}
	q, err := r.Prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.reads) != 0 {
		t.Fatalf("query retained TAE descriptors: %+v", q.reads)
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

func TestConcurrentQueryCloseWaitsWithOwnContext(t *testing.T) {
	r, d := testRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	closeEntered := make(chan struct{})
	closeGate := make(chan struct{})
	var closeGateOnce sync.Once
	openCloseGate := func() { closeGateOnce.Do(func() { close(closeGate) }) }
	t.Cleanup(openCloseGate)
	d.q.closeFn = func(context.Context) error {
		close(closeEntered)
		<-closeGate
		return nil
	}
	d.stopFn = func() error { return nil }
	var releases atomic.Int32
	req := testRequest()
	req.Release = func(context.Context) error { releases.Add(1); return nil }
	q, err := r.Prepare(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- q.Close(ctx) }()
	select {
	case <-closeEntered:
	case <-ctx.Done():
		t.Fatal("query cleanup did not start")
	}
	short, cancelShort := context.WithCancel(ctx)
	cancelShort()
	if err = q.Close(short); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Close: %v", err)
	}
	if d.q.closes.Load() != 1 || releases.Load() != 0 || d.stops.Load() != 0 {
		t.Fatalf("concurrent cleanup ownership: close=%d release=%d stop=%d", d.q.closes.Load(), releases.Load(), d.stops.Load())
	}
	openCloseGate()
	if err = <-first; err != nil {
		t.Fatal(err)
	}
	if d.q.closes.Load() != 1 || releases.Load() != 1 {
		t.Fatalf("cleanup calls: %d/%d", d.q.closes.Load(), releases.Load())
	}
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if d.stops.Load() != 1 {
		t.Fatalf("runtime stop calls: %d", d.stops.Load())
	}
}

func TestConcurrentRuntimeCloseWaitsWithOwnContext(t *testing.T) {
	r, d := testRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	closeEntered := make(chan struct{})
	closeGate := make(chan struct{})
	var closeGateOnce sync.Once
	openCloseGate := func() { closeGateOnce.Do(func() { close(closeGate) }) }
	t.Cleanup(openCloseGate)
	d.closeFn = func(context.Context) error {
		close(closeEntered)
		<-closeGate
		return nil
	}
	first := make(chan error, 1)
	go func() { first <- r.Close(ctx) }()
	select {
	case <-closeEntered:
	case <-ctx.Done():
		t.Fatal("engine cleanup did not start")
	}
	short, cancelShort := context.WithCancel(ctx)
	cancelShort()
	if err := r.Close(short); !errors.Is(err, context.Canceled) {
		t.Fatalf("second Close: %v", err)
	}
	if d.closes.Load() != 1 {
		t.Fatalf("concurrent engine close calls: %d", d.closes.Load())
	}
	openCloseGate()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if d.closes.Load() != 1 || !d.closed.Load() {
		t.Fatalf("engine close calls/success: %d/%v", d.closes.Load(), d.closed.Load())
	}
}

func TestNativeCloseSuccessReleaseFailureRetriesOnlyRelease(t *testing.T) {
	r, d := testRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var releases atomic.Int32
	req := testRequest()
	req.Release = func(context.Context) error {
		if releases.Add(1) == 1 {
			return errors.New("injected release failure")
		}
		return nil
	}
	q, err := r.Prepare(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Close(ctx); err == nil {
		t.Fatal("release failure was lost")
	}
	if d.q.closes.Load() != 1 || releases.Load() != 1 {
		t.Fatalf("first cleanup: %d/%d", d.q.closes.Load(), releases.Load())
	}
	if err = q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if d.q.closes.Load() != 1 || releases.Load() != 2 {
		t.Fatalf("retry repeated native cleanup: %d/%d", d.q.closes.Load(), releases.Load())
	}
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupPanicsBecomeRetryableMOErrors(t *testing.T) {
	t.Run("native cancel", func(t *testing.T) {
		r, d := testRuntime()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var calls atomic.Int32
		d.q.cancelFn = func() error {
			if calls.Add(1) == 1 {
				panic("native cancel boom")
			}
			return nil
		}
		q, err := r.Prepare(ctx, testRequest())
		if err != nil {
			t.Fatal(err)
		}
		if err = q.Close(ctx); err == nil || !strings.Contains(err.Error(), "native query cancel panicked") {
			t.Fatalf("Close: %v", err)
		}
		var converted *moerr.Error
		if !errors.As(err, &converted) {
			t.Fatalf("panic was not converted to moerr: %v", err)
		}
		if err = r.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("native query", func(t *testing.T) {
		r, d := testRuntime()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var calls atomic.Int32
		d.q.closeFn = func(context.Context) error {
			if calls.Add(1) == 1 {
				panic("native close boom")
			}
			return nil
		}
		q, err := r.Prepare(ctx, testRequest())
		if err != nil {
			t.Fatal(err)
		}
		if err = q.Close(ctx); err == nil || !strings.Contains(err.Error(), "native query close panicked") {
			t.Fatalf("Close: %v", err)
		}
		var converted *moerr.Error
		if !errors.As(err, &converted) {
			t.Fatalf("panic was not converted to moerr: %v", err)
		}
		if err = q.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatalf("native close attempts: %d", calls.Load())
		}
		if err = r.Close(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("engine stop", func(t *testing.T) {
		r, d := testRuntime()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var calls atomic.Int32
		d.stopFn = func() error {
			if calls.Add(1) == 1 {
				panic("engine stop boom")
			}
			return nil
		}
		err := r.Close(ctx)
		if err == nil || !strings.Contains(err.Error(), "engine stop panicked") {
			t.Fatalf("Close: %v", err)
		}
		var converted *moerr.Error
		if !errors.As(err, &converted) {
			t.Fatalf("panic was not converted to moerr: %v", err)
		}
		if err = r.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 || d.stops.Load() != 2 {
			t.Fatalf("engine stop attempts: %d/%d", calls.Load(), d.stops.Load())
		}
	})

	t.Run("engine", func(t *testing.T) {
		r, d := testRuntime()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var calls atomic.Int32
		d.closeFn = func(context.Context) error {
			if calls.Add(1) == 1 {
				panic("engine close boom")
			}
			return nil
		}
		err := r.Close(ctx)
		if err == nil || !strings.Contains(err.Error(), "engine close panicked") {
			t.Fatalf("Close: %v", err)
		}
		var converted *moerr.Error
		if !errors.As(err, &converted) {
			t.Fatalf("panic was not converted to moerr: %v", err)
		}
		if err = r.Close(ctx); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 || d.closes.Load() != 2 {
			t.Fatalf("engine close attempts: %d/%d", calls.Load(), d.closes.Load())
		}
	})
}

func TestCleanupFailureSealsAdmissionBeforeStopReturns(t *testing.T) {
	r, d := testRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stopEntered := make(chan struct{})
	stopGate := make(chan struct{})
	var stopGateOnce sync.Once
	openStopGate := func() { stopGateOnce.Do(func() { close(stopGate) }) }
	t.Cleanup(openStopGate)
	d.stopFn = func() error {
		close(stopEntered)
		<-stopGate
		return nil
	}
	var releases atomic.Int32
	req := testRequest()
	req.Release = func(context.Context) error {
		if releases.Add(1) == 1 {
			return errors.New("injected release failure")
		}
		return nil
	}
	q, err := r.Prepare(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- q.Close(ctx) }()
	select {
	case <-stopEntered:
	case <-ctx.Done():
		t.Fatal("failed cleanup did not stop the engine")
	}
	var rejectedRelease atomic.Int32
	rejected := testRequest()
	rejected.Release = func(context.Context) error { rejectedRelease.Add(1); return nil }
	if _, err = r.Prepare(ctx, rejected); err == nil {
		t.Fatal("sealed runtime admitted a query")
	}
	if d.prepares.Load() != 1 || rejectedRelease.Load() != 1 {
		t.Fatalf("post-seal native/release calls: %d/%d", d.prepares.Load(), rejectedRelease.Load())
	}
	openStopGate()
	if err = <-closed; err == nil {
		t.Fatal("cleanup failure was lost")
	}
	if err = q.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

type panicTestInput struct {
	failed   atomic.Int32
	finished atomic.Int32
	failure  error // Read only after Run joins the producer.
}

type noopInputLease struct{}

func (*noopInputLease) publish(uint32, []Vector) error { return nil }
func (*noopInputLease) release() error                 { return nil }

func (i *panicTestInput) acquire(context.Context, uint64) (inputLeaseDriver, error) {
	return &noopInputLease{}, nil
}
func (i *panicTestInput) finish() error        { i.finished.Add(1); return nil }
func (i *panicTestInput) fail(err error) error { i.failure = err; i.failed.Add(1); return nil }

type panicPublishLease struct{ released atomic.Int32 }

func (*panicPublishLease) publish(uint32, []Vector) error { panic("publish panic") }
func (l *panicPublishLease) release() error               { l.released.Add(1); return nil }

type panicPublishInput struct{ lease *panicPublishLease }

func (i *panicPublishInput) acquire(context.Context, uint64) (inputLeaseDriver, error) {
	return i.lease, nil
}
func (*panicPublishInput) finish() error    { return nil }
func (*panicPublishInput) fail(error) error { return nil }

func TestInputPushPanicStillReleasesNativeLease(t *testing.T) {
	lease := new(panicPublishLease)
	input := &Input{native: &panicPublishInput{lease: lease}}
	func() {
		defer func() {
			if recovered := recover(); recovered != "publish panic" {
				t.Fatalf("panic: %v", recovered)
			}
		}()
		_ = input.Push(context.Background(), 1, []Vector{{Data: []byte{1}}})
	}()
	if lease.released.Load() != 1 {
		t.Fatalf("release calls: %d", lease.released.Load())
	}
}

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
