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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type leaseTestInput struct {
	acquireFn func(context.Context, uint64) (inputLeaseDriver, error)
}

func (i *leaseTestInput) acquire(ctx context.Context, bytes uint64) (inputLeaseDriver, error) {
	return i.acquireFn(ctx, bytes)
}
func (*leaseTestInput) finish() error    { return nil }
func (*leaseTestInput) fail(error) error { return nil }

type leaseTestDriver struct {
	publishes atomic.Int32
	releases  atomic.Int32
	publishFn func() error
	releaseFn func() error
}

func (l *leaseTestDriver) publish(uint32, []Vector) error {
	l.publishes.Add(1)
	if l.publishFn != nil {
		return l.publishFn()
	}
	return nil
}
func (l *leaseTestDriver) release() error {
	l.releases.Add(1)
	if l.releaseFn != nil {
		return l.releaseFn()
	}
	return nil
}

func TestInputLeaseOwnership(t *testing.T) {
	for _, scenario := range []string{"success", "publish error", "oversized payload", "cancelled", "empty schema", "publish panic", "release error", "zero payload"} {
		t.Run(scenario, func(t *testing.T) {
			driver := new(leaseTestDriver)
			input := &Input{native: &leaseTestInput{acquireFn: func(context.Context, uint64) (inputLeaseDriver, error) { return driver, nil }}}
			capacity := uint64(1)
			vectors := []Vector{{Data: []byte{1}}}
			if scenario == "zero payload" {
				capacity, vectors = 0, []Vector{{Class: 2}}
			}
			lease, err := input.Acquire(t.Context(), capacity)
			require.NoError(t, err)
			t.Cleanup(func() { _ = lease.Release() })
			require.Equal(t, capacity, lease.Capacity())
			ctx := t.Context()
			failure := errors.New("injected lease failure")
			switch scenario {
			case "publish error":
				driver.publishFn = func() error { return failure }
			case "oversized payload":
				vectors[0].Area = []byte{2}
			case "empty schema":
				vectors = nil
			case "cancelled":
				var cancel context.CancelCauseFunc
				ctx, cancel = context.WithCancelCause(ctx)
				cancel(failure)
			case "publish panic":
				driver.publishFn = func() error { panic("publish panic") }
			case "release error":
				driver.releaseFn = func() error { return failure }
			}
			if scenario == "release error" {
				require.ErrorIs(t, lease.Release(), failure)
			} else if scenario == "publish panic" {
				require.PanicsWithValue(t, "publish panic", func() { _ = lease.Publish(ctx, 1, vectors) })
				require.NoError(t, lease.Release())
			} else {
				err = lease.Publish(ctx, 1, vectors)
				if scenario == "success" || scenario == "zero payload" {
					require.NoError(t, err)
					require.Error(t, lease.Publish(ctx, 1, vectors))
				} else {
					require.Error(t, err)
					if scenario == "publish error" || scenario == "cancelled" {
						require.ErrorIs(t, err, failure)
					}
				}
				require.NoError(t, lease.Release())
			}
			require.NoError(t, lease.Release())
			require.Error(t, lease.Publish(t.Context(), 1, []Vector{{}}))
			wantPublish, wantRelease := int32(0), int32(1)
			switch scenario {
			case "success", "zero payload":
				wantPublish, wantRelease = 1, 0
			case "publish error", "publish panic":
				wantPublish = 1
			}
			require.Equal(t, wantPublish, driver.publishes.Load())
			require.Equal(t, wantRelease, driver.releases.Load())
		})
	}
}

func TestInputAcquireControlsMaterialization(t *testing.T) {
	for _, cancelAcquire := range []bool{false, true} {
		t.Run(map[bool]string{false: "grant", true: "cancel"}[cancelAcquire], func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			entered, grant := make(chan struct{}), make(chan struct{})
			driver := new(leaseTestDriver)
			input := &Input{native: &leaseTestInput{acquireFn: func(ctx context.Context, bytes uint64) (inputLeaseDriver, error) {
				if bytes != 1 {
					return nil, errors.New("wrong reservation")
				}
				close(entered)
				select {
				case <-grant:
					return driver, nil
				case <-ctx.Done():
					return nil, context.Cause(ctx)
				}
			}}}
			var materialized atomic.Int32
			done := make(chan error, 1)
			go func() {
				done <- func() (err error) {
					lease, err := input.Acquire(ctx, 1)
					if err != nil {
						return err
					}
					defer func() { err = errors.Join(err, lease.Release()) }()
					materialized.Add(1)
					return lease.Publish(ctx, 1, []Vector{{Data: make([]byte, 1)}})
				}()
			}()
			<-entered
			require.Zero(t, materialized.Load())
			failure := errors.New("cancel acquisition")
			if cancelAcquire {
				cancel(failure)
				require.ErrorIs(t, <-done, failure)
				require.Zero(t, materialized.Load())
				require.Zero(t, driver.publishes.Load())
			} else {
				close(grant)
				require.NoError(t, <-done)
				require.Equal(t, int32(1), materialized.Load())
				require.Equal(t, int32(1), driver.publishes.Load())
			}
			require.Zero(t, driver.releases.Load())
		})
	}
}

func TestInputLeaseConcurrentTerminalOperations(t *testing.T) {
	for _, publishing := range []bool{true, false} {
		t.Run(map[bool]string{true: "publish versus release", false: "release versus release"}[publishing], func(t *testing.T) {
			entered, unblock := make(chan struct{}), make(chan struct{})
			var opened atomic.Bool
			open := func() {
				if opened.CompareAndSwap(false, true) {
					close(unblock)
				}
			}
			t.Cleanup(open)
			block := func() error { close(entered); <-unblock; return nil }
			driver := new(leaseTestDriver)
			if publishing {
				driver.publishFn = block
			} else {
				driver.releaseFn = block
			}
			lease := &InputLease{native: driver, capacity: 1}
			first, second := make(chan error, 1), make(chan error, 1)
			go func() {
				if publishing {
					first <- lease.Publish(t.Context(), 1, []Vector{{Data: []byte{1}}})
				} else {
					first <- lease.Release()
				}
			}()
			<-entered
			go func() { second <- lease.Release() }()
			open()
			require.NoError(t, <-first)
			require.NoError(t, <-second)
			require.NoError(t, lease.Release())
			if publishing {
				require.Equal(t, int32(1), driver.publishes.Load())
				require.Zero(t, driver.releases.Load())
			} else {
				require.Zero(t, driver.publishes.Load())
				require.Equal(t, int32(1), driver.releases.Load())
			}
		})
	}
}

func TestInputAcquireRejectsInvalidOwnership(t *testing.T) {
	for _, scenario := range []string{"oversized", "cancelled", "driver error", "missing lease"} {
		t.Run(scenario, func(t *testing.T) {
			var calls int
			failure := errors.New("acquisition failed")
			input := &Input{native: &leaseTestInput{acquireFn: func(context.Context, uint64) (inputLeaseDriver, error) {
				calls++
				if scenario == "driver error" {
					return nil, failure
				}
				return nil, nil
			}}}
			ctx := t.Context()
			bytes := uint64(1)
			if scenario == "oversized" {
				bytes = WindowBytes + 1
			}
			if scenario == "cancelled" {
				cancelled, cancel := context.WithCancelCause(ctx)
				cancel(failure)
				ctx = cancelled
			}
			lease, err := input.Acquire(ctx, bytes)
			require.Error(t, err)
			require.Nil(t, lease)
			if scenario == "cancelled" || scenario == "driver error" {
				require.ErrorIs(t, err, failure)
			}
			if scenario == "oversized" || scenario == "cancelled" {
				require.Zero(t, calls)
			} else {
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestQueryCancellationInterruptsInputAcquire(t *testing.T) {
	runtime, driver := testRuntime()
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	entered := make(chan struct{})
	driver.q.source = &leaseTestInput{acquireFn: func(ctx context.Context, _ uint64) (inputLeaseDriver, error) {
		close(entered)
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}}
	var materialized, releases atomic.Int32
	request := testRequest()
	request.Reads = []Read{{BindingID: 1, Columns: []ReadColumn{{Column: Column{Name: "c"}}}, Producer: func(ctx context.Context, input *Input) (err error) {
		lease, err := input.Acquire(ctx, 1)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, lease.Release()) }()
		materialized.Add(1)
		return lease.Publish(ctx, 1, []Vector{{Data: make([]byte, 1)}})
	}}}
	request.Release = func(context.Context) error { releases.Add(1); return nil }
	query, err := runtime.Prepare(ctx, request)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- query.Run(ctx, func(Result) error { return nil }) }()
	<-entered
	// The query cancel watcher remains independent of the blocked data path.
	cancel(errors.New("cancel blocked query"))
	require.Error(t, <-done)
	require.Zero(t, materialized.Load())
	require.Positive(t, driver.q.cancels.Load())
	require.NoError(t, query.Close(t.Context()))
	require.Equal(t, int32(1), releases.Load())
}

func TestInputLeaseReleaseFailureRemainsOwnedByQuery(t *testing.T) {
	runtime, driver := testRuntime()
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	failure := errors.New("batch release failed")
	var retained atomic.Bool
	leaseDriver := &leaseTestDriver{releaseFn: func() error {
		retained.Store(true) // Model nativeQuery.release's ownership transfer.
		return failure
	}}
	driver.q.source = &panicTestInput{lease: leaseDriver}
	request := testRequest()
	request.Reads = []Read{{BindingID: 1, Columns: []ReadColumn{{Column: Column{Name: "c"}}}, Producer: func(ctx context.Context, input *Input) error {
		lease, err := input.Acquire(ctx, 1)
		if err != nil {
			return err
		}
		err = lease.Release()
		return errors.Join(err, lease.Release())
	}}}
	query, err := runtime.Prepare(t.Context(), request)
	require.NoError(t, err)
	require.ErrorIs(t, query.Run(t.Context(), func(Result) error { return nil }), failure)
	require.True(t, retained.Load())
	require.Equal(t, int32(1), leaseDriver.releases.Load())
	driver.q.closeFn = func(context.Context) error {
		if driver.q.closes.Load() == 1 {
			return errors.New("cleanup unavailable")
		}
		retained.Store(false)
		return nil
	}
	require.Error(t, query.Close(t.Context()))
	require.True(t, retained.Load())
	require.NoError(t, query.Close(t.Context()))
	require.False(t, retained.Load())
	require.Equal(t, int32(1), leaseDriver.releases.Load())
	require.Equal(t, int32(2), driver.q.closes.Load())
}
