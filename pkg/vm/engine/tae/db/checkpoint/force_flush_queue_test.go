// Copyright 2021 Matrix Origin
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

package checkpoint

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

type forcedFlushCall struct {
	name       string
	call       func(context.Context, Flusher) error
	wrapsError bool
}

func forcedFlushCalls() []forcedFlushCall {
	ts := types.BuildTS(42, 0)
	return []forcedFlushCall{
		{name: "table", call: func(ctx context.Context, f Flusher) error {
			return f.FlushTable(ctx, 1, 2, ts)
		}},
		{name: "force", wrapsError: true, call: func(ctx context.Context, f Flusher) error {
			return f.ForceFlush(ctx, ts)
		}},
	}
}

type observedFlushQueue struct {
	sm.Queue
	calls   int
	onAdded func()
}

func (q *observedFlushQueue) Enqueue(item any) (any, error) {
	q.calls++
	item, err := q.Queue.Enqueue(item)
	if err == nil && q.onAdded != nil {
		q.onAdded()
	}
	return item, err
}

type rejectingFlushQueue struct {
	err error
}

func (*rejectingFlushQueue) Start() {}
func (*rejectingFlushQueue) Stop()  {}
func (q *rejectingFlushQueue) Enqueue(item any) (any, error) {
	return item, q.err
}

func flusherWithQueue(t *testing.T, sourcer logtail.Collector, queue sm.Queue) (Flusher, *observedFlushQueue) {
	t.Helper()
	impl := newFlusherImpl(nil, nil, nil, sourcer, WithFlusherForceCheckInterval(time.Hour))
	impl.flushRequestQ.Stop()
	observed := &observedFlushQueue{Queue: queue}
	impl.flushRequestQ = observed
	f := &flusher{}
	f.impl.Store(impl)
	t.Cleanup(f.Stop)
	return f, observed
}

func requireFlushQueueError(t *testing.T, err, cause error, wrapped bool) {
	t.Helper()
	require.Error(t, err)
	if wrapped {
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
		require.ErrorContains(t, err, "force flush failed")
		require.ErrorContains(t, err, cause.Error())
		require.NotErrorIs(t, err, cause)
	} else {
		require.ErrorIs(t, err, cause)
	}
}

func flushTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestForcedFlushQueueAdmission(t *testing.T) {
	for _, call := range forcedFlushCalls() {
		t.Run(call.name, func(t *testing.T) {
			t.Run("closed", func(t *testing.T) {
				queue := sm.NewSafeQueue(1, 1, nil)
				queue.Start()
				queue.Stop()
				f, observed := flusherWithQueue(t, &stalledCheckpointFlushCollector{}, queue)

				err := call.call(flushTestContext(t), f)
				requireFlushQueueError(t, err, sm.ErrClose, call.wrapsError)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
				require.Equal(t, 1, observed.calls)
			})

			t.Run("created", func(t *testing.T) {
				queue := sm.NewSafeQueue(1, 1, nil)
				f, observed := flusherWithQueue(t, &stalledCheckpointFlushCollector{}, queue)

				err := call.call(flushTestContext(t), f)
				requireFlushQueueError(t, err, sm.ErrClose, call.wrapsError)
				require.Equal(t, 1, observed.calls)
			})

			t.Run("other_error", func(t *testing.T) {
				cause := errors.New("injected enqueue failure")
				f, observed := flusherWithQueue(t, &stalledCheckpointFlushCollector{}, &rejectingFlushQueue{err: cause})

				err := call.call(flushTestContext(t), f)
				requireFlushQueueError(t, err, cause, call.wrapsError)
				require.Equal(t, 1, observed.calls)
			})

			t.Run("empty", func(t *testing.T) {
				queue := sm.NewSafeQueue(1, 1, nil) // Created queues reject admission.
				f, observed := flusherWithQueue(t, &stalledCheckpointFlushCollector{empty: true}, queue)

				require.NoError(t, call.call(flushTestContext(t), f))
				require.Zero(t, observed.calls)
			})

			t.Run("canceled_before_rejection", func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				queue := sm.NewSafeQueue(1, 1, nil)
				f, observed := flusherWithQueue(t, &stalledCheckpointFlushCollector{}, queue)

				err := call.call(ctx, f)
				requireFlushQueueError(t, err, sm.ErrClose, call.wrapsError)
				require.Equal(t, 1, observed.calls)
			})

			t.Run("canceled_after_admission", func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				f, observed := flusherWithQueue(t, &stalledCheckpointFlushCollector{}, newFlushRequestQueue())

				err := call.call(ctx, f)
				requireFlushQueueError(t, err, context.Canceled, call.wrapsError)
				require.Equal(t, 1, observed.calls)
			})

			t.Run("accepted_then_clean", func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					sourcer := &stalledCheckpointFlushCollector{}
					f, observed := flusherWithQueue(t, sourcer, newFlushRequestQueue())
					observed.onAdded = func() { sourcer.empty = true }

					require.NoError(t, call.call(t.Context(), f))
					require.Equal(t, 1, observed.calls)
				})
			})
		})
	}

	t.Run("force_with_explicit_interval_closed", func(t *testing.T) {
		queue := sm.NewSafeQueue(1, 1, nil)
		queue.Start()
		queue.Stop()
		f, observed := flusherWithQueue(t, &stalledCheckpointFlushCollector{}, queue)
		err := f.ForceFlushWithInterval(flushTestContext(t), types.BuildTS(42, 0), time.Minute)
		requireFlushQueueError(t, err, sm.ErrClose, true)
		require.Equal(t, 1, observed.calls)
	})

	t.Run("different_table_is_noop", func(t *testing.T) {
		queue := sm.NewSafeQueue(1, 1, nil)
		f, observed := flusherWithQueue(t, &stalledCheckpointFlushCollector{}, queue)
		require.NoError(t, f.FlushTable(flushTestContext(t), 1, 3, types.BuildTS(42, 0)))
		require.Zero(t, observed.calls)
	})
}

type blockingFlushCollector struct {
	*stalledCheckpointFlushCollector
	entered chan struct{}
	release chan struct{}
}

func (c *blockingFlushCollector) ScanInRangePruned(from, to types.TS) *logtail.DirtyTreeEntry {
	close(c.entered)
	<-c.release
	return c.stalledCheckpointFlushCollector.ScanInRangePruned(from, to)
}

func waitFlushPhase(t *testing.T, ctx context.Context, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("waiting for %s: %v", name, context.Cause(ctx))
	}
}

func TestForcedFlushConcurrentStop(t *testing.T) {
	for _, call := range forcedFlushCalls() {
		t.Run(call.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			collector := &blockingFlushCollector{
				stalledCheckpointFlushCollector: &stalledCheckpointFlushCollector{},
				entered:                         make(chan struct{}),
				release:                         make(chan struct{}),
			}
			f := NewFlusher(nil, nil, nil, collector, false,
				WithFlusherCronPeriod(time.Hour), WithFlusherForceCheckInterval(time.Hour))

			var releaseOnce, stopOnce sync.Once
			stopDone := make(chan struct{})
			workerDone := make(chan struct{})
			release := func() { releaseOnce.Do(func() { close(collector.release) }) }
			stop := func() {
				stopOnce.Do(func() { go func() { f.Stop(); close(stopDone) }() })
			}
			var result error
			workerStarted := false
			t.Cleanup(func() {
				release()
				cancel()
				stop()
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cleanupCancel()
				wait := func(name string, done <-chan struct{}) {
					select {
					case <-done:
					case <-cleanupCtx.Done():
						t.Errorf("waiting for %s cleanup: %v", name, context.Cause(cleanupCtx))
					}
				}
				if workerStarted {
					wait("flush worker", workerDone)
				}
				wait("flusher stop", stopDone)
			})
			f.Start()
			workerStarted = true
			go func() { result = call.call(ctx, f); close(workerDone) }()

			// The call loaded the old implementation before its dirty scan.
			waitFlushPhase(t, ctx, collector.entered, "dirty scan")
			stop()
			waitFlushPhase(t, ctx, stopDone, "flusher stop")
			require.True(t, f.IsNoop())
			release()
			waitFlushPhase(t, ctx, workerDone, "flush result")
			requireFlushQueueError(t, result, sm.ErrClose, call.wrapsError)
		})
	}
}
