// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
)

var (
	errScopeTaskSchedulerClosed = moerr.NewInternalErrorNoCtx("scope task scheduler is closed")
	scopeTaskSchedulerID        atomic.Uint64
)

// scopeTaskScheduler is the query-local admission point for execution tasks.
//
// Root scopes are admitted through a ready queue serviced by workers owned by
// one Compile. Scope orchestration uses event tasks whose completion is
// detached from the ready worker. VM execution is driven exclusively by
// pipeline continuations; external synchronous compatibility calls use a
// bounded blocking lane, while transport/timer completions use event channels
// and always re-admit completion to this queue.
type scopeTaskScheduler struct {
	id              uint64
	ctx             context.Context
	readyMu         sync.Mutex
	readyCond       *sync.Cond
	readyQueue      []scopeScheduledTask
	readyClosed     bool
	workers         sync.WaitGroup
	eventMu         sync.Mutex
	eventCond       *sync.Cond
	eventQueue      []scopeEventTask
	eventClosed     bool
	eventWorkers    sync.WaitGroup
	blockingMu      sync.Mutex
	blockingCond    *sync.Cond
	blockingQueue   []scopeEventTask
	blockingClosed  bool
	blockingWorkers sync.WaitGroup
	channelMu       sync.Mutex
	channelWake     chan struct{}
	channelNext     uint64
	channelEvents   map[uint64]scopeChannelEvent
	channelClosed   bool
	channelWorker   sync.WaitGroup
	mu              sync.Mutex
	taskCond        *sync.Cond
	pending         int
	closed          bool
	waitOnce        sync.Once

	onPanic func(any)
}

// scopeScheduledTask distinguishes a regular task, whose completion is tied
// to the worker call, from an event task.  Event tasks start a continuation
// and complete only when that continuation calls done.  This keeps the ready
// worker available while MergeRun waits for child/remote events.
type scopeScheduledTask struct {
	run          func()
	asynchronous bool
	done         func()
}

type scopeEventTask struct {
	name string
	run  func()
}

// scopeChannelEvent bridges a channel that is already fed by an external
// transport into the ready queue.  The dispatcher waits on all registered
// channels with one fixed goroutine; it never parks a scheduler event worker
// on a network receive.
type scopeChannelEvent struct {
	id       uint64
	name     string
	ch       <-chan morpc.Message
	errCh    <-chan error
	teardown bool
	ready    func(morpc.Message, bool)
	errReady func(error, bool)
	reject   func(error)
}

func newScopeTaskScheduler(
	ctx context.Context,
	workerCount int,
	onPanic func(any),
) *scopeTaskScheduler {
	if workerCount < 1 {
		workerCount = 1
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s := &scopeTaskScheduler{
		id:      scopeTaskSchedulerID.Add(1),
		ctx:     ctx,
		onPanic: onPanic,
	}
	s.taskCond = sync.NewCond(&s.mu)
	s.readyCond = sync.NewCond(&s.readyMu)
	s.eventCond = sync.NewCond(&s.eventMu)
	s.blockingCond = sync.NewCond(&s.blockingMu)
	s.channelWake = make(chan struct{}, 1)
	s.channelEvents = make(map[uint64]scopeChannelEvent)

	for i := 0; i < workerCount; i++ {
		s.workers.Add(1)
		go s.worker()
		s.eventWorkers.Add(1)
		go s.eventWorker()
	}
	blockingCount := workerCount
	if blockingCount > 4 {
		blockingCount = 4
	}
	for i := 0; i < blockingCount; i++ {
		s.blockingWorkers.Add(1)
		go s.blockingWorker()
	}
	s.channelWorker.Add(1)
	go s.channelEventWorker()
	logutil.Debugf("[scope-scheduler] create id=%d workers=%d blocking-workers=%d", s.id, workerCount, blockingCount)
	return s
}

func (s *scopeTaskScheduler) worker() {
	defer s.workers.Done()
	for {
		s.readyMu.Lock()
		for len(s.readyQueue) == 0 && !s.readyClosed {
			s.readyCond.Wait()
		}
		if len(s.readyQueue) == 0 && s.readyClosed {
			s.readyMu.Unlock()
			return
		}
		task := s.readyQueue[0]
		copy(s.readyQueue, s.readyQueue[1:])
		s.readyQueue[len(s.readyQueue)-1] = scopeScheduledTask{}
		s.readyQueue = s.readyQueue[:len(s.readyQueue)-1]
		s.readyMu.Unlock()
		s.runTask(task)
	}
}

func (s *scopeTaskScheduler) eventWorker() {
	defer s.eventWorkers.Done()
	for {
		s.eventMu.Lock()
		for len(s.eventQueue) == 0 && !s.eventClosed {
			s.eventCond.Wait()
		}
		if len(s.eventQueue) == 0 && s.eventClosed {
			s.eventMu.Unlock()
			return
		}
		task := s.eventQueue[0]
		copy(s.eventQueue, s.eventQueue[1:])
		s.eventQueue[len(s.eventQueue)-1] = scopeEventTask{}
		s.eventQueue = s.eventQueue[:len(s.eventQueue)-1]
		s.eventMu.Unlock()

		func() {
			defer s.finishTask()
			defer func() {
				if recovered := recover(); recovered != nil {
					logutil.Errorf("[scope-scheduler] event source panic id=%d name=%s value=%v", s.id, task.name, recovered)
					if s.onPanic != nil {
						s.onPanic(recovered)
					}
				}
			}()
			logutil.Debugf("[scope-scheduler] start id=%d lane=event-source name=%s", s.id, task.name)
			task.run()
		}()
	}
}

// blockingWorker owns compatibility APIs that still perform synchronous
// catalog/storage/DDL work. It is deliberately separate from the event lane:
// one slow reader build must not occupy the worker that admits channel and
// timer completions. The operation still publishes its continuation through
// the normal ready/event callbacks, so Scope execution remains event-driven at
// the scheduler boundary while engine APIs migrate to native Futures.
func (s *scopeTaskScheduler) blockingWorker() {
	defer s.blockingWorkers.Done()
	for {
		s.blockingMu.Lock()
		for len(s.blockingQueue) == 0 && !s.blockingClosed {
			s.blockingCond.Wait()
		}
		if len(s.blockingQueue) == 0 && s.blockingClosed {
			s.blockingMu.Unlock()
			return
		}
		task := s.blockingQueue[0]
		copy(s.blockingQueue, s.blockingQueue[1:])
		s.blockingQueue[len(s.blockingQueue)-1] = scopeEventTask{}
		s.blockingQueue = s.blockingQueue[:len(s.blockingQueue)-1]
		s.blockingMu.Unlock()

		func() {
			defer s.finishTask()
			defer func() {
				if recovered := recover(); recovered != nil {
					logutil.Errorf("[scope-scheduler] blocking event panic id=%d name=%s value=%v", s.id, task.name, recovered)
					if s.onPanic != nil {
						s.onPanic(recovered)
					}
				}
			}()
			logutil.Debugf("[scope-scheduler] start id=%d lane=blocking name=%s", s.id, task.name)
			task.run()
		}()
	}
}

func (s *scopeTaskScheduler) channelEventWorker() {
	defer s.channelWorker.Done()
	contextDone := false
	for {
		s.channelMu.Lock()
		if s.channelClosed && len(s.channelEvents) == 0 {
			s.channelMu.Unlock()
			return
		}

		cases := make([]reflect.SelectCase, 0, len(s.channelEvents)+2)
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(s.channelWake),
		})
		contextCase := -1
		if !contextDone && s.ctx != nil && s.ctx.Done() != nil {
			contextCase = len(cases)
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(s.ctx.Done()),
			})
		}
		ids := make([]uint64, 0, len(s.channelEvents))
		for id, event := range s.channelEvents {
			ids = append(ids, id)
			selectCh := reflect.ValueOf(event.ch)
			if event.ch == nil {
				selectCh = reflect.ValueOf(event.errCh)
			}
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: selectCh,
			})
		}
		s.channelMu.Unlock()

		chosen, value, ok := reflect.Select(cases)
		if chosen == 0 {
			continue
		}
		if chosen == contextCase {
			s.rejectChannelEvents(context.Cause(s.ctx))
			contextDone = true
			continue
		}
		index := chosen - 1
		if contextCase >= 0 {
			index--
		}
		if index < 0 || index >= len(ids) {
			continue
		}
		id := ids[index]
		s.channelMu.Lock()
		event, exists := s.channelEvents[id]
		if exists {
			delete(s.channelEvents, id)
		}
		s.channelMu.Unlock()
		if !exists {
			continue
		}
		if err := s.submitRootWithContext(event.name, func() {
			if event.errReady != nil {
				var sendErr error
				if ok && value.IsValid() && value.CanInterface() {
					// A successful send publishes a nil error through the
					// interface-typed channel. Reflect represents that as a
					// nil interface value, not as a concrete error.
					if value.Kind() != reflect.Interface || !value.IsNil() {
						sendErr, _ = value.Interface().(error)
					}
				}
				event.errReady(sendErr, ok)
				return
			}
			var message morpc.Message
			if value.IsValid() && value.CanInterface() {
				message, _ = value.Interface().(morpc.Message)
			}
			event.ready(message, ok)
		}, event.teardown); err != nil {
			event.reject(err)
		}
		// Keep the registration pending until the ready callback has either
		// been admitted or rejected. This closes the wait-vs-submit race in
		// scheduler shutdown where a zero pending count could retire workers
		// before the received message was re-admitted.
		s.finishTask()
	}
}

func (s *scopeTaskScheduler) rejectChannelEvents(err error) {
	if err == nil {
		err = context.Canceled
	}
	s.channelMu.Lock()
	if len(s.channelEvents) == 0 {
		s.channelMu.Unlock()
		return
	}
	events := make([]scopeChannelEvent, 0, len(s.channelEvents))
	for id, event := range s.channelEvents {
		if event.teardown {
			continue
		}
		delete(s.channelEvents, id)
		events = append(events, event)
	}
	s.channelMu.Unlock()
	for i := range events {
		s.finishTask()
		if events[i].reject != nil {
			events[i].reject(err)
		}
	}
}

func (s *scopeTaskScheduler) runTask(task scopeScheduledTask) {
	if task.asynchronous {
		defer func() {
			if recovered := recover(); recovered != nil {
				if task.done != nil {
					task.done()
				}
				logutil.Errorf("[scope-scheduler] event task panic id=%d value=%v", s.id, recovered)
				if s.onPanic != nil {
					s.onPanic(recovered)
				}
			}
		}()
		task.run()
		return
	}

	defer s.finishTask()
	defer func() {
		if recovered := recover(); recovered != nil {
			logutil.Errorf("[scope-scheduler] task panic id=%d value=%v", s.id, recovered)
			if s.onPanic != nil {
				s.onPanic(recovered)
			}
		}
	}()
	task.run()
}

func (s *scopeTaskScheduler) finishTask() {
	s.mu.Lock()
	s.pending--
	if s.pending == 0 {
		s.taskCond.Broadcast()
	}
	s.mu.Unlock()
}

func (s *scopeTaskScheduler) submitRoot(name string, task func()) error {
	return s.submitRootWithContext(name, task, false)
}

func (s *scopeTaskScheduler) submitRootWithContext(name string, task func(), allowCanceled bool) error {
	if task == nil {
		return moerr.NewInternalErrorNoCtx("nil scope task")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errScopeTaskSchedulerClosed
	}
	s.pending++
	s.mu.Unlock()

	if !allowCanceled {
		select {
		case <-s.ctx.Done():
			s.finishTask()
			return context.Cause(s.ctx)
		default:
		}
	}

	s.readyMu.Lock()
	if s.readyClosed {
		s.readyMu.Unlock()
		s.finishTask()
		return errScopeTaskSchedulerClosed
	}
	s.readyQueue = append(s.readyQueue, scopeScheduledTask{run: func() {
		logutil.Debugf("[scope-scheduler] start id=%d lane=ready name=%s", s.id, name)
		task()
	}})
	s.readyCond.Signal()
	s.readyMu.Unlock()
	return nil
}

// submitRootAsync admits an event task to the ready queue.  The ready worker
// only starts the task; the task owns completion and must invoke done exactly
// once when its event-driven continuation reaches a terminal state.
func (s *scopeTaskScheduler) submitRootAsync(name string, start func(done func())) error {
	if start == nil {
		return moerr.NewInternalErrorNoCtx("nil scope event task")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errScopeTaskSchedulerClosed
	}
	s.pending++
	s.mu.Unlock()

	var once sync.Once
	done := func() {
		once.Do(s.finishTask)
	}
	select {
	case <-s.ctx.Done():
		done()
		return context.Cause(s.ctx)
	default:
	}

	s.readyMu.Lock()
	if s.readyClosed {
		s.readyMu.Unlock()
		done()
		return errScopeTaskSchedulerClosed
	}
	s.readyQueue = append(s.readyQueue, scopeScheduledTask{
		asynchronous: true,
		done:         done,
		run: func() {
			logutil.Debugf("[scope-scheduler] start id=%d lane=event name=%s", s.id, name)
			start(done)
		},
	})
	s.readyCond.Signal()
	s.readyMu.Unlock()
	return nil
}

// submitContinuation drives a non-blocking VM continuation from the ready
// queue. A continuation must return StepWaiting with an OnReady registration
// before it can use this API; registering a callback is the hand-off from an
// external event back to the scheduler and does not park a scheduler worker.
type pipelineContinuation interface {
	Step() (pipeline.StepResult, error)
}

type pipelineContinuationContext interface {
	Context() context.Context
}

func (s *scopeTaskScheduler) submitContinuation(
	name string,
	continuation pipelineContinuation,
	done func(error),
) error {
	if continuation == nil {
		return moerr.NewInternalErrorNoCtx("nil pipeline continuation")
	}
	if done == nil {
		return moerr.NewInternalErrorNoCtx("nil continuation completion")
	}

	var once sync.Once
	finish := func(err error) {
		once.Do(func() { done(err) })
	}
	var schedule func()
	schedule = func() {
		err := s.submitRoot(name, func() {
			result, stepErr := continuation.Step()
			if stepErr != nil {
				finish(stepErr)
				return
			}
			switch result.Status {
			case pipeline.StepDone:
				finish(nil)
			case pipeline.StepReady:
				schedule()
			case pipeline.StepWaiting:
				if result.OnReady == nil {
					finish(moerr.NewInternalErrorNoCtxf(
						"continuation %s returned StepWaiting without readiness registration", name))
					return
				}
				var readyOnce sync.Once
				var stopContext func() bool
				onReady := func() {
					readyOnce.Do(func() {
						if stopContext != nil {
							stopContext()
						}
						schedule()
					})
				}
				if contextual, ok := continuation.(pipelineContinuationContext); ok {
					if ctx := contextual.Context(); ctx != nil {
						stopContext = context.AfterFunc(ctx, onReady)
					}
				}
				if err := result.OnReady(onReady); err != nil {
					if stopContext != nil {
						stopContext()
					}
					finish(err)
				}
			default:
				finish(moerr.NewInternalErrorNoCtxf(
					"continuation %s returned unknown status %d", name, result.Status))
			}
		})
		if err != nil {
			finish(err)
		}
	}
	schedule()
	return nil
}

// submitEventSource registers an external I/O source. The source owns its
// blocking transport wait and publishes completion back into the ready queue;
// ready workers are never used as parking points for that wait. Event sources
// run on the scheduler's bounded event-worker lane rather than allocating one
// goroutine per event. It is not a VM execution lane: every operator
// continuation returns to submitRoot when it needs another quantum.
func (s *scopeTaskScheduler) submitEventSource(name string, task func()) error {
	return s.submitEventSourceWithContext(name, task, false)
}

// submitBlockingEvent admits a synchronous external operation to the fixed
// blocking lane. It is transitional: once the underlying subsystem exposes a
// Future/callback, that operation should use submitChannelEvent or
// submitErrorEvent directly and no longer occupy this lane.
func (s *scopeTaskScheduler) submitBlockingEvent(name string, task func()) error {
	return s.submitBlockingEventWithContext(name, task, false)
}

func (s *scopeTaskScheduler) submitBlockingEventWithContext(name string, task func(), allowCanceled bool) error {
	if task == nil {
		return moerr.NewInternalErrorNoCtx("nil blocking scope task")
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errScopeTaskSchedulerClosed
	}
	if !allowCanceled {
		select {
		case <-s.ctx.Done():
			s.mu.Unlock()
			return context.Cause(s.ctx)
		default:
		}
	}
	s.pending++
	s.mu.Unlock()

	s.blockingMu.Lock()
	if s.blockingClosed {
		s.blockingMu.Unlock()
		s.finishTask()
		return errScopeTaskSchedulerClosed
	}
	s.blockingQueue = append(s.blockingQueue, scopeEventTask{name: name, run: task})
	s.blockingCond.Signal()
	s.blockingMu.Unlock()
	return nil
}

// submitChannelEvent registers one receive from an externally-fed MORPC
// channel.  The channel dispatcher removes the registration as soon as one
// message (or close) is observed, then admits the callback as a ready task.
// A caller must register the next receive from its callback after it has
// reduced the message; this makes backpressure and batch ACK ownership
// explicit in the state machine.
func (s *scopeTaskScheduler) submitChannelEvent(
	name string,
	ch <-chan morpc.Message,
	ready func(morpc.Message, bool),
	reject func(error),
) error {
	_, err := s.submitChannelEventCancelable(name, ch, ready, reject, false)
	return err
}

func (s *scopeTaskScheduler) submitChannelEventWithContext(
	name string,
	ch <-chan morpc.Message,
	ready func(morpc.Message, bool),
	reject func(error),
	allowCanceled bool,
) error {
	_, err := s.submitChannelEventCancelable(name, ch, ready, reject, allowCanceled)
	return err
}

// submitChannelEventCancelable is the ownership form used by teardown state
// machines. Removing a pending registration releases its scheduler task when
// a timeout wins before the transport channel produces a value.
func (s *scopeTaskScheduler) submitChannelEventCancelable(
	name string,
	ch <-chan morpc.Message,
	ready func(morpc.Message, bool),
	reject func(error),
	allowCanceled bool,
) (func(), error) {
	if ch == nil {
		return nil, moerr.NewInternalErrorNoCtx("nil scope channel event")
	}
	if ready == nil {
		return nil, moerr.NewInternalErrorNoCtx("nil scope channel callback")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errScopeTaskSchedulerClosed
	}
	if !allowCanceled && s.ctx != nil {
		select {
		case <-s.ctx.Done():
			s.mu.Unlock()
			return nil, context.Cause(s.ctx)
		default:
		}
	}
	s.pending++
	s.mu.Unlock()

	s.channelMu.Lock()
	if s.channelClosed {
		s.channelMu.Unlock()
		s.finishTask()
		return nil, errScopeTaskSchedulerClosed
	}
	s.channelNext++
	id := s.channelNext
	s.channelEvents[id] = scopeChannelEvent{
		id:       id,
		name:     name,
		ch:       ch,
		teardown: allowCanceled,
		ready:    ready,
		reject:   reject,
	}
	s.channelMu.Unlock()
	select {
	case s.channelWake <- struct{}{}:
	default:
	}
	var cancelOnce sync.Once
	return func() {
		cancelOnce.Do(func() {
			s.channelMu.Lock()
			_, exists := s.channelEvents[id]
			if exists {
				delete(s.channelEvents, id)
			}
			s.channelMu.Unlock()
			if exists {
				s.finishTask()
				select {
				case s.channelWake <- struct{}{}:
				default:
				}
			}
		})
	}, nil
}

// submitErrorEvent registers a one-shot completion channel from an external
// writer or transport. It shares the fixed channel dispatcher with MORPC
// receive events, so a ready worker is never parked waiting for an async send
// to reach the backend writer.
func (s *scopeTaskScheduler) submitErrorEvent(
	name string,
	ch <-chan error,
	ready func(error),
) error {
	return s.submitErrorEventWithContext(name, ch, ready, false)
}

func (s *scopeTaskScheduler) submitErrorEventWithContext(
	name string,
	ch <-chan error,
	ready func(error),
	allowCanceled bool,
) error {
	if ch == nil {
		return moerr.NewInternalErrorNoCtx("nil scope error event")
	}
	if ready == nil {
		return moerr.NewInternalErrorNoCtx("nil scope error callback")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errScopeTaskSchedulerClosed
	}
	if !allowCanceled && s.ctx != nil {
		select {
		case <-s.ctx.Done():
			s.mu.Unlock()
			return context.Cause(s.ctx)
		default:
		}
	}
	s.pending++
	s.mu.Unlock()

	s.channelMu.Lock()
	if s.channelClosed {
		s.channelMu.Unlock()
		s.finishTask()
		return errScopeTaskSchedulerClosed
	}
	s.channelNext++
	id := s.channelNext
	s.channelEvents[id] = scopeChannelEvent{
		id:    id,
		name:  name,
		errCh: ch,
		errReady: func(err error, ok bool) {
			if !ok {
				err = moerr.NewInternalErrorNoCtx("scope error event channel closed")
			}
			ready(err)
		},
		reject: ready,
	}
	s.channelMu.Unlock()
	select {
	case s.channelWake <- struct{}{}:
	default:
	}
	return nil
}

// submitStreamSend adapts MORPC's optional asynchronous stream writer to the
// query scheduler. Production MORPC streams use AsyncStream, so the event
// worker is released immediately after queue admission; compatibility stream
// implementations fall back to the bounded external-I/O lane.
func (s *scopeTaskScheduler) submitStreamSend(
	name string,
	stream morpc.Stream,
	ctx context.Context,
	message morpc.Message,
	ready func(error),
) error {
	return s.submitStreamSendWithContext(name, stream, ctx, message, ready, false)
}

func (s *scopeTaskScheduler) submitStreamSendWithContext(
	name string,
	stream morpc.Stream,
	ctx context.Context,
	message morpc.Message,
	ready func(error),
	allowCanceled bool,
) error {
	if stream == nil {
		return moerr.NewInternalErrorNoCtx("nil stream")
	}
	if ready == nil {
		return moerr.NewInternalErrorNoCtx("nil stream send callback")
	}
	if async, ok := stream.(morpc.AsyncStream); ok {
		future, err := async.SendAsync(ctx, message)
		if err != nil {
			return err
		}
		if err = s.submitErrorEventWithContext(name, future.SendDone(), func(sendErr error) {
			future.Close()
			ready(sendErr)
		}, allowCanceled); err != nil {
			future.Close()
			return err
		}
		return nil
	}
	return s.submitBlockingEventWithContext(name, func() {
		ready(stream.Send(ctx, message))
	}, allowCanceled)
}

// submitTimer admits a delayed external event without parking an event
// worker. The timer keeps one scheduler task pending until it either enqueues
// the event or is canceled with the scheduler context.
func (s *scopeTaskScheduler) submitTimer(name string, delay time.Duration, task func()) error {
	_, err := s.submitTimerWithContext(name, delay, task, false)
	return err
}

// submitTimerWithContext admits a delayed event and returns a cancellation
// hook. Teardown timers are allowed to outlive the query context so a remote
// FIN/STOP handshake can still publish its terminal close event.
func (s *scopeTaskScheduler) submitTimerWithContext(
	name string,
	delay time.Duration,
	task func(),
	allowCanceled bool,
) (func(), error) {
	if task == nil {
		return nil, moerr.NewInternalErrorNoCtx("nil scope timer task")
	}
	if delay < 0 {
		delay = 0
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errScopeTaskSchedulerClosed
	}
	if !allowCanceled {
		select {
		case <-s.ctx.Done():
			s.mu.Unlock()
			return nil, context.Cause(s.ctx)
		default:
		}
	}
	s.pending++
	s.mu.Unlock()

	var timer *time.Timer
	var timerOnce sync.Once
	finishTimer := func() {
		timerOnce.Do(s.finishTask)
	}
	timer = time.AfterFunc(delay, func() {
		s.eventMu.Lock()
		if s.eventClosed {
			s.eventMu.Unlock()
			finishTimer()
			return
		}
		s.eventQueue = append(s.eventQueue, scopeEventTask{name: name, run: task})
		s.eventCond.Signal()
		s.eventMu.Unlock()
	})
	if s.ctx != nil {
		if !allowCanceled {
			context.AfterFunc(s.ctx, func() {
				if timer.Stop() {
					finishTimer()
				}
			})
		}
	}
	return func() {
		if timer.Stop() {
			finishTimer()
		}
	}, nil
}

// submitTeardown admits cleanup after the pipeline context has been canceled.
// Teardown is a terminal ownership event, not new query work, so rejecting it
// on ctx.Done would leak receiver state and mask the execution error.
func (s *scopeTaskScheduler) submitTeardown(name string, task func()) error {
	return s.submitBlockingEventWithContext(name, task, true)
}

func (s *scopeTaskScheduler) submitEventSourceWithContext(name string, task func(), teardown bool) error {
	if task == nil {
		return moerr.NewInternalErrorNoCtx("nil scope task")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errScopeTaskSchedulerClosed
	}
	if !teardown {
		select {
		case <-s.ctx.Done():
			s.mu.Unlock()
			err := context.Cause(s.ctx)
			logutil.Debugf("[scope-scheduler] reject event source id=%d name=%s ctx=%v", s.id, name, err)
			return err
		default:
		}
	}
	s.pending++
	s.mu.Unlock()

	s.eventMu.Lock()
	if s.eventClosed {
		s.eventMu.Unlock()
		s.finishTask()
		return errScopeTaskSchedulerClosed
	}
	s.eventQueue = append(s.eventQueue, scopeEventTask{name: name, run: task})
	s.eventCond.Signal()
	s.eventMu.Unlock()
	return nil
}

// wait drains all accepted tasks and then retires the query-local workers.
// It is safe to call more than once, which is useful for both runOnce and the
// Compile release path.
func (s *scopeTaskScheduler) wait() {
	if s == nil {
		return
	}
	s.waitOnce.Do(func() {
		started := time.Now()
		s.mu.Lock()
		for s.pending > 0 {
			s.taskCond.Wait()
		}
		s.closed = true
		s.mu.Unlock()
		s.readyMu.Lock()
		s.readyClosed = true
		s.readyCond.Broadcast()
		s.readyMu.Unlock()
		s.eventMu.Lock()
		s.eventClosed = true
		s.eventCond.Broadcast()
		s.eventMu.Unlock()
		s.blockingMu.Lock()
		s.blockingClosed = true
		s.blockingCond.Broadcast()
		s.blockingMu.Unlock()
		s.channelMu.Lock()
		s.channelClosed = true
		s.channelMu.Unlock()
		select {
		case s.channelWake <- struct{}{}:
		default:
		}
		s.workers.Wait()
		s.eventWorkers.Wait()
		s.blockingWorkers.Wait()
		s.channelWorker.Wait()
		logutil.Debugf("[scope-scheduler] close id=%d duration=%s", s.id, time.Since(started))
	})
}

func (c *Compile) ensureScopeTaskScheduler(workerCount int) *scopeTaskScheduler {
	c.scopeSchedulerMu.Lock()
	defer c.scopeSchedulerMu.Unlock()
	if c.scopeScheduler != nil {
		return c.scopeScheduler
	}

	ctx := context.Background()
	if c.proc != nil && c.proc.Ctx != nil {
		ctx = c.proc.Ctx
	}
	c.scopeScheduler = newScopeTaskScheduler(ctx, workerCount, c.handleScopeTaskPanic)
	if c.proc != nil {
		c.proc.SetEventSubmitter(c.scopeScheduler.submitEventSource)
		c.proc.SetBlockingSubmitter(c.scopeScheduler.submitBlockingEvent)
		c.proc.SetReadySubmitter(c.scopeScheduler.submitRoot)
	}
	return c.scopeScheduler
}

func (c *Compile) waitScopeTaskScheduler() {
	if c == nil {
		return
	}
	c.scopeSchedulerMu.Lock()
	scheduler := c.scopeScheduler
	c.scopeSchedulerMu.Unlock()
	if scheduler == nil {
		return
	}
	scheduler.wait()
	if c.proc != nil {
		c.proc.SetEventSubmitter(nil)
		c.proc.SetBlockingSubmitter(nil)
		c.proc.SetReadySubmitter(nil)
	}
	c.scopeSchedulerMu.Lock()
	if c.scopeScheduler == scheduler {
		c.scopeScheduler = nil
	}
	c.scopeSchedulerMu.Unlock()
}

func (c *Compile) handleScopeTaskPanic(recovered any) {
	if c == nil || c.proc == nil {
		return
	}
	ctx := c.proc.Ctx
	err := moerr.ConvertPanicError(ctx, recovered)
	c.proc.Errorf(
		ctx,
		"panic in query-local scope scheduler: %v sql=%s",
		err,
		commonutil.Abbreviate(c.sql, 500),
	)
	c.proc.Cancel(err)
}
