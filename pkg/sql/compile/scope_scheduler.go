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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
)

var (
	errScopeTaskSchedulerClosed = errors.New("scope task scheduler is closed")
	scopeTaskSchedulerID        atomic.Uint64
)

// scopeTaskScheduler is the query-local admission point for execution tasks.
//
// Root scopes are admitted through a ready queue serviced by workers owned by
// one Compile. Scope orchestration uses event tasks whose completion is
// detached from the ready worker. VM execution is driven exclusively by
// pipeline continuations; a bounded event-worker lane owns external blocking
// I/O and teardown, and always re-admits completion to this queue.
type scopeTaskScheduler struct {
	id           uint64
	ctx          context.Context
	readyMu      sync.Mutex
	readyCond    *sync.Cond
	readyQueue   []scopeScheduledTask
	readyClosed  bool
	workers      sync.WaitGroup
	eventMu      sync.Mutex
	eventCond    *sync.Cond
	eventQueue   []scopeEventTask
	eventClosed  bool
	eventWorkers sync.WaitGroup
	mu           sync.Mutex
	taskCond     *sync.Cond
	pending      int
	closed       bool
	waitOnce     sync.Once

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

	for i := 0; i < workerCount; i++ {
		s.workers.Add(1)
		go s.worker()
		s.eventWorkers.Add(1)
		go s.eventWorker()
	}
	logutil.Debugf("[scope-scheduler] create id=%d workers=%d", s.id, workerCount)
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
			defer func() {
				if recovered := recover(); recovered != nil {
					logutil.Errorf("[scope-scheduler] event source panic id=%d name=%s value=%v", s.id, task.name, recovered)
					if s.onPanic != nil {
						s.onPanic(recovered)
					}
				}
				s.finishTask()
			}()
			logutil.Debugf("[scope-scheduler] start id=%d lane=event-source name=%s", s.id, task.name)
			task.run()
		}()
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
	if task == nil {
		return errors.New("nil scope task")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errScopeTaskSchedulerClosed
	}
	s.pending++
	s.mu.Unlock()

	select {
	case <-s.ctx.Done():
		s.finishTask()
		return context.Cause(s.ctx)
	default:
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
		return errors.New("nil scope event task")
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
		return errors.New("nil pipeline continuation")
	}
	if done == nil {
		return errors.New("nil continuation completion")
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

// submitTeardown admits cleanup after the pipeline context has been canceled.
// Teardown is a terminal ownership event, not new query work, so rejecting it
// on ctx.Done would leak receiver state and mask the execution error.
func (s *scopeTaskScheduler) submitTeardown(name string, task func()) error {
	return s.submitEventSourceWithContext(name, task, true)
}

func (s *scopeTaskScheduler) submitEventSourceWithContext(name string, task func(), teardown bool) error {
	if task == nil {
		return errors.New("nil scope task")
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
		s.workers.Wait()
		s.eventWorkers.Wait()
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
