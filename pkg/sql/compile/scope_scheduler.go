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
)

var (
	errScopeTaskSchedulerClosed = errors.New("scope task scheduler is closed")
	scopeTaskSchedulerID        atomic.Uint64
)

// scopeTaskScheduler is the query-local admission point for execution tasks.
//
// Root scopes are admitted through a ready queue serviced by workers owned by
// one Compile. Scope orchestration uses event tasks whose completion is
// detached from the ready worker. Blocking VM islands still use the
// query-owned dependency lane until every operator exposes a resumable step.
type scopeTaskScheduler struct {
	id       uint64
	ctx      context.Context
	ready    chan scopeScheduledTask
	workers  sync.WaitGroup
	mu       sync.Mutex
	taskCond *sync.Cond
	pending  int
	closed   bool
	waitOnce sync.Once

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
		ready:   make(chan scopeScheduledTask, workerCount),
		onPanic: onPanic,
	}
	s.taskCond = sync.NewCond(&s.mu)

	for i := 0; i < workerCount; i++ {
		s.workers.Add(1)
		go s.worker()
	}
	logutil.Debugf("[scope-scheduler] create id=%d workers=%d", s.id, workerCount)
	return s
}

func (s *scopeTaskScheduler) worker() {
	defer s.workers.Done()
	for task := range s.ready {
		s.runTask(task)
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

	select {
	case s.ready <- scopeScheduledTask{run: func() {
		logutil.Debugf("[scope-scheduler] start id=%d lane=ready name=%s", s.id, name)
		task()
	}}:
		return nil
	case <-s.ctx.Done():
		s.finishTask()
		return context.Cause(s.ctx)
	}
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

	select {
	case s.ready <- scopeScheduledTask{
		asynchronous: true,
		done:         done,
		run: func() {
			logutil.Debugf("[scope-scheduler] start id=%d lane=event name=%s", s.id, name)
			start(done)
		},
	}:
		return nil
	case <-s.ctx.Done():
		done()
		return context.Cause(s.ctx)
	}
}

// submitDependency is the compatibility lane for blocking VM and network
// operations. Event tasks use the ready queue for orchestration and keep this
// lane only for work that cannot yet yield a resumable continuation.
func (s *scopeTaskScheduler) submitDependency(name string, task func()) error {
	if task == nil {
		return errors.New("nil scope task")
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errScopeTaskSchedulerClosed
	}
	select {
	case <-s.ctx.Done():
		s.mu.Unlock()
		err := context.Cause(s.ctx)
		logutil.Debugf("[scope-scheduler] reject dependency id=%d name=%s ctx=%v", s.id, name, err)
		return err
	default:
	}
	s.pending++
	s.mu.Unlock()

	go func() {
		logutil.Debugf("[scope-scheduler] start id=%d lane=dependency name=%s", s.id, name)
		s.runTask(scopeScheduledTask{run: task})
	}()
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
		close(s.ready)
		s.workers.Wait()
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
	return c.scopeScheduler
}

func (c *Compile) submitScopeDependency(name string, task func()) error {
	if c == nil {
		return errScopeTaskSchedulerClosed
	}
	scheduler := c.ensureScopeTaskScheduler(1)
	return scheduler.submitDependency(name, task)
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
