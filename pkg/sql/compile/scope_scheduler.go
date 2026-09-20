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
// Root scopes are put on a ready queue serviced by workers owned by one
// Compile.  Pipeline dependencies are deliberately run on an overflow lane.
// MergeRun currently waits synchronously for those dependencies; putting a
// dependency behind the same finite queue would deadlock when a parent holds
// the last worker.  Keeping this boundary explicit lets the scheduler become
// fully cooperative later without reintroducing the process-wide ants pool.
type scopeTaskScheduler struct {
	id       uint64
	ctx      context.Context
	ready    chan func()
	workers  sync.WaitGroup
	mu       sync.Mutex
	taskCond *sync.Cond
	pending  int
	closed   bool
	waitOnce sync.Once

	onPanic func(any)
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
		ready:   make(chan func(), workerCount),
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

func (s *scopeTaskScheduler) runTask(task func()) {
	defer s.finishTask()
	defer func() {
		if recovered := recover(); recovered != nil {
			logutil.Errorf("[scope-scheduler] task panic id=%d value=%v", s.id, recovered)
			if s.onPanic != nil {
				s.onPanic(recovered)
			}
		}
	}()
	task()
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
	case s.ready <- func() {
		logutil.Debugf("[scope-scheduler] start id=%d lane=ready name=%s", s.id, name)
		task()
	}:
		return nil
	case <-s.ctx.Done():
		s.finishTask()
		return context.Cause(s.ctx)
	}
}

// submitDependency uses a query-owned overflow goroutine for now.  This is
// intentional: a MergeRun parent waits for its child and therefore cannot
// yield a finite ready-queue worker yet.  It still removes dependency work
// from the global ants pool and makes the eventual cooperative transition a
// local scheduler change.
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
		return context.Cause(s.ctx)
	default:
	}
	s.pending++
	s.mu.Unlock()

	go func() {
		logutil.Debugf("[scope-scheduler] start id=%d lane=dependency name=%s", s.id, name)
		s.runTask(task)
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
