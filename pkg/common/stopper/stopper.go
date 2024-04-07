// Copyright 2021 - 2022 Matrix Origin
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

package stopper

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"go.uber.org/zap"
)

var (
	// ErrUnavailable stopper is not running
	ErrUnavailable = moerr.NewInternalErrorNoCtx("runner is unavailable")
)

var (
	defaultStoppedTimeout = time.Second * 30
)

type state int

const (
	running  = state(0)
	stopping = state(1)
	stopped  = state(2)
)

// Option stop option
type Option func(*options)

type options struct {
	stopTimeout        time.Duration
	logger             *zap.Logger
	timeoutTaskHandler func(tasks []string, timeAfterStop time.Duration)
}

func (opts *options) adjust() {
	if opts.stopTimeout == 0 {
		opts.stopTimeout = defaultStoppedTimeout
	}
	opts.logger = logutil.Adjust(opts.logger)
}

// WithStopTimeout the stopper will print the names of tasks that are still running beyond this timeout.
func WithStopTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.stopTimeout = timeout
	}
}

// WithLogger set the logger
func WithLogger(logger *zap.Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}

// WithTimeoutTaskHandler set handler to handle timeout tasks
func WithTimeoutTaskHandler(handler func(tasks []string, timeAfterStop time.Duration)) Option {
	return func(opts *options) {
		opts.timeoutTaskHandler = handler
	}
}

// Stopper a stopper used to manage all tasks that are executed in a separate goroutine,
// and Stopper can manage these goroutines centrally to avoid leaks.
// When Stopper's Stop method is called, if some tasks do not exit within the specified time,
// the names of these tasks will be returned for analysis.
type Stopper struct {
	name  string
	opts  *options
	stopC chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	lastId atomic.Uint64

	tasks struct {
		sync.RWMutex
		m map[uint64]string
	}

	mu struct {
		sync.RWMutex
		state state
	}
}

// NewStopper create a stopper
func NewStopper(name string, opts ...Option) *Stopper {
	s := &Stopper{
		name:  name,
		opts:  &options{},
		stopC: make(chan struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.tasks.m = make(map[uint64]string)
	for _, opt := range opts {
		opt(s.opts)
	}
	s.opts.adjust()

	s.mu.state = running
	return s
}

// RunTask run a task that can be cancelled. ErrUnavailable returned if stopped is not running
// See also `RunNamedTask`
// Example:
//
//	err := s.RunTask(func(ctx context.Context) {
//		select {
//		case <-ctx.Done():
//		// cancelled
//		case <-time.After(time.Second):
//			// do something
//		}
//	})
//
//	if err != nil {
//		// handle error
//		return
//	}
func (s *Stopper) RunTask(task func(context.Context)) error {
	return s.RunNamedTask("undefined", task)
}

// RunNamedTask run a task that can be cancelled. ErrUnavailable returned if stopped is not running
// Example:
//
//	err := s.RunNamedTask("named task", func(ctx context.Context) {
//		select {
//		case <-ctx.Done():
//		// cancelled
//		case <-time.After(time.Second):
//			// do something
//		}
//	})
//
//	if err != nil {
//		// handle error
//		return
//	}
func (s *Stopper) RunNamedTask(name string, task func(context.Context)) error {
	// we use read lock here for avoid race
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.mu.state != running {
		return ErrUnavailable
	}

	id, ctx := s.allocate()
	s.doRunCancelableTask(ctx, id, name, task)
	return nil
}

func (s *Stopper) RunNamedRetryTask(name string, accountId int32, retryLimit uint32, task func(context.Context, int32) error) error {
	// we use read lock here for avoid race
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.mu.state != running {
		return ErrUnavailable
	}

	id, ctx := s.allocate()
	s.doRunCancelableRetryTask(ctx, id, name, accountId, retryLimit, task)
	return nil
}

// Stop stops all task, and wait to all tasks canceled. If some tasks do not exit within the specified time,
// the names of these tasks will be print to the given logger.
func (s *Stopper) Stop() {
	s.mu.Lock()
	state := s.mu.state
	s.mu.state = stopping
	s.mu.Unlock()

	switch state {
	case stopped:
		return
	case stopping:
		<-s.stopC // wait concurrent stop completed
		return
	default:
	}

	defer func() {
		close(s.stopC)
	}()

	s.cancel()

	stopAt := time.Now()
	ticker := time.NewTicker(s.opts.stopTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tasks := s.runningTasks()
			continuous := time.Since(stopAt)
			s.opts.logger.Warn("tasks still running in stopper",
				zap.String("stopper", s.name),
				zap.Duration("continuous", continuous),
				zap.String("tasks", strings.Join(tasks, ",")))
			if s.opts.timeoutTaskHandler != nil {
				s.opts.timeoutTaskHandler(tasks, continuous)
			}
		default:
			if s.getTaskCount() == 0 {
				return
			}
		}

		// Such 5ms delay can be a problem if we need to repeatedly create different stoppers,
		// e.g. one stopper for each incoming request.
		time.Sleep(time.Millisecond * 5)
	}
}

func (s *Stopper) runningTasks() []string {
	s.tasks.RLock()
	defer s.tasks.RUnlock()
	if s.getTaskCount() == 0 {
		return nil
	}

	tasks := make([]string, 0, len(s.tasks.m))
	for _, name := range s.tasks.m {
		tasks = append(tasks, name)
	}
	return tasks
}

func (s *Stopper) setupTask(id uint64, name string) {
	s.tasks.Lock()
	defer s.tasks.Unlock()
	s.tasks.m[id] = name
}

func (s *Stopper) shutdownTask(id uint64) {
	s.tasks.Lock()
	defer s.tasks.Unlock()
	delete(s.tasks.m, id)
}

func (s *Stopper) doRunCancelableTask(ctx context.Context, taskID uint64, name string, task func(context.Context)) {
	s.setupTask(taskID, name)
	go func() {
		defer func() {
			s.shutdownTask(taskID)
		}()

		task(ctx)
	}()
}

// doRunCancelableRetryTask Canceleable and able to retry execute asynchronous tasks
func (s *Stopper) doRunCancelableRetryTask(ctx context.Context,
	taskID uint64,
	name string,
	accountId int32,
	retryLimit uint32,
	task func(context.Context, int32) error) {
	s.setupTask(taskID, name)
	go func() {
		defer func() {
			s.shutdownTask(taskID)
		}()

		wait := time.Second
		maxWait := time.Second * 10
		for i := 0; i < int(retryLimit); i++ {
			if err := task(ctx, accountId); err == nil {
				return
			}
			time.Sleep(wait)
			wait *= 2
			if wait > maxWait {
				wait = maxWait
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
}

func (s *Stopper) allocate() (uint64, context.Context) {
	// fill span{trace_id} in ctx
	return s.lastId.Add(1), trace.Generate(s.ctx)
}

// getTaskCount returns number of the running task
func (s *Stopper) getTaskCount() int {
	s.tasks.RLock()
	defer s.tasks.RUnlock()
	return len(s.tasks.m)
}
