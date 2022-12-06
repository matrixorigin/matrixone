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

// Stopper a stopper used to to manage all tasks that are executed in a separate goroutine,
// and Stopper can manage these goroutines centrally to avoid leaks.
// When Stopper's Stop method is called, if some tasks do not exit within the specified time,
// the names of these tasks will be returned for analysis.
type Stopper struct {
	name    string
	opts    *options
	stopC   chan struct{}
	cancels sync.Map // id -> cancelFunc
	tasks   sync.Map // id -> name

	atomic struct {
		lastID    uint64
		taskCount int64
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
//		// hanle error
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
//		// hanle error
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

// Stop stop all task, and wait to all tasks canceled. If some tasks do not exit within the specified time,
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
	}

	defer func() {
		close(s.stopC)
	}()

	s.cancels.Range(func(key, value interface{}) bool {
		cancel := value.(context.CancelFunc)
		cancel()
		return true
	})

	stopAt := time.Now()
	timer := time.NewTimer(s.opts.stopTimeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			tasks := s.runningTasks()
			continuous := time.Since(stopAt)
			s.opts.logger.Warn("tasks still running in stopper",
				zap.String("stopper", s.name),
				zap.Duration("continuous", continuous),
				zap.String("tasks", strings.Join(tasks, ",")))
			if s.opts.timeoutTaskHandler != nil {
				s.opts.timeoutTaskHandler(tasks, continuous)
			}
			timer.Reset(s.opts.stopTimeout)
		default:
			if s.GetTaskCount() == 0 {
				return
			}
		}

		// Such 5ms delay can be a problem if we need to repeatedly create different stoppers,
		// e.g. one stopper for each incoming request.
		time.Sleep(time.Millisecond * 5)
	}
}

func (s *Stopper) runningTasks() []string {
	if s.GetTaskCount() == 0 {
		return nil
	}

	var tasks []string
	s.tasks.Range(func(key, value interface{}) bool {
		tasks = append(tasks, value.(string))
		return true
	})
	return tasks
}

func (s *Stopper) setupTask(id uint64, name string) {
	s.tasks.Store(id, name)
	s.addTask(1)
}

func (s *Stopper) shutdownTask(id uint64) {
	s.tasks.Delete(id)
	s.addTask(-1)
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

func (s *Stopper) allocate() (uint64, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = trace.Generate(ctx) // fill span{trace_id} in ctx
	id := s.nextTaskID()
	s.cancels.Store(id, cancel)
	return id, ctx
}

func (s *Stopper) nextTaskID() uint64 {
	return atomic.AddUint64(&s.atomic.lastID, 1)
}

func (s *Stopper) addTask(v int64) {
	atomic.AddInt64(&s.atomic.taskCount, v)
}

// GetTaskCount returns number of the running task
func (s *Stopper) GetTaskCount() int64 {
	return atomic.LoadInt64(&s.atomic.taskCount)
}
