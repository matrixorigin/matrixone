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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
)

type testPipelineContinuation struct {
	statuses   []pipeline.StepStatus
	step       int
	registered chan func()
	noRegister bool
}

type cancelAwarePipelineContinuation struct {
	ctx        context.Context
	registered chan func()
	step       int
}

func (c *cancelAwarePipelineContinuation) Context() context.Context {
	return c.ctx
}

func (c *cancelAwarePipelineContinuation) Step() (pipeline.StepResult, error) {
	if c.step == 0 {
		c.step++
		return pipeline.StepResult{
			Status: pipeline.StepWaiting,
			OnReady: func(ready func()) error {
				c.registered <- ready
				return nil
			},
		}, nil
	}
	return pipeline.StepResult{Status: pipeline.StepDone}, nil
}

func (c *testPipelineContinuation) Step() (pipeline.StepResult, error) {
	status := c.statuses[c.step]
	c.step++
	result := pipeline.StepResult{Status: status}
	if status == pipeline.StepWaiting && !c.noRegister {
		result.OnReady = func(ready func()) error {
			c.registered <- ready
			return nil
		}
	}
	return result, nil
}

func TestScopeTaskSchedulerRunsReadyAndEventSources(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 2, nil)
	readyDone := make(chan struct{})
	dependencyDone := make(chan struct{})
	submitErr := make(chan error, 1)

	if err := scheduler.submitRoot("test-root", func() {
		close(readyDone)
		submitErr <- scheduler.submitEventSource("test-event-source", func() {
			close(dependencyDone)
		})
	}); err != nil {
		t.Fatalf("submit root: %v", err)
	}
	scheduler.wait()

	if err := <-submitErr; err != nil {
		t.Fatalf("submit dependency: %v", err)
	}
	select {
	case <-readyDone:
	default:
		t.Fatal("ready task did not run")
	}
	select {
	case <-dependencyDone:
	default:
		t.Fatal("dependency task did not run")
	}
}

func TestScopeTaskSchedulerEventSourceDoesNotDeadlockSingleWorker(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	dependencyDone := make(chan struct{})
	submitErr := make(chan error, 1)

	if err := scheduler.submitRoot("blocking-parent", func() {
		if err := scheduler.submitEventSource("event-source", func() {
			close(dependencyDone)
		}); err != nil {
			submitErr <- err
			close(dependencyDone)
			return
		}
		submitErr <- nil
		<-dependencyDone
	}); err != nil {
		t.Fatalf("submit root: %v", err)
	}
	scheduler.wait()
	if err := <-submitErr; err != nil {
		t.Fatalf("submit dependency: %v", err)
	}
}

func TestScopeTaskSchedulerReportsPanic(t *testing.T) {
	panicSeen := make(chan any, 1)
	scheduler := newScopeTaskScheduler(context.Background(), 1, func(value any) {
		panicSeen <- value
	})

	if err := scheduler.submitRoot("panic", func() { panic("scope task failure") }); err != nil {
		t.Fatalf("submit root: %v", err)
	}
	scheduler.wait()

	select {
	case value := <-panicSeen:
		if value != "scope task failure" {
			t.Fatalf("unexpected panic value: %v", value)
		}
	default:
		t.Fatal("scheduler did not report panic")
	}
}

func TestScopeTaskSchedulerRejectsTasksAfterWait(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	scheduler.wait()
	if err := scheduler.submitEventSource("late", func() {}); !errors.Is(err, errScopeTaskSchedulerClosed) {
		t.Fatalf("expected closed scheduler error, got %v", err)
	}
}

func TestScopeTaskSchedulerRejectsRootOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	scheduler := newScopeTaskScheduler(ctx, 1, nil)
	if err := scheduler.submitRoot("canceled", func() {}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerEventTaskWaitsForCompletion(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := scheduler.submitRootAsync("event", func(done func()) {
		close(started)
		go func() {
			<-release
			done()
		}()
	}); err != nil {
		t.Fatalf("submit event task: %v", err)
	}
	<-started
	ready := make(chan struct{})
	if err := scheduler.submitRoot("ready-after-event", func() {
		close(ready)
	}); err != nil {
		t.Fatalf("submit ready task: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("ready worker remained occupied by event task")
	}

	waitDone := make(chan struct{})
	go func() {
		scheduler.wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		t.Fatal("scheduler retired before event completion")
	default:
	}

	close(release)
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not retire after event completion")
	}
}

func TestScopeTaskSchedulerContinuationReturnsReadyWorkerWhileWaiting(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	continuation := &testPipelineContinuation{
		statuses:   []pipeline.StepStatus{pipeline.StepWaiting, pipeline.StepReady, pipeline.StepDone},
		registered: make(chan func(), 1),
	}
	done := make(chan error, 1)
	if err := scheduler.submitContinuation("continuation", continuation, func(err error) {
		done <- err
	}); err != nil {
		t.Fatalf("submit continuation: %v", err)
	}
	var ready func()
	select {
	case ready = <-continuation.registered:
	case <-time.After(time.Second):
		t.Fatal("continuation did not register its readiness callback")
	}
	select {
	case err := <-done:
		t.Fatalf("continuation completed before readiness event: %v", err)
	default:
	}
	ready()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("continuation failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("continuation did not complete after readiness event")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerContinuationRequiresReadinessRegistration(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	continuation := &testPipelineContinuation{
		statuses:   []pipeline.StepStatus{pipeline.StepWaiting},
		noRegister: true,
	}
	done := make(chan error, 1)
	if err := scheduler.submitContinuation("missing-registration", continuation, func(err error) {
		done <- err
	}); err != nil {
		t.Fatalf("submit continuation: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected missing readiness registration error")
		}
	case <-time.After(time.Second):
		t.Fatal("continuation did not fail closed")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerContinuationWakesOnPipelineCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	continuation := &cancelAwarePipelineContinuation{
		ctx:        ctx,
		registered: make(chan func(), 1),
	}
	done := make(chan error, 1)
	if err := scheduler.submitContinuation("cancel-aware", continuation, func(err error) {
		done <- err
	}); err != nil {
		t.Fatalf("submit continuation: %v", err)
	}
	select {
	case <-continuation.registered:
	case <-time.After(time.Second):
		t.Fatal("continuation did not enter waiting state")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("continuation failed after cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline cancellation did not wake continuation")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerReentrantReadySubmissionDoesNotBlock(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	completed := make(chan struct{}, 3)
	if err := scheduler.submitRoot("reentrant-parent", func() {
		for i := 0; i < 3; i++ {
			if err := scheduler.submitRoot("reentrant-child", func() {
				completed <- struct{}{}
			}); err != nil {
				t.Errorf("submit child: %v", err)
			}
		}
	}); err != nil {
		t.Fatalf("submit parent: %v", err)
	}
	scheduler.wait()
	if got := len(completed); got != 3 {
		t.Fatalf("completed children: got %d, want 3", got)
	}
}
