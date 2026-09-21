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

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
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

func TestScopeTaskSchedulerBlockingEventDoesNotOccupyEventWorker(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	blockingStarted := make(chan struct{})
	releaseBlocking := make(chan struct{})
	blockingDone := make(chan struct{})
	if err := scheduler.submitBlockingEvent("blocking-reader", func() {
		close(blockingStarted)
		<-releaseBlocking
		close(blockingDone)
	}); err != nil {
		t.Fatalf("submit blocking event: %v", err)
	}
	select {
	case <-blockingStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking worker did not start")
	}
	eventDone := make(chan struct{})
	if err := scheduler.submitEventSource("event-while-reader-blocks", func() {
		close(eventDone)
	}); err != nil {
		t.Fatalf("submit event source: %v", err)
	}
	select {
	case <-eventDone:
	case <-time.After(time.Second):
		t.Fatal("event worker was blocked by reader construction")
	}
	close(releaseBlocking)
	select {
	case <-blockingDone:
	case <-time.After(time.Second):
		t.Fatal("blocking event did not finish")
	}
	scheduler.wait()
}

func TestScopeFutureReadmitsCompletionToReadyQueue(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	operationStarted := make(chan struct{})
	callbackDone := make(chan int, 1)
	future, err := submitBlockingFuture(scheduler, "future-reader", false, func() (int, error) {
		close(operationStarted)
		return 42, nil
	})
	if err != nil {
		t.Fatalf("submit future: %v", err)
	}
	if err := future.OnComplete(func(value int, err error) {
		if err != nil {
			t.Errorf("future callback error: %v", err)
			return
		}
		callbackDone <- value
	}); err != nil {
		t.Fatalf("register future callback: %v", err)
	}
	select {
	case <-operationStarted:
	case <-time.After(time.Second):
		t.Fatal("future operation did not start")
	}
	select {
	case value := <-callbackDone:
		if value != 42 {
			t.Fatalf("future value: got %d, want 42", value)
		}
	case <-time.After(time.Second):
		t.Fatal("future callback did not run")
	}
	value, err := future.Await(context.Background())
	if err != nil {
		t.Fatalf("await future: %v", err)
	}
	if value != 42 {
		t.Fatalf("awaited value: got %d, want 42", value)
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerNestedBlockingFutureDoesNotDeadlock(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	completed := make(chan struct{})

	if err := scheduler.submitBlockingEvent("outer-control", func() {
		future, err := submitBlockingFuture(scheduler, "nested-reader", false, func() (int, error) {
			return 42, nil
		})
		if err != nil {
			t.Errorf("submit nested future: %v", err)
			close(completed)
			return
		}
		value, err := future.Await(context.Background())
		if err != nil {
			t.Errorf("await nested future: %v", err)
		} else if value != 42 {
			t.Errorf("nested future value: got %d, want 42", value)
		}
		close(completed)
	}); err != nil {
		t.Fatalf("submit outer blocking event: %v", err)
	}

	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("nested blocking future deadlocked the blocking lane")
	}
	scheduler.wait()
}

func TestScopeFutureConvertsOperationPanic(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	future, err := submitBlockingFuture(scheduler, "future-panic", false, func() (struct{}, error) {
		panic("future operation failure")
	})
	if err != nil {
		t.Fatalf("submit future: %v", err)
	}
	_, err = future.Await(context.Background())
	if err == nil {
		t.Fatal("expected panic conversion error")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerChannelEventDoesNotOccupyEventWorker(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	messages := make(chan morpc.Message, 1)
	channelDone := make(chan struct{})
	readyDone := make(chan struct{})

	if err := scheduler.submitChannelEvent(
		"channel-event",
		messages,
		func(morpc.Message, bool) { close(channelDone) },
		func(error) { close(channelDone) },
	); err != nil {
		t.Fatalf("submit channel event: %v", err)
	}
	if err := scheduler.submitRoot("ready-while-channel-waits", func() {
		close(readyDone)
	}); err != nil {
		t.Fatalf("submit ready task: %v", err)
	}
	select {
	case <-readyDone:
	case <-time.After(time.Second):
		t.Fatal("ready worker was blocked by channel event")
	}

	messages <- nil
	select {
	case <-channelDone:
	case <-time.After(time.Second):
		t.Fatal("channel event did not run")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerErrorEventDoesNotOccupyEventWorker(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	results := make(chan error, 1)
	readyDone := make(chan struct{})

	if err := scheduler.submitErrorEvent("send-event", results, func(err error) {
		if err != nil {
			results <- err
			return
		}
		close(readyDone)
	}); err != nil {
		t.Fatalf("submit error event: %v", err)
	}
	workerDone := make(chan struct{})
	if err := scheduler.submitRoot("ready-while-send-waits", func() {
		close(workerDone)
	}); err != nil {
		t.Fatalf("submit ready task: %v", err)
	}
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("ready worker was blocked by error event")
	}
	results <- nil
	select {
	case <-readyDone:
	case <-time.After(time.Second):
		t.Fatal("error event did not run")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerErrorEventCancellationReleasesRegistration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := newScopeTaskScheduler(ctx, 1, nil)
	result := make(chan error, 1)
	if err := scheduler.submitErrorEvent("canceled-send", make(chan error), func(err error) {
		result <- err
	}); err != nil {
		t.Fatalf("submit error event: %v", err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled error event was not rejected")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerTeardownChannelSurvivesContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := newScopeTaskScheduler(ctx, 1, nil)
	messages := make(chan morpc.Message, 1)
	done := make(chan struct{})
	if _, err := scheduler.submitChannelEventCancelable(
		"teardown-channel",
		messages,
		func(morpc.Message, bool) { close(done) },
		func(error) { t.Fatal("teardown channel must not be rejected") },
		true,
	); err != nil {
		t.Fatalf("submit teardown channel: %v", err)
	}
	cancel()
	messages <- nil
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("teardown channel was lost after context cancellation")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerTimerDoesNotOccupyEventWorker(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	timerDone := make(chan struct{})
	if err := scheduler.submitTimer("delayed-event", 50*time.Millisecond, func() {
		close(timerDone)
	}); err != nil {
		t.Fatalf("submit timer: %v", err)
	}
	readyDone := make(chan struct{})
	if err := scheduler.submitRoot("ready-while-delayed", func() {
		close(readyDone)
	}); err != nil {
		t.Fatalf("submit ready task: %v", err)
	}
	select {
	case <-readyDone:
	case <-time.After(time.Second):
		t.Fatal("ready worker was blocked by delayed event")
	}
	select {
	case <-timerDone:
	case <-time.After(time.Second):
		t.Fatal("delayed event did not run")
	}
	scheduler.wait()
}

func TestScopeTaskSchedulerTimerCancellationDrainsPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := newScopeTaskScheduler(ctx, 1, nil)
	timerRan := make(chan struct{}, 1)
	if err := scheduler.submitTimer("canceled-event", time.Hour, func() {
		timerRan <- struct{}{}
	}); err != nil {
		t.Fatalf("submit timer: %v", err)
	}
	cancel()
	done := make(chan struct{})
	go func() {
		scheduler.wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not drain a canceled timer")
	}
	select {
	case <-timerRan:
		t.Fatal("canceled timer ran")
	default:
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
