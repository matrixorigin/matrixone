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
)

func TestScopeTaskSchedulerRunsReadyAndDependencyTasks(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 2, nil)
	readyDone := make(chan struct{})
	dependencyDone := make(chan struct{})
	submitErr := make(chan error, 1)

	if err := scheduler.submitRoot("test-root", func() {
		close(readyDone)
		submitErr <- scheduler.submitDependency("test-dependency", func() {
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

func TestScopeTaskSchedulerDependencyDoesNotDeadlockSingleWorker(t *testing.T) {
	scheduler := newScopeTaskScheduler(context.Background(), 1, nil)
	dependencyDone := make(chan struct{})
	submitErr := make(chan error, 1)

	if err := scheduler.submitRoot("blocking-parent", func() {
		if err := scheduler.submitDependency("blocking-child", func() {
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
	if err := scheduler.submitDependency("late", func() {}); !errors.Is(err, errScopeTaskSchedulerClosed) {
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
