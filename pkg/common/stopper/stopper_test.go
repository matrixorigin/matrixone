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
	"io"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunTaskOnNotRunning(t *testing.T) {
	s := NewStopper(t.Name())
	s.Stop()
	called := false
	require.ErrorIs(t, s.RunTask(func(context.Context) { called = true }), ErrUnavailable)
	require.ErrorIs(t, s.RunNamedRetryTask("retry", 17, 2, func(context.Context, int32) error {
		called = true
		return nil
	}), ErrUnavailable)
	s.Stop()
	require.False(t, called)
}

func TestRunTask(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "running context"
		if canceled {
			name = "canceled context"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := NewStopper(t.Name())
				defer s.Stop()
				if canceled {
					s.cancel()
				}
				called := false
				var err error
				require.NoError(t, s.RunTask(func(ctx context.Context) { called = true; err = ctx.Err() }))
				synctest.Wait()
				require.True(t, called, "accepted ordinary tasks must execute even if cancellation precedes invocation")
				if canceled {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.NoError(t, err)
				}
				require.Empty(t, s.runningTasks())
			})
		})
	}
}

func TestRunTaskWithTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		releaseTask := sync.OnceFunc(func() { close(release) })
		handlerRelease := make(chan struct{})
		releaseHandler := sync.OnceFunc(func() { close(handlerRelease) })
		type diagnostic struct {
			tasks   []string
			elapsed time.Duration
		}
		diagnostics := make(chan diagnostic, 1)
		s := NewStopper(t.Name(), WithStopTimeout(10*time.Millisecond),
			WithTimeoutTaskHandler(func(tasks []string, elapsed time.Duration) {
				diagnostics <- diagnostic{tasks, elapsed}
				<-handlerRelease
			}))
		defer s.Stop()
		defer releaseTask()
		defer releaseHandler()
		require.NoError(t, s.RunNamedTask("timeout", func(context.Context) { close(started); <-release }))
		<-started
		stopped := make(chan struct{}, 2)
		for range 2 {
			go func() { s.Stop(); stopped <- struct{}{} }()
		}
		select {
		case info := <-diagnostics:
			require.Equal(t, []string{"timeout"}, info.tasks)
			require.Equal(t, 10*time.Millisecond, info.elapsed)
		case <-time.After(time.Second):
			t.Fatal("blocked task was not diagnosed")
		}
		releaseTask()
		synctest.Wait()
		require.Empty(t, s.runningTasks())
		select {
		case <-stopped:
			t.Fatal("Stop returned before its diagnostic handler completed")
		default:
		}
		releaseHandler()
		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
		for range 2 {
			select {
			case <-stopped:
			case <-deadline.C:
				t.Fatal("Stop did not join its diagnostic handler")
			}
		}
	})
}

func TestRunNamedRetryTask(t *testing.T) {
	for _, tc := range []struct {
		name      string
		limit     uint32
		successAt int
		attempts  []time.Duration
	}{
		{name: "zero attempts"},
		{name: "large limit with immediate success", limit: ^uint32(0), successAt: 1, attempts: []time.Duration{0}},
		{name: "one failure", limit: 1, attempts: []time.Duration{0}},
		{name: "retry then success", limit: 3, successAt: 2, attempts: []time.Duration{0, time.Second}},
		{name: "exhaustion", limit: 3, attempts: []time.Duration{0, time.Second, 3 * time.Second}},
		{name: "capped backoff", limit: 7, attempts: []time.Duration{0, time.Second, 3 * time.Second, 7 * time.Second, 15 * time.Second, 25 * time.Second, 35 * time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := NewStopper(t.Name())
				defer s.Stop()
				start := time.Now()
				var attempts []time.Duration
				var accounts []int32
				called := make(chan struct{}, max(1, len(tc.attempts)))
				require.NoError(t, s.RunNamedRetryTask("retry", 17, tc.limit, func(ctx context.Context, account int32) error {
					attempts = append(attempts, time.Since(start))
					accounts = append(accounts, account)
					select {
					case called <- struct{}{}:
					case <-ctx.Done():
						return ctx.Err()
					}
					if len(attempts) == tc.successAt {
						return nil
					}
					return io.EOF
				}))
				deadline := time.NewTimer(time.Minute)
				defer deadline.Stop()
				for range tc.attempts {
					select {
					case <-called:
					case <-deadline.C:
						t.Fatal("retry attempt did not execute")
					}
				}
				synctest.Wait()
				require.Equal(t, tc.attempts, attempts)
				for _, account := range accounts {
					require.Equal(t, int32(17), account)
				}
				require.Empty(t, s.runningTasks(), "success or exhaustion must retire without another backoff")
			})
		})
	}
}

func TestRunNamedRetryTaskCancellation(t *testing.T) {
	t.Run("before execution", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s := NewStopper(t.Name())
			defer s.Stop()
			s.cancel()
			called := false
			require.NoError(t, s.RunNamedRetryTask("retry", 17, 2, func(context.Context, int32) error {
				called = true
				return io.EOF
			}))
			synctest.Wait()
			require.False(t, called)
			require.Empty(t, s.runningTasks())
		})
	})
	t.Run("during backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			s := NewStopper(t.Name())
			defer s.Stop()
			called := 0
			first := make(chan struct{}, 2)
			require.NoError(t, s.RunNamedRetryTask("retry", 17, 2, func(context.Context, int32) error {
				called++
				first <- struct{}{}
				return io.EOF
			}))
			<-first
			synctest.Wait()
			stopped := make(chan struct{})
			go func() { s.Stop(); close(stopped) }()
			select {
			case <-stopped:
			case <-time.After(100 * time.Millisecond):
				t.Error("Stop remained blocked behind retry backoff after cancellation")
			}
			<-stopped
			require.Equal(t, 1, called)
			require.Empty(t, s.runningTasks())
		})
	})
}

func TestStopWaitsForTaskDone(t *testing.T) {
	for _, retry := range []bool{false, true} {
		name := "task"
		if retry {
			name = "retry task"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				s := NewStopper(t.Name())
				defer s.Stop()
				// An earlier empty epoch must not let Stop skip a later accepted task.
				for range 3 {
					require.NoError(t, s.RunTask(func(context.Context) {}))
					synctest.Wait()
					require.Empty(t, s.runningTasks())
				}
				release := make(chan struct{})
				releaseTask := sync.OnceFunc(func() { close(release) })
				defer releaseTask()
				started := make(chan struct{})
				canceled := make(chan struct{})
				task := func(ctx context.Context) {
					close(started)
					<-ctx.Done()
					close(canceled)
					<-release
				}
				if retry {
					require.NoError(t, s.RunNamedRetryTask("retry", 17, 2, func(ctx context.Context, _ int32) error { task(ctx); return io.EOF }))
				} else {
					require.NoError(t, s.RunTask(task))
				}
				<-started
				stopped := make(chan struct{}, 2)
				for range 2 {
					go func() { s.Stop(); stopped <- struct{}{} }()
				}
				synctest.Wait()
				select {
				case <-canceled:
				default:
					t.Fatal("Stop did not cancel the accepted task")
				}
				require.ErrorIs(t, s.RunTask(func(context.Context) {}), ErrUnavailable)
				require.ErrorIs(t, s.RunNamedRetryTask("rejected", 17, 2, func(context.Context, int32) error { return nil }), ErrUnavailable)
				select {
				case <-stopped:
					t.Fatal("Stop returned before the accepted task exited")
				default:
				}
				releaseTask()
				deadline := time.NewTimer(time.Second)
				defer deadline.Stop()
				for range 2 {
					select {
					case <-stopped:
					case <-deadline.C:
						t.Fatal("concurrent Stop did not join the released task")
					}
				}
				require.Empty(t, s.runningTasks())
			})
		})
	}
}

func BenchmarkRunTask(b *testing.B) {
	for _, n := range []int{0, 1, 1000, 10000, 100000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			for range b.N {
				runTasks(b, n)
			}
		})
	}
}

func runTasks(b *testing.B, n int) {
	s := NewStopper("BenchmarkRunTask")
	defer s.Stop()
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		assert.NoError(b, s.RunTask(func(ctx context.Context) {
			wg.Done()
			<-ctx.Done()
		}))
	}
	wg.Wait()
}
