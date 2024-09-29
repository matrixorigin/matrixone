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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunTaskOnNotRunning(t *testing.T) {
	s := NewStopper("TestRunTaskOnNotRunning")
	s.Stop()
	assert.Equal(t, ErrUnavailable, s.RunTask(func(ctx context.Context) {

	}))
}

func TestRunTask(t *testing.T) {
	s := NewStopper("TestRunTask")
	defer s.Stop()

	c := make(chan struct{})
	assert.NoError(t, s.RunTask(func(ctx context.Context) {
		close(c)
	}))
	select {
	case <-c:
		break
	case <-time.After(time.Second):
		assert.Fail(t, "run task timeout")
	}
}

func TestRunTaskWithTimeout(t *testing.T) {
	c := make(chan struct{})
	defer close(c)
	var names []string
	s := NewStopper("TestRunTaskWithTimeout",
		WithStopTimeout(time.Millisecond*10),
		WithTimeoutTaskHandler(func(tasks []string, timeAfterStop time.Duration) {
			select {
			case c <- struct{}{}:
			default:
			}
			names = append(names, tasks...)
		}))

	assert.NoError(t, s.RunNamedTask("timeout", func(ctx context.Context) {
		<-c
	}))

	s.Stop()
	assert.Equal(t, 1, len(names))
	assert.Contains(t, names[0], "timeout")
}

func TestRunNamedRetryTask(t *testing.T) {
	s := NewStopper("TestRunNamedRetryTask")

	called := 0
	err := s.RunNamedRetryTask(
		"retry",
		0,
		2,
		func(ctx context.Context, _ int32) error {
			called++
			if called == 0 {
				return io.EOF
			}
			return nil
		},
	)
	require.NoError(t, err)

	s.Stop()
	require.Equal(t, 1, called)
}

func BenchmarkRunTask1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runTasks(b, 1000)
	}
}

func BenchmarkRunTask10000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runTasks(b, 10000)
	}
}

func BenchmarkRunTask100000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runTasks(b, 100000)
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
