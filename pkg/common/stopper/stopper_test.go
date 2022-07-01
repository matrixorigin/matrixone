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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, "timeout", names[0])
}
