// Copyright 2025 Matrix Origin
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

package fileservice

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestObjectStorageSemaphoreSerializes(t *testing.T) {
	start := make(chan struct{}, 2)
	wait := make(chan struct{})
	upstream := &blockingObjectStorage{
		start: start,
		wait:  wait,
	}
	sem := newObjectStorageSemaphore(upstream, 1)

	done := make(chan struct{})
	go func() {
		require.NoError(t, sem.Write(context.Background(), "a", nil, nil, nil))
		close(done)
	}()

	select {
	case <-start:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}

	startSecond := make(chan struct{})
	go func() {
		defer close(startSecond)
		require.NoError(t, sem.Write(context.Background(), "b", nil, nil, nil))
	}()

	select {
	case <-startSecond:
		t.Fatal("second write started before release")
	case <-time.After(50 * time.Millisecond):
	}

	close(wait) // release first
	select {
	case <-startSecond:
	case <-time.After(time.Second):
		t.Fatal("second write not started after release")
	}
	<-done
}

func TestObjectStorageSemaphoreReleasesOnError(t *testing.T) {
	start := make(chan struct{}, 1)
	wait := make(chan struct{})
	upstream := &blockingObjectStorage{
		start: start,
		wait:  wait,
		err:   context.DeadlineExceeded,
	}
	sem := newObjectStorageSemaphore(upstream, 1)

	err := sem.Write(context.Background(), "a", nil, nil, nil)
	require.Error(t, err)
	close(wait)

	// another call should proceed after the failed one
	select {
	case start <- struct{}{}:
	default:
	}
	require.NoError(t, sem.Delete(context.Background(), "x"))
}
