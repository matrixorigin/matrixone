// Copyright 2021 Matrix Origin
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

package sm

import (
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoop1(t *testing.T) {
	defer testutils.AfterTest(t)()
	q1 := make(chan any, 100)
	fn := func(batch []any, q chan any) {
		for _, item := range batch {
			t.Logf("loop1 %d", item.(int))
		}
	}
	loop := NewLoop(q1, nil, fn, 100)
	loop.Start()
	for i := 0; i < 10; i++ {
		q1 <- i
	}
	loop.Stop()
}

func TestSafeQueueStopIsIdempotent(t *testing.T) {
	queue := NewSafeQueue(1, 1, func(...any) {})
	queue.Start()

	require.NotPanics(t, func() {
		queue.Stop()
		queue.Stop()
	})

	_, err := queue.Enqueue("after stop")
	require.ErrorIs(t, err, ErrClose)
}

func TestSafeQueueRejectsEnqueueAfterStopBegins(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	queue := NewSafeQueue(1, 1, func(...any) {
		close(entered)
		<-release
	})
	queue.Start()
	_, err := queue.Enqueue("in flight")
	require.NoError(t, err)
	<-entered

	stopped := make(chan struct{})
	go func() {
		queue.Stop()
		close(stopped)
	}()
	require.Eventually(t, func() bool {
		return queue.state.Load() >= ReceiverStopped
	}, 10*time.Second, time.Millisecond)

	_, err = queue.Enqueue("after stop begins")
	require.ErrorIs(t, err, ErrClose)
	close(release)
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("queue stop did not finish")
	}
}

func TestSafeQueueWithoutHandlerStops(t *testing.T) {
	queue := NewSafeQueue(1, 1, nil)
	queue.Start()
	_, err := queue.Enqueue("discarded")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		queue.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("queue with nil handler did not stop")
	}
}

func TestNewNonBlockingQueue(t *testing.T) {
	wait := sync.WaitGroup{}
	wait.Add(1)
	defer func() {
		testutils.AfterTest(t)
	}()

	queueSize := 10
	batchSize := 0
	queue := NewNonBlockingQueue(queueSize, batchSize, func(items ...any) {
		// blocking handler
		wait.Wait()
	})

	queue.Start()

	for i := 0; i < queueSize+1; i++ {
		item, err := queue.Enqueue(i)
		assert.NotNil(t, item)
		assert.Nil(t, err)
		time.Sleep(time.Millisecond * 10)
	}

	item, err := queue.Enqueue(11)
	assert.NotNil(t, item)
	assert.Equal(t, err, ErrFull)

	wait.Done()
	time.Sleep(time.Millisecond * 100)

}
