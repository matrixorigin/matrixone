// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireWaiter(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	defer w.close()

	assert.Equal(t, 0, len(w.c))
	assert.Equal(t, int32(1), w.refCount.Load())
	assert.Equal(t, uint64(0), w.waiters.Len())
}

func TestAddNewWaiter(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	w1 := acquireWaiter([]byte("w1"))
	defer func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		assert.NoError(t, w1.wait(ctx))
		w1.close()
	}()

	w.add(w1)
	assert.Equal(t, uint64(1), w.waiters.Len())
	assert.Equal(t, int32(2), w1.refCount.Load())
	w.close()
}

func TestCloseWaiter(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	w1 := acquireWaiter([]byte("w1"))
	w2 := acquireWaiter([]byte("w2"))

	w.add(w1)
	w.add(w2)

	v := w.close()
	assert.NotNil(t, v)
	assert.Equal(t, uint64(1), v.waiters.Len())
	assert.Equal(t, w1, v)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assert.NoError(t, w1.wait(ctx))

	v = w1.close()
	assert.NotNil(t, v)
	assert.Equal(t, uint64(0), v.waiters.Len())
	assert.Equal(t, w2, v)

	assert.NoError(t, w2.wait(ctx))
	assert.Nil(t, w2.close())
}

func TestWait(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	w1 := acquireWaiter([]byte("w1"))
	defer w1.close()

	w.add(w1)
	go func() {
		time.Sleep(time.Millisecond * 10)
		w.close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assert.NoError(t, w1.wait(ctx))
}

func TestWaitWithTimeout(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	defer w.close()
	w1 := acquireWaiter([]byte("w1"))
	defer w1.close()

	w.add(w1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	assert.Error(t, w1.wait(ctx))
}

func TestWaitAndNotifyConcurrent(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	defer w.close()

	w.beforeSwapStatusAdjustFunc = func() {
		w.status.Store(notifyAddedStatus)
		w.c <- nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()
	assert.NoError(t, w.wait(ctx))
}

func TestWaitMultiTimes(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	w1 := acquireWaiter([]byte("w1"))
	w2 := acquireWaiter([]byte("w2"))
	defer w2.close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	w.add(w2)
	w.close()
	assert.NoError(t, w2.wait(ctx))
	w2.resetWait()

	w1.add(w2)
	w1.close()
	assert.NoError(t, w2.wait(ctx))

}

func TestSkipCompletedWaiters(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	w1 := acquireWaiter([]byte("w1"))
	defer w1.close()
	w2 := acquireWaiter([]byte("w2"))
	w3 := acquireWaiter([]byte("w3"))
	defer w3.close()

	w.add(w1)
	w.add(w2)
	w.add(w3)

	// make w1 completed
	w1.setCompleted()

	v := w.close()
	assert.Equal(t, w2, v)

	v = w2.close()
	assert.Equal(t, w3, v)
}

func TestNotifyAfterCompleted(t *testing.T) {
	w := acquireWaiter(nil)
	require.Equal(t, 0, len(w.c))
	defer w.close()
	w.setCompleted()
	assert.False(t, w.notify(nil))
}

func TestNotifyAfterAlreadyNotified(t *testing.T) {
	w := acquireWaiter(nil)
	defer w.close()
	assert.True(t, w.notify(nil))
	<-w.c
	defer func() {
		if err := recover(); err != nil {
			return
		}
		t.Fail()
	}()
	w.notify(nil)
}

func TestNotifyWithStatusChanged(t *testing.T) {
	w := acquireWaiter(nil)
	defer w.close()

	w.beforeSwapStatusAdjustFunc = func() {
		w.status.Store(waitCompletedStatus)
	}
	assert.False(t, w.notify(nil))
}
