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
	w := acquireWaiter("s1", []byte("w"))
	defer w.close("s1", nil)

	assert.Equal(t, 0, len(w.c))
	assert.Equal(t, int32(1), w.refCount.Load())
	assert.Equal(t, 0, w.waiters.len())
}

func TestAddNewWaiter(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	defer func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		assert.NoError(t, w1.wait(ctx, "s1"))
		w1.close("s1", nil)
	}()

	w.add("s1", w1)
	assert.Equal(t, 1, w.waiters.len())
	assert.Equal(t, int32(2), w1.refCount.Load())
	w.close("s1", nil)
}

func TestCloseWaiter(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	w2 := acquireWaiter("s1", []byte("w2"))

	w.add("s1", w1)
	w.add("s1", w2)

	v := w.close("s1", nil)
	assert.NotNil(t, v)
	assert.Equal(t, 1, v.waiters.len())
	assert.Equal(t, w1, v)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assert.NoError(t, w1.wait(ctx, "s1"))

	v = w1.close("s1", nil)
	assert.NotNil(t, v)
	assert.Equal(t, 0, v.waiters.len())
	assert.Equal(t, w2, v)

	assert.NoError(t, w2.wait(ctx, "s1"))
	assert.Nil(t, w2.close("s1", nil))
}

func TestWait(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	defer w1.close("s1", nil)

	w.add("s1", w1)
	go func() {
		time.Sleep(time.Millisecond * 10)
		w.close("s1", nil)
	}()

	assert.NoError(t, w1.wait(context.Background(), "s1"))
}

func TestWaitWithTimeout(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	defer w.close("s1", nil)
	w1 := acquireWaiter("s1", []byte("w1"))
	defer w1.close("s1", nil)

	w.add("s1", w1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	assert.Error(t, w1.wait(ctx, "s1"))
}

func TestWaitAndNotifyConcurrent(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	defer w.close("s1", nil)

	w.beforeSwapStatusAdjustFunc = func() {
		w.setStatus("s1", notified)
		w.c <- nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()
	assert.NoError(t, w.wait(ctx, "s1"))
}

func TestWaitMultiTimes(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	w2 := acquireWaiter("s1", []byte("w2"))
	defer w2.close("s1", nil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	w.add("s1", w2)
	w.close("s1", nil)
	assert.NoError(t, w2.wait(ctx, "s1"))
	w2.resetWait("s1")

	w1.add("s1", w2)
	w1.close("s1", nil)
	assert.NoError(t, w2.wait(ctx, "s1"))

}

func TestSkipCompletedWaiters(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	defer w1.close("s1", nil)
	w2 := acquireWaiter("s1", []byte("w2"))
	w3 := acquireWaiter("s1", []byte("w3"))
	defer w3.close("s1", nil)

	w.add("s1", w1)
	w.add("s1", w2)
	w.add("s1", w3)

	// make w1 completed
	w1.setStatus("s1", completed)

	v := w.close("s1", nil)
	assert.Equal(t, w2, v)

	v = w2.close("s1", nil)
	assert.Equal(t, w3, v)
}

func TestNotifyAfterCompleted(t *testing.T) {
	w := acquireWaiter("s1", nil)
	require.Equal(t, 0, len(w.c))
	defer w.close("s1", nil)
	w.setStatus("s1", completed)
	assert.False(t, w.notify("s1", nil))
}

func TestNotifyAfterAlreadyNotified(t *testing.T) {
	w := acquireWaiter("s1", nil)
	defer w.close("s1", nil)
	assert.True(t, w.notify("s1", nil))
	assert.NoError(t, w.wait(context.Background(), "s1"))
	assert.False(t, w.notify("s1", nil))
}

func TestNotifyWithStatusChanged(t *testing.T) {
	w := acquireWaiter("s1", nil)
	defer w.close("s1", nil)

	w.beforeSwapStatusAdjustFunc = func() {
		w.setStatus("s1", completed)
	}
	assert.False(t, w.notify("s1", nil))
}
