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

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireWaiter(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	defer w.close("s1", notifyValue{})

	assert.Equal(t, 0, len(w.c))
	assert.Equal(t, int32(1), w.refCount.Load())
	assert.Equal(t, 0, len(w.waiters.(*sliceBasedWaiterQueue).waiters))
}

func TestAddNewWaiter(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	defer func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		assert.NoError(t, w1.wait(ctx, "s1").err)
		w1.close("s1", notifyValue{})
	}()

	w1.casStatus("", ready, blocking)
	w.add("s1", true, w1)
	assert.Equal(t, 1, len(w.waiters.(*sliceBasedWaiterQueue).waiters))
	assert.Equal(t, int32(2), w1.refCount.Load())
	w.close("s1", notifyValue{})
}

func TestCloseWaiter(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))

	w1 := acquireWaiter("s1", []byte("w1"))
	w1.setStatus("", blocking)

	w2 := acquireWaiter("s1", []byte("w2"))
	w2.setStatus("", blocking)

	w.add("s1", true, w1)
	w.add("s1", true, w2)

	v := w.close("s1", notifyValue{})
	assert.NotNil(t, v)
	assert.Equal(t, 1, len(v.waiters.(*sliceBasedWaiterQueue).waiters))
	assert.Equal(t, w1, v)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assert.NoError(t, w1.wait(ctx, "s1").err)

	v = w1.close("s1", notifyValue{})
	assert.NotNil(t, v)
	assert.Equal(t, 0, len(v.waiters.(*sliceBasedWaiterQueue).waiters))
	assert.Equal(t, w2, v)

	assert.NoError(t, w2.wait(ctx, "s1").err)
	assert.Nil(t, w2.close("s1", notifyValue{}))
}

func TestWait(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	w1.setStatus("", blocking)
	defer w1.close("s1", notifyValue{})

	w.add("s1", true, w1)
	go func() {
		time.Sleep(time.Millisecond * 10)
		w.close("s1", notifyValue{})
	}()

	assert.NoError(t, w1.wait(context.Background(), "s1").err)
}

func TestWaitWithTimeout(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	defer w.close("s1", notifyValue{})
	w1 := acquireWaiter("s1", []byte("w1"))
	w1.setStatus("", blocking)
	defer w1.close("s1", notifyValue{})

	w.add("s1", true, w1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	assert.Error(t, w1.wait(ctx, "s1").err)
}

func TestWaitAndNotifyConcurrent(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w.setStatus("", blocking)
	defer w.close("s1", notifyValue{})

	w.beforeSwapStatusAdjustFunc = func() {
		w.setStatus("s1", notified)
		w.c <- notifyValue{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()
	assert.NoError(t, w.wait(ctx, "s1").err)
}

func TestWaitMultiTimes(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	w2 := acquireWaiter("s1", []byte("w2"))
	defer w2.close("s1", notifyValue{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	w2.setStatus("", blocking)
	w.add("s1", true, w2)
	w.close("s1", notifyValue{})
	assert.NoError(t, w2.wait(ctx, "s1").err)
	w2.resetWait("s1")

	w2.setStatus("", blocking)
	w1.add("s1", true, w2)
	w1.close("s1", notifyValue{})
	assert.NoError(t, w2.wait(ctx, "s1").err)

}

func TestSkipCompletedWaiters(t *testing.T) {
	w := acquireWaiter("s1", []byte("w"))
	w1 := acquireWaiter("s1", []byte("w1"))
	defer w1.close("s1", notifyValue{})
	w2 := acquireWaiter("s1", []byte("w2"))
	w3 := acquireWaiter("s1", []byte("w3"))
	defer func() {
		w3.wait(context.Background(), "s1")
		w3.close("s1", notifyValue{})
	}()

	w1.setStatus("", blocking)
	w2.setStatus("", blocking)
	w3.setStatus("", blocking)

	w.add("s1", true, w1)
	w.add("s1", true, w2)
	w.add("s1", true, w3)

	// make w1 completed
	w1.setStatus("s1", completed)

	v := w.close("s1", notifyValue{})
	assert.Equal(t, w2, v)

	w2.wait(context.Background(), "s1")
	v = w2.close("s1", notifyValue{})
	assert.Equal(t, w3, v)
}

func TestNotifyAfterCompleted(t *testing.T) {
	w := acquireWaiter("s1", nil)
	require.Equal(t, 0, len(w.c))
	defer w.close("s1", notifyValue{})
	w.setStatus("s1", completed)
	assert.False(t, w.notify("s1", notifyValue{}))
}

func TestNotifyAfterAlreadyNotified(t *testing.T) {
	w := acquireWaiter("s1", nil)
	w.setStatus("", blocking)
	defer w.close("s1", notifyValue{})
	assert.True(t, w.notify("s1", notifyValue{}))
	assert.NoError(t, w.wait(context.Background(), "s1").err)
	assert.False(t, w.notify("s1", notifyValue{}))
}

func TestNotifyWithStatusChanged(t *testing.T) {
	w := acquireWaiter("s1", nil)
	defer w.close("s1", notifyValue{})

	w.beforeSwapStatusAdjustFunc = func() {
		w.setStatus("s1", completed)
	}
	assert.False(t, w.notify("s1", notifyValue{}))
}

func TestCanGetCommitTSInWaitQueue(t *testing.T) {
	w1 := acquireWaiter("s1", []byte("w1"))
	w2 := acquireWaiter("s1", []byte("w2"))
	w3 := acquireWaiter("s1", []byte("w3"))
	w4 := acquireWaiter("s1", []byte("w4"))
	w5 := acquireWaiter("s5", []byte("w5"))

	w2.setStatus("", blocking)
	w3.setStatus("", blocking)
	w4.setStatus("", blocking)
	w5.setStatus("", blocking)
	w1.add("s1", true, w2, w3, w4, w5)

	// w1 commit at 1
	w1.close("s1", notifyValue{ts: timestamp.Timestamp{PhysicalTime: 1}})

	// w2 abort
	assert.Equal(t, int64(1), w2.wait(context.Background(), "s1").ts.PhysicalTime)
	w2.close("s1", notifyValue{})

	// w3 commit at 3
	assert.Equal(t, int64(1), w3.wait(context.Background(), "s1").ts.PhysicalTime)
	w3.close("s1", notifyValue{ts: timestamp.Timestamp{PhysicalTime: 3}})

	// w4 commit at 2
	assert.Equal(t, int64(3), w4.wait(context.Background(), "s1").ts.PhysicalTime)
	w4.close("s1", notifyValue{ts: timestamp.Timestamp{PhysicalTime: 2}})

	assert.Equal(t, int64(3), w5.wait(context.Background(), "s1").ts.PhysicalTime)
	w5.close("s1", notifyValue{})
}
