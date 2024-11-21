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

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireWaiter(t *testing.T) {
	reuse.RunReuseTests(func() {
		w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")}, "", nil)
		defer w.close("", nil)

		assert.Equal(t, 0, len(w.c))
		assert.Equal(t, int32(1), w.refCount.Load())
	})
}

func TestWait(t *testing.T) {
	reuse.RunReuseTests(func() {
		w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")}, "", nil)
		defer w.close("", nil)

		w.setStatus(blocking)
		go func() {
			time.Sleep(time.Millisecond * 10)
			w.notify(notifyValue{}, getLogger(""))
		}()

		assert.NoError(t, w.wait(context.Background(), getLogger("")).err)
	})
}

func TestWaitWithTimeout(t *testing.T) {
	reuse.RunReuseTests(func() {
		w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")}, "", nil)
		defer w.close("", nil)
		w.setStatus(blocking)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()
		assert.Error(t, w.wait(ctx, getLogger("")).err)
	})
}

func TestWaitAndNotifyConcurrent(t *testing.T) {
	reuse.RunReuseTests(func() {
		w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")}, "", nil)
		w.setStatus(blocking)
		defer w.close("", nil)

		w.beforeSwapStatusAdjustFunc = func() {
			w.setStatus(notified)
			w.c <- notifyValue{}
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
		defer cancel()
		assert.NoError(t, w.wait(ctx, getLogger("")).err)
	})

}

func TestWaitMultiTimes(t *testing.T) {
	reuse.RunReuseTests(func() {
		w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")}, "", nil)
		defer w.close("", nil)

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
		defer cancel()

		for i := 0; i < 100; i++ {
			w.setStatus(blocking)
			w.notify(notifyValue{}, getLogger(""))
			assert.NoError(t, w.wait(ctx, getLogger("")).err)
			w.resetWait(getLogger(""))
		}
	})
}

func TestNotifyAfterCompleted(t *testing.T) {
	reuse.RunReuseTests(func() {
		w := acquireWaiter(pb.WaitTxn{}, "", nil)
		require.Equal(t, 0, len(w.c))
		defer w.close("", nil)
		w.setStatus(completed)
		assert.False(t, w.notify(notifyValue{}, getLogger("")))
	})
}

func TestNotifyAfterAlreadyNotified(t *testing.T) {
	reuse.RunReuseTests(func() {
		w := acquireWaiter(pb.WaitTxn{}, "", nil)
		w.setStatus(blocking)
		defer w.close("", nil)
		assert.True(t, w.notify(notifyValue{}, getLogger("")))
		assert.NoError(t, w.wait(context.Background(), getLogger("")).err)
		assert.False(t, w.notify(notifyValue{}, getLogger("")))
	})
}

func TestNotifyWithStatusChanged(t *testing.T) {
	reuse.RunReuseTests(func() {
		w := acquireWaiter(pb.WaitTxn{}, "", nil)
		defer w.close("", nil)

		w.beforeSwapStatusAdjustFunc = func() {
			w.setStatus(completed)
		}
		assert.False(t, w.notify(notifyValue{}, getLogger("")))
	})
}
