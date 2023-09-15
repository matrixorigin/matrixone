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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireWaiter(t *testing.T) {
	w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")})
	defer w.close()

	assert.Equal(t, 0, len(w.c))
	assert.Equal(t, int32(1), w.refCount.Load())
}

func TestWait(t *testing.T) {
	w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")})
	defer w.close()

	w.setStatus(blocking)
	go func() {
		time.Sleep(time.Millisecond * 10)
		w.notify(notifyValue{})
	}()

	assert.NoError(t, w.wait(context.Background()).err)
}

func TestWaitWithTimeout(t *testing.T) {
	w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")})
	w.setStatus(blocking)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	assert.Error(t, w.wait(ctx).err)
}

func TestWaitAndNotifyConcurrent(t *testing.T) {
	w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")})
	w.setStatus(blocking)
	defer w.close()

	w.beforeSwapStatusAdjustFunc = func() {
		w.setStatus(notified)
		w.c <- notifyValue{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()
	assert.NoError(t, w.wait(ctx).err)
}

func TestWaitMultiTimes(t *testing.T) {
	w := acquireWaiter(pb.WaitTxn{TxnID: []byte("w")})
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	for i := 0; i < 100; i++ {
		w.setStatus(blocking)
		w.notify(notifyValue{})
		assert.NoError(t, w.wait(ctx).err)
		w.resetWait()
	}
}

func TestNotifyAfterCompleted(t *testing.T) {
	w := acquireWaiter(pb.WaitTxn{})
	require.Equal(t, 0, len(w.c))
	defer w.close()
	w.setStatus(completed)
	assert.False(t, w.notify(notifyValue{}))
}

func TestNotifyAfterAlreadyNotified(t *testing.T) {
	w := acquireWaiter(pb.WaitTxn{})
	w.setStatus(blocking)
	defer w.close()
	assert.True(t, w.notify(notifyValue{}))
	assert.NoError(t, w.wait(context.Background()).err)
	assert.False(t, w.notify(notifyValue{}))
}

func TestNotifyWithStatusChanged(t *testing.T) {
	w := acquireWaiter(pb.WaitTxn{})
	defer w.close()

	w.beforeSwapStatusAdjustFunc = func() {
		w.setStatus(completed)
	}
	assert.False(t, w.notify(notifyValue{}))
}
