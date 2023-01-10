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
)

func TestAcquireWaiter(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	defer w.close()

	assert.Equal(t, 0, len(w.c))
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

	assert.NoError(t, w.add(w1))
	assert.Equal(t, uint64(1), w.waiters.Len())

	w.close()
}

func TestCloseWaiter(t *testing.T) {
	w := acquireWaiter([]byte("w"))
	w1 := acquireWaiter([]byte("w1"))
	w2 := acquireWaiter([]byte("w2"))

	assert.NoError(t, w.add(w1))
	assert.NoError(t, w.add(w2))

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

	assert.NoError(t, w.add(w1))
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

	assert.NoError(t, w.add(w1))

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	assert.Error(t, w1.wait(ctx))
}
