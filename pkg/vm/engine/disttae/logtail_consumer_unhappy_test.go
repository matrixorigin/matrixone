// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type controlledCancellationContext struct {
	context.Context

	errEntered chan struct{}
	errRelease chan struct{}
	done       chan struct{}
	once       sync.Once

	mu  sync.RWMutex
	err error
}

func newControlledCancellationContext() *controlledCancellationContext {
	return &controlledCancellationContext{
		Context:    context.Background(),
		errEntered: make(chan struct{}),
		errRelease: make(chan struct{}),
		done:       make(chan struct{}),
	}
}

func (c *controlledCancellationContext) Done() <-chan struct{} { return c.done }

func (c *controlledCancellationContext) Err() error {
	c.once.Do(func() {
		close(c.errEntered)
		<-c.errRelease
	})
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

func (c *controlledCancellationContext) cancel() {
	c.mu.Lock()
	c.err = context.Canceled
	c.mu.Unlock()
	close(c.done)
}

func TestLogtailSubscriberWaitReadyHonorsContextCancellation(t *testing.T) {
	subscriber := newLogTailSubscriber()
	ctx := newControlledCancellationContext()

	// The controlled Err call lets this test prove that the waiter checked the
	// context, then released the cond lock in Wait, before cancellation occurs.
	done := make(chan error, 1)
	go func() { done <- subscriber.waitReady(ctx) }()
	<-ctx.errEntered
	close(ctx.errRelease)
	subscriber.mu.Lock()
	ctx.cancel()
	subscriber.mu.Unlock()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		subscriber.setReady() // free the intentionally blocked goroutine before failing.
		<-done
		t.Fatal("waitReady did not wake when its context was cancelled")
	}
}
