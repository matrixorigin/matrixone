// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package morpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/stretchr/testify/require"
)

// Cancellation happens in the writer after queue admission, not before Send.
// A later cleanup/control request uses a separate live context on the same
// stream. Neither that request nor an unrelated stream may acquire a gap.
func TestQueuedStreamCancellationPreservesWireSequence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queuedCtx, cancelQueued := context.WithCancel(ctx)
	defer cancelQueued()
	type frame struct {
		id       uint64
		sequence uint32
	}
	wire := make(chan frame, 4)
	var attempts atomic.Int32
	testBackendSend(t,
		func(_ goetty.IOSession, value interface{}, _ uint64) error {
			message := value.(RPCMessage)
			select {
			case wire <- frame{message.Message.GetID(), message.streamSequence}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		func(b *remoteBackend) {
			first, err := b.NewStream(false)
			require.NoError(t, err)
			defer func() { require.NoError(t, first.Close(false)) }()
			other, err := b.NewStream(false)
			require.NoError(t, err)
			defer func() { require.NoError(t, other.Close(false)) }()

			require.NoError(t, first.Send(ctx, newTestMessage(first.ID())))
			require.ErrorIs(t, first.Send(queuedCtx, newTestMessage(first.ID())), context.Canceled)
			require.NoError(t, first.Send(ctx, newTestMessage(first.ID())))
			require.NoError(t, first.Close(false))
			require.NoError(t, other.Send(ctx, newTestMessage(other.ID())))
			next, err := b.NewStream(false)
			require.NoError(t, err)
			defer func() { require.NoError(t, next.Close(false)) }()
			require.NoError(t, next.Send(ctx, newTestMessage(next.ID())))
			for _, want := range []frame{{first.ID(), 1}, {first.ID(), 2}, {other.ID(), 1}, {next.ID(), 1}} {
				select {
				case got := <-wire:
					require.Equal(t, want, got)
				case <-ctx.Done():
					t.Fatal("writer did not deliver the next live stream request")
				}
			}
		},
		WithBackendBatchSendSize(1),
		WithBackendFilter(func(Message, string) bool {
			if attempts.Add(1) == 2 {
				cancelQueued()
			}
			return true
		}),
	)
}
