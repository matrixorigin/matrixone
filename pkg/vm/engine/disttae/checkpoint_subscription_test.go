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

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

// Done is consulted at gate admission and then at the real partition lock.
type checkpointArrivalContext struct {
	context.Context
	arrivals chan struct{}
}

func (c *checkpointArrivalContext) Done() <-chan struct{} {
	select {
	case c.arrivals <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

type checkpointLoadResult struct {
	state SubscribeState
	err   error
}

type checkpointFixture struct {
	client *PushClient
	part   *logtailreplay.Partition
	ctx    *checkpointArrivalContext
	cancel context.CancelFunc
	result chan checkpointLoadResult
	joined bool
	locked bool
}

func newCheckpointFixture(t *testing.T, location string) *checkpointFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	f := &checkpointFixture{
		part:   logtailreplay.NewPartition("", nil, 0, 10, 100, nil),
		ctx:    &checkpointArrivalContext{Context: ctx, arrivals: make(chan struct{}, 8)},
		cancel: cancel,
		result: make(chan checkpointLoadResult, 1),
	}
	require.NoError(t, f.part.Lock(ctx))
	f.locked = true
	t.Cleanup(f.release)
	state, publish := f.part.MutateState()
	state.AppendCheckpoint(location, f.part)
	publish()
	e := &Engine{partitions: map[[2]uint64]*logtailreplay.Partition{{10, 100}: f.part}}
	f.client = &PushClient{eng: e, subscribed: subscribedTable{m: map[uint64]*subEntry{
		100: {dbID: 10, state: SubRspReceived},
		101: {dbID: 10, state: Subscribed},
	}}}
	go func() {
		state, err := f.client.loadAndConsumeLatestCkp(f.ctx, 0, 100, "a", 10, "db")
		f.result <- checkpointLoadResult{state, err}
	}()
	t.Cleanup(func() {
		cancel()
		if !f.joined {
			f.join(t)
		}
	})
	f.arrived(t) // gate
	f.arrived(t) // checkpoint partition lock
	return f
}

func (f *checkpointFixture) arrived(t *testing.T) {
	t.Helper()
	select {
	case <-f.ctx.arrivals:
	case <-f.ctx.Context.Done():
		t.Fatal(f.ctx.Err())
	}
}

func (f *checkpointFixture) release() {
	if f.locked {
		f.locked = false
		f.part.Unlock()
	}
}

func (f *checkpointFixture) join(t *testing.T) checkpointLoadResult {
	t.Helper()
	select {
	case result := <-f.result:
		f.joined = true
		return result
	case <-time.After(10 * time.Second):
		t.Fatal("checkpoint loader did not terminate")
		return checkpointLoadResult{}
	}
}

func TestCheckpointSubscriptionIsolation(t *testing.T) {
	f := newCheckpointFixture(t, "1")
	require.True(t, f.client.subscribed.isSubscribed(10, 101))
	f.client.subscribed.rw.RLock()
	state := f.client.subscribed.m[100].state
	f.client.subscribed.rw.RUnlock()
	require.Equal(t, SubRspReceived, state)
	to := timestamp.Timestamp{PhysicalTime: 10}
	f.client.subscribed.setTablePendingUpdate(10, 100, to)
	f.release()
	result := f.join(t)
	require.NoError(t, result.err)
	require.Equal(t, Subscribed, result.state)
	require.True(t, f.client.subscribed.hasPendingUpdate(10, 100))
	f.client.subscribed.clearTablePendingUpdate(10, 100, timestamp.Timestamp{PhysicalTime: 9})
	require.True(t, f.client.subscribed.hasPendingUpdate(10, 100))
	f.client.subscribed.clearTablePendingUpdate(10, 100, to)
	require.False(t, f.client.subscribed.hasPendingUpdate(10, 100))
}

func TestCheckpointSubscriptionReplacement(t *testing.T) {
	for _, location := range []string{"1", "invalid;invalid"} {
		t.Run(location, func(t *testing.T) {
			f := newCheckpointFixture(t, location)
			replacement := logtailreplay.NewPartition("", nil, 0, 10, 100, nil)
			require.NoError(t, replacement.Lock(t.Context()))
			locked := true
			t.Cleanup(func() {
				if locked {
					replacement.Unlock()
				}
			})
			state, publish := replacement.MutateState()
			state.AppendCheckpoint("1", replacement)
			publish()
			f.client.subscribed.rw.Lock()
			f.client.subscribed.m = map[uint64]*subEntry{100: {dbID: 10, state: SubRspReceived}}
			f.client.eng.Lock()
			f.client.eng.partitions = map[[2]uint64]*logtailreplay.Partition{{10, 100}: replacement}
			f.client.eng.Unlock()
			f.client.subscribed.rw.Unlock()
			f.release()
			f.arrived(t) // the replacement checkpoint must also be fenced
			f.client.subscribed.rw.RLock()
			subState := f.client.subscribed.m[100].state
			f.client.subscribed.rw.RUnlock()
			require.Equal(t, SubRspReceived, subState)
			locked = false
			replacement.Unlock()
			result := f.join(t)
			require.NoError(t, result.err)
			require.Equal(t, Subscribed, result.state)
		})
	}
}

func TestCheckpointSubscriptionTerminalPaths(t *testing.T) {
	for _, terminal := range []string{"error", "cancel", "cancel-replaced", "unsubscribe", "cleared", "database-mismatch"} {
		t.Run(terminal, func(t *testing.T) {
			location := "1"
			if terminal == "error" {
				location = "invalid;invalid"
			}
			f := newCheckpointFixture(t, location)
			expected := InvalidSubState
			switch terminal {
			case "cancel":
				f.cancel()
			case "cancel-replaced":
				f.client.SetSubscribeState(10, 100, SubRspReceived)
				f.cancel()
			case "unsubscribe":
				f.client.SetSubscribeState(10, 100, Unsubscribing)
				expected = Unsubscribing
			case "cleared":
				f.client.subscriber = &logTailSubscriber{}
				f.client.subscriber.mu.cond = sync.NewCond(&f.client.subscriber.mu)
				f.client.subscriber.setReady()
				f.client.subscriber.sendSubscribe = func(_ context.Context, id api.TableID) error {
					require.Equal(t, api.TableID{DbId: 10, TbId: 100}, id)
					return nil
				}
				f.client.subscribed.clearTable(10, 100)
				expected = Subscribing
			case "database-mismatch":
				f.client.SetSubscribeState(11, 100, SubRspReceived)
			}
			if terminal != "cancel" && terminal != "cancel-replaced" {
				f.release()
			}
			result := f.join(t)
			require.Equal(t, expected, result.state)
			if terminal == "error" || terminal == "cancel" || terminal == "cancel-replaced" || terminal == "database-mismatch" {
				require.Error(t, result.err)
				if terminal == "cancel" || terminal == "cancel-replaced" {
					require.ErrorIs(t, result.err, context.Canceled)
				}
				f.client.subscribed.rw.RLock()
				state := f.client.subscribed.m[100].state
				f.client.subscribed.rw.RUnlock()
				require.Equal(t, SubRspReceived, state)
			} else {
				require.NoError(t, result.err)
			}
			require.Empty(t, f.client.checkpointReplay)
		})
	}
}

func TestCheckpointSubscriptionReplayCapacity(t *testing.T) {
	f := newCheckpointFixture(t, "1")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	queued := &checkpointArrivalContext{Context: ctx, arrivals: make(chan struct{}, 8)}
	result := make(chan checkpointLoadResult, 1)
	go func() {
		state, err := f.client.loadAndConsumeLatestCkp(queued, 0, 102, "b", 10, "db")
		result <- checkpointLoadResult{state, err}
	}()
	defer func() {
		cancel()
		select {
		case outcome := <-result:
			require.ErrorIs(t, outcome.err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("queued replay did not cancel")
		}
	}()
	select {
	case <-queued.arrivals:
	case <-f.ctx.Context.Done():
		t.Fatal(f.ctx.Err())
	}
	require.Len(t, f.client.checkpointReplay, 1)
	f.client.eng.Lock()
	_, pinned := f.client.eng.partitions[[2]uint64{10, 102}]
	f.client.eng.Unlock()
	require.False(t, pinned)
	require.True(t, f.client.subscribed.isSubscribed(10, 101))
}
