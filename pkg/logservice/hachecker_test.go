// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"context"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/leaktest"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestIDAllocatorDefaultState(t *testing.T) {
	alloc := newIDAllocator()
	assert.Equal(t, uint64(0), alloc.Capacity())
	v, ok := alloc.Next()
	assert.False(t, ok)
	assert.Equal(t, uint64(0), v)
}

func TestIDAllocatorCapacity(t *testing.T) {
	tests := []struct {
		next     uint64
		last     uint64
		capacity uint64
	}{
		{1, 1, 1},
		{2, 1, 0},
		{1, 2, 2},
		{100, 200, 101},
	}

	for _, tt := range tests {
		alloc := idAllocator{nextID: tt.next, lastID: tt.last}
		assert.Equal(t, tt.capacity, alloc.Capacity())
	}
}

func TestIDAllocatorSet(t *testing.T) {
	alloc := idAllocator{nextID: 100, lastID: 200}
	alloc.Set(200, 300)
	assert.Equal(t, idAllocator{nextID: 200, lastID: 300}, alloc)
}

func TestIDAllocatorNext(t *testing.T) {
	tests := []struct {
		next     uint64
		last     uint64
		capacity uint64
	}{
		{1, 1, 1},
		{2, 1, 0},
		{1, 2, 2},
		{100, 200, 101},
	}

	for _, tt := range tests {
		expected := tt.next
		alloc := idAllocator{nextID: tt.next, lastID: tt.last}
		for {
			hasID := alloc.Capacity() != 0
			v, ok := alloc.Next()
			assert.Equal(t, hasID, ok)
			if hasID {
				assert.Equal(t, expected, v)
				expected++
			} else {
				assert.Equal(t, uint64(0), v)
				break
			}
		}
	}
}

func TestHandleBootstrapFailure(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to trigger panic")
		}
	}()
	s := store{}
	s.handleBootstrapFailure()
}

func runHAKeeperStoreTest(t *testing.T, fn func(*testing.T, *store)) {
	defer leaktest.AfterTest(t)()
	cfg := getStoreTestConfig()
	defer vfs.ReportLeakedFD(cfg.FS, t)
	store, err := getTestStore(cfg)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, store.close())
	}()
	peers := make(map[uint64]dragonboat.Target)
	peers[1] = store.id()
	assert.NoError(t, store.startHAKeeperReplica(1, peers, false))
	fn(t, store)
}

func TestGetCheckerState(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		state, err := store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperCreated, state.State)
	}
	runHAKeeperStoreTest(t, fn)
}

func TestSetInitialClusterInfo(t *testing.T) {
	fn := func(t *testing.T, store *store) {
		state, err := store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperCreated, state.State)
		require.NoError(t, store.setInitialClusterInfo(1, 1, 1))
		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapping, state.State)
	}
	runHAKeeperStoreTest(t, fn)
}

func TestFailedBootstrap(t *testing.T) {
	testBootstrap(t, true)
}

// FIXME: re-enable this test
/*
func TestBootstrap(t *testing.T) {
	testBootstrap(t, false)
}*/

func testBootstrap(t *testing.T, fail bool) {
	fn := func(t *testing.T, store *store) {
		state, err := store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperCreated, state.State)
		require.NoError(t, store.setInitialClusterInfo(1, 1, 1))
		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapping, state.State)
		m := store.getHeartbeatMessage()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		assert.NoError(t, store.addLogStoreHeartbeat(ctx, m))

		dnMsg := pb.DNStoreHeartbeat{
			UUID:   store.id(),
			Shards: make([]pb.DNShardInfo, 0),
		}
		dnMsg.Shards = append(dnMsg.Shards, pb.DNShardInfo{ShardID: 2, ReplicaID: 3})
		assert.NoError(t, store.addDNStoreHeartbeat(ctx, dnMsg))

		_, term, err := store.isLeaderHAKeeper()
		require.NoError(t, err)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		store.bootstrap(term, state)

		state, err = store.getCheckerState()
		require.NoError(t, err)
		assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived, state.State)
		assert.Equal(t, uint64(checkBootstrapInterval), store.bootstrapCheckInterval)
		assert.NotNil(t, store.bootstrapMgr)
		assert.False(t, store.bootstrapMgr.CheckBootstrap(state.LogState))

		if fail {
			// keep checking, bootstrap will eventually be set as failed
			for i := 0; i <= checkBootstrapInterval; i++ {
				store.checkBootstrap(state)
			}

			state, err = store.getCheckerState()
			require.NoError(t, err)
			assert.Equal(t, pb.HAKeeperBootstrapFailed, state.State)
		} else {
			// FIXME: waiting for the bootstrap code to be fixed
			cb, err := store.getCommandBatch(ctx, store.id())
			require.NoError(t, err)
			require.Equal(t, 1, len(cb.Commands))
			service := &Service{store: store}
			service.handleStartReplica(cb.Commands[0])

			m := store.getHeartbeatMessage()
			assert.NoError(t, store.addLogStoreHeartbeat(ctx, m))

			state, err = store.getCheckerState()
			require.NoError(t, err)
			store.checkBootstrap(state)

			state, err = store.getCheckerState()
			require.NoError(t, err)
			assert.Equal(t, pb.HAKeeperRunning, state.State)
		}
	}
	runHAKeeperStoreTest(t, fn)
}
