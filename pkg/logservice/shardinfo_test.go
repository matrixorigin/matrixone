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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestGetShardInfo(t *testing.T) {
	fn := func(t *testing.T, s *Service) {
		testServiceAddress := s.cfg.LogServiceServiceAddr()
		done := false
		for i := 0; i < 1000; i++ {
			si, ok, err := GetShardInfo("", testServiceAddress, 1)
			if err != nil || !ok {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			done = true
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, uint64(1), si.ReplicaID)
			addr, ok := si.Replicas[si.ReplicaID]
			assert.True(t, ok)
			assert.Equal(t, testServiceAddress, addr)
			break
		}
		if !done {
			t.Fatalf("failed to get shard info")
		}
		_, ok, err := GetShardInfo("", testServiceAddress, 2)
		require.Equal(t, dragonboat.ErrShardNotFound, err)
		assert.False(t, ok)
	}
	runServiceTest(t, false, true, fn)
}

// TestGetShardInfo_LeaderAddressEmptyReturnsNotReady guards the discovery
// semantics of GetShardInfo after Service.getShardInfo was changed to keep
// membership entries whose owning store's gossip metadata has not yet been
// delivered (ServiceAddress == ""). The membership self-check needs those
// entries in the raw response so the ID set is authoritative, but
// GetShardInfo's callers (e.g. connectToLogServiceByReverseProxy) must
// still see "leader unknown" in that state — otherwise they would dial an
// empty leader address.
func TestGetShardInfo_LeaderAddressEmptyReturnsNotReady(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID:  shardID,
			LeaderID: 1,
			Replicas: map[uint64]pb.ReplicaInfo{
				1: {UUID: "uuid-1", ServiceAddress: ""}, // leader's meta not propagated yet
				2: {UUID: "uuid-2", ServiceAddress: "10.0.0.2:1"},
			},
		}, true, nil
	}

	_, ok, err := GetShardInfo("", "doesnt-matter", 3)
	require.NoError(t, err)
	assert.False(t, ok,
		"leader entry without a resolved address must be treated as leader-unknown")
}

// TestGetShardInfo_OmitsUnreachableFollowers makes sure address-less
// follower entries do not leak into the addresses GetShardInfo's callers
// iterate to reach the raft shard.
func TestGetShardInfo_OmitsUnreachableFollowers(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID:  shardID,
			LeaderID: 1,
			Replicas: map[uint64]pb.ReplicaInfo{
				1: {UUID: "uuid-1", ServiceAddress: "10.0.0.1:1"},
				2: {UUID: "uuid-2", ServiceAddress: ""}, // follower meta not propagated
				3: {UUID: "uuid-3", ServiceAddress: "10.0.0.3:1"},
			},
		}, true, nil
	}

	info, ok, err := GetShardInfo("", "doesnt-matter", 3)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "10.0.0.1:1", info.Replicas[1])
	assert.Equal(t, "10.0.0.3:1", info.Replicas[3])
	_, follower2Present := info.Replicas[2]
	assert.False(t, follower2Present,
		"follower entry without a resolved address must be omitted from Replicas")
}

func TestGetShardMembership_IncludesExpiredReplicaAddresses(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.True(t, includeExpiredReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID: shardID,
			Replicas: map[uint64]pb.ReplicaInfo{
				1: {UUID: "uuid-1", ServiceAddress: "10.0.0.1:1"},
				2: {UUID: "uuid-2", ServiceAddress: "10.0.0.2:1"},
			},
		}, true, nil
	}

	members, ok, err := getShardMembership(context.Background(), "", "doesnt-matter", 3)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "10.0.0.1:1", members[1])
	assert.Equal(t, "10.0.0.2:1", members[2])
}

func TestIsExpiredLogStoreInState(t *testing.T) {
	cfg := hakeeper.Config{
		TickPerSecond:   10,
		LogStoreTimeout: 5 * time.Second,
	}
	state := &pb.CheckerState{
		Tick: 100,
		LogState: pb.LogState{
			Stores: map[string]pb.LogStoreInfo{
				"expired": {Tick: 49},
				"live":    {Tick: 50},
			},
		},
	}

	assert.True(t, isExpiredLogStoreInState(cfg, state, "expired"))
	assert.False(t, isExpiredLogStoreInState(cfg, state, "live"))
	assert.False(t, isExpiredLogStoreInState(cfg, state, "unknown"))
	assert.False(t, isExpiredLogStoreInState(cfg, nil, "expired"))
}
