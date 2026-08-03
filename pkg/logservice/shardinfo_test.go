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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		assert.True(t, excludeHardDownReplicaAddresses)
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

func TestGetShardInfo_NoLeaderReturnsNotReady(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		assert.True(t, excludeHardDownReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID:  shardID,
			LeaderID: 0,
			Replicas: map[uint64]pb.ReplicaInfo{
				1: {UUID: "uuid-1", ServiceAddress: "10.0.0.1:1"},
				2: {UUID: "uuid-2", ServiceAddress: "10.0.0.2:1"},
			},
		}, true, nil
	}

	_, ok, err := GetShardInfo("", "doesnt-matter", 3)
	require.NoError(t, err)
	assert.False(t, ok, "LeaderID == 0 means no or unknown leader")
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
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		assert.True(t, excludeHardDownReplicaAddresses)
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

func TestGetShardInfoExcludesHardDownReplicaAddressesByDefault(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		assert.True(t, excludeHardDownReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID:  shardID,
			LeaderID: 1,
			Replicas: map[uint64]pb.ReplicaInfo{
				1: {UUID: "uuid-1", ServiceAddress: "10.0.0.1:1"},
			},
		}, true, nil
	}

	info, ok, err := GetShardInfo("", "doesnt-matter", 3)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "10.0.0.1:1", info.Replicas[1])
}

func TestGetShardInfoDoesNotDropTransientLeaderCandidate(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		assert.True(t, excludeHardDownReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID:  shardID,
			LeaderID: 1,
			Replicas: map[uint64]pb.ReplicaInfo{
				1: {UUID: "suspect-leader", ServiceAddress: "10.0.0.1:1"},
				2: {UUID: "alive-follower", ServiceAddress: "10.0.0.2:1"},
			},
		}, true, nil
	}

	info, ok, err := GetShardInfo("", "doesnt-matter", 3)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(1), info.ReplicaID)
	assert.Equal(t, "10.0.0.1:1", info.Replicas[1])
}

func TestGetShardInfoCanDisableHardDownReplicaAddressFiltering(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		assert.False(t, excludeHardDownReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID:  shardID,
			LeaderID: 1,
			Replicas: map[uint64]pb.ReplicaInfo{
				1: {UUID: "uuid-1", ServiceAddress: "10.0.0.1:1"},
			},
		}, true, nil
	}

	info, ok, err := GetShardInfo("", "doesnt-matter", 3, false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "10.0.0.1:1", info.Replicas[1])
}

// TestGetShardInfo_StaleDeadLeaderFallsBackToSurvivingFollowers reproduces the
// stale-leader window: gossip still reports the old leader id while memberlist
// has already marked that node hard-down. The server-side filter drops the dead
// leader from Replicas, so GetShardInfo must fall back to the surviving
// follower addresses instead of returning ok=false.
func TestGetShardInfo_StaleDeadLeaderFallsBackToSurvivingFollowers(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		assert.True(t, excludeHardDownReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID:  shardID,
			LeaderID: 1, // stale leader — hard-down, filtered out server-side
			Replicas: map[uint64]pb.ReplicaInfo{
				// replica 1 (leader) is absent because the server dropped it
				2: {UUID: "uuid-2", ServiceAddress: "10.0.0.2:1"},
				3: {UUID: "uuid-3", ServiceAddress: "10.0.0.3:1"},
			},
		}, true, nil
	}

	info, ok, err := GetShardInfo("", "doesnt-matter", 3)
	require.NoError(t, err)
	require.True(t, ok, "stale dead leader with live followers must return ok=true")
	assert.NotZero(t, info.ReplicaID)
	assert.Equal(t, "10.0.0.2:1", info.Replicas[2])
	assert.Equal(t, "10.0.0.3:1", info.Replicas[3])
	// The stale leader must not appear.
	_, leaderPresent := info.Replicas[1]
	assert.False(t, leaderPresent)
}

func TestGetShardInfo_StaleExpiredLeaderFallsBackToSurvivingFollowers(t *testing.T) {
	orig := queryShardInfoRawFn
	defer func() { queryShardInfoRawFn = orig }()
	queryShardInfoRawFn = func(
		ctx context.Context,
		sid string,
		address string,
		shardID uint64,
		includeExpiredReplicaAddresses bool,
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.False(t, includeExpiredReplicaAddresses)
		assert.True(t, excludeHardDownReplicaAddresses)
		return pb.ShardInfoQueryResult{
			ShardID:  shardID,
			LeaderID: 1, // stale leader, filtered out by HAKeeper expiry
			Replicas: map[uint64]pb.ReplicaInfo{
				2: {UUID: "uuid-2", ServiceAddress: "10.0.0.2:1"},
			},
		}, true, nil
	}

	info, ok, err := GetShardInfo("", "doesnt-matter", 3)
	require.NoError(t, err)
	require.True(t, ok, "stale expired leader with live followers must return ok=true")
	assert.Equal(t, "10.0.0.2:1", info.Replicas[2])
	_, leaderPresent := info.Replicas[1]
	assert.False(t, leaderPresent)
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
		excludeHardDownReplicaAddresses bool,
	) (pb.ShardInfoQueryResult, bool, error) {
		assert.True(t, includeExpiredReplicaAddresses)
		assert.False(t, excludeHardDownReplicaAddresses)
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

func TestServiceGetShardInfoOmitsExpiredReplicasByDefault(t *testing.T) {
	orig := getCheckerStateForShardInfo
	defer func() { getCheckerStateForShardInfo = orig }()

	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		var info pb.ShardInfoQueryResult
		var ok bool
		for i := 0; i < 1000; i++ {
			info, ok = s.getShardInfo(context.Background(), 1, true, true)
			if ok {
				if _, present := info.Replicas[1]; present {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.True(t, ok)
		_, present := info.Replicas[1]
		require.True(t, present)

		getCheckerStateForShardInfo = func(ctx context.Context, l *store) (*pb.CheckerState, error) {
			return &pb.CheckerState{
				Tick: 100000,
				LogState: pb.LogState{
					Stores: map[string]pb.LogStoreInfo{
						s.ID(): {Tick: 0},
					},
				},
			}, nil
		}

		info, ok = s.getShardInfo(context.Background(), 1, false, true)
		require.True(t, ok)
		assert.Empty(t, info.Replicas, "default discovery should omit known-expired replicas")

		info, ok = s.getShardInfo(context.Background(), 1, true, true)
		require.True(t, ok)
		replica, ok := info.Replicas[1]
		require.True(t, ok)
		assert.Equal(t, s.cfg.LogServiceServiceAddr(), replica.ServiceAddress)
	})
}

func TestIsHardDownReplica(t *testing.T) {
	registry := &testNodeHostRegistry{
		states: map[string]string{
			"alive":   dragonboat.NodeHostStateAlive,
			"suspect": dragonboat.NodeHostStateSuspect,
			"dead":    dragonboat.NodeHostStateDead,
			"left":    dragonboat.NodeHostStateLeft,
			"unknown": dragonboat.NodeHostStateUnknown,
		},
	}

	assert.False(t, isHardDownReplica(registry, "alive"))
	assert.False(t, isHardDownReplica(registry, "suspect"))
	assert.True(t, isHardDownReplica(registry, "dead"))
	assert.True(t, isHardDownReplica(registry, "left"))
	assert.False(t, isHardDownReplica(registry, "unknown"))
	assert.False(t, isHardDownReplica(registry, "missing"))
}

type testNodeHostRegistry struct {
	states map[string]string
}

func (r *testNodeHostRegistry) NumOfShards() int {
	return 0
}

func (r *testNodeHostRegistry) GetMeta(string) ([]byte, bool) {
	return nil, false
}

func (r *testNodeHostRegistry) GetNodeHostState(nhID string) (string, time.Time, bool) {
	state, ok := r.states[nhID]
	return state, time.Time{}, ok
}

func (r *testNodeHostRegistry) GetShardInfo(uint64) (dragonboat.ShardView, bool) {
	return dragonboat.ShardView{}, false
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

func TestGetExpiredLogStoreStateFailsOpenBeforeGetShardInfoDeadline(t *testing.T) {
	orig := getCheckerStateForShardInfo
	defer func() { getCheckerStateForShardInfo = orig }()

	deadlineC := make(chan time.Duration, 1)
	getCheckerStateForShardInfo = func(ctx context.Context, l *store) (*pb.CheckerState, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		deadlineC <- time.Until(deadline)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithTimeoutCause(
		context.Background(),
		time.Second,
		moerr.CauseGetShardInfo,
	)
	defer cancel()

	start := time.Now()
	state := (&Service{store: &store{}}).getExpiredLogStoreState(ctx, false)
	elapsed := time.Since(start)

	require.Nil(t, state)
	assert.Less(t, elapsed, 500*time.Millisecond)
	assert.LessOrEqual(t, <-deadlineC, getShardInfoCheckerStateTimeout)
}
