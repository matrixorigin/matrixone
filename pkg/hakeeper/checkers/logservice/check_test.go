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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "info",
		Format: "console",
	})

	runtime.SetupServiceBasedRuntime("", runtime.NewRuntime(metadata.ServiceType_LOG, "test", logutil.GetGlobalLogger()))
	m.Run()
}

func TestCheckerUsesReportingVoterToRemoveExpiredPeers(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	const tick = uint64(10000)
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: make(map[string]pb.LogStoreInfo)}
	for id, uuid := range shard.Replicas {
		store := pb.LogStoreInfo{Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}}
		if uuid == "a" || uuid == "b" {
			store.Tick = 0
		}
		state.Stores[uuid] = store
	}
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 5}}}
	fields := hakeeper.NewCheckerCommonFields("", cfg, util.NewTestIDAllocator(10), cluster, pb.TaskTableUser{}, tick)
	checker := NewLogServiceChecker(fields, state, pb.TNState{}, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, pb.Locality{}, false)
	ops := checker.Check()
	require.Len(t, ops, 1)
	first, ok := ops[0].OpSteps()[0].(operator.RemoveLogService)
	require.True(t, ok)
	require.Equal(t, "a", first.UUID)
	require.Equal(t, "c", first.Target)
	delete(shard.Replicas, first.ReplicaID)
	shard.Epoch++
	state.Shards[1] = shard
	ops = NewLogServiceChecker(fields, state, pb.TNState{}, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, pb.Locality{}, false).Check()
	require.Len(t, ops, 1)
	second, ok := ops[0].OpSteps()[0].(operator.RemoveLogService)
	require.True(t, ok)
	require.Equal(t, "b", second.UUID)
	require.Equal(t, shard.Epoch, second.Epoch)
}

func TestCheckerPrefersSafeSurplusAndStartsWhenQuorumIsMissing(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	const tick = uint64(10000)
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, LeaderID: 3, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: make(map[string]pb.LogStoreInfo)}
	for id, uuid := range shard.Replicas {
		store := pb.LogStoreInfo{Tick: tick}
		if id <= 3 {
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}
		}
		state.Stores[uuid] = store
	}
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}}
	fields := hakeeper.NewCheckerCommonFields("", cfg, util.NewTestIDAllocator(10), cluster, pb.TaskTableUser{}, tick)
	check := func(executing operator.ExecutingReplicas) []*operator.Operator {
		return NewLogServiceChecker(fields, state, pb.TNState{}, executing, operator.ExecutingReplicas{}, 0, pb.Locality{}, false).Check()
	}
	ops := check(operator.ExecutingReplicas{})
	require.Len(t, ops, 2)
	removed := make(map[string]bool)
	started := make(map[string]bool)
	for _, op := range ops {
		switch step := op.OpSteps()[0].(type) {
		case operator.RemoveLogService:
			require.Equal(t, "a", step.Target)
			removed[step.UUID] = true
		case operator.StartLogService:
			started[step.UUID] = true
		default:
			t.Fatalf("unexpected step %T", step)
		}
	}
	require.Equal(t, map[string]bool{"d": true}, removed)
	require.Equal(t, map[string]bool{"e": true}, started, "an independent start is not blocked by a possible later removal")
	ops = check(operator.ExecutingReplicas{Starting: map[uint64][]uint64{1: {5}}})
	removed = make(map[string]bool)
	for _, op := range ops {
		if step, ok := op.OpSteps()[0].(operator.RemoveLogService); ok {
			removed[step.UUID] = true
		}
	}
	require.Equal(t, map[string]bool{"d": true}, removed, "an independent safe removal still proceeds")

	// A previously issued removal may still commit using a different live
	// majority. Do not race it with a start of the same identity.
	c := state.Stores["c"]
	c.Replicas = nil
	state.Stores["c"] = c
	ops = check(operator.ExecutingReplicas{Removing: map[uint64][]uint64{1: {4}}})
	var startedD bool
	for _, op := range ops {
		switch step := op.OpSteps()[0].(type) {
		case operator.StartLogService:
			startedD = startedD || step.UUID == "d"
		case operator.RemoveLogService:
			t.Fatalf("removed %s without current quorum", step.UUID)
		}
	}
	require.False(t, startedD, "wait for the in-flight removal before restarting the same identity")
	pending := operator.NewOperator("", 1, 1, operator.StartLogService{Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4}})
	require.NotContains(t, PendingOperators(fields, state, operator.ExecutingReplicas{Removing: map[uint64][]uint64{1: {4}}}, operator.ExecutingReplicas{}, 0, pb.Locality{}, []*operator.Operator{pending}), pending)
	c.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}
	state.Stores["c"] = c
	delete(shard.Replicas, 4)
	shard.Epoch++
	state.Shards[1] = shard
	ops = check(operator.ExecutingReplicas{})
	require.Len(t, ops, 1)
	second, ok := ops[0].OpSteps()[0].(operator.RemoveLogService)
	require.True(t, ok)
	require.Equal(t, "e", second.UUID)
	require.Equal(t, shard.Epoch, second.Epoch)
}

func TestCheckerSerializesMembershipPerShard(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	state := pb.LogState{Shards: make(map[uint64]pb.LogShardInfo), Stores: make(map[string]pb.LogStoreInfo)}
	cluster := pb.ClusterInfo{}
	for _, shardID := range []uint64{1, 2} {
		shard := pb.LogShardInfo{ShardID: shardID, Epoch: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}}
		state.Shards[shardID] = shard
		cluster.LogShards = append(cluster.LogShards, metadata.LogShardRecord{ShardID: shardID, NumberOfReplicas: 5})
		for id, uuid := range shard.Replicas {
			store := state.Stores[uuid]
			store.Tick = 1
			store.Replicas = append(store.Replicas, pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: id})
			state.Stores[uuid] = store
		}
	}
	state.Stores["d"] = pb.LogStoreInfo{Tick: 1}
	state.Stores["e"] = pb.LogStoreInfo{Tick: 1}
	state.Stores["f"] = pb.LogStoreInfo{Tick: 1}
	alloc := util.NewTestIDAllocator(10)
	fields := hakeeper.NewCheckerCommonFields("", cfg, alloc, cluster, pb.TaskTableUser{}, 1)
	check := func(voting, nonVoting operator.ExecutingReplicas) []*operator.Operator {
		return NewLogServiceChecker(fields, state, pb.TNState{}, voting, nonVoting, 1, pb.Locality{}, false).Check()
	}
	ops := check(operator.ExecutingReplicas{}, operator.ExecutingReplicas{})
	require.Len(t, ops, 2, "separate shards make progress independently")
	seen := make(map[uint64]bool)
	for _, op := range ops {
		step, ok := op.OpSteps()[0].(operator.AddLogService)
		require.True(t, ok, "voting and non-voting changes share a slot")
		require.False(t, seen[step.ShardID])
		seen[step.ShardID] = true
	}
	require.Equal(t, map[uint64]bool{1: true, 2: true}, seen)
	next, ok := alloc.Next()
	require.True(t, ok)
	require.Equal(t, uint64(13), next, "allocate only for selected changes")
	// A current non-voter change occupies shard 1 but does not block shard 2.
	ops = check(operator.ExecutingReplicas{}, operator.ExecutingReplicas{Removing: map[uint64][]uint64{1: {99}}})
	require.Len(t, ops, 1)
	step, ok := ops[0].OpSteps()[0].(operator.AddLogService)
	require.True(t, ok)
	require.Equal(t, uint64(2), step.ShardID)
}

var expiredTick = uint64(hakeeper.DefaultLogStoreTimeout / time.Second * hakeeper.DefaultTickPerSecond)

func TestCheckerReplacesReplicaAfterStoreIncarnationChange(t *testing.T) {
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{
		ShardID:          1,
		NumberOfReplicas: 3,
	}}}
	shard := pb.LogShardInfo{
		ShardID:                  1,
		Replicas:                 map[uint64]string{1: "a", 2: "b", 3: "c"},
		ReplicaStoreIncarnations: map[uint64]string{1: "old-a", 2: "disk-b", 3: "disk-c"},
		Epoch:                    1,
		LeaderID:                 2,
		Term:                     1,
	}
	state := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{1: shard},
		Stores: map[string]pb.LogStoreInfo{
			"a": {StoreIncarnation: "new-a"},
			"b": {
				StoreIncarnation: "disk-b",
				Replicas:         []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}},
			},
			"c": {
				StoreIncarnation: "disk-c",
				Replicas:         []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}},
			},
		},
	}
	cfg := hakeeper.Config{}
	cfg.Fill()
	newChecker := func(state pb.LogState, alloc util.IDAllocator) hakeeper.ModuleChecker {
		return NewLogServiceChecker(
			hakeeper.NewCheckerCommonFields(
				"",
				cfg,
				alloc,
				cluster,
				pb.TaskTableUser{},
				0,
			),
			state,
			pb.TNState{},
			operator.ExecutingReplicas{},
			operator.ExecutingReplicas{},
			0,
			pb.Locality{},
			false,
		)
	}

	ops := newChecker(state, util.NewTestIDAllocator(10)).Check()
	if assert.Len(t, ops, 1) && assert.Len(t, ops[0].OpSteps(), 1) {
		assert.Equal(t, operator.RemoveLogService{
			Target: "b",
			Replica: operator.Replica{
				UUID:      "a",
				ShardID:   1,
				ReplicaID: 1,
				Epoch:     1,
			},
		}, ops[0].OpSteps()[0])
	}

	// After the old membership is removed, the ordinary deficit repair path
	// allocates a new replica ID on the same live store.
	shard.Epoch = 2
	shard.Replicas = map[uint64]string{2: "b", 3: "c"}
	delete(shard.ReplicaStoreIncarnations, 1)
	state.Shards[1] = shard
	ops = newChecker(state, util.NewTestIDAllocator(10)).Check()
	if assert.Len(t, ops, 1) && assert.Len(t, ops[0].OpSteps(), 1) {
		assert.Equal(t, operator.AddLogService{
			Target: "b",
			Replica: operator.Replica{
				UUID:      "a",
				ShardID:   1,
				ReplicaID: 11,
				Epoch:     2,
			},
		}, ops[0].OpSteps()[0])
	}

	// A newly allocated ID has no old incarnation binding and is therefore
	// allowed to start on the replacement storage.
	shard.Epoch = 3
	shard.Replicas[11] = "a"
	state.Shards[1] = shard
	ops = newChecker(state, util.NewTestIDAllocator(20)).Check()
	if assert.Len(t, ops, 1) && assert.Len(t, ops[0].OpSteps(), 1) {
		assert.Equal(t, operator.StartLogService{Replica: operator.Replica{
			UUID:      "a",
			ShardID:   1,
			ReplicaID: 11,
		}}, ops[0].OpSteps()[0])
	}
}

// TestCheckerRejectsFinalReplicaRemovalAfterStoreIncarnationChange exercises
// the checker path that previously reached Builder.targets[0] with no target.
// A fresh StoreIncarnation makes the sole existing replica unsafe to retain;
// HAKeeper must keep running and emit no invalid remove operator.
func TestCheckerRejectsFinalReplicaRemovalAfterStoreIncarnationChange(t *testing.T) {
	newChecker := func(shard pb.LogShardInfo, stores map[string]pb.LogStoreInfo, nonVoting uint64) hakeeper.ModuleChecker {
		cfg := hakeeper.Config{}
		cfg.Fill()
		return NewLogServiceChecker(
			hakeeper.NewCheckerCommonFields(
				"", cfg, util.NewTestIDAllocator(10),
				pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 1}}},
				pb.TaskTableUser{}, 0,
			),
			pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: stores},
			pb.TNState{},
			operator.ExecutingReplicas{}, operator.ExecutingReplicas{},
			nonVoting, pb.Locality{}, false,
		)
	}

	t.Run("final voting replica has changed storage incarnation", func(t *testing.T) {
		checker := newChecker(
			pb.LogShardInfo{
				ShardID: 1, Replicas: map[uint64]string{1: "a"},
				ReplicaStoreIncarnations: map[uint64]string{1: "disk-a-old"}, Epoch: 1,
			},
			map[string]pb.LogStoreInfo{"a": {StoreIncarnation: "disk-a-new"}}, 0,
		)

		for i := 0; i < 2; i++ {
			assert.NotPanics(t, func() { assert.Empty(t, checker.Check()) })
		}
	})

	t.Run("final non-voting replica has no retained voting command target", func(t *testing.T) {
		checker := newChecker(
			pb.LogShardInfo{
				ShardID: 1, NonVotingReplicas: map[uint64]string{2: "b"},
				ReplicaStoreIncarnations: map[uint64]string{2: "disk-b-old"}, Epoch: 1,
			},
			map[string]pb.LogStoreInfo{"b": {StoreIncarnation: "disk-b-new"}}, 1,
		)

		for i := 0; i < 2; i++ {
			assert.NotPanics(t, func() { assert.Empty(t, checker.Check()) })
		}
	})
}

func TestCheck(t *testing.T) {
	cases := []struct {
		desc                string
		cluster             pb.ClusterInfo
		logState            pb.LogState
		users               pb.TaskTableUser
		removing            map[uint64][]uint64
		adding              map[uint64][]uint64
		currentTick         uint64
		nonVotingReplicaNum uint64
		idStartFrom         uint64
		expected            []*operator.Operator
		standbyEnabled      bool
	}{
		// for normal replicas
		{
			desc: "normal case",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 1}}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 3}}},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected:    nil,
		},
		{
			desc: "store \"a\" expired",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 1}}},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 3}}},
					"d": {
						Tick: expiredTick + 1,
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: expiredTick + 1,
			expected: []*operator.Operator{
				operator.NewOperator("", 1, 1,
					operator.RemoveLogService{
						Target: "b",
						Replica: operator.Replica{
							UUID:      "a",
							ShardID:   1,
							ReplicaID: 1,
							Epoch:     1},
					}),
			},
		},
		{
			desc: "shard 1 has only 2 replicas, which expected as 3",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{"a": {
					Tick: 0,
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
							LeaderID: 1,
							Term:     1,
						}, ReplicaID: 1}}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {Tick: 0, Replicas: []pb.LogReplicaInfo{}},
				},
			},
			idStartFrom: 3,
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("adding 1:4(at epoch 1) to c", 1,
				1, operator.AddLogService{
					Target: "a",
					Replica: operator.Replica{
						UUID:      "c",
						ShardID:   1,
						ReplicaID: 4,
						Epoch:     1,
					},
				})},
		},
		{
			desc: "replica 3 on store c is not started",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{"a": {
					Tick: 0,
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
							LeaderID: 1,
							Term:     1,
						}, ReplicaID: 1}}},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{}},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("", 1,
				1, operator.StartLogService{
					Replica: operator.Replica{
						UUID:      "c",
						ShardID:   1,
						ReplicaID: 3},
				})},
		},
		{
			desc: "store \"a\" expired and is processing",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{"a": {
					Tick: 0,
					Replicas: []pb.LogReplicaInfo{{
						LogShardInfo: pb.LogShardInfo{
							ShardID:  1,
							Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
							Epoch:    1,
							LeaderID: 1,
							Term:     1,
						}, ReplicaID: 1}}},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 2}}},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							}, ReplicaID: 3}}},
				},
			},
			removing:    map[uint64][]uint64{1: {1}},
			adding:      nil,
			currentTick: expiredTick + 1,
			expected:    []*operator.Operator{},
		},
		{
			desc: "no more stores but need to create task service",
			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{
					ShardID:    1,
					LogShardID: 1,
				}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID: 1,
					Replicas: map[uint64]string{
						1: "a",
						2: "b",
						3: "c",
					},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							},
							ReplicaID: 1,
						}},
						TaskServiceCreated: true,
					},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							},
							ReplicaID: 2,
						}},
						TaskServiceCreated: true,
					},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{{
							LogShardInfo: pb.LogShardInfo{
								ShardID:  1,
								Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
								Epoch:    1,
								LeaderID: 1,
								Term:     1,
							},
							ReplicaID: 3,
						}},
						TaskServiceCreated: false,
					},
				},
			},
			users:       pb.TaskTableUser{Username: "abc"},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("", 1,
				1, operator.CreateTaskService{
					StoreID:  "c",
					TaskUser: pb.TaskTableUser{Username: "abc"},
				})},
		},
		{
			desc: "bootstrapping for the shard that has no replicas",
			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{
					ShardID:    1,
					LogShardID: 1,
				}},
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick:               expiredTick + 1,
						Replicas:           []pb.LogReplicaInfo{},
						TaskServiceCreated: true,
					},
					"b": {
						Tick:               expiredTick + 1,
						Replicas:           []pb.LogReplicaInfo{},
						TaskServiceCreated: true,
					},
					"c": {
						Tick:               expiredTick + 1,
						Replicas:           []pb.LogReplicaInfo{},
						TaskServiceCreated: true,
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{
				operator.NewOperator("", 1,
					1, operator.BootstrapShard{
						UUID:      "a",
						ShardID:   1,
						ReplicaID: 1,
						InitialMembers: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Join: false,
					},
				),
				operator.NewOperator("", 1,
					1, operator.BootstrapShard{
						UUID:      "b",
						ShardID:   1,
						ReplicaID: 2,
						InitialMembers: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Join: false,
					},
				),
				operator.NewOperator("", 1,
					1, operator.BootstrapShard{
						UUID:      "c",
						ShardID:   1,
						ReplicaID: 3,
						InitialMembers: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Join: false,
					},
				),
			},
		},
		{
			desc:           "add new shard",
			standbyEnabled: true,
			cluster: pb.ClusterInfo{
				TNShards: []metadata.TNShardRecord{{
					ShardID:    2,
					LogShardID: 1,
				}},
				LogShards: []metadata.LogShardRecord{
					{
						ShardID:          0,
						NumberOfReplicas: 3,
					},
					{
						ShardID:          1,
						NumberOfReplicas: 3,
					},
				},
			},
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{
					0: {
						ShardID: 0,
						Replicas: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Epoch:    1,
						LeaderID: 1,
						Term:     1,
					},
					1: {
						ShardID: 1,
						Replicas: map[uint64]string{
							1: "a",
							2: "b",
							3: "c",
						},
						Epoch:    1,
						LeaderID: 1,
						Term:     1,
					},
				},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: expiredTick + 2,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  0,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 1,
							},
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 1,
							},
						},
						TaskServiceCreated: true,
					},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  0,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 2,
							},
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 2,
							},
						},
						TaskServiceCreated: true,
					},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  0,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 3,
							},
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 3,
							},
						},
						TaskServiceCreated: true,
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{
				operator.NewOperator("", 1,
					1, operator.AddLogShard{
						UUID:    "a",
						ShardID: 2,
					},
				),
			},
		},
		// for non-voting replicas
		{
			desc: "shard 1 has no non-voting replicas, which expected as 1",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 1,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:  1,
					Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:    1,
					LeaderID: 1,
					Term:     1,
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:  1,
									Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"},
									Epoch:    1,
									LeaderID: 1,
									Term:     1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{},
					},
				},
			},
			idStartFrom: 3,
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("adding 1:4(at epoch 1) to c", 1,
				1, operator.AddNonVotingLogService{
					Target: "a",
					Replica: operator.Replica{
						UUID:      "d",
						ShardID:   1,
						ReplicaID: 4,
						Epoch:     1,
					},
				}),
			},
		},
		{
			desc: "shard 1 has only 2 non-voting replicas, which expected as 3",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:             1,
					LeaderID:          1,
					Term:              1,
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 4, IsNonVoting: true,
							},
						},
					},
					"e": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 5, IsNonVoting: true,
							},
						},
					},
					"f": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{},
					},
				},
			},
			idStartFrom: 5,
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("adding 1:4(at epoch 1) to c", 1,
				1, operator.AddNonVotingLogService{
					Target: "a",
					Replica: operator.Replica{
						UUID:      "f",
						ShardID:   1,
						ReplicaID: 6,
						Epoch:     1,
					},
				}),
			},
		},
		{
			desc: "store d expired",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:             1,
					LeaderID:          1,
					Term:              1,
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 4, IsNonVoting: true,
							},
						},
					},
					"e": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 5, IsNonVoting: true,
							},
						},
					},
					"f": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 6, IsNonVoting: true,
							},
						},
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: expiredTick + 1,
			expected: []*operator.Operator{
				operator.NewOperator("", 1, 1,
					operator.RemoveNonVotingLogService{
						Target: "a",
						Replica: operator.Replica{
							UUID:      "d",
							ShardID:   1,
							ReplicaID: 4,
							Epoch:     1,
						},
					}),
			},
		},
		{
			desc: "replica 6 on store f is not started",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:             1,
					LeaderID:          1,
					Term:              1,
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 4, IsNonVoting: true,
							},
						},
					},
					"e": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 5, IsNonVoting: true,
							},
						},
					},
					"f": {
						Tick:     0,
						Replicas: []pb.LogReplicaInfo{},
					},
				},
			},
			removing:    nil,
			adding:      nil,
			currentTick: 0,
			expected: []*operator.Operator{operator.NewOperator("", 1,
				1, operator.StartNonVotingLogService{
					Replica: operator.Replica{
						UUID:      "f",
						ShardID:   1,
						ReplicaID: 6},
				}),
			},
		},
		{
			desc: "store f expired and is processing",
			cluster: pb.ClusterInfo{
				LogShards: []metadata.LogShardRecord{{
					ShardID:          1,
					NumberOfReplicas: 3,
				}},
			},
			nonVotingReplicaNum: 3,
			logState: pb.LogState{
				Shards: map[uint64]pb.LogShardInfo{1: {
					ShardID:           1,
					Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
					Epoch:             1,
					LeaderID:          1,
					Term:              1,
					NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
				}},
				Stores: map[string]pb.LogStoreInfo{
					"a": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 1,
							},
						},
					},
					"b": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 2,
							},
						},
					},
					"c": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 3,
							},
						},
					},
					"d": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 4, IsNonVoting: true,
							},
						},
					},
					"e": {
						Tick: expiredTick + 1,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 5, IsNonVoting: true,
							},
						},
					},
					"f": {
						Tick: 0,
						Replicas: []pb.LogReplicaInfo{
							{
								LogShardInfo: pb.LogShardInfo{
									ShardID:           1,
									Replicas:          map[uint64]string{1: "a", 2: "b", 3: "c"},
									NonVotingReplicas: map[uint64]string{4: "d", 5: "e", 6: "f"},
									Epoch:             1,
									LeaderID:          1,
									Term:              1,
								},
								ReplicaID: 6, IsNonVoting: true,
							},
						},
					},
				},
			},
			removing:    map[uint64][]uint64{1: {6}},
			adding:      nil,
			currentTick: expiredTick + 1,
			expected:    []*operator.Operator{},
		},
	}

	for i, c := range cases {
		fmt.Printf("case %v: %s\n", i, c.desc)
		alloc := util.NewTestIDAllocator(c.idStartFrom)
		cfg := hakeeper.Config{}
		cfg.Fill()
		executing := operator.ExecutingReplicas{
			Adding:   c.adding,
			Removing: c.removing,
		}
		lc := NewLogServiceChecker(
			hakeeper.NewCheckerCommonFields(
				"",
				cfg,
				alloc,
				c.cluster,
				c.users,
				c.currentTick,
			),
			c.logState,
			pb.TNState{},
			executing,
			executing,
			c.nonVotingReplicaNum,
			pb.Locality{},
			c.standbyEnabled,
		)
		operators := lc.Check()

		assert.Equal(t, len(c.expected), len(operators))
		for j, op := range operators {
			assert.Equal(t, len(c.expected[j].OpSteps()), len(op.OpSteps()))
			for k, step := range op.OpSteps() {
				assert.Equal(t, c.expected[j].OpSteps()[k], step)
			}
		}
	}
}

func TestStartDoesNotRacePreviouslyDispatchedRemoval(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, LeaderID: 1,
		Replicas: map[uint64]string{1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{}}
	for id, name := range shard.Replicas {
		store := pb.LogStoreInfo{Tick: 1}
		if id == 1 || id == 3 || id == 5 {
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}
		}
		state.Stores[name] = store
	}
	fields := hakeeper.NewCheckerCommonFields("", cfg, util.NewTestIDAllocator(100),
		pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 4}}}, pb.TaskTableUser{}, 1)
	executing := operator.ExecutingReplicas{Removing: map[uint64][]uint64{1: {4}}}
	checker := NewLogServiceChecker(fields, state, pb.TNState{}, executing, operator.ExecutingReplicas{}, 0, pb.Locality{}, false)
	var starts, removals []uint64
	for _, op := range checker.Check() {
		for _, step := range op.OpSteps() {
			switch step := step.(type) {
			case operator.StartLogService:
				starts = append(starts, step.ReplicaID)
			case operator.RemoveLogService:
				removals = append(removals, step.ReplicaID)
			}
		}
	}
	require.NotContains(t, starts, uint64(4), "the already-dispatched removal can still commit")
	require.Contains(t, starts, uint64(2), "another replica can start while the membership slot is occupied")
	require.Empty(t, removals, "another removal must wait for the current configuration change")
	require.NotEmpty(t, hakeeper.LogMembershipTargets(cfg, state, 1, pb.RemoveReplica,
		pb.Replica{ShardID: 1, ReplicaID: 4, UUID: "d", Epoch: 7}))
}

func TestStandbyShardUsesRegisteredConfiguration(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	cluster := pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 0, NumberOfReplicas: 1}, {ShardID: 1, NumberOfReplicas: 1}}}
	state := pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{0: {ShardID: 0}, 1: {ShardID: 1}, 77: {ShardID: 77}},
		Stores: map[string]pb.LogStoreInfo{"a": {Tick: 1}},
	}
	check := func(cluster pb.ClusterInfo) []uint64 {
		fields := hakeeper.NewCheckerCommonFields("", cfg, util.NewTestIDAllocator(100), cluster, pb.TaskTableUser{}, 1)
		checker := NewLogServiceChecker(fields, state, pb.TNState{}, operator.ExecutingReplicas{}, operator.ExecutingReplicas{}, 0, pb.Locality{}, true)
		var shards []uint64
		for _, op := range checker.Check() {
			if step, ok := op.OpSteps()[0].(operator.AddLogShard); ok {
				shards = append(shards, step.ShardID)
			}
		}
		return shards
	}
	require.Equal(t, []uint64{78}, check(cluster), "an observed orphan is not a registered standby shard")
	cluster.LogShards = append(cluster.LogShards, metadata.LogShardRecord{ShardID: 4, NumberOfReplicas: 1})
	require.Empty(t, check(cluster), "a configured standby need not have reported yet")
}
