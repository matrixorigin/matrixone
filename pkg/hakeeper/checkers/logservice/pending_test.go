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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func TestPendingOperatorsFollowCurrentLogCandidates(t *testing.T) {
	tests := []struct {
		name              string
		step              operator.OpStep
		normal, nonVoting uint32
		mutate            func(*pb.LogState)
		want              bool
	}{
		{name: "start", step: operator.StartLogService{Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3}}, normal: 3, want: true},
		{name: "nonvoting_start", step: operator.StartNonVotingLogService{Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4}}, normal: 3, nonVoting: 1, want: true},
		{name: "add", step: operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 1}}, normal: 4, nonVoting: 1, want: true},
		{name: "nonvoting_add", step: operator.AddNonVotingLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 1}}, normal: 3, nonVoting: 2, want: true},
		{name: "remove", step: operator.RemoveLogService{Target: "a", Replica: operator.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 1}}, normal: 2, nonVoting: 1, want: true},
		{name: "nonvoting_remove", step: operator.RemoveNonVotingLogService{Target: "a", Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4, Epoch: 1}}, normal: 3, want: true},
		{name: "partial_bootstrap", step: operator.BootstrapShard{UUID: "c", ShardID: 1, ReplicaID: 3, InitialMembers: map[uint64]string{1: "a", 2: "b", 3: "c"}}, normal: 3, want: true},
		{name: "empty_bootstrap", step: operator.BootstrapShard{UUID: "c", ShardID: 1, ReplicaID: 3}, normal: 3, mutate: func(s *pb.LogState) { v := s.Shards[1]; v.Replicas = nil; s.Shards[1] = v }, want: true},
		{name: "stale_epoch", step: operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 0}}, normal: 4},
		{name: "recovered_add_capacity", step: operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "e", ShardID: 1, ReplicaID: 5, Epoch: 1}}, normal: 3},
		{name: "stale_remove_epoch", step: operator.RemoveLogService{Target: "a", Replica: operator.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 0}}, normal: 2},
		{name: "obsolete_remove", step: operator.RemoveLogService{Target: "a", Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3, Epoch: 1}}, normal: 3},
		{name: "wrong_role", step: operator.StartLogService{Replica: operator.Replica{UUID: "d", ShardID: 1, ReplicaID: 4}}, normal: 3},
		{name: "wrong_replica", step: operator.StartLogService{Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 99}}, normal: 3},
		{name: "missing_target", step: operator.StartLogService{Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3}}, normal: 3, mutate: func(s *pb.LogState) { delete(s.Stores, "c") }},
		{name: "expired_target", step: operator.StartLogService{Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3}}, normal: 3, mutate: func(s *pb.LogState) { v := s.Stores["c"]; v.Tick = 0; s.Stores["c"] = v }},
		{name: "obsolete_bootstrap", step: operator.BootstrapShard{UUID: "e", ShardID: 1, ReplicaID: 5}, normal: 3},
		{name: "no_destructive_tn_retry", step: operator.RemoveTnReplica{StoreID: "tn", ShardID: 1, ReplicaID: 99}, normal: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := hakeeper.Config{}
			cfg.Fill()
			tick := uint64(10000)
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, LeaderID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, NonVotingReplicas: map[uint64]string{4: "d"}}
			state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
				"a": {Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				"b": {Tick: tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
				"c": {Tick: tick}, "d": {Tick: tick}, "e": {Tick: tick},
			}}
			if tt.mutate != nil {
				tt.mutate(&state)
			}
			fields := hakeeper.NewCheckerCommonFields("", cfg, nil, pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: uint64(tt.normal)}}}, pb.TaskTableUser{}, tick)
			op := operator.NewOperator("", 1, 1, tt.step)
			got := PendingOperators(fields, state, uint64(tt.nonVoting), []*operator.Operator{nil, op, op})
			if tt.want {
				require.Equal(t, []*operator.Operator{op}, got)
			} else {
				require.Empty(t, got)
			}
			for _, terminal := range []operator.OpStatus{operator.SUCCESS, operator.CANCELED, operator.EXPIRED} {
				op.SetStatus(terminal)
				require.Empty(t, PendingOperators(fields, state, uint64(tt.nonVoting), []*operator.Operator{op}))
			}
		})
	}
}

func TestPendingAddSlotsAndStepProgress(t *testing.T) {
	cfg := hakeeper.Config{}
	cfg.Fill()
	fields := hakeeper.NewCheckerCommonFields("", cfg, nil, pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 2}}}, pb.TaskTableUser{}, 1)
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, LeaderID: 1, Replicas: map[uint64]string{1: "a"}}
	state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{"a": {Tick: 1}, "b": {Tick: 1}, "c": {Tick: 1}}}
	add := operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "b", ShardID: 1, ReplicaID: 2, Epoch: 1}}
	start := operator.StartLogService{Replica: operator.Replica{UUID: "b", ShardID: 1, ReplicaID: 2}}
	op := operator.NewOperator("", 1, 1, add, start)
	extra := operator.NewOperator("", 1, 1, operator.AddLogService{Target: "a", Replica: operator.Replica{UUID: "c", ShardID: 1, ReplicaID: 3, Epoch: 1}})
	require.Equal(t, []*operator.Operator{op}, PendingOperators(fields, state, 0, []*operator.Operator{op, extra}))
	controller := operator.NewController()
	cmds := controller.Dispatch([]*operator.Operator{op}, state, pb.TNState{}, pb.CNState{}, pb.ProxyState{})
	require.Equal(t, pb.AddReplica, cmds[0].ConfigChange.ChangeType)
	// Only after membership progress may the current step become StartReplica.
	shard.Replicas[2] = "b"
	shard.Epoch = 2
	state.Shards[1] = shard
	require.Equal(t, []*operator.Operator{op}, PendingOperators(fields, state, 0, []*operator.Operator{op, extra}))
	cmds = controller.Dispatch([]*operator.Operator{op}, state, pb.TNState{}, pb.CNState{}, pb.ProxyState{})
	require.Equal(t, pb.StartReplica, cmds[0].ConfigChange.ChangeType)
	store := state.Stores["b"]
	store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}
	state.Stores["b"] = store
	require.Empty(t, PendingOperators(fields, state, 0, []*operator.Operator{op}))
	require.Equal(t, operator.SUCCESS, op.Status())
}

func TestPendingStartsRespectStoreIncarnation(t *testing.T) {
	for _, nonVoting := range []bool{false, true} {
		cfg := hakeeper.Config{}
		cfg.Fill()
		fields := hakeeper.NewCheckerCommonFields("", cfg, nil, pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 1}}}, pb.TaskTableUser{}, 1)
		shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, Replicas: map[uint64]string{1: "a"}, ReplicaStoreIncarnations: map[uint64]string{1: "old"}}
		var step operator.OpStep = operator.StartLogService{Replica: operator.Replica{UUID: "a", ShardID: 1, ReplicaID: 1}}
		n := uint64(0)
		if nonVoting {
			shard.NonVotingReplicas = shard.Replicas
			shard.Replicas = nil
			n = 1
			step = operator.StartNonVotingLogService{Replica: operator.Replica{UUID: "a", ShardID: 1, ReplicaID: 1}}
		}
		op := operator.NewOperator("", 1, 1, step)
		state := pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{"a": {Tick: 1, StoreIncarnation: "old"}}}
		require.Equal(t, []*operator.Operator{op}, PendingOperators(fields, state, n, []*operator.Operator{op}))
		store := state.Stores["a"]
		store.StoreIncarnation = "new"
		state.Stores["a"] = store
		require.Empty(t, PendingOperators(fields, state, n, []*operator.Operator{op}))
		require.Equal(t, operator.STARTED, op.Status(), "an obsolete projection is not a fabricated completion")
	}
}
