// Copyright 2021 - 2026 Matrix Origin
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

package checkers

import (
	"testing"
	"time"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	tncheck "github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/tnservice"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func tnStartScenario(t *testing.T) (*Coordinator, pb.CheckerState, *util.TestIDAllocator) {
	t.Helper()
	// The existing coordinator fixture owns this runtime. Restore its check state.
	rt := runtime.ServiceRuntime("")
	previous, exists := rt.GetGlobalVariables("log_service_init_state")
	tncheck.InitCheckState("")
	t.Cleanup(func() {
		if exists {
			rt.SetGlobalVariables("log_service_init_state", previous)
		} else {
			tncheck.InitCheckState("")
		}
	})
	state := pb.CheckerState{
		Tick:        1,
		ClusterInfo: pb.ClusterInfo{TNShards: []metadata.TNShardRecord{{ShardID: 2, LogShardID: 1}}},
		TNState:     pb.TNState{Stores: map[string]pb.TNStoreInfo{"tn-a": {Tick: 1, TaskServiceCreated: true}}},
	}
	return NewCoordinator("", hakeeper.Config{TNStoreTimeout: time.Minute}), state, util.NewTestIDAllocator(100)
}

func TestTNStartRescheduledAfterLegacyCommandLoss(t *testing.T) {
	for _, overwrite := range []bool{false, true} {
		name := "lost_response"
		if overwrite {
			name = "overwritten_batch"
		}
		t.Run(name, func(t *testing.T) {
			c, state, alloc := tnStartScenario(t)
			rsm := hakeeper.NewStateMachine(0, 1)
			var index uint64
			apply := func(cmd []byte) sm.Result {
				t.Helper()
				index++
				result, err := rsm.Update(sm.Entry{Index: index, Cmd: cmd})
				require.NoError(t, err)
				return result
			}
			heartbeat := func() pb.CommandBatch {
				t.Helper()
				hb, err := (&pb.TNStoreHeartbeat{UUID: "tn-a"}).Marshal()
				require.NoError(t, err)
				var b pb.CommandBatch
				require.NoError(t, b.Unmarshal(apply(hakeeper.GetTNStoreHeartbeatCmd(hb)).Data))
				return b
			}
			first := c.Check(alloc, state, false)
			require.Len(t, first, 1)
			require.False(t, first[0].Bootstrapping)
			op := c.OperatorController.GetOperators(2)[0]
			apply(hakeeper.GetUpdateCommandsCmd(1, first))
			if overwrite {
				apply(hakeeper.GetUpdateCommandsCmd(1, []pb.ScheduleCommand{{UUID: "cn-other", ServiceType: pb.CNService}}))
			} else {
				require.Equal(t, first, heartbeat().Commands) // Discard this committed response.
			}
			require.Empty(t, heartbeat().Commands, "legacy RSM consumption remains historical")
			for i := 0; i < 3; i++ {
				state.Tick++
				retried := c.Check(alloc, state, false)
				require.Equal(t, first, retried)
				require.Equal(t, []*operator.Operator{op}, c.OperatorController.GetOperators(2))
				apply(hakeeper.GetUpdateCommandsCmd(1, retried))
				require.Equal(t, first, heartbeat().Commands)
			}
			nextID, ok := alloc.Next()
			require.True(t, ok)
			require.Equal(t, uint64(102), nextID, "retry did not allocate another ID")
			info := state.TNState.Stores["tn-a"]
			info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: first[0].ConfigChange.Replica.ReplicaID}}
			state.TNState.Stores["tn-a"] = info
			require.Empty(t, c.Check(alloc, state, false))
			require.Empty(t, c.OperatorController.GetOperators(2))
		})
	}
}

func TestTNStartReconcilesReportsAndTargets(t *testing.T) {
	for _, name := range []string{"same_id", "different_id", "other_live_target", "wrong_shard", "expired_report", "missing_target", "expired_target", "no_live_target", "canceled", "canceled_siblings", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			c, state, alloc := tnStartScenario(t)
			first := c.Check(alloc, state, false)
			require.Len(t, first, 1)
			op := c.OperatorController.GetOperators(2)[0]
			id := first[0].ConfigChange.Replica.ReplicaID
			state.Tick = 2
			info := state.TNState.Stores["tn-a"]
			switch name {
			case "same_id":
				info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: id}}
			case "different_id":
				info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: id + 10}}
			case "other_live_target":
				state.TNState.Stores["tn-b"] = pb.TNStoreInfo{Tick: 2, TaskServiceCreated: true, Shards: []pb.TNShardInfo{{ShardID: 2, ReplicaID: id + 10}}}
			case "wrong_shard":
				info.Shards = []pb.TNShardInfo{{ShardID: 99, ReplicaID: id}}
			case "expired_report":
				state.Tick = 10000
				info.Tick = state.Tick
				state.TNState.Stores["tn-old"] = pb.TNStoreInfo{Shards: []pb.TNShardInfo{{ShardID: 2, ReplicaID: id + 10}}}
			case "missing_target":
				delete(state.TNState.Stores, "tn-a")
				state.TNState.Stores["tn-b"] = pb.TNStoreInfo{Tick: 2, TaskServiceCreated: true}
			case "expired_target":
				state.Tick = 10000
				state.TNState.Stores["tn-b"] = pb.TNStoreInfo{Tick: state.Tick, TaskServiceCreated: true}
			case "no_live_target":
				state.Tick = 10000
			case "canceled":
				op.Cancel()
			case "canceled_siblings":
				siblings := []*operator.Operator{op}
				op.Cancel()
				for i := uint64(1); i <= 2; i++ {
					canceled := operator.NewOperator("tnservice", 2, operator.NoopEpoch, operator.AddTnReplica{StoreID: "tn-a", ShardID: 2, ReplicaID: id + i, LogShardID: 1})
					c.OperatorController.Dispatch([]*operator.Operator{canceled}, state.LogState, state.TNState, state.CNState, state.ProxyState)
					canceled.Cancel()
					siblings = append(siblings, canceled)
				}
				require.Equal(t, siblings, c.OperatorController.GetOperators(2), "all three adjacent canceled entries must be present before reconciliation")
			case "duplicate":
				duplicate := operator.NewOperator("tnservice", 2, operator.NoopEpoch, operator.AddTnReplica{StoreID: "tn-a", ShardID: 2, ReplicaID: id + 10, LogShardID: 1})
				c.OperatorController.Dispatch([]*operator.Operator{duplicate}, state.LogState, state.TNState, state.CNState, state.ProxyState)
			}
			if name != "missing_target" {
				state.TNState.Stores["tn-a"] = info
			}
			commands := c.Check(alloc, state, false)
			switch name {
			case "same_id", "different_id", "other_live_target", "no_live_target", "canceled", "canceled_siblings":
				require.Empty(t, commands)
				require.Empty(t, c.OperatorController.GetOperators(2))
				if name == "same_id" {
					require.Equal(t, operator.SUCCESS, op.Status())
				} else {
					require.Equal(t, operator.CANCELED, op.Status())
				}
			case "missing_target":
				require.Empty(t, commands)
				require.Equal(t, operator.CANCELED, op.Status())
				require.Empty(t, c.OperatorController.GetOperators(2))
				// Reuse the established eligibility policy after abandoning this target.
				state.Tick = 10000
				info := state.TNState.Stores["tn-b"]
				info.Tick = state.Tick
				state.TNState.Stores["tn-b"] = info
				next := c.Check(alloc, state, false)
				require.Len(t, next, 1)
				require.Equal(t, "tn-b", next[0].UUID)
			case "expired_target":
				require.Len(t, commands, 1)
				require.Equal(t, "tn-b", commands[0].UUID)
				require.Equal(t, operator.CANCELED, op.Status())
			default:
				require.Equal(t, first, commands)
				require.Equal(t, []*operator.Operator{op}, c.OperatorController.GetOperators(2))
			}
		})
	}
}

func TestTNStartReservationsAndPartialReports(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	first := c.Check(alloc, state, false)
	require.Len(t, first, 1)
	state.TNState.Stores["tn-b"] = pb.TNStoreInfo{Tick: state.Tick, TaskServiceCreated: true}
	state.ClusterInfo.TNShards = append(state.ClusterInfo.TNShards, metadata.TNShardRecord{ShardID: 3, LogShardID: 1})
	// The existing initial-shard wait remains in force for newly discovered work.
	require.Len(t, c.Check(alloc, state, false), 1)
	state.Tick = 10000
	for id, info := range state.TNState.Stores {
		info.Tick = state.Tick
		state.TNState.Stores[id] = info
	}
	both := c.Check(alloc, state, false)
	require.Len(t, both, 2)
	require.Equal(t, "tn-a", both[0].UUID)
	require.Equal(t, "tn-b", both[1].UUID)
	info := state.TNState.Stores["tn-a"]
	info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: first[0].ConfigChange.Replica.ReplicaID}}
	state.TNState.Stores["tn-a"] = info
	remaining := c.Check(alloc, state, false)
	require.Equal(t, both[1:], remaining)
	require.Empty(t, c.OperatorController.GetOperators(2))
	require.Len(t, c.OperatorController.GetOperators(3), 1)
}

func TestTNStartRebuildAndTeardown(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	first := c.Check(alloc, state, false)
	require.Len(t, first, 1)
	old := c.OperatorController.GetOperators(2)[0]
	// A new coordinator rebuilds missing intent from current cluster records.
	c = NewCoordinator("", hakeeper.Config{TNStoreTimeout: time.Minute})
	state.Tick = 10000
	info := state.TNState.Stores["tn-a"]
	info.Tick = state.Tick
	state.TNState.Stores["tn-a"] = info
	restarted := c.Check(alloc, state, false)
	require.Len(t, restarted, 1)
	require.NotEqual(t, first[0].ConfigChange.Replica.ReplicaID, restarted[0].ConfigChange.Replica.ReplicaID)
	// An old leader's retained attempt must yield to a current different-ID report.
	stale := NewCoordinator("", hakeeper.Config{TNStoreTimeout: time.Minute})
	stale.OperatorController.Dispatch([]*operator.Operator{old}, state.LogState, state.TNState, state.CNState, state.ProxyState)
	info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: restarted[0].ConfigChange.Replica.ReplicaID}}
	state.TNState.Stores["tn-a"] = info
	require.Empty(t, stale.Check(alloc, state, false))
	require.Empty(t, stale.OperatorController.GetOperators(2))
	// Keep this coordinator's start unfinished when entering teardown.
	info.Shards = nil
	state.TNState.Stores["tn-a"] = info
	pending := c.OperatorController.GetOperators(2)[0]
	// Expired majority forces the actual syshealth teardown branch.
	state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
	state.LogState = pb.LogState{Stores: map[string]pb.LogStoreInfo{}}
	for i, id := range []string{"log-a", "log-b"} {
		state.LogState.Stores[id] = pb.LogStoreInfo{Replicas: []pb.LogReplicaInfo{{LogShardInfo: pb.LogShardInfo{ShardID: 1}, ReplicaID: uint64(i + 1)}}}
	}
	for i := 0; i < 2; i++ {
		commands := c.Check(alloc, state, false)
		require.NotEmpty(t, commands)
		for _, cmd := range commands {
			require.Nil(t, cmd.ConfigChange)
			require.NotNil(t, cmd.ShutdownStore)
		}
		require.Empty(t, c.OperatorController.GetOperators(2))
		require.Equal(t, operator.CANCELED, pending.Status())
	}
}

func TestTNRetryPreservesPendingLogStart(t *testing.T) {
	for _, tt := range []struct {
		name              string
		pending, consumed bool
	}{
		{name: "tn_already_running"}, {name: "tn_start_pending", pending: true}, {name: "log_response_consumed", pending: true, consumed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			coordinator, state, alloc := tnStartScenario(t)
			shard := pb.LogShardInfo{ShardID: 1, Replicas: map[uint64]string{1: "a", 2: "b", 3: "c"}, Epoch: 1, LeaderID: 1, Term: 1}
			state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
			state.LogState = pb.LogState{Shards: map[uint64]pb.LogShardInfo{1: shard}, Stores: map[string]pb.LogStoreInfo{
				"a": {Tick: 1, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				"b": {Tick: 1, TaskServiceCreated: true, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
				"c": {Tick: 1, TaskServiceCreated: true},
			}}
			if !tt.pending {
				info := state.TNState.Stores["tn-a"]
				info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: 99}}
				state.TNState.Stores["tn-a"] = info
			}
			rsm := hakeeper.NewStateMachine(0, 1)
			var index uint64
			apply := func(cmd []byte) sm.Result {
				index++
				r, err := rsm.Update(sm.Entry{Index: index, Cmd: cmd})
				require.NoError(t, err)
				return r
			}
			first := coordinator.Check(alloc, state, false)
			var logStart *pb.ScheduleCommand
			for i := range first {
				cmd := &first[i]
				if cmd.UUID == "c" && cmd.ServiceType == pb.LogService && cmd.ConfigChange != nil && cmd.ConfigChange.ChangeType == pb.StartReplica {
					logStart = cmd
				}
			}
			require.NotNil(t, logStart, "fixture must schedule the actual missing log replica")
			t.Logf("first batch: %+v", first)
			apply(hakeeper.GetUpdateCommandsCmd(1, first))
			hb, err := (&pb.LogStoreHeartbeat{UUID: "c"}).Marshal()
			require.NoError(t, err)
			if tt.consumed {
				var original pb.CommandBatch
				require.NoError(t, original.Unmarshal(apply(hakeeper.GetLogStoreHeartbeatCmd(hb)).Data))
				require.Contains(t, original.Commands, *logStart)
			}
			state.Tick++
			second := coordinator.Check(alloc, state, false)
			t.Logf("second batch: %+v", second)
			if len(second) > 0 {
				apply(hakeeper.GetUpdateCommandsCmd(1, second))
			}
			var received pb.CommandBatch
			require.NoError(t, received.Unmarshal(apply(hakeeper.GetLogStoreHeartbeatCmd(hb)).Data))
			t.Logf("log heartbeat commands: %+v", received.Commands)
			require.Contains(t, received.Commands, *logStart, "a TN-only retry must not erase an unrelated undelivered log start")
			// A real report ends the retained Log intent, even while TN still retries.
			store := state.LogState.Stores["c"]
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}
			state.LogState.Stores["c"] = store
			next := coordinator.Check(alloc, state, false)
			for _, cmd := range next {
				require.NotEqual(t, pb.LogService, cmd.ServiceType)
			}
			require.Empty(t, coordinator.OperatorController.GetOperators(1))
		})
	}
}

func TestTNRetryDoesNotResurrectObsoleteRemoval(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	state.ClusterInfo.TNShards = append(state.ClusterInfo.TNShards, metadata.TNShardRecord{ShardID: 4, LogShardID: 1})
	info := state.TNState.Stores["tn-a"]
	info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: 99}}
	state.TNState.Stores["tn-a"] = info
	old := operator.NewOperator("", 2, operator.NoopEpoch, operator.RemoveTnReplica{StoreID: "tn-a", ShardID: 2, ReplicaID: 99, LogShardID: 1})
	// The removal was scheduled while another replica existed. That peer has
	// since disappeared; tn-a is now the sole working replica of shard 2.
	c.OperatorController.Dispatch([]*operator.Operator{old}, state.LogState, state.TNState, state.CNState, state.ProxyState)
	first := c.Check(alloc, state, false)
	require.Len(t, first, 1)
	require.Equal(t, uint64(4), first[0].ConfigChange.Replica.ShardID)
	next := c.Check(alloc, state, false)
	require.Equal(t, first, next)
	require.Equal(t, operator.STARTED, old.Status())
}
