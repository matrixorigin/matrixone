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

func TestTNStartRebuildAndRecovery(t *testing.T) {
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
	// A temporary shortage of Log replica reports leaves the TN start in flight.
	info.Shards = nil
	state.TNState.Stores["tn-a"] = info
	pending := c.OperatorController.GetOperators(2)[0]
	state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}}
	state.LogState = pb.LogState{Stores: map[string]pb.LogStoreInfo{}}
	for i, id := range []string{"log-a", "log-b"} {
		state.LogState.Stores[id] = pb.LogStoreInfo{Replicas: []pb.LogReplicaInfo{{LogShardInfo: pb.LogShardInfo{ShardID: 1}, ReplicaID: uint64(i + 1)}}}
	}
	for _, cmd := range c.Check(alloc, state, false) {
		require.Nil(t, cmd.ShutdownStore)
	}
	require.Same(t, pending, c.OperatorController.GetOperators(2)[0])
	info.Shards = []pb.TNShardInfo{{ShardID: 2, ReplicaID: restarted[0].ConfigChange.Replica.ReplicaID}}
	state.TNState.Stores["tn-a"] = info
	c.Check(alloc, state, false)
	require.Empty(t, c.OperatorController.GetOperators(2))
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

func TestCoordinatorRepairsAfterMissingReplicaReport(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	state.Tick = 10000
	state.TNState.Stores["tn-a"] = pb.TNStoreInfo{Tick: state.Tick, TaskServiceCreated: true}
	state.ClusterInfo.LogShards = []metadata.LogShardRecord{
		{ShardID: 0, NumberOfReplicas: 3}, {ShardID: 1, NumberOfReplicas: 3},
	}
	members := map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c"}
	data := pb.LogShardInfo{ShardID: 1, Replicas: members, LeaderID: 1, Epoch: 1}
	control := pb.LogShardInfo{Replicas: members, LeaderID: 1, Epoch: 1}
	state.LogState = pb.LogState{
		Shards: map[uint64]pb.LogShardInfo{0: control, 1: data},
		Stores: map[string]pb.LogStoreInfo{
			"log-a": {Tick: state.Tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: control, ReplicaID: 1}, {LogShardInfo: data, ReplicaID: 1}}},
			"log-b": {Replicas: []pb.LogReplicaInfo{{LogShardInfo: control, ReplicaID: 2}, {LogShardInfo: data, ReplicaID: 2}}},
			"log-c": {Tick: state.Tick, Replicas: []pb.LogReplicaInfo{{LogShardInfo: control, ReplicaID: 3}}},
			"log-d": {Tick: state.Tick},
		},
	}
	recoveryCommands := func() []pb.ScheduleCommand {
		t.Helper()
		cmds := c.Check(alloc, state, false)
		for _, cmd := range cmds {
			require.Nil(t, cmd.ShutdownStore)
		}
		return cmds
	}
	hasChange := func(cmds []pb.ScheduleCommand, change pb.ConfigChangeType, shard uint64, target string) bool {
		for _, cmd := range cmds {
			if cc := cmd.ConfigChange; cmd.ServiceType == pb.LogService && cc != nil &&
				cc.ChangeType == change && cc.Replica.ShardID == shard && cc.Replica.UUID == target {
				return true
			}
		}
		return false
	}
	cmds := recoveryCommands()
	require.True(t, hasChange(cmds, pb.StartReplica, 1, "log-c"), "start the missing configured replica")
	require.True(t, hasChange(cmds, pb.RemoveReplica, 0, "log-b"), "repair a shard with enough voter reports")
	require.False(t, hasChange(cmds, pb.RemoveReplica, 1, "log-b"))
	report := state.LogState.Stores["log-c"]
	report.Replicas = append(report.Replicas, pb.LogReplicaInfo{LogShardInfo: data, ReplicaID: 3, IsNonVoting: true})
	state.LogState.Stores["log-c"] = report
	require.False(t, hasChange(recoveryCommands(), pb.RemoveReplica, 1, "log-b"), "a non-voting report is insufficient")

	report.Replicas[1].IsNonVoting = false
	report.Replicas[1].ReplicaID = 4
	require.False(t, hasChange(recoveryCommands(), pb.RemoveReplica, 1, "log-b"), "a different replica ID is insufficient")
	report.Replicas[1].ReplicaID = 3
	data.ReplicaStoreIncarnations = map[uint64]string{3: "old-incarnation"}
	state.LogState.Shards[1] = data
	report.StoreIncarnation = "replacement-incarnation"
	state.LogState.Stores["log-c"] = report
	require.False(t, hasChange(recoveryCommands(), pb.RemoveReplica, 1, "log-b"), "a report from a replacement store is insufficient")

	report.StoreIncarnation = "old-incarnation"
	state.LogState.Stores["log-c"] = report
	require.True(t, hasChange(recoveryCommands(), pb.RemoveReplica, 1, "log-b"), "resume on the same coordinator when the voter reports")
	data.Replicas = map[uint64]string{1: "log-a", 3: "log-c"}
	data.Epoch++
	report.Replicas[1].LogShardInfo = data
	state.LogState.Shards[1] = data
	require.True(t, hasChange(recoveryCommands(), pb.AddReplica, 1, "log-d"), "replace the expired voter after removal")
}

func TestCoordinatorSerializesRepairThroughReportingVoters(t *testing.T) {
	c, state, alloc := tnStartScenario(t)
	state.Tick = 10000
	state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 5}}
	members := map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c", 4: "log-d", 5: "log-e"}
	state.LogState = pb.LogState{
		Shards: make(map[uint64]pb.LogShardInfo),
		Stores: make(map[string]pb.LogStoreInfo),
	}
	for _, uuid := range []string{"log-a", "log-b", "log-c", "log-d", "log-e", "log-f", "log-g"} {
		tick := state.Tick
		if uuid == "log-a" || uuid == "log-b" {
			tick = 0
		}
		state.LogState.Stores[uuid] = pb.LogStoreInfo{Tick: tick}
	}

	observe := func(epoch uint64, voters map[uint64]string, reporting []uint64) {
		data := pb.LogShardInfo{ShardID: 1, Epoch: epoch, Replicas: voters}
		data.ReplicaStoreIncarnations = make(map[uint64]string, len(voters))
		for id, uuid := range voters {
			data.ReplicaStoreIncarnations[id] = "inc-" + uuid
		}
		state.LogState.Shards[1] = data
		for _, uuid := range voters {
			store := state.LogState.Stores[uuid]
			if store.Tick == 0 { // Keep expired stores' last-known reports; they are not evidence.
				continue
			}
			store.Replicas = nil
			state.LogState.Stores[uuid] = store
		}
		for _, id := range reporting {
			uuid := voters[id]
			store := state.LogState.Stores[uuid]
			store.Replicas = append(store.Replicas, pb.LogReplicaInfo{LogShardInfo: data, ReplicaID: id})
			store.StoreIncarnation = "inc-" + uuid
			state.LogState.Stores[uuid] = store
		}
	}
	observe(1, members, []uint64{3, 4, 5})
	for id, uuid := range members {
		store := state.LogState.Stores[uuid]
		if store.Tick == 0 {
			store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: state.LogState.Shards[1], ReplicaID: id}}
			state.LogState.Stores[uuid] = store
		}
	}

	membershipChanges := func() []pb.ScheduleCommand {
		commands := c.Check(alloc, state, false)
		var changes []pb.ScheduleCommand
		for _, command := range commands {
			if command.ServiceType != pb.LogService || command.ConfigChange == nil {
				continue
			}
			switch command.ConfigChange.ChangeType {
			case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
				changes = append(changes, command)
			}
		}
		return changes
	}
	assertLiveExecutor := func(command pb.ScheduleCommand) {
		require.Contains(t, []string{"log-c", "log-d", "log-e"}, command.UUID)
	}

	changes := membershipChanges()
	require.Len(t, changes, 1, "only one config change may use the observed shard epoch")
	require.Equal(t, pb.RemoveReplica, changes[0].ConfigChange.ChangeType)
	require.Equal(t, "log-a", changes[0].ConfigChange.Replica.UUID)
	assertLiveExecutor(changes[0])

	members = map[uint64]string{2: "log-b", 3: "log-c", 4: "log-d", 5: "log-e"}
	observe(2, members, []uint64{3, 4, 5})
	changes = membershipChanges()
	require.Len(t, changes, 1, "the second removal waits for the observed epoch advance")
	require.Equal(t, pb.RemoveReplica, changes[0].ConfigChange.ChangeType)
	require.Equal(t, "log-b", changes[0].ConfigChange.Replica.UUID)
	assertLiveExecutor(changes[0])

	members = map[uint64]string{3: "log-c", 4: "log-d", 5: "log-e"}
	observe(3, members, []uint64{3, 4, 5})
	changes = membershipChanges()
	require.Len(t, changes, 1, "only one replacement is proposed per observed epoch")
	require.Equal(t, pb.AddReplica, changes[0].ConfigChange.ChangeType)
	assertLiveExecutor(changes[0])
	added := changes[0].ConfigChange.Replica
	require.Contains(t, []string{"log-f", "log-g"}, added.UUID)

	members[added.ReplicaID] = added.UUID
	observe(4, members, []uint64{3, 4, 5})
	commands := c.Check(alloc, state, false)
	started := false
	for _, command := range commands {
		if command.ConfigChange != nil && command.ConfigChange.ChangeType == pb.StartReplica &&
			command.ConfigChange.Replica.UUID == added.UUID {
			started = true
		}
	}
	require.True(t, started, "the configured replacement is started after membership is observed")

	observe(4, members, []uint64{3, 4, 5, added.ReplicaID})
	commands = c.Check(alloc, state, false)
	for _, command := range commands {
		if command.ConfigChange != nil {
			require.False(t, command.ConfigChange.ChangeType == pb.StartReplica && command.ConfigChange.Replica.UUID == added.UUID,
				"a reported replacement is no longer started repeatedly")
		}
	}
}

func TestCoordinatorPreservesVotingQuorumAfterRemoval(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failedID   uint64
		wantRemove string
	}{
		{name: "remove unavailable voter before losing quorum", failedID: 5, wantRemove: "log-e"},
		{name: "do not remove a reporting voter when projection loses quorum", failedID: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			currentTick := uint64(1)
			if tc.failedID != 0 {
				currentTick = expiredTick + 1
			}
			c := NewCoordinator("", hakeeper.Config{})
			state := pb.CheckerState{
				Tick: currentTick,
				ClusterInfo: pb.ClusterInfo{
					LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 3}},
				},
				LogState: pb.LogState{
					Shards: map[uint64]pb.LogShardInfo{},
					Stores: map[string]pb.LogStoreInfo{},
				},
			}
			voters := map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c", 4: "log-d", 5: "log-e"}
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 7, Replicas: voters}
			state.LogState.Shards[1] = shard
			for id, uuid := range voters {
				tick := currentTick
				if id == tc.failedID {
					tick = 0
				}
				store := pb.LogStoreInfo{Tick: tick}
				if id <= 3 {
					store.Replicas = []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: id}}
				}
				state.LogState.Stores[uuid] = store
			}

			commands := c.Check(util.NewTestIDAllocator(100), state, false)
			var membership []pb.ScheduleCommand
			for _, command := range commands {
				if command.ServiceType == pb.LogService && command.ConfigChange != nil {
					switch command.ConfigChange.ChangeType {
					case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
						membership = append(membership, command)
					}
				}
			}
			if tc.wantRemove == "" {
				require.Empty(t, membership)
				return
			}
			require.Len(t, membership, 1)
			require.Equal(t, pb.RemoveReplica, membership[0].ConfigChange.ChangeType)
			require.Equal(t, tc.wantRemove, membership[0].ConfigChange.Replica.UUID)
			require.Contains(t, []string{"log-a", "log-b", "log-c"}, membership[0].UUID)
		})
	}
}

func TestCoordinatorRetiresStaleMembershipOperators(t *testing.T) {
	for _, staleBy := range []string{"epoch", "executor", "destination_missing", "destination_expired"} {
		t.Run(staleBy, func(t *testing.T) {
			c := NewCoordinator("", hakeeper.Config{})
			shard := pb.LogShardInfo{ShardID: 1, Epoch: 1, Replicas: map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c"}}
			state := pb.CheckerState{
				Tick:        1,
				ClusterInfo: pb.ClusterInfo{LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 4}}},
				LogState: pb.LogState{
					Shards: map[uint64]pb.LogShardInfo{1: shard},
					Stores: map[string]pb.LogStoreInfo{
						"log-a": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
						"log-b": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 2}}},
						"log-c": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 3}}},
						"log-d": {Tick: 1},
						"log-e": {Tick: 1},
					},
				},
			}
			stale := operator.NewOperator("stale", 1, 1, operator.AddLogService{
				Target: "log-a", Replica: operator.Replica{UUID: "log-d", ShardID: 1, ReplicaID: 4, Epoch: 1},
			})
			commands := c.OperatorController.Dispatch([]*operator.Operator{stale}, state.LogState, state.TNState, state.CNState, state.ProxyState)
			require.Len(t, commands, 1)

			switch staleBy {
			case "epoch":
				shard.Epoch = 2
				state.LogState.Shards[1] = shard
			case "executor":
				store := state.LogState.Stores["log-a"]
				store.Replicas = nil
				state.LogState.Stores["log-a"] = store
			case "destination_missing":
				delete(state.LogState.Stores, "log-d")
			case "destination_expired":
				state.Tick = expiredTick + 2
				for _, uuid := range []string{"log-a", "log-b", "log-c", "log-e"} {
					store := state.LogState.Stores[uuid]
					store.Tick = state.Tick
					state.LogState.Stores[uuid] = store
				}
			}

			commands = c.Check(util.NewTestIDAllocator(100), state, false)
			require.Equal(t, operator.CANCELED, stale.Status())
			var add *pb.ScheduleCommand
			for i := range commands {
				if commands[i].ServiceType == pb.LogService && commands[i].ConfigChange != nil &&
					commands[i].ConfigChange.ChangeType == pb.AddReplica {
					add = &commands[i]
					break
				}
			}
			require.NotNil(t, add, "fresh membership repair must proceed without waiting for operator TTL")
			require.Equal(t, shard.Epoch, add.ConfigChange.Replica.Epoch)
			if staleBy == "executor" {
				require.NotEqual(t, "log-a", add.UUID)
			}
		})
	}
}

func TestCoordinatorGatesMembershipWithoutVoterMajority(t *testing.T) {
	c := NewCoordinator("", hakeeper.Config{})
	shard := pb.LogShardInfo{ShardID: 1, Epoch: 3, Replicas: map[uint64]string{1: "log-a", 2: "log-b", 3: "log-c"}}
	state := pb.CheckerState{
		Tick: 1,
		ClusterInfo: pb.ClusterInfo{
			LogShards: []metadata.LogShardRecord{{ShardID: 1, NumberOfReplicas: 5}},
		},
		NonVotingReplicaNum: 1,
		LogState: pb.LogState{
			Shards: map[uint64]pb.LogShardInfo{1: shard},
			Stores: map[string]pb.LogStoreInfo{
				"log-a": {Tick: 1, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 1}}},
				"log-b": {Tick: 1},
				"log-c": {Tick: 1},
				"log-d": {Tick: 1},
				"log-e": {Tick: 1},
			},
		},
	}
	commands := c.Check(util.NewTestIDAllocator(100), state, false)
	for _, command := range commands {
		if command.ServiceType == pb.LogService && command.ConfigChange != nil {
			switch command.ConfigChange.ChangeType {
			case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
				t.Fatalf("membership change without voting majority: %+v", command)
			}
		}
	}
}
