// Copyright 2026 Matrix Origin
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

package hakeeper

import (
	"bytes"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/hakeeper/operator"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func dispatchedTNStart(t *testing.T, store string, shard, replica uint64) pb.ScheduleCommand {
	t.Helper()
	op := operator.NewOperator("tnservice", shard, operator.NoopEpoch,
		operator.AddTnReplica{StoreID: store, ShardID: shard, ReplicaID: replica, LogShardID: 1})
	commands := operator.NewController().Dispatch([]*operator.Operator{op}, pb.LogState{}, pb.TNState{}, pb.CNState{}, pb.ProxyState{})
	require.Len(t, commands, 1)
	require.False(t, commands[0].Bootstrapping, "exercise the real running-cluster producer")
	return commands[0]
}

func updateTNCommands(t *testing.T, rsm *stateMachine, term uint64, commands ...pb.ScheduleCommand) sm.Result {
	t.Helper()
	result, err := rsm.Update(sm.Entry{Cmd: GetUpdateCommandsCmd(term, commands)})
	require.NoError(t, err)
	return result
}

func heartbeatTNCommands(t *testing.T, rsm *stateMachine, store string, shards ...pb.TNShardInfo) []pb.ScheduleCommand {
	t.Helper()
	data, err := (&pb.TNStoreHeartbeat{UUID: store, Shards: shards}).Marshal()
	require.NoError(t, err)
	result, err := rsm.Update(sm.Entry{Cmd: GetTNStoreHeartbeatCmd(data)})
	require.NoError(t, err)
	var batch pb.CommandBatch
	require.NoError(t, batch.Unmarshal(result.Data))
	return batch.Commands
}

func TestTNStartSurvivesLostResponsesAndCheckerUpdates(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.State = pb.HAKeeperRunning
	start := dispatchedTNStart(t, "tn-1", 2, 10)
	task := pb.ScheduleCommand{UUID: "tn-1", ServiceType: pb.TNService, CreateTaskService: &pb.CreateTaskService{}}
	updateTNCommands(t, rsm, 1, start, task)
	// The server committed this heartbeat, but its caller never observed it.
	require.Equal(t, []pb.ScheduleCommand{start, task}, heartbeatTNCommands(t, rsm, "tn-1"))

	updateTNCommands(t, rsm, 1, task)
	require.Equal(t, []pb.ScheduleCommand{start, task}, heartbeatTNCommands(t, rsm, "tn-1"))
	require.Equal(t, []pb.ScheduleCommand{start}, heartbeatTNCommands(t, rsm, "tn-1"), "task remains one-shot")

	// Carry the same existing protobuf state through snapshot recovery.
	var snapshot bytes.Buffer
	require.NoError(t, rsm.SaveSnapshot(&snapshot, nil, nil))
	recovered := NewStateMachine(0, 2).(*stateMachine)
	require.NoError(t, recovered.RecoverFromSnapshot(&snapshot, nil, nil))
	other := pb.ScheduleCommand{UUID: "log-1", ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{ChangeType: pb.AddReplica}}
	updateTNCommands(t, recovered, 2, other)
	require.Equal(t, []pb.ScheduleCommand{start}, heartbeatTNCommands(t, recovered, "tn-1"))
	updateTNCommands(t, recovered, 2)
	_, oldOneShot := recovered.state.ScheduleCommands["log-1"]
	require.False(t, oldOneShot, "unconsumed unrelated commands retain full-replacement semantics")
	require.Equal(t, []pb.ScheduleCommand{start}, heartbeatTNCommands(t, recovered, "tn-1"))
	require.Empty(t, heartbeatTNCommands(t, recovered, "other", pb.TNShardInfo{ShardID: 2, ReplicaID: 10}))
	require.Equal(t, []pb.ScheduleCommand{start}, heartbeatTNCommands(t, recovered, "tn-1", pb.TNShardInfo{ShardID: 3, ReplicaID: 10}))
	require.Empty(t, heartbeatTNCommands(t, recovered, "tn-1", pb.TNShardInfo{ShardID: 2, ReplicaID: 10}))
	require.Empty(t, recovered.state.ScheduleCommands)
}

func TestTNStartReconciliation(t *testing.T) {
	start := dispatchedTNStart(t, "tn-1", 2, 10)
	later := dispatchedTNStart(t, "tn-1", 2, 9) // Accepted order, not the largest replica ID.
	moved := dispatchedTNStart(t, "tn-2", 2, 8)
	removal := func(store string, shard uint64, change pb.ConfigChangeType) pb.ScheduleCommand {
		return pb.ScheduleCommand{UUID: store, ServiceType: pb.TNService, ConfigChange: &pb.ConfigChange{
			ChangeType: change, Replica: pb.Replica{ShardID: shard, ReplicaID: 99},
		}}
	}
	stop := removal("tn-1", 2, pb.StopReplica)
	remove := removal("tn-1", 2, pb.RemoveReplica)
	legacy := removal("tn-1", 2, pb.AddReplica)
	shutdown := pb.ScheduleCommand{UUID: "tn-1", ServiceType: pb.TNService, ShutdownStore: &pb.ShutdownStore{}}
	for _, tc := range []struct {
		name     string
		incoming []pb.ScheduleCommand
		want     []pb.ScheduleCommand
	}{
		{"duplicate", []pb.ScheduleCommand{start, start}, []pb.ScheduleCommand{start}},
		{"new replica", []pb.ScheduleCommand{later}, []pb.ScheduleCommand{later}},
		{"new target", []pb.ScheduleCommand{moved}, []pb.ScheduleCommand{moved}},
		{"stop", []pb.ScheduleCommand{stop}, []pb.ScheduleCommand{stop}},
		{"remove alias", []pb.ScheduleCommand{remove}, []pb.ScheduleCommand{remove}},
		{"legacy add stays one-shot", []pb.ScheduleCommand{legacy}, []pb.ScheduleCommand{legacy}},
		{"shutdown", []pb.ScheduleCommand{shutdown}, []pb.ScheduleCommand{shutdown}},
		{"start then stop", []pb.ScheduleCommand{later, stop}, []pb.ScheduleCommand{stop}},
		{"stop then start", []pb.ScheduleCommand{stop, later}, []pb.ScheduleCommand{stop, later}},
		{"other shard", []pb.ScheduleCommand{removal("tn-1", 3, pb.StopReplica)}, []pb.ScheduleCommand{start, removal("tn-1", 3, pb.StopReplica)}},
		{"other target", []pb.ScheduleCommand{removal("tn-2", 2, pb.RemoveReplica)}, []pb.ScheduleCommand{start, removal("tn-2", 2, pb.RemoveReplica)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rsm := NewStateMachine(0, 1).(*stateMachine)
			rsm.state.State = pb.HAKeeperRunning
			updateTNCommands(t, rsm, 1, start)
			updateTNCommands(t, rsm, 1, tc.incoming...)
			for _, store := range []string{"tn-1", "tn-2"} {
				var want, retry []pb.ScheduleCommand
				for _, command := range tc.want {
					if command.UUID == store {
						want = append(want, command)
						if command.ConfigChange != nil && command.ConfigChange.ChangeType == pb.StartReplica {
							retry = append(retry, command)
						}
					}
				}
				require.Equal(t, want, heartbeatTNCommands(t, rsm, store))
				require.Equal(t, retry, heartbeatTNCommands(t, rsm, store))
			}
		})
	}
	// Check repeated checker intents do not accumulate history or old targets.
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.State = pb.HAKeeperRunning
	for _, command := range []pb.ScheduleCommand{start, later, moved, start} {
		updateTNCommands(t, rsm, 1, command)
		require.Len(t, rsm.state.ScheduleCommands, 1)
		require.Equal(t, []pb.ScheduleCommand{command}, rsm.state.ScheduleCommands[command.UUID].Commands)
	}
}

func TestTNStartFreshReportCancelsObsoleteReplica(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.State = pb.HAKeeperRunning
	// A new accepted start is not dropped using an older stored report.
	require.Empty(t, heartbeatTNCommands(t, rsm, "tn-1", pb.TNShardInfo{ShardID: 2, ReplicaID: 8}))
	start := dispatchedTNStart(t, "tn-1", 2, 10)
	updateTNCommands(t, rsm, 1, start)
	require.Equal(t, []pb.ScheduleCommand{start}, rsm.state.ScheduleCommands["tn-1"].Commands)
	// TN cannot replace a local shard by replica ID. Its fresh started report
	// cancels the obsolete request rather than falsely acknowledging ID 10.
	require.Empty(t, heartbeatTNCommands(t, rsm, "tn-1", pb.TNShardInfo{ShardID: 2, ReplicaID: 8}))
	require.Empty(t, rsm.state.ScheduleCommands)
}

func TestTNStartRejectedUpdatesLeavePendingIntent(t *testing.T) {
	for _, rejection := range []string{"stale term", "recovery barrier", "bootstrap flag"} {
		t.Run(rejection, func(t *testing.T) {
			rsm := NewStateMachine(0, 1).(*stateMachine)
			rsm.state.State = pb.HAKeeperRunning
			start := dispatchedTNStart(t, "tn-1", 2, 10)
			updateTNCommands(t, rsm, 2, start)
			replacement := dispatchedTNStart(t, "tn-2", 2, 11)
			term := uint64(2)
			switch rejection {
			case "stale term":
				term = 1
			case "recovery barrier":
				rsm.state.LogServiceRecoveryPending = true
			case "bootstrap flag":
				replacement.Bootstrapping = true
			}
			updateTNCommands(t, rsm, term, replacement)
			require.Equal(t, uint64(2), rsm.state.Term)
			require.Equal(t, pb.HAKeeperRunning, rsm.state.State)
			require.Equal(t, []pb.ScheduleCommand{start}, heartbeatTNCommands(t, rsm, "tn-1"))
			require.Empty(t, heartbeatTNCommands(t, rsm, "tn-2"))
		})
	}
}

func TestTNStartCarryDoesNotRepeatBootstrapTransition(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.State = pb.HAKeeperBootstrapping
	start := dispatchedTNStart(t, "tn-1", 2, 10)
	start.Bootstrapping = true
	updateTNCommands(t, rsm, 1, start)
	require.Equal(t, pb.HAKeeperBootstrapCommandsReceived, rsm.state.State)
	updateTNCommands(t, rsm, 1)
	require.Equal(t, pb.HAKeeperBootstrapCommandsReceived, rsm.state.State)
	require.Equal(t, []pb.ScheduleCommand{start}, heartbeatTNCommands(t, rsm, "tn-1"))
}

func TestTNStartNormalizesRestoredDuplicateTargets(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	first := dispatchedTNStart(t, "tn-1", 2, 10)
	last := dispatchedTNStart(t, "tn-2", 2, 9)
	// Snapshots predate normalization and do not preserve cross-store order.
	rsm.state.ScheduleCommands[first.UUID] = pb.CommandBatch{Commands: []pb.ScheduleCommand{first}}
	rsm.state.ScheduleCommands[last.UUID] = pb.CommandBatch{Commands: []pb.ScheduleCommand{last}}
	updateTNCommands(t, rsm, 1)
	require.Len(t, rsm.state.ScheduleCommands, 1)
	require.Equal(t, []pb.ScheduleCommand{last}, heartbeatTNCommands(t, rsm, last.UUID))
	require.Empty(t, heartbeatTNCommands(t, rsm, first.UUID))
}

func TestReplicaStartLogServiceControls(t *testing.T) {
	for _, tc := range []struct {
		name           string
		bootstrap      bool
		shard, replica uint64
		wantRetry      bool
	}{
		{"ordinary start is one-shot", false, 0, 0, false},
		{"bootstrap waits for shard", true, 0, 0, true},
		{"wrong shard", true, 3, 10, true},
		{"wrong replica", true, 2, 9, true},
		{"exact replica", true, 2, 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rsm := NewStateMachine(0, 1).(*stateMachine)
			rsm.state.State = pb.HAKeeperBootstrapping
			command := pb.ScheduleCommand{UUID: "log-1", ServiceType: pb.LogService, Bootstrapping: tc.bootstrap,
				ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{ShardID: 2, ReplicaID: 10}}}
			updateTNCommands(t, rsm, 1, command)
			data, err := (&pb.LogStoreHeartbeat{UUID: command.UUID, Replicas: []pb.LogReplicaInfo{{
				LogShardInfo: pb.LogShardInfo{ShardID: tc.shard}, ReplicaID: tc.replica,
			}}}).Marshal()
			require.NoError(t, err)
			result, err := rsm.Update(sm.Entry{Cmd: GetLogStoreHeartbeatCmd(data)})
			require.NoError(t, err)
			var delivered pb.CommandBatch
			require.NoError(t, delivered.Unmarshal(result.Data))
			if tc.bootstrap && tc.shard == 2 && tc.replica == 10 {
				require.Empty(t, delivered.Commands)
			} else {
				require.Equal(t, []pb.ScheduleCommand{command}, delivered.Commands)
			}
			_, pending := rsm.state.ScheduleCommands[command.UUID]
			require.Equal(t, tc.wantRetry, pending)
		})
	}
}
