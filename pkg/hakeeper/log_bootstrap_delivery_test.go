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

package hakeeper

import (
	"testing"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

func newPendingBootstrapState(t *testing.T, commandDelivery bool) (*stateMachine, map[uint64]string) {
	t.Helper()
	s := NewStateMachine(0, 1).(*stateMachine)
	installMembershipPolicy(t, s, 100)
	members := map[uint64]string{11: "log-a", 12: "log-b", 13: "log-c"}
	s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 77, NumberOfReplicas: 3}}
	for _, storeID := range members {
		s.state.LogState.Stores[storeID] = pb.LogStoreInfo{StoreIncarnation: "inc-" + storeID}
	}
	s.state.CommandDeliveryEnabled = commandDelivery
	return s, members
}

func pendingBootstrapCommand(storeID string, replicaID uint64, members map[uint64]string) pb.ScheduleCommand {
	return pb.ScheduleCommand{
		UUID:        storeID,
		ServiceType: pb.LogService,
		BootstrapShard: &pb.BootstrapShard{
			ShardID:        77,
			ReplicaID:      replicaID,
			InitialMembers: members,
		},
	}
}

func TestPendingBootstrapRedeliversLostResponses(t *testing.T) {
	for _, mode := range []string{"legacy", "preparing", "enabled"} {
		t.Run(mode, func(t *testing.T) {
			s, members := newPendingBootstrapState(t, false)
			s.state.CommandDeliveryPreparing = mode == "preparing"
			s.state.CommandDeliveryEnabled = mode == "enabled"
			a := pendingBootstrapCommand("log-a", 11, members)
			b := pendingBootstrapCommand("log-b", 12, members)
			c := pendingBootstrapCommand("log-c", 13, members)
			unhappyQueue(t, s, a, b, c)
			heartbeat := func(cmd pb.ScheduleCommand, replicas ...pb.LogReplicaInfo) pb.CommandBatch {
				t.Helper()
				return membershipHeartbeat(t, s, pb.LogStoreHeartbeat{
					UUID: cmd.UUID, StoreIncarnation: "inc-" + cmd.UUID, Replicas: replicas,
				}, true)
			}
			// Both responses commit and are lost before either recipient starts.
			// The third member is unavailable and sends no heartbeat.
			firstA, firstB := heartbeat(a), heartbeat(b)
			require.Equal(t, []pb.ScheduleCommand{a}, firstA.Commands)
			require.Equal(t, []pb.ScheduleCommand{b}, firstB.Commands)
			if mode != "legacy" {
				require.Len(t, firstA.CommandIDs, 1)
				require.Len(t, firstB.CommandIDs, 1)
			}

			s = runtimeRestore(t, s)
			tnTask := pb.ScheduleCommand{UUID: "tn-a", ServiceType: pb.TNService,
				CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}}
			unhappyQueue(t, s, tnTask)
			require.Equal(t, []pb.ScheduleCommand{tnTask}, s.state.ScheduleCommands[tnTask.UUID].Commands)
			require.Equal(t, firstA, heartbeat(a))
			require.Equal(t, firstB, heartbeat(b))

			// A completed voter must not acknowledge B. Even a B report with
			// the right shard but wrong ID or local role does not acknowledge B.
			prefix := pb.LogShardInfo{ShardID: 77, Epoch: 1, Replicas: map[uint64]string{11: "log-a"}}
			require.Empty(t, heartbeat(a, pb.LogReplicaInfo{LogShardInfo: prefix, ReplicaID: 11}).Commands)
			require.NotContains(t, s.state.ScheduleCommands, a.UUID)
			require.Equal(t, firstB, heartbeat(b, pb.LogReplicaInfo{LogShardInfo: prefix, ReplicaID: 12}))
			shard := pb.LogShardInfo{ShardID: 77, Epoch: 3, Replicas: members}
			require.Equal(t, firstB, heartbeat(b, pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 11}))
			require.Equal(t, firstB, heartbeat(b, pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 12, IsNonVoting: true}))
			require.Empty(t, heartbeat(b, pb.LogReplicaInfo{LogShardInfo: shard, ReplicaID: 12}).Commands)
			require.NotContains(t, s.state.ScheduleCommands, b.UUID)
			require.Equal(t, []pb.ScheduleCommand{c}, s.state.ScheduleCommands[c.UUID].Commands)
		})
	}
}

func TestGuardedBootstrapBeforePolicyTick(t *testing.T) {
	s := NewStateMachine(0, 1).(*stateMachine)
	members := map[uint64]string{11: "log-a", 12: "log-b", 13: "log-c"}
	s.state.ClusterInfo.LogShards = []metadata.LogShardRecord{{ShardID: 77, NumberOfReplicas: 3}}
	a := pendingBootstrapCommand("log-a", 11, members)
	b := pendingBootstrapCommand("log-b", 12, members)
	unhappyQueue(t, s, a, b)
	require.Zero(t, s.state.LogStoreTimeoutPolicyTerm)

	require.Equal(t, []pb.ScheduleCommand{a}, membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: a.UUID}, true).Commands)
	require.Equal(t, []pb.ScheduleCommand{a}, s.state.ScheduleCommands[a.UUID].Commands)
	require.Equal(t, []pb.ScheduleCommand{b}, membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: b.UUID}, false).Commands)
	require.NotContains(t, s.state.ScheduleCommands, b.UUID, "released unguarded entries still consume dynamic bootstrap")

	s = runtimeRestore(t, s)
	installMembershipPolicy(t, s, 100)
	unhappyQueue(t, s, pb.ScheduleCommand{UUID: "tn-a", ServiceType: pb.TNService,
		CreateTaskService: &pb.CreateTaskService{TaskDatabase: "task"}})
	require.Equal(t, []pb.ScheduleCommand{a}, membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: a.UUID}, true).Commands)
}

func TestPendingBootstrapAdmitsOnlyMissingTargetsOfSameCohort(t *testing.T) {
	s, members := newPendingBootstrapState(t, true)
	a := pendingBootstrapCommand("log-a", 11, members)
	b := pendingBootstrapCommand("log-b", 12, members)
	c := pendingBootstrapCommand("log-c", 13, members)
	competing := pendingBootstrapCommand("log-d", 14, map[uint64]string{11: "log-a", 12: "log-b", 14: "log-d"})
	unhappyQueue(t, s, a, competing, b, c)
	require.NotContains(t, s.state.ScheduleCommands, competing.UUID, "a competitor in the same proposal must not create a second cohort")
	originalC := s.state.ScheduleCommands[c.UUID]
	require.Len(t, originalC.CommandIDs, 1)

	// Model a historical entry consuming A and B's responses before the
	// new guard is running. C still has the original committed cohort.
	for _, cmd := range []pb.ScheduleCommand{a, b} {
		require.Equal(t, []pb.ScheduleCommand{cmd}, membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: cmd.UUID}, false).Commands)
		require.NotContains(t, s.state.ScheduleCommands, cmd.UUID)
	}
	s = runtimeRestore(t, s)
	unhappyQueue(t, s, competing, a, b, c, b)
	require.NotContains(t, s.state.ScheduleCommands, competing.UUID)
	require.Equal(t, []pb.ScheduleCommand{a}, s.state.ScheduleCommands[a.UUID].Commands)
	require.Equal(t, []pb.ScheduleCommand{b}, s.state.ScheduleCommands[b.UUID].Commands)
	require.Equal(t, originalC, s.state.ScheduleCommands[c.UUID])

	shard := pb.LogShardInfo{ShardID: 77, Epoch: 3, Replicas: members}
	require.Empty(t, membershipHeartbeat(t, s, pb.LogStoreHeartbeat{
		UUID: a.UUID, Replicas: []pb.LogReplicaInfo{{LogShardInfo: shard, ReplicaID: 11}},
	}, true).Commands)
	unhappyQueue(t, s, a)
	require.NotContains(t, s.state.ScheduleCommands, a.UUID, "an exact already-reported target requires no retry")
}

func TestPendingBootstrapSurvivesInitialRaftMembershipPrefix(t *testing.T) {
	for _, tc := range []struct {
		name          string
		epoch         uint64
		members       map[uint64]string
		retry         bool
		noQueuedClaim bool
		reportByPeer  bool
	}{
		{name: "initial prefix", epoch: 1, members: map[uint64]string{7: "log-a"}, retry: true},
		{name: "later removal", epoch: 5, members: map[uint64]string{7: "log-a"}},
		{name: "not in initial order", epoch: 2, members: map[uint64]string{7: "log-a", 29: "log-b"}},
		{name: "conflicting store", epoch: 1, members: map[uint64]string{7: "log-d"}},
		{name: "later same voter map supersedes a queued genesis", epoch: 7,
			members: map[uint64]string{7: "log-a", 29: "log-b", 13: "log-c"}, reportByPeer: true},
		{name: "prefix alone cannot authorize bootstrap", epoch: 1, members: map[uint64]string{7: "log-a"}, noQueuedClaim: true},
		{name: "later matching map alone cannot authorize bootstrap", epoch: 7,
			members: map[uint64]string{7: "log-a", 29: "log-b", 13: "log-c"}, noQueuedClaim: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newPendingBootstrapState(t, false)
			b := pendingBootstrapCommand("log-b", 29, map[uint64]string{7: "log-a", 29: "log-b", 13: "log-c"})
			if !tc.noQueuedClaim {
				unhappyQueue(t, s, b)
			}
			// Dragonboat can report a local ID before its AddNode entry is
			// applied, with an initial configuration that does not contain it.
			hb := pb.LogStoreHeartbeat{UUID: b.UUID, StoreIncarnation: "inc-log-b", Replicas: []pb.LogReplicaInfo{{
				LogShardInfo: pb.LogShardInfo{ShardID: 77, Epoch: tc.epoch, Replicas: tc.members}, ReplicaID: 29,
			}}}
			if tc.noQueuedClaim || tc.reportByPeer {
				// A reports the shard. A's map includes B in the full-map case,
				// but there is neither a local B report nor a queue claim for it.
				hb.UUID, hb.StoreIncarnation, hb.Replicas[0].ReplicaID = "log-a", "inc-log-a", 7
			}
			got := membershipHeartbeat(t, s, hb, true)
			if tc.reportByPeer {
				current := pb.ScheduleCommand{UUID: b.UUID, ServiceType: pb.LogService,
					ConfigChange: &pb.ConfigChange{ChangeType: pb.StartReplica, Replica: pb.Replica{
						ShardID: 77, ReplicaID: 29, UUID: b.UUID, Epoch: tc.epoch,
					}}}
				unhappyQueue(t, s, current)
				got = membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: b.UUID}, true)
				require.Equal(t, []pb.ScheduleCommand{current}, got.Commands)
				require.NotContains(t, s.state.ScheduleCommands, b.UUID)
				return
			}
			if tc.noQueuedClaim {
				unhappyQueue(t, s, b)
			}
			if tc.retry {
				require.Equal(t, []pb.ScheduleCommand{b}, got.Commands)
				require.Equal(t, []pb.ScheduleCommand{b}, s.state.ScheduleCommands[b.UUID].Commands)
			} else {
				require.Empty(t, got.Commands)
				require.NotContains(t, s.state.ScheduleCommands, b.UUID)
			}
		})
	}
}
func TestInitialBootstrapPrefixRejectsUnneededReplacementAdd(t *testing.T) {
	s, members := newPendingBootstrapState(t, true)
	a := pendingBootstrapCommand("log-a", 11, members)
	b := pendingBootstrapCommand("log-b", 12, members)
	c := pendingBootstrapCommand("log-c", 13, members)
	unhappyQueue(t, s, a, b, c)
	prefix := pb.LogShardInfo{ShardID: 77, Epoch: 2, Replicas: map[uint64]string{11: a.UUID, 12: b.UUID}}
	report := func(cmd pb.ScheduleCommand, id uint64) pb.LogStoreHeartbeat {
		return pb.LogStoreHeartbeat{UUID: cmd.UUID, StoreIncarnation: "inc-" + cmd.UUID,
			Replicas: []pb.LogReplicaInfo{{LogShardInfo: prefix, ReplicaID: id}}}
	}
	require.Empty(t, membershipHeartbeat(t, s, report(a, 11), true).Commands)
	require.Empty(t, membershipHeartbeat(t, s, report(b, 12), true).Commands)
	membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: "replacement"}, true)
	add := pb.ScheduleCommand{UUID: a.UUID, ServiceType: pb.LogService, ConfigChange: &pb.ConfigChange{
		ChangeType: pb.AddReplica, Replica: pb.Replica{ShardID: 77, ReplicaID: 14, UUID: "replacement", Epoch: 2},
	}}
	unhappyQueue(t, s, add)
	require.Empty(t, membershipHeartbeat(t, s, report(a, 11), true).Commands,
		"do not allocate an extra voter while initial Raft membership is still applying")
	require.Equal(t, []pb.ScheduleCommand{c}, membershipHeartbeat(t, s,
		pb.LogStoreHeartbeat{UUID: c.UUID}, true).Commands)
}

func TestDynamicBootstrapDoesNotFollowReplacedStoreDisk(t *testing.T) {
	s, members := newPendingBootstrapState(t, true)
	a := pendingBootstrapCommand("log-a", 11, members)
	b := pendingBootstrapCommand("log-b", 12, members)
	c := pendingBootstrapCommand("log-c", 13, members)
	older := pb.ScheduleCommand{UUID: c.UUID, ServiceType: pb.LogService,
		CreateTaskService: &pb.CreateTaskService{TaskDatabase: "before"}}
	newer := pb.ScheduleCommand{UUID: c.UUID, ServiceType: pb.LogService,
		CreateTaskService: &pb.CreateTaskService{TaskDatabase: "after"}}
	unhappyQueue(t, s, a, b, older, c, newer)
	original := s.state.ScheduleCommands[c.UUID]
	require.Len(t, original.CommandIDs, 3)
	require.Zero(t, s.state.LogState.Shards[77].Epoch, "disk replacement predates any native voter report")
	s = runtimeRestore(t, s)
	got := membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: c.UUID, StoreIncarnation: "replacement-disk"}, true)
	require.Equal(t, []pb.ScheduleCommand{older, newer}, got.Commands)
	require.Equal(t, []pb.ScheduleCommandID{original.CommandIDs[0], original.CommandIDs[2]}, got.CommandIDs)
	for _, want := range []pb.ScheduleCommand{a, b} {
		got := membershipHeartbeat(t, s, pb.LogStoreHeartbeat{UUID: want.UUID, StoreIncarnation: "inc-" + want.UUID}, true)
		require.Equal(t, []pb.ScheduleCommand{want}, got.Commands)
	}
}
