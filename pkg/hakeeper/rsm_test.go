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

package hakeeper

import (
	"bytes"
	"sort"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

func TestAssignID(t *testing.T) {
	tsm := NewStateMachine(0, 1).(*stateMachine)
	assert.Equal(t, uint64(0), tsm.state.NextID)
	assert.Equal(t, uint64(1), tsm.assignID())
	assert.Equal(t, uint64(1), tsm.state.NextID)
}

func TestHAKeeperStateMachineCanBeCreated(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to panic")
		}
	}()
	tsm := NewStateMachine(0, 1).(*stateMachine)
	assert.Equal(t, uint64(1), tsm.replicaID)
	NewStateMachine(1, 1)
}

func TestHAKeeperStateMachineSnapshot(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	tsm2 := NewStateMachine(0, 2).(*stateMachine)
	tsm1.state.NextID = 12345
	tsm1.state.LogShards["test1"] = 23456
	tsm1.state.LogShards["test2"] = 34567

	buf := bytes.NewBuffer(nil)
	assert.Nil(t, tsm1.SaveSnapshot(buf, nil, nil))
	assert.Nil(t, tsm2.RecoverFromSnapshot(buf, nil, nil))
	assert.Equal(t, tsm1.state.NextID, tsm2.state.NextID)
	assert.Equal(t, tsm1.state.LogShards, tsm2.state.LogShards)
	assert.True(t, tsm1.replicaID != tsm2.replicaID)
}

func TestHAKeeperCanBeClosed(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	assert.Nil(t, tsm1.Close())
}

func TestHAKeeperTick(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	assert.Equal(t, uint64(0), tsm1.state.Tick)
	cmd := GetTickCmd()
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), tsm1.state.Tick)
}

func TestHandleLogHeartbeat(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	cmd := GetTickCmd()
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.LogStoreHeartbeat{
		UUID:           "uuid1",
		RaftAddress:    "localhost:9090",
		ServiceAddress: "localhost:9091",
		GossipAddress:  "localhost:9092",
		Replicas: []pb.LogReplicaInfo{
			{
				LogShardInfo: pb.LogShardInfo{
					ShardID: 100,
					Replicas: map[uint64]string{
						200: "localhost:8000",
						300: "localhost:9000",
					},
					Epoch:    200,
					LeaderID: 200,
					Term:     10,
				},
			},
			{
				LogShardInfo: pb.LogShardInfo{
					ShardID: 101,
					Replicas: map[uint64]string{
						201: "localhost:8000",
						301: "localhost:9000",
					},
					Epoch:    202,
					LeaderID: 201,
					Term:     30,
				},
			},
		},
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetLogStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s := tsm1.state.LogState
	assert.Equal(t, 1, len(s.Stores))
	lsinfo, ok := s.Stores[hb.UUID]
	require.True(t, ok)
	assert.Equal(t, uint64(3), lsinfo.Tick)
	assert.Equal(t, hb.RaftAddress, lsinfo.RaftAddress)
	assert.Equal(t, hb.ServiceAddress, lsinfo.ServiceAddress)
	assert.Equal(t, hb.GossipAddress, lsinfo.GossipAddress)
	assert.Equal(t, 2, len(lsinfo.Replicas))
	assert.Equal(t, hb.Replicas, lsinfo.Replicas)
}

func TestHandleDNHeartbeat(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	cmd := GetTickCmd()
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.DNStoreHeartbeat{
		UUID: "uuid1",
		Shards: []pb.DNShardInfo{
			{ShardID: 1, ReplicaID: 1},
			{ShardID: 2, ReplicaID: 1},
			{ShardID: 3, ReplicaID: 1},
		},
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetDNStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s := tsm1.state.DNState
	assert.Equal(t, 1, len(s.Stores))
	dninfo, ok := s.Stores[hb.UUID]
	assert.True(t, ok)
	assert.Equal(t, uint64(3), dninfo.Tick)
	require.Equal(t, 3, len(dninfo.Shards))
	assert.Equal(t, hb.Shards, dninfo.Shards)
}

func TestHandleCNHeartbeat(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	cmd := GetTickCmd()
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.CNStoreHeartbeat{
		UUID: "uuid1",
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetCNStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s := tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	cninfo, ok := s.Stores[hb.UUID]
	assert.True(t, ok)
	assert.Equal(t, uint64(3), cninfo.Tick)
}

func TestGetIDCmd(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	tsm1.state.State = pb.HAKeeperRunning
	cmd := GetGetIDCmd(100)
	result, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 1}, result)
	result, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 101}, result)
	assert.Equal(t, uint64(201), tsm1.assignID())

	result, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 202}, result)
}

func TestUpdateScheduleCommandsCmd(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	sc1 := pb.ScheduleCommand{
		UUID: "uuid1",
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				ShardID: 1,
			},
		},
	}
	sc2 := pb.ScheduleCommand{
		UUID: "uuid2",
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				ShardID: 2,
			},
		},
	}
	sc3 := pb.ScheduleCommand{
		UUID: "uuid1",
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				ShardID: 3,
			},
		},
	}
	sc4 := pb.ScheduleCommand{
		UUID: "uuid3",
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				ShardID: 4,
			},
		},
	}

	b := pb.CommandBatch{
		Term:     101,
		Commands: []pb.ScheduleCommand{sc1, sc2, sc3},
	}
	cmd := GetUpdateCommandsCmd(b.Term, b.Commands)
	result, err := tsm1.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, b.Term, tsm1.state.Term)
	require.Equal(t, 2, len(tsm1.state.ScheduleCommands))
	l1, ok := tsm1.state.ScheduleCommands["uuid1"]
	assert.True(t, ok)
	assert.Equal(t, pb.CommandBatch{Commands: []pb.ScheduleCommand{sc1, sc3}}, l1)
	l2, ok := tsm1.state.ScheduleCommands["uuid2"]
	assert.True(t, ok)
	assert.Equal(t, pb.CommandBatch{Commands: []pb.ScheduleCommand{sc2}}, l2)

	cmd2 := GetUpdateCommandsCmd(b.Term-1,
		[]pb.ScheduleCommand{sc1, sc2, sc3, sc4})
	result, err = tsm1.Update(sm.Entry{Cmd: cmd2})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, b.Term, tsm1.state.Term)
	require.Equal(t, 2, len(tsm1.state.ScheduleCommands))
	l1, ok = tsm1.state.ScheduleCommands["uuid1"]
	assert.True(t, ok)
	assert.Equal(t, pb.CommandBatch{Commands: []pb.ScheduleCommand{sc1, sc3}}, l1)
	l2, ok = tsm1.state.ScheduleCommands["uuid2"]
	assert.True(t, ok)
	assert.Equal(t, pb.CommandBatch{Commands: []pb.ScheduleCommand{sc2}}, l2)
}

func TestScheduleCommandQuery(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	sc1 := pb.ScheduleCommand{
		UUID: "uuid1",
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				ShardID: 1,
			},
		},
	}
	sc2 := pb.ScheduleCommand{
		UUID: "uuid2",
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				ShardID: 2,
			},
		},
	}
	sc3 := pb.ScheduleCommand{
		UUID: "uuid1",
		ConfigChange: &pb.ConfigChange{
			Replica: pb.Replica{
				ShardID: 3,
			},
		},
	}
	b := pb.CommandBatch{
		Term:     101,
		Commands: []pb.ScheduleCommand{sc1, sc2, sc3},
	}
	cmd := GetUpdateCommandsCmd(b.Term, b.Commands)
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	r, err := tsm1.Lookup(&ScheduleCommandQuery{UUID: "uuid1"})
	require.NoError(t, err)
	cb, ok := r.(*pb.CommandBatch)
	require.True(t, ok)
	assert.Equal(t, 2, len(cb.Commands))
	b = pb.CommandBatch{
		Commands: []pb.ScheduleCommand{sc1, sc3},
	}
	assert.Equal(t, b, *cb)
}

func TestClusterDetailsQuery(t *testing.T) {
	tsm := NewStateMachine(0, 1).(*stateMachine)
	tsm.state.CNState = pb.CNState{
		Stores: make(map[string]pb.CNStoreInfo),
	}
	tsm.state.CNState.Stores["uuid1"] = pb.CNStoreInfo{
		Tick:           1,
		ServiceAddress: "addr1",
	}
	tsm.state.CNState.Stores["uuid2"] = pb.CNStoreInfo{
		Tick:           2,
		ServiceAddress: "addr2",
	}
	tsm.state.DNState = pb.DNState{
		Stores: make(map[string]pb.DNStoreInfo),
	}
	tsm.state.DNState.Stores["uuid3"] = pb.DNStoreInfo{
		Tick:           3,
		ServiceAddress: "addr3",
	}
	tsm.state.LogState.Shards[1] = pb.LogShardInfo{
		ShardID:  1,
		Replicas: map[uint64]string{1: "store-1", 2: "store-2", 3: "store-3"},
		Epoch:    1, LeaderID: 1, Term: 1,
	}

	tsm.state.LogState.Stores["store-1"] = pb.LogStoreInfo{
		Tick:           100,
		ServiceAddress: "addr-log-1",
		Replicas: []pb.LogReplicaInfo{{
			LogShardInfo: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "store-1", 2: "store-2", 3: "store-3"},
				Epoch:    1, LeaderID: 1, Term: 1,
			}, ReplicaID: 1,
		}},
	}

	tsm.state.LogState.Stores["store-2"] = pb.LogStoreInfo{
		Tick:           100,
		ServiceAddress: "addr-log-2",
		Replicas: []pb.LogReplicaInfo{{
			LogShardInfo: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "store-1", 2: "store-2", 3: "store-3"},
				Epoch:    1, LeaderID: 1, Term: 1,
			}, ReplicaID: 2,
		}},
	}

	tsm.state.LogState.Stores["store-3"] = pb.LogStoreInfo{
		Tick:           100,
		ServiceAddress: "addr-log-3",
		Replicas: []pb.LogReplicaInfo{{
			LogShardInfo: pb.LogShardInfo{
				ShardID:  1,
				Replicas: map[uint64]string{1: "store-1", 2: "store-2", 3: "store-3"},
				Epoch:    1, LeaderID: 1, Term: 1,
			}, ReplicaID: 3,
		}},
	}

	v, err := tsm.Lookup(&ClusterDetailsQuery{})
	require.NoError(t, err)
	expected := &pb.ClusterDetails{
		DNNodes: []pb.DNNode{
			{
				UUID:           "uuid3",
				Tick:           3,
				ServiceAddress: "addr3",
			},
		},
		CNNodes: []pb.CNNode{
			{
				UUID:           "uuid1",
				Tick:           1,
				ServiceAddress: "addr1",
			},
			{
				UUID:           "uuid2",
				Tick:           2,
				ServiceAddress: "addr2",
			},
		},
		LogNodes: []pb.LogNode{
			{
				UUID:           "store-1",
				ServiceAddress: "addr-log-1",
				Tick:           100,
				State:          0,
				Replicas: []pb.LogReplicaInfo{{
					LogShardInfo: pb.LogShardInfo{
						ShardID:  1,
						Replicas: map[uint64]string{1: "store-1", 2: "store-2", 3: "store-3"},
						Epoch:    1, LeaderID: 1, Term: 1,
					}, ReplicaID: 1,
				}},
			},
			{
				UUID:           "store-2",
				ServiceAddress: "addr-log-2",
				Tick:           100,
				State:          0,
				Replicas: []pb.LogReplicaInfo{{
					LogShardInfo: pb.LogShardInfo{
						ShardID:  1,
						Replicas: map[uint64]string{1: "store-1", 2: "store-2", 3: "store-3"},
						Epoch:    1, LeaderID: 1, Term: 1,
					}, ReplicaID: 2,
				}},
			},
			{
				UUID:           "store-3",
				ServiceAddress: "addr-log-3",
				Tick:           100,
				State:          0,
				Replicas: []pb.LogReplicaInfo{{
					LogShardInfo: pb.LogShardInfo{
						ShardID:  1,
						Replicas: map[uint64]string{1: "store-1", 2: "store-2", 3: "store-3"},
						Epoch:    1, LeaderID: 1, Term: 1,
					}, ReplicaID: 3,
				}},
			},
		},
	}
	result := v.(*pb.ClusterDetails)
	sort.Slice(result.CNNodes, func(i, j int) bool {
		return result.CNNodes[i].UUID < result.CNNodes[j].UUID
	})
	sort.Slice(result.LogNodes, func(i, j int) bool {
		return result.LogNodes[i].UUID < result.LogNodes[j].UUID
	})
	assert.Equal(t, expected, result)
}

func TestInitialState(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	assert.Equal(t, pb.HAKeeperCreated, rsm.state.State)
}

func TestSetState(t *testing.T) {
	tests := []struct {
		initialState pb.HAKeeperState
		newState     pb.HAKeeperState
		result       pb.HAKeeperState
	}{
		{pb.HAKeeperCreated, pb.HAKeeperBootstrapping, pb.HAKeeperCreated},
		{pb.HAKeeperCreated, pb.HAKeeperBootstrapFailed, pb.HAKeeperCreated},
		{pb.HAKeeperCreated, pb.HAKeeperRunning, pb.HAKeeperCreated},
		{pb.HAKeeperCreated, pb.HAKeeperCreated, pb.HAKeeperCreated},
		{pb.HAKeeperCreated, pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperCreated},

		{pb.HAKeeperBootstrapping, pb.HAKeeperCreated, pb.HAKeeperBootstrapping},
		{pb.HAKeeperBootstrapping, pb.HAKeeperBootstrapFailed, pb.HAKeeperBootstrapping},
		{pb.HAKeeperBootstrapping, pb.HAKeeperRunning, pb.HAKeeperBootstrapping},
		{pb.HAKeeperBootstrapping, pb.HAKeeperBootstrapping, pb.HAKeeperBootstrapping},
		{pb.HAKeeperBootstrapping, pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperBootstrapCommandsReceived},

		{pb.HAKeeperBootstrapFailed, pb.HAKeeperBootstrapFailed, pb.HAKeeperBootstrapFailed},
		{pb.HAKeeperBootstrapFailed, pb.HAKeeperCreated, pb.HAKeeperBootstrapFailed},
		{pb.HAKeeperBootstrapFailed, pb.HAKeeperBootstrapping, pb.HAKeeperBootstrapFailed},
		{pb.HAKeeperBootstrapFailed, pb.HAKeeperRunning, pb.HAKeeperBootstrapFailed},
		{pb.HAKeeperBootstrapFailed, pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperBootstrapFailed},

		{pb.HAKeeperRunning, pb.HAKeeperRunning, pb.HAKeeperRunning},
		{pb.HAKeeperRunning, pb.HAKeeperCreated, pb.HAKeeperRunning},
		{pb.HAKeeperRunning, pb.HAKeeperBootstrapping, pb.HAKeeperRunning},
		{pb.HAKeeperRunning, pb.HAKeeperBootstrapFailed, pb.HAKeeperRunning},
		{pb.HAKeeperRunning, pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperRunning},

		{pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperCreated, pb.HAKeeperBootstrapCommandsReceived},
		{pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperBootstrapping, pb.HAKeeperBootstrapCommandsReceived},
		{pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperBootstrapCommandsReceived},
		{pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperBootstrapFailed, pb.HAKeeperBootstrapFailed},
		{pb.HAKeeperBootstrapCommandsReceived, pb.HAKeeperRunning, pb.HAKeeperRunning},
	}

	for _, tt := range tests {
		rsm := stateMachine{
			state: pb.HAKeeperRSMState{
				State: tt.initialState,
			},
		}
		cmd := GetSetStateCmd(tt.newState)
		_, err := rsm.Update(sm.Entry{Cmd: cmd})
		require.NoError(t, err)
		assert.Equal(t, tt.result, rsm.state.State)
	}
}

func TestInitialClusterRequestCmd(t *testing.T) {
	cmd := GetInitialClusterRequestCmd(2, 2, 3)
	req := parseInitialClusterRequestCmd(cmd)
	assert.Equal(t, uint64(2), req.NumOfLogShards)
	assert.Equal(t, uint64(2), req.NumOfDNShards)
	assert.Equal(t, uint64(3), req.NumOfLogReplicas)
}

func TestHandleInitialClusterRequestCmd(t *testing.T) {
	cmd := GetInitialClusterRequestCmd(2, 2, 3)
	rsm := NewStateMachine(0, 1).(*stateMachine)
	result, err := rsm.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 0}, result)

	expected := pb.ClusterInfo{
		LogShards: []metadata.LogShardRecord{
			{
				ShardID:          0,
				NumberOfReplicas: 3,
			},
			{
				ShardID:          1,
				NumberOfReplicas: 3,
			},
			{
				ShardID:          3,
				NumberOfReplicas: 3,
			},
		},
		DNShards: []metadata.DNShardRecord{
			{
				ShardID:    2,
				LogShardID: 1,
			},
			{
				ShardID:    4,
				LogShardID: 3,
			},
		},
	}

	assert.Equal(t, expected, rsm.state.ClusterInfo)
	assert.Equal(t, pb.HAKeeperBootstrapping, rsm.state.State)
	assert.Equal(t, K8SIDRangeEnd, rsm.state.NextID)
}
