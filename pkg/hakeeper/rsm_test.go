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
	"time"

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
	tsm1.state.LogServiceRecoveryPending = true
	tsm1.state.IDWatermarkRestoreGeneration = 7
	tsm1.state.LogShards["test1"] = 23456
	tsm1.state.LogShards["test2"] = 34567
	tsm1.state.CommandDeliveryEnabled = true
	tsm1.state.CommandDeliveryBatchIDsAssigned = true
	tsm1.state.CommandDeliveryCommandIDsAssigned = true
	tsm1.state.ScheduleCommands["tn-1"] = pb.CommandBatch{
		BatchID:    9,
		Commands:   []pb.ScheduleCommand{{UUID: "tn-1", ServiceType: pb.TNService}},
		CommandIDs: []pb.ScheduleCommandID{{OriginBatchID: 8, CommandIndex: 1}},
	}

	buf := bytes.NewBuffer(nil)
	assert.Nil(t, tsm1.SaveSnapshot(buf, nil, nil))
	assert.Nil(t, tsm2.RecoverFromSnapshot(buf, nil, nil))
	assert.Equal(t, tsm1.state.NextID, tsm2.state.NextID)
	assert.Equal(t, tsm1.state.LogShards, tsm2.state.LogShards)
	assert.True(t, tsm2.state.LogServiceRecoveryPending)
	assert.Equal(t, uint64(7), tsm2.state.IDWatermarkRestoreGeneration)
	assert.True(t, tsm2.state.CommandDeliveryEnabled)
	assert.True(t, tsm2.state.CommandDeliveryCommandIDsAssigned)
	assert.Equal(t, tsm1.state.ScheduleCommands, tsm2.state.ScheduleCommands)
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

func TestHandleTNHeartbeat(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	cmd := GetTickCmd()
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.TNStoreHeartbeat{
		UUID: "uuid1",
		Shards: []pb.TNShardInfo{
			{ShardID: 1, ReplicaID: 1},
			{ShardID: 2, ReplicaID: 1},
			{ShardID: 3, ReplicaID: 1},
		},
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetTNStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s := tsm1.state.TNState
	assert.Equal(t, 1, len(s.Stores))
	tninfo, ok := s.Stores[hb.UUID]
	assert.True(t, ok)
	assert.Equal(t, uint64(3), tninfo.Tick)
	require.Equal(t, 3, len(tninfo.Shards))
	assert.Equal(t, hb.Shards, tninfo.Shards)
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
		UUID:     "uuid1",
		CommitID: "c123",
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
	assert.Equal(t, hb.CommitID, cninfo.CommitID)
}

func TestGetIDCmd(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	tsm1.state.State = pb.HAKeeperRunning
	cmd := GetAllocateIDCmd(pb.CNAllocateID{Batch: 100})
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

func TestGetIDCmdRejectedDuringBootstrapCommandsReceived(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	tsm1.state.State = pb.HAKeeperBootstrapCommandsReceived
	tsm1.state.NextID = 50000000
	tsm1.state.NextIDByKey["____server_conn_id"] = 900

	cmd := GetAllocateIDCmd(pb.CNAllocateID{Batch: 100})
	result, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, uint64(50000000), tsm1.state.NextID)

	cmd = GetAllocateIDCmd(pb.CNAllocateID{Key: "____server_conn_id", Batch: 100})
	result, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, uint64(900), tsm1.state.NextIDByKey["____server_conn_id"])
}

func TestAllocateIDByKeyCmd(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	tsm1.state.State = pb.HAKeeperRunning

	cmd := GetAllocateIDCmd(pb.CNAllocateID{Key: "k1", Batch: 100})

	result, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 1}, result)

	result, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 101}, result)

	assert.Equal(t, uint64(201), tsm1.assignIDByKey("k1"))

	result, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 202}, result)

	cmd = GetAllocateIDCmd(pb.CNAllocateID{Key: "k2", Batch: 50})

	result, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 1}, result)

	result, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, sm.Result{Value: 51}, result)

	assert.Equal(t, uint64(101), tsm1.assignIDByKey("k2"))
}

func TestAllocateIDByKeyWithRequestIDIsIdempotentAcrossSnapshot(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	tsm1.state.State = pb.HAKeeperRunning
	cmd := GetAllocateIDCmd(pb.CNAllocateID{
		Key:       "bootstrap",
		Batch:     1,
		RequestID: "cn-1",
	})

	result, err := tsm1.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)

	buf := bytes.NewBuffer(nil)
	require.NoError(t, tsm1.SaveSnapshot(buf, nil, nil))
	tsm2 := NewStateMachine(0, 2).(*stateMachine)
	require.NoError(t, tsm2.RecoverFromSnapshot(buf, nil, nil))

	result, err = tsm2.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)

	result, err = tsm2.Update(sm.Entry{Cmd: GetAllocateIDCmd(pb.CNAllocateID{
		Key:       "bootstrap",
		Batch:     1,
		RequestID: "cn-2",
	})})
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.Value)
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
	tsm.state.TNState = pb.TNState{
		Stores: make(map[string]pb.TNStoreInfo),
	}
	tsm.state.TNState.Stores["uuid3"] = pb.TNStoreInfo{
		Tick:           3,
		ServiceAddress: "addr3",
		Shards: []pb.TNShardInfo{
			{
				ShardID:   2,
				ReplicaID: 1,
			},
		},
		LogtailServerAddress:        "addr4",
		AutoIncrEpochFenceSupported: true,
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
	tsm.state.ProxyState.Stores["store-4"] = pb.ProxyStore{
		UUID:          "store-4",
		Tick:          100,
		ListenAddress: "proxy-addr1",
	}

	v, err := tsm.Lookup(&ClusterDetailsQuery{})
	require.NoError(t, err)
	expected := &pb.ClusterDetails{
		TNStores: []pb.TNStore{
			{
				UUID:           "uuid3",
				Tick:           3,
				ServiceAddress: "addr3",
				Shards: []pb.TNShardInfo{
					{
						ShardID:   2,
						ReplicaID: 1,
					},
				},
				LogtailServerAddress:        "addr4",
				AutoIncrEpochFenceSupported: true,
			},
		},
		CNStores: []pb.CNStore{
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
		LogStores: []pb.LogStore{
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
		ProxyStores: []pb.ProxyStore{
			{
				UUID:          "store-4",
				Tick:          100,
				ListenAddress: "proxy-addr1",
			},
		},
	}
	result := v.(*pb.ClusterDetails)
	sort.Slice(result.CNStores, func(i, j int) bool {
		return result.CNStores[i].UUID < result.CNStores[j].UUID
	})
	sort.Slice(result.LogStores, func(i, j int) bool {
		return result.LogStores[i].UUID < result.LogStores[j].UUID
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

func TestRecoveryPendingRejectsRunningStateAndIDAllocation(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.State = pb.HAKeeperBootstrapCommandsReceived
	rsm.state.NextID = 5000
	rsm.state.NextIDByKey["index_key"] = 200
	rsm.state.LogServiceRecoveryPending = true

	result, err := rsm.Update(sm.Entry{Cmd: GetSetStateCmd(pb.HAKeeperRunning)})
	require.NoError(t, err)
	require.Len(t, result.Data, headerSize)
	assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived,
		pb.HAKeeperState(binaryEnc.Uint32(result.Data)))
	assert.Equal(t, pb.HAKeeperBootstrapCommandsReceived, rsm.state.State)

	rsm.state.State = pb.HAKeeperBootstrapping
	result, err = rsm.Update(sm.Entry{Cmd: GetAllocateIDCmd(pb.CNAllocateID{Batch: 100})})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, uint64(5000), rsm.state.NextID)

	rsm.state.State = pb.HAKeeperRunning
	result, err = rsm.Update(sm.Entry{Cmd: GetAllocateIDCmd(pb.CNAllocateID{
		Key:   "index_key",
		Batch: 100,
	})})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, uint64(200), rsm.state.NextIDByKey["index_key"])
}

func TestSetTaskSchedulerState(t *testing.T) {
	tests := []struct {
		initialState pb.TaskSchedulerState
		newState     pb.TaskSchedulerState
		result       pb.TaskSchedulerState
	}{
		{pb.TaskSchedulerCreated, pb.TaskSchedulerCreated, pb.TaskSchedulerCreated},
		{pb.TaskSchedulerCreated, pb.TaskSchedulerRunning, pb.TaskSchedulerCreated},
		{pb.TaskSchedulerCreated, pb.TaskSchedulerStopped, pb.TaskSchedulerCreated},

		{pb.TaskSchedulerRunning, pb.TaskSchedulerCreated, pb.TaskSchedulerRunning},
		{pb.TaskSchedulerRunning, pb.TaskSchedulerRunning, pb.TaskSchedulerRunning},
		{pb.TaskSchedulerRunning, pb.TaskSchedulerStopped, pb.TaskSchedulerStopped},

		{pb.TaskSchedulerStopped, pb.TaskSchedulerCreated, pb.TaskSchedulerStopped},
		{pb.TaskSchedulerStopped, pb.TaskSchedulerRunning, pb.TaskSchedulerRunning},
		{pb.TaskSchedulerStopped, pb.TaskSchedulerStopped, pb.TaskSchedulerStopped},
	}

	for _, tt := range tests {
		rsm := stateMachine{
			state: pb.HAKeeperRSMState{
				State:              pb.HAKeeperRunning,
				TaskSchedulerState: tt.initialState,
			},
		}
		cmd := GetSetTaskSchedulerStateCmd(tt.newState)
		_, err := rsm.Update(sm.Entry{Cmd: cmd})
		require.NoError(t, err)
		assert.Equal(t, tt.result, rsm.state.TaskSchedulerState)
	}
}

func TestInitialClusterRequestCmd(t *testing.T) {
	nextIDByKey := map[string]uint64{"a": 1, "b": 2}
	cmd := GetInitialClusterRequestCmd(
		2,
		2,
		3,
		10,
		nextIDByKey,
		nil,
	)
	req := parseInitialClusterRequestCmd(cmd)
	assert.Equal(t, uint64(2), req.NumOfLogShards)
	assert.Equal(t, uint64(2), req.NumOfTNShards)
	assert.Equal(t, uint64(3), req.NumOfLogReplicas)
	assert.Equal(t, uint64(10), req.NextID)
	assert.Equal(t, nextIDByKey, req.NextIDByKey)
}

func TestHandleInitialClusterRequestCmd(t *testing.T) {
	nextIDByKey := map[string]uint64{"a": 1, "b": 2}
	cmd := GetInitialClusterRequestCmd(
		1,
		1,
		3,
		K8SIDRangeEnd+10,
		nextIDByKey,
		nil,
	)
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
		},
		TNShards: []metadata.TNShardRecord{
			{
				ShardID:    2,
				LogShardID: 1,
			},
		},
	}

	assert.Equal(t, expected, rsm.state.ClusterInfo)
	assert.Equal(t, pb.HAKeeperBootstrapping, rsm.state.State)
	assert.Equal(t, K8SIDRangeEnd+10, rsm.state.NextID)
	assert.Equal(t, nextIDByKey, rsm.state.NextIDByKey)
}

func TestLogServiceRecoveryBlocksIDsAndSchedulesUntilPrepared(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	result, err := rsm.Update(sm.Entry{Cmd: GetInitialClusterRequestCmdWithRecovery(
		1, 1, 3, 0, nil, nil, true,
	)})
	require.NoError(t, err)
	require.Equal(t, sm.Result{Value: uint64(pb.HAKeeperCreated)}, result)
	require.Equal(t, pb.HAKeeperBootstrapping, rsm.state.State)
	require.True(t, rsm.state.LogServiceRecoveryPending)
	require.False(t, rsm.state.LogServiceRecoveryPrepared)
	require.False(t, rsm.state.LogServiceRecoveryCompleted)

	nextID := rsm.state.NextID
	result, err = rsm.Update(sm.Entry{Cmd: GetAllocateIDCmd(pb.CNAllocateID{Batch: 100})})
	require.NoError(t, err)
	require.Equal(t, sm.Result{}, result)
	require.Equal(t, nextID, rsm.state.NextID)

	result, err = rsm.Update(sm.Entry{Cmd: GetUpdateCommandsCmd(1, nil)})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, result.Data)
	require.Zero(t, rsm.state.Term)
	result, err = rsm.Update(sm.Entry{Cmd: GetCompleteLogServiceRecoveryCmd()})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, result.Data)
	require.True(t, rsm.state.LogServiceRecoveryPending)
	require.False(t, rsm.state.LogServiceRecoveryCompleted)

	result, err = rsm.Update(sm.Entry{Cmd: GetRestoreIDWatermarkCmd(
		K8SIDRangeEnd+100,
		map[string]uint64{"index_key": 200},
		true,
	)})
	require.NoError(t, err)
	require.Empty(t, result.Data)
	require.True(t, rsm.state.LogServiceRecoveryPrepared)
	require.Equal(t, K8SIDRangeEnd+100, rsm.state.NextID)
	require.Equal(t, uint64(200), rsm.state.NextIDByKey["index_key"])
	require.Equal(t, uint64(1), rsm.state.IDWatermarkRestoreGeneration)

	result, err = rsm.Update(sm.Entry{Cmd: GetAllocateIDCmd(pb.CNAllocateID{Batch: 1})})
	require.NoError(t, err)
	require.Equal(t, K8SIDRangeEnd+101, result.Value)
	result, err = rsm.Update(sm.Entry{Cmd: GetUpdateCommandsCmd(1, nil)})
	require.NoError(t, err)
	require.Empty(t, result.Data)
	require.Equal(t, uint64(1), rsm.state.Term)

	result, err = rsm.Update(sm.Entry{Cmd: GetCompleteLogServiceRecoveryCmd()})
	require.NoError(t, err)
	require.Empty(t, result.Data)
	require.False(t, rsm.state.LogServiceRecoveryPending)
	require.False(t, rsm.state.LogServiceRecoveryPrepared)
	require.True(t, rsm.state.LogServiceRecoveryCompleted)
	result, err = rsm.Update(sm.Entry{Cmd: GetCompleteLogServiceRecoveryCmd()})
	require.NoError(t, err)
	require.Empty(t, result.Data)
	require.True(t, rsm.state.LogServiceRecoveryCompleted)
}

func TestInitialClusterRecoveryAppliesWatermarksAtomically(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	result, err := rsm.Update(sm.Entry{Cmd: GetInitialClusterRequestCmdWithRecovery(
		1,
		1,
		3,
		K8SIDRangeEnd+100,
		map[string]uint64{"index_key": 200},
		nil,
		true,
	)})
	require.NoError(t, err)
	require.Equal(t, sm.Result{Value: uint64(pb.HAKeeperCreated)}, result)
	require.True(t, rsm.state.LogServiceRecoveryPending)
	require.True(t, rsm.state.LogServiceRecoveryPrepared)
	require.False(t, rsm.state.LogServiceRecoveryCompleted)
	require.Equal(t, K8SIDRangeEnd+100, rsm.state.NextID)
	require.Equal(t, uint64(200), rsm.state.NextIDByKey["index_key"])
	require.Equal(t, uint64(1), rsm.state.IDWatermarkRestoreGeneration)
}

func TestRestoreIDWatermarkRequiresReplicatedRecoveryIntent(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	result, err := rsm.Update(sm.Entry{Cmd: GetInitialClusterRequestCmd(
		1, 1, 3, K8SIDRangeEnd+10, map[string]uint64{"existing": 10}, nil,
	)})
	require.NoError(t, err)
	require.Equal(t, sm.Result{Value: uint64(pb.HAKeeperCreated)}, result)

	result, err = rsm.Update(sm.Entry{Cmd: GetRestoreIDWatermarkCmd(
		K8SIDRangeEnd+100, map[string]uint64{"restored": 20}, false,
	)})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, result.Data)
	require.Equal(t, K8SIDRangeEnd+10, rsm.state.NextID)
	require.Equal(t, uint64(10), rsm.state.NextIDByKey["existing"])
	require.Zero(t, rsm.state.NextIDByKey["restored"])
}

func TestHandleRestoreIDWatermarkCmdRejectsLateLogServiceRecovery(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.State = pb.HAKeeperRunning
	rsm.state.NextID = 1000
	rsm.state.NextIDByKey = map[string]uint64{
		"index_key":          100,
		"____server_conn_id": 900,
	}
	rsm.state.ClusterInfo = pb.ClusterInfo{
		LogShards: []metadata.LogShardRecord{
			{
				ShardID:          10,
				NumberOfReplicas: 3,
			},
		},
	}

	cmd := GetRestoreIDWatermarkCmd(
		5000,
		map[string]uint64{
			"index_key":          200,
			"____server_conn_id": 800,
			"_mo_bootstrap":      1,
		},
		true,
	)
	assert.Equal(t, pb.RestoreIDWatermarkUpdate, parseCmdTag(cmd))
	result, err := rsm.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)

	assert.Equal(t, sm.Result{Value: uint64(pb.HAKeeperRunning), Data: []byte{1}}, result)
	assert.Equal(t, pb.HAKeeperRunning, rsm.state.State)
	assert.Equal(t, uint64(1000), rsm.state.NextID)
	assert.Equal(t, uint64(100), rsm.state.NextIDByKey["index_key"])
	assert.Equal(t, uint64(900), rsm.state.NextIDByKey["____server_conn_id"])
	assert.False(t, rsm.state.LogServiceRecoveryPending)
	assert.Zero(t, rsm.state.IDWatermarkRestoreGeneration)
	rsm.state.State = pb.HAKeeperBootstrapFailed
	result, err = rsm.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{Value: uint64(pb.HAKeeperBootstrapFailed), Data: []byte{1}}, result)
	assert.Equal(t, uint64(1000), rsm.state.NextID)
	assert.False(t, rsm.state.LogServiceRecoveryPending)
	rsm.state.State = pb.HAKeeperRunning

	// ID-only restoration is also rejected after bootstrap. Managed clients
	// can already hold allocated batches once HAKeeper is Running.
	result, err = rsm.Update(sm.Entry{Cmd: GetRestoreIDWatermarkCmd(
		5000,
		map[string]uint64{
			"index_key":          200,
			"____server_conn_id": 800,
			"_mo_bootstrap":      1,
		},
		false,
	)})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{Value: uint64(pb.HAKeeperRunning), Data: []byte{1}}, result)
	assert.Equal(t, uint64(1000), rsm.state.NextID)
	assert.Equal(t, uint64(100), rsm.state.NextIDByKey["index_key"])
	assert.Equal(t, uint64(900), rsm.state.NextIDByKey["____server_conn_id"])
	assert.Zero(t, rsm.state.NextIDByKey["_mo_bootstrap"])
	assert.False(t, rsm.state.LogServiceRecoveryPending)
	assert.Zero(t, rsm.state.IDWatermarkRestoreGeneration)
	assert.Equal(t, uint64(10), rsm.state.ClusterInfo.LogShards[0].ShardID)

	// A normal duplicate initial-cluster request remains a no-op, even when
	// its ID fields are higher than the live state.
	cmd = GetInitialClusterRequestCmd(
		1,
		1,
		3,
		9000,
		map[string]uint64{"index_key": 9000},
		nil,
	)
	_, err = rsm.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), rsm.state.NextID)
	assert.Equal(t, uint64(100), rsm.state.NextIDByKey["index_key"])
}

func TestRestoreIDWatermarkCannotInitializeHAKeeper(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	result, err := rsm.Update(sm.Entry{Cmd: GetRestoreIDWatermarkCmd(
		5000,
		map[string]uint64{"index_key": 200},
		true,
	)})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{Value: uint64(pb.HAKeeperCreated)}, result)
	assert.Equal(t, pb.HAKeeperCreated, rsm.state.State)
	assert.Equal(t, uint64(0), rsm.state.NextID)
	assert.Equal(t, uint64(0), rsm.state.NextIDByKey["index_key"])
	assert.False(t, rsm.state.LogServiceRecoveryPending)
	assert.Zero(t, rsm.state.IDWatermarkRestoreGeneration)
	result, err = rsm.Update(sm.Entry{Cmd: GetCompleteLogServiceRecoveryCmd()})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{Value: uint64(pb.HAKeeperCreated), Data: []byte{1}}, result)
	assert.False(t, rsm.state.LogServiceRecoveryPending)
	assert.False(t, rsm.state.LogServiceRecoveryCompleted)
}

func TestGetCommandBatch(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	cb := pb.CommandBatch{
		Term: 12345,
		Commands: []pb.ScheduleCommand{{
			UUID:        "uuid1",
			ServiceType: pb.LogService,
			ConfigChange: &pb.ConfigChange{
				ChangeType: pb.AddReplica,
			},
		}},
	}
	rsm.state.ScheduleCommands["uuid1"] = cb
	result := rsm.getCommandBatch("uuid1")
	var ncb pb.CommandBatch
	require.NoError(t, ncb.Unmarshal(result.Data))
	assert.Equal(t, cb, ncb)
	_, ok := rsm.state.ScheduleCommands["uuid1"]
	assert.False(t, ok)
}

func TestScheduleCommandReadIsStableUntilHeartbeatDelivery(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	command := pb.ScheduleCommand{
		UUID:        "uuid1",
		ServiceType: pb.TNService,
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.StartReplica,
			InitialMembers: map[uint64]string{
				1: "tn-1",
			},
		},
	}
	_, err := rsm.Update(sm.Entry{
		Index: 42,
		Cmd:   GetUpdateCommandsCmd(1, []pb.ScheduleCommand{command}),
	})
	require.NoError(t, err)

	value, err := rsm.Lookup(&ScheduleCommandQuery{UUID: command.UUID})
	require.NoError(t, err)
	readBatch := value.(*pb.CommandBatch)
	require.Zero(t, readBatch.BatchID,
		"delivery IDs stay disabled until every HAKeeper replica upgrades")
	require.Equal(t, []pb.ScheduleCommand{command}, readBatch.Commands)
	_, ok := rsm.state.ScheduleCommands[command.UUID]
	require.True(t, ok, "read-only polling must not consume commands")
	readBatch.Commands[0].UUID = "caller-owned"
	readBatch.Commands[0].ConfigChange.InitialMembers[1] = "caller-owned"
	require.Equal(t, command, rsm.state.ScheduleCommands[command.UUID].Commands[0],
		"read results must not alias replicated state")

	value, err = rsm.Lookup(&ScheduleCommandQuery{UUID: command.UUID})
	require.NoError(t, err)
	readBatch = value.(*pb.CommandBatch)

	heartbeat, err := (&pb.TNStoreHeartbeat{
		UUID:                        command.UUID,
		CommandDeliveryAckSupported: true,
	}).Marshal()
	require.NoError(t, err)
	result, err := rsm.Update(sm.Entry{Index: 43, Cmd: GetTNStoreHeartbeatCmd(heartbeat)})
	require.NoError(t, err)
	var delivered pb.CommandBatch
	require.NoError(t, delivered.Unmarshal(result.Data))
	require.Equal(t, *readBatch, delivered)
	_, ok = rsm.state.ScheduleCommands[command.UUID]
	require.False(t, ok,
		"mixed HAKeeper versions keep legacy consumption even for a new TN")
}

func TestTickAssignsIDsToLegacyScheduleCommandBatchesOnce(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryEnabled = true
	command := pb.ScheduleCommand{UUID: "tn-1", ServiceType: pb.TNService}
	rsm.state.ScheduleCommands[command.UUID] = pb.CommandBatch{
		Commands: []pb.ScheduleCommand{command},
	}

	_, err := rsm.Update(sm.Entry{Index: 41, Cmd: GetTickCmd()})
	require.NoError(t, err)
	assigned := rsm.state.ScheduleCommands[command.UUID]
	require.Equal(t, uint64(41), assigned.BatchID)
	require.Equal(t, []pb.ScheduleCommandID{{OriginBatchID: 41}}, assigned.CommandIDs)
	require.True(t, rsm.state.CommandDeliveryBatchIDsAssigned)
	require.True(t, rsm.state.CommandDeliveryCommandIDsAssigned)

	_, err = rsm.Update(sm.Entry{Index: 42, Cmd: GetTickCmd()})
	require.NoError(t, err)
	require.Equal(t, uint64(41), rsm.state.ScheduleCommands[command.UUID].BatchID,
		"subsequent ticks must not rewrite a stable delivery generation")
	require.Equal(t, assigned.CommandIDs, rsm.state.ScheduleCommands[command.UUID].CommandIDs,
		"subsequent ticks must not rewrite stable command identities")
}

func TestTickAssignsCommandIDsFromBatchOnlySnapshot(t *testing.T) {
	oldState := pb.NewRSMState()
	oldState.CommandDeliveryEnabled = true
	oldState.CommandDeliveryBatchIDsAssigned = true
	command := pb.ScheduleCommand{UUID: "tn-1", ServiceType: pb.TNService}
	oldState.ScheduleCommands[command.UUID] = pb.CommandBatch{
		BatchID:  7,
		Commands: []pb.ScheduleCommand{command},
	}
	data, err := oldState.Marshal()
	require.NoError(t, err)
	rsm := NewStateMachine(0, 1).(*stateMachine)
	require.NoError(t, rsm.RecoverFromSnapshot(bytes.NewReader(data), nil, nil))
	require.True(t, rsm.state.CommandDeliveryBatchIDsAssigned)
	require.False(t, rsm.state.CommandDeliveryCommandIDsAssigned)

	_, err = rsm.Update(sm.Entry{Index: 41, Cmd: GetTickCmd()})
	require.NoError(t, err)
	batch := rsm.state.ScheduleCommands[command.UUID]
	require.Equal(t, uint64(7), batch.BatchID,
		"the migration must preserve an existing batch acknowledgement identity")
	require.Equal(t, []pb.ScheduleCommandID{{OriginBatchID: 41}}, batch.CommandIDs)
	require.True(t, rsm.state.CommandDeliveryCommandIDsAssigned)
}

func TestAcknowledgedHeartbeatAssignsMissingBatchID(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryEnabled = true
	command := pb.ScheduleCommand{UUID: "tn-1", ServiceType: pb.TNService}
	rsm.state.ScheduleCommands[command.UUID] = pb.CommandBatch{
		Commands: []pb.ScheduleCommand{command},
	}

	hb, err := (&pb.TNStoreHeartbeat{
		UUID:                        command.UUID,
		CommandDeliveryAckSupported: true,
	}).Marshal()
	require.NoError(t, err)
	result, err := rsm.Update(sm.Entry{Index: 10, Cmd: GetTNStoreHeartbeatCmd(hb)})
	require.NoError(t, err)
	var batch pb.CommandBatch
	require.NoError(t, batch.Unmarshal(result.Data))
	require.Equal(t, uint64(10), batch.BatchID)
	require.Equal(t, []pb.ScheduleCommandID{{OriginBatchID: 10}}, batch.CommandIDs)
	require.Equal(t, uint64(10), rsm.state.ScheduleCommands[command.UUID].BatchID)
}

func TestScheduleCommandUpdateMigratesLegacyIDsBeforeRollover(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryEnabled = true
	oldCommand := pb.ScheduleCommand{
		UUID:        "tn-1",
		ServiceType: pb.TNService,
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.StopReplica,
			Replica:    pb.Replica{ShardID: 1},
		},
	}
	rsm.state.ScheduleCommands[oldCommand.UUID] = pb.CommandBatch{
		Commands: []pb.ScheduleCommand{oldCommand},
	}
	newCommand := oldCommand
	newCommand.ConfigChange = &pb.ConfigChange{
		ChangeType: pb.StopReplica,
		Replica:    pb.Replica{ShardID: 2},
	}
	anotherCommand := oldCommand
	anotherCommand.ConfigChange = &pb.ConfigChange{
		ChangeType: pb.StopReplica,
		Replica:    pb.Replica{ShardID: 3},
	}

	_, err := rsm.Update(sm.Entry{
		Index: 20,
		Cmd: GetUpdateCommandsCmd(1, []pb.ScheduleCommand{
			newCommand,
			anotherCommand,
		}),
	})
	require.NoError(t, err)
	batch := rsm.state.ScheduleCommands[oldCommand.UUID]
	require.Equal(t, []pb.ScheduleCommandID{
		{OriginBatchID: 20, CommandIndex: 0},
		{OriginBatchID: 20, CommandIndex: 1},
		{OriginBatchID: 20, CommandIndex: 2},
	}, batch.CommandIDs)
	require.Len(t, batch.Commands, 3)
}

func TestEnsureScheduleCommandIDsRepairsPartialDuplicateState(t *testing.T) {
	batch := pb.CommandBatch{
		BatchID: 5,
		Commands: []pb.ScheduleCommand{
			{UUID: "tn-1"},
			{UUID: "tn-1"},
			{UUID: "tn-1"},
		},
		CommandIDs: []pb.ScheduleCommandID{
			{OriginBatchID: 5},
			{OriginBatchID: 5},
		},
	}

	require.True(t, ensureScheduleCommandIDs(&batch, 7))
	require.Equal(t, []pb.ScheduleCommandID{
		{OriginBatchID: 5},
		{OriginBatchID: 7},
		{OriginBatchID: 7, CommandIndex: 1},
	}, batch.CommandIDs)
	require.False(t, ensureScheduleCommandIDs(&batch, 8),
		"a valid migrated batch must not be rewritten on later entries")
}

func TestCommandDeliveryActivationUsesPostBarrierCapabilities(t *testing.T) {
	newRSM := func(allCapabilitiesObserved bool) *stateMachine {
		rsm := NewStateMachine(0, 1).(*stateMachine)
		rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
			ShardID: DefaultHAKeeperShardID,
			Replicas: map[uint64]string{
				1: "log-1",
				2: "log-2",
			},
			NonVotingReplicas: map[uint64]string{3: "log-3"},
		}
		if allCapabilitiesObserved {
			for _, uuid := range []string{"log-1", "log-2", "log-3"} {
				rsm.state.LogState.Stores[uuid] = pb.LogStoreInfo{
					CommandDeliverySupported: true,
				}
			}
		}
		return rsm
	}
	// Model a leader that observed the capability heartbeats before an upgraded
	// follower. Applying the same phase-one entry must erase that difference.
	rsms := []*stateMachine{newRSM(true), newRSM(false)}
	for _, rsm := range rsms {
		result, err := rsm.Update(sm.Entry{Index: 40, Cmd: GetEnableCommandDeliveryCmd()})
		require.NoError(t, err)
		require.Equal(t, uint64(2), result.Value)
		require.True(t, rsm.state.CommandDeliveryPreparing)
		require.False(t, rsm.state.CommandDeliveryEnabled)
		require.Empty(t, rsm.state.CommandDeliveryReady)
	}

	heartbeat := func(rsm *stateMachine, index uint64, uuid string, supported bool) {
		t.Helper()
		data, err := (&pb.LogStoreHeartbeat{
			UUID:                     uuid,
			CommandDeliverySupported: supported,
		}).Marshal()
		require.NoError(t, err)
		_, err = rsm.Update(sm.Entry{Index: index, Cmd: GetLogStoreHeartbeatCmd(data)})
		require.NoError(t, err)
	}
	for _, rsm := range rsms {
		heartbeat(rsm, 41, "log-1", true)
		heartbeat(rsm, 42, "log-2", true)
		heartbeat(rsm, 43, "log-3", false)
		value, err := rsm.Lookup(&CommandDeliveryStateQuery{})
		require.NoError(t, err)
		delivery := value.(CommandDeliveryState)
		delivery.Ready["log-1"] = false
		require.True(t, rsm.state.CommandDeliveryReady["log-1"],
			"read results must not alias replicated readiness state")
		result, err := rsm.Update(sm.Entry{Index: 44, Cmd: GetEnableCommandDeliveryCmd()})
		require.NoError(t, err)
		require.Zero(t, result.Value)
		require.False(t, rsm.state.CommandDeliveryEnabled)

		heartbeat(rsm, 45, "log-3", true)
		result, err = rsm.Update(sm.Entry{Index: 46, Cmd: GetEnableCommandDeliveryCmd()})
		require.NoError(t, err)
		require.Equal(t, uint64(1), result.Value)
		require.True(t, rsm.state.CommandDeliveryEnabled)
		require.False(t, rsm.state.CommandDeliveryPreparing)
		require.Nil(t, rsm.state.CommandDeliveryReady)
	}
	require.Equal(t, rsms[0].state, rsms[1].state)
}

func TestCommandDeliveryActivationWaitsForServiceCapabilities(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		ShardID: DefaultHAKeeperShardID,
		Replicas: map[uint64]string{
			1: "log-1",
		},
	}
	// These stores advertised support before the barrier. That observation is
	// deliberately not sufficient: an old service can still be running while
	// HAKeeper is upgraded first, so readiness must be observed after phase one.
	rsm.state.LogState.Stores["log-1"] = pb.LogStoreInfo{
		CommandDeliverySupported: true,
	}
	rsm.state.CNState.Stores["cn-1"] = pb.CNStoreInfo{
		CommandDeliveryAckSupported: true,
	}
	rsm.state.TNState.Stores["tn-1"] = pb.TNStoreInfo{
		CommandDeliveryAckSupported: true,
	}

	result, err := rsm.Update(sm.Entry{Index: 10, Cmd: GetEnableCommandDeliveryCmd()})
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.Value)
	require.True(t, rsm.state.CommandDeliveryPreparing)
	require.Empty(t, rsm.state.CommandDeliveryCNReady)
	require.Empty(t, rsm.state.CommandDeliveryTNReady)

	heartbeatLog := func(index uint64) {
		data, err := (&pb.LogStoreHeartbeat{
			UUID:                     "log-1",
			CommandDeliverySupported: true,
		}).Marshal()
		require.NoError(t, err)
		_, err = rsm.Update(sm.Entry{Index: index, Cmd: GetLogStoreHeartbeatCmd(data)})
		require.NoError(t, err)
	}
	heartbeatCN := func(index uint64, supported bool) {
		data, err := (&pb.CNStoreHeartbeat{
			UUID:                        "cn-1",
			CommandDeliveryAckSupported: supported,
		}).Marshal()
		require.NoError(t, err)
		_, err = rsm.Update(sm.Entry{Index: index, Cmd: GetCNStoreHeartbeatCmd(data)})
		require.NoError(t, err)
	}
	heartbeatTN := func(index uint64, supported bool) {
		data, err := (&pb.TNStoreHeartbeat{
			UUID:                        "tn-1",
			CommandDeliveryAckSupported: supported,
		}).Marshal()
		require.NoError(t, err)
		_, err = rsm.Update(sm.Entry{Index: index, Cmd: GetTNStoreHeartbeatCmd(data)})
		require.NoError(t, err)
	}

	heartbeatLog(11)
	// An old heartbeat is explicit negative capability and must keep the
	// transition disabled rather than falling through to legacy semantics.
	heartbeatCN(12, false)
	heartbeatTN(13, false)
	result, err = rsm.Update(sm.Entry{Index: 14, Cmd: GetEnableCommandDeliveryCmd()})
	require.NoError(t, err)
	require.Zero(t, result.Value)
	require.False(t, rsm.state.CommandDeliveryEnabled)

	heartbeatCN(15, true)
	heartbeatTN(16, true)
	result, err = rsm.Update(sm.Entry{Index: 17, Cmd: GetEnableCommandDeliveryCmd()})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)
	require.True(t, rsm.state.CommandDeliveryEnabled)
	require.Nil(t, rsm.state.CommandDeliveryCNReady)
	require.Nil(t, rsm.state.CommandDeliveryTNReady)
}

func TestCommandDeliveryActivationIgnoresExpiredServiceRecords(t *testing.T) {
	cfg := Config{
		TickPerSecond:  1,
		CNStoreTimeout: 10 * time.Second,
		TNStoreTimeout: 10 * time.Second,
	}
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.Tick = 20
	rsm.state.CommandDeliveryPreparing = true
	rsm.state.CommandDeliveryReady = map[string]bool{"log-1": true}
	rsm.state.CommandDeliveryCNReady = map[string]bool{
		"cn-live": true,
		"cn-dead": false,
	}
	rsm.state.CommandDeliveryTNReady = map[string]bool{
		"tn-live": true,
		"tn-dead": false,
	}
	rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		ShardID:  DefaultHAKeeperShardID,
		Replicas: map[uint64]string{1: "log-1"},
	}
	// TN records are retained after a store stops heartbeating. The replicated
	// command carries the deterministic expiry thresholds, and the RSM applies
	// them to its current store state at the activation entry's commit point.
	rsm.state.CNState.Stores["cn-live"] = pb.CNStoreInfo{Tick: 20}
	rsm.state.CNState.Stores["cn-dead"] = pb.CNStoreInfo{Tick: 1}
	rsm.state.TNState.Stores["tn-live"] = pb.TNStoreInfo{Tick: 20}
	rsm.state.TNState.Stores["tn-dead"] = pb.TNStoreInfo{Tick: 1}

	result, err := rsm.Update(sm.Entry{
		Index: 10,
		Cmd:   GetEnableCommandDeliveryCmdForConfig(cfg),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), result.Value)
	require.True(t, rsm.state.CommandDeliveryEnabled)

	// An active unsupported target remains a hard barrier.
	rsm = NewStateMachine(0, 1).(*stateMachine)
	rsm.state.Tick = 20
	rsm.state.CommandDeliveryPreparing = true
	rsm.state.CommandDeliveryReady = map[string]bool{"log-1": true}
	rsm.state.CommandDeliveryTNReady = map[string]bool{"tn-live": false}
	rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		ShardID:  DefaultHAKeeperShardID,
		Replicas: map[uint64]string{1: "log-1"},
	}
	rsm.state.TNState.Stores["tn-live"] = pb.TNStoreInfo{Tick: 20}
	result, err = rsm.Update(sm.Entry{
		Index: 10,
		Cmd:   GetEnableCommandDeliveryCmdForConfig(cfg),
	})
	require.NoError(t, err)
	require.Zero(t, result.Value)
	require.False(t, rsm.state.CommandDeliveryEnabled)
}

func TestCommandDeliveryActivationUsesStoreStateAtCommit(t *testing.T) {
	cfg := Config{
		TickPerSecond:  1,
		CNStoreTimeout: 10 * time.Second,
		TNStoreTimeout: 10 * time.Second,
	}
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.Tick = 20
	rsm.state.CommandDeliveryPreparing = true
	rsm.state.CommandDeliveryReady = map[string]bool{"log-1": true}
	rsm.state.CommandDeliveryCNReady = make(map[string]bool)
	rsm.state.CommandDeliveryTNReady = map[string]bool{"tn-1": false}
	rsm.state.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		ShardID:  DefaultHAKeeperShardID,
		Replicas: map[uint64]string{1: "log-1"},
	}
	// The TN was expired when the leader performed its phase-two precheck.
	rsm.state.TNState.Stores["tn-1"] = pb.TNStoreInfo{Tick: 1}
	activation := GetEnableCommandDeliveryCmdForConfig(cfg)

	// Before the activation entry commits, an old TN heartbeats and becomes a
	// current command target. The RSM must observe that heartbeat, rather than
	// enabling from the leader's stale pre-proposal view.
	data, err := (&pb.TNStoreHeartbeat{
		UUID:                        "tn-1",
		CommandDeliveryAckSupported: false,
	}).Marshal()
	require.NoError(t, err)
	_, err = rsm.Update(sm.Entry{Index: 21, Cmd: GetTNStoreHeartbeatCmd(data)})
	require.NoError(t, err)
	require.Equal(t, uint64(20), rsm.state.TNState.Stores["tn-1"].Tick)

	result, err := rsm.Update(sm.Entry{Index: 22, Cmd: activation})
	require.NoError(t, err)
	require.Zero(t, result.Value)
	require.False(t, rsm.state.CommandDeliveryEnabled)
}

func TestCommandDeliveryActivationRebuildsServiceBarrierAfterOldSnapshot(t *testing.T) {
	oldState := pb.NewRSMState()
	oldState.CommandDeliveryPreparing = true
	// This is the state shape produced before CN/TN readiness was added.
	oldState.CommandDeliveryReady = map[string]bool{"log-1": true}
	oldState.CommandDeliveryCNReady = nil
	oldState.CommandDeliveryTNReady = nil
	oldState.LogState.Shards[DefaultHAKeeperShardID] = pb.LogShardInfo{
		ShardID: DefaultHAKeeperShardID,
		Replicas: map[uint64]string{
			1: "log-1",
		},
	}
	oldState.LogState.Stores["log-1"] = pb.LogStoreInfo{
		CommandDeliverySupported: true,
	}
	data, err := oldState.Marshal()
	require.NoError(t, err)

	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryBatchIDsAssigned = true
	rsm.state.CommandDeliveryCommandIDsAssigned = true
	require.NoError(t, rsm.RecoverFromSnapshot(bytes.NewReader(data), nil, nil))
	require.Nil(t, rsm.state.CommandDeliveryCNReady)
	require.Nil(t, rsm.state.CommandDeliveryTNReady)
	require.False(t, rsm.state.CommandDeliveryBatchIDsAssigned)
	require.False(t, rsm.state.CommandDeliveryCommandIDsAssigned)
	result, err := rsm.Update(sm.Entry{Index: 20, Cmd: GetEnableCommandDeliveryCmd()})
	require.NoError(t, err)
	require.Equal(t, uint64(2), result.Value)
	require.Empty(t, rsm.state.CommandDeliveryReady)
	require.Empty(t, rsm.state.CommandDeliveryCNReady)
	require.Empty(t, rsm.state.CommandDeliveryTNReady)
	require.True(t, rsm.state.CommandDeliveryPreparing)
}

func TestLegacyServiceCannotConsumeAfterCommandDeliveryActivation(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryEnabled = true
	rsm.state.ScheduleCommands["old-cn"] = pb.CommandBatch{
		BatchID: 7,
		Commands: []pb.ScheduleCommand{{
			UUID:        "old-cn",
			ServiceType: pb.CNService,
		}},
	}

	hb, err := (&pb.CNStoreHeartbeat{
		UUID: "old-cn",
	}).Marshal()
	require.NoError(t, err)
	result, err := rsm.Update(sm.Entry{Index: 8, Cmd: GetCNStoreHeartbeatCmd(hb)})
	require.NoError(t, err)
	require.Empty(t, result.Data)
	_, ok := rsm.state.ScheduleCommands["old-cn"]
	require.True(t, ok, "an old service must not consume a durable batch")
}

func TestLegacyServiceCannotConsumeDuringCommandDeliveryPreparation(t *testing.T) {
	tests := []struct {
		name        string
		serviceType pb.ServiceType
		heartbeat   func(t *testing.T, uuid string, supported bool, ack uint64) []byte
	}{
		{
			name:        "cn",
			serviceType: pb.CNService,
			heartbeat: func(t *testing.T, uuid string, supported bool, ack uint64) []byte {
				data, err := (&pb.CNStoreHeartbeat{
					UUID:                        uuid,
					CommandDeliveryAckSupported: supported,
					AckedCommandBatchID:         ack,
				}).Marshal()
				require.NoError(t, err)
				return GetCNStoreHeartbeatCmd(data)
			},
		},
		{
			name:        "tn",
			serviceType: pb.TNService,
			heartbeat: func(t *testing.T, uuid string, supported bool, ack uint64) []byte {
				data, err := (&pb.TNStoreHeartbeat{
					UUID:                        uuid,
					CommandDeliveryAckSupported: supported,
					AckedCommandBatchID:         ack,
				}).Marshal()
				require.NoError(t, err)
				return GetTNStoreHeartbeatCmd(data)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rsm := NewStateMachine(0, 1).(*stateMachine)
			rsm.state.CommandDeliveryPreparing = true
			command := pb.ScheduleCommand{
				UUID:        test.name + "-1",
				ServiceType: test.serviceType,
				CreateTaskService: &pb.CreateTaskService{
					TaskDatabase: "mo_task",
				},
			}
			_, err := rsm.Update(sm.Entry{
				Index: 10,
				Cmd:   GetUpdateCommandsCmd(1, []pb.ScheduleCommand{command}),
			})
			require.NoError(t, err)

			result, err := rsm.Update(sm.Entry{
				Index: 11,
				Cmd:   test.heartbeat(t, command.UUID, false, 0),
			})
			require.NoError(t, err)
			require.Empty(t, result.Data)
			pending, ok := rsm.state.ScheduleCommands[command.UUID]
			require.True(t, ok, "a legacy preparation heartbeat must not consume work")
			require.Equal(t, uint64(10), pending.BatchID)

			result, err = rsm.Update(sm.Entry{
				Index: 12,
				Cmd:   test.heartbeat(t, command.UUID, true, 0),
			})
			require.NoError(t, err)
			var delivered pb.CommandBatch
			require.NoError(t, delivered.Unmarshal(result.Data))
			require.Equal(t, pending, delivered)

			result, err = rsm.Update(sm.Entry{
				Index: 13,
				Cmd:   test.heartbeat(t, command.UUID, true, delivered.BatchID),
			})
			require.NoError(t, err)
			require.Empty(t, result.Data)
			_, ok = rsm.state.ScheduleCommands[command.UUID]
			require.False(t, ok)
		})
	}
}

func TestPreparingCommandDeliveryIsNonDestructiveForSupportedService(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryPreparing = true
	command := pb.ScheduleCommand{
		UUID:        "tn-1",
		ServiceType: pb.TNService,
		ShutdownStore: &pb.ShutdownStore{
			StoreID: "tn-1",
		},
	}
	_, err := rsm.Update(sm.Entry{
		Index: 10,
		Cmd:   GetUpdateCommandsCmd(1, []pb.ScheduleCommand{command}),
	})
	require.NoError(t, err)

	heartbeat := func(ack uint64) sm.Result {
		t.Helper()
		data, err := (&pb.TNStoreHeartbeat{
			UUID:                        command.UUID,
			AckedCommandBatchID:         ack,
			CommandDeliveryAckSupported: true,
		}).Marshal()
		require.NoError(t, err)
		result, err := rsm.Update(sm.Entry{
			Index: rsm.state.Index + 1,
			Cmd:   GetTNStoreHeartbeatCmd(data),
		})
		require.NoError(t, err)
		return result
	}

	// Losing the first response cannot remove work during the preparation
	// window; the exact batch remains available for the next heartbeat.
	result := heartbeat(0)
	var delivered pb.CommandBatch
	require.NoError(t, delivered.Unmarshal(result.Data))
	require.Equal(t, []pb.ScheduleCommand{command}, delivered.Commands)
	require.NotZero(t, delivered.BatchID)
	_, ok := rsm.state.ScheduleCommands[command.UUID]
	require.True(t, ok)

	result = heartbeat(delivered.BatchID)
	require.Empty(t, result.Data)
	_, ok = rsm.state.ScheduleCommands[command.UUID]
	require.False(t, ok)
}

func TestAcknowledgedCommandDeliverySurvivesLostResponse(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryEnabled = true
	first := pb.ScheduleCommand{
		UUID:        "tn-1",
		ServiceType: pb.TNService,
		ConfigChange: &pb.ConfigChange{
			ChangeType: pb.StartReplica,
			Replica:    pb.Replica{ShardID: 2, ReplicaID: 7},
		},
	}
	_, err := rsm.Update(sm.Entry{
		Index: 42,
		Cmd:   GetUpdateCommandsCmd(1, []pb.ScheduleCommand{first}),
	})
	require.NoError(t, err)

	heartbeat := func(ack uint64, supported bool) sm.Result {
		t.Helper()
		data, err := (&pb.TNStoreHeartbeat{
			UUID:                        first.UUID,
			AckedCommandBatchID:         ack,
			CommandDeliveryAckSupported: supported,
		}).Marshal()
		require.NoError(t, err)
		result, err := rsm.Update(sm.Entry{Index: rsm.state.Index + 1, Cmd: GetTNStoreHeartbeatCmd(data)})
		require.NoError(t, err)
		return result
	}

	// The proposal commits and returns the command, but model a lost response by
	// deliberately ignoring it. The durable batch must remain pollable.
	_ = heartbeat(0, true)
	value, err := rsm.Lookup(&ScheduleCommandQuery{UUID: first.UUID})
	require.NoError(t, err)
	pending := value.(*pb.CommandBatch)
	require.Equal(t, uint64(42), pending.BatchID)
	require.Equal(t, []pb.ScheduleCommand{first}, pending.Commands)
	require.Equal(t, []pb.ScheduleCommandID{{OriginBatchID: 42}}, pending.CommandIDs)
	firstCommandID := pending.CommandIDs[0]

	// A second operator for the same store is dispatched only once. It must be
	// merged with the unacknowledged commands under a new generation; dropping
	// it or overwriting the old command would lose work in one failure ordering.
	second := first
	second.ConfigChange = &pb.ConfigChange{
		ChangeType: pb.StartReplica,
		Replica:    pb.Replica{ShardID: 2, ReplicaID: 8},
	}
	_, err = rsm.Update(sm.Entry{
		Index: 44,
		Cmd:   GetUpdateCommandsCmd(2, []pb.ScheduleCommand{second}),
	})
	require.NoError(t, err)
	value, err = rsm.Lookup(&ScheduleCommandQuery{UUID: first.UUID})
	require.NoError(t, err)
	pending = value.(*pb.CommandBatch)
	require.Equal(t, uint64(44), pending.BatchID)
	require.Equal(t, []pb.ScheduleCommand{first, second}, pending.Commands)
	require.Equal(t, []pb.ScheduleCommandID{
		firstCommandID,
		{OriginBatchID: 44},
	}, pending.CommandIDs, "rollover must preserve inherited command identity")

	// A delayed ack for the old generation cannot delete either command.
	result := heartbeat(42, true)
	var delivered pb.CommandBatch
	require.NoError(t, delivered.Unmarshal(result.Data))
	require.Equal(t, *pending, delivered)

	// Only the merged generation's exact ack removes it. Losing this heartbeat's
	// response is harmless because removal itself is replicated.
	result = heartbeat(44, true)
	require.Empty(t, result.Data)
	_, ok := rsm.state.ScheduleCommands[first.UUID]
	require.False(t, ok)

	// The next checker run can install another generation. Replaying the prior
	// exact ack is now stale and cannot delete it.
	third := second
	third.ConfigChange = &pb.ConfigChange{
		ChangeType: pb.StartReplica,
		Replica:    pb.Replica{ShardID: 2, ReplicaID: 9},
	}
	_, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetUpdateCommandsCmd(3, []pb.ScheduleCommand{third}),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(47), rsm.state.ScheduleCommands[first.UUID].BatchID)
	result = heartbeat(44, true)
	delivered = pb.CommandBatch{}
	require.NoError(t, delivered.Unmarshal(result.Data))
	require.Equal(t, uint64(47), delivered.BatchID)
	require.Equal(t, []pb.ScheduleCommand{third}, delivered.Commands)
	require.Equal(t, []pb.ScheduleCommandID{{OriginBatchID: 47}}, delivered.CommandIDs)

	result = heartbeat(47, true)
	require.Empty(t, result.Data)
	_, ok = rsm.state.ScheduleCommands[first.UUID]
	require.False(t, ok)

	// An old CN/TN binary that appears after activation must not fall back to
	// consume-on-heartbeat. The command remains durable until the service is
	// upgraded and can acknowledge it.
	_, err = rsm.Update(sm.Entry{
		Index: rsm.state.Index + 1,
		Cmd:   GetUpdateCommandsCmd(4, []pb.ScheduleCommand{first}),
	})
	require.NoError(t, err)
	result = heartbeat(0, false)
	require.Empty(t, result.Data)
	_, ok = rsm.state.ScheduleCommands[first.UUID]
	require.True(t, ok)
	require.NotEqual(t, firstCommandID,
		rsm.state.ScheduleCommands[first.UUID].CommandIDs[0],
		"identical work installed after acknowledgement needs a new identity")
}

func TestPendingScheduleCommandsDeduplicateRetries(t *testing.T) {
	rsm := NewStateMachine(0, 1).(*stateMachine)
	rsm.state.CommandDeliveryEnabled = true

	task := pb.ScheduleCommand{
		UUID:        "cn-1",
		ServiceType: pb.CNService,
		CreateTaskService: &pb.CreateTaskService{
			TaskDatabase: "mo_task",
		},
	}
	for i := uint64(10); i < 13; i++ {
		_, err := rsm.Update(sm.Entry{
			Index: i,
			Cmd:   GetUpdateCommandsCmd(i, []pb.ScheduleCommand{task}),
		})
		require.NoError(t, err)
	}

	value, err := rsm.Lookup(&ScheduleCommandQuery{UUID: task.UUID})
	require.NoError(t, err)
	batch := value.(*pb.CommandBatch)
	require.Equal(t, uint64(10), batch.BatchID,
		"a repeated checker command does not create a new delivery generation")
	require.Equal(t, []pb.ScheduleCommand{task}, batch.Commands)

	join := pb.ScheduleCommand{
		UUID:        task.UUID,
		ServiceType: pb.CNService,
		JoinGossipCluster: &pb.JoinGossipCluster{
			Existing: []string{"cn-2", "cn-3"},
		},
	}
	_, err = rsm.Update(sm.Entry{
		Index: 13,
		Cmd:   GetUpdateCommandsCmd(13, []pb.ScheduleCommand{join}),
	})
	require.NoError(t, err)

	// The checker builds the peer list from a map. Its order is not part of
	// the command's meaning, so a reordered retry must not grow the batch.
	reorderedJoin := join
	reorderedJoin.JoinGossipCluster = &pb.JoinGossipCluster{
		Existing: []string{"cn-3", "cn-2"},
	}
	_, err = rsm.Update(sm.Entry{
		Index: 14,
		Cmd:   GetUpdateCommandsCmd(14, []pb.ScheduleCommand{reorderedJoin}),
	})
	require.NoError(t, err)

	value, err = rsm.Lookup(&ScheduleCommandQuery{UUID: task.UUID})
	require.NoError(t, err)
	batch = value.(*pb.CommandBatch)
	require.Equal(t, uint64(13), batch.BatchID,
		"a semantically identical retry must retain the current generation")
	require.Equal(t, []pb.ScheduleCommand{task, join}, batch.Commands)

	// A changed peer set is still the same join operation. Replace the stale
	// seed list instead of retaining both commands (the handler marks itself
	// joined before the asynchronous join, so running the stale command first
	// could otherwise suppress the useful retry).
	updatedJoin := join
	updatedJoin.JoinGossipCluster = &pb.JoinGossipCluster{
		Existing: []string{"cn-2", "cn-4"},
	}
	_, err = rsm.Update(sm.Entry{
		Index: 15,
		Cmd:   GetUpdateCommandsCmd(15, []pb.ScheduleCommand{updatedJoin}),
	})
	require.NoError(t, err)
	value, err = rsm.Lookup(&ScheduleCommandQuery{UUID: task.UUID})
	require.NoError(t, err)
	batch = value.(*pb.CommandBatch)
	require.Equal(t, uint64(15), batch.BatchID)
	require.Equal(t, []pb.ScheduleCommand{task, updatedJoin}, batch.Commands)

	// Replica IDs are allocated afresh by the checker when a target remains
	// silent, but these are retries of one logical start operation. Coalesce
	// them by shard/change type so the durable batch cannot grow forever.
	start := func(replicaID uint64) pb.ScheduleCommand {
		return pb.ScheduleCommand{
			UUID:        "tn-1",
			ServiceType: pb.TNService,
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					ShardID:    7,
					ReplicaID:  replicaID,
					LogShardID: 8,
				},
				ChangeType: pb.StartReplica,
			},
		}
	}
	for i, replicaID := range []uint64{101, 102, 103} {
		_, err = rsm.Update(sm.Entry{
			Index: uint64(20 + i),
			Cmd:   GetUpdateCommandsCmd(uint64(20+i), []pb.ScheduleCommand{start(replicaID)}),
		})
		require.NoError(t, err)
	}
	value, err = rsm.Lookup(&ScheduleCommandQuery{UUID: "tn-1"})
	require.NoError(t, err)
	batch = value.(*pb.CommandBatch)
	require.Equal(t, uint64(20), batch.BatchID)
	require.Len(t, batch.Commands, 1)
	require.Equal(t, uint64(101), batch.Commands[0].ConfigChange.Replica.ReplicaID,
		"the first valid retry remains the durable command")
}

func TestBootstrapReplicaCommandsRetriedUntilHeartbeatAcknowledges(t *testing.T) {
	const (
		storeID   = "store-1"
		shardID   = uint64(1)
		replicaID = uint64(2)
	)

	run := func(
		t *testing.T,
		serviceType pb.ServiceType,
		heartbeat func(reported bool) []byte,
	) {
		t.Helper()

		rsm := NewStateMachine(0, 1).(*stateMachine)
		rsm.state.State = pb.HAKeeperBootstrapping
		command := pb.ScheduleCommand{
			UUID:          storeID,
			Bootstrapping: true,
			ServiceType:   serviceType,
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					UUID:      storeID,
					ShardID:   shardID,
					ReplicaID: replicaID,
				},
				ChangeType: pb.StartReplica,
			},
		}
		_, err := rsm.Update(sm.Entry{
			Cmd: GetUpdateCommandsCmd(1, []pb.ScheduleCommand{command}),
		})
		require.NoError(t, err)
		require.Equal(t, pb.HAKeeperBootstrapCommandsReceived, rsm.state.State)

		// Model a heartbeat whose proposal commits but whose response is lost:
		// ignoring the first result must not consume the bootstrap command.
		_, err = rsm.Update(sm.Entry{Cmd: heartbeat(false)})
		require.NoError(t, err)

		result, err := rsm.Update(sm.Entry{Cmd: heartbeat(false)})
		require.NoError(t, err)
		var batch pb.CommandBatch
		require.NoError(t, batch.Unmarshal(result.Data))
		require.Equal(t, []pb.ScheduleCommand{command}, batch.Commands)
		_, ok := rsm.state.ScheduleCommands[storeID]
		require.True(t, ok)

		result, err = rsm.Update(sm.Entry{Cmd: heartbeat(true)})
		require.NoError(t, err)
		require.Empty(t, result.Data)
		_, ok = rsm.state.ScheduleCommands[storeID]
		require.False(t, ok)
	}

	t.Run("log-service", func(t *testing.T) {
		run(t, pb.LogService, func(reported bool) []byte {
			hb := pb.LogStoreHeartbeat{UUID: storeID}
			if reported {
				hb.Replicas = []pb.LogReplicaInfo{{
					LogShardInfo: pb.LogShardInfo{ShardID: shardID},
					ReplicaID:    replicaID,
				}}
			}
			data, err := hb.Marshal()
			require.NoError(t, err)
			return GetLogStoreHeartbeatCmd(data)
		})
	})

	t.Run("tn-service", func(t *testing.T) {
		run(t, pb.TNService, func(reported bool) []byte {
			hb := pb.TNStoreHeartbeat{UUID: storeID}
			if reported {
				hb.Shards = []pb.TNShardInfo{{
					ShardID:   shardID,
					ReplicaID: replicaID,
				}}
			}
			data, err := hb.Marshal()
			require.NoError(t, err)
			return GetTNStoreHeartbeatCmd(data)
		})
	})

	t.Run("acknowledged-tn-service", func(t *testing.T) {
		rsm := NewStateMachine(0, 1).(*stateMachine)
		rsm.state.State = pb.HAKeeperBootstrapping
		rsm.state.CommandDeliveryEnabled = true
		command := pb.ScheduleCommand{
			UUID:          storeID,
			Bootstrapping: true,
			ServiceType:   pb.TNService,
			ConfigChange: &pb.ConfigChange{
				Replica: pb.Replica{
					UUID:      storeID,
					ShardID:   shardID,
					ReplicaID: replicaID,
				},
				ChangeType: pb.StartReplica,
			},
		}
		_, err := rsm.Update(sm.Entry{
			Index: 10,
			Cmd:   GetUpdateCommandsCmd(1, []pb.ScheduleCommand{command}),
		})
		require.NoError(t, err)

		heartbeat := func(reported bool) sm.Result {
			t.Helper()
			hb := pb.TNStoreHeartbeat{
				UUID:                        storeID,
				AckedCommandBatchID:         10,
				CommandDeliveryAckSupported: true,
			}
			if reported {
				hb.Shards = []pb.TNShardInfo{{
					ShardID:   shardID,
					ReplicaID: replicaID,
				}}
			}
			data, err := hb.Marshal()
			require.NoError(t, err)
			result, err := rsm.Update(sm.Entry{
				Index: rsm.state.Index + 1,
				Cmd:   GetTNStoreHeartbeatCmd(data),
			})
			require.NoError(t, err)
			return result
		}

		// Transport acknowledgement alone is insufficient: a transient local
		// start failure leaves the shard absent and must redeliver the command.
		result := heartbeat(false)
		var batch pb.CommandBatch
		require.NoError(t, batch.Unmarshal(result.Data))
		require.Equal(t, uint64(10), batch.BatchID)
		require.Equal(t, []pb.ScheduleCommand{command}, batch.Commands)
		_, ok := rsm.state.ScheduleCommands[storeID]
		require.True(t, ok)

		result = heartbeat(true)
		require.Empty(t, result.Data)
		_, ok = rsm.state.ScheduleCommands[storeID]
		require.False(t, ok)
	})
}

func TestHandleUpdateCNLabel(t *testing.T) {
	uuid := "uuid1"
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	label := pb.CNStoreLabel{
		UUID: uuid,
		Labels: map[string]metadata.LabelList{
			"account": {Labels: []string{"a", "b"}},
			"role":    {Labels: []string{"1", "2"}},
		},
	}
	cmd := GetUpdateCNLabelCmd(label)
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s := tsm1.state.CNState
	assert.Equal(t, 0, len(s.Stores))

	cmd = GetTickCmd()
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.CNStoreHeartbeat{
		UUID: uuid,
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetCNStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))

	label = pb.CNStoreLabel{
		UUID: uuid,
		Labels: map[string]metadata.LabelList{
			"account": {Labels: []string{"a", "b"}},
			"role":    {Labels: []string{"1", "2"}},
		},
	}
	cmd = GetUpdateCNLabelCmd(label)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok := s.Stores[uuid]
	assert.True(t, ok)
	labels, ok := info.Labels["account"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"a", "b"})
	labels, ok = info.Labels["role"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"1", "2"})

	label = pb.CNStoreLabel{
		UUID: uuid,
		Labels: map[string]metadata.LabelList{
			"role": {Labels: []string{"1", "2"}},
		},
	}
	cmd = GetUpdateCNLabelCmd(label)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok = s.Stores[uuid]
	assert.True(t, ok)
	_, ok = info.Labels["account"]
	assert.False(t, ok)
	labels, ok = info.Labels["role"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"1", "2"})
}

func TestHandleUpdateCNWorkState(t *testing.T) {
	uuid := "uuid1"
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	state := pb.CNWorkState{
		UUID:  uuid,
		State: metadata.WorkState_Unknown,
	}
	cmd := GetUpdateCNWorkStateCmd(state)
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s := tsm1.state.CNState
	assert.Equal(t, 0, len(s.Stores))

	cmd = GetTickCmd()
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.CNStoreHeartbeat{
		UUID: uuid,
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetCNStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))

	state = pb.CNWorkState{
		UUID:  uuid,
		State: metadata.WorkState_Working,
	}
	cmd = GetUpdateCNWorkStateCmd(state)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok := s.Stores[uuid]
	assert.True(t, ok)
	assert.Equal(t, metadata.WorkState_Working, info.WorkState)

	state = pb.CNWorkState{
		UUID:  uuid,
		State: metadata.WorkState_Unknown,
	}
	cmd = GetUpdateCNWorkStateCmd(state)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok = s.Stores[uuid]
	assert.True(t, ok)
	assert.Equal(t, metadata.WorkState_Working, info.WorkState)

	state = pb.CNWorkState{
		UUID:  uuid,
		State: metadata.WorkState_Draining,
	}
	cmd = GetUpdateCNWorkStateCmd(state)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok = s.Stores[uuid]
	assert.True(t, ok)
	assert.Equal(t, metadata.WorkState_Draining, info.WorkState)
}

func TestHandlePatchCNStore(t *testing.T) {
	uuid := "uuid1"
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	stateLabel := pb.CNStateLabel{
		UUID:  uuid,
		State: metadata.WorkState_Unknown,
		Labels: map[string]metadata.LabelList{
			"account": {Labels: []string{"a", "b"}},
			"role":    {Labels: []string{"1", "2"}},
		},
	}
	cmd := GetPatchCNStoreCmd(stateLabel)
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s := tsm1.state.CNState
	assert.Equal(t, 0, len(s.Stores))

	cmd = GetTickCmd()
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.CNStoreHeartbeat{
		UUID: uuid,
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetCNStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))

	cmd = GetPatchCNStoreCmd(stateLabel)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok := s.Stores[uuid]
	assert.True(t, ok)
	assert.Equal(t, metadata.WorkState_Working, info.WorkState)
	labels, ok := info.Labels["account"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"a", "b"})
	labels, ok = info.Labels["role"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"1", "2"})

	stateLabel = pb.CNStateLabel{
		UUID:  uuid,
		State: metadata.WorkState_Working,
	}
	cmd = GetPatchCNStoreCmd(stateLabel)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok = s.Stores[uuid]
	assert.True(t, ok)
	assert.Equal(t, metadata.WorkState_Working, info.WorkState)
	labels, ok = info.Labels["account"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"a", "b"})
	labels, ok = info.Labels["role"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"1", "2"})

	stateLabel = pb.CNStateLabel{
		UUID: uuid,
		Labels: map[string]metadata.LabelList{
			"role": {Labels: []string{"1", "2"}},
		},
	}
	cmd = GetPatchCNStoreCmd(stateLabel)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok = s.Stores[uuid]
	assert.True(t, ok)
	assert.Equal(t, metadata.WorkState_Working, info.WorkState)
	_, ok = info.Labels["account"]
	assert.False(t, ok)
	labels, ok = info.Labels["role"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"1", "2"})

	stateLabel = pb.CNStateLabel{
		UUID:  uuid,
		State: metadata.WorkState_Draining,
	}
	cmd = GetPatchCNStoreCmd(stateLabel)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s = tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))
	info, ok = s.Stores[uuid]
	assert.True(t, ok)
	assert.Equal(t, metadata.WorkState_Draining, info.WorkState)
	_, ok = info.Labels["account"]
	assert.False(t, ok)
	labels, ok = info.Labels["role"]
	assert.True(t, ok)
	assert.Equal(t, labels.Labels, []string{"1", "2"})
}

func TestHandleDeleteCNStore(t *testing.T) {
	uuid := "uuid1"
	tsm1 := NewStateMachine(0, 1).(*stateMachine)

	cmd := GetTickCmd()
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.CNStoreHeartbeat{
		UUID: uuid,
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetCNStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s := tsm1.state.CNState
	assert.Equal(t, 1, len(s.Stores))

	cnStore := pb.DeleteCNStore{
		StoreID: uuid,
	}
	cmd = GetDeleteCNStoreCmd(cnStore)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s = tsm1.state.CNState
	assert.Equal(t, 0, len(s.Stores))
}

func TestHandleProxyHeartbeat(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	cmd := GetTickCmd()
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)

	hb := pb.ProxyHeartbeat{
		UUID: "uuid1",
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetProxyHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s := tsm1.state.ProxyState
	assert.Equal(t, 1, len(s.Stores))
	info, ok := s.Stores[hb.UUID]
	assert.True(t, ok)
	assert.Equal(t, uint64(3), info.Tick)
}

func TestHandleUpdateNonVotingReplicaNum(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	cmd := GetUpdateNonVotingReplicaNumCmd(10)
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	n := tsm1.state.NonVotingReplicaNum
	assert.Equal(t, uint64(10), n)
}

func TestHandleUpdateNonVotingLocality(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	cmd := GetUpdateNonVotingLocality(pb.Locality{
		Value: map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "",
		},
	})
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	l := tsm1.state.NonVotingLocality
	assert.Equal(t, pb.Locality{
		Value: map[string]string{
			"k1": "v1",
			"k2": "v2",
		},
	}, l)

	cmd = GetUpdateNonVotingLocality(pb.Locality{
		Value: map[string]string{
			"k1": "v1",
		},
	})
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	l = tsm1.state.NonVotingLocality
	assert.Equal(t, pb.Locality{
		Value: map[string]string{
			"k1": "v1",
		},
	}, l)
}

func TestHandleLogShardUpdate(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	cmd := GetAddLogShardCmd(pb.AddLogShard{
		ShardID: 10,
	})
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	shards := tsm1.state.LogState.Shards
	_, ok := shards[10]
	assert.True(t, ok)
}
