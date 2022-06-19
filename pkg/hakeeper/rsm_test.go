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
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hapb "github.com/matrixorigin/matrixone/pkg/pb/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

func TestAssignID(t *testing.T) {
	tsm := NewStateMachine(0, 1).(*stateMachine)
	assert.Equal(t, uint64(0), tsm.NextID)
	assert.Equal(t, uint64(1), tsm.assignID())
	assert.Equal(t, uint64(1), tsm.NextID)
}

func TestCreateLogShardCmd(t *testing.T) {
	cmd := getCreateLogShardCmd("test")
	name, ok := isCreateLogShardCmd(cmd)
	assert.True(t, ok)
	assert.Equal(t, "test", name)
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
	tsm1.NextID = 12345
	tsm1.LogShards["test1"] = 23456
	tsm1.LogShards["test2"] = 34567

	buf := bytes.NewBuffer(nil)
	assert.Nil(t, tsm1.SaveSnapshot(buf, nil, nil))
	assert.Nil(t, tsm2.RecoverFromSnapshot(buf, nil, nil))
	assert.Equal(t, tsm1.NextID, tsm2.NextID)
	assert.Equal(t, tsm1.LogShards, tsm2.LogShards)
	assert.True(t, tsm1.replicaID != tsm2.replicaID)
}

func TestHAKeeperLogShardCanBeCreated(t *testing.T) {
	cmd := getCreateLogShardCmd("test1")
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	tsm1.NextID = 100

	result, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.Nil(t, err)
	assert.Equal(t, sm.Result{Value: 101}, result)
	assert.Equal(t, uint64(101), tsm1.NextID)

	tsm1.NextID = 200
	result, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.Nil(t, err)
	data := make([]byte, 8)
	binaryEnc.PutUint64(data, 101)
	assert.Equal(t, sm.Result{Data: data}, result)
}

func TestHAKeeperQueryLogShardID(t *testing.T) {
	cmd := getCreateLogShardCmd("test1")
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	tsm1.NextID = 100
	result, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.Nil(t, err)
	assert.Equal(t, sm.Result{Value: 101}, result)

	q1 := &logShardIDQuery{name: "test1"}
	r, err := tsm1.Lookup(q1)
	assert.NoError(t, err)
	r1, ok := r.(*logShardIDQueryResult)
	assert.True(t, ok)
	assert.True(t, r1.found)
	assert.Equal(t, uint64(101), r1.id)

	q2 := &logShardIDQuery{name: "test2"}
	r, err = tsm1.Lookup(q2)
	assert.NoError(t, err)
	r2, ok := r.(*logShardIDQueryResult)
	assert.True(t, ok)
	assert.False(t, r2.found)
}

func TestHAKeeperCanBeClosed(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	assert.Nil(t, tsm1.Close())
}

func TestHAKeeperTick(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
	assert.Equal(t, uint64(0), tsm1.Tick)
	cmd := GetTickCmd()
	_, err := tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), tsm1.Tick)
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
		Shards: []pb.LogShardInfo{
			{
				ShardID: 100,
				Replicas: map[uint64]string{
					200: "localhost:8000",
					300: "localhost:9000",
				},
				Epoch:    200,
				LeaderID: 200,
				Term:     10,
			},
			{
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
	}
	data, err := hb.Marshal()
	require.NoError(t, err)
	cmd = GetLogStoreHeartbeatCmd(data)
	_, err = tsm1.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	s := tsm1.LogState
	assert.Equal(t, 1, len(s.Stores))
	lsinfo, ok := s.Stores[hb.UUID]
	require.True(t, ok)
	assert.Equal(t, uint64(3), lsinfo.Tick)
	assert.Equal(t, hb.RaftAddress, lsinfo.RaftAddress)
	assert.Equal(t, hb.ServiceAddress, lsinfo.ServiceAddress)
	assert.Equal(t, hb.GossipAddress, lsinfo.GossipAddress)
	assert.Equal(t, 2, len(lsinfo.Shards))
	assert.Equal(t, hb.Shards, lsinfo.Shards)
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
	s := tsm1.DNState
	assert.Equal(t, 1, len(s.Stores))
	dninfo, ok := s.Stores[hb.UUID]
	assert.True(t, ok)
	assert.Equal(t, uint64(3), dninfo.Tick)
	require.Equal(t, 3, len(dninfo.Shards))
	assert.Equal(t, hb.Shards, dninfo.Shards)
}

func TestGetIDCmd(t *testing.T) {
	tsm1 := NewStateMachine(0, 1).(*stateMachine)
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
	sc1 := hapb.ScheduleCommand{
		UUID: "uuid1",
		ConfigChange: hapb.ConfigChange{
			Replica: hapb.Replica{
				ShardID: 1,
			},
		},
	}
	sc2 := hapb.ScheduleCommand{
		UUID: "uuid2",
		ConfigChange: hapb.ConfigChange{
			Replica: hapb.Replica{
				ShardID: 2,
			},
		},
	}
	sc3 := hapb.ScheduleCommand{
		UUID: "uuid1",
		ConfigChange: hapb.ConfigChange{
			Replica: hapb.Replica{
				ShardID: 3,
			},
		},
	}
	sc4 := hapb.ScheduleCommand{
		UUID: "uuid3",
		ConfigChange: hapb.ConfigChange{
			Replica: hapb.Replica{
				ShardID: 4,
			},
		},
	}

	b := hapb.CommandBatch{
		Term:     101,
		Commands: []hapb.ScheduleCommand{sc1, sc2, sc3},
	}
	cmd := GetUpdateCommandsCmd(b.Term, b.Commands)
	result, err := tsm1.Update(sm.Entry{Cmd: cmd})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, b.Term, tsm1.term)
	require.Equal(t, 2, len(tsm1.scheduleCommands))
	l1, ok := tsm1.scheduleCommands["uuid1"]
	assert.True(t, ok)
	assert.Equal(t, []hapb.ScheduleCommand{sc1, sc3}, l1)
	l2, ok := tsm1.scheduleCommands["uuid2"]
	assert.True(t, ok)
	assert.Equal(t, []hapb.ScheduleCommand{sc2}, l2)

	cmd2 := GetUpdateCommandsCmd(b.Term-1,
		[]hapb.ScheduleCommand{sc1, sc2, sc3, sc4})
	result, err = tsm1.Update(sm.Entry{Cmd: cmd2})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, b.Term, tsm1.term)
	require.Equal(t, 2, len(tsm1.scheduleCommands))
	l1, ok = tsm1.scheduleCommands["uuid1"]
	assert.True(t, ok)
	assert.Equal(t, []hapb.ScheduleCommand{sc1, sc3}, l1)
	l2, ok = tsm1.scheduleCommands["uuid2"]
	assert.True(t, ok)
	assert.Equal(t, []hapb.ScheduleCommand{sc2}, l2)
}
