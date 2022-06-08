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
