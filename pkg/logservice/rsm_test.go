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
	"bytes"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

func TestGetLeaseHistory(t *testing.T) {
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.state.LeaseHistory[100] = 1000
	tsm.state.LeaseHistory[200] = 2000
	tsm.state.LeaseHistory[300] = 3000
	lease, index := tsm.getLeaseHistory(150)
	assert.Equal(t, uint64(1000), lease)
	assert.Equal(t, uint64(100), index)

	lease, index = tsm.getLeaseHistory(200)
	assert.Equal(t, uint64(1000), lease)
	assert.Equal(t, uint64(100), index)

	lease, index = tsm.getLeaseHistory(100)
	assert.Equal(t, uint64(0), lease)
	assert.Equal(t, uint64(0), index)

	lease, index = tsm.getLeaseHistory(400)
	assert.Equal(t, uint64(3000), lease)
	assert.Equal(t, uint64(300), index)
}

func TestTruncateLeaseHistory(t *testing.T) {
	getSM := func() *stateMachine {
		tsm := newStateMachine(1, 2).(*stateMachine)
		tsm.state.LeaseHistory[100] = 1000
		tsm.state.LeaseHistory[200] = 2000
		tsm.state.LeaseHistory[300] = 3000
		return tsm
	}

	tsm := getSM()
	tsm.truncateLeaseHistory(105)
	assert.Equal(t, 3, len(tsm.state.LeaseHistory))
	tsm.truncateLeaseHistory(200)
	assert.Equal(t, 3, len(tsm.state.LeaseHistory))
	tsm.truncateLeaseHistory(201)
	assert.Equal(t, 2, len(tsm.state.LeaseHistory))
	_, ok1 := tsm.state.LeaseHistory[200]
	_, ok2 := tsm.state.LeaseHistory[300]
	assert.True(t, ok1)
	assert.True(t, ok2)

	tsm = getSM()
	tsm.truncateLeaseHistory(300)
	assert.Equal(t, 2, len(tsm.state.LeaseHistory))
	_, ok := tsm.state.LeaseHistory[100]
	assert.False(t, ok)

	tsm = getSM()
	tsm.truncateLeaseHistory(301)
	assert.Equal(t, 1, len(tsm.state.LeaseHistory))
	_, ok = tsm.state.LeaseHistory[300]
	assert.True(t, ok)
}

func TestGetSetLeaseHolderCmd(t *testing.T) {
	cmd := getSetLeaseHolderCmd(100)
	assert.True(t, isSetLeaseHolderUpdate(cmd))
	cmd2 := getSetTruncatedLsnCmd(200)
	assert.False(t, isSetLeaseHolderUpdate(cmd2))
}

func TestIsUserUpdate(t *testing.T) {
	cmd := make([]byte, headerSize+8+1)
	binaryEnc.PutUint32(cmd, uint32(pb.UserEntryUpdate))
	assert.True(t, isUserUpdate(cmd))
	cmd2 := getSetLeaseHolderCmd(1234)
	cmd3 := getSetTruncatedLsnCmd(200)
	assert.False(t, isUserUpdate(cmd2))
	assert.False(t, isUserUpdate(cmd3))
}

func TestNewStateMachine(t *testing.T) {
	tsm := newStateMachine(100, 200).(*stateMachine)
	assert.Equal(t, uint64(100), tsm.shardID)
	assert.Equal(t, uint64(200), tsm.replicaID)
}

func TestStateMachineCanBeClosed(t *testing.T) {
	tsm := newStateMachine(100, 200)
	assert.Nil(t, tsm.Close())
}

func TestTNLeaseHolderCanBeUpdated(t *testing.T) {
	cmd := getSetLeaseHolderCmd(500)
	tsm := newStateMachine(1, 2).(*stateMachine)
	assert.Equal(t, uint64(0), tsm.state.LeaseHolderID)
	e := sm.Entry{Cmd: cmd, Index: 100}
	result, err := tsm.Update(e)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(500), tsm.state.LeaseHolderID)
	assert.Equal(t, 1, len(tsm.state.LeaseHistory))
	v, ok := tsm.state.LeaseHistory[100]
	assert.True(t, ok)
	assert.Equal(t, uint64(500), v)

	cmd = getSetLeaseHolderCmd(1000)
	e = sm.Entry{Cmd: cmd, Index: 200}
	result, err = tsm.Update(e)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1000), tsm.state.LeaseHolderID)
	assert.Equal(t, 2, len(tsm.state.LeaseHistory))
	v, ok = tsm.state.LeaseHistory[200]
	assert.True(t, ok)
	assert.Equal(t, uint64(1000), v)

	cmd = getSetTruncatedLsnCmd(110)
	e = sm.Entry{Cmd: cmd}
	result, err = tsm.Update(e)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)
	// first lease history record won't be truncated
	assert.Equal(t, 2, len(tsm.state.LeaseHistory))
}

func TestTruncatedIndexCanBeUpdated(t *testing.T) {
	cmd := getSetTruncatedLsnCmd(200)
	tsm := newStateMachine(1, 2).(*stateMachine)
	e := sm.Entry{Cmd: cmd}
	result, err := tsm.Update(e)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)

	cmd2 := getSetTruncatedLsnCmd(220)
	e2 := sm.Entry{Cmd: cmd2}
	result, err = tsm.Update(e2)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)

	cmd3 := getSetTruncatedLsnCmd(100)
	e3 := sm.Entry{Cmd: cmd3}
	result, err = tsm.Update(e3)
	assert.Equal(t, sm.Result{Value: 220}, result)
	assert.Nil(t, err)
}

func TestStateMachineUserUpdate(t *testing.T) {
	cmd := make([]byte, headerSize+8+1)
	binaryEnc.PutUint32(cmd, uint32(pb.UserEntryUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], uint64(1234))

	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.state.LeaseHolderID = 1234
	e := sm.Entry{Index: 100, Cmd: cmd}
	result, err := tsm.Update(e)
	assert.Nil(t, err)
	assert.Equal(t, e.Index, result.Value)
	assert.Nil(t, result.Data)

	tsm.state.LeaseHolderID = 2345
	e = sm.Entry{Cmd: cmd}
	result, err = tsm.Update(e)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), result.Value)
	assert.NotNil(t, result.Data)
	assert.Equal(t, tsm.state.LeaseHolderID, binaryEnc.Uint64(result.Data))
}

func TestTsoUpdate(t *testing.T) {
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.state.Tso = 200
	cmd := getTsoUpdateCmd(100)
	result, err := tsm.Update(sm.Entry{Cmd: cmd})
	assert.NoError(t, err)
	assert.Equal(t, uint64(200), result.Value)
	assert.Equal(t, uint64(300), tsm.state.Tso)
}

func TestStateMachineSnapshot(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.state.LeaseHolderID = 123456
	tsm.state.TruncatedLsn = 456789
	assert.Nil(t, tsm.SaveSnapshot(buf, nil, nil))

	tsm2 := newStateMachine(3, 4).(*stateMachine)
	assert.Nil(t, tsm2.RecoverFromSnapshot(buf, nil, nil))
	assert.Equal(t, tsm.state.LeaseHolderID, tsm2.state.LeaseHolderID)
	assert.Equal(t, tsm.state.TruncatedLsn, tsm2.state.TruncatedLsn)
	assert.Equal(t, uint64(3), tsm2.shardID)
	assert.Equal(t, uint64(4), tsm2.replicaID)
}

func TestStateMachineLookup(t *testing.T) {
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.state.Index = 1234
	tsm.state.LeaseHolderID = 123456
	tsm.state.TruncatedLsn = 456789
	tsm.state.RequiredLsn = 888999
	v, err := tsm.Lookup(leaseHolderIDQuery{})
	assert.Nil(t, err)
	assert.Equal(t, tsm.state.LeaseHolderID, v.(uint64))

	v2, err := tsm.Lookup(truncatedLsnQuery{})
	assert.Nil(t, err)
	assert.Equal(t, tsm.state.TruncatedLsn, v2.(uint64))

	v3, err := tsm.Lookup(indexQuery{})
	assert.Nil(t, err)
	assert.Equal(t, tsm.state.Index, v3.(uint64))

	v4, err := tsm.Lookup(requiredLsnQuery{})
	assert.Nil(t, err)
	assert.Equal(t, tsm.state.RequiredLsn, v4.(uint64))
}

func TestStateMachineLookupPanicOnUnexpectedInputValue(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to panic")
		}
	}()
	tsm := newStateMachine(1, 2).(*stateMachine)
	_, err := tsm.Lookup(uint16(1234))
	assert.NoError(t, err)
}

func TestStateMachineLookupPanicOnUnexpectedInputType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to panic")
		}
	}()
	tsm := newStateMachine(1, 2).(*stateMachine)
	_, err := tsm.Lookup(uint64(1234))
	assert.NoError(t, err)
}

func TestRequiredLsnUpdate(t *testing.T) {
	tsm := newStateMachine(1, 2).(*stateMachine)
	var i uint64 = 200
	tsm.state.Index = i

	i++
	cmd := getSetRequiredLsnCmd(100)
	e := sm.Entry{Index: i, Cmd: cmd}
	result, err := tsm.Update(e)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(100), tsm.state.RequiredLsn)

	i++
	cmd2 := getSetRequiredLsnCmd(180)
	e2 := sm.Entry{Index: i, Cmd: cmd2}
	result, err = tsm.Update(e2)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(180), tsm.state.RequiredLsn)

	i++
	cmd3 := getSetRequiredLsnCmd(300)
	e3 := sm.Entry{Index: i, Cmd: cmd3}
	result, err = tsm.Update(e3)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(203), tsm.state.RequiredLsn)

	i++
	cmd4 := getSetRequiredLsnCmd(90)
	e4 := sm.Entry{Index: i, Cmd: cmd4}
	result, err = tsm.Update(e4)
	assert.Equal(t, sm.Result{Value: 203}, result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(203), tsm.state.RequiredLsn)
}
