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

	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/stretchr/testify/assert"
)

func TestGetLeaseHistory(t *testing.T) {
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.LeaseHistory[100] = 1000
	tsm.LeaseHistory[200] = 2000
	tsm.LeaseHistory[300] = 3000
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
		tsm.LeaseHistory[100] = 1000
		tsm.LeaseHistory[200] = 2000
		tsm.LeaseHistory[300] = 3000
		return tsm
	}

	tsm := getSM()
	tsm.truncateLeaseHistory(105)
	assert.Equal(t, 3, len(tsm.LeaseHistory))
	tsm.truncateLeaseHistory(200)
	assert.Equal(t, 3, len(tsm.LeaseHistory))
	tsm.truncateLeaseHistory(201)
	assert.Equal(t, 2, len(tsm.LeaseHistory))
	_, ok1 := tsm.LeaseHistory[200]
	_, ok2 := tsm.LeaseHistory[300]
	assert.True(t, ok1)
	assert.True(t, ok2)

	tsm = getSM()
	tsm.truncateLeaseHistory(300)
	assert.Equal(t, 2, len(tsm.LeaseHistory))
	_, ok := tsm.LeaseHistory[100]
	assert.False(t, ok)

	tsm = getSM()
	tsm.truncateLeaseHistory(301)
	assert.Equal(t, 1, len(tsm.LeaseHistory))
	_, ok = tsm.LeaseHistory[300]
	assert.True(t, ok)
}

func TestGetSetLeaseHolderCmd(t *testing.T) {
	cmd := getSetLeaseHolderCmd(100)
	assert.True(t, isSetLeaseHolderUpdate(cmd))
	cmd2 := getSetTruncatedIndexCmd(200)
	assert.False(t, isSetLeaseHolderUpdate(cmd2))
}

func TestGetSetTruncatedIndexCmd(t *testing.T) {
	cmd := getSetTruncatedIndexCmd(1234)
	assert.True(t, isSetTruncatedIndexUpdate(cmd))
	cmd2 := getSetLeaseHolderCmd(1234)
	assert.False(t, isSetTruncatedIndexUpdate(cmd2))
}

func TestIsUserUpdate(t *testing.T) {
	cmd := make([]byte, headerSize+8+1)
	binaryEnc.PutUint16(cmd, userEntryTag)
	assert.True(t, isUserUpdate(cmd))
	cmd2 := getSetLeaseHolderCmd(1234)
	cmd3 := getSetTruncatedIndexCmd(200)
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

func TestDNLeaseHolderCanBeUpdated(t *testing.T) {
	cmd := getSetLeaseHolderCmd(500)
	tsm := newStateMachine(1, 2).(*stateMachine)
	assert.Equal(t, uint64(0), tsm.LeaseHolderID)
	entries := []sm.Entry{{Cmd: cmd, Index: 100}}
	result, err := tsm.Update(entries)
	assert.Equal(t, sm.Result{}, result[0].Result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(500), tsm.LeaseHolderID)
	assert.Equal(t, 1, len(tsm.LeaseHistory))
	v, ok := tsm.LeaseHistory[100]
	assert.True(t, ok)
	assert.Equal(t, uint64(500), v)

	cmd = getSetLeaseHolderCmd(1000)
	entries = []sm.Entry{{Cmd: cmd, Index: 200}}
	result, err = tsm.Update(entries)
	assert.Equal(t, sm.Result{}, result[0].Result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1000), tsm.LeaseHolderID)
	assert.Equal(t, 2, len(tsm.LeaseHistory))
	v, ok = tsm.LeaseHistory[200]
	assert.True(t, ok)
	assert.Equal(t, uint64(1000), v)

	cmd = getSetTruncatedIndexCmd(110)
	entries = []sm.Entry{{Cmd: cmd}}
	result, err = tsm.Update(entries)
	assert.Equal(t, sm.Result{}, result[0].Result)
	assert.Nil(t, err)
	// first lease history record won't be truncated
	assert.Equal(t, 2, len(tsm.LeaseHistory))
}

func TestTruncatedIndexCanBeUpdated(t *testing.T) {
	cmd := getSetTruncatedIndexCmd(200)
	tsm := newStateMachine(1, 2).(*stateMachine)
	entries := []sm.Entry{{Cmd: cmd}}
	result, err := tsm.Update(entries)
	assert.Equal(t, sm.Result{}, result[0].Result)
	assert.Nil(t, err)

	cmd2 := getSetTruncatedIndexCmd(220)
	entries2 := []sm.Entry{{Cmd: cmd2}}
	result, err = tsm.Update(entries2)
	assert.Equal(t, sm.Result{}, result[0].Result)
	assert.Nil(t, err)

	cmd3 := getSetTruncatedIndexCmd(100)
	entries3 := []sm.Entry{{Cmd: cmd3}}
	result, err = tsm.Update(entries3)
	assert.Equal(t, sm.Result{Value: 220}, result[0].Result)
	assert.Nil(t, err)
}

func TestStateMachineUserUpdate(t *testing.T) {
	cmd := make([]byte, headerSize+8+1)
	binaryEnc.PutUint16(cmd, userEntryTag)
	binaryEnc.PutUint64(cmd[headerSize:], uint64(1234))

	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.LeaseHolderID = 1234
	entries := []sm.Entry{{Cmd: cmd}}
	result, err := tsm.Update(entries)
	assert.Nil(t, err)
	assert.Equal(t, sm.Result{}, result[0].Result)

	tsm.LeaseHolderID = 2345
	entries = []sm.Entry{{Cmd: cmd}}
	result, err = tsm.Update(entries)
	assert.Nil(t, err)
	assert.Equal(t, sm.Result{Value: 2345}, result[0].Result)
}

func TestStateMachineSnapshot(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.LeaseHolderID = 123456
	tsm.TruncatedIndex = 456789
	sess, err := tsm.PrepareSnapshot()
	assert.NoError(t, err)
	assert.Nil(t, tsm.SaveSnapshot(sess, buf, nil, nil))

	tsm2 := newStateMachine(3, 4).(*stateMachine)
	assert.Nil(t, tsm2.RecoverFromSnapshot(buf, nil, nil))
	assert.Equal(t, tsm.LeaseHolderID, tsm2.LeaseHolderID)
	assert.Equal(t, tsm.TruncatedIndex, tsm2.TruncatedIndex)
	assert.Equal(t, uint64(3), tsm2.shardID)
	assert.Equal(t, uint64(4), tsm2.replicaID)
}

func TestStateMachineLookup(t *testing.T) {
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.LeaseHolderID = 123456
	tsm.TruncatedIndex = 456789
	v, err := tsm.Lookup(leaseHolderIDTag)
	assert.Nil(t, err)
	assert.Equal(t, tsm.LeaseHolderID, v.(uint64))

	v2, err := tsm.Lookup(truncatedIndexTag)
	assert.Nil(t, err)
	assert.Equal(t, tsm.TruncatedIndex, v2.(uint64))
}

func TestStateMachineLookupPanicOnUnexpectedInputValue(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to panic")
		}
	}()
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.Lookup(uint16(1234))
}

func TestStateMachineLookupPanicOnUnexpectedInputType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to panic")
		}
	}()
	tsm := newStateMachine(1, 2).(*stateMachine)
	tsm.Lookup(uint64(1234))
}
