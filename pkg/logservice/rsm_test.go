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
	"errors"
	"strings"
	"testing"

	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestWALRecoveryLeaseFence(t *testing.T) {
	tsm := newStateMachine(1, 2).(*stateMachine)
	digest := strings.Repeat("a", walRecoveryDigestSize)
	otherDigest := strings.Repeat("b", walRecoveryDigestSize)

	result, err := tsm.Update(sm.Entry{Index: 1, Cmd: getSetLeaseHolderCmd(42)})
	assert.NoError(t, err)
	assert.Empty(t, result.Data)
	result, err = tsm.Update(sm.Entry{Index: 2, Cmd: getEndWALRecoveryCmd(digest, 1)})
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), binaryEnc.Uint64(result.Data))
	assert.Equal(t, uint64(42), tsm.state.LeaseHolderID)

	normalUserCmd := buildUserEntryCmd(42, []byte("normal"))
	result, err = tsm.Update(sm.Entry{Index: 3, Cmd: normalUserCmd})
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), result.Value)

	result, err = tsm.Update(sm.Entry{Index: 4, Cmd: getBeginWALRecoveryCmd(digest, 1)})
	assert.NoError(t, err)
	assert.Equal(t, uint64(4), result.Value)
	assert.Equal(t, walRecoveryLeaseHolderID, tsm.state.LeaseHolderID)
	assert.Equal(t, digest, tsm.state.WALRecoveryDigest)
	assert.Equal(t, uint64(1), tsm.state.WALRecoveryEntryCount)
	assert.Equal(t, uint64(4), tsm.state.WALRecoveryBaseLsn)
	assert.Equal(t, uint64(42), tsm.state.WALRecoveryOriginalLeaseHolderID)

	result, err = tsm.Update(sm.Entry{Index: 5, Cmd: getSetLeaseHolderCmd(99)})
	assert.NoError(t, err)
	assert.Equal(t, walRecoveryLeaseHolderID, binaryEnc.Uint64(result.Data))
	assert.Equal(t, walRecoveryLeaseHolderID, tsm.state.LeaseHolderID)

	result, err = tsm.Update(sm.Entry{Index: 6, Cmd: normalUserCmd})
	assert.NoError(t, err)
	assert.Equal(t, walRecoveryLeaseHolderID, binaryEnc.Uint64(result.Data))

	recoveryUserCmd := buildUserEntryCmd(walRecoveryLeaseHolderID, []byte("recovery"))
	result, err = tsm.Update(sm.Entry{Index: 7, Cmd: recoveryUserCmd})
	assert.NoError(t, err)
	assert.Equal(t, uint64(7), result.Value)
	assert.Equal(t, uint64(1), tsm.state.WALRecoveryCompletedEntries)
	assert.Equal(t, uint64(7), tsm.state.WALRecoveryLastLsn)

	result, err = tsm.Update(sm.Entry{Index: 8, Cmd: recoveryUserCmd})
	assert.NoError(t, err)
	assert.Equal(t, walRecoveryLeaseHolderID, binaryEnc.Uint64(result.Data))

	result, err = tsm.Update(sm.Entry{Index: 9, Cmd: getBeginWALRecoveryCmd(otherDigest, 1)})
	assert.NoError(t, err)
	assert.Equal(t, walRecoveryLeaseHolderID, binaryEnc.Uint64(result.Data))

	result, err = tsm.Update(sm.Entry{Index: 10, Cmd: getBeginWALRecoveryCmd(digest, 1)})
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), result.Value)
	assert.Equal(t, walRecoveryLeaseHolderID, tsm.state.LeaseHolderID)

	result, err = tsm.Update(sm.Entry{Index: 11, Cmd: getEndWALRecoveryCmd(digest, 1)})
	assert.NoError(t, err)
	assert.Equal(t, uint64(11), result.Value)
	assert.Equal(t, uint64(42), tsm.state.LeaseHolderID)
	assert.True(t, tsm.state.WALRecoveryComplete)

	result, err = tsm.Update(sm.Entry{Index: 12, Cmd: normalUserCmd})
	assert.NoError(t, err)
	assert.Equal(t, uint64(12), result.Value)

	value, err := tsm.Lookup(walRecoveryStateQuery{})
	assert.NoError(t, err)
	assert.Equal(t, walRecoveryMarkerState{
		Digest:                digest,
		EntryCount:            1,
		CompletedEntries:      1,
		BaseLSN:               4,
		LastLSN:               7,
		OriginalLeaseHolderID: 42,
		Complete:              true,
	}, value)
}

func TestTruncatedIndexCanBeUpdated(t *testing.T) {
	cmd := getSetTruncatedLsnCmd(200)
	tsm := newStateMachine(1, 2).(*stateMachine)
	e := sm.Entry{Index: 201, Cmd: cmd}
	result, err := tsm.Update(e)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)

	cmd2 := getSetTruncatedLsnCmd(220)
	e2 := sm.Entry{Index: 221, Cmd: cmd2}
	result, err = tsm.Update(e2)
	assert.Equal(t, sm.Result{}, result)
	assert.Nil(t, err)

	cmd3 := getSetTruncatedLsnCmd(100)
	e3 := sm.Entry{Index: 222, Cmd: cmd3}
	result, err = tsm.Update(e3)
	assert.Equal(t, sm.Result{Value: 220}, result)
	assert.Nil(t, err)
}

func TestTruncateRejectsFutureAndCurrentEntry(t *testing.T) {
	tsm := newStateMachine(1, 2).(*stateMachine)
	for _, index := range []uint64{5, 6} {
		result, err := tsm.Update(sm.Entry{Index: index, Cmd: getSetTruncatedLsnCmd(6)})
		require.NoError(t, err)
		assert.Equal(t, index, result.Value)
		assert.Zero(t, tsm.state.TruncatedLsn)
	}
	result, err := tsm.Update(sm.Entry{Index: 7, Cmd: getSetTruncatedLsnCmd(4)})
	require.NoError(t, err)
	assert.Equal(t, sm.Result{}, result)
	assert.Equal(t, uint64(4), tsm.state.TruncatedLsn)
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
	assert.Equal(t, uint64(300), tsm.state.RequiredLsn)

	i++
	cmd4 := getSetRequiredLsnCmd(90)
	e4 := sm.Entry{Index: i, Cmd: cmd4}
	result, err = tsm.Update(e4)
	assert.Equal(t, sm.Result{Value: 300}, result)
	assert.Nil(t, err)
	assert.Equal(t, uint64(300), tsm.state.RequiredLsn)
}

func TestRequiredLsnOwnerFencesResetAndDelayedAdvance(t *testing.T) {
	s := newStateMachine(1, 2).(*stateMachine)
	term := uint64(1)
	s.entryTerm = func(uint64) (uint64, error) { return term, nil }
	index := uint64(1)
	apply := func(cmd []byte) sm.Result {
		index++
		result, err := s.Update(sm.Entry{Index: index, Cmd: cmd})
		assert.NoError(t, err)
		return result
	}

	assert.Zero(t, apply(getSetRequiredLsnCmd(100)).Value)
	assert.Zero(t, apply(getClaimRequiredLsnCmd(100, 100, 11, 1, 2)).Value)
	assert.Zero(t, apply(getSetRequiredLsnIfCurrentCmd(120, 100, 101, 11)).Value)
	assert.NotZero(t, apply(getSetRequiredLsnIfCurrentCmd(0, 100, 101, 11)).Value)
	assert.Equal(t, uint64(120), s.state.RequiredLsn)
	assert.Zero(t, apply(getSetRequiredLsnIfCurrentCmd(0, 120, 102, 11)).Value)
	assert.Equal(t, uint64(0), s.state.RequiredLsn)
	term = 2
	assert.Zero(t, apply(getClaimRequiredLsnCmd(0, 103, 22, 2, 3)).Value)
	assert.NotZero(t, apply(getClaimRequiredLsnCmd(0, 104, 11, 1, 2)).Value)
	assert.NotZero(t, apply(getSetRequiredLsnIfCurrentCmd(500, 0, 103, 11)).Value)
	assert.NotZero(t, apply(getSetRequiredLsnIfCurrentCmd(500, 0, 104, 11)).Value)
	assert.Zero(t, apply(getSetRequiredLsnIfCurrentCmd(1, 0, 104, 22)).Value)
	assert.NotEmpty(t, apply(getSetRequiredLsnCmd(500)).Data)
	assert.Equal(t, uint64(1), s.state.RequiredLsn)

	var buf bytes.Buffer
	assert.NoError(t, s.SaveSnapshot(&buf, nil, nil))
	restored := newStateMachine(1, 3).(*stateMachine)
	assert.NoError(t, restored.RecoverFromSnapshot(&buf, nil, nil))
	assert.Equal(t, s.state.RequiredLsn, restored.state.RequiredLsn)
	assert.Equal(t, s.state.RequiredLsnRevision, restored.state.RequiredLsnRevision)
	assert.Equal(t, s.state.RequiredLsnOwner, restored.state.RequiredLsnOwner)
	assert.Equal(t, s.state.RequiredLsnLeaderTerm, restored.state.RequiredLsnLeaderTerm)
}

func TestRequiredLsnLegacySnapshotsAgreeAfterDifferentReplay(t *testing.T) {
	restoreLegacy := func(index, lsn uint64) *stateMachine {
		legacy := newStateMachine(1, 2).(*stateMachine)
		legacy.state.Index = index
		legacy.state.RequiredLsn = lsn
		var snapshot bytes.Buffer
		require.NoError(t, legacy.SaveSnapshot(&snapshot, nil, nil))
		restored := newStateMachine(1, 3).(*stateMachine)
		restored.state.RequiredLsnConditional = true
		restored.state.RequiredLsnRevision = 999
		restored.state.RequiredLsnOwner = 99
		require.NoError(t, restored.RecoverFromSnapshot(&snapshot, nil, nil))
		require.False(t, restored.state.RequiredLsnConditional)
		require.Zero(t, restored.state.RequiredLsnOwner)
		restored.entryTerm = func(uint64) (uint64, error) { return 1, nil }
		return restored
	}

	early := restoreLegacy(10, 100)
	late := restoreLegacy(15, 200)
	result, err := early.Update(sm.Entry{Index: 12, Cmd: getSetRequiredLsnCmd(200)})
	require.NoError(t, err)
	require.Zero(t, result.Value)
	for index := uint64(13); index <= 15; index++ {
		_, err = early.Update(sm.Entry{Index: index, Cmd: getTsoUpdateCmd(0)})
		require.NoError(t, err)
	}
	require.Equal(t, late.state.RequiredLsnRevision, early.state.RequiredLsnRevision)
	require.Equal(t, uint64(200), early.state.RequiredLsnRevision)

	claim := getClaimRequiredLsnCmd(200, 200, 11, 1, 2)
	for _, replica := range []*stateMachine{early, late} {
		result, err = replica.Update(sm.Entry{Index: 16, Cmd: claim})
		require.NoError(t, err)
		require.Zero(t, result.Value)
	}
	require.Equal(t, early.state.RequiredLsnRevision, late.state.RequiredLsnRevision)
	require.Equal(t, early.state.RequiredLsnOwner, late.state.RequiredLsnOwner)
}

func TestRequiredLsnRejectsStaleOwnerAtCommittedTerm(t *testing.T) {
	s := newStateMachine(1, 2).(*stateMachine)
	term := uint64(1)
	s.entryTerm = func(uint64) (uint64, error) { return term, nil }
	index := uint64(1)
	apply := func(cmd []byte) sm.Result {
		index++
		result, err := s.Update(sm.Entry{Index: index, Cmd: cmd})
		require.NoError(t, err)
		return result
	}
	require.Zero(t, apply(getClaimRequiredLsnCmd(0, 0, 11, 1, 2)).Value)
	require.Zero(t, apply(getSetRequiredLsnIfCurrentCmd(100, 0, 1, 11)).Value)
	term = 2 // election after the RPC's leader check but before Raft commit
	require.NotZero(t, apply(getSetRequiredLsnIfCurrentCmd(0, 100, 2, 11)).Value)
	require.NotZero(t, apply(getClaimRequiredLsnCmd(100, 2, 12, 1, 2)).Value)
	require.Equal(t, uint64(100), s.state.RequiredLsn)
	require.Zero(t, apply(getClaimRequiredLsnCmd(100, 2, 22, 2, 3)).Value)
	require.Zero(t, apply(getSetRequiredLsnIfCurrentCmd(0, 100, 3, 22)).Value)
	require.Zero(t, s.state.RequiredLsn)
}

func TestRequiredLsnTermLookupFailureDoesNotApplyClaim(t *testing.T) {
	s := newStateMachine(1, 2).(*stateMachine)
	s.entryTerm = func(uint64) (uint64, error) { return 0, errors.New("log reader unavailable") }
	_, err := s.Update(sm.Entry{Index: 2, Cmd: getClaimRequiredLsnCmd(0, 0, 11, 1, 2)})
	require.ErrorContains(t, err, "log reader unavailable")
	require.Zero(t, s.state.RequiredLsnOwner)
}

func TestWALTruncatePreservesCheckpointedDSN(t *testing.T) {
	s := newStateMachine(1, 2).(*stateMachine)
	index := uint64(100)
	apply := func(cmd []byte) sm.Result {
		index++
		result, err := s.Update(sm.Entry{Index: index, Cmd: cmd})
		assert.NoError(t, err)
		return result
	}
	assert.NotZero(t, apply(getWALTruncateCmd(0, 0)).Value)
	assert.NotZero(t, apply(getWALTruncateCmd(1, 0)).Value)
	assert.NotZero(t, apply(getWALTruncateCmd(index+1, 7)).Value)
	assert.Zero(t, apply(getWALTruncateCmd(10, 7)).Value)
	assert.NotZero(t, apply(getWALTruncateCmd(9, 8)).Value)
	assert.NotZero(t, apply(getWALTruncateCmd(11, 6)).Value)
	assert.Equal(t, walTruncateState{10, 7}, walTruncateState{s.state.TruncatedLsn, s.state.CheckpointedDSN})
	assert.Zero(t, apply(getWALTruncateCmd(11, 8)).Value)
	assert.NotZero(t, apply(getSetTruncatedLsnCmd(12)).Value)
	assert.Equal(t, uint64(8), s.state.CheckpointedDSN)

	var buf bytes.Buffer
	assert.NoError(t, s.SaveSnapshot(&buf, nil, nil))
	restored := newStateMachine(1, 3).(*stateMachine)
	assert.NoError(t, restored.RecoverFromSnapshot(&buf, nil, nil))
	assert.Equal(t, walTruncateState{11, 8},
		walTruncateState{restored.state.TruncatedLsn, restored.state.CheckpointedDSN})
}
