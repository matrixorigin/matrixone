// Copyright 2021 Matrix Origin
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

package dispatch

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestPrepareRemote(t *testing.T) {
	_ = colexec.NewServer(nil)

	proc := testutil.NewProcess(t)

	uid, err := uuid.NewV7()
	require.NoError(t, err)

	d := Dispatch{
		FuncId: SendToAllFunc,
		ctr:    &container{},
		RemoteRegs: []colexec.ReceiveInfo{
			{Uuid: uid},
		},
	}

	// uuid map should have this pipeline information after prepare remote.
	require.NoError(t, d.prepareRemote(proc))

	p, c, b := colexec.Get().GetProcByUuid(uid, false)
	require.True(t, b)
	require.Equal(t, proc, p)
	require.Equal(t, d.ctr.remoteInfo, c)
}

// TestReceiverDone_OldBehavior tests the old behavior (kept for backward compatibility verification)
func TestReceiverDone_OldBehavior(t *testing.T) {
	proc := testutil.NewProcess(t)
	d := &Dispatch{
		ctr: &container{},
	}
	d.ctr.localRegsCnt = 1
	d.ctr.remoteReceivers = make([]*process.WrapCs, 1)
	d.ctr.remoteReceivers[0] = &process.WrapCs{ReceiverDone: true, Err: make(chan error, 2)}
	d.ctr.remoteToIdx = make(map[uuid.UUID]int)
	d.ctr.remoteToIdx[d.ctr.remoteReceivers[0].Uid] = 0
	bat := batch.New(nil)
	bat.SetRowCount(1)

	// Note: After fix, these should return errors in strict mode
	err := sendBatToIndex(d, proc, bat, 0)
	require.Error(t, err, "shuffle should fail when receiver is done")

	err = sendBatToMultiMatchedReg(d, proc, bat, 0)
	require.Error(t, err, "shuffle should fail when receiver is done")
}

func Test_waitRemoteRegsReady(t *testing.T) {
	d := &Dispatch{
		ctr: &container{},
		RemoteRegs: []colexec.ReceiveInfo{
			{},
		},
	}
	proc := testutil.NewProcess(t)
	//wait waitNotifyTimeout seconds
	ret, err := d.waitRemoteRegsReady(proc)
	assert.Error(t, err)
	assert.False(t, ret)
}

func Test_removeIdxReceiver(t *testing.T) {
	d := &Dispatch{
		ctr: &container{},
	}

	w1 := &process.WrapCs{}
	w2 := &process.WrapCs{}
	w3 := &process.WrapCs{}
	d.ctr.remoteReceivers = []*process.WrapCs{w1, w2, w3}
	d.ctr.remoteRegsCnt = 3
	d.ctr.aliveRegCnt = 10

	d.ctr.removeIdxReceiver(1)

	require.Equal(t, 9, d.ctr.aliveRegCnt)
	require.Equal(t, 2, d.ctr.remoteRegsCnt)
	require.Equal(t, 2, len(d.ctr.remoteReceivers))
	require.Equal(t, w1, d.ctr.remoteReceivers[0])
	require.Equal(t, w3, d.ctr.remoteReceivers[1])
}

// TestSendBatchToClientSession_StrictMode tests that strict mode returns error when ReceiverDone=true
func TestSendBatchToClientSession_StrictMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	wcs := &process.WrapCs{
		ReceiverDone: true,
		Err:          make(chan error, 1),
	}

	// Test strict mode - should return error
	done, err := sendBatchToClientSession(
		proc.Ctx,
		[]byte("test data"),
		wcs,
		FailureModeStrict,
		"test-receiver-strict",
	)

	require.True(t, done, "receiver should be marked as done")
	require.Error(t, err, "strict mode MUST return error when ReceiverDone=true")
	require.Contains(t, err.Error(), "test-receiver-strict", "error should contain receiver ID")
	require.Contains(t, err.Error(), "data loss may occur", "error should mention data loss risk")
}

// TestSendBatchToClientSession_TolerantMode tests that tolerant mode does NOT return error when ReceiverDone=true
func TestSendBatchToClientSession_TolerantMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	wcs := &process.WrapCs{
		ReceiverDone: true,
		Err:          make(chan error, 1),
	}

	// Test tolerant mode - should NOT return error
	done, err := sendBatchToClientSession(
		proc.Ctx,
		[]byte("test data"),
		wcs,
		FailureModeTolerant,
		"test-receiver-tolerant",
	)

	require.True(t, done, "receiver should be marked as done")
	require.NoError(t, err, "tolerant mode should NOT return error when ReceiverDone=true")
}

// TestSendToAllRemoteFunc_ReceiverFailure tests SendToAll scenario with receiver failure
func TestSendToAllRemoteFunc_ReceiverFailure(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Create a dispatch with one failed receiver
	d := &Dispatch{
		ctr: &container{
			prepared:      true,
			remoteRegsCnt: 1,
			remoteReceivers: []*process.WrapCs{
				{
					ReceiverDone: true, // Simulate CN failure
					Err:          make(chan error, 1),
				},
			},
		},
	}

	bat := batch.New(nil)
	bat.SetRowCount(1)

	// SendToAll should fail when any receiver is done
	end, err := sendToAllRemoteFunc(bat, d, proc)

	require.False(t, end, "should not be marked as end")
	require.Error(t, err, "SendToAll MUST fail when receiver is done to prevent data loss")
	require.Contains(t, err.Error(), "data loss may occur", "error should mention data loss")
}

// TestSendToAnyRemoteFunc_ReceiverFailure tests SendToAny scenario with receiver failure
func TestSendToAnyRemoteFunc_ReceiverFailure(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Create a dispatch with all receivers failed (to test failure path)
	uid1, _ := uuid.NewV7()

	d := &Dispatch{
		ctr: &container{
			prepared:      true,
			remoteRegsCnt: 1,
			localRegsCnt:  0,
			aliveRegCnt:   1,
			sendCnt:       0,
			remoteReceivers: []*process.WrapCs{
				{
					Uid:          uid1,
					ReceiverDone: true, // Receiver failed
					Err:          make(chan error, 1),
				},
			},
		},
	}

	bat := batch.New(nil)
	bat.SetRowCount(1)

	// SendToAny should fail when all receivers are unavailable
	// But it should use tolerant mode (try to failover first)
	end, err := sendToAnyRemoteFunc(bat, d, proc)

	// Should fail because all receivers are unavailable
	require.False(t, end, "should not be marked as end")
	require.Error(t, err, "should fail when all receivers unavailable")
	require.Contains(t, err.Error(), "unavailable", "error should mention unavailability")
}

// TestShuffleScenario_TargetReceiverFailed tests shuffle with specific target receiver failed
func TestShuffleScenario_TargetReceiverFailed(t *testing.T) {
	proc := testutil.NewProcess(t)

	uid := uuid.UUID{}
	d := &Dispatch{
		ctr: &container{
			remoteRegsCnt: 1,
			remoteReceivers: []*process.WrapCs{
				{
					Uid:          uid,
					ReceiverDone: true, // Target receiver failed
					Err:          make(chan error, 1),
				},
			},
			remoteToIdx: map[uuid.UUID]int{
				uid: 0, // This receiver handles shuffle index 0
			},
		},
	}

	bat := batch.New(nil)
	bat.SetRowCount(1)
	bat.ShuffleIDX = 0 // This batch should go to receiver 0

	// Shuffle should fail because target receiver is done
	err := sendBatToIndex(d, proc, bat, 0)
	require.Error(t, err, "shuffle MUST fail when target receiver is done")
	require.Contains(t, err.Error(), "data loss may occur", "error should mention data loss")
}

// TestDataLossPrevention_ComparisonTable documents the fix behavior
func TestDataLossPrevention_ComparisonTable(t *testing.T) {
	t.Run("SendToAll_Before_Fix", func(t *testing.T) {
		// Before fix: ReceiverDone=true was silently ignored
		// Result: Query "succeeds" but returns incomplete data (CN2's data lost)
		// This was the CRITICAL BUG
		t.Log("Before fix: SendToAll silently skipped failed receivers")
		t.Log("Result: Users got incomplete data without knowing it")
	})

	t.Run("SendToAll_After_Fix", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		wcs := &process.WrapCs{ReceiverDone: true, Err: make(chan error, 1)}

		// After fix: ReceiverDone=true returns error in strict mode
		_, err := sendBatchToClientSession(proc.Ctx, []byte("test"), wcs, FailureModeStrict, "CN2")

		require.Error(t, err, "After fix: SendToAll MUST report error")
		require.Contains(t, err.Error(), "data loss may occur")
		t.Log("After fix: Query fails with clear error message")
		t.Log("Result: Users know data is incomplete and can retry")
	})

	t.Run("SendToAny_Still_Works", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		wcs := &process.WrapCs{ReceiverDone: true, Err: make(chan error, 1)}

		// SendToAny uses tolerant mode - should still work
		_, err := sendBatchToClientSession(proc.Ctx, []byte("test"), wcs, FailureModeTolerant, "CN2")

		require.NoError(t, err, "SendToAny can tolerate failures")
		t.Log("SendToAny: Can failover to other receivers")
	})
}
