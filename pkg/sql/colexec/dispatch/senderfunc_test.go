// Copyright 2024 Matrix Origin
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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBatchReferenceAdjust_dispatch(t *testing.T) {
	// if we want to send a batch to a local receiver, we should increase the reference of the batch.
	// if it is sent unsuccessfully, we should decrease the reference of the batch.
	// remote receiver has no need to adjust batch reference.
	proc := testutil.NewProcess()
	mp := proc.Mp()
	{
		sendBatch := &batch.Batch{Cnt: 1}
		list, err := increaseReferenceBeforeSend(sendBatch, 10, false, proc)
		require.NoError(t, err)
		require.Equal(t, 0, len(list))

		require.Equal(t, int64(1+10), sendBatch.GetCnt())

		decreaseReferenceAfterSend(sendBatch, false, 10, 4)
		require.Equal(t, int64(1+4), sendBatch.GetCnt())

		sendBatch.SetCnt(1)
		sendBatch.Clean(mp)
	}

	// todo: in fact, it was a bug that the operator need to copy the batch for CTE.
	// 	but I kept the old behavior and added this test case.
	//
	// isRecSink is a flag about CTE.
	// if isRecSink is true, the batch will be copied to the local receiver.
	// so the reference of the batch should never change.
	{
		sendBatch := &batch.Batch{Cnt: 1}
		list, err := increaseReferenceBeforeSend(sendBatch, 10, true, proc)
		require.NoError(t, err)
		require.Equal(t, 10, len(list))

		require.Equal(t, int64(2), list[0].GetCnt())
		list[0].Clean(mp)

		for i := 0; i < len(list); i++ {
			b := list[i]
			require.Equal(t, int64(1), b.GetCnt())
			b.Clean(mp)
		}
		sendBatch.Clean(mp)
	}
	proc.FreeVectors()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDispatchToLocalReceiver(t *testing.T) {
	// prepare the environment.
	proc := testutil.NewProcess()
	mp := proc.Mp()
	aliveCtx := proc.Ctx
	deadCtx, deadCancel := context.WithCancel(context.Background())
	deadCancel()

	arg := &Argument{
		IsSink:  false,
		RecSink: false,
	}

	// case 1: send a batch to all local receivers successfully.
	// after sending, the batch's reference should be increased by the number of local receivers.
	cancels := mockLocalReceivers(arg, mp, 4)
	input1 := mockSimpleInput(mp)
	require.NoError(t, arg.sendToAllLocalReceivers(proc, input1))
	require.Equal(t, int64(1+len(arg.LocalRegs)), input1.GetCnt())
	input1.Clean(mp)

	// case 2: send a batch to any local receiver successfully.
	// after sending, the batch's reference should be increased by 1.
	cancels = mockLocalReceivers(arg, mp, 4)
	input2 := mockSimpleInput(mp)
	require.NoError(t, arg.sendToAnyLocalReceiver(proc, input2))
	require.Equal(t, int64(1+1), input2.GetCnt())
	input2.Clean(mp)

	// case 3: sender context canceled. it may stop the sending.
	// so after sending, the batch's reference must be between 1 and 1+len(arg.LocalRegs).
	cancels = mockLocalReceivers(arg, mp, 4)
	input3 := mockSimpleInput(mp)
	proc.Ctx = deadCtx
	require.NoError(t, arg.sendToAllLocalReceivers(proc, input3))
	if arg.ctr.isStopSending() {
		require.True(t, int64(1) <= input3.GetCnt() && input3.GetCnt() <= int64(1+len(arg.LocalRegs)))
	} else {
		require.Equal(t, int64(1+len(arg.LocalRegs)), input3.GetCnt())
	}
	input3.Clean(mp)

	// case 4. sender context canceled, it may stop the sending work.
	// so after sending, the batch's reference must be 1 or 2.
	cancels = mockLocalReceivers(arg, mp, 4)
	input4 := mockSimpleInput(mp)
	proc.Ctx = deadCtx
	require.NoError(t, arg.sendToAnyLocalReceiver(proc, input4))
	if arg.ctr.isStopSending() {
		require.Equal(t, int64(1), input4.GetCnt())
	} else {
		require.Equal(t, int64(1+1), input4.GetCnt())
	}
	input4.Clean(mp)

	// case 5: one receiver context canceled, it may cause sending a batch to all local receivers failed.
	// after sending, the batch's reference must be between 1 and 1+len(arg.LocalRegs).
	cancels = mockLocalReceivers(arg, mp, 4)
	input5 := mockSimpleInput(mp)
	proc.Ctx = aliveCtx
	cancels[1]()
	require.NoError(t, arg.sendToAllLocalReceivers(proc, input5))
	if arg.ctr.isStopSending() {
		require.True(t, int64(1) <= input5.GetCnt() && input5.GetCnt() <= int64(1+len(arg.LocalRegs)))
	} else {
		require.Equal(t, int64(1+4), input5.GetCnt())
	}
	input5.Clean(mp)

	// case 6. all receiver context canceled, and it may cause sending a batch to any local receiver failed.
	// after sending, the batch's reference must be 1 or 2.
	cancels = mockLocalReceivers(arg, mp, 4)
	input6 := mockSimpleInput(mp)
	proc.Ctx = aliveCtx
	for i := 0; i < len(cancels); i++ {
		cancels[i]()
	}
	require.NoError(t, arg.sendToAnyLocalReceiver(proc, input6))
	if arg.ctr.isStopSending() {
		require.Equal(t, int64(1), input6.GetCnt())
	} else {
		require.Equal(t, int64(1+1), input6.GetCnt())
	}
	input6.Clean(mp)

	// test done. and clean the environment.
	cleanReceivers(arg.LocalRegs, mp)
	proc.FreeVectors()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDispatchToRemoteReceiver(t *testing.T) {
	// prepare the environment.
	proc := testutil.NewProcess()
	mp := proc.Mp()
	aliveCtx := proc.Ctx
	deadCtx, deadCancel := context.WithCancel(context.Background())
	deadCancel()

	arg := &Argument{
		IsSink:  false,
		RecSink: false,
	}

	// case 1: send a batch to all remote receivers successfully.
	cancels := mockRemoteReceivers(arg, 3)
	input1 := mockSimpleInput(mp)
	require.NoError(t, arg.sendToAllRemoteReceivers(proc, input1))
	require.Equal(t, int64(1), input1.GetCnt())
	input1.Clean(mp)

	// case 2: send a batch to any remote receiver successfully.
	cancels = mockRemoteReceivers(arg, 3)
	input2 := mockSimpleInput(mp)
	require.NoError(t, arg.sendToAnyRemoteReceiver(proc, input2))
	require.Equal(t, int64(1), input2.GetCnt())
	input2.Clean(mp)

	// case 3: the sender context canceled, it may stop the sending work.
	// but the batch's reference should be 1.
	cancels = mockRemoteReceivers(arg, 3)
	input3 := mockSimpleInput(mp)
	proc.Ctx = deadCtx
	require.NoError(t, arg.sendToAllRemoteReceivers(proc, input3))
	require.Equal(t, int64(1), input3.GetCnt())
	require.NoError(t, arg.sendToAnyRemoteReceiver(proc, input3))
	require.Equal(t, int64(1), input3.GetCnt())
	input3.Clean(mp)

	// case 4: one remote receiver has been closed.
	// it will cause send to all failed. but send to any should be successful.
	// and the batch's reference should be 1.
	cancels = mockRemoteReceivers(arg, 3)
	cancels[1]()
	proc.Ctx = aliveCtx
	input4 := mockSimpleInput(mp)
	require.Error(t, arg.sendToAllRemoteReceivers(proc, input4))
	require.Equal(t, int64(1), input4.GetCnt())
	require.NoError(t, arg.sendToAnyRemoteReceiver(proc, input4))
	require.Equal(t, int64(1), input4.GetCnt())
	input4.Clean(mp)

	// case 5: all remote receivers have been closed. it will cause send failed.
	cancels = mockRemoteReceivers(arg, 3)
	for i := 0; i < len(cancels); i++ {
		cancels[i]()
	}
	proc.Ctx = aliveCtx
	input5 := mockSimpleInput(mp)
	require.Error(t, arg.sendToAllRemoteReceivers(proc, input5))
	require.Equal(t, int64(1), input5.GetCnt())
	require.Error(t, arg.sendToAnyRemoteReceiver(proc, input5))
	require.Equal(t, int64(1), input5.GetCnt())
	require.True(t, arg.ctr.isStopSending())
	input5.Clean(mp)

	// clean the environment.
	proc.FreeVectors()
	require.Equal(t, int64(0), mp.CurrNB())
}

func mockSimpleInput(mp *mpool.MPool) *batch.Batch {
	return testutil.NewBatch(
		[]types.Type{types.T_bool.ToType()}, false, 10, mp)
}

func mockLocalReceivers(arg *Argument, mp *mpool.MPool, receiverCount int) (receiverCancels []context.CancelFunc) {
	if receiverCount <= 0 {
		panic("receiverCount should be greater than 0")
	}
	if arg.LocalRegs != nil {
		cleanReceivers(arg.LocalRegs, mp)
	}
	arg.LocalRegs = make([]*process.WaitRegister, receiverCount)

	if arg.ctr == nil {
		arg.ctr = new(container)
	}

	arg.ctr.localRegsCnt = len(arg.LocalRegs)
	arg.ctr.aliveRegCnt = arg.ctr.localRegsCnt + arg.ctr.remoteRegsCnt
	arg.ctr.resumeSending()

	receiverContexts := make([]context.Context, receiverCount)
	cancels := make([]context.CancelFunc, receiverCount)
	channels := make([]chan *batch.Batch, receiverCount)
	for i := 0; i < receiverCount; i++ {
		receiverContexts[i], cancels[i] = context.WithCancel(context.Background())
		channels[i] = make(chan *batch.Batch, 10)

		arg.LocalRegs[i] = &process.WaitRegister{
			Ctx: receiverContexts[i],
			Ch:  channels[i],
		}
	}

	return cancels
}

func mockRemoteReceivers(arg *Argument, receiverCount int) []context.CancelFunc {
	if receiverCount <= 0 {
		panic("receiverCount should be greater than 0")
	}

	if arg.ctr == nil {
		arg.ctr = new(container)
	}

	arg.ctr.remoteRegsCnt = receiverCount
	arg.ctr.aliveRegCnt = arg.ctr.localRegsCnt + arg.ctr.remoteRegsCnt
	arg.ctr.resumeSending()

	arg.ctr.remoteReceivers = make([]process.WrapCs, receiverCount)
	cancels := make([]context.CancelFunc, receiverCount)
	for i := 0; i < receiverCount; i++ {
		arg.ctr.remoteReceivers[i] = process.WrapCs{
			Cs: &mockRemoteReceiver{},
		}

		j := i
		cancels[i] = func() {
			_ = arg.ctr.remoteReceivers[j].Cs.Close()
		}
	}
	return cancels
}

var _ morpc.ClientSession = &mockRemoteReceiver{}

type mockRemoteReceiver struct {
	writeError error
	asyncError error
}

func (m *mockRemoteReceiver) Close() error {
	m.writeError = moerr.NewInternalErrorNoCtx("remote receiver closed.")
	m.asyncError = m.writeError
	return nil
}
func (m *mockRemoteReceiver) Write(_ context.Context, _ morpc.Message) error {
	return m.writeError
}
func (m *mockRemoteReceiver) AsyncWrite(_ morpc.Message) error { return m.asyncError }
func (m *mockRemoteReceiver) CreateCache(_ context.Context, _ uint64) (morpc.MessageCache, error) {
	panic("implement me")
}
func (m *mockRemoteReceiver) DeleteCache(_ uint64) { panic("implement me") }
func (m *mockRemoteReceiver) GetCache(_ uint64) (morpc.MessageCache, error) {
	panic("implement me")
}
func (m *mockRemoteReceiver) RemoteAddress() string { return "mockRemoteReceiver" }

func cleanReceivers(receivers []*process.WaitRegister, mp *mpool.MPool) {
	for _, receiver := range receivers {
		if receiver == nil {
			continue
		}
		for len(receiver.Ch) > 0 {
			b := <-receiver.Ch
			if b != nil {
				b.Clean(mp)
			}
		}
	}
}
