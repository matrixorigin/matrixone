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
	"bytes"
	"context"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestWaitRemoteReceiversReady(t *testing.T) {
	proc := testutil.NewProcess()
	mp := proc.Mp()
	colexec.Srv = colexec.NewServer(nil)

	// case 1. the remote receiver has been ready.
	ids := []int{ShuffleToAllFunc, SendToAllFunc, SendToAnyFunc}
	for _, id := range ids {
		arg1 := &Argument{
			ctr:    new(container),
			FuncId: id,
			RemoteRegs: []colexec.ReceiveInfo{
				{
					Uuid: uuid.New(),
				},
			},
		}
		if id == ShuffleToAllFunc {
			arg1.ShuffleRegIdxRemote = make([]int, len(arg1.RemoteRegs))
		}
		proc.DispatchNotifyCh = make(chan process.WrapCs, 1)
		proc.DispatchNotifyCh <- process.WrapCs{}
		require.NoError(t, arg1.waitRemoteReceiversReady(proc, 2*time.Second), "functionID is %d", id)
	}

	// case 2. the remote receiver has been closed. so it does never be ready.
	// waitRemoteReceiversReady should return an error after wait timeout.
	arg2 := &Argument{
		ctr:    new(container),
		FuncId: SendToAllFunc,
		RemoteRegs: []colexec.ReceiveInfo{
			{
				Uuid: uuid.New(),
			},
		},
	}
	proc.DispatchNotifyCh = make(chan process.WrapCs, 1)
	require.Error(t, arg2.waitRemoteReceiversReady(proc, 500*time.Millisecond))

	// case 3. the waiting process is canceled.
	proc.Ctx, proc.Cancel = context.WithCancel(context.TODO())
	proc.Cancel()
	arg3 := &Argument{
		ctr:    new(container),
		FuncId: SendToAllFunc,
		RemoteRegs: []colexec.ReceiveInfo{
			{
				Uuid: uuid.New(),
			},
		},
	}
	require.Error(t, arg3.waitRemoteReceiversReady(proc, time.Second))

	require.Equal(t, int64(0), mp.CurrNB())
}

func TestCTE_Dispatch(t *testing.T) {
	proc := testutil.NewProcess()
	mp := proc.Mp()

	arg := &Argument{
		ctr:     new(container),
		RecSink: true,
	}
	// case 1. cte change the nil batch to CteEndBatch.
	result, free, err := cteBatchRewrite(proc, arg, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	{
		// do check for the CteEndBatch.
		require.True(t, result.End())
		require.Equal(t, int64(1), result.GetCnt())
	}
	require.True(t, free)
	result.Clean(mp)

	// case 2. cte deals with the last batch (special batch by cte).
	// if hasData was false, it will change the end flag.
	// it will set the hasData flag to false, because the data will be sent.
	{
		bat := batch.NewWithSize(0)
		bat.SetLast()
		arg.ctr.hasData = true
		result, free, err = cteBatchRewrite(proc, arg, bat)
		require.NoError(t, err)
		require.False(t, free)
		require.False(t, arg.ctr.hasData)
		require.False(t, result.End())
		result.Clean(mp)
	}
	{
		bat := batch.NewWithSize(0)
		bat.SetLast()
		arg.ctr.hasData = false
		result, free, err = cteBatchRewrite(proc, arg, bat)
		require.NoError(t, err)
		require.False(t, free)
		require.False(t, arg.ctr.hasData)
		require.True(t, result.End())
		result.Clean(mp)
	}

	// case 3. the batch may change the hasData flag.
	arg.ctr.hasData = false
	result, free, err = cteBatchRewrite(proc, arg, batch.EmptyBatch)
	require.NoError(t, err)
	require.False(t, free)
	require.False(t, arg.ctr.hasData)
	result.Clean(mp)

	arg.ctr.hasData = false
	bat := batch.NewWithSize(0)
	bat.SetRowCount(1)
	result, free, err = cteBatchRewrite(proc, arg, bat)
	require.NoError(t, err)
	require.False(t, free)
	require.True(t, arg.ctr.hasData)
	result.Clean(mp)

	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDispatchString(t *testing.T) {
	arg := &Argument{}
	buf := new(bytes.Buffer)
	arg.String(buf)
	require.True(t, len(buf.String()) > 0)
}

func TestDispatchCall(t *testing.T) {
	proc := testutil.NewProcess()
	mp := proc.Mp()

	// because we have tested the send function in the test of `pkg/sql/colexec/dispatch/sendfunc_test.go`.
	// we only test if the dispatch can deal with the special batch.
	// special batch means the batch which is nil or is the empty batch.
	data := mockSimpleInput(mp)
	child := &value_scan.Argument{
		Batchs: []*batch.Batch{data, batch.EmptyBatch, nil},
	}
	require.NoError(t, child.Prepare(proc))
	arg := &Argument{
		ctr:      new(container),
		IsSink:   false,
		RecSink:  false,
		Children: []vm.Operator{child},
		info:     &vm.OperatorInfo{},
	}
	arg.ctr.sendFunc = func(proc *process.Process, bat *batch.Batch) error {
		return nil
	}

	cnt := 0
	for {
		cnt++
		result, err := arg.Call(proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop {
			break
		}
	}
	require.Equal(t, 3, cnt)

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)

	proc.FreeVectors()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestDispatchPrepare(t *testing.T) {
	proc := testutil.NewProcess()
	mp := proc.Mp()

	// because we have tested the waitRemoteReceiversReady in the test of `pkg/sql/colexec/dispatch/dispatch_test.go`.
	// we only test the fail case of the prepare function.
	toLocalAndRemote := []int{SendToAllFunc, SendToAnyFunc}
	toLocal := []int{SendToAnyLocalFunc, SendToAllLocalFunc}
	// case 1. `send to all` requires the remote receiver.
	{
		for _, id := range toLocalAndRemote {
			arg := &Argument{
				FuncId:     id,
				LocalRegs:  make([]*process.WaitRegister, 2),
				RemoteRegs: nil,
			}
			require.Error(t, arg.Prepare(proc))
		}
	}

	// case 2. `send to local` requires no remote receiver.
	{
		for _, id := range toLocal {
			arg := &Argument{
				FuncId:     id,
				LocalRegs:  make([]*process.WaitRegister, 2),
				RemoteRegs: make([]colexec.ReceiveInfo, 1),
			}
			require.Error(t, arg.Prepare(proc))
		}
	}

	// case 3. unexpected function id.
	arg := &Argument{
		FuncId: -5,
	}
	require.Error(t, arg.Prepare(proc))

	require.Equal(t, int64(0), mp.CurrNB())
}
