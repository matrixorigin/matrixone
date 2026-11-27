// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dispatch

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// withTestDispatchPool swaps the operators' send pool for a fresh one, and
// checks at the end of the test that every operator freed its buffer.
func withTestDispatchPool(t *testing.T) *mpool.MPool {
	mp := mpool.MustNewZero()
	old := dispatchSendPool
	dispatchSendPool = mp
	t.Cleanup(func() {
		dispatchSendPool = old
		require.Zero(t, mp.CurrNB(), "a dispatch send buffer leaked")
	})
	return mp
}

func newSendTestBatch(t *testing.T, proc *process.Process, base int64) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), proc.Mp())
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), proc.Mp())
	for i := int64(0); i < 50; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], base+i, false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("payload-value-longer-than-inline"), i%5 == 0, proc.Mp()))
	}
	bat.SetRowCount(50)
	return bat
}

// recordingReceiver decodes every batch inside Write, the only time the
// message's bytes are valid: the sender reuses its buffer once Write
// returns.
func recordingReceiver(t *testing.T, ctrl *gomock.Controller, fail error, got *[]int64) *process.WrapCs {
	session := mock_morpc.NewMockClientSession(ctrl)
	session.EXPECT().Write(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, message any) error {
			if fail != nil {
				return fail
			}
			decoded, err := batch.ViewBinary(message.(*pipeline.Message).GetData())
			require.NoError(t, err)
			*got = append(*got, vector.GetFixedAtNoTypeCheck[int64](decoded.Vecs[0], 0))
			return nil
		})
	return &process.WrapCs{Uid: uuid.Must(uuid.NewV7()), Cs: session}
}

func newRemoteDispatch(receivers ...*process.WrapCs) *Dispatch {
	d := NewArgument()
	d.ctr = &container{
		prepared:        true,
		remoteReceivers: receivers,
		remoteRegsCnt:   len(receivers),
		aliveRegCnt:     len(receivers),
	}
	return d
}

// Every remote send of an operator marshals into its one buffer, reused
// across batches, across receivers and across executions, and freed by
// Free and by Release.
func TestDispatchRemoteSendsReuseOperatorBuffer(t *testing.T) {
	mp := withTestDispatchPool(t)
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	var first, second []int64
	d := newRemoteDispatch(recordingReceiver(t, ctrl, nil, &first), recordingReceiver(t, ctrl, nil, &second))

	for i := int64(0); i < 4; i++ {
		bat := newSendTestBatch(t, proc, i*100)
		_, err := sendToAllRemoteFunc(bat, d, proc)
		require.NoError(t, err)
		bat.Clean(proc.Mp())
		if i == 0 {
			require.Equal(t, int64(1), mp.Stats().NumAlloc.Load())
		}
	}
	require.Equal(t, int64(1), mp.Stats().NumAlloc.Load(), "one buffer for every send")
	require.Equal(t, []int64{0, 100, 200, 300}, first)
	require.Equal(t, first, second)

	// SendToAny marshals into the same buffer.
	bat := newSendTestBatch(t, proc, 900)
	_, err := sendToAnyRemoteFunc(bat, d, proc)
	require.NoError(t, err)
	bat.Clean(proc.Mp())
	require.Equal(t, int64(1), mp.Stats().NumAlloc.Load())

	// The operator keeps the buffer past an execution's Reset.
	d.Reset(proc, false, nil)
	require.NotZero(t, mp.CurrNB())
	d.Free(proc, false, nil)
	require.Zero(t, mp.CurrNB(), "Free frees it")
	d.Free(proc, false, nil)

	// A later execution allocates again; Release frees it.
	d.ctr = newRemoteDispatch(recordingReceiver(t, ctrl, nil, &first)).ctr
	bat = newSendTestBatch(t, proc, 5)
	_, err = sendToAllRemoteFunc(bat, d, proc)
	require.NoError(t, err)
	bat.Clean(proc.Mp())
	require.NotZero(t, mp.CurrNB())
	d.Release()
	require.Zero(t, mp.CurrNB(), "Release frees it")
}

// A send that fails leaves the buffer owned by the operator, which still
// frees it.
func TestDispatchRemoteSendFailureFreesBuffer(t *testing.T) {
	mp := withTestDispatchPool(t)
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	var got []int64
	d := newRemoteDispatch(recordingReceiver(t, ctrl, errors.New("broken"), &got))
	bat := newSendTestBatch(t, proc, 0)
	defer bat.Clean(proc.Mp())
	_, err := sendToAllRemoteFunc(bat, d, proc)
	require.Error(t, err)
	require.Empty(t, got)
	require.NotZero(t, mp.CurrNB())
	d.Free(proc, false, err)
	d.Release()
	require.Zero(t, mp.CurrNB())
}

// A batch that fails to marshal sends nothing.
func TestDispatchRemoteMarshalFailure(t *testing.T) {
	withTestDispatchPool(t)
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	var got []int64
	d := newRemoteDispatch(recordingReceiver(t, ctrl, nil, &got))
	defer d.Release()
	bat := newSendTestBatch(t, proc, 0)
	defer bat.Clean(proc.Mp())
	bat.Vecs = append(bat.Vecs, nil)
	_, err := sendToAllRemoteFunc(bat, d, proc)
	require.Error(t, err)
	require.Empty(t, got)
	bat.Vecs = bat.Vecs[:2]
}
