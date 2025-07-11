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

func TestReceiverDone(t *testing.T) {
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
	sendBatToIndex(d, proc, bat, 0)
	sendBatToMultiMatchedReg(d, proc, bat, 0)
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
