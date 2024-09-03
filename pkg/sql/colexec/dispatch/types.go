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
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/pSpool"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Dispatch)

const (
	maxMessageSizeToMoRpc = 64 * mpool.MB
	waitNotifyTimeout     = 45 * time.Second

	SendToAllLocalFunc = iota
	SendToAllFunc
	SendToAnyLocalFunc
	SendToAnyFunc
	ShuffleToAllFunc
)

type container struct {
	sp pSpool.PipelineCommunication

	// the clientsession info for the channel you want to dispatch
	remoteReceivers []*process.WrapCs
	// sendFunc is the rule you want to send batch
	sendFunc func(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error)

	// isRemote specify it is a remote receiver or not
	isRemote bool
	// prepared specify waiting remote receiver ready or not
	prepared bool
	hasData  bool

	// for send-to-any function decide send to which reg
	sendCnt       int
	aliveRegCnt   int
	localRegsCnt  int
	remoteRegsCnt int

	remoteToIdx map[uuid.UUID]int

	batchCnt []int
	rowCnt   []int
}

type Dispatch struct {
	ctr *container

	// IsSink means this is a Sink Node
	IsSink bool
	// RecSink means this is a Recursive Sink Node
	RecSink bool

	ShuffleType int32
	// FuncId means the sendFunc you want to call
	FuncId int
	// LocalRegs means the local register you need to send to.
	LocalRegs []*process.WaitRegister
	// RemoteRegs specific the remote reg you need to send to.
	RemoteRegs []colexec.ReceiveInfo
	// for shuffle dispatch
	ShuffleRegIdxLocal  []int
	ShuffleRegIdxRemote []int

	vm.OperatorBase
}

func (dispatch *Dispatch) GetOperatorBase() *vm.OperatorBase {
	return &dispatch.OperatorBase
}

func init() {
	reuse.CreatePool[Dispatch](
		func() *Dispatch {
			return &Dispatch{}
		},
		func(a *Dispatch) {
			*a = Dispatch{}
		},
		reuse.DefaultOptions[Dispatch]().
			WithEnableChecker(),
	)
}

func (dispatch Dispatch) TypeName() string {
	return opName
}

func (dispatch *Dispatch) OpType() vm.OpType {
	return vm.Dispatch
}

func NewArgument() *Dispatch {
	return reuse.Alloc[Dispatch](nil)
}

func (dispatch *Dispatch) Release() {
	if dispatch != nil {
		reuse.Free[Dispatch](dispatch, nil)
	}
}

func (dispatch *Dispatch) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if dispatch.ctr != nil {
		if dispatch.ctr.isRemote {
			for _, r := range dispatch.ctr.remoteReceivers {
				r.Err <- err
			}

			uuids := make([]uuid.UUID, 0, len(dispatch.RemoteRegs))
			for i := range dispatch.RemoteRegs {
				uuids = append(uuids, dispatch.RemoteRegs[i].Uuid)
			}
			colexec.Get().DeleteUuids(uuids)
		}
	}

	// told the local receiver to stop if it is still running.
	if dispatch.ctr != nil && dispatch.ctr.sp != nil {
		_, _ = dispatch.ctr.sp.SendBatch(context.TODO(), pSpool.SendToAllLocal, nil, err)
		for i, reg := range dispatch.LocalRegs {
			reg.Ch2 <- process.NewPipelineSignalToGetFromSpool(dispatch.ctr.sp, i)
		}

		dispatch.ctr.sp.Close()
		dispatch.ctr.sp = nil
	} else {
		for _, reg := range dispatch.LocalRegs {
			reg.Ch2 <- process.NewPipelineSignalToDirectly(nil)
		}
	}
	dispatch.ctr = nil
}

func (dispatch *Dispatch) Free(proc *process.Process, pipelineFailed bool, err error) {
}
