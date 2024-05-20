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
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(Argument)

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
	// the clientsession info for the channel you want to dispatch
	remoteReceivers []process.WrapCs
	// sendFunc is the rule you want to send batch
	sendFunc func(bat *batch.Batch, ap *Argument, proc *process.Process) (bool, error)

	// isRemote specify it is a remote receiver or not
	isRemote bool
	// prepared specify waiting remote receiver ready or not
	prepared bool

	// for send-to-any function decide send to which reg
	sendCnt       int
	aliveRegCnt   int
	localRegsCnt  int
	remoteRegsCnt int

	remoteToIdx map[uuid.UUID]int
	hasData     bool

	batchCnt []int
	rowCnt   []int
}

type Argument struct {
	ctr *container

	// IsSink means this is a Sink Node
	IsSink bool
	// RecSink means this is a Recursive Sink Node
	RecSink bool
	// FuncId means the sendFunc you want to call
	FuncId int
	// LocalRegs means the local register you need to send to.
	LocalRegs []*process.WaitRegister
	// RemoteRegs specific the remote reg you need to send to.
	RemoteRegs []colexec.ReceiveInfo
	// for shuffle dispatch
	ShuffleType         int32
	ShuffleRegIdxLocal  []int
	ShuffleRegIdxRemote []int

	vm.OperatorBase
}

func (arg *Argument) GetOperatorBase() *vm.OperatorBase {
	return &arg.OperatorBase
}

func init() {
	reuse.CreatePool[Argument](
		func() *Argument {
			return &Argument{}
		},
		func(a *Argument) {
			*a = Argument{}
		},
		reuse.DefaultOptions[Argument]().
			WithEnableChecker(),
	)
}

func (arg Argument) TypeName() string {
	return argName
}

func NewArgument() *Argument {
	return reuse.Alloc[Argument](nil)
}

func (arg *Argument) Release() {
	if arg != nil {
		reuse.Free[Argument](arg, nil)
	}
}

func (arg *Argument) Reset(proc *process.Process, pipelineFailed bool, err error) {
	arg.Free(proc, pipelineFailed, err)
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool, err error) {
	if arg.ctr != nil {
		if arg.ctr.isRemote {
			for _, r := range arg.ctr.remoteReceivers {
				r.Err <- err
			}

			uuids := make([]uuid.UUID, 0, len(arg.RemoteRegs))
			for i := range arg.RemoteRegs {
				uuids = append(uuids, arg.RemoteRegs[i].Uuid)
			}
			colexec.Get().DeleteUuids(uuids)
		}

		arg.ctr = nil
	}

	// told the local receiver to stop if it is still running.
	for i := range arg.LocalRegs {
		select {
		case <-arg.LocalRegs[i].Ctx.Done():
		case arg.LocalRegs[i].Ch <- nil:
		}
	}
}
