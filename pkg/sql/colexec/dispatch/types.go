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

	argName = "dispatch"
)

type senderStatus int

// the status of the sender.
const (
	normalSending senderStatus = iota
	stopSending
)

type container struct {
	// the status of the container.
	// if status is stopSending, the container can stop sending.
	sendStatus senderStatus

	// the information of remote receivers.
	remoteReceivers []process.WrapCs

	// sendFunc was responsible for sending batch to the receivers.
	// this function should fill the reference count of the batch.
	sendFunc func(proc *process.Process, bat *batch.Batch) error

	// the number of receivers.
	aliveRegCnt   int
	localRegsCnt  int
	remoteRegsCnt int

	// function `send to any` will use this field to determine which receiver to send.
	sendCnt int

	remoteToIdx map[uuid.UUID]int
	hasData     bool

	batchCnt []int
	rowCnt   []int
}

func (ctr *container) resumeSending() {
	ctr.sendStatus = normalSending
}

func (ctr *container) stopSending() {
	ctr.sendStatus = stopSending
}

func (ctr *container) isStopSending() bool {
	return ctr.sendStatus == stopSending
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

	info     *vm.OperatorInfo
	Children []vm.Operator
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

func (arg *Argument) SetInfo(info *vm.OperatorInfo) {
	arg.info = info
}

func (arg *Argument) GetCnAddr() string {
	return arg.info.CnAddr
}

func (arg *Argument) GetOperatorID() int32 {
	return arg.info.OperatorID
}

func (arg *Argument) GetParalleID() int32 {
	return arg.info.ParallelID
}

func (arg *Argument) GetMaxParallel() int32 {
	return arg.info.MaxParallel
}

func (arg *Argument) AppendChild(child vm.Operator) {
	arg.Children = append(arg.Children, child)
}

func (arg *Argument) Free(proc *process.Process, executeFailed bool, err error) {
	if arg.ctr != nil {
		for _, r := range arg.ctr.remoteReceivers {
			r.Err <- err
		}

		if arg.ctr.remoteRegsCnt > 0 {
			uuids := make([]uuid.UUID, len(arg.RemoteRegs))
			for i := range arg.RemoteRegs {
				uuids[i] = arg.RemoteRegs[i].Uuid
			}
			colexec.Srv.DeleteUuids(uuids)
		}
	}

	for i := range arg.LocalRegs {
		select {
		case <-arg.LocalRegs[i].Ctx.Done():
		case arg.LocalRegs[i].Ch <- nil:
		}
		// todo: it seems that it was a bug here that we need to close the channel.
		//  else sometimes the receiver will block forever.
		//  so I kept the old code here.
		//  it's a good case to reproduce the bug.
		//   comment the close code below, and run the following sql:
		//	 ```
		// 	 create table t5(a int primary key, b int);
		//	 create table t6(b int, c int, foreign key(b) references t5(a));
		//   alter table t6 add column d int;
		//   ```
		//  the alter sql will be blocked.
		close(arg.LocalRegs[i].Ch)
	}
}
