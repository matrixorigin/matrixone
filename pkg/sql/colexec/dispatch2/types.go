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

package dispatch2

//
//import (
//	"github.com/matrixorigin/matrixone/pkg/sql/colexec/pipelineSpool"
//	"time"
//
//	"github.com/google/uuid"
//	"github.com/matrixorigin/matrixone/pkg/common/mpool"
//	"github.com/matrixorigin/matrixone/pkg/common/reuse"
//	"github.com/matrixorigin/matrixone/pkg/container/batch"
//	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
//	"github.com/matrixorigin/matrixone/pkg/vm"
//	"github.com/matrixorigin/matrixone/pkg/vm/process"
//)
//
//var _ vm.Operator = new(Dispatch)
//
//const (
//	maxMessageSizeToMoRpc = 64 * mpool.MB
//	waitNotifyTimeout     = 45 * time.Second
//
//	SendToAllLocalFunc = iota
//	SendToAllFunc
//	SendToAnyLocalFunc
//	SendToAnyFunc
//	ShuffleToAllFunc
//)
//
//type container struct {
//	// the clientsession info for the channel you want to dispatch
//	remoteReceivers []*process.WrapCs
//	// sendFunc is the rule you want to send batch
//	sendFunc func(bat *batch.Batch, ap *Dispatch, proc *process.Process) (bool, error)
//
//	// isRemote specify it is a remote receiver or not
//	isRemote bool
//	// prepared specify waiting remote receiver ready or not
//	prepared bool
//	hasData  bool
//
//	// for send-to-any function decide send to which reg
//	sendCnt       int
//	aliveRegCnt   int
//	localRegsCnt  int
//	remoteRegsCnt int
//
//	remoteToIdx map[uuid.UUID]int
//
//	batchCnt []int
//	rowCnt   []int
//}
//
//type Dispatch struct {
//	ctr *container
//
//	// IsSink means this is a Sink Node
//	IsSink bool
//	// RecSink means this is a Recursive Sink Node
//	RecSink bool
//
//	ShuffleType int32
//	// FuncId means the sendFunc you want to call
//	FuncId int
//
//	// receivers at local node.
//	LocalNext pipelineSpool.SenderToLocalPipeline
//
//	// RemoteRegs specific the remote reg you need to send to.
//	RemoteRegs []colexec.ReceiveInfo
//	// for shuffle dispatch
//	ShuffleRegIdxLocal  []int
//	ShuffleRegIdxRemote []int
//
//	vm.OperatorBase
//}
//
//func (dispatch *Dispatch) GetOperatorBase() *vm.OperatorBase {
//	return &dispatch.OperatorBase
//}
//
//func init() {
//	reuse.CreatePool[Dispatch](
//		func() *Dispatch {
//			return &Dispatch{}
//		},
//		func(a *Dispatch) {
//			*a = Dispatch{}
//		},
//		reuse.DefaultOptions[Dispatch]().
//			WithEnableChecker(),
//	)
//}
//
//func (dispatch Dispatch) TypeName() string {
//	return opName
//}
//
//func NewArgument() *Dispatch {
//	return reuse.Alloc[Dispatch](nil)
//}
//
//func (dispatch *Dispatch) Release() {
//	if dispatch != nil {
//		reuse.Free[Dispatch](dispatch, nil)
//	}
//}
//
//func (dispatch *Dispatch) Reset(_ *process.Process, _ bool, err error) {
//	if dispatch.ctr != nil {
//		if dispatch.ctr.isRemote {
//			for _, r := range dispatch.ctr.remoteReceivers {
//				r.Err <- err
//			}
//
//			uuids := make([]uuid.UUID, 0, len(dispatch.RemoteRegs))
//			for i := range dispatch.RemoteRegs {
//				uuids = append(uuids, dispatch.RemoteRegs[i].Uuid)
//			}
//			colexec.Get().DeleteUuids(uuids)
//		}
//
//		dispatch.ctr = nil
//	}
//
//	dispatch.LocalNext.Reset()
//}
//
//func (dispatch *Dispatch) Free(_ *process.Process, _ bool, err error) {
//	if dispatch.ctr != nil {
//		if dispatch.ctr.isRemote {
//			for _, r := range dispatch.ctr.remoteReceivers {
//				r.Err <- err
//			}
//
//			uuids := make([]uuid.UUID, 0, len(dispatch.RemoteRegs))
//			for i := range dispatch.RemoteRegs {
//				uuids = append(uuids, dispatch.RemoteRegs[i].Uuid)
//			}
//			colexec.Get().DeleteUuids(uuids)
//		}
//
//		dispatch.ctr = nil
//	}
//
//	dispatch.LocalNext.Close()
//}
