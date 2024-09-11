// Copyright 2023 Matrix Origin
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

package colexec

import (
	"context"
	"reflect"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (r *ReceiverOperator) InitReceiver(proc *process.Process, mergeReceivers []*process.WaitRegister) {
	r.proc = proc
	r.MergeReceivers = mergeReceivers
	r.aliveMergeReceiver = len(mergeReceivers)
	r.chs = make([]chan *process.RegisterMessage, r.aliveMergeReceiver)
	r.nilBatchCnt = make([]int, r.aliveMergeReceiver)
	r.receiverListener = make([]reflect.SelectCase, r.aliveMergeReceiver+1)
	r.receiverListener[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.proc.Ctx.Done())}
	for i, mr := range mergeReceivers {
		//r.chs[i] = mr.Ch
		r.nilBatchCnt[i] = mr.NilBatchCnt
		r.receiverListener[i+1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			//Chan: reflect.ValueOf(mr.Ch),
		}
	}
}

// You MUST Init ReceiverOperator with Merge-Type
// if you want to use this function
func (r *ReceiverOperator) ReceiveFromAllRegs(analyzer process.Analyzer) *process.RegisterMessage {
	for {
		if r.aliveMergeReceiver == 0 {
			return process.NormalEndRegisterMessage
		}

		start := time.Now()
		chosen, msg, ok := r.selectFromAllReg()
		analyzer.WaitStop(start)

		// chosen == 0 means the info comes from proc context.Done
		if chosen == 0 {
			return process.NormalEndRegisterMessage
		}

		if !ok {
			return process.NormalEndRegisterMessage
		}

		if msg == nil {
			continue
		}

		if msg.Err != nil {
			return msg
		}

		if msg.Batch == nil {
			continue
		}

		if msg.Batch.IsEmpty() {
			r.proc.PutBatch(msg.Batch)
			continue
		}
		analyzer.Input(msg.Batch)
		return msg
	}
}

func (r *ReceiverOperator) FreeMergeTypeOperator(failed bool) {
	if len(r.receiverListener) > 0 {
		// Remove the proc context.Done waiter because it MUST BE done
		// when called this function
		r.receiverListener = r.receiverListener[1:]
	}

	mp := r.proc.Mp()
	// Senders will never send more because the context is done.
	for _, ch := range r.chs {
		for len(ch) > 0 {
			msg := <-ch
			if msg != nil && msg.Batch != nil {
				msg.Batch.Clean(mp)
			}
		}
	}
}

func (r *ReceiverOperator) RemoveChosen(idx int) {
	if idx == 0 {
		return
	}
	r.receiverListener = append(r.receiverListener[:idx], r.receiverListener[idx+1:]...)
	//remove idx-1 from chs
	r.chs = append(r.chs[:idx-1], r.chs[idx:]...)
	r.aliveMergeReceiver--
}

func (r *ReceiverOperator) DisableChosen(idx int) {
	if idx == 0 {
		return
	}
	//disable idx-1 from chs
	r.chs[idx-1] = nil
	r.aliveMergeReceiver--
}

func (r *ReceiverOperator) selectFromAllReg() (int, *process.RegisterMessage, bool) {
	var msg *process.RegisterMessage
	chosen := 0
	var ok bool
	switch len(r.chs) {
	case 1:
		chosen, msg, ok = r.selectFrom1Reg()
	case 2:
		chosen, msg, ok = r.selectFrom2Reg()
	case 3:
		chosen, msg, ok = r.selectFrom3Reg()
	case 4:
		chosen, msg, ok = r.selectFrom4Reg()
	case 5:
		chosen, msg, ok = r.selectFrom5Reg()
	case 6:
		chosen, msg, ok = r.selectFrom6Reg()
	case 7:
		chosen, msg, ok = r.selectFrom7Reg()
	case 8:
		chosen, msg, ok = r.selectFrom8Reg()
	case 9:
		chosen, msg, ok = r.selectFrom9Reg()
	case 10:
		chosen, msg, ok = r.selectFrom10Reg()
	case 11:
		chosen, msg, ok = r.selectFrom11Reg()
	case 12:
		chosen, msg, ok = r.selectFrom12Reg()
	case 13:
		chosen, msg, ok = r.selectFrom13Reg()
	case 14:
		chosen, msg, ok = r.selectFrom14Reg()
	case 15:
		chosen, msg, ok = r.selectFrom15Reg()
	case 16:
		chosen, msg, ok = r.selectFrom16Reg()
	case 32:
		chosen, msg, ok = r.selectFrom32Reg()
	case 48:
		chosen, msg, ok = r.selectFrom48Reg()
	case 64:
		chosen, msg, ok = r.selectFrom64Reg()
	case 80:
		chosen, msg, ok = r.selectFrom80Reg()
	default:
		var value reflect.Value
		chosen, value, ok = reflect.Select(r.receiverListener)
		if chosen != 0 && ok {
			msg = (*process.RegisterMessage)(value.UnsafePointer())
		}
		if !ok || msg == nil || msg.Batch == nil {
			if msg != nil && msg.Batch == nil && chosen > 0 {
				idx := chosen - 1
				if r.nilBatchCnt[idx] > 0 {
					r.nilBatchCnt[idx]--
				}
				if r.nilBatchCnt[idx] > 0 {
					return r.selectFromAllReg()
				}
			}
			r.RemoveChosen(chosen)
		}
		return chosen, msg, ok
	}

	if !ok || msg == nil || msg.Batch == nil {
		if msg != nil && msg.Batch == nil && chosen > 0 {
			idx := chosen - 1
			if r.nilBatchCnt[idx] > 0 {
				r.nilBatchCnt[idx]--
			}
			if r.nilBatchCnt[idx] > 0 {
				return r.selectFromAllReg()
			}
		}
		r.DisableChosen(chosen)
	}
	return chosen, msg, ok
}

func (r *ReceiverOperator) InitProc(proc *process.Process) {
	r.proc = proc
}

func ReceiveBitmapFromChannel(usr context.Context, ch chan *bitmap.Bitmap) *bitmap.Bitmap {
	select {
	case <-usr.Done():
		return nil
	case bm, ok := <-ch:
		if !ok {
			return nil
		}
		return bm
	}
}
