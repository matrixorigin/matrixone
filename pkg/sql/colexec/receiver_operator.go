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
	"reflect"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// isMergeType means the receiver operator receive batch from all regs or single by some order
// Merge/MergeGroup/MergeLimit ... are Merge-Type
// while Join/Intersect/Minus ... are not
func (r *ReceiverOperator) InitReceiver(proc *process.Process, isMergeType bool) {
	r.proc = proc
	if isMergeType {
		r.aliveMergeReceiver = len(proc.Reg.MergeReceivers)
		r.chs = make([]chan *process.RegisterMessage, r.aliveMergeReceiver)
		r.receiverListener = make([]reflect.SelectCase, r.aliveMergeReceiver+1)
		r.receiverListener[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(r.proc.Ctx.Done())}
		for i, mr := range proc.Reg.MergeReceivers {
			r.chs[i] = mr.Ch
			r.receiverListener[i+1] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(mr.Ch),
			}
		}
	}
}

func (r *ReceiverOperator) ReceiveFromSingleReg(regIdx int, analyze process.Analyze) *process.RegisterMessage {
	start := time.Now()
	defer analyze.WaitStop(start)
	select {
	case <-r.proc.Ctx.Done():
		return process.NormalEndRegisterMessage
	case msg, ok := <-r.proc.Reg.MergeReceivers[regIdx].Ch:
		if !ok || msg == nil {
			return process.NormalEndRegisterMessage
		}

		return msg
	}
}

// func (r *ReceiverOperator) ReceiveFromSingleRegNonBlock(regIdx int, analyze process.Analyze) (*process.RegisterMessage, bool, error) {
// 	start := time.Now()
// 	defer analyze.WaitStop(start)
// 	select {
// 	case <-r.proc.Ctx.Done():
// 		return process.EmtpyRegisterMessage, true, nil
// 	case msg, ok := <-r.proc.Reg.MergeReceivers[regIdx].Ch:
// 		if !ok || msg == nil {
// 			return process.EmtpyRegisterMessage, true, nil
// 		}
// 		return msg, false, msg.Err
// 	default:
// 		return process.EmtpyRegisterMessage, false, nil
// 	}
// }

func (r *ReceiverOperator) FreeAllReg() {
	for i := range r.proc.Reg.MergeReceivers {
		r.FreeSingleReg(i)
	}
}

func (r *ReceiverOperator) FreeSingleReg(regIdx int) {
	w := r.proc.Reg.MergeReceivers[regIdx]
	w.CleanChannel(r.proc.GetMPool())
}

// You MUST Init ReceiverOperator with Merge-Type
// if you want to use this function
func (r *ReceiverOperator) ReceiveFromAllRegs(analyze process.Analyze) *process.RegisterMessage {
	for {
		if r.aliveMergeReceiver == 0 {
			return process.NormalEndRegisterMessage
		}

		start := time.Now()
		chosen, msg, ok := r.selectFromAllReg()
		analyze.WaitStop(start)

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
			r.RemoveChosen(chosen)
		}
		return chosen, msg, ok
	}

	if !ok || msg == nil || msg.Batch == nil {
		r.DisableChosen(chosen)
	}
	return chosen, msg, ok
}
