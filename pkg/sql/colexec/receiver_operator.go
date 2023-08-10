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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// isMergeType means the receiver operator receive batch from all regs or single by some order
// Merge/MergeGroup/MergeLimit ... are Merge-Type
// while Join/Intersect/Minus ... are not
func (r *ReceiverOperator) InitReceiver(proc *process.Process, isMergeType bool) {
	r.proc = proc
	if isMergeType {
		r.aliveMergeReceiver = len(proc.Reg.MergeReceivers)
		r.chs = make([]chan *batch.Batch, r.aliveMergeReceiver)
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

func (r *ReceiverOperator) ReceiveFromSingleReg(regIdx int, analyze process.Analyze) (*batch.Batch, bool, error) {
	start := time.Now()
	defer analyze.WaitStop(start)
	select {
	case <-r.proc.Ctx.Done():
		return nil, true, nil
	case bat, ok := <-r.proc.Reg.MergeReceivers[regIdx].Ch:
		if !ok {
			return nil, true, nil
		}
		return bat, false, nil
	}
}

func (r *ReceiverOperator) ReceiveFromSingleRegNonBlock(regIdx int, analyze process.Analyze) (*batch.Batch, bool, error) {
	start := time.Now()
	defer analyze.WaitStop(start)
	select {
	case <-r.proc.Ctx.Done():
		return nil, true, nil
	case bat, ok := <-r.proc.Reg.MergeReceivers[regIdx].Ch:
		if !ok || bat == nil {
			return nil, true, nil
		}
		return bat, false, nil
	default:
		return nil, false, nil
	}
}

func (r *ReceiverOperator) FreeAllReg() {
	for i := range r.proc.Reg.MergeReceivers {
		r.FreeSingleReg(i)
	}
}

// clean up the batch left in channel
func (r *ReceiverOperator) FreeSingleReg(regIdx int) {
	ch := r.proc.Reg.MergeReceivers[regIdx].Ch
	for len(ch) > 0 {
		bat := <-ch
		if bat != nil {
			bat.Clean(r.proc.GetMPool())
		}
	}
}

func (r *ReceiverOperator) CloseAllReg() {
	for _, c := range r.proc.Reg.MergeReceivers {
		close(c.Ch)
	}
}

// You MUST Init ReceiverOperator with Merge-Type
// if you want to use this function
func (r *ReceiverOperator) ReceiveFromAllRegs(analyze process.Analyze) (*batch.Batch, bool, error) {
	for {
		if r.aliveMergeReceiver == 0 {
			return nil, true, nil
		}

		start := time.Now()
		chosen, value, ok := reflect.Select(r.receiverListener)
		analyze.WaitStop(start)

		// chosen == 0 means the info comes from proc context.Done
		if chosen == 0 {
			logutil.Debugf("process context done during merge receive")
			return nil, true, nil
		}

		if !ok {
			logutil.Errorf("children pipeline closed unexpectedly")
			r.removeChosen(chosen)
			return nil, true, nil
		}

		bat := (*batch.Batch)(value.UnsafePointer())
		if bat == nil {
			r.removeChosen(chosen)
			continue
		}

		if bat.IsEmpty() {
			bat.Clean(r.proc.Mp())
			continue
		}

		return bat, false, nil
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
			bat := <-ch
			if bat != nil {
				bat.Clean(mp)
			}
		}
	}
}

func (r *ReceiverOperator) removeChosen(idx int) {
	r.receiverListener = append(r.receiverListener[:idx], r.receiverListener[idx+1:]...)
	r.aliveMergeReceiver--
}
