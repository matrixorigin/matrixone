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

package merge

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" union all ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.receiverListener = make([]reflect.SelectCase, len(proc.Reg.MergeReceivers))
	ap.ctr.idxs = make([]int, len(proc.Reg.MergeReceivers))
	str := "("
	for i, mr := range proc.Reg.MergeReceivers {
		if i != 0 {
			str += ", "
		}
		str += fmt.Sprintf("%p", &(mr.Ch))
		ap.ctr.receiverListener[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(mr.Ch),
		}
		ap.ctr.idxs[i] = i
	}
	str += ")"
	fmt.Printf("[colexecMerge] prepare add = %s. proc = %p. ch = %s\n", ap.Addr, proc, str)

	ap.ctr.aliveMergeReceiver = len(proc.Reg.MergeReceivers)
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr

	for {
		if ctr.aliveMergeReceiver == 0 {
			fmt.Printf("[colexecMerge] merge end. proc = %p, idx = %d\n", proc, idx)
			proc.SetInputBatch(nil)
			return true, nil
		}

		str := "["
		for i, n := range ctr.idxs {
			if i != 0 {
				str += " ,"
			}
			str += fmt.Sprintf("%d", n)
		}
		str += "]"

		fmt.Printf("[colexecMerge] merge wait (left = %s) ... proc = %p, idx = %d\n", str, proc, idx)

		start := time.Now()
		chosen, value, ok := reflect.Select(ctr.receiverListener)
		if !ok {
			fmt.Printf("[colexecMerge] pipeline closed unexpectedly\n")
			return false, moerr.NewInternalError(proc.Ctx, "pipeline closed unexpectedly")
		}
		anal.WaitStop(start)

		pointer := value.UnsafePointer()
		bat := (*batch.Batch)(pointer)
		if bat == nil {
			fmt.Printf("[colexecMerge] nil batch, close channel ch[%d]. proc = %p, idx = %d\n", ctr.idxs[chosen], proc, idx)
			ctr.receiverListener = append(ctr.receiverListener[:chosen], ctr.receiverListener[chosen+1:]...)
			ctr.idxs = append(ctr.idxs[:chosen], ctr.idxs[chosen+1:]...)
			ctr.aliveMergeReceiver--
			continue
		}
		fmt.Printf("[colexecMerge] merge received batch from channel ch[%d] ... proc = %p, idx = %d\n", chosen, proc, idx)
		if bat.Length() == 0 {
			continue
		}
		anal.Input(bat, isFirst)
		anal.Output(bat, isLast)
		proc.SetInputBatch(bat)
		return false, nil
	}
}
