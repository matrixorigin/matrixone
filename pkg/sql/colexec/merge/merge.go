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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"reflect"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" union all ")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.receiverListener = make([]reflect.SelectCase, len(proc.Reg.MergeReceivers))
	for i, mr := range proc.Reg.MergeReceivers {
		ap.ctr.receiverListener[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(mr.Ch),
		}
	}
	ap.ctr.aliveMergeReceiver = len(proc.Reg.MergeReceivers)
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	ctr := ap.ctr

	for {
		if ctr.aliveMergeReceiver == 0 {
			proc.SetInputBatch(nil)
			return true, nil
		}

		chosen, value, ok := reflect.Select(ctr.receiverListener)
		if !ok {
			return false, moerr.NewInternalError(proc.Ctx, "pipeline closed unexpectedly")
		}

		pointer := value.UnsafePointer()
		bat := (*batch.Batch)(pointer)
		if bat == nil {
			ctr.receiverListener = append(ctr.receiverListener[:chosen], ctr.receiverListener[chosen+1:]...)
			ctr.aliveMergeReceiver--
			continue
		}
		if bat.Length() == 0 {
			continue
		}
		anal.Input(bat)
		anal.Output(bat)
		proc.SetInputBatch(bat)
		return false, nil
	}
}
