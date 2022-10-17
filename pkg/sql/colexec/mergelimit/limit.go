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

package mergelimit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("mergeLimit(%d)", ap.Limit))
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.seen = 0
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	ok, err := ap.ctr.eval(ap, proc, anal)
	anal.Stop()
	return ok, err
}

func (ctr *container) eval(ap *Argument, proc *process.Process, anal process.Analyze) (bool, error) {
	for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
		reg := proc.Reg.MergeReceivers[i]
		bat := <-reg.Ch

		// 1. the last batch at this receiver
		if bat == nil {
			proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
			i--
			continue
		}
		// 2. an empty batch
		if bat.Length() == 0 {
			i--
			continue
		}
		anal.Input(bat)
		if ap.ctr.seen >= ap.Limit {
			proc.SetInputBatch(nil)
			bat.Clean(proc.Mp())
			return false, nil
		}
		newSeen := ap.ctr.seen + uint64(bat.Length())
		if newSeen < ap.Limit {
			ap.ctr.seen = newSeen
			anal.Output(bat)
			proc.SetInputBatch(bat)
			return false, nil
		} else {
			num := int(newSeen - ap.Limit)
			batch.SetLength(bat, bat.Length()-num)
			ap.ctr.seen = newSeen
			anal.Output(bat)
			proc.Reg.InputBatch = bat
			return false, nil
		}
	}
	proc.SetInputBatch(nil)
	return true, nil

}
