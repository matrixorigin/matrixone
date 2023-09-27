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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	ap := arg
	buf.WriteString(fmt.Sprintf("mergeLimit(%d)", ap.Limit))
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.seen = 0
	ap.ctr.InitReceiver(proc, true)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	ap := arg
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()
	ctr := ap.ctr
	result := vm.NewCallResult()
	for {
		bat, end, err := ctr.ReceiveFromAllRegs(anal)
		if err != nil {
			result.Status = vm.ExecStop
			return result, nil
		}
		if end {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		if bat.Last() {
			result.Batch = bat
			return result, nil
		}

		anal.Input(bat, arg.info.IsFirst)
		if ap.ctr.seen >= ap.Limit {
			proc.PutBatch(bat)
			continue
		}
		newSeen := ap.ctr.seen + uint64(bat.RowCount())
		if newSeen < ap.Limit {
			ap.ctr.seen = newSeen
			anal.Output(bat, arg.info.IsLast)
			result.Batch = bat
			return result, nil
		} else {
			num := int(newSeen - ap.Limit)
			batch.SetLength(bat, bat.RowCount()-num)
			ap.ctr.seen = newSeen
			anal.Output(bat, arg.info.IsLast)
			result.Batch = bat
			return result, nil
		}
	}
}
