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

package mergerecursive

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" merge recursive ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, true)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()
	ap := arg
	ctr := ap.ctr
	var sb *batch.Batch
	result := vm.NewCallResult()
	for !ctr.last {
		bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}
		if bat == nil || bat.End() {
			proc.SetInputBatch(nil)
			result.Status = vm.ExecStop
			return result, nil
		}
		if bat.Last() {
			ctr.last = true
		}
		ctr.bats = append(ctr.bats, bat)
	}
	sb = ctr.bats[0]
	ctr.bats = ctr.bats[1:]

	if sb.Last() {
		ctr.last = false
	}

	if sb.End() {
		sb.Clean(proc.Mp())
		proc.SetInputBatch(nil)
		result.Status = vm.ExecStop
		return result, nil
	}

	anal.Input(sb, arg.info.IsFirst)
	anal.Output(sb, arg.info.IsLast)
	proc.SetInputBatch(sb)
	return result, nil
}
