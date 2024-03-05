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

package indexjoin

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "index"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": index join ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, false)
	return err
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ap := arg
	ctr := ap.ctr
	result := vm.NewCallResult()
	for {
		switch ctr.state {

		case Probe:
			bat, _, err := ctr.ReceiveFromSingleReg(0, anal)
			if err != nil {
				return result, err
			}
			if bat == nil {
				ctr.state = End
				continue
			}
			if bat.IsEmpty() {
				proc.PutBatch(bat)
				continue
			}
			vecused := make([]bool, len(bat.Vecs))
			newvecs := make([]*vector.Vector, len(ap.Result))
			for i, pos := range ap.Result {
				vecused[pos] = true
				newvecs[i] = bat.Vecs[pos]
			}
			for i := range bat.Vecs {
				if !vecused[i] {
					bat.Vecs[i].Free(proc.Mp())
				}
			}
			bat.Vecs = newvecs
			result.Batch = bat
			anal.Output(bat, arg.GetIsLast())
			return result, nil

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}
