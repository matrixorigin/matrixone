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

	"github.com/matrixorigin/matrixone/pkg/container/batch"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(" union all ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	ap := arg
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, true)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (process.ExecStatus, error) {
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()
	ap := arg
	ctr := ap.ctr
	var bat *batch.Batch
	var end bool

	for {
		bat, end, _ = ctr.ReceiveFromAllRegs(anal)
		if end {
			proc.SetInputBatch(nil)
			return process.ExecStop, nil
		}

		if bat.Last() && ap.SinkScan {
			proc.PutBatch(bat)
			continue
		}
		break
	}

	anal.Input(bat, arg.info.IsFirst)
	anal.Output(bat, arg.info.IsLast)
	proc.SetInputBatch(bat)
	return process.ExecNext, nil
}
