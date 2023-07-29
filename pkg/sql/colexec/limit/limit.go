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

package limit

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("limit(%v)", n.Limit))
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

// Call returning only the first n tuples from its input
func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	bat := proc.InputBatch()

	if bat == nil {
		return process.ExecStop, nil
	}
	if bat.Last() {
		proc.SetInputBatch(bat)
		return process.ExecNext, nil
	}
	if bat.RowCount() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	}
	ap := arg.(*Argument)
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, isFirst)
	if ap.Seen >= ap.Limit {
		proc.Reg.InputBatch = nil
		proc.PutBatch(bat)
		return process.ExecStop, nil
	}
	length := bat.RowCount()
	newSeen := ap.Seen + uint64(length)
	if newSeen >= ap.Limit { // limit - seen
		batch.SetLength(bat, int(ap.Limit-ap.Seen))
		ap.Seen = newSeen
		anal.Output(bat, isLast)

		proc.SetInputBatch(bat)
		return process.ExecStop, nil
	}
	anal.Output(bat, isLast)
	ap.Seen = newSeen
	return process.ExecNext, nil
}
