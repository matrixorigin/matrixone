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

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(fmt.Sprintf("limit(%v)", arg.Limit))
}

func (arg *Argument) Prepare(_ *process.Process) error {
	return nil
}

// Call returning only the first n tuples from its input
func (arg *Argument) Call(proc *process.Process) (process.ExecStatus, error) {
	bat := proc.InputBatch()

	if bat == nil {
		return process.ExecStop, nil
	}
	if bat.Last() {
		proc.SetInputBatch(bat)
		return process.ExecNext, nil
	}
	if bat.IsEmpty() {
		proc.PutBatch(bat)
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	}
	ap := arg
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()
	anal.Input(bat, arg.info.IsFirst)
	if ap.Seen >= ap.Limit {
		proc.SetInputBatch(nil)
		proc.PutBatch(bat)
		return process.ExecStop, nil
	}
	length := bat.RowCount()
	newSeen := ap.Seen + uint64(length)
	if newSeen >= ap.Limit { // limit - seen
		batch.SetLength(bat, int(ap.Limit-ap.Seen))
		ap.Seen = newSeen
		anal.Output(bat, arg.info.IsLast)

		proc.SetInputBatch(bat)
		return process.ExecStop, nil
	}
	anal.Output(bat, arg.info.IsLast)
	ap.Seen = newSeen
	return process.ExecNext, nil
}
