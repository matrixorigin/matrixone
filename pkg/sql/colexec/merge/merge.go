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

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" union all ")
}

func Prepare(_ *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	return nil
}

func Call(idx int, proc *process.Process, arg any) (bool, error) {
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()
	ap := arg.(*Argument)
	for {
		if len(proc.Reg.MergeReceivers) == 0 {
			proc.SetInputBatch(nil)
			return true, nil
		}
		bat := <-proc.Reg.MergeReceivers[ap.ctr.i].Ch
		if bat == nil {
			proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:ap.ctr.i], proc.Reg.MergeReceivers[ap.ctr.i+1:]...)
			if ap.ctr.i >= len(proc.Reg.MergeReceivers) {
				ap.ctr.i = 0
			}
			continue
		}
		if bat.Length() == 0 {
			continue
		}
		anal.Input(bat)
		anal.Output(bat)
		proc.SetInputBatch(bat)
		if ap.ctr.i = ap.ctr.i + 1; ap.ctr.i >= len(proc.Reg.MergeReceivers) {
			ap.ctr.i = 0
		}
		return false, nil
	}
}
