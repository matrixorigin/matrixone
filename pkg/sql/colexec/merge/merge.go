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

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" + ")
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.ctr = new(Container)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	for {
		if len(proc.Reg.MergeReceivers) == 0 {
			return true, nil
		}
		reg := proc.Reg.MergeReceivers[n.ctr.i]
		bat := <-reg.Ch
		if bat == nil {
			proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:n.ctr.i], proc.Reg.MergeReceivers[n.ctr.i+1:]...)
			if n.ctr.i >= len(proc.Reg.MergeReceivers) {
				n.ctr.i = 0
			}
			continue
		}
		if len(bat.Zs) == 0 {
			continue
		}
		proc.Reg.InputBatch = bat
		if n.ctr.i = n.ctr.i + 1; n.ctr.i >= len(proc.Reg.MergeReceivers) {
			n.ctr.i = 0
		}
		return false, nil
	}
}
