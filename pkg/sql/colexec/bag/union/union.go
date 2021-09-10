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

package union

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("%s ∪  %s", n.R, n.S))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

// sql union all is just a fake bag union
func Call(proc *process.Process, arg interface{}) (bool, error) {
	for {
		if len(proc.Reg.Ws) == 0 {
			proc.Reg.Ax = nil
			return true, nil
		}
		reg := proc.Reg.Ws[0]
		v := <-reg.Ch
		if v == nil {
			reg.Ch = nil
			reg.Wg.Done()
			proc.Reg.Ws = proc.Reg.Ws[1:]
			continue
		}
		reg.Wg.Done()
		proc.Reg.Ax = v.(*batch.Batch)
		break
	}
	return false, nil
}
