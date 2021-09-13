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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("merge")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, _ interface{}) (bool, error) {
	if len(proc.Reg.Ws) == 0 {
		return true, nil
	}
	for i := 0; i < len(proc.Reg.Ws); i++ {
		reg := proc.Reg.Ws[i]
		v := <-reg.Ch
		if v == nil {
			reg.Ch = nil
			reg.Wg.Done()
			proc.Reg.Ws = append(proc.Reg.Ws[:i], proc.Reg.Ws[i+1:]...)
			i--
			continue
		}
		bat := v.(*batch.Batch)
		if bat == nil || bat.Attrs == nil {
			reg.Wg.Done()
			continue
		}
		proc.Reg.Ax = bat
		reg.Wg.Done()
		return false, nil
	}
	return false, nil
}
