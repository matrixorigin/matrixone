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

package output

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("sql output")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	ap := arg.(*Argument)
	if proc.Reg.InputBatch != nil {
		if bat := proc.Reg.InputBatch.(*batch.Batch); bat != nil && bat.Attrs != nil {
			if len(ap.Attrs) > 0 {
				bat.Reorder(ap.Attrs)
			}
			if err := ap.Func(ap.Data, bat); err != nil {
				bat.Clean(proc)
				return true, err
			}
			bat.Clean(proc)
		}
	} else {
		ap.Func(ap.Data, nil)
	}
	return false, nil
}
