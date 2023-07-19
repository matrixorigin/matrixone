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

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("sql output")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(_ int, proc *process.Process, arg any, isFirst bool, isLast bool) (process.ExecStatus, error) {
	// WTF?   This operator NEVER return ExecStop!!!???
	ap := arg.(*Argument)
	if bat := proc.InputBatch(); bat != nil && !bat.IsEmpty() {
		if err := ap.Func(ap.Data, bat); err != nil {
			proc.PutBatch(bat)
			return process.ExecNext, err
		}
		proc.PutBatch(bat)
	}
	return process.ExecNext, nil
}
