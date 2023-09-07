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

package connector

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("pipe connector")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(_ int, proc *process.Process, arg any, _ bool, _ bool) (process.ExecStatus, error) {
	ap := arg.(*Argument)
	reg := ap.Reg
	bat := proc.InputBatch()
	if bat == nil {
		return process.ExecStop, nil
	}
	if bat.IsEmpty() {
		proc.PutBatch(bat)
		proc.SetInputBatch(batch.EmptyBatch)
		return process.ExecNext, nil
	}

	// there is no need to log anything here.
	// because the context is already canceled means the pipeline closed normally.
	select {
	case <-proc.Ctx.Done():
		proc.PutBatch(bat)
		return process.ExecStop, nil

	case <-reg.Ctx.Done():
		proc.PutBatch(bat)
		return process.ExecStop, nil

	case reg.Ch <- bat:
		proc.SetInputBatch(nil)
		return process.ExecNext, nil
	}
}
