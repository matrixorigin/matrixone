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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("pipe connector")
}

func Prepare(_ *process.Process, _ any) error {
	return nil
}

func Call(_ int, proc *process.Process, arg any, _ bool, _ bool) (bool, error) {
	ap := arg.(*Argument)
	reg := ap.Reg
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		return false, nil
	}
	select {
	case <-proc.Ctx.Done():
		proc.PutBatch(bat)
		logutil.Warn("proc context done during connector send")
		return true, nil
	case <-reg.Ctx.Done():
		proc.PutBatch(bat)
		logutil.Warn("reg.Ctx done during connector send")
		return true, nil
	case reg.Ch <- bat:
		proc.SetInputBatch(nil)
		return false, nil
	}
}
