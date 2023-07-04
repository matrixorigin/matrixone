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

package shuffle

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("shuffle")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ctr := new(container)
	ap.ctr = ctr
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	//ap := arg.(*Argument)

	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}

	if bat.Length() == 0 {
		bat.Clean(proc.Mp())
		proc.SetInputBatch(batch.EmptyBatch)
		return false, nil
	}
	proc.SetInputBatch(bat)
	return false, nil
}
