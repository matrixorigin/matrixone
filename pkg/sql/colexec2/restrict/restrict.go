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

package restrict

import (
	"bytes"
	"fmt"

	batch "github.com/matrixorigin/matrixone/pkg/container/batch2"
	colexec "github.com/matrixorigin/matrixone/pkg/sql/colexec2"
	process "github.com/matrixorigin/matrixone/pkg/vm/process2"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("σ(%s)", n.E))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}
	ap := arg.(*Argument)
	vec, err := colexec.EvalExpr(bat, proc, ap.E)
	if err != nil {
		batch.Clean(bat, proc.Mp)
		return false, err
	}
	bs := vec.Col.([]bool)
	sels := make([]int64, 0, 8)
	for i, b := range bs {
		if b {
			sels = append(sels, int64(i))
		}
	}
	batch.Shrink(bat, sels)
	proc.Reg.InputBatch = bat
	return false, nil
}
