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
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString(fmt.Sprintf("σ(%s)", n.E))
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Attrs = n.E.Attributes()
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	vec, _, err := n.E.Eval(bat, proc)
	if err != nil {
		batch.Clean(bat, proc.Mp)
		return false, err
	}
	sels := vec.Col.([]int64)
	if len(sels) == 0 {
		bat.Zs = bat.Zs[:0]
		proc.Reg.InputBatch = bat
		return false, nil
	}
	batch.Reduce(bat, n.E.Attributes(), proc.Mp)
	batch.Shrink(bat, sels)
	process.Put(proc, vec)
	proc.Reg.InputBatch = bat
	return false, nil
}
