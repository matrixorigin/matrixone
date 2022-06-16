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

package queryinsert

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct {
	Ts          uint64
	TargetTable engine.Relation
	Affected    uint64
}

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(fmt.Sprintf("insert select"))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	// TODO: scalar vector's extension

	println(fmt.Sprintf("query insert, and bat is %v, vec is %v", bat, bat.Vecs))
	bat.Ro = false
	err := n.TargetTable.Write(n.Ts, bat, proc.Snapshot)
	println("query insert has wrote over")
	n.Affected += uint64(len(bat.Zs))
	return false, err
}
