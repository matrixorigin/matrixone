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

package vm

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// call each operator's string function to show a query plan
func String(rootOp Operator, buf *bytes.Buffer) {
	HandleAllOp(rootOp, func(parentOp Operator, op Operator) error {
		if op.GetOperatorBase().NumChildren() > 0 {
			buf.WriteString(" -> ")
		}
		op.String(buf)
		return nil
	})
}

// do init work for each operator by calling its prepare function
func Prepare(op Operator, proc *process.Process) error {
	return HandleAllOp(op, func(parentOp Operator, op Operator) error {
		return op.Prepare(proc)
	})
}

func ModifyOutputOpNodeIdx(rootOp Operator, proc *process.Process) {
	HandleAllOp(rootOp, func(parentOp Operator, op Operator) error {
		switch op.OpType() {
		case Output:
			op.GetOperatorBase().SetIdx(-1)
		}
		return nil
	})
}
