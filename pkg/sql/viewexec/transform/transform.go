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

package transform

import (
	"bytes"
	"fmt"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/viewexec/transformer"
	"matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	if n.Restrict != nil {
		restrict.String(n.Restrict, buf)
		buf.WriteString(" -> ")
	}
	if n.Projection != nil {
		projection.String(n.Projection, buf)
		buf.WriteString(" -> ")
	}
	buf.WriteString("∏([")
	for i, fvar := range n.FreeVars {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fvar)
	}
	buf.WriteString("], [")
	for i, bvar := range n.BoundVars {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s <- %s(%s)", bvar.Alias, transformer.TransformerNames[bvar.Op], bvar.Name))
	}
	buf.WriteString("])")
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = new(Container)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	return false, nil
}
