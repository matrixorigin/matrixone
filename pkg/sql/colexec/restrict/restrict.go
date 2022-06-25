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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	if e, ok := n.E.(*extend.ValueExtend); ok {
		switch v := e.V; v.Typ.Oid {
		case types.T_int8:
			if v.Col.([]int8)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_int16:
			if v.Col.([]int16)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_int32:
			if v.Col.([]int32)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_int64:
			if v.Col.([]int64)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_uint8:
			if v.Col.([]uint8)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_uint16:
			if v.Col.([]uint16)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_uint32:
			if v.Col.([]uint32)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_uint64:
			if v.Col.([]uint64)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_float32:
			if v.Col.([]float32)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_float64:
			if v.Col.([]float64)[0] == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		case types.T_char, types.T_varchar:
			if len(v.Data) == 0 {
				proc.Reg.InputBatch = &batch.Batch{}
			} else {
				proc.Reg.InputBatch = bat
			}
		}
		return false, nil
	}
	vec, _, err := n.E.Eval(bat, proc)
	if err != nil {
		batch.Clean(bat, proc.Mp)
		proc.Reg.InputBatch = &batch.Batch{}
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
