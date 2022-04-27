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

package updateTag

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("update table rows")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	p := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}

	affectedRows := uint64(vector.Length(bat.Vecs[0]))

	// the reference of vector add 1
	for i := range bat.Vecs {
		bat.Vecs[i].Ref++
	}

	// update calculate
	updateBatch := &batch.Batch{Attrs: append(p.UpdateAttrs, p.OtherAttrs...)}
	for _, etd := range p.UpdateList {
		vec, _, err := etd.Eval(bat, proc)
		if err != nil {
			batch.Clean(updateBatch, proc.Mp)
			proc.Reg.InputBatch = &batch.Batch{}
			return false, err
		}
		vec, err = vector.Dup(vec, proc.Mp)
		if err != nil {
			batch.Clean(updateBatch, proc.Mp)
			proc.Reg.InputBatch = &batch.Batch{}
			return false, err
		}
		err = constantPadding(vec, affectedRows)
		if err != nil {
			batch.Clean(updateBatch, proc.Mp)
			proc.Reg.InputBatch = &batch.Batch{}
			return false, err
		}
		updateBatch.Vecs = append(updateBatch.Vecs, vec)
	}
	for _, attr := range p.OtherAttrs {
		vec := batch.GetVector(bat, attr)
		// newVec := &vector.Vector{Typ: vec.Typ, Col: vec.Col, Nsp: vec.Nsp}
		vec, err := vector.Dup(vec, proc.Mp)
		if err != nil {
			batch.Clean(updateBatch, proc.Mp)
			proc.Reg.InputBatch = &batch.Batch{}
			return false, err
		}
		updateBatch.Vecs = append(updateBatch.Vecs, vec)
	}

	// delete tag
	for i := range bat.Zs {
		bat.Zs[i] = -1
	}

	// update tag
	updateBatch.Zs = make([]int64, affectedRows)
	for i := range updateBatch.Zs {
		updateBatch.Zs[i] = 1
	}

	unionBat, err := bat.Append(proc.Mp, updateBatch)
	if err != nil {
		batch.Clean(unionBat, proc.Mp)
		batch.Clean(updateBatch, proc.Mp)
		proc.Reg.InputBatch = &batch.Batch{}
		return false, err
	}
	// write batch to the storage
	if err := p.Relation.Write(p.Ts, unionBat); err != nil {
		batch.Clean(unionBat, proc.Mp)
		batch.Clean(updateBatch, proc.Mp)
		proc.Reg.InputBatch = &batch.Batch{}
		return false, err
	}
	batch.Clean(unionBat, proc.Mp)
	batch.Clean(updateBatch, proc.Mp)
	proc.Reg.InputBatch = &batch.Batch{}

	p.M.Lock()
	p.AffectedRows += affectedRows
	p.M.Unlock()
	return false, nil
}

func constantPadding(vec *vector.Vector, count uint64) error {
	length := uint64(vector.Length(vec))
	if length == count {
		return nil
	}
	if length != 1 && !nulls.Contains(vec.Nsp, 0) {
		panic("constant result rows are not one")
	}
	if nulls.Contains(vec.Nsp, 0) {
		for i := uint64(0); i < count-1; i++ {
			nulls.Add(vec.Nsp, i+1)
		}
		return nil
	}
	switch vec.Typ.Oid {
	case types.T_int8:
		value := vec.Col.([]int8)[0]
		values := vec.Col.([]int8)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_int16:
		value := vec.Col.([]int16)[0]
		values := vec.Col.([]int16)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_int32:
		value := vec.Col.([]int32)[0]
		values := vec.Col.([]int32)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_int64:
		value := vec.Col.([]int64)[0]
		values := vec.Col.([]int64)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_uint8:
		value := vec.Col.([]uint8)[0]
		values := vec.Col.([]uint8)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_uint16:
		value := vec.Col.([]uint16)[0]
		values := vec.Col.([]uint16)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_uint32:
		value := vec.Col.([]uint32)[0]
		values := vec.Col.([]uint32)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_uint64:
		value := vec.Col.([]uint64)[0]
		values := vec.Col.([]uint64)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_float32:
		value := vec.Col.([]float32)[0]
		values := vec.Col.([]float32)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_float64:
		value := vec.Col.([]float64)[0]
		values := vec.Col.([]float64)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_sel:
		value := vec.Col.([]int64)[0]
		values := vec.Col.([]int64)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_tuple:
		value := vec.Col.([][]interface{})[0]
		values := vec.Col.([][]interface{})
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_char, types.T_varchar, types.T_json:
		value := vec.Col.(*types.Bytes).Data
		offset := vec.Col.(*types.Bytes).Offsets[0]
		cnt := vec.Col.(*types.Bytes).Lengths[0]
		values := vec.Col.(*types.Bytes)
		for i := uint64(0); i < count-1; i++ {
			values.Data = append(values.Data, value...)
			values.Lengths = append(values.Lengths, cnt)
			offset += cnt
			values.Offsets = append(values.Offsets, offset)
		}
		vector.SetCol(vec, values)
	case types.T_date:
		value := vec.Col.([]types.Date)[0]
		values := vec.Col.([]types.Date)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	case types.T_datetime:
		value := vec.Col.([]types.Datetime)[0]
		values := vec.Col.([]types.Datetime)
		for i := uint64(0); i < count-1; i++ {
			values = append(values, value)
		}
		vector.SetCol(vec, values)
	default:
		panic(fmt.Sprintf("unexpect type %s for function constantPadding", vec.Typ))
	}
	return nil
}
