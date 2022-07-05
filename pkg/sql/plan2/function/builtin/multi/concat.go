// Copyright 2021 - 2022 Matrix Origin
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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Concat(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	isAllConst := true

	for i := range vectors {
		if vectors[i].IsScalarNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		if !vectors[i].IsScalar() {
			isAllConst = false
		}
	}
	if isAllConst {
		return concatWithAllConst(vectors, proc)
	}
	return concatWithSomeCols(vectors, proc)
}

func concatWithAllConst(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vct := proc.AllocScalarVector(types.Type{Oid: types.T_varchar, Size: 24})
	val := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 1),
		Lengths: make([]uint32, 1),
	}
	length := uint32(0)
	for i := range vectors {
		col := vector.MustBytesCols(vectors[i])
		val.Data = append(val.Data, col.Get(0)...)
		length += col.Lengths[0]
	}
	val.Lengths[0] = length
	vector.SetCol(vct, val)
	return vct, nil
}

func concatWithSomeCols(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	length := vector.Length(vectors[0])
	vct, err := proc.AllocVector(types.Type{Oid: types.T_varchar, Size: 24}, 0)
	if err != nil {
		return nil, moerr.NewError(moerr.INTERNAL_ERROR, "out of memory")
	}
	nsp := new(nulls.Nulls)
	val := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, length),
		Lengths: make([]uint32, length),
	}
	offset := uint32(0)
	for i := 0; i < length; i++ {
		rowLen := uint32(0)
		for j := range vectors {
			if nulls.Contains(vectors[j].Nsp, uint64(i)) {
				nulls.Add(nsp, uint64(i))
				rowLen = 0
				break
			}
			col := vector.MustBytesCols(vectors[j])
			if vectors[j].IsScalar() {
				val.Data = append(val.Data, col.Get(0)...)
				rowLen += col.Lengths[0]
			} else {
				val.Data = append(val.Data, col.Get(int64(i))...)
				rowLen += col.Lengths[i]
			}
		}
		val.Offsets[i] = offset
		val.Lengths[i] = rowLen
		offset += rowLen
	}
	nulls.Set(vct.Nsp, nsp)
	vector.SetCol(vct, val)
	return vct, nil
}
