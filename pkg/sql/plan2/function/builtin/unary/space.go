// Copyright 2022 Matrix Origin
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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/space"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// space function's evaluation for arguments: [int64]
func FdsSpaceInt64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]int64)

	bytesNeed := space.CountSpacesForSignedInt(origVecCol)

	resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(origVecCol)),
		Lengths: make([]uint32, len(origVecCol)),
	}
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, vs[0].Nsp)
	vector.SetCol(resultVector, space.FillSpacesInt64(origVecCol, results))
	return resultVector, nil
}

// space function's evaluation for arguments: [uint64]
func FdsSpaceUInt64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]uint64)

	bytesNeed := space.CountSpacesForUnsignedInt(origVecCol)

	resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(origVecCol)),
		Lengths: make([]uint32, len(origVecCol)),
	}
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, vs[0].Nsp)
	vector.SetCol(resultVector, space.FillSpacesUint64(origVecCol, results))
	return resultVector, nil
}

// space function's evaluation for arguments: [float64]
func FdsSpaceFloat64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]float64)

	bytesNeed := space.CountSpacesForFloat(origVecCol)

	resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(origVecCol)),
		Lengths: make([]uint32, len(origVecCol)),
	}
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, vs[0].Nsp)
	vector.SetCol(resultVector, space.FillSpacesFloat64(origVecCol, results))
	return resultVector, nil
}

// space function's evaluation for arguments: [varchar]
func FdsSpaceVarchar(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.(*types.Bytes)

	bytesNeed := space.CountSpacesForCharVarChar(origVecCol)

	resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(origVecCol.Offsets)),
		Lengths: make([]uint32, len(origVecCol.Lengths)),
	}
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, vs[0].Nsp)
	vector.SetCol(resultVector, space.FillSpacesCharVarChar(origVecCol, results))
	return resultVector, nil
}

// space function's evaluation for arguments: [char]
func FdsSpaceChar(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.(*types.Bytes)

	bytesNeed := space.CountSpacesForCharVarChar(origVecCol)

	resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, len(origVecCol.Offsets)),
		Lengths: make([]uint32, len(origVecCol.Lengths)),
	}
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, vs[0].Nsp)
	vector.SetCol(resultVector, space.FillSpacesCharVarChar(origVecCol, results))
	return resultVector, nil
}
