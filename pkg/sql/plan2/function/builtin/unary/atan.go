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
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/atan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// atan function's evaluation for arguments: [int64]
func FdsAtanInt64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]int64)
	resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
	if err != nil {
		return nil, err
	}
	results := encoding.DecodeFloat64Slice(resultVector.Data)
	results = results[:len(origVecCol)]
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, vs[0].Nsp)
	vector.SetCol(resultVector, atan.AtanInt64(origVecCol, results))
	return resultVector, nil
}

// atan function's evaluation for arguments: [uint64]
func FdsAtanUInt64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]uint64)
	resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
	if err != nil {
		return nil, err
	}
	results := encoding.DecodeFloat64Slice(resultVector.Data)
	results = results[:len(origVecCol)]
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, vs[0].Nsp)
	vector.SetCol(resultVector, atan.AtanUint64(origVecCol, results))
	return resultVector, nil
}

// atan function's evaluation for arguments: [float64]
func FdsAtanFloat64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]float64)
	resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
	if err != nil {
		return nil, err
	}
	results := encoding.DecodeFloat64Slice(resultVector.Data)
	results = results[:len(origVecCol)]
	resultVector.Col = results
	nulls.Set(resultVector.Nsp, vs[0].Nsp)
	vector.SetCol(resultVector, atan.AtanFloat64(origVecCol, results))
	return resultVector, nil
}
