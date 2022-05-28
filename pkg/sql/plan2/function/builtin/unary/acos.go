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
	"github.com/matrixorigin/matrixone/pkg/vectorize/acos"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// acos function's evaluation for arguments: [int64]
func AcosInt64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]int64)
	if vs[0].IsScalar() {
		if vs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
		} else {
			resultVector := proc.AllocScalarVector(types.Type{Oid: types.T_float64, Size: 8})
			results := make([]float64, 1)
			nulls.Set(resultVector.Nsp, vs[0].Nsp)
			acosResult := acos.AcosInt64(origVecCol, results)
			checkResultValues(resultVector, vs[0], acosResult)
			return resultVector, nil
		}
	} else {
		resultVector, err := proc.AllocVector(types.Type{Oid: types.T_float64, Size: 8}, 8*int64(len(origVecCol)))
		if err != nil {
			return nil, err
		}
		results := encoding.DecodeFloat64Slice(resultVector.Data)
		results = results[:len(origVecCol)]
		nulls.Set(resultVector.Nsp, vs[0].Nsp)
		acosResult := acos.AcosInt64(origVecCol, results)
		checkResultValues(resultVector, vs[0], acosResult)
		return resultVector, err
	}
}

// acos function's evaluation for arguments: [uint64]
func AcosUInt64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]uint64)
	if vs[0].IsScalar() {
		if vs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
		}
		resultVector := proc.AllocScalarVector(types.Type{Oid: types.T_float64, Size: 8})
		results := make([]float64, 1)
		nulls.Set(resultVector.Nsp, vs[0].Nsp)
		acosResult := acos.AcosUint64(origVecCol, results)
		checkResultValues(resultVector, vs[0], acosResult)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(types.Type{Oid: types.T_float64, Size: 8}, 8*int64(len(origVecCol)))
		if err != nil {
			return nil, err
		}
		results := encoding.DecodeFloat64Slice(resultVector.Data)
		results = results[:len(origVecCol)]
		nulls.Set(resultVector.Nsp, vs[0].Nsp)
		acosResult := acos.AcosUint64(origVecCol, results)
		checkResultValues(resultVector, vs[0], acosResult)
		return resultVector, err
	}
}

// acos function's evaluation for arguments: [float64]
func AcosFloat64(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVecCol := vs[0].Col.([]float64)
	if vs[0].IsScalar() {
		if vs[0].IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
		}
		resultVector := proc.AllocScalarVector(types.Type{Oid: types.T_float64, Size: 8})
		results := make([]float64, 1)
		nulls.Set(resultVector.Nsp, vs[0].Nsp)
		acosResult := acos.AcosFloat64(origVecCol, results)
		checkResultValues(resultVector, vs[0], acosResult)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(types.Type{Oid: types.T_float64, Size: 8}, 8*int64(len(origVecCol)))
		if err != nil {
			return nil, err
		}
		results := encoding.DecodeFloat64Slice(resultVector.Data)
		results = results[:len(origVecCol)]
		nulls.Set(resultVector.Nsp, vs[0].Nsp)
		acosResult := acos.AcosFloat64(origVecCol, results)
		checkResultValues(resultVector, vs[0], acosResult)
		return resultVector, err
	}
}

//  checkResultValues : Check whether the acosresult result contains a null value. If so, reset the resultvector NSP
func checkResultValues(resultVector *vector.Vector, srcVector *vector.Vector, acosResult acos.AcosResult) {
	if nulls.Any(acosResult.Nsp) {
		if !nulls.Any(srcVector.Nsp) {
			resultVector.Nsp = acosResult.Nsp
		} else {
			resultVector.Nsp.Or(acosResult.Nsp)
		}
	}
	resultVector.Col = acosResult.Result
}
