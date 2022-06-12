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
	"golang.org/x/exp/constraints"
)

func Acos[T constraints.Integer | constraints.Float](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	origVec := vs[0]
	//Here we need to classfy it into three scenes
	//1. if it is a constant
	//	1.1 if it's not a null value
	//  1.2 if it's a null value
	//2 common scene
	if origVec.IsScalar() {
		if origVec.IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
		} else {
			origVecCol := origVec.Col.([]T)
			resultVector := proc.AllocScalarVector(types.Type{Oid: types.T_float64, Size: 8})
			resultValues := make([]float64, 1)
			// nulls.Set(resultVector.Nsp, origVec.Nsp)
			results := acos.Acos[T](origVecCol, resultValues)
			if nulls.Any(results.Nsp) {
				return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
			}
			vector.SetCol(resultVector, results.Result)
			return resultVector, nil
		}
	} else {
		origVecCol := origVec.Col.([]T)
		resultVector, err := proc.AllocVector(types.Type{Oid: types.T_float64, Size: 8}, 8*int64(len(origVecCol)))
		if err != nil {
			return nil, err
		}
		results := encoding.DecodeFloat64Slice(resultVector.Data)
		results = results[:len(origVecCol)]
		resultVector.Col = results
		nulls.Set(resultVector.Nsp, origVec.Nsp)
		vector.SetCol(resultVector, acos.Acos[T](origVecCol, results))
		return resultVector, nil
	}
}
