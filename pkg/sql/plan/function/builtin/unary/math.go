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
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type mathFn func(*vector.Vector, *vector.Vector) error

func math1(vs []*vector.Vector, proc *process.Process, fn mathFn) (*vector.Vector, error) {
	origVec := vs[0]
	//Here we need to classify it into three scenes
	//1. if it is a constant
	//	1.1 if it's not a null value
	//  1.2 if it's a null value
	//2 common scene
	if origVec.IsScalar() {
		if origVec.IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_float64, Size: 8}), nil
		} else {
			resultVector := proc.AllocScalarVector(types.Type{Oid: types.T_float64, Size: 8})
			resultValues := make([]float64, 1)
			vector.SetCol(resultVector, resultValues)
			if err := fn(origVec, resultVector); err != nil {
				return nil, err
			}
			return resultVector, nil
		}
	} else {
		vecLen := int64(vector.Length(origVec))
		resultVector, err := proc.AllocVector(types.Type{Oid: types.T_float64, Size: 8}, 8*vecLen)
		if err != nil {
			return nil, err
		}
		resCol := types.DecodeFloat64Slice(resultVector.Data)
		resCol = resCol[:vecLen]
		nulls.Set(resultVector.Nsp, origVec.Nsp)
		vector.SetCol(resultVector, resCol)
		if err = fn(origVec, resultVector); err != nil {
			return nil, err
		}
		return resultVector, nil
	}
}

func Acos(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Acos)
}

func Atan(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Atan)
}

func Cos(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Cos)
}

func Cot(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Cot)
}

func Exp(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Exp)
}

func Ln(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Ln)
}

func Log(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// MySQL log is the same as ln.
	return math1(vs, proc, momath.Ln)
}

func Sin(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Sin)
}

func Sinh(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Sinh)
}

func Tan(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return math1(vs, proc, momath.Tan)
}
