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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/operator"
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
		resultVector, err := proc.AllocVectorOfRows(types.T_float64.ToType(), vecLen, origVec.Nsp)
		if err != nil {
			return nil, err
		}
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
	//If the vs's lenght is 1, just use the  function with one parameter
	if len(vs) == 1 {
		return math1(vs, proc, momath.Atan)
	} else {
		return operator.Arith[float64, float64](vs, proc, vs[0].GetType(), momath.AtanWithTwoArg)
	}

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
	if len(vs) == 1 {
		return math1(vs, proc, momath.Ln)
	}
	if vs[0].IsScalarNull() {
		return vector.NewConstNull(vs[0].Typ, vs[1].Length()), nil
	}
	vals := vs[0].Col.([]float64)
	for i := range vals {
		if vals[i] == float64(1) {
			return nil, moerr.NewInvalidArgNoCtx("log base", 1)
		}
	}
	v1, err := math1([]*vector.Vector{vs[0]}, proc, momath.Ln)
	if err != nil {
		return nil, moerr.NewInvalidArgNoCtx("log input", "<= 0")
	}
	v2, err := math1([]*vector.Vector{vs[1]}, proc, momath.Ln)
	if err != nil {
		return nil, moerr.NewInvalidArgNoCtx("log input", "<= 0")
	}
	return operator.DivFloat[float64]([]*vector.Vector{v2, v1}, proc)
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
