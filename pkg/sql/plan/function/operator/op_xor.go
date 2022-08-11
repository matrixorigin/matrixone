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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type opXorT interface {
	constraints.Integer
}

func OpXorGeneral[T opXorT](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vs[0], vs[1]
	returnTyp := left.Typ

	if left.IsScalarNull() {
		return proc.AllocConstNullVector(returnTyp, vector.Length(right)), nil
	}
	if right.IsScalarNull() {
		return proc.AllocConstNullVector(returnTyp, vector.Length(left)), nil
	}

	getXorVec := func(scalar *vector.Vector, noScalar *vector.Vector) (*vector.Vector, error) {
		typeLen := noScalar.Typ.Oid.TypeLen()
		vecLen := vector.Length(noScalar)
		vec, err := proc.AllocVector(returnTyp, int64(vecLen*typeLen))
		if err != nil {
			return nil, err
		}
		vec.Col = vector.DecodeFixedCol[T](vec, typeLen)
		vec.Col = vec.Col.([]T)[:vecLen]
		if nulls.Any(noScalar.Nsp) {
			for i := 0; i < vecLen; i++ {
				if noScalar.Nsp.Contains(uint64(i)) {
					nulls.Add(vec.Nsp, uint64(i))
				} else {
					vec.Col.([]T)[i] = scalar.Col.([]T)[0] ^ noScalar.Col.([]T)[i]
				}
			}
		} else {
			for i := 0; i < vecLen; i++ {
				vec.Col.([]T)[i] = scalar.Col.([]T)[0] ^ noScalar.Col.([]T)[i]
			}
		}
		return vec, nil
	}

	if left.IsScalar() && right.IsScalar() {
		vec := proc.AllocScalarVector(left.Typ.Oid.ToType())
		vec.Col = make([]T, 1)
		vec.Col.([]T)[0] = left.Col.([]T)[0] ^ right.Col.([]T)[0]
		return vec, nil
	} else {
		if left.IsScalar() {
			return getXorVec(left, right)
		} else if right.IsScalar() {
			return getXorVec(right, left)
		} else {
			typeLen := left.Typ.Oid.TypeLen()
			vecLen := vector.Length(left)
			vec, err := proc.AllocVector(returnTyp, int64(vecLen*typeLen))
			if err != nil {
				return nil, err
			}
			vec.Col = vector.DecodeFixedCol[T](vec, typeLen)
			vec.Col = vec.Col.([]T)[:vecLen]

			for i := 0; i < vecLen; i++ {
				if left.Nsp.Contains(uint64(i)) || right.Nsp.Contains(uint64(i)) {
					nulls.Add(vec.Nsp, uint64(i))
				} else {
					vec.Col.([]T)[i] = left.Col.([]T)[i] ^ right.Col.([]T)[i]
				}
			}
			return vec, nil
		}
	}
}
