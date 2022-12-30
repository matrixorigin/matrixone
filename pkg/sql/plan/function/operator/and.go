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
)

func HandleAndNullCol(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	if v1.IsConstNull() {
		if v2.IsConstNull() {
			return proc.AllocScalarNullVector(boolType), nil
		} else if v2.IsConst() {
			vec := proc.AllocScalarVector(boolType)
			vec.Col = make([]bool, 1)
			value := vector.MustTCols[bool](v2)[0]
			if value {
				nulls.Add(vec.GetNulls(), 0)
			}
			return vec, nil
		} else {
			length := v2.Length()
			vec := allocateBoolVector(length, proc)
			value := vector.MustTCols[bool](v2)
			for i := 0; i < int(length); i++ {
				if value[i] || nulls.Contains(v2.GetNulls(), uint64(i)) {
					nulls.Add(vec.GetNulls(), uint64(i))
				}
			}
			return vec, nil
		}
	} else {
		if v1.IsConst() {
			vec := proc.AllocScalarVector(boolType)
			vec.Col = make([]bool, 1)
			value := vector.MustTCols[bool](v1)[0]
			if value {
				nulls.Add(vec.GetNulls(), 0)
			}
			return vec, nil
		} else {
			length := v1.Length()
			vec := allocateBoolVector(length, proc)
			value := vector.MustTCols[bool](v1)
			for i := 0; i < int(length); i++ {
				if value[i] || nulls.Contains(v1.GetNulls(), uint64(i)) {
					nulls.Add(vec.GetNulls(), uint64(i))
				}
			}
			return vec, nil
		}
	}
}

func ScalarAndNotScalar(_, nsv *vector.Vector, col1, col2 []bool, proc *process.Process) (*vector.Vector, error) {
	length := nsv.Length()
	vec := allocateBoolVector(length, proc)
	vcols := vector.MustTCols[bool](vec)
	value := col1[0]
	for i := range vcols {
		vcols[i] = value && col2[i]
	}
	if value {
		nulls.Or(nsv.GetNulls(), nil, vec.GetNulls())
	}
	return vec, nil
}

func And(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1, v2 := vs[0], vs[1]
	col1, col2 := vector.MustTCols[bool](v1), vector.MustTCols[bool](v2)
	if v1.IsConstNull() || v2.IsConstNull() {
		return HandleAndNullCol(vs, proc)
	}

	c1, c2 := v1.IsConst(), v2.IsConst()
	switch {
	case c1 && c2:
		vec := proc.AllocScalarVector(boolType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = col1[0] && col2[0]
		return vec, nil
	case c1 && !c2:
		return ScalarAndNotScalar(v1, v2, col1, col2, proc)
	case !c1 && c2:
		return ScalarAndNotScalar(v2, v1, col2, col1, proc)
	}
	// case !c1 && !c2
	length := v1.Length()
	vec := allocateBoolVector(length, proc)
	nulls.Or(v1.GetNulls(), v2.GetNulls(), vec.GetNulls())
	vcols := vector.MustTCols[bool](vec)
	for i := range vcols {
		vcols[i] = col1[i] && col2[i]
	}
	if nulls.Any(v1.GetNulls()) {
		rows := v1.GetNulls().Np.ToArray()
		cols := vector.MustTCols[bool](v2)
		for _, row := range rows {
			if !nulls.Contains(v2.GetNulls(), row) && !cols[row] {
				vec.GetNulls().Np.Remove(row)
			}
		}
	}
	if nulls.Any(v2.GetNulls()) {
		rows := v2.GetNulls().Np.ToArray()
		cols := vector.MustTCols[bool](v1)
		for _, row := range rows {
			if !nulls.Contains(v1.GetNulls(), row) && !cols[row] {
				vec.GetNulls().Np.Remove(row)
			}
		}
	}
	return vec, nil
}
