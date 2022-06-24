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

func NotScalar(sv, nsv *vector.Vector, col1, col2 []bool, proc *process.Process) (*vector.Vector, error) {
	length := int64(vector.Length(nsv))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	value := col1[0]
	for i := range vcols {
		vcols[i] = value && col2[i]
	}
	nulls.Or(nsv.Nsp, nil, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}

func Not(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1 := vs[0]
	col1 := vector.MustTCols[bool](v1)
	if v1.IsScalarNull() {
		return proc.AllocScalarNullVector(retType), nil
	}

	c1 := v1.IsScalar()
	switch {
	case c1:
		vec := proc.AllocScalarVector(retType)
		vec.Col = make([]bool, 1)
		vec.Col.([]bool)[0] = !col1[0]
		return vec, nil
	}
	length := int64(vector.Length(v1))
	vec, err := allocateBoolVector(length, proc)
	if err != nil {
		return nil, err
	}
	vcols := vec.Col.([]bool)
	for i := range vcols {
		vcols[i] = !col1[i]
	}
	nulls.Or(v1.Nsp, nil, vec.Nsp)
	FillNullPos(vec)
	return vec, nil
}
