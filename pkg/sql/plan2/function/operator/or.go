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
	"errors"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ColOrCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	rvs, ok := rv.Col.([]bool)
	if !ok {
		return nil, errors.New("the right vec col is not []bool type")
	}
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	col := make([]bool, len(lvs))
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	for i := 0; i < len(lvs); i++ {
		col[i] = lvs[i] || rvs[i]
		ln, rn := nulls.Contains(lv.Nsp, uint64(i)), nulls.Contains(rv.Nsp, uint64(i))
		if (ln && !rn) || (!ln && rn) {
			if ln && !rn {
				if rvs[i] {
					vec.Nsp.Np.Remove(uint64(i))
				}
			} else {
				if lvs[i] {
					vec.Nsp.Np.Remove(uint64(i))
				}
			}
		}
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ColOrConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	rvs, ok := rv.Col.([]bool)
	if !ok {
		return nil, errors.New("the right vec col is not []bool type")
	}
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	rb := rvs[0]
	col := make([]bool, len(lvs))
	nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
	for i := 0; i < len(lvs); i++ {
		col[i] = lvs[i] || rb
		if nulls.Contains(lv.Nsp, uint64(i)) && rb {
			vec.Nsp.Np.Remove(uint64(i))
		}
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ColOrNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	col := make([]bool, len(lvs))
	for i := 0; i < len(lvs); i++ {
		if nulls.Contains(lv.Nsp, uint64(i)) || !lvs[i] {
			nulls.Add(vec.Nsp, uint64(i))
		} else {
			col[i] = true
		}
	}
	vector.SetCol(vec, col)
	return vec, nil
}

func ConstOrCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ColOrConst(rv, lv, proc)
}

func ConstOrConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	rvs, ok := rv.Col.([]bool)
	if !ok {
		return nil, errors.New("the right vec col is not []bool type")
	}
	vec := proc.AllocScalarVector(lv.Typ)
	vector.SetCol(vec, []bool{lvs[0] || rvs[0]})
	return vec, nil
}

func ConstOrNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs, ok := lv.Col.([]bool)
	if !ok {
		return nil, errors.New("the left vec col is not []bool type")
	}
	if !lvs[0] {
		return proc.AllocScalarNullVector(lv.Typ), nil
	} else {
		vec := proc.AllocScalarVector(lv.Typ)
		vector.SetCol(vec, []bool{true})
		return vec, nil
	}
}

func NullOrCol(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ColOrNull(rv, lv, proc)
}

func NullOrConst(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return ConstOrNull(rv, lv, proc)
}

func NullOrNull(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(lv.Typ), nil
}

type OrFunc = func(lv, rv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var OrFuncMap = map[int]OrFunc{}

var OrFuncVec = []OrFunc{
	ColOrCol, ColOrConst, ColOrNull,
	ConstOrCol, ConstOrConst, ConstOrNull,
	NullOrCol, NullOrConst, NullOrNull,
}

func InitOrFuncMap() {
	for i := 0; i < len(OrFuncVec); i++ {
		OrFuncMap[i] = OrFuncVec[i]
	}
}

func Or(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	lt, rt := GetTypeID(lv), GetTypeID(rv)
	vec, err := OrFuncMap[lt*3+rt](lv, rv, proc)
	if err != nil {
		return nil, errors.New("Or function: " + err.Error())
	}
	return vec, nil
}
