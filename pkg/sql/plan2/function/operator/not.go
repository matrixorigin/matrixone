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

func NotCol(lv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[bool](lv)
	n := len(lvs)
	vec, err := proc.AllocVector(lv.Typ, int64(n)*1)
	if err != nil {
		return nil, err
	}
	col := make([]bool, len(lvs))
	for i := 0; i < len(lvs); i++ {
		col[i] = !lvs[i]
		if nulls.Contains(lv.Nsp, uint64(i)) {
			col[i] = false
		}
	}
	nulls.Or(lv.Nsp, nil, vec.Nsp)
	vector.SetCol(vec, col)
	return vec, nil
}

func NotConst(lv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lvs := vector.MustTCols[bool](lv)
	vec := proc.AllocScalarVector(lv.Typ)
	vector.SetCol(vec, []bool{!lvs[0]})
	return vec, nil
}

func NotNull(lv *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return proc.AllocScalarNullVector(lv.Typ), nil
}

type NotFunc = func(lv *vector.Vector, proc *process.Process) (*vector.Vector, error)

var NotFuncMap = map[int]NotFunc{}

var NotFuncVec = []NotFunc{
	NotCol, NotConst, NotNull,
}

func InitNotFuncMap() {
	for i := 0; i < len(NotFuncVec); i++ {
		NotFuncMap[i] = NotFuncVec[i]
	}
}

func Not(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	lt := GetTypeID(lv)
	vec, err := NotFuncMap[lt](lv, proc)
	if err != nil {
		return nil, errors.New("Not function: " + err.Error())
	}
	return vec, nil
}
