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

func Is(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	l := int64(1)

	lefts, ok := lv.Col.([]bool)
	if !ok && !lv.IsScalarNull() {
		return nil, errors.New("the left vec col is not []bool type")
	}

	right, ok := rv.Col.(bool)
	if !ok {
		return nil, errors.New("the right vec col is not bool type")
	}

	if !lv.IsScalarNull() {
		l = int64(len(lefts))
	}
	vec, err := proc.AllocVector(lv.Typ, l*1)
	if err != nil {
		return nil, err
	}
	vec.IsConst = lv.IsScalar()
	col := make([]bool, l)
	for i := range lefts {
		if nulls.Contains(lv.Nsp, uint64(i)) {
			col[i] = false
		} else {
			col[i] = (lefts[i] == right)
		}
	}

	vector.SetCol(vec, col)
	return vec, nil
}
