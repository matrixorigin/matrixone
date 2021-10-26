// Copyright 2021 Matrix Origin
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

package build

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/op/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/tree"
)

func (b *build) buildWhere(o op.OP, stmt *tree.Where) (op.OP, error) {
	e, err := b.buildPrunedExtend(o, stmt.Expr)
	if err != nil {
		return nil, err
	}
	if v, ok := e.(*extend.ValueExtend); ok {
		switch v.V.Typ.Oid {
		case types.T_int64:
			if v.V.Col.([]int64)[0] != 0 {
				return o, nil
			} else {
				return nil, nil
			}
		case types.T_float64:
			if v.V.Col.([]float64)[0] != 0 {
				return o, nil
			} else {
				return nil, nil
			}
		default:
			return nil, nil
		}
	}
	return restrict.New(o, e), nil
}
