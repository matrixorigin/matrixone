// Copyright 2021 - 2022 Matrix Origin
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

package rule

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type ConstantFold struct {
	bat *batch.Batch
}

func NewConstantFlod() *ConstantFold {
	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	return &ConstantFold{
		bat: bat,
	}
}

// Match always true
func (r *ConstantFold) Match(n *plan.Node) bool {
	return true
}

func (r *ConstantFold) Apply(n *plan.Node, _ *plan.Query) {
	if n.Limit != nil {
		n.Limit = r.constantFold(n.Limit)
	}
	if n.Offset != nil {
		n.Offset = r.constantFold(n.Offset)
	}
	if len(n.OnList) > 0 {
		for i := range n.OnList {
			n.OnList[i] = r.constantFold(n.OnList[i])
		}
	}
	if len(n.FilterList) > 0 {
		for i := range n.FilterList {
			n.FilterList[i] = r.constantFold(n.FilterList[i])
		}
	}
	if len(n.ProjectList) > 0 {
		for i := range n.ProjectList {
			n.ProjectList[i] = r.constantFold(n.ProjectList[i])
		}
	}
}

func (r *ConstantFold) constantFold(e *plan.Expr) *plan.Expr {
	ef, ok := e.Expr.(*plan.Expr_F)
	if !ok {
		return e
	}
	overloadID := ef.F.Func.GetObj()
	f, err := function.GetFunctionByID(overloadID)
	if err != nil {
		return e
	}
	if f.Volatile { // function cannot be fold
		return e
	}
	for i := range ef.F.Args {
		ef.F.Args[i] = r.constantFold(ef.F.Args[i])
	}
	if !isConstant(e) {
		return e
	}
	vec, err := colexec.EvalExpr(r.bat, nil, e)
	if err != nil {
		return e
	}
	c := getConstantValue(vec)
	if c == nil {
		return e
	}
	ec := &plan.Expr_C{
		C: c,
	}
	e.Expr = ec
	return e
}

func getConstantValue(vec *vector.Vector) *plan.Const {
	if nulls.Any(vec.Nsp) {
		return &plan.Const{Isnull: true}
	}
	switch vec.Typ.Oid {
	case types.T_bool:
		return &plan.Const{
			Value: &plan.Const_Bval{
				Bval: vec.Col.([]bool)[0],
			},
		}
	case types.T_int64:
		return &plan.Const{
			Value: &plan.Const_Ival{
				Ival: vec.Col.([]int64)[0],
			},
		}
	case types.T_float64:
		return &plan.Const{
			Value: &plan.Const_Dval{
				Dval: vec.Col.([]float64)[0],
			},
		}
	case types.T_varchar:
		return &plan.Const{
			Value: &plan.Const_Sval{
				Sval: string(vec.Col.(*types.Bytes).Data),
			},
		}
	default:
		return nil
	}
}

func isConstant(e *plan.Expr) bool {
	switch ef := e.Expr.(type) {
	case *plan.Expr_C:
		return true
	case *plan.Expr_F:
		overloadID := ef.F.Func.GetObj()
		f, err := function.GetFunctionByID(overloadID)
		if err != nil {
			return false
		}
		if f.Volatile { // function cannot be fold
			return false
		}
		for i := range ef.F.Args {
			if !isConstant(ef.F.Args[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
