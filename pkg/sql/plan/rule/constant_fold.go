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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ConstantFold struct {
	bat *batch.Batch
}

func NewConstantFold() *ConstantFold {
	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	return &ConstantFold{
		bat: bat,
	}
}

func (r *ConstantFold) GetBatch() *batch.Batch {
	return r.bat
}

// Match always true
func (r *ConstantFold) Match(n *plan.Node) bool {
	return true
}

func (r *ConstantFold) Apply(n *plan.Node, _ *plan.Query, proc *process.Process) {
	if n.Limit != nil {
		n.Limit = r.constantFold(n.Limit, proc)
	}
	if n.Offset != nil {
		n.Offset = r.constantFold(n.Offset, proc)
	}
	if len(n.OnList) > 0 {
		for i := range n.OnList {
			n.OnList[i] = r.constantFold(n.OnList[i], proc)
		}
	}
	if len(n.FilterList) > 0 {
		for i := range n.FilterList {
			n.FilterList[i] = r.constantFold(n.FilterList[i], proc)
		}
	}
	if len(n.ProjectList) > 0 {
		for i := range n.ProjectList {
			n.ProjectList[i] = r.constantFold(n.ProjectList[i], proc)
		}
	}
}

func (r *ConstantFold) constantFold(e *plan.Expr, proc *process.Process) *plan.Expr {
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
		ef.F.Args[i] = r.constantFold(ef.F.Args[i], proc)
	}
	if !isConstant(e) {
		return e
	}
	vec, err := colexec.EvalExpr(r.bat, proc, e)
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
	case types.T_int8:
		return &plan.Const{
			Value: &plan.Const_I8Val{
				I8Val: int32(vec.Col.([]int8)[0]),
			},
		}
	case types.T_int16:
		return &plan.Const{
			Value: &plan.Const_I16Val{
				I16Val: int32(vec.Col.([]int16)[0]),
			},
		}
	case types.T_int32:
		return &plan.Const{
			Value: &plan.Const_I32Val{
				I32Val: vec.Col.([]int32)[0],
			},
		}
	case types.T_int64:
		return &plan.Const{
			Value: &plan.Const_I64Val{
				I64Val: vec.Col.([]int64)[0],
			},
		}
	case types.T_uint8:
		return &plan.Const{
			Value: &plan.Const_U8Val{
				U8Val: uint32(vec.Col.([]uint8)[0]),
			},
		}
	case types.T_uint16:
		return &plan.Const{
			Value: &plan.Const_U16Val{
				U16Val: uint32(vec.Col.([]uint16)[0]),
			},
		}
	case types.T_uint32:
		return &plan.Const{
			Value: &plan.Const_U32Val{
				U32Val: vec.Col.([]uint32)[0],
			},
		}
	case types.T_uint64:
		return &plan.Const{
			Value: &plan.Const_U64Val{
				U64Val: vec.Col.([]uint64)[0],
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
				Sval: vec.GetString(0),
			},
		}
	default:
		return nil
	}
}

func isConstant(e *plan.Expr) bool {
	switch ef := e.Expr.(type) {
	case *plan.Expr_C, *plan.Expr_T:
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
