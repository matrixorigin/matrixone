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
	bat        *batch.Batch
	isPrepared bool
}

func NewConstantFold(isPrepared bool) *ConstantFold {
	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	return &ConstantFold{
		bat:        bat,
		isPrepared: isPrepared,
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
		if el, ok := e.Expr.(*plan.Expr_List); ok {
			lenList := len(el.List.List)
			for i := 0; i < lenList; i++ {
				el.List.List[i] = r.constantFold(el.List.List[i], proc)
			}
		}
		return e
	}
	overloadID := ef.F.Func.GetObj()
	f, exists := function.GetFunctionByIDWithoutError(overloadID)
	if !exists {
		return e
	}
	if f.Volatile { // function cannot be fold
		return e
	}
	if f.RealTimeRelated && r.isPrepared {
		return e
	}
	for i := range ef.F.Args {
		ef.F.Args[i] = r.constantFold(ef.F.Args[i], proc)
	}
	if !IsConstant(e) {
		return e
	}
	vec, err := colexec.EvalExpr(r.bat, proc, e)
	if err != nil {
		return e
	}
	c := GetConstantValue(vec, false)
	if c == nil {
		return e
	}

	if f.RealTimeRelated {
		c.Src = &plan.Expr{
			Typ: &plan.Type{
				Id:          e.Typ.Id,
				NotNullable: e.Typ.NotNullable,
				Width:       e.Typ.Width,
				Size:        e.Typ.Size,
				Scale:       e.Typ.Scale,
				AutoIncr:    e.Typ.AutoIncr,
				Table:       e.Typ.Table,
			},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						Server:     ef.F.Func.GetServer(),
						Db:         ef.F.Func.GetDb(),
						Schema:     ef.F.Func.GetSchema(),
						Obj:        ef.F.Func.GetObj(),
						ServerName: ef.F.Func.GetServerName(),
						DbName:     ef.F.Func.GetDbName(),
						SchemaName: ef.F.Func.GetSchemaName(),
						ObjName:    ef.F.Func.GetObjName(),
					},
					Args: make([]*plan.Expr, 0),
				},
			},
		}
	} else {
		existRealTimeFunc := false
		for i, expr := range ef.F.Args {
			if ac, cok := expr.Expr.(*plan.Expr_C); cok && ac.C.Src != nil {
				if _, pok := ac.C.Src.Expr.(*plan.Expr_V); !pok {
					ef.F.Args[i] = ac.C.Src
					existRealTimeFunc = true
				}
			}
		}
		if existRealTimeFunc {
			c.Src = &plan.Expr{
				Typ: &plan.Type{
					Id:          e.Typ.Id,
					NotNullable: e.Typ.NotNullable,
					Width:       e.Typ.Width,
					Size:        e.Typ.Size,
					Scale:       e.Typ.Scale,
					AutoIncr:    e.Typ.AutoIncr,
					Table:       e.Typ.Table,
				},
				Expr: ef,
			}
		}
	}

	ec := &plan.Expr_C{
		C: c,
	}
	e.Typ = &plan.Type{Id: int32(vec.Typ.Oid), Scale: vec.Typ.Scale, Width: vec.Typ.Width, Size: vec.Typ.Size}
	e.Expr = ec
	return e
}

func GetConstantValue(vec *vector.Vector, transAll bool) *plan.Const {
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
	case types.T_float32:
		return &plan.Const{
			Value: &plan.Const_Fval{
				Fval: vec.Col.([]float32)[0],
			},
		}
	case types.T_float64:
		return &plan.Const{
			Value: &plan.Const_Dval{
				Dval: vec.Col.([]float64)[0],
			},
		}
	case types.T_varchar, types.T_char,
		types.T_binary, types.T_varbinary, types.T_text, types.T_blob:
		return &plan.Const{
			Value: &plan.Const_Sval{
				Sval: vec.GetString(0),
			},
		}
	case types.T_json:
		if !transAll {
			return nil
		}
		return &plan.Const{
			Value: &plan.Const_Sval{
				Sval: vec.GetString(0),
			},
		}
	case types.T_timestamp:
		return &plan.Const{
			Value: &plan.Const_Timestampval{
				Timestampval: int64(vector.MustTCols[types.Timestamp](vec)[0]),
			},
		}
	case types.T_date:
		return &plan.Const{
			Value: &plan.Const_Dateval{
				Dateval: int32(vector.MustTCols[types.Date](vec)[0]),
			},
		}
	case types.T_time:
		if !transAll {
			return nil
		}
		return &plan.Const{
			Value: &plan.Const_Timeval{
				Timeval: int64(vector.MustTCols[types.Time](vec)[0]),
			},
		}
	case types.T_datetime:
		if !transAll {
			return nil
		}
		return &plan.Const{
			Value: &plan.Const_Datetimeval{
				Datetimeval: int64(vector.MustTCols[types.Datetime](vec)[0]),
			},
		}
	case types.T_decimal64:
		if !transAll {
			return nil
		}
		return &plan.Const{
			Value: &plan.Const_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: types.Decimal64ToInt64Raw(vector.MustTCols[types.Decimal64](vec)[0])},
			},
		}
	case types.T_decimal128:
		if !transAll {
			return nil
		}
		decimalValue := &plan.Decimal128{}
		decimalValue.A, decimalValue.B = types.Decimal128ToInt64Raw(vector.MustTCols[types.Decimal128](vec)[0])
		return &plan.Const{Value: &plan.Const_Decimal128Val{Decimal128Val: decimalValue}}
	default:
		return nil
	}
}

func IsConstant(e *plan.Expr) bool {
	switch ef := e.Expr.(type) {
	case *plan.Expr_C, *plan.Expr_T:
		return true
	case *plan.Expr_F:
		overloadID := ef.F.Func.GetObj()
		f, exists := function.GetFunctionByIDWithoutError(overloadID)
		if !exists {
			return false
		}
		if f.Volatile { // function cannot be fold
			return false
		}
		for i := range ef.F.Args {
			if !IsConstant(ef.F.Args[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
