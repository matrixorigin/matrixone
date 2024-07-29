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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
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
	bat := batch.EmptyForConstFoldBatch
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
	// if n.Interval != nil {
	// 	n.Interval = r.constantFold(n.Interval, proc)
	// }
	// if n.Sliding != nil {
	// 	n.Sliding = r.constantFold(n.Sliding, proc)
	// }

	for i := range n.OnList {
		n.OnList[i] = r.constantFold(n.OnList[i], proc)
	}

	for i := range n.FilterList {
		n.FilterList[i] = r.constantFold(n.FilterList[i], proc)
	}

	for i := range n.BlockFilterList {
		n.BlockFilterList[i] = r.constantFold(n.BlockFilterList[i], proc)
	}

	for i := range n.ProjectList {
		n.ProjectList[i] = r.constantFold(n.ProjectList[i], proc)
	}

	for i := range n.GroupBy {
		n.GroupBy[i] = r.constantFold(n.GroupBy[i], proc)
	}

	// for i := range n.GroupingSet {
	// 	n.GroupingSet[i] = r.constantFold(n.GroupingSet[i], proc)
	// }

	for i := range n.AggList {
		n.AggList[i] = r.constantFold(n.AggList[i], proc)
	}

	for i := range n.WinSpecList {
		n.WinSpecList[i] = r.constantFold(n.WinSpecList[i], proc)
	}

	for _, orderBy := range n.OrderBy {
		orderBy.Expr = r.constantFold(orderBy.Expr, proc)
	}

	// for i := range n.TblFuncExprList {
	// 	n.TblFuncExprList[i] = r.constantFold(n.TblFuncExprList[i], proc)
	// }

	// for i := range n.FillVal {
	// 	n.FillVal[i] = r.constantFold(n.FillVal[i], proc)
	// }

	for i := range n.OnUpdateExprs {
		n.OnUpdateExprs[i] = r.constantFold(n.OnUpdateExprs[i], proc)
	}
}

func (r *ConstantFold) constantFold(expr *plan.Expr, proc *process.Process) *plan.Expr {
	if expr.Typ.Id == int32(types.T_interval) {
		panic(moerr.NewInternalError(proc.Ctx, "not supported type INTERVAL"))
	}

	fn := expr.GetF()
	if fn == nil {
		if elist := expr.GetList(); elist != nil {
			exprList := elist.List
			cannotFold := false
			for i := range exprList {
				exprList[i] = r.constantFold(exprList[i], proc)
				if exprList[i].GetLit() == nil {
					cannotFold = true
				}
			}

			if cannotFold {
				return expr
			}

			vec, err := colexec.GenerateConstListExpressionExecutor(proc, exprList)
			if err != nil {
				return expr
			}
			defer vec.Free(proc.Mp())

			vec.InplaceSortAndCompact()

			data, err := vec.MarshalBinary()
			if err != nil {
				return expr
			}

			return &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Vec{
					Vec: &plan.LiteralVec{
						Len:  int32(vec.Length()),
						Data: data,
					},
				},
			}
		}

		return expr
	}
	overloadID := fn.Func.GetObj()
	f, exists := function.GetFunctionByIdWithoutError(overloadID)
	if !exists {
		return expr
	}
	if f.CannotFold() { // function cannot be fold
		return expr
	}
	if f.IsRealTimeRelated() && r.isPrepared {
		return expr
	}
	isVec := false
	for i := range fn.Args {
		fn.Args[i] = r.constantFold(fn.Args[i], proc)
		isVec = isVec || fn.Args[i].GetVec() != nil
	}
	if f.IsAgg() || f.IsWin() {
		return expr
	}
	if !IsConstant(expr, false) {
		return expr
	}

	vec, err := colexec.EvalExpressionOnce(proc, expr, []*batch.Batch{r.bat})
	if err != nil {
		return expr
	}
	defer vec.Free(proc.Mp())

	if isVec {
		data, err := vec.MarshalBinary()
		if err != nil {
			return expr
		}

		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Vec{
				Vec: &plan.LiteralVec{
					Len:  int32(vec.Length()),
					Data: data,
				},
			},
		}
	}

	c := GetConstantValue(vec, false, 0)
	if c == nil {
		return expr
	}

	if f.IsRealTimeRelated() {
		c.Src = &plan.Expr{
			Typ: plan.Type{
				Id:          expr.Typ.Id,
				NotNullable: expr.Typ.NotNullable,
				Width:       expr.Typ.Width,
				Scale:       expr.Typ.Scale,
				AutoIncr:    expr.Typ.AutoIncr,
				Table:       expr.Typ.Table,
			},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						Server:     fn.Func.GetServer(),
						Db:         fn.Func.GetDb(),
						Schema:     fn.Func.GetSchema(),
						Obj:        fn.Func.GetObj(),
						ServerName: fn.Func.GetServerName(),
						DbName:     fn.Func.GetDbName(),
						SchemaName: fn.Func.GetSchemaName(),
						ObjName:    fn.Func.GetObjName(),
					},
					Args: make([]*plan.Expr, 0),
				},
			},
		}
	} else {
		existRealTimeFunc := false
		for i, expr := range fn.Args {
			if ac, cok := expr.Expr.(*plan.Expr_Lit); cok && ac.Lit.Src != nil {
				if _, pok := ac.Lit.Src.Expr.(*plan.Expr_V); !pok {
					fn.Args[i] = ac.Lit.Src
					existRealTimeFunc = true
				}
			}
		}
		if existRealTimeFunc {
			c.Src = &plan.Expr{
				Typ: plan.Type{
					Id:          expr.Typ.Id,
					NotNullable: expr.Typ.NotNullable,
					Width:       expr.Typ.Width,
					Scale:       expr.Typ.Scale,
					AutoIncr:    expr.Typ.AutoIncr,
					Table:       expr.Typ.Table,
				},
				Expr: &plan.Expr_F{
					F: fn,
				},
			}
		}
	}

	ec := &plan.Expr_Lit{
		Lit: c,
	}
	expr.Typ = plan.Type{Id: int32(vec.GetType().Oid), Scale: vec.GetType().Scale, Width: vec.GetType().Width}
	expr.Expr = ec

	return expr
}

func GetConstantValue(vec *vector.Vector, transAll bool, row uint64) *plan.Literal {
	if vec.IsConstNull() || vec.GetNulls().Contains(row) {
		return &plan.Literal{Isnull: true}
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		return &plan.Literal{
			Value: &plan.Literal_Bval{
				Bval: vector.MustFixedCol[bool](vec)[row],
			},
		}
	case types.T_bit:
		return &plan.Literal{
			Value: &plan.Literal_U64Val{
				U64Val: vector.MustFixedCol[uint64](vec)[row],
			},
		}
	case types.T_int8:
		return &plan.Literal{
			Value: &plan.Literal_I8Val{
				I8Val: int32(vector.MustFixedCol[int8](vec)[row]),
			},
		}
	case types.T_int16:
		return &plan.Literal{
			Value: &plan.Literal_I16Val{
				I16Val: int32(vector.MustFixedCol[int16](vec)[row]),
			},
		}
	case types.T_int32:
		return &plan.Literal{
			Value: &plan.Literal_I32Val{
				I32Val: vector.MustFixedCol[int32](vec)[row],
			},
		}
	case types.T_int64:
		return &plan.Literal{
			Value: &plan.Literal_I64Val{
				I64Val: vector.MustFixedCol[int64](vec)[row],
			},
		}
	case types.T_uint8:
		return &plan.Literal{
			Value: &plan.Literal_U8Val{
				U8Val: uint32(vector.MustFixedCol[uint8](vec)[row]),
			},
		}
	case types.T_uint16:
		return &plan.Literal{
			Value: &plan.Literal_U16Val{
				U16Val: uint32(vector.MustFixedCol[uint16](vec)[row]),
			},
		}
	case types.T_uint32:
		return &plan.Literal{
			Value: &plan.Literal_U32Val{
				U32Val: vector.MustFixedCol[uint32](vec)[row],
			},
		}
	case types.T_uint64:
		return &plan.Literal{
			Value: &plan.Literal_U64Val{
				U64Val: vector.MustFixedCol[uint64](vec)[row],
			},
		}
	case types.T_float32:
		return &plan.Literal{
			Value: &plan.Literal_Fval{
				Fval: vector.MustFixedCol[float32](vec)[row],
			},
		}
	case types.T_float64:
		return &plan.Literal{
			Value: &plan.Literal_Dval{
				Dval: vector.MustFixedCol[float64](vec)[row],
			},
		}
	case types.T_varchar, types.T_char,
		types.T_binary, types.T_varbinary, types.T_text, types.T_blob, types.T_datalink:
		return &plan.Literal{
			Value: &plan.Literal_Sval{
				Sval: vec.GetStringAt(int(row)),
			},
		}
	case types.T_json:
		if !transAll {
			return nil
		}
		return &plan.Literal{
			Value: &plan.Literal_Sval{
				Sval: vec.GetStringAt(int(row)),
			},
		}
	case types.T_timestamp:
		return &plan.Literal{
			Value: &plan.Literal_Timestampval{
				Timestampval: int64(vector.MustFixedCol[types.Timestamp](vec)[row]),
			},
		}
	case types.T_date:
		return &plan.Literal{
			Value: &plan.Literal_Dateval{
				Dateval: int32(vector.MustFixedCol[types.Date](vec)[row]),
			},
		}
	case types.T_time:
		return &plan.Literal{
			Value: &plan.Literal_Timeval{
				Timeval: int64(vector.MustFixedCol[types.Time](vec)[row]),
			},
		}
	case types.T_datetime:
		return &plan.Literal{
			Value: &plan.Literal_Datetimeval{
				Datetimeval: int64(vector.MustFixedCol[types.Datetime](vec)[row]),
			},
		}
	case types.T_enum:
		if !transAll {
			return nil
		}
		return &plan.Literal{
			Value: &plan.Literal_EnumVal{
				EnumVal: uint32(vector.MustFixedCol[types.Enum](vec)[row]),
			},
		}
	case types.T_decimal64:
		return &plan.Literal{
			Value: &plan.Literal_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: int64(vector.MustFixedCol[types.Decimal64](vec)[row])},
			},
		}
	case types.T_decimal128:
		decimalValue := &plan.Decimal128{}
		decimalValue.A = int64(vector.MustFixedCol[types.Decimal128](vec)[row].B0_63)
		decimalValue.B = int64(vector.MustFixedCol[types.Decimal128](vec)[row].B64_127)
		return &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: decimalValue}}
	default:
		return nil
	}
}

func IsConstant(e *plan.Expr, varAndParamIsConst bool) bool {
	switch ef := e.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_Vec:
		return true
	case *plan.Expr_F:
		overloadID := ef.F.Func.GetObj()
		f, exists := function.GetFunctionByIdWithoutError(overloadID)
		if !exists {
			return false
		}
		if f.CannotFold() { // function cannot be fold
			return false
		}
		if f.IsRealTimeRelated() && !varAndParamIsConst {
			return false
		}
		for i := range ef.F.Args {
			if !IsConstant(ef.F.Args[i], varAndParamIsConst) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		for _, arg := range ef.List.List {
			if !IsConstant(arg, varAndParamIsConst) {
				return false
			}
		}
		return true
	case *plan.Expr_P:
		return varAndParamIsConst
	case *plan.Expr_V:
		return varAndParamIsConst
	default:
		return false
	}
}
