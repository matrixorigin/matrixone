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
func (r *ConstantFold) Match(node *plan.Node) bool {
	return true
}

func (r *ConstantFold) Apply(node *plan.Node, _ *plan.Query, proc *process.Process) {
	if node.Limit != nil {
		node.Limit = r.constantFold(node.Limit, proc)
	}
	if node.Offset != nil {
		node.Offset = r.constantFold(node.Offset, proc)
	}
	// if n.Interval != nil {
	// 	n.Interval = r.constantFold(n.Interval, proc)
	// }
	// if n.Sliding != nil {
	// 	n.Sliding = r.constantFold(n.Sliding, proc)
	// }

	for i := range node.OnList {
		node.OnList[i] = r.constantFold(node.OnList[i], proc)
	}

	for i := range node.FilterList {
		node.FilterList[i] = r.constantFold(node.FilterList[i], proc)
	}

	for i := range node.BlockFilterList {
		node.BlockFilterList[i] = r.constantFold(node.BlockFilterList[i], proc)
	}

	for i := range node.ProjectList {
		node.ProjectList[i] = r.constantFold(node.ProjectList[i], proc)
	}

	for i := range node.GroupBy {
		node.GroupBy[i] = r.constantFold(node.GroupBy[i], proc)
	}

	// for i := range n.GroupingSet {
	// 	n.GroupingSet[i] = r.constantFold(n.GroupingSet[i], proc)
	// }

	for i := range node.AggList {
		node.AggList[i] = r.constantFold(node.AggList[i], proc)
	}

	for i := range node.WinSpecList {
		node.WinSpecList[i] = r.constantFold(node.WinSpecList[i], proc)
	}

	for _, orderBy := range node.OrderBy {
		orderBy.Expr = r.constantFold(orderBy.Expr, proc)
	}

	if node.IndexReaderParam != nil {
		for _, orderBy := range node.IndexReaderParam.OrderBy {
			orderBy.Expr = r.constantFold(orderBy.Expr, proc)
		}

		node.IndexReaderParam.Limit = r.constantFold(node.IndexReaderParam.Limit, proc)

		if distRange := node.IndexReaderParam.DistRange; distRange != nil {
			distRange.LowerBound = r.constantFold(distRange.LowerBound, proc)
			distRange.UpperBound = r.constantFold(distRange.UpperBound, proc)
		}
	}

	// for i := range n.TblFuncExprList {
	// 	n.TblFuncExprList[i] = r.constantFold(n.TblFuncExprList[i], proc)
	// }

	// for i := range n.FillVal {
	// 	n.FillVal[i] = r.constantFold(n.FillVal[i], proc)
	// }

	for i := range node.OnUpdateExprs {
		node.OnUpdateExprs[i] = r.constantFold(node.OnUpdateExprs[i], proc)
	}
}

func (r *ConstantFold) constantFold(expr *plan.Expr, proc *process.Process) *plan.Expr {
	if expr == nil {
		return expr
	}

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

	// Skip constant folding for division/modulo by zero.
	// This allows runtime to check sql_mode and statement type for proper error handling.
	if IsDivisionByZeroConstant(fn) {
		return expr
	}

	vec, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{r.bat})
	if err != nil {
		return expr
	}
	defer free()

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
	// Preserve the original expr.Typ (from retType) instead of using vec.GetType()
	// This ensures that expr.Typ matches the retType, preventing type mismatches
	// when FunctionExpressionExecutor creates result wrapper using planExpr.Typ
	// For example, TIMESTAMPADD with DATE input returns DATETIME type (from retType),
	// but the actual vector type might be DATETIME (after TempSetType) or DATE (before TempSetType)
	// We should preserve the retType (DATETIME) to ensure consistency
	expr.Typ = plan.Type{Id: expr.Typ.Id, Scale: vec.GetType().Scale, Width: vec.GetType().Width}
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
				Bval: vector.MustFixedColNoTypeCheck[bool](vec)[row],
			},
		}
	case types.T_bit:
		return &plan.Literal{
			Value: &plan.Literal_U64Val{
				U64Val: vector.MustFixedColNoTypeCheck[uint64](vec)[row],
			},
		}
	case types.T_int8:
		return &plan.Literal{
			Value: &plan.Literal_I8Val{
				I8Val: int32(vector.MustFixedColNoTypeCheck[int8](vec)[row]),
			},
		}
	case types.T_int16:
		return &plan.Literal{
			Value: &plan.Literal_I16Val{
				I16Val: int32(vector.MustFixedColNoTypeCheck[int16](vec)[row]),
			},
		}
	case types.T_int32:
		return &plan.Literal{
			Value: &plan.Literal_I32Val{
				I32Val: vector.MustFixedColNoTypeCheck[int32](vec)[row],
			},
		}
	case types.T_int64:
		return &plan.Literal{
			Value: &plan.Literal_I64Val{
				I64Val: vector.MustFixedColNoTypeCheck[int64](vec)[row],
			},
		}
	case types.T_uint8:
		return &plan.Literal{
			Value: &plan.Literal_U8Val{
				U8Val: uint32(vector.MustFixedColNoTypeCheck[uint8](vec)[row]),
			},
		}
	case types.T_uint16:
		return &plan.Literal{
			Value: &plan.Literal_U16Val{
				U16Val: uint32(vector.MustFixedColNoTypeCheck[uint16](vec)[row]),
			},
		}
	case types.T_uint32:
		return &plan.Literal{
			Value: &plan.Literal_U32Val{
				U32Val: vector.MustFixedColNoTypeCheck[uint32](vec)[row],
			},
		}
	case types.T_uint64:
		return &plan.Literal{
			Value: &plan.Literal_U64Val{
				U64Val: vector.MustFixedColNoTypeCheck[uint64](vec)[row],
			},
		}
	case types.T_float32:
		return &plan.Literal{
			Value: &plan.Literal_Fval{
				Fval: vector.MustFixedColNoTypeCheck[float32](vec)[row],
			},
		}
	case types.T_float64:
		return &plan.Literal{
			Value: &plan.Literal_Dval{
				Dval: vector.MustFixedColNoTypeCheck[float64](vec)[row],
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
				Timestampval: int64(vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[row]),
			},
		}
	case types.T_date:
		return &plan.Literal{
			Value: &plan.Literal_Dateval{
				Dateval: int32(vector.MustFixedColNoTypeCheck[types.Date](vec)[row]),
			},
		}
	case types.T_time:
		return &plan.Literal{
			Value: &plan.Literal_Timeval{
				Timeval: int64(vector.MustFixedColNoTypeCheck[types.Time](vec)[row]),
			},
		}
	case types.T_datetime:
		return &plan.Literal{
			Value: &plan.Literal_Datetimeval{
				Datetimeval: int64(vector.MustFixedColNoTypeCheck[types.Datetime](vec)[row]),
			},
		}
	case types.T_enum:
		if !transAll {
			return nil
		}
		return &plan.Literal{
			Value: &plan.Literal_EnumVal{
				EnumVal: uint32(vector.MustFixedColNoTypeCheck[types.Enum](vec)[row]),
			},
		}
	case types.T_decimal64:
		return &plan.Literal{
			Value: &plan.Literal_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: int64(vector.MustFixedColNoTypeCheck[types.Decimal64](vec)[row])},
			},
		}
	case types.T_decimal128:
		decimalValue := &plan.Decimal128{}
		decimalValue.A = int64(vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[row].B0_63)
		decimalValue.B = int64(vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[row].B64_127)
		return &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: decimalValue}}
	case types.T_array_float32, types.T_array_float64:
		data := vec.GetStringAt(int(row))
		return &plan.Literal{
			Value: &plan.Literal_VecVal{
				VecVal: data,
			},
		}
	default:
		return nil
	}
}

func GetConstantValue2(proc *process.Process, expr *plan.Expr, vec *vector.Vector) (get bool, err error) {
	if cExpr, ok := expr.Expr.(*plan.Expr_Lit); ok {
		if cExpr.Lit.Isnull {
			err = vector.AppendBytes(vec, nil, true, proc.Mp())
			return true, err
		}
		switch vec.GetType().Oid {
		case types.T_bool:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Bval); ok {
				val := val.Bval
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_bit:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				val := val.U64Val
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_int8:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_I8Val); ok {
				val := int8(val.I8Val)
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_int16:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_I16Val); ok {
				val := int16(val.I16Val)
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_int32:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_I32Val); ok {
				val := val.I32Val
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_int64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_I64Val); ok {
				val := val.I64Val
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_uint8:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U8Val); ok {
				val := uint8(val.U8Val)
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_uint16:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U16Val); ok {
				val := uint16(val.U16Val)
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_uint32:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U32Val); ok {
				val := val.U32Val
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_uint64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				val := val.U64Val
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_float32:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Fval); ok {
				val := val.Fval
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_float64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Dval); ok {
				val := val.Dval
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary, types.T_text,
			types.T_blob, types.T_datalink, types.T_json:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Sval); ok {
				val := val.Sval
				err = vector.AppendBytes(vec, []byte(val), false, proc.Mp())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, false, proc.Mp())
				return false, err
			}
		case types.T_array_float32, types.T_array_float64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_VecVal); ok {
				val := val.VecVal
				err = vector.AppendBytes(vec, []byte(val), false, proc.Mp())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, false, proc.Mp())
				return false, err
			}
		case types.T_timestamp:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Timestampval); ok {
				val := val.Timestampval
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_date:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Dval); ok {
				val := val.Dval
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_time:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Dval); ok {
				val := val.Dval
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_datetime:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Datetimeval); ok {
				val := val.Datetimeval
				err = vector.AppendFixed(vec, types.Datetime(val), false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return true, err
			}
		case types.T_enum:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_EnumVal); ok {
				val := val.EnumVal
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_decimal64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Decimal64Val); ok {
				val := val.Decimal64Val.A
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_decimal128:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Decimal128Val); ok {
				val := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
				err = vector.AppendFixed(vec, val, false, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		default:
			return false, nil
		}
	} else {
		err = vector.AppendBytes(vec, nil, true, proc.Mp())
		return false, err
	}
}

func IsConstant(e *plan.Expr, varAndParamIsConst bool) bool {
	switch ef := e.GetExpr().(type) {
	case *plan.Expr_Lit, *plan.Expr_T, *plan.Expr_Vec:
		return true
	case *plan.Expr_F:
		// CASE expressions should always be evaluated at runtime to preserve
		// branch semantics; treat them as non-constant.
		if fid, _ := function.DecodeOverloadID(ef.F.Func.GetObj()); fid == function.CASE {
			return false
		}
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
	case *plan.Expr_P, *plan.Expr_V:
		return varAndParamIsConst
	default:
		return false
	}
}

// IsDivisionByZeroConstant checks if the expression is a division/modulo operation
// where the divisor is a constant zero or either operand is NULL.
// We skip constant folding for such cases to allow runtime to properly handle them.
func IsDivisionByZeroConstant(fn *plan.Function) bool {
	fid, _ := function.DecodeOverloadID(fn.Func.GetObj())
	if fid != function.DIV && fid != function.INTEGER_DIV && fid != function.MOD {
		return false
	}
	if len(fn.Args) < 2 {
		return false
	}

	// Check if either operand is NULL
	for _, arg := range fn.Args {
		lit := arg.GetLit()
		if lit != nil && lit.GetIsnull() {
			return true
		}
	}

	divisor := fn.Args[1]
	lit := divisor.GetLit()
	if lit == nil {
		return false
	}
	return isZeroLiteral(lit)
}

// isZeroLiteral checks if a literal value is zero
func isZeroLiteral(lit *plan.Literal) bool {
	switch v := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return v.I8Val == 0
	case *plan.Literal_I16Val:
		return v.I16Val == 0
	case *plan.Literal_I32Val:
		return v.I32Val == 0
	case *plan.Literal_I64Val:
		return v.I64Val == 0
	case *plan.Literal_U8Val:
		return v.U8Val == 0
	case *plan.Literal_U16Val:
		return v.U16Val == 0
	case *plan.Literal_U32Val:
		return v.U32Val == 0
	case *plan.Literal_U64Val:
		return v.U64Val == 0
	case *plan.Literal_Fval:
		return v.Fval == 0
	case *plan.Literal_Dval:
		return v.Dval == 0
	case *plan.Literal_Decimal64Val:
		return v.Decimal64Val.A == 0
	case *plan.Literal_Decimal128Val:
		return v.Decimal128Val.A == 0 && v.Decimal128Val.B == 0
	}
	return false
}
