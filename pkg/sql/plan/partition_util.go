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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// add this code in buildListPartitionItem
// return buildListPartitionItem(partitionBinder, partitionInfo, defs)
func buildListPartitionItem(binder *PartitionBinder, partitionInfo *plan.PartitionInfo, defs []*tree.Partition) error {
	for _, def := range defs {
		if len(partitionInfo.Columns) > 0 {
			if err := checkListColumnsTypeAndValuesMatch(binder, partitionInfo, def); err != nil {
				return err
			}
		} else {
			if err := checkListPartitionValuesIsInt(binder, def, partitionInfo); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkListColumnsTypeAndValuesMatch(binder *PartitionBinder, partitionInfo *plan.PartitionInfo, partition *tree.Partition) error {
	if valuesIn, ok := partition.Values.(*tree.ValuesIn); ok {
		exprs := valuesIn.ValueList

		// Validate() has already checked len(colNames) = len(exprs)
		// create table ... partition by range columns (cols)
		// partition p0 values less than (expr)
		// check the type of cols[i] and expr is consistent.
		colTypes := collectColumnsType(partitionInfo)
		for _, colExpr := range exprs {
			val, err := binder.BindExpr(colExpr, 0, true)
			if err != nil {
				return err
			}

			switch tuple := val.Expr.(type) {
			case *plan.Expr_List:
				if len(colTypes) != len(tuple.List.List) {
					return moerr.NewInternalError("Inconsistency in usage of column lists for partitioning")
				}
				for i, elem := range tuple.List.List {
					switch elem.Expr.(type) {
					case *plan.Expr_C:
					case *plan.Expr_F:
					default:
						return moerr.NewInternalError("This partition function is not allowed")
					}

					colType := colTypes[i]
					// Check val.ConvertTo(colType) doesn't work, so we need this case by case check.
					err = partitionValueTypeCheck(colType, elem.Typ)
					if err != nil {
						return err
					}
				}
			case *plan.Expr_C, *plan.Expr_F:
				if len(colTypes) != 1 {
					return moerr.NewInternalError("Inconsistency in usage of column lists for partitioning")
				} else {
					err = partitionValueTypeCheck(colTypes[0], val.Typ)
					if err != nil {
						return err
					}
				}
			default:
				return moerr.NewInternalError("This partition function is not allowed")
			}
		}
		return nil
	} else {
		return moerr.NewInternalError("list partition function is not values in expression")
	}
}

// check whether the types of partition functions and partition values match
func partitionValueTypeCheck(funcTyp *Type, valueTyp *Type) error {
	switch types.T(funcTyp.Id) {
	case types.T_date, types.T_datetime:
		switch types.T(valueTyp.Id) {
		case types.T_varchar, types.T_char:
		default:
			return moerr.NewInternalError("Partition column values of incorrect type")
		}
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		switch types.T(valueTyp.Id) {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_any:
		default:
			return moerr.NewInternalError("Partition column values of incorrect type")
		}
	case types.T_float32, types.T_float64:
		switch types.T(valueTyp.Id) {
		case types.T_float32, types.T_float64, types.T_any:
		default:
			return moerr.NewInternalError("Partition column values of incorrect type")
		}
	case types.T_varchar, types.T_char:
		switch types.T(valueTyp.Id) {
		case types.T_varchar, types.T_char, types.T_any:
		default:
			return moerr.NewInternalError("Partition column values of incorrect type")
		}
	}
	return nil
}

func checkListPartitionValuesIsInt(binder *PartitionBinder, partition *tree.Partition, info *plan.PartitionInfo) error {
	unsignedFlag := types.IsUnsignedInt(types.T(info.Expr.Typ.Id))
	if valuesIn, ok := partition.Values.(*tree.ValuesIn); ok {
		exprs := valuesIn.ValueList
		for _, exp := range exprs {
			if _, ok := exp.(*tree.MaxValue); ok {
				continue
			}
			val, err := binder.BindExpr(exp, 0, true)
			if err != nil {
				return err
			}

			evalExpr, err := EvalPlanExpr(val)
			if err != nil {
				return err
			}

			cval, ok1 := evalExpr.Expr.(*plan.Expr_C)
			if !ok1 {
				return moerr.NewInternalError("This partition function is not allowed")
			}

			switch types.T(evalExpr.Typ.Id) {
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_any:
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
				switch value := cval.C.Value.(type) {
				case *plan.Const_I8Val:
					if value.I8Val < 0 && unsignedFlag {
						return moerr.NewInternalError("Partition constant is out of partition function domain")
					}
				case *plan.Const_I16Val:
					if value.I16Val < 0 && unsignedFlag {
						return moerr.NewInternalError("Partition constant is out of partition function domain")
					}
				case *plan.Const_I32Val:
					if value.I32Val < 0 && unsignedFlag {
						return moerr.NewInternalError("Partition constant is out of partition function domain")
					}
				case *plan.Const_I64Val:
					if value.I64Val < 0 && unsignedFlag {
						return moerr.NewInternalError("Partition constant is out of partition function domain")
					}
				default:
					return moerr.NewInternalError("VALUES value for partition '%-.64s' must have type INT", partition.Name)
				}
			default:
				return moerr.NewInternalError("VALUES value for partition '%-.64s' must have type INT", partition.Name)
			}
		}
	}
	return nil
}

// add this code in buildRangePartitionDefinitions
// return buildRangePartitionDefinitionItem(partitionBinder, partitionInfo, defs)
func buildRangePartitionItem(binder *PartitionBinder, partitionInfo *plan.PartitionInfo, defs []*tree.Partition) error {
	for _, def := range defs {
		if len(partitionInfo.Columns) > 0 {
			if err := checkRangeColumnsTypeAndValuesMatch(binder, partitionInfo, def); err != nil {
				return err
			}
		} else {
			if err := checkPartitionValuesIsInt(binder, def, partitionInfo); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkRangeColumnsTypeAndValuesMatch(binder *PartitionBinder, partitionInfo *plan.PartitionInfo, partition *tree.Partition) error {
	if valuesLessThan, ok := partition.Values.(*tree.ValuesLessThan); ok {
		exprs := valuesLessThan.ValueList
		// Validate() has already checked len(colNames) = len(exprs)
		// create table ... partition by range columns (cols)
		// partition p0 values less than (expr)
		// check the type of cols[i] and expr is consistent.
		colTypes := collectColumnsType(partitionInfo)
		for i, colExpr := range exprs {
			if _, ok1 := colExpr.(*tree.MaxValue); ok1 {
				continue
			}
			colType := colTypes[i]
			val, err := binder.BindExpr(colExpr, 0, true)
			if err != nil {
				return err
			}
			switch val.Expr.(type) {
			case *plan.Expr_C, *plan.Expr_Max:
			case *plan.Expr_F:
			default:
				return moerr.NewInternalError("This partition function is not allowed")
			}

			// Check val.ConvertTo(colType) doesn't work, so we need this case by case check.
			vkind := val.Typ
			switch types.T(colType.Id) {
			case types.T_date, types.T_datetime:
				switch types.T(vkind.Id) {
				case types.T_varchar, types.T_char:
				default:
					return moerr.NewInternalError("Partition column values of incorrect type")
				}
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
				switch types.T(vkind.Id) {
				case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64: //+types.T_null:
				default:
					return moerr.NewInternalError("Partition column values of incorrect type")
				}
			case types.T_float32, types.T_float64:
				switch types.T(vkind.Id) {
				case types.T_float32, types.T_float64: //+types.T_null:
				default:
					return moerr.NewInternalError("Partition column values of incorrect type")
				}
			case types.T_varchar, types.T_char:
				switch types.T(vkind.Id) {
				case types.T_varchar, types.T_char: //+types.T_null:
				default:
					return moerr.NewInternalError("Partition column values of incorrect type")
				}
			}
		}
		return nil
	} else {
		return moerr.NewInternalError("list partition function is not values in expression")
	}
}

func checkPartitionValuesIsInt(binder *PartitionBinder, partition *tree.Partition, info *plan.PartitionInfo) error {
	unsignedFlag := types.IsUnsignedInt(types.T(info.Expr.Typ.Id))
	if valuesLess, ok := partition.Values.(*tree.ValuesLessThan); ok {
		exprs := valuesLess.ValueList
		for _, exp := range exprs {
			if _, ok := exp.(*tree.MaxValue); ok {
				continue
			}
			val, err := binder.BindExpr(exp, 0, true)
			if err != nil {
				return err
			}

			evalExpr, err := EvalPlanExpr(val)
			if err != nil {
				return err
			}

			cval, ok1 := evalExpr.Expr.(*plan.Expr_C)
			if !ok1 {
				return moerr.NewInternalError("This partition function is not allowed")
			}

			switch types.T(evalExpr.Typ.Id) {
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_any:
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
				switch value := cval.C.Value.(type) {
				case *plan.Const_I8Val:
					if value.I8Val < 0 && unsignedFlag {
						return moerr.NewInternalError("Partition constant is out of partition function domain")
					}
				case *plan.Const_I16Val:
					if value.I16Val < 0 && unsignedFlag {
						return moerr.NewInternalError("Partition constant is out of partition function domain")
					}
				case *plan.Const_I32Val:
					if value.I32Val < 0 && unsignedFlag {
						return moerr.NewInternalError("Partition constant is out of partition function domain")
					}
				case *plan.Const_I64Val:
					if value.I64Val < 0 && unsignedFlag {
						return moerr.NewInternalError("Partition constant is out of partition function domain")
					}
				default:
					return moerr.NewInternalError("VALUES value for partition '%-.64s' must have type INT", partition.Name)
				}
			default:
				return moerr.NewInternalError("VALUES value for partition '%-.64s' must have type INT", partition.Name)
			}
		}
	}
	return nil
}

func collectColumnsType(partitionInfo *plan.PartitionInfo) []*Type {
	if len(partitionInfo.Columns) > 0 {
		colTypes := make([]*Type, 0, len(partitionInfo.Columns))
		for _, col := range partitionInfo.Columns {
			colTypes = append(colTypes, col.Typ)
		}
		return colTypes
	}
	return nil
}

func findColumnByName(colName string, tbdef *TableDef) *ColDef {
	if tbdef == nil {
		return nil
	}
	for _, colDef := range tbdef.Cols {
		if colDef.Name == colName {
			return colDef
		}
	}
	return nil
}

//func checkPartitionExprAllowed(_ sessionctx.Context, tb *model.TableInfo, e ast.ExprNode) error {
//	switch v := e.(type) {
//	case *ast.FuncCallExpr:
//		if _, ok := expression.AllowedPartitionFuncMap[v.FnName.L]; ok {
//			return nil
//		}
//	case *ast.BinaryOperationExpr:
//		if _, ok := expression.AllowedPartition4BinaryOpMap[v.Op]; ok {
//			return errors.Trace(checkNoTimestampArgs(tb, v.L, v.R))
//		}
//	case *ast.UnaryOperationExpr:
//		if _, ok := expression.AllowedPartition4UnaryOpMap[v.Op]; ok {
//			return errors.Trace(checkNoTimestampArgs(tb, v.V))
//		}
//	case *ast.ColumnNameExpr, *ast.ParenthesesExpr, *driver.ValueExpr, *ast.MaxValueExpr,
//		*ast.TimeUnitExpr:
//		return nil
//	}
//	return errors.Trace(dbterror.ErrPartitionFunctionIsNotAllowed)
//}

/*
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
				Sval: vec.GetString(0),
			},
		}
	default:
		return nil
	}
}
*/

func EvalPlanExpr(expr *plan.Expr) (*plan.Expr, error) {
	switch expr.Expr.(type) {
	case *plan.Expr_C:
		return expr, nil
	default:
		// try to calculate default value, return err if fails
		bat := batch.NewWithSize(0)
		bat.Zs = []int64{1}
		newExpr, err := ConstantFold(bat, expr)
		if err != nil {
			return nil, err
		}
		if _, ok := newExpr.Expr.(*plan.Expr_C); ok {
			return newExpr, nil
		} else {
			return nil, moerr.NewInternalError("This partition function is not allowed")
		}

	}
}

// checkPartitionFuncValid checks partition function validly.
//func checkPartitionFuncValid(ctx sessionctx.Context, tblInfo *model.TableInfo, expr ast.ExprNode) error {
//	if expr == nil {
//		return nil
//	}
//	exprChecker := newPartitionExprChecker(ctx, tblInfo, checkPartitionExprArgs, checkPartitionExprAllowed)
//	expr.Accept(exprChecker)
//	if exprChecker.err != nil {
//		return errors.Trace(exprChecker.err)
//	}
//	if len(exprChecker.columns) == 0 {
//		return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
//	}
//	return nil
//}
//
//func checkPartitionFuncValid(tableDef *TableDef, partby tree.PartitionBy) error {
//	if partby.PType == nil {
//		return nil
//	}
//}

func checkPartitionExprAllowed(tb *TableDef, e tree.Expr) error {
	switch v := e.(type) {
	case *tree.FuncExpr:
		funcRef, ok := v.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return moerr.NewNYI("function expr '%v'", v)
		}
		funcName := funcRef.Parts[0]
		if _, ok = AllowedPartitionFuncMap[funcName]; ok {
			return nil
		}
	case *tree.BinaryExpr:
		if _, ok := AllowedPartition4BinaryOpMap[v.Op]; ok {
			return nil
		}
	case *tree.UnaryExpr:
		if _, ok := AllowedPartition4UnaryOpMap[v.Op]; ok {
			return nil
		}
	case *tree.ParenExpr, *tree.MaxValue, *tree.UnresolvedName:
		return nil
	}
	return moerr.NewInternalError("This partition function is not allowed")
}

// AllowedPartitionFuncMap stores functions which can be used in the partition expression.
var AllowedPartitionFuncMap = map[string]int{
	"to_days":        1,
	"to_seconds":     1,
	"dayofmonth":     1,
	"month":          1,
	"dayofyear":      1,
	"quarter":        1,
	"yearweek":       1,
	"year":           1,
	"weekday":        1,
	"dayofweek":      1,
	"day":            1,
	"hour":           1,
	"minute":         1,
	"second":         1,
	"time_to_sec":    1,
	"microsecond":    1,
	"unix_timestamp": 1,
	"from_days":      1,
	"extract":        1,
	"abs":            1,
	"ceiling":        1,
	"ceil":           1,
	"datediff":       1,
	"floor":          1,
	"mod":            1,
}

// AllowedPartition4BinaryOpMap store the operator for Binary Expr
// link ref:https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations.html
var AllowedPartition4BinaryOpMap = map[tree.BinaryOp]string{
	tree.PLUS:        "+",
	tree.MINUS:       "-",
	tree.MULTI:       "*",
	tree.INTEGER_DIV: "div",
	tree.MOD:         "%",
}

// AllowedPartition4UnaryOpMap store the operator for Unary Expr
var AllowedPartition4UnaryOpMap = map[tree.UnaryOp]string{
	tree.UNARY_PLUS:  "+",
	tree.UNARY_MINUS: "-",
}

/*
func checkNoTimestampArgs(tbInfo *TableDef, exprs ...ast.ExprNode) error {
	argsType, err := collectArgsType(tbInfo, exprs...)
	if err != nil {
		return err
	}
	if hasTimestampArgs(argsType...) {
		return errors.Trace(dbterror.ErrWrongExprInPartitionFunc)
	}
	return nil
}

func collectArgsType(tblInfo *TableDef, exprs ...tree.Expr) ([]byte, error) {
	ts := make([]byte, 0, len(exprs))
	for _, arg := range exprs {
		col, ok := arg.(*ast.ColumnNameExpr)
		if !ok {
			continue
		}
		columnInfo := findColumnByName(col.Name.Name.L, tblInfo)
		if columnInfo == nil {
			return nil, errors.Trace(dbterror.ErrBadField.GenWithStackByArgs(col.Name.Name.L, "partition function"))
		}
		ts = append(ts, columnInfo.GetType())
	}

	return ts, nil
}

func hasDateArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDate || argsType[i] == mysql.TypeDatetime
	})
}

func hasTimeArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDuration || argsType[i] == mysql.TypeDatetime
	})
}

func hasTimestampArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeTimestamp
	})
}

func hasDatetimeArgs(argsType ...byte) bool {
	return slice.AnyOf(argsType, func(i int) bool {
		return argsType[i] == mysql.TypeDatetime
	})
}
*/
