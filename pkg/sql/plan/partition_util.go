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
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// add this code in buildListPartitionItem
// return buildListPartitionItem(partitionBinder, partitionDef, defs)
func buildListPartitionItem(binder *PartitionBinder, partitionDef *plan.PartitionByDef, defs []*tree.Partition) error {
	for _, def := range defs {
		if partitionDef.PartitionColumns != nil {
			if err := checkListColumnsTypeAndValuesMatch(binder, partitionDef, def); err != nil {
				return err
			}
		} else {
			if err := checkListPartitionValuesIsInt(binder, def, partitionDef); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkPartitionExprAllowed Check whether the ast expression or sub ast expression of partition expression is used legally
func checkPartitionExprAllowed(ctx context.Context, tb *plan.TableDef, e tree.Expr) error {
	switch v := e.(type) {
	case *tree.FuncExpr:
		funcRef, ok := v.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return moerr.NewNYI(ctx, "invalid function expr '%v'", v)
		}
		funcName := strings.ToLower(funcRef.Parts[0])
		if _, ok := AllowedPartitionFuncMap[funcName]; ok {
			return nil
		}

	case *tree.BinaryExpr:
		if _, ok := AllowedPartitionBinaryOpMap[v.Op]; ok {
			return checkNoTimestampArgs(ctx, tb, v.Left, v.Right)
		}
	case *tree.UnaryExpr:
		if _, ok := AllowedPartitionUnaryOpMap[v.Op]; ok {
			return checkNoTimestampArgs(ctx, tb, v.Expr)
		}
	case *tree.ParenExpr, *tree.NumVal, *tree.UnresolvedName, *tree.MaxValue:
		return nil
	}
	return moerr.NewPartitionFunctionIsNotAllowed(ctx)
}

// checkPartitionExprArgs Check whether the parameters of the partition function are allowed
// see link: https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations-functions.html
func checkPartitionExprArgs(ctx context.Context, tblInfo *plan.TableDef, e tree.Expr) error {
	expr, ok := e.(*tree.FuncExpr)
	if !ok {
		return nil
	}

	funcRef, ok := expr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return moerr.NewNYI(ctx, "invalid function expr '%v'", expr)
	}
	funcName := strings.ToLower(funcRef.Parts[0])

	argsType, err := collectArgsType(ctx, tblInfo, expr.Exprs...)
	if err != nil {
		return err
	}

	switch funcName {
	case "to_days", "to_seconds", "dayofmonth", "month", "dayofyear", "quarter", "yearweek",
		"year", "weekday", "dayofweek", "day":
		return checkResultOK(ctx, hasDateArgs(argsType...))
	case "hour", "minute", "second", "time_to_sec", "microsecond":
		return checkResultOK(ctx, hasTimeArgs(argsType...))
	case "unix_timestamp":
		return checkResultOK(ctx, hasTimestampArgs(argsType...))
	case "from_days":
		return checkResultOK(ctx, hasDateArgs(argsType...) || hasTimeArgs(argsType...))
	case "extract":
		// see link: https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals
		switch strings.ToUpper(expr.Exprs[0].String()) {
		case INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_QUARTER, INTERVAL_MONTH, INTERVAL_DAY:
			return checkResultOK(ctx, hasDateArgs(argsType...))
		case INTERVAL_DAY_MICROSECOND, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND:
			return checkResultOK(ctx, hasDatetimeArgs(argsType...))
		case INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE,
			INTERVAL_MINUTE_SECOND, INTERVAL_SECOND, INTERVAL_MICROSECOND, INTERVAL_HOUR_MICROSECOND,
			INTERVAL_MINUTE_MICROSECOND, INTERVAL_SECOND_MICROSECOND:
			return checkResultOK(ctx, hasTimeArgs(argsType...))
		default:
			// EXTRACT() function with WEEK specifier. The value returned by the EXTRACT() function,
			// when used as EXTRACT(WEEK FROM col), depends on the value of the default_week_format system variable.
			// For this reason, EXTRACT() is not permitted as a partitioning function when it specifies the unit as WEEK.
			return moerr.NewWrongExprInPartitionFunc(ctx)
		}
	case "datediff":
		return checkResultOK(ctx, hasDateArgs(argsType...))

	case "abs", "ceiling", "floor", "mod":
		has := hasTimestampArgs(argsType...)
		if has {
			return moerr.NewWrongExprInPartitionFunc(ctx)
		}
	}
	return nil
}

// Collect the types of columns which used as the partition function parameter
func collectArgsType(ctx context.Context, tblInfo *plan.TableDef, exprs ...tree.Expr) ([]int32, error) {
	types := make([]int32, 0, len(exprs))
	for _, arg := range exprs {
		col, ok := arg.(*tree.UnresolvedName)
		if !ok {
			continue
		}

		// Check whether column name exist in the table
		column := findColumnByName(col.Parts[0], tblInfo)
		if column == nil {
			return nil, moerr.NewBadFieldError(ctx, col.Parts[0], "partition function")
		}
		types = append(types, column.GetTyp().GetId())
	}
	return types, nil
}

// hasDateArgs Check if the arguments contains date or datetime type
func hasDateArgs(argsType ...int32) bool {
	for _, typeId := range argsType {
		if typeId == int32(types.T_date) || typeId == int32(types.T_datetime) {
			return true
		}
	}
	return false
}

// hasTimeArgs Check if the arguments contains time or datetime type
func hasTimeArgs(argsType ...int32) bool {
	for _, typeId := range argsType {
		return typeId == int32(types.T_time) || typeId == int32(types.T_datetime)
	}
	return false
}

// hasTimestampArgs Check if the arguments contains timestamp(time zone) type
func hasTimestampArgs(argsType ...int32) bool {
	for _, typeId := range argsType {
		return typeId == int32(types.T_timestamp)
	}
	return false
}

// hasTimestampArgs Check if the arguments contains datetime type
func hasDatetimeArgs(argsType ...int32) bool {
	for _, typeId := range argsType {
		return typeId == int32(types.T_datetime)
	}
	return false

}

// checkNoTimestampArgs Check to confirm that there are no timestamp type arguments in the partition expression
func checkNoTimestampArgs(ctx context.Context, tbInfo *plan.TableDef, exprs ...tree.Expr) error {
	argsType, err := collectArgsType(ctx, tbInfo, exprs...)
	if err != nil {
		return err
	}
	if hasTimestampArgs(argsType...) {
		return moerr.NewWrongExprInPartitionFunc(ctx)
	}
	return nil
}

// checkResultOK For partition table in mysql, Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed
func checkResultOK(ctx context.Context, ok bool) error {
	if !ok {
		return moerr.NewWrongExprInPartitionFunc(ctx)
	}
	return nil
}

func checkListColumnsTypeAndValuesMatch(binder *PartitionBinder, partitionDef *plan.PartitionByDef, partition *tree.Partition) error {
	if valuesIn, ok := partition.Values.(*tree.ValuesIn); ok {
		exprs := valuesIn.ValueList

		// Validate() has already checked len(colNames) = len(exprs)
		// create table ... partition by range columns (cols)
		// partition p0 values less than (expr)
		// check the type of cols[i] and expr is consistent.
		colTypes := collectColumnsType(partitionDef)
		for _, colExpr := range exprs {
			val, err := binder.BindExpr(colExpr, 0, true)
			if err != nil {
				return err
			}

			switch tuple := val.Expr.(type) {
			case *plan.Expr_List:
				if len(colTypes) != len(tuple.List.List) {
					//return moerr.NewInternalError(binder.GetContext(), "Inconsistency in usage of column lists for partitioning")
					return moerr.NewPartitionColumnList(binder.GetContext())
				}
				for i, elem := range tuple.List.List {
					switch elem.Expr.(type) {
					case *plan.Expr_C:
					case *plan.Expr_F:
					default:
						//return moerr.NewInternalError(binder.GetContext(), "This partition function is not allowed")
						return moerr.NewPartitionFunctionIsNotAllowed(binder.GetContext())
					}

					colType := colTypes[i]
					// Check val.ConvertTo(colType) doesn't work, so we need this case by case check.
					err = partitionValueTypeCheck(binder.GetContext(), colType, elem.Typ)
					if err != nil {
						return err
					}
				}
			case *plan.Expr_C, *plan.Expr_F:
				if len(colTypes) != 1 {
					//return moerr.NewInternalError(binder.GetContext(), "Inconsistency in usage of column lists for partitioning")
					return moerr.NewPartitionColumnList(binder.GetContext())
				} else {
					err = partitionValueTypeCheck(binder.GetContext(), colTypes[0], val.Typ)
					if err != nil {
						return err
					}
				}
			default:
				//return moerr.NewInternalError(binder.GetContext(), "This partition function is not allowed")
				return moerr.NewPartitionFunctionIsNotAllowed(binder.GetContext())
			}
		}
		return nil
	} else {
		return moerr.NewInternalError(binder.GetContext(), "list partition function is not values in expression")
	}
}

// check whether the types of partition functions and partition values match
func partitionValueTypeCheck(ctx context.Context, funcTyp *Type, valueTyp *Type) error {
	switch types.T(funcTyp.Id) {
	case types.T_date, types.T_datetime:
		switch types.T(valueTyp.Id) {
		case types.T_varchar, types.T_char:
		default:
			//return moerr.NewInternalError(ctx, "Partition column values of incorrect type")
			return moerr.NewWrongTypeColumnValue(ctx)
		}
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		switch types.T(valueTyp.Id) {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_any:
		default:
			//return moerr.NewInternalError(ctx, "Partition column values of incorrect type")
			return moerr.NewWrongTypeColumnValue(ctx)
		}
	case types.T_float32, types.T_float64:
		switch types.T(valueTyp.Id) {
		case types.T_float32, types.T_float64, types.T_any:
		default:
			//return moerr.NewInternalError(ctx, "Partition column values of incorrect type")
			return moerr.NewWrongTypeColumnValue(ctx)
		}
	case types.T_varchar, types.T_char:
		switch types.T(valueTyp.Id) {
		case types.T_varchar, types.T_char, types.T_any:
		default:
			//return moerr.NewInternalError(ctx, "Partition column values of incorrect type")
			return moerr.NewWrongTypeColumnValue(ctx)
		}
	}
	return nil
}

func checkListPartitionValuesIsInt(binder *PartitionBinder, partition *tree.Partition, info *plan.PartitionByDef) error {
	unsignedFlag := types.T(info.PartitionExpr.Expr.Typ.Id).IsUnsignedInt()
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

			compilerContext := binder.builder.compCtx
			evalExpr, err := EvalPlanExpr(binder.GetContext(), val, compilerContext.GetProcess())
			if err != nil {
				return err
			}

			cval, ok1 := evalExpr.Expr.(*plan.Expr_C)
			if !ok1 {
				//return moerr.NewInternalError(binder.GetContext(), "This partition function is not allowed")
				return moerr.NewPartitionFunctionIsNotAllowed(binder.GetContext())
			}

			switch types.T(evalExpr.Typ.Id) {
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_any:
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
				switch value := cval.C.Value.(type) {
				case *plan.Const_I8Val:
					if value.I8Val < 0 && unsignedFlag {
						//return moerr.NewInternalError(binder.GetContext(), "Partition constant is out of partition function domain")
						return moerr.NewPartitionConstDomain(binder.GetContext())
					}
				case *plan.Const_I16Val:
					if value.I16Val < 0 && unsignedFlag {
						//return moerr.NewInternalError(binder.GetContext(), "Partition constant is out of partition function domain")
						return moerr.NewPartitionConstDomain(binder.GetContext())
					}
				case *plan.Const_I32Val:
					if value.I32Val < 0 && unsignedFlag {
						//return moerr.NewInternalError(binder.GetContext(), "Partition constant is out of partition function domain")
						return moerr.NewPartitionConstDomain(binder.GetContext())
					}
				case *plan.Const_I64Val:
					if value.I64Val < 0 && unsignedFlag {
						//return moerr.NewInternalError(binder.GetContext(), "Partition constant is out of partition function domain")
						return moerr.NewPartitionConstDomain(binder.GetContext())
					}
				default:
					//return moerr.NewInternalError(binder.GetContext(), "VALUES value for partition '%-.64s' must have type INT", partition.Name)
					return moerr.NewValuesIsNotIntType(binder.GetContext(), partition.Name)
				}
			default:
				//return moerr.NewInternalError(binder.GetContext(), "VALUES value for partition '%-.64s' must have type INT", partition.Name)
				return moerr.NewValuesIsNotIntType(binder.GetContext(), partition.Name)
			}
		}
	}
	return nil
}

// add this code in buildRangePartitionDefinitions
// return buildRangePartitionDefinitionItem(partitionBinder, partitionDef, defs)
func buildRangePartitionItem(binder *PartitionBinder, partitionDef *plan.PartitionByDef, defs []*tree.Partition) error {
	for _, def := range defs {
		if partitionDef.PartitionColumns != nil && len(partitionDef.PartitionColumns.Columns) > 0 {
			if err := checkRangeColumnsTypeAndValuesMatch(binder, partitionDef, def); err != nil {
				return err
			}
		} else {
			if err := checkPartitionValuesIsInt(binder, def, partitionDef); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkRangeColumnsTypeAndValuesMatch(binder *PartitionBinder, partitionDef *plan.PartitionByDef, partition *tree.Partition) error {
	if valuesLessThan, ok := partition.Values.(*tree.ValuesLessThan); ok {
		exprs := valuesLessThan.ValueList
		// Validate() has already checked len(colNames) = len(exprs)
		// create table ... partition by range columns (cols)
		// partition p0 values less than (expr)
		// check the type of cols[i] and expr is consistent.
		colTypes := collectColumnsType(partitionDef)
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
				//return moerr.NewInternalError(binder.GetContext(), "This partition function is not allowed")
				return moerr.NewPartitionFunctionIsNotAllowed(binder.GetContext())
			}

			// Check val.ConvertTo(colType) doesn't work, so we need this case by case check.
			vkind := val.Typ
			switch types.T(colType.Id) {
			case types.T_date, types.T_datetime:
				switch types.T(vkind.Id) {
				case types.T_varchar, types.T_char:
				default:
					//return moerr.NewInternalError(binder.GetContext(), "Partition column values of incorrect type")
					return moerr.NewWrongTypeColumnValue(binder.GetContext())
				}
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
				switch types.T(vkind.Id) {
				case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64: //+types.T_null:
				default:
					//return moerr.NewInternalError(binder.GetContext(), "Partition column values of incorrect type")
					return moerr.NewWrongTypeColumnValue(binder.GetContext())
				}
			case types.T_float32, types.T_float64:
				switch types.T(vkind.Id) {
				case types.T_float32, types.T_float64: //+types.T_null:
				default:
					//return moerr.NewInternalError(binder.GetContext(), "Partition column values of incorrect type")
					return moerr.NewWrongTypeColumnValue(binder.GetContext())
				}
			case types.T_varchar, types.T_char:
				switch types.T(vkind.Id) {
				case types.T_varchar, types.T_char: //+types.T_null:
				default:
					//return moerr.NewInternalError(binder.GetContext(), "Partition column values of incorrect type")
					return moerr.NewWrongTypeColumnValue(binder.GetContext())
				}
			}
		}
		return nil
	} else {
		return moerr.NewInternalError(binder.GetContext(), "list partition function is not values in expression")
	}
}

func checkPartitionValuesIsInt(binder *PartitionBinder, partition *tree.Partition, info *plan.PartitionByDef) error {
	unsignedFlag := types.T(info.PartitionExpr.Expr.Typ.Id).IsUnsignedInt()
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

			compilerContext := binder.builder.compCtx
			evalExpr, err := EvalPlanExpr(binder.GetContext(), val, compilerContext.GetProcess())
			if err != nil {
				return err
			}

			cval, ok1 := evalExpr.Expr.(*plan.Expr_C)
			if !ok1 {
				//return moerr.NewInternalError(binder.GetContext(), "This partition function is not allowed")
				return moerr.NewPartitionFunctionIsNotAllowed(binder.GetContext())
			}

			switch types.T(evalExpr.Typ.Id) {
			case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_any:
			case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
				switch value := cval.C.Value.(type) {
				case *plan.Const_I8Val:
					if value.I8Val < 0 && unsignedFlag {
						//return moerr.NewInternalError(binder.GetContext(), "Partition constant is out of partition function domain")
						return moerr.NewPartitionConstDomain(binder.GetContext())
					}
				case *plan.Const_I16Val:
					if value.I16Val < 0 && unsignedFlag {
						//return moerr.NewInternalError(binder.GetContext(), "Partition constant is out of partition function domain")
						return moerr.NewPartitionConstDomain(binder.GetContext())
					}
				case *plan.Const_I32Val:
					if value.I32Val < 0 && unsignedFlag {
						//return moerr.NewInternalError(binder.GetContext(), "Partition constant is out of partition function domain")
						return moerr.NewPartitionConstDomain(binder.GetContext())
					}
				case *plan.Const_I64Val:
					if value.I64Val < 0 && unsignedFlag {
						//return moerr.NewInternalError(binder.GetContext(), "Partition constant is out of partition function domain")
						return moerr.NewPartitionConstDomain(binder.GetContext())
					}
				default:
					//return moerr.NewInternalError(binder.GetContext(), "VALUES value for partition '%-.64s' must have type INT", partition.Name)
					return moerr.NewValuesIsNotIntType(binder.GetContext(), partition.Name)
				}
			default:
				//return moerr.NewInternalError(binder.GetContext(), "VALUES value for partition '%-.64s' must have type INT", partition.Name)
				return moerr.NewValuesIsNotIntType(binder.GetContext(), partition.Name)
			}
		}
	}
	return nil
}

// checkPartitionByList checks validity of list partition.
func checkPartitionByList(partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, tableDef *TableDef) error {
	return checkListPartitionValue(partitionBinder, partitionDef, tableDef)
}

// check list expr ?
func checkListPartitionValue(partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, tableDef *TableDef) error {
	//pi := tblInfo.Partition
	ctx := partitionBinder.GetContext()
	if len(partitionDef.Partitions) == 0 {
		//return moerr.NewInternalError(ctx, "For %-.64s partitions each partition must be defined", "LIST")
		return moerr.NewPartitionsMustBeDefined(ctx, "LIST")
	}
	expStrs, err := formatListPartitionValue(partitionBinder, partitionDef, tableDef)
	if err != nil {
		return err
	}

	partitionsValuesMap := make(map[string]struct{})
	for _, str := range expStrs {
		if _, ok := partitionsValuesMap[str]; ok {
			//return moerr.NewInternalError(ctx, "Multiple definition of same constant in list partitioning")
			return moerr.NewMultipleDefConstInListPart(ctx)
		}
		partitionsValuesMap[str] = struct{}{}
	}
	return nil
}

// construct list expr ?
func formatListPartitionValue(binder *PartitionBinder, partitionDef *plan.PartitionByDef, tableDef *TableDef) ([]string, error) {
	pi := partitionDef
	defs := partitionDef.Partitions
	if pi.PartitionColumns != nil {
		for _, column := range pi.PartitionColumns.PartitionColumns {
			colInfo := findColumnByName(column, tableDef)
			if colInfo == nil {
				//return nil, moerr.NewInternalError(binder.GetContext(), "Field in list of fields for partition function not found in table")
				return nil, moerr.NewFieldNotFoundPart(binder.GetContext())
			}
		}
	}

	exprStrs := make([]string, 0)
	inValueStrs := make([]string, 0)
	for i := range defs {
		inValueStrs = inValueStrs[:0]
		for _, val := range defs[i].InValues {
			compilerContext := binder.builder.compCtx
			evalExpr, err := EvalPlanExpr(binder.GetContext(), val, compilerContext.GetProcess())
			if err != nil {
				return nil, err
			}

			cval, ok1 := evalExpr.Expr.(*plan.Expr_C)
			if !ok1 {
				//return nil, moerr.NewInternalError(binder.GetContext(), "This partition function is not allowed")
				return nil, moerr.NewPartitionFunctionIsNotAllowed(binder.GetContext())
			}
			s := cval.C.String()
			inValueStrs = append(inValueStrs, s)
		}
		exprStrs = append(exprStrs, inValueStrs...)
	}
	return exprStrs, nil
}

func collectColumnsType(partitionDef *plan.PartitionByDef) []*Type {
	if len(partitionDef.PartitionColumns.Columns) > 0 {
		colTypes := make([]*Type, 0, len(partitionDef.PartitionColumns.Columns))
		for _, col := range partitionDef.PartitionColumns.Columns {
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

func EvalPlanExpr(ctx context.Context, expr *plan.Expr, process *process.Process) (*plan.Expr, error) {
	switch expr.Expr.(type) {
	case *plan.Expr_C:
		return expr, nil
	default:
		// try to calculate default value, return err if fails
		newExpr, err := ConstantFold(batch.EmptyForConstFoldBatch, expr, process, false)
		if err != nil {
			return nil, err
		}
		if _, ok := newExpr.Expr.(*plan.Expr_C); ok {
			return newExpr, nil
		} else {
			//return nil, moerr.NewInternalError(ctx, "This partition function is not allowed")
			return nil, moerr.NewPartitionFunctionIsNotAllowed(ctx)
		}

	}
}

// AllowedPartitionFuncMap stores functions which can be used in the partition expression.
// See Link: https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations-functions.html
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

// AllowedPartitionBinaryOpMap store the operators of Binary operation expression
// link ref:https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations.html
var AllowedPartitionBinaryOpMap = map[tree.BinaryOp]string{
	tree.PLUS:        "+",
	tree.MINUS:       "-",
	tree.MULTI:       "*",
	tree.INTEGER_DIV: "div",
	tree.MOD:         "%",
}

// AllowedPartitionUnaryOpMap store the operators of Unary expression
// link ref:https://dev.mysql.com/doc/refman/8.0/en/partitioning-limitations.html
var AllowedPartitionUnaryOpMap = map[tree.UnaryOp]string{
	tree.UNARY_PLUS:  "+",
	tree.UNARY_MINUS: "-",
}

// The following code shows the expected form of the expr argument for each unit value.
// see link: https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals
const (
	// INTERVAL_MICROSECOND is the time or timestamp unit MICROSECOND.
	INTERVAL_MICROSECOND = "MICROSECOND"
	// INTERVAL_SECOND is the time or timestamp unit SECOND.
	INTERVAL_SECOND = "SECOND"
	// INTERVAL_MINUTE is the time or timestamp unit MINUTE.
	INTERVAL_MINUTE = "MINUTE"
	// INTERVAL_HOUR is the time or timestamp unit HOUR.
	INTERVAL_HOUR = "HOUR"
	// INTERVAL_DAY is the time or timestamp unit DAY.
	INTERVAL_DAY = "DAY"
	// INTERVAL_WEEK is the time or timestamp unit WEEK.
	INTERVAL_WEEK = "WEEK"
	// INTERVAL_MONTH is the time or timestamp unit MONTH.
	INTERVAL_MONTH = "MONTH"
	// INTERVAL_QUARTER is the time or timestamp unit QUARTER.
	INTERVAL_QUARTER = "QUARTER"
	// INTERVAL_YEAR is the time or timestamp unit YEAR.
	INTERVAL_YEAR = "YEAR"
	// INTERVAL_SECOND_MICROSECOND is the time unit SECOND_MICROSECOND.
	INTERVAL_SECOND_MICROSECOND = "SECOND_MICROSECOND"
	// INTERVAL_MINUTE_MICROSECOND is the time unit MINUTE_MICROSECOND.
	INTERVAL_MINUTE_MICROSECOND = "MINUTE_MICROSECOND"
	// INTERVAL_MINUTE_SECOND is the time unit MINUTE_SECOND.
	INTERVAL_MINUTE_SECOND = "MINUTE_SECOND"
	// INTERVAL_HOUR_MICROSECOND is the time unit HOUR_MICROSECOND.
	INTERVAL_HOUR_MICROSECOND = "HOUR_MICROSECOND"
	// INTERVAL_HOUR_SECOND is the time unit HOUR_SECOND.
	INTERVAL_HOUR_SECOND = "HOUR_SECOND"
	// INTERVAL_HOUR_MINUTE is the time unit HOUR_MINUTE.
	INTERVAL_HOUR_MINUTE = "HOUR_MINUTE"
	// INTERVAL_DAY_MICROSECOND is the time unit DAY_MICROSECOND.
	INTERVAL_DAY_MICROSECOND = "DAY_MICROSECOND"
	// INTERVAL_DAY_SECOND is the time unit DAY_SECOND.
	INTERVAL_DAY_SECOND = "DAY_SECOND"
	// INTERVAL_DAY_MINUTE is the time unit DAY_MINUTE.
	INTERVAL_DAY_MINUTE = "DAY_MINUTE"
	// INTERVAL_DAY_HOUR is the time unit DAY_HOUR.
	INTERVAL_DAY_HOUR = "DAY_HOUR"
	// INTERVAL_YEAR_MONTH is the time unit YEAR_MONTH.
	INTERVAL_YEAR_MONTH = "YEAR_MONTH"
)

// onlyHasHiddenPrimaryKey checks the primary key is hidden or not
func onlyHasHiddenPrimaryKey(tableDef *TableDef) bool {
	if tableDef == nil {
		return false
	}
	pk := tableDef.GetPkey()
	return pk != nil && pk.GetPkeyColName() == catalog.FakePrimaryKeyColName
}

// checkPartitionByRange Check the validity of each partition definition in range partition.
func checkPartitionByRange(partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, tbInfo *TableDef) error {
	if partitionDef.PartitionColumns != nil {
		return checkRangeColumnsPartitionValue(partitionBinder, partitionDef, tbInfo)
	}
	return checkRangePartitionValue(partitionBinder, partitionDef, tbInfo)
}

// checkRangePartitionValue check if the 'less than value' for each partition is strictly monotonically increasing.
func checkRangePartitionValue(partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, tbInfo *TableDef) error {
	ctx := partitionBinder.GetContext()

	partdefs := partitionDef.Partitions
	if len(partdefs) == 0 {
		return nil
	}

	if _, ok := partdefs[len(partdefs)-1].LessThan[0].Expr.(*plan.Expr_Max); ok {
		partdefs = partdefs[:len(partdefs)-1]
	}
	isUnsigned := types.T(partitionDef.PartitionExpr.Expr.Typ.Id).IsUnsignedInt()
	var prevRangeValue interface{}
	for i := 0; i < len(partdefs); i++ {
		if _, isMaxVal := partdefs[i].LessThan[0].Expr.(*plan.Expr_Max); isMaxVal {
			return moerr.NewErrPartitionMaxvalue(ctx)
		}
		currentRangeValue, err := getRangeValue(ctx, partitionDef, partdefs[i].LessThan[0], isUnsigned, partitionBinder.builder.compCtx.GetProcess())
		if err != nil {
			return err
		}

		if i == 0 {
			prevRangeValue = currentRangeValue
			continue
		}

		if isUnsigned {
			if currentRangeValue.(uint64) <= prevRangeValue.(uint64) {
				return moerr.NewErrRangeNotIncreasing(ctx)
			}
		} else {
			if currentRangeValue.(int64) <= prevRangeValue.(int64) {
				return moerr.NewErrRangeNotIncreasing(ctx)
			}
		}
		prevRangeValue = currentRangeValue
	}
	return nil
}

// checkRangeColumnsPartitionValue check whether "less than value" of each partition is strictly increased in Lexicographic order order.
func checkRangeColumnsPartitionValue(partitionBinder *PartitionBinder, partitionDef *plan.PartitionByDef, tbInfo *TableDef) error {
	ctx := partitionBinder.GetContext()
	partdefs := partitionDef.Partitions
	if len(partdefs) < 1 {
		return moerr.NewPartitionsMustBeDefined(ctx, "RANGE")
	}

	curr := partdefs[0]
	if len(curr.LessThan) != len(partitionDef.PartitionColumns.PartitionColumns) {
		return moerr.NewPartitionColumnList(ctx)
	}

	var prev *plan.PartitionItem
	for i := 1; i < len(partdefs); i++ {
		prev, curr = curr, partdefs[i]
		res, err := compareTwoRangeColumns(ctx, curr, prev, partitionDef, tbInfo, partitionBinder)
		if err != nil {
			return err
		}
		if !res {
			return moerr.NewErrRangeNotIncreasing(ctx)
		}
	}
	return nil
}

// getRangeValue gets an integer value from range value expression
// The second returned boolean value indicates whether the input string is a constant expression.
func getRangeValue(ctx context.Context, partitionDef *plan.PartitionByDef, expr *Expr, unsigned bool, process *process.Process) (interface{}, error) {
	// Unsigned integer was converted to uint64
	if unsigned {
		if cExpr, ok := expr.Expr.(*plan.Expr_C); ok {
			return getIntConstVal[uint64](cExpr), nil
		}
		evalExpr, err := EvalPlanExpr(ctx, expr, process)
		if err != nil {
			return 0, err
		}
		cVal, ok := evalExpr.Expr.(*plan.Expr_C)
		if ok {
			return getIntConstVal[uint64](cVal), nil
		}
	} else {
		// signed integer was converted to int64
		if cExpr, ok := expr.Expr.(*plan.Expr_C); ok {
			return getIntConstVal[int64](cExpr), nil
		}

		// The range partition `less than value` may be not an integer, it could be a constant expression.
		evalExpr, err := EvalPlanExpr(ctx, expr, process)
		if err != nil {
			return 0, err
		}
		cVal, ok := evalExpr.Expr.(*plan.Expr_C)
		if ok {
			return getIntConstVal[int64](cVal), nil
		}
	}
	return 0, moerr.NewFieldTypeNotAllowedAsPartitionField(ctx, partitionDef.PartitionExpr.ExprStr)
}

// compareTwoRangeColumns Check whether the two range columns partition definition values increase in Lexicographic order order
func compareTwoRangeColumns(ctx context.Context, curr, prev *plan.PartitionItem, partitionDef *plan.PartitionByDef, tbldef *TableDef, binder *PartitionBinder) (bool, error) {
	if len(curr.LessThan) != len(partitionDef.PartitionColumns.PartitionColumns) {
		return false, moerr.NewPartitionColumnList(ctx)
	}

	for i := 0; i < len(partitionDef.PartitionColumns.Columns); i++ {
		// handling `MAXVALUE` in partition less than value
		_, ok1 := curr.LessThan[i].Expr.(*plan.Expr_Max)
		_, ok2 := prev.LessThan[i].Expr.(*plan.Expr_Max)
		if ok1 && !ok2 {
			// If current is maxvalue, then it must be greater than previous, so previous cannot be maxvalue
			return true, nil
		}

		if ok2 {
			// Current is not maxvalue, and  the previous cannot be maxvalue
			return false, nil
		}

		// The range columns tuples values are strictly increasing in dictionary order
		colInfo := findColumnByName(partitionDef.PartitionColumns.PartitionColumns[i], tbldef)
		res, err := evalPartitionBoolExpr(ctx, curr.LessThan[i], prev.LessThan[i], colInfo, binder)
		if err != nil {
			return false, err
		}

		if res {
			return true, nil
		}
	}
	return false, nil
}

// evalPartitionBoolExpr Calculate the bool result of comparing the values of two range partition `less than value`
func evalPartitionBoolExpr(ctx context.Context, lOriExpr *Expr, rOriExpr *Expr, colInfo *plan.ColDef, binder *PartitionBinder) (bool, error) {
	lexpr, err := makePlan2CastExpr(ctx, lOriExpr, colInfo.Typ)
	if err != nil {
		return false, err
	}

	rexpr, err := makePlan2CastExpr(ctx, rOriExpr, colInfo.Typ)
	if err != nil {
		return false, err
	}

	retExpr, err := bindFuncExprImplByPlanExpr(ctx, ">", []*Expr{lexpr, rexpr})
	if err != nil {
		return false, err
	}

	vec, err := colexec.EvalExpressionOnce(binder.builder.compCtx.GetProcess(), retExpr, []*batch.Batch{batch.EmptyForConstFoldBatch, batch.EmptyForConstFoldBatch})
	if err != nil {
		return false, err
	}
	fixedCol := vector.MustFixedCol[bool](vec)
	return fixedCol[0], nil
}

// getIntConstVal Get an integer constant value, the `cExpr` must be integral constant expression
func getIntConstVal[T uint64 | int64](cExpr *plan.Expr_C) T {
	switch value := cExpr.C.Value.(type) {
	case *plan.Const_U8Val:
		return T(value.U8Val)
	case *plan.Const_U16Val:
		return T(value.U16Val)
	case *plan.Const_U32Val:
		return T(value.U32Val)
	case *plan.Const_U64Val:
		return T(value.U64Val)
	case *plan.Const_I8Val:
		return T(value.I8Val)
	case *plan.Const_I16Val:
		return T(value.I16Val)
	case *plan.Const_I32Val:
		return T(value.I32Val)
	case *plan.Const_I64Val:
		return T(value.I64Val)
	default:
		panic("the `expr` must be integral constant expression")
	}
}
