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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
		bat := batch.NewWithSize(0)
		bat.Zs = []int64{1}
		newExpr, err := ConstantFold(bat, expr, process)
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

// onlyHasHiddenPrimaryKey checks the primary key is hidden or not
func onlyHasHiddenPrimaryKey(tableDef *TableDef) bool {
	if tableDef == nil {
		return false
	}
	pk := tableDef.GetPkey()
	return pk != nil && pk.GetPkeyColName() == catalog.FakePrimaryKeyColName
}
