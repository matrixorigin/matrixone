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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strings"
)

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

func hasDateArgs(argsType ...int32) bool {
	for _, typeId := range argsType {
		if typeId == int32(types.T_date) || typeId == int32(types.T_datetime) {
			return true
		}
	}
	return false
}

func hasTimeArgs(argsType ...int32) bool {
	for _, typeId := range argsType {
		return typeId == int32(types.T_time) || typeId == int32(types.T_datetime)
	}
	return false
}

func hasTimestampArgs(argsType ...int32) bool {
	for _, typeId := range argsType {
		return typeId == int32(types.T_timestamp)
	}
	return false
}

func hasDatetimeArgs(argsType ...int32) bool {
	for _, typeId := range argsType {
		return typeId == int32(types.T_datetime)
	}
	return false

}

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

// checkResultOK for partition table in mysql, Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed
func checkResultOK(ctx context.Context, ok bool) error {
	if !ok {
		return moerr.NewWrongExprInPartitionFunc(ctx)
	}
	return nil
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
