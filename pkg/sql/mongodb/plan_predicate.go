// Copyright 2026 Matrix Origin
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

package mongodb

import (
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// PushdownPlanFilters translates the conservative, driver-neutral subset of
// scan filters. Callers must retain every original expression as an MO
// residual. In strict conversion mode a source value with an unexpected BSON
// type must still reach the converter and fail, so comparison pushdown is
// disabled; try_null mappings are eligible for IS NOT NULL, and try_null
// numeric mappings are eligible for comparisons, because malformed values
// become SQL NULL and cannot satisfy either residual. String/collation
// predicates and IS NULL remain residual-only until their full candidate-set
// semantics are proven.
func PushdownPlanFilters(ctx context.Context, filters []*plan.Expr, columns []*plan.MongoColumnMapping) (*plan.MongoPredicate, string) {
	children := make([]*Predicate, 0, len(filters))
	for _, filter := range filters {
		if predicate, ok := planExprToPredicate(filter, columns); ok {
			children = append(children, predicate)
		}
	}
	if len(children) == 0 {
		return nil, residualDigest(filters)
	}
	var predicate *Predicate
	if len(children) == 1 {
		predicate = children[0]
	} else {
		predicate = &Predicate{Op: PredicateAnd, Children: children}
	}
	converted, err := PredicateToPlan(ctx, predicate)
	if err != nil {
		return nil, residualDigest(filters)
	}
	return converted, residualDigest(filters)
}

func planExprToPredicate(expr *plan.Expr, columns []*plan.MongoColumnMapping) (*Predicate, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return nil, false
	}
	name := strings.ToLower(fn.Func.ObjName)
	if name == "and" && len(fn.Args) == 2 {
		left, leftOK := planExprToPredicate(fn.Args[0], columns)
		right, rightOK := planExprToPredicate(fn.Args[1], columns)
		if !leftOK || !rightOK {
			return nil, false
		}
		return &Predicate{Op: PredicateAnd, Children: []*Predicate{left, right}}, true
	}
	if (name == "is_null" || name == "isnull" || name == "is_not_null" || name == "isnotnull") && len(fn.Args) == 1 {
		column, ok := mappedColumn(fn.Args[0], columns)
		if !ok {
			return nil, false
		}
		if name == "is_null" || name == "isnull" {
			// {field:null} excludes present-but-malformed values. That would
			// hide a strict conversion error and is also a false negative for
			// try_null, where malformed becomes SQL NULL.
			return nil, false
		}
		if !strings.EqualFold(column.ConversionMode, ConversionTryNull) {
			// A strict dotted mapping must inspect malformed intermediate
			// values. MongoDB's $exists/$ne candidate would discard scalar or
			// null parents before conversion and silently hide that error.
			return nil, false
		}
		return &Predicate{Op: PredicateIsNotNull, Path: column.Path}, true
	}
	if name == "in" && len(fn.Args) == 2 {
		column, ok := mappedColumn(fn.Args[0], columns)
		if !ok || !eligibleComparisonColumn(column) {
			return nil, false
		}
		list := fn.Args[1].GetList()
		if list == nil || len(list.List) == 0 {
			return nil, false
		}
		values := make([]any, 0, len(list.List))
		for _, item := range list.List {
			value, ok := comparisonPlanLiteral(column, item, PredicateEqual)
			if !ok || value == nil {
				return nil, false
			}
			values = append(values, value)
		}
		return &Predicate{Op: PredicateIn, Path: column.Path, Values: values}, true
	}
	if len(fn.Args) != 2 {
		return nil, false
	}
	op, ok := comparisonPredicateOp(name)
	if !ok {
		return nil, false
	}
	column, columnOK := mappedColumn(fn.Args[0], columns)
	value, valueOK := comparisonPlanLiteral(column, fn.Args[1], op)
	if !columnOK || !valueOK {
		column, columnOK = mappedColumn(fn.Args[1], columns)
		if columnOK {
			op = reversePredicateOp(op)
			value, valueOK = comparisonPlanLiteral(column, fn.Args[0], op)
		}
	}
	if !columnOK || !valueOK || value == nil || !eligibleComparisonColumn(column) {
		return nil, false
	}
	return &Predicate{Op: op, Path: column.Path, Value: value}, true
}

func eligibleComparisonColumn(column *plan.MongoColumnMapping) bool {
	if column == nil || !strings.EqualFold(column.ConversionMode, ConversionTryNull) {
		return false
	}
	switch types.T(column.MoType.Id) {
	case types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_datetime, types.T_timestamp:
		return true
	default:
		// BSON int64/double values can collapse to the same FLOAT after
		// float32 narrowing or float64's >2^53 rounding. Pushing the SQL
		// comparison against the pre-conversion BSON value can then remove a
		// row that the residual comparison would accept. Keep floating-point
		// mappings residual-only until an outward-rounded candidate predicate
		// is implemented and differentially proven.
		return false
	}
}

func comparisonPlanLiteral(column *plan.MongoColumnMapping, expr *plan.Expr, op PredicateOp) (any, bool) {
	if column == nil || expr == nil {
		return nil, false
	}
	switch types.T(column.MoType.Id) {
	case types.T_datetime, types.T_timestamp:
		microseconds, ok := temporalLiteralUnixMicroseconds(expr)
		if !ok {
			return nil, false
		}
		milliseconds, ok := temporalCandidateMilliseconds(microseconds, op)
		if !ok {
			return nil, false
		}
		return bson.DateTime(milliseconds), true
	default:
		return scalarPlanLiteral(expr)
	}
}

func temporalLiteralUnixMicroseconds(expr *plan.Expr) (int64, bool) {
	literal := expr.GetLit()
	if literal != nil && !literal.Isnull {
		if value, ok := literal.GetValue().(*plan.Literal_I64Val); ok {
			switch types.T(expr.Typ.Id) {
			case types.T_datetime:
				datetime := types.Datetime(value.I64Val)
				if datetime == types.ZeroDatetime {
					return 0, false
				}
				return datetime.ConvertToGoTime(time.UTC).UnixMicro(), true
			case types.T_timestamp:
				timestamp := types.Timestamp(value.I64Val)
				if timestamp == types.ZeroTimestamp {
					return 0, false
				}
				return timestamp.ToDatetime(time.UTC).ConvertToGoTime(time.UTC).UnixMicro(), true
			}
		}
	}

	// SQL datetime string literals are bound as CAST(varchar AS datetime), not
	// as an encoded I64 literal. DATETIME has no session-time-zone conversion,
	// so it is safe to fold this narrow shape while planning a BSON DateTime
	// candidate. TIMESTAMP casts remain residual-only because their meaning
	// depends on the session time zone, which this driver-neutral bridge does
	// not receive.
	fn := expr.GetF()
	if types.T(expr.Typ.Id) != types.T_datetime || fn == nil || fn.Func == nil ||
		!strings.EqualFold(fn.Func.ObjName, "cast") || len(fn.Args) == 0 {
		return 0, false
	}
	source := fn.Args[0].GetLit()
	if source == nil || source.Isnull {
		return 0, false
	}
	text, ok := source.GetValue().(*plan.Literal_Sval)
	if !ok {
		return 0, false
	}
	datetime, err := types.ParseDatetime(text.Sval, expr.Typ.Scale)
	if err != nil || datetime == types.ZeroDatetime {
		return 0, false
	}
	return datetime.ConvertToGoTime(time.UTC).UnixMicro(), true
}

func temporalCandidateMilliseconds(microseconds int64, op PredicateOp) (int64, bool) {
	const microsPerMilli = int64(1000)
	switch op {
	case PredicateEqual, PredicateNotEqual:
		if microseconds%microsPerMilli != 0 {
			// BSON DateTime has millisecond precision, so no source value can be
			// equal to this literal. Avoid manufacturing a narrower predicate;
			// the MO residual will decide the constant true/false outcome.
			return 0, false
		}
		return microseconds / microsPerMilli, true
	case PredicateGreaterEqual, PredicateLess:
		return ceilDiv(microseconds, microsPerMilli), true
	case PredicateGreater, PredicateLessEqual:
		return floorDiv(microseconds, microsPerMilli), true
	default:
		return 0, false
	}
}

func floorDiv(value, divisor int64) int64 {
	quotient, remainder := value/divisor, value%divisor
	if remainder < 0 {
		quotient--
	}
	return quotient
}

func ceilDiv(value, divisor int64) int64 {
	quotient, remainder := value/divisor, value%divisor
	if remainder > 0 {
		quotient++
	}
	return quotient
}

func mappedColumn(expr *plan.Expr, columns []*plan.MongoColumnMapping) (*plan.MongoColumnMapping, bool) {
	ref := expr.GetCol()
	if ref == nil || ref.ColPos < 0 || int(ref.ColPos) >= len(columns) || columns[ref.ColPos] == nil {
		return nil, false
	}
	return columns[ref.ColPos], true
}

func comparisonPredicateOp(name string) (PredicateOp, bool) {
	switch name {
	case "=":
		return PredicateEqual, true
	case "!=", "<>":
		return PredicateNotEqual, true
	case "<":
		return PredicateLess, true
	case "<=":
		return PredicateLessEqual, true
	case ">":
		return PredicateGreater, true
	case ">=":
		return PredicateGreaterEqual, true
	default:
		return PredicateInvalid, false
	}
}

func reversePredicateOp(op PredicateOp) PredicateOp {
	switch op {
	case PredicateLess:
		return PredicateGreater
	case PredicateLessEqual:
		return PredicateGreaterEqual
	case PredicateGreater:
		return PredicateLess
	case PredicateGreaterEqual:
		return PredicateLessEqual
	default:
		return op
	}
}

func scalarPlanLiteral(expr *plan.Expr) (any, bool) {
	literal := expr.GetLit()
	if literal == nil {
		return nil, false
	}
	if literal.Isnull {
		return nil, true
	}
	switch value := literal.Value.(type) {
	case *plan.Literal_Bval:
		return value.Bval, true
	case *plan.Literal_I8Val:
		return int32(value.I8Val), true
	case *plan.Literal_I16Val:
		return int32(value.I16Val), true
	case *plan.Literal_I32Val:
		return value.I32Val, true
	case *plan.Literal_I64Val:
		return value.I64Val, true
	case *plan.Literal_U8Val:
		return int32(value.U8Val), true
	case *plan.Literal_U16Val:
		return int32(value.U16Val), true
	case *plan.Literal_U32Val:
		return int64(value.U32Val), true
	case *plan.Literal_U64Val:
		if value.U64Val > uint64(^uint64(0)>>1) {
			return nil, false
		}
		return int64(value.U64Val), true
	case *plan.Literal_Fval:
		return value.Fval, true
	case *plan.Literal_Dval:
		return value.Dval, true
	case *plan.Literal_Sval:
		return value.Sval, true
	default:
		return nil, false
	}
}

func residualDigest(filters []*plan.Expr) string {
	if len(filters) == 0 {
		return ""
	}
	// Deliberately expose shape only. Expression values and source endpoints
	// must not appear in EXPLAIN, logs, or traces.
	return "mo-residual:" + strings.Repeat("f", len(filters))
}
