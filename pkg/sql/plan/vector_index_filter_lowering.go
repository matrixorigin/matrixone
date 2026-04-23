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

package plan

import (
	"encoding/json"
	"strings"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type hnswFilterPayload struct {
	Version    int               `json:"version"`
	Conj       string            `json:"conj"`
	Exprs      []*hnswFilterExpr `json:"exprs,omitempty"`
	ColumnMode string            `json:"column_mode"`
}

type hnswFilterExpr struct {
	Kind   string            `json:"kind"`
	Op     string            `json:"op,omitempty"`
	Column string            `json:"column,omitempty"`
	Value  any               `json:"value,omitempty"`
	Args   []*hnswFilterExpr `json:"args,omitempty"`
}

type cagraFilterPredicate struct {
	Column string `json:"col"`
	Op     string `json:"op"`
	Val    any    `json:"val,omitempty"`
	Vals   []any  `json:"vals,omitempty"`
	Lo     any    `json:"lo,omitempty"`
	Hi     any    `json:"hi,omitempty"`
}

// lowerFiltersToHnswPayload defines HNSW's filter payload boundary. It keeps
// the backend payload as a named expression tree, never as planner binding
// coordinates. Runtime integration can consume this payload without exposing
// RelPos/ColPos outside planner lowering.
func lowerFiltersToHnswPayload(filters []*planpb.Expr, scanNode *planpb.Node, partPos int32) (string, []*planpb.Expr, []*planpb.Expr, error) {
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return "", nil, filters, nil
	}

	scanTag := scanNode.BindingTags[0]
	payload := hnswFilterPayload{
		Version:    1,
		Conj:       "and",
		ColumnMode: "logical_name",
		Exprs:      make([]*hnswFilterExpr, 0, len(filters)),
	}
	pushdown := make([]*planpb.Expr, 0, len(filters))
	remaining := make([]*planpb.Expr, 0, len(filters))

	for _, filter := range filters {
		expr, ok := lowerExprToHnswFilterExpr(filter, scanNode, scanTag, partPos)
		if !ok {
			remaining = append(remaining, filter)
			continue
		}
		payload.Exprs = append(payload.Exprs, expr)
		pushdown = append(pushdown, filter)
	}
	if len(payload.Exprs) == 0 {
		return "", nil, remaining, nil
	}

	bytes, err := json.Marshal(payload)
	if err != nil {
		return "", nil, nil, err
	}
	return string(bytes), pushdown, remaining, nil
}

func lowerExprToHnswFilterExpr(expr *planpb.Expr, scanNode *planpb.Node, scanTag, partPos int32) (*hnswFilterExpr, bool) {
	if expr == nil {
		return &hnswFilterExpr{Kind: "literal", Value: true}, true
	}

	switch impl := expr.Expr.(type) {
	case *planpb.Expr_Col:
		colName, ok := vectorIndexColumnNameFromColRef(impl.Col, scanNode, scanTag)
		if !ok {
			return nil, false
		}
		return &hnswFilterExpr{Kind: "column", Column: colName}, true
	case *planpb.Expr_Lit:
		val, ok := vectorIndexLiteralPayloadValue(impl.Lit)
		if !ok {
			return nil, false
		}
		return &hnswFilterExpr{Kind: "literal", Value: val}, true
	case *planpb.Expr_List:
		args := make([]*hnswFilterExpr, 0, len(impl.List.List))
		for _, item := range impl.List.List {
			lowered, ok := lowerExprToHnswFilterExpr(item, scanNode, scanTag, partPos)
			if !ok {
				return nil, false
			}
			args = append(args, lowered)
		}
		return &hnswFilterExpr{Kind: "list", Args: args}, true
	case *planpb.Expr_F:
		if isVectorDistanceExpr(expr, scanTag, partPos) || impl.F == nil || impl.F.Func == nil || impl.F.Func.ObjName == "" {
			return nil, false
		}
		args := make([]*hnswFilterExpr, 0, len(impl.F.Args))
		for _, arg := range impl.F.Args {
			lowered, ok := lowerExprToHnswFilterExpr(arg, scanNode, scanTag, partPos)
			if !ok {
				return nil, false
			}
			args = append(args, lowered)
		}
		return &hnswFilterExpr{Kind: "func", Op: strings.ToLower(impl.F.Func.ObjName), Args: args}, true
	default:
		return nil, false
	}
}

// lowerFiltersToCagraJSON defines the planner-side CAGRA JSON predicate
// contract. The JSON uses logical column names; a future CAGRA runtime can map
// names to include-column ordinals while keeping planner binding coordinates
// private to this lowering layer.
func lowerFiltersToCagraJSON(filters []*planpb.Expr, scanNode *planpb.Node, partPos int32) (string, []*planpb.Expr, []*planpb.Expr, error) {
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return "", nil, filters, nil
	}

	scanTag := scanNode.BindingTags[0]
	predicates := make([]cagraFilterPredicate, 0, len(filters))
	pushdown := make([]*planpb.Expr, 0, len(filters))
	remaining := make([]*planpb.Expr, 0, len(filters))

	for _, filter := range filters {
		start := len(predicates)
		if !appendCagraPredicatesFromExpr(filter, scanNode, scanTag, partPos, &predicates) {
			predicates = predicates[:start]
			remaining = append(remaining, filter)
			continue
		}
		pushdown = append(pushdown, filter)
	}
	if len(predicates) == 0 {
		return "", nil, remaining, nil
	}

	bytes, err := json.Marshal(predicates)
	if err != nil {
		return "", nil, nil, err
	}
	return string(bytes), pushdown, remaining, nil
}

func appendCagraPredicatesFromExpr(expr *planpb.Expr, scanNode *planpb.Node, scanTag, partPos int32, out *[]cagraFilterPredicate) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName == "" || isVectorDistanceExpr(expr, scanTag, partPos) {
		return false
	}

	fnName := strings.ToLower(fn.Func.ObjName)
	args := fn.Args
	switch fnName {
	case "and":
		if len(args) != 2 {
			return false
		}
		return appendCagraPredicatesFromExpr(args[0], scanNode, scanTag, partPos, out) &&
			appendCagraPredicatesFromExpr(args[1], scanNode, scanTag, partPos, out)
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		if len(args) != 2 {
			return false
		}
		column, val, op, ok := cagraComparisonArgs(fnName, args[0], args[1], scanNode, scanTag)
		if !ok {
			return false
		}
		*out = append(*out, cagraFilterPredicate{Column: column, Op: op, Val: val})
		return true
	case "between":
		if len(args) != 3 {
			return false
		}
		column, ok := cagraColumnNameFromExpr(args[0], scanNode, scanTag)
		if !ok {
			return false
		}
		lo, ok := cagraLiteralValueFromExpr(args[1])
		if !ok {
			return false
		}
		hi, ok := cagraLiteralValueFromExpr(args[2])
		if !ok {
			return false
		}
		*out = append(*out, cagraFilterPredicate{Column: column, Op: "between", Lo: lo, Hi: hi})
		return true
	case "in", "partition_in":
		if len(args) < 2 {
			return false
		}
		column, ok := cagraColumnNameFromExpr(args[0], scanNode, scanTag)
		if !ok {
			return false
		}
		vals, ok := cagraLiteralList(args[1:])
		if !ok {
			return false
		}
		*out = append(*out, cagraFilterPredicate{Column: column, Op: "in", Vals: vals})
		return true
	default:
		return false
	}
}

func cagraComparisonArgs(fnName string, left, right *planpb.Expr, scanNode *planpb.Node, scanTag int32) (string, any, string, bool) {
	if column, ok := cagraColumnNameFromExpr(left, scanNode, scanTag); ok {
		val, ok := cagraLiteralValueFromExpr(right)
		return column, val, normalizeCagraComparisonOp(fnName), ok
	}
	if column, ok := cagraColumnNameFromExpr(right, scanNode, scanTag); ok {
		val, ok := cagraLiteralValueFromExpr(left)
		return column, val, flipCagraComparisonOp(fnName), ok
	}
	return "", nil, "", false
}

func cagraColumnNameFromExpr(expr *planpb.Expr, scanNode *planpb.Node, scanTag int32) (string, bool) {
	if expr == nil {
		return "", false
	}
	col := expr.GetCol()
	if col == nil {
		return "", false
	}
	return vectorIndexColumnNameFromColRef(col, scanNode, scanTag)
}

func cagraLiteralValueFromExpr(expr *planpb.Expr) (any, bool) {
	if expr == nil {
		return nil, false
	}
	lit := expr.GetLit()
	if lit == nil {
		return nil, false
	}
	return vectorIndexLiteralPayloadValue(lit)
}

func cagraLiteralList(exprs []*planpb.Expr) ([]any, bool) {
	if len(exprs) == 1 {
		if list := exprs[0].GetList(); list != nil {
			return cagraLiteralList(list.List)
		}
	}

	vals := make([]any, 0, len(exprs))
	for _, expr := range exprs {
		val, ok := cagraLiteralValueFromExpr(expr)
		if !ok {
			return nil, false
		}
		vals = append(vals, val)
	}
	return vals, true
}

func normalizeCagraComparisonOp(op string) string {
	if op == "<>" {
		return "!="
	}
	return op
}

func flipCagraComparisonOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	default:
		return normalizeCagraComparisonOp(op)
	}
}

func vectorIndexLiteralPayloadValue(lit *planpb.Literal) (any, bool) {
	if lit == nil {
		return nil, false
	}
	if lit.Isnull {
		return nil, true
	}

	switch v := lit.Value.(type) {
	case *planpb.Literal_I8Val:
		return int64(int8(v.I8Val)), true
	case *planpb.Literal_I16Val:
		return int64(int16(v.I16Val)), true
	case *planpb.Literal_I32Val:
		return int64(v.I32Val), true
	case *planpb.Literal_I64Val:
		return v.I64Val, true
	case *planpb.Literal_U8Val:
		return uint64(uint8(v.U8Val)), true
	case *planpb.Literal_U16Val:
		return uint64(uint16(v.U16Val)), true
	case *planpb.Literal_U32Val:
		return uint64(v.U32Val), true
	case *planpb.Literal_U64Val:
		return v.U64Val, true
	case *planpb.Literal_Fval:
		return float64(v.Fval), true
	case *planpb.Literal_Dval:
		return v.Dval, true
	case *planpb.Literal_Sval:
		return v.Sval, true
	case *planpb.Literal_Bval:
		return v.Bval, true
	case *planpb.Literal_Dateval:
		return v.Dateval, true
	case *planpb.Literal_Timeval:
		return v.Timeval, true
	case *planpb.Literal_Datetimeval:
		return v.Datetimeval, true
	case *planpb.Literal_Timestampval:
		return v.Timestampval, true
	case *planpb.Literal_Jsonval:
		return v.Jsonval, true
	case *planpb.Literal_EnumVal:
		return v.EnumVal, true
	case *planpb.Literal_VecVal:
		return v.VecVal, true
	case *planpb.Literal_Decimal64Val:
		if v.Decimal64Val == nil {
			return nil, false
		}
		return v.Decimal64Val.String(), true
	case *planpb.Literal_Decimal128Val:
		if v.Decimal128Val == nil {
			return nil, false
		}
		return v.Decimal128Val.String(), true
	default:
		return nil, false
	}
}
