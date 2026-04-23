// Copyright 2024 Matrix Origin
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

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// Shared predicate-pushdown helpers for GPU vector indexes that use the
// C++ FilterStore / eval_filter_bitmap_cpu path (CAGRA + IVFPQ).
//
// The output is a JSON array of predicate objects, parsed on the C++ side by
// parse_preds() in cgo/cuvs/filter.hpp. The array's entries are implicitly
// ANDed together; "col" is a 0-based ordinal into the INCLUDE column list.
//
// Predicate object shape (one entry per supported op):
//
//	[
//	  {"col": 0, "op": "=",            "val": 10},
//	  {"col": 1, "op": "!=",           "val": 5.5},
//	  {"col": 1, "op": "<",            "val": 5.5},
//	  {"col": 1, "op": "<=",           "val": 5.5},
//	  {"col": 1, "op": ">",            "val": 5.5},
//	  {"col": 1, "op": ">=",           "val": 5.5},
//	  {"col": 2, "op": "between",      "lo":  1.0, "hi": 9.9},
//	  {"col": 3, "op": "in",           "vals": [100, 200, 300]},
//	  {"col": 4, "op": "is_null"},
//	  {"col": 5, "op": "is_not_null"}
//	]
//
// Value encoding: numeric literals are emitted as bare JSON numbers (int or
// float). The C++ side narrows them to the FilterStore column's physical
// type at eval time. NULL literals are not representable here — predicates
// referencing them stay as residual filters on the TABLE_SCAN.
//
// NULL semantics (SQL three-valued logic) are enforced on the C++ side: a
// value-comparison predicate on a NULL cell evaluates to UNKNOWN and the row
// is treated as non-matching. Only is_null / is_not_null inspect validity.

// parseIncludedColumnsFromParams reads the comma-joined "included_columns"
// entry from an index's algo-params JSON. Returns nil when the key is absent
// or empty (treated as "no INCLUDE columns declared").
func parseIncludedColumnsFromParams(indexAlgoParams string) ([]string, error) {
	if indexAlgoParams == "" {
		return nil, nil
	}
	val, err := sonic.Get([]byte(indexAlgoParams), catalog.IncludedColumns)
	if err != nil {
		return nil, nil
	}
	joined, err := val.StrictString()
	if err != nil || joined == "" {
		return nil, nil
	}
	raw := strings.Split(joined, ",")
	out := make([]string, 0, len(raw))
	for _, n := range raw {
		n = strings.TrimSpace(n)
		if n != "" {
			out = append(out, n)
		}
	}
	return out, nil
}

// filterJSONPred mirrors one entry of the predicate array (see file header
// for the full schema). omitempty lets a single struct cover every op:
// scalar comparisons use Val; "between" uses Lo+Hi; "in" uses Vals;
// "is_null" / "is_not_null" use neither.
type filterJSONPred struct {
	Col  int    `json:"col"`
	Op   string `json:"op"`
	Val  any    `json:"val,omitempty"`
	Lo   any    `json:"lo,omitempty"`
	Hi   any    `json:"hi,omitempty"`
	Vals []any  `json:"vals,omitempty"`
}

// buildFilterPredicateJSON walks scanNode.FilterList-style predicates and
// peels off those that reference only INCLUDE columns. Peeled predicates
// are serialized into the CAGRA/IVFPQ filter JSON array; unrecognized or
// mixed-reference predicates stay as residual filters the caller should
// leave on the TABLE_SCAN.
//
// Returns:
//   - predsJSON:  JSON array (empty "" if nothing peeled)
//   - serialized: the source exprs that made it into predsJSON
//   - residual:   the remainder that stays on scanNode.FilterList
func buildFilterPredicateJSON(
	filters []*plan.Expr,
	scanNode *plan.Node,
	includeColumns []string,
) (predsJSON string, serialized []*plan.Expr, residual []*plan.Expr, err error) {
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return "", nil, filters, nil
	}
	if len(includeColumns) == 0 || len(filters) == 0 {
		return "", nil, filters, nil
	}

	colOrd := make(map[string]int, len(includeColumns))
	for i, n := range includeColumns {
		colOrd[n] = i
	}
	scanTag := scanNode.BindingTags[0]
	td := scanNode.TableDef

	preds := make([]filterJSONPred, 0, len(filters))
	for _, expr := range filters {
		entries, ok, ferr := filterExprToPreds(expr, scanTag, td, colOrd)
		if ferr != nil {
			return "", nil, nil, ferr
		}
		if !ok {
			residual = append(residual, expr)
			continue
		}
		preds = append(preds, entries...)
		serialized = append(serialized, expr)
	}
	if len(preds) == 0 {
		return "", nil, residual, nil
	}
	buf, err := json.Marshal(preds)
	if err != nil {
		return "", nil, nil, err
	}
	return string(buf), serialized, residual, nil
}

// filterExprToPreds produces zero-or-more filterJSONPred entries for a single
// top-level filter. Top-level AND is decomposed (SQL AND semantics match the
// C++ side's implicit-AND of the predicate array). ok=false means the
// expression isn't serializable and must remain a residual filter.
func filterExprToPreds(
	expr *plan.Expr, scanTag int32, td *plan.TableDef, colOrd map[string]int,
) ([]filterJSONPred, bool, error) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return nil, false, nil
	}
	name := strings.ToLower(fn.Func.ObjName)

	if name == "and" && len(fn.Args) == 2 {
		left, okL, err := filterExprToPreds(fn.Args[0], scanTag, td, colOrd)
		if err != nil {
			return nil, false, err
		}
		right, okR, err := filterExprToPreds(fn.Args[1], scanTag, td, colOrd)
		if err != nil {
			return nil, false, err
		}
		if !okL || !okR {
			return nil, false, nil
		}
		return append(left, right...), true, nil
	}

	if op, okCmp := filterCmpOpFromFnName(name); okCmp && len(fn.Args) == 2 {
		ord, lit, flipped, ok := filterExtractColAndLit(fn.Args[0], fn.Args[1], scanTag, td, colOrd)
		if !ok {
			return nil, false, nil
		}
		if flipped {
			op = filterFlipCmpOp(op)
		}
		v, ok := filterLiteralToJSONValue(lit)
		if !ok {
			return nil, false, nil
		}
		return []filterJSONPred{{Col: ord, Op: op, Val: v}}, true, nil
	}

	if name == "between" && len(fn.Args) == 3 {
		ord, ok := filterColOrdinal(fn.Args[0], scanTag, td, colOrd)
		if !ok {
			return nil, false, nil
		}
		lo, okL := filterLiteralToJSONValue(fn.Args[1].GetLit())
		hi, okH := filterLiteralToJSONValue(fn.Args[2].GetLit())
		if !okL || !okH {
			return nil, false, nil
		}
		return []filterJSONPred{{Col: ord, Op: "between", Lo: lo, Hi: hi}}, true, nil
	}

	if name == "in" && len(fn.Args) >= 2 {
		ord, ok := filterColOrdinal(fn.Args[0], scanTag, td, colOrd)
		if !ok {
			return nil, false, nil
		}
		items := filterInListItems(fn.Args)
		if len(items) == 0 {
			return nil, false, nil
		}
		vals := make([]any, 0, len(items))
		for _, it := range items {
			v, ok := filterLiteralToJSONValue(it.GetLit())
			if !ok {
				return nil, false, nil
			}
			vals = append(vals, v)
		}
		return []filterJSONPred{{Col: ord, Op: "in", Vals: vals}}, true, nil
	}

	if (name == "isnull" || name == "is_null") && len(fn.Args) == 1 {
		ord, ok := filterColOrdinal(fn.Args[0], scanTag, td, colOrd)
		if !ok {
			return nil, false, nil
		}
		return []filterJSONPred{{Col: ord, Op: "is_null"}}, true, nil
	}
	if (name == "isnotnull" || name == "is_not_null") && len(fn.Args) == 1 {
		ord, ok := filterColOrdinal(fn.Args[0], scanTag, td, colOrd)
		if !ok {
			return nil, false, nil
		}
		return []filterJSONPred{{Col: ord, Op: "is_not_null"}}, true, nil
	}
	return nil, false, nil
}

func filterCmpOpFromFnName(name string) (string, bool) {
	switch name {
	case "=":
		return "=", true
	case "!=", "<>":
		return "!=", true
	case "<":
		return "<", true
	case "<=":
		return "<=", true
	case ">":
		return ">", true
	case ">=":
		return ">=", true
	}
	return "", false
}

// filterFlipCmpOp is used when the comparison was written as `lit OP col`
// — flipping turns it into an equivalent `col OP' lit` so the serialized
// JSON always has the column on the left.
func filterFlipCmpOp(op string) string {
	switch op {
	case "<":
		return ">"
	case "<=":
		return ">="
	case ">":
		return "<"
	case ">=":
		return "<="
	}
	return op
}

// filterColOrdinal returns the INCLUDE-list ordinal (0-based) of the column
// referenced by `e`, provided `e` is a ColRef into the scan with a name in
// the covered set.
func filterColOrdinal(e *plan.Expr, scanTag int32, td *plan.TableDef, colOrd map[string]int) (int, bool) {
	col := e.GetCol()
	if col == nil || col.RelPos != scanTag {
		return 0, false
	}
	if int(col.ColPos) >= len(td.Cols) {
		return 0, false
	}
	ord, ok := colOrd[td.Cols[col.ColPos].Name]
	return ord, ok
}

// filterExtractColAndLit handles both orientations of a binary comparison:
// (col OP lit) and (lit OP col). The returned `flipped` flag tells the
// caller to invert the operator for the latter.
func filterExtractColAndLit(
	a, b *plan.Expr, scanTag int32, td *plan.TableDef, colOrd map[string]int,
) (int, *plan.Literal, bool, bool) {
	if ord, ok := filterColOrdinal(a, scanTag, td, colOrd); ok {
		if lit := b.GetLit(); lit != nil {
			return ord, lit, false, true
		}
	}
	if ord, ok := filterColOrdinal(b, scanTag, td, colOrd); ok {
		if lit := a.GetLit(); lit != nil {
			return ord, lit, true, true
		}
	}
	return 0, nil, false, false
}

// filterInListItems normalises the two shapes an IN clause may take after
// planning: (col, Expr_List{...}) or (col, lit0, lit1, ...).
func filterInListItems(args []*plan.Expr) []*plan.Expr {
	if len(args) == 2 {
		if lst, ok := args[1].Expr.(*plan.Expr_List); ok && lst.List != nil {
			return lst.List.List
		}
	}
	return args[1:]
}

// filterLiteralToJSONValue converts a plan.Literal into an `any` that
// json.Marshal emits as a bare JSON number (for numerics) or string.
// Unsupported shapes (NULL, date/time types, decimals, binary, vectors)
// return ok=false so the caller treats the predicate as residual.
func filterLiteralToJSONValue(lit *plan.Literal) (any, bool) {
	if lit == nil || lit.Isnull {
		return nil, false
	}
	switch v := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return int64(int8(v.I8Val)), true
	case *plan.Literal_I16Val:
		return int64(int16(v.I16Val)), true
	case *plan.Literal_I32Val:
		return int64(v.I32Val), true
	case *plan.Literal_I64Val:
		return v.I64Val, true
	case *plan.Literal_U8Val:
		return uint64(uint8(v.U8Val)), true
	case *plan.Literal_U16Val:
		return uint64(uint16(v.U16Val)), true
	case *plan.Literal_U32Val:
		return uint64(v.U32Val), true
	case *plan.Literal_U64Val:
		return v.U64Val, true
	case *plan.Literal_Fval:
		return float64(v.Fval), true
	case *plan.Literal_Dval:
		return v.Dval, true
	case *plan.Literal_Bval:
		if v.Bval {
			return int64(1), true
		}
		return int64(0), true
	}
	// VARCHAR/string literals require FNV-1a hashing to match the
	// UINT64-hashed column storage — deferred; treat as residual for now.
	return nil, false
}
