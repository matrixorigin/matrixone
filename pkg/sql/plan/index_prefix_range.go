// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"bytes"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// secondaryIndexPrefixRanges adds physical bounds after a contiguous equality
// prefix. The original SQL filters remain authoritative on both scans.
func (builder *QueryBuilder) secondaryIndexPrefixRanges(idx *plan.IndexDef, scan, indexScan *plan.Node, equalityFilters []int32, colMap map[[2]int32]*plan.Expr) []*plan.Expr {
	if idx == nil || idx.Unique || scan == nil || scan.TableDef == nil || len(scan.BindingTags) == 0 ||
		indexScan == nil || len(indexScan.BindingTags) == 0 || len(indexScan.FilterList) == 0 || len(equalityFilters) == 0 || len(equalityFilters) >= len(idx.Parts)-1 {
		return nil
	}
	lookup := indexScan.FilterList[0].GetF()
	if !hasNonNilFunctionArgs(lookup, 2) || lookup.Func.ObjName != "prefix_eq" || lookup.Args[0].GetCol() == nil {
		return nil
	}
	if col := lookup.Args[0].GetCol(); col.RelPos != indexScan.BindingTags[0] || col.ColPos != 0 {
		return nil
	}
	prefix := lookup.Args[1]
	serial := prefix.GetF()
	if serial == nil || serial.Func == nil || serial.Func.ObjName != function.SerialFullFunctionName || serial.Func.Obj != function.SerialFullFunctionEncodeID || len(serial.Args) != len(equalityFilters) {
		return nil
	}
	lengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idx.IndexAlgoParams)
	if err != nil {
		return nil
	}
	safeValue := func(expr *plan.Expr, typ plan.Type) bool {
		return expr != nil && expr.Typ.Id == typ.Id && (expr.GetLit() != nil || expr.GetP() != nil)
	}
	for i, filterID := range equalityFilters {
		if filterID < 0 || int(filterID) >= len(scan.FilterList) {
			return nil
		}
		name := catalog.ResolveAlias(idx.Parts[i])
		pos, ok := scan.TableDef.Name2ColIndex[name]
		if !ok || pos < 0 || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil || lengths[name] != 0 {
			return nil
		}
		typ := scan.TableDef.Cols[pos].Typ
		if _, _, ok := integerTypeWidth(types.T(typ.Id)); !ok {
			return nil
		}
		eq := scan.FilterList[filterID].GetF()
		if !hasNonNilFunctionArgs(eq, 2) || eq.Func.ObjName != "=" {
			return nil
		}
		col := eq.Args[0].GetCol()
		if col == nil || col.RelPos != scan.BindingTags[0] || col.ColPos != pos ||
			!safeValue(eq.Args[1], typ) || !safeValue(serial.Args[i], typ) {
			return nil
		}
	}
	name := catalog.ResolveAlias(idx.Parts[len(equalityFilters)])
	pos, ok := scan.TableDef.Name2ColIndex[name]
	if !ok || pos < 0 || int(pos) >= len(scan.TableDef.Cols) || scan.TableDef.Cols[pos] == nil || lengths[name] != 0 {
		return nil
	}
	typ := scan.TableDef.Cols[pos].Typ
	if _, _, ok := integerTypeWidth(types.T(typ.Id)); !ok {
		return nil
	}

	// Most equality-prefix scans have no usable range; allocate only after a match.
	//nolint:prealloc
	var bounds []*plan.Expr
	for i, filter := range scan.FilterList {
		if slices.Contains(equalityFilters, int32(i)) || filter == nil {
			continue
		}
		fn := filter.GetF()
		if fn == nil || fn.Func == nil {
			continue
		}
		var col *plan.ColRef
		var lower, upper *plan.Expr
		var flag *plan.Expr
		switch fn.Func.ObjName {
		case "between", "in_range":
			count := 3
			if fn.Func.ObjName == "in_range" {
				count = 4
			}
			if !hasNonNilFunctionArgs(fn, count) {
				continue
			}
			col = fn.Args[0].GetCol()
			lower, upper = fn.Args[1], fn.Args[2]
			if count == 4 {
				if fn.Args[3].GetLit() == nil {
					continue
				}
				flag = DeepCopyExpr(fn.Args[3])
			}
		case ">", ">=", "<", "<=":
			if !hasNonNilFunctionArgs(fn, 2) {
				continue
			}
			col, _ = classifyRangeBound(fn)
			bound := rangeFilterConstValue(fn)
			switch canonicalRangeOp(fn) {
			case ">":
				lower, upper, flag = bound, nil, MakePlan2Uint8ConstExprWithType(1)
			case ">=":
				lower, upper, flag = bound, nil, MakePlan2Uint8ConstExprWithType(0)
			case "<":
				lower, upper, flag = nil, bound, MakePlan2Uint8ConstExprWithType(2)
			case "<=":
				lower, upper, flag = nil, bound, MakePlan2Uint8ConstExprWithType(0)
			}
		default:
			continue
		}
		if col == nil || col.RelPos != scan.BindingTags[0] || col.ColPos != pos ||
			lower != nil && !safeValue(lower, typ) || upper != nil && !safeValue(upper, typ) ||
			lower == nil && upper == nil {
			continue
		}
		// The range contributes to statistics through its decoded SQL recheck.
		// Only build this access bound when that recheck was actually published.
		if !exprUsesOnlyMappedCols(filter, colMap) || exprMayChangeWhenRepeated(filter) {
			continue
		}
		mapped := replaceColumnsForExpr(DeepCopyExpr(filter), colMap)
		if !slices.ContainsFunc(indexScan.FilterList[1:], func(existing *plan.Expr) bool {
			return exprStructuralEqual(existing, mapped)
		}) {
			continue
		}
		encode := func(value *plan.Expr) (*plan.Expr, error) {
			if value == nil {
				return DeepCopyExpr(prefix), nil
			}
			args := append(DeepCopyExprList(serial.Args), DeepCopyExpr(value))
			return BindFuncExprImplByPlanExpr(builder.GetContext(), function.SerialFullFunctionName, args)
		}
		lo, err := encode(lower)
		if err != nil {
			propagateKeySetControlError(builder.GetContext(), err)
			continue
		}
		hi, err := encode(upper)
		if err != nil {
			propagateKeySetControlError(builder.GetContext(), err)
			continue
		}
		args := []*plan.Expr{DeepCopyExpr(lookup.Args[0]), lo, hi}
		predicate := "prefix_between"
		if flag != nil {
			predicate = "prefix_in_range"
			args = append(args, flag)
		}
		bound, err := BindFuncExprImplByPlanExpr(builder.GetContext(), predicate, args)
		if err != nil {
			propagateKeySetControlError(builder.GetContext(), err)
			continue
		}
		// The decoded recheck already accounts for the range. The access
		// predicate contributes only the preceding equality prefix.
		bound.Selectivity = indexScan.FilterList[0].Selectivity
		bounds = append(bounds, bound)
	}
	return bounds
}

// indexAccessAccountsForSelectivity recognizes a SQL predicate whose estimate
// is already included in an encoded index access bound. The decoded predicate
// remains in the execution plan: this proof only avoids counting it twice.
func indexAccessAccountsForSelectivity(def *plan.IndexDef, base, index *plan.Node, filter *plan.Expr) bool {
	if def == nil || base == nil || base.TableDef == nil || index == nil ||
		len(def.Parts) == 0 || len(base.BindingTags) != 1 || len(index.BindingTags) != 1 ||
		len(index.FilterList) == 0 || filter == nil || filter.Selectivity <= 0 {
		return false
	}
	if index.FilterList[0] == nil {
		return false
	}
	fn, access := filter.GetF(), index.FilterList[0].GetF()
	if !hasNonNilFunctionArgs(fn, 2) || access == nil || access.Func == nil ||
		len(access.Args) < 2 || access.Args[0] == nil || access.Args[1] == nil ||
		index.FilterList[0].Selectivity != filter.Selectivity {
		return false
	}
	key := access.Args[0].GetCol()
	if key == nil || key.RelPos != index.BindingTags[0] || key.ColPos != 0 {
		return false
	}
	column, value := fn.Args[0], fn.Args[1]
	if column.GetCol() == nil && fn.Func.ObjName == "=" {
		column, value = value, column
	}
	col := column.GetCol()
	name := catalog.ResolveAlias(def.Parts[0])
	pos, ok := base.TableDef.Name2ColIndex[name]
	if !ok || col == nil || col.RelPos != base.BindingTags[0] || col.ColPos != pos ||
		pos < 0 || int(pos) >= len(base.TableDef.Cols) || base.TableDef.Cols[pos] == nil {
		return false
	}
	typ := base.TableDef.Cols[pos].Typ
	if _, _, ok := integerTypeWidth(types.T(typ.Id)); !ok || column.Typ.Id != typ.Id {
		return false
	}
	lengths, err := catalog.IndexPrefixLengthsFromParamsWithError(def.IndexAlgoParams)
	if err != nil || lengths[name] != 0 {
		return false
	}
	serialArgs := func(expr *plan.Expr) []*plan.Expr {
		if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil ||
			expr.GetF().Func.ObjName != function.SerialFullFunctionName ||
			expr.GetF().Func.Obj != function.SerialFullFunctionEncodeID {
			return nil
		}
		return expr.GetF().Args
	}
	switch access.Func.ObjName {
	case "prefix_between", "prefix_in_range":
		if fn.Func.ObjName != "=" || len(access.Args) < 3 || value.GetLit() == nil ||
			value.GetLit().Isnull || value.GetLit().Src != nil || value.Typ.Id != typ.Id {
			return false
		}
		lower, upper := serialArgs(access.Args[1]), serialArgs(access.Args[2])
		if len(lower) == 0 || len(upper) == 0 {
			return false
		}
		for _, bound := range []*plan.Expr{lower[0], upper[0]} {
			if bound == nil || bound.GetLit() == nil || bound.GetLit().Isnull ||
				bound.GetLit().Src != nil || bound.Typ.Id != typ.Id || !exprStructuralEqual(value, bound) {
				return false
			}
		}
		return true
	case "prefix_in":
		if fn.Func.ObjName != "in" || value.GetVec() == nil || literalVectorMayContainNull(value.GetVec()) {
			return false
		}
		args := serialArgs(access.Args[1])
		if len(args) != 1 || args[0] == nil || args[0].GetVec() == nil {
			return false
		}
		left, right := value.GetVec(), args[0].GetVec()
		if left.Len != right.Len || !bytes.Equal(left.Data, right.Data) {
			return false
		}
		vec, _, ok := decodeLiteralVec(left)
		return ok && vec.GetType().Oid == types.T(typ.Id)
	default:
		return false
	}
}
