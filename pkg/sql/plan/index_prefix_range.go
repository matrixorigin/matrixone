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

// indexScanSelectivityFilters builds an estimation-only view. An encoded
// regular-index bound can imply decoded SQL rechecks in the same scan. Those
// rechecks must still execute, but must not reduce the row estimate again.
// The actual expressions retain their own selectivity for other consumers.
func indexScanSelectivityFilters(node *plan.Node) []*plan.Expr {
	if node == nil {
		return nil
	}
	if !node.IndexScanInfo.IsIndexScan || node.IndexScanInfo.IsUnique || len(node.FilterList) < 2 {
		return node.FilterList
	}

	var key *plan.Expr
	var fixed []*plan.Expr
	var in *plan.LiteralVec
	for _, filter := range node.FilterList {
		fn := filter.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) < 2 || fn.Args[0] == nil {
			continue
		}
		col := fn.Args[0].GetCol()
		if col == nil || col.ColPos != 0 {
			continue
		}
		switch fn.Func.ObjName {
		case "prefix_eq":
			if hasNonNilFunctionArgs(fn, 2) {
				args := serialFullArgs(fn.Args[1])
				n := 0
				for n < len(args) && staticIntegerLiteral(args[n]) {
					n++
				}
				fixed = args[:n]
			}
		case "prefix_between", "prefix_in_range":
			n := 3
			if fn.Func.ObjName == "prefix_in_range" {
				n = 4
			}
			if hasNonNilFunctionArgs(fn, n) {
				lower, upper := serialFullArgs(fn.Args[1]), serialFullArgs(fn.Args[2])
				common := 0
				for common < len(lower) && common < len(upper) &&
					staticIntegerLiteral(lower[common]) && staticIntegerLiteral(upper[common]) &&
					exprStructuralEqual(lower[common], upper[common]) {
					common++
				}
				fixed = lower[:common]
			}
		case "prefix_in":
			if hasNonNilFunctionArgs(fn, 2) {
				args := serialFullArgs(fn.Args[1])
				if len(args) == 1 && args[0] != nil {
					in = args[0].GetVec()
				}
			}
		}
		if len(fixed) > 0 || in != nil {
			key = fn.Args[0]
			break
		}
	}
	if key == nil {
		return node.FilterList
	}

	var estimates []*plan.Expr
	for i, filter := range node.FilterList {
		if !indexAccessImpliesRecheck(key, fixed, in, filter) {
			continue
		}
		if estimates == nil {
			estimates = slices.Clone(node.FilterList)
		}
		estimate := *filter
		estimate.Selectivity = 1
		estimates[i] = &estimate
	}
	if estimates == nil {
		return node.FilterList
	}
	return estimates
}

func serialFullArgs(expr *plan.Expr) []*plan.Expr {
	if expr == nil {
		return nil
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != function.SerialFullFunctionName ||
		fn.Func.Obj != function.SerialFullFunctionEncodeID {
		return nil
	}
	return fn.Args
}

func staticIntegerLiteral(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	lit := expr.GetLit()
	_, _, integer := integerTypeWidth(types.T(expr.Typ.Id))
	return integer && lit != nil && !lit.Isnull && lit.Src == nil
}

func indexAccessImpliesRecheck(key *plan.Expr, fixed []*plan.Expr, in *plan.LiteralVec, filter *plan.Expr) bool {
	if filter == nil {
		return false
	}
	fn := filter.GetF()
	if !hasNonNilFunctionArgs(fn, 2) || (fn.Func.ObjName != "=" && fn.Func.ObjName != "in") {
		return false
	}
	column, value := fn.Args[0], fn.Args[1]
	if fn.Func.ObjName == "=" && column.GetF() == nil {
		column, value = value, column
	}
	extract := column.GetF()
	if !hasNonNilFunctionArgs(extract, 3) || extract.Func.ObjName != "serial_extract" ||
		!exprStructuralEqual(extract.Args[0], key) || extract.Args[2].GetT() == nil ||
		extract.Args[2].Typ.Id != column.Typ.Id {
		return false
	}
	id, _ := function.DecodeOverloadID(extract.Func.Obj)
	if id != function.SERIAL_EXTRACT {
		return false
	}
	positionLiteral := extract.Args[1].GetLit()
	if positionLiteral == nil || positionLiteral.Isnull || positionLiteral.Src != nil {
		return false
	}
	position, ok := positionLiteral.Value.(*plan.Literal_I64Val)
	if !ok || position == nil || position.I64Val < 0 {
		return false
	}
	_, _, integer := integerTypeWidth(types.T(column.Typ.Id))
	if !integer {
		return false
	}
	if fn.Func.ObjName == "=" {
		if position.I64Val >= int64(len(fixed)) || !staticIntegerLiteral(value) || value.Typ.Id != column.Typ.Id {
			return false
		}
		return exprStructuralEqual(value, fixed[position.I64Val])
	}

	if position.I64Val != 0 || in == nil || value.GetVec() == nil {
		return false
	}
	values := value.GetVec()
	if values.Len != in.Len || !bytes.Equal(values.Data, in.Data) || literalVectorMayContainNull(values) {
		return false
	}
	vec, _, ok := decodeLiteralVec(values)
	return ok && vec.GetType().Oid == types.T(column.Typ.Id)
}
