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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// A new access bound changes which rows reach the other filters. Restrict it
// to comparisons that cannot throw or vary with evaluation order. In
// particular, a foldable expression is not necessarily a total expression.
func indexRangeFiltersAreTotal(node *plan.Node) bool {
	if node == nil || len(node.BindingTags) == 0 || len(node.FilterList) == 0 {
		return false
	}
	for _, filter := range node.FilterList {
		if filter == nil {
			return false
		}
		fn := filter.GetF()
		if fn == nil || fn.Func == nil {
			return false
		}
		argc := 2
		switch fn.Func.ObjName {
		case "=", "<", "<=", ">", ">=":
		case "between":
			argc = 3
		case "in_range":
			argc = 4
		default:
			return false
		}
		if len(fn.Args) != argc {
			return false
		}
		col := indexComparisonColumn(fn)
		if col == nil || col.GetCol().RelPos != node.BindingTags[0] {
			return false
		}
		if _, _, ok := integerTypeWidth(types.T(col.Typ.Id)); !ok {
			return false
		}
		for i, arg := range fn.Args {
			if arg == col {
				continue
			}
			if arg == nil || arg.GetLit() == nil || arg.GetLit().Isnull || arg.GetLit().Src != nil {
				return false
			}
			if fn.Func.ObjName == "in_range" && i == 3 {
				flag, ok := arg.GetLit().Value.(*plan.Literal_U8Val)
				if arg.Typ.Id != int32(types.T_uint8) || !ok || flag.U8Val > 3 {
					return false
				}
			} else if _, _, ok := integerTypeWidth(types.T(arg.Typ.Id)); !ok {
				return false
			}
		}
	}
	return true
}

// A hinted covering scan already owns the chosen index and mapped SQL filters.
// Add an optional physical bound without changing its metadata or error path.
func (builder *QueryBuilder) tightenHintedCoveringRange(idx *IndexDef, scan, indexScan *plan.Node, colMap map[[2]int32]*plan.Expr) {
	if idx == nil || idx.Unique || len(idx.Parts) < 2 || !indexRangeFiltersAreTotal(scan) ||
		builder.optimizerHints != nil && builder.optimizerHints.blockFilter == 2 {
		return
	}
	lengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idx.IndexAlgoParams)
	if err != nil || lengths[catalog.ResolveAlias(idx.Parts[0])] != 0 {
		return
	}
	positions, equal := findLeadingFilter(idx, scan)
	if len(positions) == 0 {
		return
	}
	if equal {
		positions = tryMatchMoreLeadingFilters(idx, scan, positions[0])
	} else {
		positions = coveringLeadingRangePair(scan, positions[0])
	}
	paired := !equal && len(positions) == 2
	normalized, ok := normalizeIndexAccessFilters(builder.GetContext(), idx, scan.TableDef, scan.FilterList, positions, paired)
	if !ok {
		return
	}
	tag := indexScan.BindingTags[0]
	var lookup *plan.Expr
	if equal {
		lookup = builder.replaceEqualCondition(idx, normalized, positions, tag, indexScan.TableDef)
	} else if paired {
		lookup = builder.replaceRangePairCondition(idx, normalized, positions, tag, indexScan.TableDef)
	} else {
		filter := normalized[positions[0]]
		if hasUnsafeRangeOp(filter.GetF()) {
			lookup = builder.hintedOneSidedIndexRange(idx, filter, tag, indexScan.TableDef)
		} else {
			lookup = builder.replaceNonEqualCondition(idx, filter, tag, indexScan.TableDef)
		}
	}
	if lookup == nil || !ExprIsZonemappable(builder.GetContext(), lookup) {
		return
	}

	filters := make([]*plan.Expr, 0, len(indexScan.FilterList)+1)
	filters = append(filters, lookup)
	for i, filter := range indexScan.FilterList {
		if slices.Contains(positions, int32(i)) {
			if needsIndexOnlyResidualLeadingFilters(idx, scan.FilterList, []int32{int32(i)}) {
				residual := DeepCopyExpr(filter)
				residual.Selectivity = 1
				filters = append(filters, residual)
			}
			continue
		}
		filters = append(filters, filter)
	}
	candidate := *indexScan
	candidate.FilterList = filters
	candidate.BlockFilterList = slices.Clone(indexScan.BlockFilterList)
	if equal {
		// Do not replace a decoded equality unless a suffix bound actually
		// tightens it. The shared helper retains the decoded suffix recheck.
		if !builder.installSecondaryIndexPrefixRanges(idx, scan, &candidate, positions, colMap) {
			return
		}
	} else {
		appendUniqueBlockFilter(&candidate, lookup, true)
	}
	indexScan.FilterList = candidate.FilterList
	indexScan.BlockFilterList = candidate.BlockFilterList
}

// The range-pair builder expects a lower bound followed by an upper bound.
func coveringLeadingRangePair(scan *plan.Node, first int32) []int32 {
	col, lower := classifyRangeBound(scan.FilterList[first].GetF())
	if col == nil {
		return []int32{first}
	}
	for i, filter := range scan.FilterList {
		if int32(i) == first {
			continue
		}
		other, otherLower := classifyRangeBound(filter.GetF())
		if other == nil || otherLower == lower || other.RelPos != col.RelPos || other.ColPos != col.ColPos {
			continue
		}
		if lower {
			return []int32{first, int32(i)}
		}
		return []int32{int32(i), first}
	}
	return []int32{first}
}

// Combine two one-sided bounds within the same equality prefix. The decoded
// SQL filters remain authoritative; this only narrows the physical lookup.
func (builder *QueryBuilder) combineIndexPrefixBounds(prefixLookup *plan.Expr, bounds []*plan.Expr) *plan.Expr {
	prefixFn := prefixLookup.GetF()
	if !hasNonNilFunctionArgs(prefixFn, 2) || prefixFn.Func.ObjName != "prefix_eq" {
		return nil
	}
	prefix := prefixFn.Args[1]
	if len(bounds) < 2 {
		return nil
	}
	first := bounds[0].GetF()
	if !hasNonNilFunctionArgs(first, 4) || first.Func.ObjName != "prefix_in_range" ||
		exprStructuralEqual(first.Args[1], prefix) == exprStructuralEqual(first.Args[2], prefix) {
		// Keep an existing two-sided lookup; replacing it with a different
		// pair could widen the physical scan.
		return nil
	}
	var lower, upper *plan.Expr
	var flag uint8
	for _, bound := range bounds {
		fn := bound.GetF()
		if !hasNonNilFunctionArgs(fn, 4) || fn.Func.ObjName != "prefix_in_range" || fn.Args[3].GetLit() == nil {
			continue
		}
		value, ok := fn.Args[3].GetLit().Value.(*plan.Literal_U8Val)
		if !ok || value.U8Val > 3 {
			continue
		}
		if lower == nil && !exprStructuralEqual(fn.Args[1], prefix) && exprStructuralEqual(fn.Args[2], prefix) {
			lower = fn.Args[1]
			flag |= uint8(value.U8Val) & 1
		} else if upper == nil && exprStructuralEqual(fn.Args[1], prefix) && !exprStructuralEqual(fn.Args[2], prefix) {
			upper = fn.Args[2]
			flag |= uint8(value.U8Val) & 2
		}
	}
	if lower == nil || upper == nil {
		return nil
	}
	combined := builder.bindOptionalIndexFunc("prefix_in_range", []*plan.Expr{
		DeepCopyExpr(prefixFn.Args[0]), DeepCopyExpr(lower), DeepCopyExpr(upper), MakePlan2Uint8ConstExprWithType(flag),
	})
	if combined != nil {
		combined.Selectivity = prefixLookup.Selectivity
	}
	return combined
}

// A serialized key has more components after the leading value. Prefix range
// comparison handles the otherwise unsafe single-sided > and <= endpoints.
func (builder *QueryBuilder) hintedOneSidedIndexRange(idx *IndexDef, filter *plan.Expr, tag int32, table *plan.TableDef) *plan.Expr {
	fn := filter.GetF()
	bound := rangeFilterConstValue(fn)
	if bound == nil {
		return nil
	}
	encoded := builder.bindOptionalIndexFunc(indexTableLookupSerialFunc(idx), []*plan.Expr{DeepCopyExpr(bound)})
	if encoded == nil {
		return nil
	}
	empty := makePlan2StringConstExprWithType("")
	empty.Typ.Id = int32(types.T_varchar)
	key := GetColExpr(table.Cols[0].Typ, tag, 0)
	var args []*plan.Expr
	switch canonicalRangeOp(fn) {
	case ">":
		args = []*plan.Expr{key, encoded, empty, MakePlan2Uint8ConstExprWithType(1)}
	case "<=":
		args = []*plan.Expr{key, empty, encoded, MakePlan2Uint8ConstExprWithType(0)}
	default:
		return nil
	}
	lookup := builder.bindOptionalIndexFunc("prefix_in_range", args)
	if lookup != nil {
		lookup.Selectivity = filter.Selectivity
	}
	return lookup
}
