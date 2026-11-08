// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// These limits bound both analysis and the maximum encoded candidate. A
// prepared VARCHAR comparison is normally bound as VARCHAR(65535), even when
// the key column is narrower; its bound must therefore use the comparison
// value's type, not the column's declared width.
const (
	maxCompositePointNodes = 2048
	maxCompositePointDepth = 256
	maxCompositePoints     = 128
	maxCompositePointBytes = 80 << 20
)

type compositePointAnalysis struct {
	tag     int32
	parts   map[int32]int
	types   []plan.Type
	visited int
}

// compositePointCandidate returns references to the original values, one
// tuple per correlated OR arm. It never mutates the original predicates. A nil
// result means that ordinary composite-key lowering must be used instead.
func compositePointCandidate(node *plan.Node) [][]*plan.Expr {
	if node == nil || node.TableDef == nil || node.TableDef.Pkey == nil ||
		len(node.BindingTags) == 0 || node.TableDef.ClusterBy != nil {
		return nil
	}
	table := node.TableDef
	key := table.Pkey
	if len(key.Names) < 2 || len(key.Names) > maxCompositePointNodes ||
		key.PkeyColName != catalog.CPrimaryKeyColName || key.CompPkeyCol == nil {
		return nil
	}
	// Most composite-key scans contain no disjunction. Avoid allocating the
	// fact map or examining scalar expressions on that common path.
	remaining := maxCompositePointNodes
	hasDisjunction := false
	for _, filter := range node.FilterList {
		if compositePointHasOr(filter, 0, &remaining) {
			hasDisjunction = true
			break
		}
	}
	if !hasDisjunction {
		return nil
	}
	keyPos, ok := table.Name2ColIndex[key.PkeyColName]
	if !ok || keyPos < 0 || int(keyPos) >= len(table.Cols) || table.Cols[keyPos] == nil ||
		!table.Cols[keyPos].Hidden || table.Cols[keyPos].Name != key.PkeyColName ||
		types.T(table.Cols[keyPos].Typ.Id) != types.T_varchar {
		return nil
	}
	a := compositePointAnalysis{
		tag:   node.BindingTags[0],
		parts: make(map[int32]int, len(key.Names)),
		types: make([]plan.Type, len(key.Names)),
	}
	for i, name := range key.Names {
		pos, found := table.Name2ColIndex[name]
		if !found || pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil || table.Cols[pos].Name != name {
			return nil
		}
		if _, duplicate := a.parts[pos]; duplicate || !compositePointType(table.Cols[pos].Typ) {
			return nil
		}
		a.parts[pos], a.types[i] = i, table.Cols[pos].Typ
	}
	// A point candidate can suppress evaluation on rows it prunes. Do not add
	// it when any scan predicate might throw or have an observable effect.
	for _, filter := range node.FilterList {
		if !a.safePredicate(filter, 0) {
			return nil
		}
	}
	points, ok := a.conjunction(node.FilterList, make([]*plan.Expr, len(key.Names)), false, 0)
	if !ok || len(points) < 2 {
		return nil
	}
	var bytes uint64
	for _, point := range points {
		for _, value := range point {
			bound, supported := compositePointValueSize(value)
			if !supported || bound > maxCompositePointBytes-bytes {
				return nil
			}
			bytes += bound
		}
	}
	return points
}

func compositePointValueSize(expr *plan.Expr) (uint64, bool) {
	if lit := expr.GetLit(); lit != nil && types.T(expr.Typ.Id).IsMySQLString() {
		if lit.Isnull {
			return 0, true
		}
		value, ok := lit.Value.(*plan.Literal_Sval)
		if !ok || uint64(len(value.Sval)) > (maxCompositePointBytes-3)/2 {
			return 0, false
		}
		// Folded literals need not be bounded by their expression's declared
		// width. Count their actual bytes, including worst-case zero escaping.
		return 2*uint64(len(value.Sval)) + 3, true
	}
	return function.SerialEncodedTypeSizeBound(makeTypeByPlan2Expr(expr))
}

func compositePointHasOr(expr *plan.Expr, depth int, remaining *int) bool {
	if expr == nil || depth > maxCompositePointDepth || *remaining <= 0 {
		return false
	}
	*remaining = *remaining - 1
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	if fn.Func.ObjName == "or" {
		return true
	}
	if fn.Func.ObjName == "and" {
		for _, arg := range fn.Args {
			if compositePointHasOr(arg, depth+1, remaining) {
				return true
			}
		}
	}
	return false
}

func compositePointType(typ plan.Type) bool {
	switch types.T(typ.Id) {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_year, types.T_uuid,
		types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		return true
	default:
		// In particular, serialized float keys distinguish SQL-equal signed
		// zeroes. JSON and arrays also have non-byte equality semantics.
		return false
	}
}

func (a *compositePointAnalysis) step(depth int) bool {
	a.visited++
	return depth <= maxCompositePointDepth && a.visited <= maxCompositePointNodes
}

func (a *compositePointAnalysis) safePredicate(expr *plan.Expr, depth int) bool {
	if expr == nil || !a.step(depth) {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	switch fn.Func.ObjName {
	case "and", "or":
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		if len(fn.Args) < 2 || fn.Func.ObjName == "and" && id != function.AND || fn.Func.ObjName == "or" && id != function.OR {
			return false
		}
		for _, arg := range fn.Args {
			if !a.safePredicate(arg, depth+1) {
				return false
			}
		}
		return true
	case "=":
		_, _, ok := a.equality(expr)
		return ok
	default:
		return false
	}
}

// equality accepts only a bare local column and a total, stable scalar in the
// same physical comparison domain. It examines commuted equality without
// reordering the original expression.
func (a *compositePointAnalysis) equality(expr *plan.Expr) (int32, *plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return 0, nil, false
	}
	id, _ := function.DecodeOverloadID(fn.Func.Obj)
	if id != function.EQUAL {
		return 0, nil, false
	}
	colExpr, value := fn.Args[0], fn.Args[1]
	if colExpr.GetCol() == nil {
		colExpr, value = value, colExpr
	}
	col := colExpr.GetCol()
	if col == nil || col.RelPos != a.tag || !compositePointType(colExpr.Typ) ||
		colExpr.Typ.Id != value.Typ.Id || colExpr.Typ.Scale != value.Typ.Scale || !compositePointScalar(value) {
		return 0, nil, false
	}
	if part, ok := a.parts[col.ColPos]; ok {
		actual := a.types[part]
		if actual.Id != colExpr.Typ.Id || actual.Scale != colExpr.Typ.Scale {
			return 0, nil, false
		}
	}
	return col.ColPos, value, true
}

func compositePointScalar(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.Src == nil
	}
	// Raw parameters are not constrained by their declared width. Only the
	// total text-to-character casts used by bound comparisons supply a runtime
	// bound; casts to binary can instead raise a truncation error.
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) != 2 ||
		fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[1].GetT() == nil ||
		(fn.Args[0].GetP() == nil && fn.Args[0].GetLit() == nil) ||
		fn.Args[1].Typ.Id != expr.Typ.Id || fn.Args[1].Typ.Width != expr.Typ.Width ||
		!types.T(fn.Args[0].Typ.Id).IsMySQLString() ||
		(types.T(expr.Typ.Id) != types.T_char && types.T(expr.Typ.Id) != types.T_varchar) {
		return false
	}
	if lit := fn.Args[0].GetLit(); lit != nil && lit.Src != nil {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	return id == function.CAST && (overload == 0 || overload == 1) && expr.Typ.Width > 0
}

func (a *compositePointAnalysis) factors(expr *plan.Expr, out *[]*plan.Expr, depth int) bool {
	if expr == nil || !a.step(depth) {
		return false
	}
	if fn := expr.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "and" {
		for _, arg := range fn.Args {
			if !a.factors(arg, out, depth+1) {
				return false
			}
		}
		return true
	}
	*out = append(*out, expr)
	return true
}

// conjunction selects one independently sufficient OR, never a Cartesian
// product of sibling ORs. All arms of the chosen OR must yield a full key.
func (a *compositePointAnalysis) conjunction(exprs []*plan.Expr, inherited []*plan.Expr, arm bool, depth int) ([][]*plan.Expr, bool) {
	var factors []*plan.Expr
	for _, expr := range exprs {
		if !a.factors(expr, &factors, depth+1) {
			return nil, false
		}
	}
	facts := append([]*plan.Expr(nil), inherited...)
	for _, factor := range factors {
		if pos, value, ok := a.equality(factor); ok {
			if part, found := a.parts[pos]; found && facts[part] == nil {
				facts[part] = value
			}
		}
	}
	complete := true
	for _, fact := range facts {
		complete = complete && fact != nil
	}
	if complete {
		if arm {
			return [][]*plan.Expr{facts}, true
		}
		// The existing singleton lowering is already a point lookup.
		return nil, false
	}
	for _, factor := range factors {
		fn := factor.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "or" {
			continue
		}
		if points, ok := a.disjunction(fn, facts, depth+1); ok {
			return points, true
		}
	}
	return nil, false
}

func (a *compositePointAnalysis) disjunction(fn *plan.Function, inherited []*plan.Expr, depth int) ([][]*plan.Expr, bool) {
	if !a.step(depth) {
		return nil, false
	}
	var points [][]*plan.Expr
	for _, arg := range fn.Args {
		var branch [][]*plan.Expr
		var ok bool
		if f := arg.GetF(); f != nil && f.Func != nil && f.Func.ObjName == "or" {
			branch, ok = a.disjunction(f, inherited, depth+1)
		} else {
			branch, ok = a.conjunction([]*plan.Expr{arg}, inherited, true, depth+1)
		}
		if !ok || len(points)+len(branch) > maxCompositePoints {
			return nil, false
		}
		points = append(points, branch...)
	}
	return points, len(points) > 0
}

func hasCompositePointFilter(node *plan.Node) bool {
	if node == nil || node.TableDef == nil || node.TableDef.Pkey == nil || node.TableDef.ClusterBy != nil ||
		len(node.TableDef.Pkey.Names) < 2 || len(node.BindingTags) == 0 {
		return false
	}
	pos, ok := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
	if !ok {
		return false
	}
	for _, filter := range node.FilterList {
		fn := filter.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "in" || len(fn.Args) != 2 || fn.Args[0] == nil {
			continue
		}
		if col := fn.Args[0].GetCol(); col != nil && col.RelPos == node.BindingTags[0] && col.ColPos == pos {
			return true
		}
	}
	return false
}

// addCompositePointCandidates runs after join costing and before index
// rewriting, which can move component predicates away from the base scan.
// It publishes no partial candidate if encoding or binding is unsupported.
func (builder *QueryBuilder) addCompositePointCandidates(nodeID int32, deferredPoints map[int32]struct{}) bool {
	visited := make(map[int32]struct{})
	changed := false
	var visit func(int32)
	visit = func(id int32) {
		if _, ok := visited[id]; ok {
			return
		}
		visited[id] = struct{}{}
		node := builder.qry.Nodes[id]
		for _, child := range node.Children {
			visit(child)
		}
		if node.NodeType != plan.Node_TABLE_SCAN || hasCompositePointFilter(node) {
			return
		}
		fallback := func() {
			if _, wasDeferred := deferredPoints[id]; !wasDeferred {
				return
			}
			if compositeKeyFiltersNeedIndexSelection(node.TableDef) {
				// Restore the logical normalization the early phase would have
				// performed. Physical lowering must still wait for index selection.
				changed = builder.normalizeCompositeKeyFiltersForIndex(node) || changed
				return
			}
			partFilters, _ := builder.collectCompositePartBlockFilters(id, false)
			node.FilterList = builder.doMergeFiltersOnCompositeKey(true, node.TableDef, node.BindingTags[0], node.FilterList...)
			builder.retainConsumedCompositePartBlockFilters(partFilters)
			builder.appendCompositePartBlockFilters(partFilters)
			builder.appendCompoundKeyBlockFilters(id)
			changed = true
		}
		points := compositePointCandidate(node)
		if len(points) == 0 {
			fallback()
			return
		}
		pos := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
		key := &plan.Expr{Typ: node.TableDef.Cols[pos].Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: node.BindingTags[0], ColPos: pos, Name: node.TableDef.Pkey.PkeyColName,
		}}}
		values := make([]*plan.Expr, len(points))
		for i, point := range points {
			// The scan compiler folds the whole IN list. Folding every tuple here
			// would copy and visit it once more, including prepared parameters.
			value, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", DeepCopyExprList(point))
			if err != nil {
				fallback()
				return
			}
			values[i] = value
		}
		keyType := makeTypeByPlan2Expr(key)
		overload, err := function.GetFunctionByName(builder.GetContext(), "in", []types.Type{keyType, keyType})
		if err != nil {
			fallback()
			return
		}
		overloadID := overload.GetEncodedOverloadID()
		resultType := overload.GetReturnType()
		args := []*plan.Expr{key, {Typ: key.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{List: values}}}}
		typ := makePlan2Type(&resultType)
		typ.NotNullable = function.DeduceNotNullable(overloadID, args)
		// DML performs another statistics pass after createQuery. This is a
		// necessary consequence of the residual, not an independent filter.
		candidate := &plan.Expr{Typ: typ, Selectivity: 1, Expr: &plan.Expr_F{F: &plan.Function{
			Func: getFunctionObjRef(overloadID, "in"), Args: args,
		}}}
		node.FilterList = append(node.FilterList, candidate)
		changed = true
		if (builder.optimizerHints == nil || builder.optimizerHints.blockFilter != 2) &&
			ExprIsZonemappable(builder.GetContext(), candidate) {
			appendUniqueBlockFilter(node, candidate, true)
		}
	}
	visit(nodeID)
	return changed
}
