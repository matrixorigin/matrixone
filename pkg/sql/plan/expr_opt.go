// Copyright 2024 Matrix Origin
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
	"context"
	"fmt"
	"hash/fnv"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func (builder *QueryBuilder) mergeFiltersOnCompositeKey(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_TABLE_SCAN {
		for _, childID := range node.Children {
			builder.mergeFiltersOnCompositeKey(childID)
		}

		return
	}

	if node.TableDef.Pkey == nil {
		return
	}

	newFilterList := builder.doMergeFiltersOnCompositeKey(node.TableDef, node.BindingTags[0], node.FilterList...)
	node.FilterList = newFilterList
	node.Stats = calcScanStats(node, builder)
	resetHashMapStats(node.Stats)
}

// collectCompositePartBlockFilters preserves zonemappable predicates on the
// physical columns that make up a composite primary/cluster key.  The regular
// filter rewrite replaces a useful equality/range prefix with one predicate on
// the hidden compound column.  That compound predicate is ideal for the read
// filter and sorted-key pruning, but ranges can additionally prune blocks with
// the zonemap of every constituent column.
type compositePartBlockFilter struct {
	original *plan.Expr
	copy     *plan.Expr
}

func (builder *QueryBuilder) collectCompositePartBlockFilters(nodeID int32) map[int32][]compositePartBlockFilter {
	ret := make(map[int32][]compositePartBlockFilter)
	if builder.optimizerHints != nil && builder.optimizerHints.blockFilter == 2 {
		return ret
	}
	visited := make(map[int32]struct{})
	var visit func(int32)
	visit = func(id int32) {
		if _, ok := visited[id]; ok {
			return
		}
		visited[id] = struct{}{}
		node := builder.qry.Nodes[id]
		if node.NodeType != plan.Node_TABLE_SCAN {
			for _, childID := range node.Children {
				visit(childID)
			}
			return
		}

		partCols := compositeKeyPartColumns(node.TableDef)
		if len(partCols) < 2 || len(node.BindingTags) == 0 {
			return
		}
		filters := make([]*plan.Expr, 0, len(node.FilterList)+len(node.BlockFilterList))
		filters = append(filters, node.FilterList...)
		// A high-cost/AP optimization can revisit an already rewritten scan.  In
		// that case the constituent predicates only survive in BlockFilterList;
		// preserve them before the next stats pass rebuilds that list.
		filters = append(filters, node.BlockFilterList...)
		for _, filter := range filters {
			if ExprIsZonemappable(builder.GetContext(), filter) &&
				exprOnlyReferencesColumns(filter, node.BindingTags[0], partCols) {
				ret[id] = append(ret[id], compositePartBlockFilter{original: filter, copy: DeepCopyExpr(filter)})
			}
		}
	}
	visit(nodeID)
	return ret
}

func compositeKeyPartColumns(tableDef *plan.TableDef) map[int32]struct{} {
	if tableDef == nil {
		return nil
	}
	var parts []string
	if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		parts = util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
	} else if tableDef.Pkey != nil && len(tableDef.Pkey.Names) > 1 {
		parts = tableDef.Pkey.Names
	}
	if len(parts) < 2 {
		return nil
	}
	cols := make(map[int32]struct{}, len(parts))
	for _, part := range parts {
		if pos, ok := tableDef.Name2ColIndex[part]; ok {
			cols[pos] = struct{}{}
		}
	}
	return cols
}

// exprOnlyReferencesColumns accepts constants nested in the predicate but
// requires at least one column and rejects any column outside allowed.
func exprOnlyReferencesColumns(expr *plan.Expr, tableTag int32, allowed map[int32]struct{}) bool {
	found := false
	var visit func(*plan.Expr) bool
	visit = func(e *plan.Expr) bool {
		if e == nil {
			return true
		}
		switch impl := e.Expr.(type) {
		case *plan.Expr_Col:
			if impl.Col.RelPos != tableTag {
				return false
			}
			if _, ok := allowed[impl.Col.ColPos]; !ok {
				return false
			}
			found = true
			return true
		case *plan.Expr_F:
			for _, arg := range impl.F.Args {
				if !visit(arg) {
					return false
				}
			}
			return true
		case *plan.Expr_List:
			for _, item := range impl.List.List {
				if !visit(item) {
					return false
				}
			}
			return true
		case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Raw,
			*plan.Expr_Vec, *plan.Expr_Max, *plan.Expr_T, *plan.Expr_Fold:
			return true
		default:
			return false
		}
	}
	return visit(expr) && found
}

func (builder *QueryBuilder) appendCompositePartBlockFilters(filters map[int32][]compositePartBlockFilter) {
	for nodeID, candidates := range filters {
		node := builder.qry.Nodes[nodeID]
		for _, candidate := range candidates {
			duplicate := false
			for _, existing := range node.BlockFilterList {
				if blockFilterEquivalent(existing, candidate.copy) {
					duplicate = true
					break
				}
			}
			if !duplicate {
				node.BlockFilterList = append(node.BlockFilterList, candidate.copy)
			}
		}
	}
}

// retainConsumedCompositePartBlockFilters keeps only predicates removed by the
// compound-key rewrite.  Predicates that remain in FilterList will be handled
// by the later date/LIKE rewrites and ReCalcNodeStats; re-appending their stale
// pre-rewrite form could disable the ranges fast compiler for the entire list.
func (builder *QueryBuilder) retainConsumedCompositePartBlockFilters(filters map[int32][]compositePartBlockFilter) {
	for nodeID, candidates := range filters {
		node := builder.qry.Nodes[nodeID]
		remaining := node.FilterList
		if !hasCompoundKeyFilter(remaining, node) {
			delete(filters, nodeID)
			continue
		}
		partCols := compositeKeyPartColumns(node.TableDef)
		tableTag := node.BindingTags[0]
		kept := candidates[:0]
		for _, candidate := range candidates {
			stillPresent := false
			for _, filter := range remaining {
				if candidate.original == filter && exprOnlyReferencesColumns(filter, tableTag, partCols) {
					stillPresent = true
					break
				}
			}
			if !stillPresent {
				kept = append(kept, candidate)
			}
		}
		if len(kept) == 0 {
			delete(filters, nodeID)
		} else {
			filters[nodeID] = kept
		}
	}
}

func hasCompoundKeyFilter(filters []*plan.Expr, node *plan.Node) bool {
	if node.TableDef == nil || len(node.BindingTags) == 0 {
		return false
	}
	compoundPos, ok := compoundKeyColumn(node.TableDef)
	if !ok {
		return false
	}
	allowed := map[int32]struct{}{compoundPos: {}}
	for _, filter := range filters {
		if exprOnlyReferencesColumns(filter, node.BindingTags[0], allowed) {
			return true
		}
	}
	return false
}

func compoundKeyColumn(tableDef *plan.TableDef) (int32, bool) {
	if tableDef == nil {
		return 0, false
	}
	compoundName := ""
	if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		compoundName = tableDef.ClusterBy.Name
	} else if tableDef.Pkey != nil && len(tableDef.Pkey.Names) > 1 {
		compoundName = tableDef.Pkey.PkeyColName
	}
	if compoundName == "" {
		return 0, false
	}
	pos, ok := tableDef.Name2ColIndex[compoundName]
	return pos, ok
}

// appendCompoundKeyBlockFilters keeps the generated compound predicate in the
// block-pruning set even when estimates are unavailable or selectivity rounds
// to one.  Compound-key filters are sort-key predicates and use the ranges fast
// path, so skipping them loses the cheapest pruning opportunity.
func (builder *QueryBuilder) appendCompoundKeyBlockFilters(nodeID int32) {
	if builder.optimizerHints != nil && builder.optimizerHints.blockFilter == 2 {
		return
	}
	visited := make(map[int32]struct{})
	var visit func(int32)
	visit = func(id int32) {
		if _, ok := visited[id]; ok {
			return
		}
		visited[id] = struct{}{}
		node := builder.qry.Nodes[id]
		if node.NodeType != plan.Node_TABLE_SCAN {
			for _, childID := range node.Children {
				visit(childID)
			}
			return
		}
		if node.TableDef == nil || len(node.BindingTags) == 0 {
			return
		}
		compoundPos, ok := compoundKeyColumn(node.TableDef)
		if !ok {
			return
		}
		allowed := map[int32]struct{}{compoundPos: {}}
		for _, filter := range node.FilterList {
			if !ExprIsZonemappable(builder.GetContext(), filter) ||
				!exprOnlyReferencesColumns(filter, node.BindingTags[0], allowed) {
				continue
			}
			duplicate := false
			for _, existing := range node.BlockFilterList {
				if blockFilterEquivalent(existing, filter) {
					duplicate = true
					break
				}
			}
			if !duplicate {
				node.BlockFilterList = append(node.BlockFilterList, DeepCopyExpr(filter))
			}
		}
	}
	visit(nodeID)
}

func existingCompositeBlockFilters(node *plan.Node) []*plan.Expr {
	if node.TableDef == nil || len(node.BindingTags) == 0 {
		return nil
	}
	allowed := compositeKeyPartColumns(node.TableDef)
	if allowed == nil {
		return nil
	}
	if compoundPos, ok := compoundKeyColumn(node.TableDef); ok {
		allowed[compoundPos] = struct{}{}
	}
	ret := node.BlockFilterList[:0]
	for _, filter := range node.BlockFilterList {
		if exprOnlyReferencesColumns(filter, node.BindingTags[0], allowed) {
			// calcScanStats replaces BlockFilterList at the end of the pass and
			// does not mutate preserved entries. Transfer the already-independent
			// block-filter expression instead of deep-copying it on every stats
			// recalculation; the initial FilterList boundary is still copied.
			ret = append(ret, filter)
		}
	}
	return ret
}

func blockFilterEquivalent(left, right *plan.Expr) bool {
	return exprStructuralEqual(left, right)
}

// blockFilterSemanticallyEquivalent handles the two representation changes a
// block predicate can undergo between stats passes: constant IN lists are
// folded into vectors, and logical expressions can be flattened. Keep this
// deliberately narrower than general expression equivalence so deduplication
// can never discard two different range predicates on the same column.
func blockFilterSemanticallyEquivalent(left, right *plan.Expr) bool {
	if exprStructuralEqual(left, right) {
		return true
	}
	if left == nil || right == nil {
		return false
	}
	leftFn, rightFn := left.GetF(), right.GetF()
	if leftFn == nil || rightFn == nil || leftFn.Func == nil || rightFn.Func == nil ||
		leftFn.Func.ObjName != rightFn.Func.ObjName {
		return false
	}

	switch leftFn.Func.ObjName {
	case "and", "or":
		var leftArgs, rightArgs []*plan.Expr
		flattenLogicalExpressions(left, leftFn.Func.ObjName, &leftArgs)
		flattenLogicalExpressions(right, rightFn.Func.ObjName, &rightArgs)
		if len(leftArgs) != len(rightArgs) {
			return false
		}
		// Flattening preserves operand order. Comparing positionally keeps this
		// linear for large OR trees and avoids a matched-object allocation.
		for i := range leftArgs {
			if !blockFilterSemanticallyEquivalent(leftArgs[i], rightArgs[i]) {
				return false
			}
		}
		return true
	case "in", "not_in", "prefix_in":
		if len(leftFn.Args) != 2 || len(rightFn.Args) != 2 ||
			!exprStructuralEqual(leftFn.Args[0], rightFn.Args[0]) {
			return false
		}
		return blockFilterConstantSetsEqual(leftFn.Args[1], rightFn.Args[1])
	default:
		return false
	}
}

func blockFilterConstantSetsEqual(left, right *plan.Expr) bool {
	// Build value maps only after structural equality failed. The one-time O(n)
	// comparison is cheaper than retaining an O(n) IN predicate for every block.
	leftSet, ok := blockFilterConstantSet(left)
	if !ok {
		return false
	}
	rightSet, ok := blockFilterConstantSet(right)
	if !ok || len(leftSet) != len(rightSet) {
		return false
	}
	for key := range leftSet {
		if _, ok = rightSet[key]; !ok {
			return false
		}
	}
	return true
}

func blockFilterConstantSet(expr *plan.Expr) (map[string]struct{}, bool) {
	if expr == nil {
		return nil, false
	}
	if list := expr.GetList(); list != nil {
		ret := make(map[string]struct{}, len(list.List))
		for _, item := range list.List {
			lit, typ, ok := blockFilterConstLiteral(item)
			if !ok {
				return nil, false
			}
			key, ok := blockFilterLiteralKey(lit, typ)
			if !ok {
				return nil, false
			}
			ret[key] = struct{}{}
		}
		return ret, true
	}

	return blockFilterConstantVectorSet(expr.GetVec())
}

func blockFilterConstantVectorSet(literalVec *plan.LiteralVec) (ret map[string]struct{}, ok bool) {
	defer func() {
		if recover() != nil {
			ret = nil
			ok = false
		}
	}()

	vec, physicalLength, ok := decodeLiteralVec(literalVec)
	if !ok {
		return nil, false
	}
	typ := plan.Type{
		Id:    int32(vec.GetType().Oid),
		Scale: vec.GetType().Scale,
		Width: vec.GetType().Width,
	}
	if physicalLength == 0 {
		return make(map[string]struct{}), true
	}
	ret = make(map[string]struct{}, physicalLength)
	for i := 0; i < physicalLength; i++ {
		lit := rule.GetConstantValue(&vec, true, uint64(i))
		key, ok := blockFilterLiteralKey(lit, typ)
		if !ok {
			return nil, false
		}
		ret[key] = struct{}{}
	}
	return ret, true
}

// inRHSValues returns the individual constants represented by an IN RHS.
// Constant folding changes Expr_List into Expr_Vec, but composite-key rewrites
// need individual values to serialize each compound key. A serialized vector
// must match the comparison column's type: its payload type is executable data,
// so accepting inconsistent plan metadata could encode a different compound key.
func inRHSValues(expr *plan.Expr, expectedType plan.Type) (values []*plan.Expr, ok bool) {
	if expr == nil {
		return nil, false
	}
	if list := expr.GetList(); list != nil {
		return list.List, true
	}

	defer func() {
		if recover() != nil {
			values = nil
			ok = false
		}
	}()

	vec, physicalLength, ok := decodeLiteralVec(expr.GetVec())
	if !ok {
		return nil, false
	}
	if !literalVecMatchesType(vec.GetType(), expectedType) {
		return nil, false
	}

	typ := makePlan2Type(vec.GetType())
	values = make([]*plan.Expr, physicalLength)
	for i := range physicalLength {
		lit := rule.GetConstantValue(&vec, true, uint64(i))
		if lit == nil {
			return nil, false
		}
		literalTyp := typ
		literalTyp.NotNullable = !lit.Isnull
		values[i] = &plan.Expr{
			Typ:  literalTyp,
			Expr: &plan.Expr_Lit{Lit: lit},
		}
	}
	return values, true
}

func literalVecMatchesType(vecType *types.Type, expectedType plan.Type) bool {
	return vecType != nil &&
		int32(vecType.Oid) == expectedType.Id &&
		vecType.Width == expectedType.Width &&
		vecType.Scale == expectedType.Scale
}

// decodeLiteralVec validates the minimum structural invariants needed before
// reading a serialized literal vector. UnmarshalBinary is intentionally no-copy
// and trusts encoded lengths, so an invalid plan must disable an optimization
// rather than panic or allocate from an untrusted logical length. Callers own
// the panic boundary because malformed varlen metadata can also fail while
// materializing individual values after decoding.
func decodeLiteralVec(literalVec *plan.LiteralVec) (vec vector.Vector, physicalLength int, ok bool) {
	if literalVec == nil || literalVec.Len < 0 {
		return vector.Vector{}, 0, false
	}
	if err := vec.UnmarshalBinary(literalVec.Data); err != nil || int64(vec.Length()) != int64(literalVec.Len) {
		return vector.Vector{}, 0, false
	}

	physicalLength = vec.Length()
	if vec.IsConst() && physicalLength > 0 {
		// A const vector stores one physical value for every logical row. IN has
		// set semantics, so one value is sufficient for the compound predicate.
		return vec, 1, true
	}
	if physicalLength == 0 {
		return vec, 0, true
	}

	typeSize := vec.GetType().TypeSize()
	if typeSize <= 0 || physicalLength > len(vec.GetData())/typeSize {
		return vector.Vector{}, 0, false
	}
	return vec, physicalLength, true
}

func blockFilterConstLiteral(expr *plan.Expr) (*plan.Literal, plan.Type, bool) {
	if expr == nil {
		return nil, plan.Type{}, false
	}
	effectiveTyp := expr.Typ
	for {
		if lit := expr.GetLit(); lit != nil {
			return lit, effectiveTyp, true
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" || len(fn.Args) == 0 {
			return nil, plan.Type{}, false
		}
		expr = fn.Args[0]
	}
}

func blockFilterLiteralKey(lit *plan.Literal, typ plan.Type) (string, bool) {
	if lit == nil {
		return "", false
	}
	litBytes, err := lit.Marshal()
	if err != nil {
		return "", false
	}
	key := make([]byte, 12+len(litBytes))
	key[0] = byte(typ.Id)
	key[1] = byte(typ.Id >> 8)
	key[2] = byte(typ.Id >> 16)
	key[3] = byte(typ.Id >> 24)
	key[4] = byte(typ.Scale)
	key[5] = byte(typ.Scale >> 8)
	key[6] = byte(typ.Scale >> 16)
	key[7] = byte(typ.Scale >> 24)
	key[8] = byte(typ.Width)
	key[9] = byte(typ.Width >> 8)
	key[10] = byte(typ.Width >> 16)
	key[11] = byte(typ.Width >> 24)
	copy(key[12:], litBytes)
	return string(key), true
}

// blockFilterSemanticHash buckets expressions that can be equal under
// blockFilterSemanticallyEquivalent. Collisions are resolved with that exact
// comparison. Unsupported expression forms retain their structural hash.
func blockFilterSemanticHash(expr *plan.Expr) uint64 {
	h := fnv.New64a()
	hashBlockFilterSemanticInto(h, expr)
	return h.Sum64()
}

func hashBlockFilterSemanticInto(h writeByter, expr *plan.Expr) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		writeByte(h, 0)
		writeUint64(h, exprStructuralHash(expr))
		return
	}

	switch fn.Func.ObjName {
	case "and", "or":
		writeByte(h, 1)
		writeString(h, fn.Func.ObjName)
		var args []*plan.Expr
		flattenLogicalExpressions(expr, fn.Func.ObjName, &args)
		writeUint64(h, uint64(len(args)))
		for _, arg := range args {
			hashBlockFilterSemanticInto(h, arg)
		}
	case "in", "not_in", "prefix_in":
		if len(fn.Args) != 2 {
			writeByte(h, 0)
			writeUint64(h, exprStructuralHash(expr))
			return
		}
		set, ok := blockFilterConstantSet(fn.Args[1])
		if !ok {
			writeByte(h, 0)
			writeUint64(h, exprStructuralHash(expr))
			return
		}
		writeByte(h, 2)
		writeString(h, fn.Func.ObjName)
		writeUint64(h, exprStructuralHash(fn.Args[0]))
		writeUint64(h, uint64(len(set)))
		// Two order-independent accumulators keep key construction linear. A
		// collision only adds an exact comparison; it cannot remove a filter.
		var xor, sum uint64
		for value := range set {
			valueHash := hashString(value)
			xor ^= valueHash
			sum += valueHash
		}
		writeUint64(h, xor)
		writeUint64(h, sum)
	default:
		writeByte(h, 0)
		writeUint64(h, exprStructuralHash(expr))
	}
}

func deduplicateBlockFilterList(filters []*plan.Expr) []*plan.Expr {
	if len(filters) < 2 {
		return filters
	}
	originalLength := len(filters)
	unique := filters[:0]
	buckets := make(map[uint64][]*plan.Expr, len(filters))
	for _, filter := range filters {
		hash := blockFilterSemanticHash(filter)
		duplicate := false
		for _, existing := range buckets[hash] {
			if blockFilterSemanticallyEquivalent(existing, filter) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		unique = append(unique, filter)
		buckets[hash] = append(buckets[hash], filter)
	}
	clear(filters[len(unique):originalLength])
	return unique
}

func (builder *QueryBuilder) deduplicateBlockFilters(nodeID int32) {
	visited := make(map[int32]struct{})
	var visit func(int32)
	visit = func(id int32) {
		if _, ok := visited[id]; ok {
			return
		}
		visited[id] = struct{}{}
		node := builder.qry.Nodes[id]
		for _, childID := range node.Children {
			visit(childID)
		}
		if len(node.BlockFilterList) < 2 {
			return
		}
		node.BlockFilterList = deduplicateBlockFilterList(node.BlockFilterList)
	}
	visit(nodeID)
}

func (builder *QueryBuilder) rewriteInDomainNotInFilters(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType != plan.Node_TABLE_SCAN {
		for _, childID := range node.Children {
			builder.rewriteInDomainNotInFilters(childID)
		}
		return
	}
	if len(node.FilterList) == 0 {
		return
	}

	node.FilterList = builder.normalizeColumnDomain(node.FilterList)
}

type domainKind int

const (
	domainOther domainKind = iota
	domainPointIn
	domainPointOut
	domainBetween
	domainIsNull
	domainIsNotNull
)

type orderedValueSet struct {
	keys   map[string]int
	values []*plan.Expr
}

func newOrderedValueSet() *orderedValueSet {
	return &orderedValueSet{keys: make(map[string]int)}
}

func (s *orderedValueSet) add(key string, value *plan.Expr) {
	if _, ok := s.keys[key]; ok {
		return
	}
	s.keys[key] = len(s.values)
	s.values = append(s.values, value)
}

func (s *orderedValueSet) intersect(other map[string]struct{}) *orderedValueSet {
	result := newOrderedValueSet()
	for _, value := range s.values {
		key, ok := constLiteralKey(value)
		if !ok {
			continue
		}
		if _, hit := other[key]; hit {
			result.add(key, value)
		}
	}
	return result
}

func (s *orderedValueSet) difference(other map[string]struct{}) []*plan.Expr {
	diff := make([]*plan.Expr, 0, len(s.values))
	for _, value := range s.values {
		key, ok := constLiteralKey(value)
		if !ok {
			return nil
		}
		if _, excluded := other[key]; excluded {
			continue
		}
		diff = append(diff, DeepCopyExpr(value))
	}
	return diff
}

func (s *orderedValueSet) keySet() map[string]struct{} {
	out := make(map[string]struct{}, len(s.keys))
	for k := range s.keys {
		out[k] = struct{}{}
	}
	return out
}

type columnDomain struct {
	colExpr       *plan.Expr
	pointsIn      *orderedValueSet
	pointsInInit  bool
	pointsInIdxs  []int
	pointsOut     *orderedValueSet
	pointsOutIdxs []int
	hasRange      bool
	isNullSeen    bool
}

func (builder *QueryBuilder) normalizeColumnDomain(filters []*plan.Expr) []*plan.Expr {
	var conjuncts []*plan.Expr
	for _, filter := range filters {
		flattenLogicalExpressions(filter, "and", &conjuncts)
	}

	kinds := make([]domainKind, len(conjuncts))
	domains := make(map[[2]int32]*columnDomain)
	for idx, conjunct := range conjuncts {
		col, kind, vs := classifyDomainConjunct(conjunct)
		kinds[idx] = kind
		if col == nil || kind == domainOther {
			continue
		}
		ref := col.GetCol()
		key := [2]int32{ref.RelPos, ref.ColPos}
		domain := domains[key]
		if domain == nil {
			domain = &columnDomain{colExpr: DeepCopyExpr(col)}
			domains[key] = domain
		}
		switch kind {
		case domainPointIn:
			if !domain.pointsInInit {
				domain.pointsIn = newOrderedValueSet()
				for _, v := range vs {
					k, _ := constLiteralKey(v)
					domain.pointsIn.add(k, DeepCopyExpr(v))
				}
				domain.pointsInInit = true
			} else {
				incoming := make(map[string]struct{}, len(vs))
				for _, v := range vs {
					k, _ := constLiteralKey(v)
					incoming[k] = struct{}{}
				}
				domain.pointsIn = domain.pointsIn.intersect(incoming)
			}
			domain.pointsInIdxs = append(domain.pointsInIdxs, idx)
		case domainPointOut:
			if domain.pointsOut == nil {
				domain.pointsOut = newOrderedValueSet()
			}
			for _, v := range vs {
				k, _ := constLiteralKey(v)
				domain.pointsOut.add(k, DeepCopyExpr(v))
			}
			domain.pointsOutIdxs = append(domain.pointsOutIdxs, idx)
		case domainBetween:
			domain.hasRange = true
		case domainIsNull:
			domain.isNullSeen = true
		}
	}

	toDrop := make(map[int]struct{})
	appended := make([]*plan.Expr, 0, len(domains))
	changed := false

	for _, domain := range domains {
		if domain.isNullSeen && domain.pointsInInit {
			return []*plan.Expr{MakeFalseExpr()}
		}
		if !domain.pointsInInit {
			continue
		}
		if domain.hasRange {
			continue
		}
		if domain.pointsIn.keys != nil && len(domain.pointsIn.values) == 0 {
			return []*plan.Expr{MakeFalseExpr()}
		}

		col := domain.colExpr
		var outKeys map[string]struct{}
		if domain.pointsOut != nil {
			outKeys = domain.pointsOut.keySet()
		}

		mergePointsIn := len(domain.pointsInIdxs) > 1
		if outKeys == nil && !mergePointsIn {
			continue
		}

		keep := domain.pointsIn.difference(outKeys)
		if keep == nil {
			continue
		}

		for _, i := range domain.pointsInIdxs {
			toDrop[i] = struct{}{}
		}
		for _, i := range domain.pointsOutIdxs {
			toDrop[i] = struct{}{}
		}

		if len(keep) == 0 {
			return []*plan.Expr{MakeFalseExpr()}
		}

		newExpr, err := buildColumnDomainExpr(builder.GetContext(), col, keep)
		if err != nil {
			for _, i := range domain.pointsInIdxs {
				delete(toDrop, i)
			}
			for _, i := range domain.pointsOutIdxs {
				delete(toDrop, i)
			}
			continue
		}
		appended = append(appended, newExpr)
		changed = true
	}

	// Propagate domains into non-domain conjuncts (OR branches, inner NOTs).
	inheritedDomains := make(map[[2]int32]*inFilterDomain)
	for key, domain := range domains {
		if !domain.pointsInInit || domain.pointsIn == nil || len(domain.pointsIn.values) == 0 {
			continue
		}
		inheritedDomains[key] = &inFilterDomain{
			colExpr: DeepCopyExpr(domain.colExpr),
			values:  append([]*plan.Expr{}, domain.pointsIn.values...),
		}
	}
	propagatedChanged := false
	if len(inheritedDomains) > 0 {
		for idx, conjunct := range conjuncts {
			if _, drop := toDrop[idx]; drop {
				continue
			}
			if kinds[idx] != domainOther {
				continue
			}
			rewritten, rewritten2 := builder.rewriteExprByInDomains(conjunct, inheritedDomains)
			if rewritten2 {
				conjuncts[idx] = rewritten
				changed = true
				propagatedChanged = true
			}
		}
	}

	if !changed {
		return filters
	}

	newFilters := make([]*plan.Expr, 0, len(conjuncts)+len(appended)-len(toDrop))
	for idx, conjunct := range conjuncts {
		if _, drop := toDrop[idx]; drop {
			continue
		}
		newFilters = append(newFilters, conjunct)
	}
	newFilters = append(newFilters, appended...)
	if len(newFilters) == 0 {
		return []*plan.Expr{makePlan2BoolConstExprWithType(true)}
	}
	if propagatedChanged {
		return builder.normalizeColumnDomain(newFilters)
	}
	return newFilters
}

// inFilterDomain carries a column's constant IN set for propagation into
// OR branches or nested NOT expressions.
type inFilterDomain struct {
	colExpr *plan.Expr
	values  []*plan.Expr
}

type domainFilterOperand struct {
	colExpr *plan.Expr
	relPos  int32
	colPos  int32
	castTyp plan.Type
	hasCast bool
}

// rewriteExprByInDomains rewrites NOT IN / <> under an outer IN domain.
// Returns the (possibly rewritten) expression and whether any rewrite happened.
func (builder *QueryBuilder) rewriteExprByInDomains(
	expr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) (*plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil {
		return expr, false
	}

	switch fn.Func.ObjName {
	case "in":
		if rewritten := builder.rewriteInFuncByDomains(expr, domains); rewritten != nil {
			return rewritten, true
		}
		return expr, false
	case "=":
		if rewritten := builder.rewriteEqualByDomains(expr, domains); rewritten != nil {
			return rewritten, true
		}
		return expr, false
	case "not":
		if len(fn.Args) != 1 {
			return expr, false
		}
		if rewritten := builder.rewriteNotInByDomains(fn.Args[0], domains); rewritten != nil {
			return rewritten, true
		}
		newArg, changed := builder.rewriteExprByInDomains(fn.Args[0], domains)
		if changed {
			fn.Args[0] = newArg
		}
		return expr, changed
	case "not_in":
		if rewritten := builder.rewriteNotInFuncByDomains(expr, domains); rewritten != nil {
			return rewritten, true
		}
		return expr, false
	case "!=", "<>":
		if rewritten := builder.rewriteNotEqualByDomains(expr, domains); rewritten != nil {
			return rewritten, true
		}
		return expr, false
	case "and":
		anyChanged := false
		for idx, arg := range fn.Args {
			newArg, changed := builder.rewriteExprByInDomains(arg, domains)
			if changed {
				fn.Args[idx] = newArg
				anyChanged = true
			}
		}
		newExpr, changed := builder.mergeInsInAnd(expr)
		return newExpr, anyChanged || changed
	case "or":
		anyChanged := false
		for idx, arg := range fn.Args {
			newArg, changed := builder.rewriteExprByInDomains(arg, domains)
			if changed {
				fn.Args[idx] = newArg
				anyChanged = true
			}
		}
		newExpr, changed := builder.mergeEqualsInOr(expr)
		return newExpr, anyChanged || changed
	default:
		anyChanged := false
		for idx, arg := range fn.Args {
			newArg, changed := builder.rewriteExprByInDomains(arg, domains)
			if changed {
				fn.Args[idx] = newArg
				anyChanged = true
			}
		}
		return expr, anyChanged
	}
}

func (builder *QueryBuilder) rewriteNotInByDomains(
	inExpr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) *plan.Expr {
	operand, listValues, ok := extractInListFilterForDomain(inExpr)
	if !ok {
		return nil
	}
	domain := domains[[2]int32{operand.relPos, operand.colPos}]
	if domain == nil {
		return nil
	}
	return builder.buildDomainDifferenceExpr(domain, listValues, operand)
}

func (builder *QueryBuilder) rewriteInFuncByDomains(
	inExpr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) *plan.Expr {
	operand, listValues, ok := extractInListFilterForDomain(inExpr)
	if !ok {
		return nil
	}
	if !operand.hasCast {
		return nil
	}
	domain := domains[[2]int32{operand.relPos, operand.colPos}]
	if domain == nil {
		return nil
	}
	return builder.buildDomainIntersectionExpr(domain, listValues, operand)
}

func (builder *QueryBuilder) rewriteNotInFuncByDomains(
	notInExpr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) *plan.Expr {
	operand, listValues, ok := extractNotInListFilterForDomain(notInExpr)
	if !ok {
		return nil
	}
	domain := domains[[2]int32{operand.relPos, operand.colPos}]
	if domain == nil {
		return nil
	}
	return builder.buildDomainDifferenceExpr(domain, listValues, operand)
}

// rewriteNotEqualByDomains rewrites a bare `col <> v` under an outer IN(S)
// domain into `col IN (S \ {v})`. Returns nil when the column has no active
// IN domain or v is not a literal.
func (builder *QueryBuilder) rewriteNotEqualByDomains(
	expr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) *plan.Expr {
	operand, value, ok := extractNotEqualConstForDomain(expr)
	if !ok {
		return nil
	}
	domain := domains[[2]int32{operand.relPos, operand.colPos}]
	if domain == nil {
		return nil
	}
	return builder.buildDomainDifferenceExpr(domain, []*plan.Expr{value}, operand)
}

func (builder *QueryBuilder) rewriteEqualByDomains(
	expr *plan.Expr,
	domains map[[2]int32]*inFilterDomain,
) *plan.Expr {
	operand, value, ok := extractEqualConstForDomain(expr)
	if !ok {
		return nil
	}
	if !operand.hasCast {
		return nil
	}
	domain := domains[[2]int32{operand.relPos, operand.colPos}]
	if domain == nil {
		return nil
	}
	return builder.buildDomainIntersectionExpr(domain, []*plan.Expr{value}, operand)
}

// mergeInsInAnd collapses multiple `col IN (...)` siblings under an AND into a
// single IN with the intersection of their point sets. Runs after child
// rewrites have turned NOT IN / <> / not_in into IN, which can leave several
// equivalent IN filters on the same column.
func (builder *QueryBuilder) mergeInsInAnd(expr *plan.Expr) (*plan.Expr, bool) {
	conjuncts := make([]*plan.Expr, 0, 4)
	flattenLogicalExpressions(expr, "and", &conjuncts)
	if len(conjuncts) < 2 {
		return expr, false
	}

	type inGroup struct {
		positions  []int
		colExpr    *plan.Expr
		values     *orderedValueSet
		firstInit  bool
		invalidate bool
	}
	groups := make(map[[2]int32]*inGroup)
	groupOrder := make([][2]int32, 0)
	for idx, conjunct := range conjuncts {
		colExpr, listValues, ok := extractInListFilter(conjunct)
		if !ok {
			continue
		}
		ref := colExpr.GetCol()
		colKey := [2]int32{ref.RelPos, ref.ColPos}
		group := groups[colKey]
		if group == nil {
			group = &inGroup{colExpr: DeepCopyExpr(colExpr)}
			groups[colKey] = group
			groupOrder = append(groupOrder, colKey)
		}
		if group.invalidate {
			continue
		}
		_, keys, ok := constLiteralListValues(listValues)
		if !ok {
			group.invalidate = true
			continue
		}
		if !group.firstInit {
			group.values = newOrderedValueSet()
			for _, v := range listValues {
				k, _ := constLiteralKey(v)
				group.values.add(k, DeepCopyExpr(v))
			}
			group.firstInit = true
		} else {
			group.values = group.values.intersect(keys)
		}
		group.positions = append(group.positions, idx)
	}

	replaced := false
	skip := make(map[int]struct{})
	extras := make([]*plan.Expr, 0)
	for _, colKey := range groupOrder {
		group := groups[colKey]
		if group.invalidate || !group.firstInit || len(group.positions) < 2 {
			continue
		}
		if len(group.values.values) == 0 {
			return MakeFalseExpr(), true
		}
		merged, err := buildColumnDomainExpr(builder.GetContext(), group.colExpr, group.values.values)
		if err != nil {
			continue
		}
		for _, pos := range group.positions {
			skip[pos] = struct{}{}
		}
		extras = append(extras, merged)
		replaced = true
	}
	if !replaced {
		return expr, false
	}

	newConjuncts := make([]*plan.Expr, 0, len(conjuncts)+len(extras)-len(skip))
	for idx, conjunct := range conjuncts {
		if _, ok := skip[idx]; ok {
			continue
		}
		newConjuncts = append(newConjuncts, conjunct)
	}
	newConjuncts = append(newConjuncts, extras...)
	if len(newConjuncts) == 0 {
		return makePlan2BoolConstExprWithType(true), true
	}
	newExpr, err := combinePlanConjunction(builder.GetContext(), newConjuncts)
	if err != nil {
		return expr, false
	}
	return newExpr, true
}

// mergeEqualsInOr collapses `col = v1 OR col = v2 ...` into `col IN (...)`.
// It runs after cast-aware rewrites, where a cast IN list may have been
// represented as many equality disjuncts by the binder.
func (builder *QueryBuilder) mergeEqualsInOr(expr *plan.Expr) (*plan.Expr, bool) {
	disjuncts := make([]*plan.Expr, 0, 4)
	flattenLogicalExpressions(expr, "or", &disjuncts)
	if len(disjuncts) < 2 {
		return expr, false
	}

	type equalGroup struct {
		positions []int
		colExpr   *plan.Expr
		values    *orderedValueSet
	}
	groups := make(map[[2]int32]*equalGroup)
	groupOrder := make([][2]int32, 0)
	skip := make(map[int]struct{})
	changed := false
	for idx, disjunct := range disjuncts {
		if IsFalseExpr(disjunct) {
			skip[idx] = struct{}{}
			changed = true
			continue
		}
		colExpr, value, ok := extractEqualConst(disjunct)
		if !ok {
			continue
		}
		key, ok := constLiteralKey(value)
		if !ok {
			continue
		}
		ref := colExpr.GetCol()
		colKey := [2]int32{ref.RelPos, ref.ColPos}
		group := groups[colKey]
		if group == nil {
			group = &equalGroup{colExpr: DeepCopyExpr(colExpr), values: newOrderedValueSet()}
			groups[colKey] = group
			groupOrder = append(groupOrder, colKey)
		}
		group.positions = append(group.positions, idx)
		group.values.add(key, DeepCopyExpr(value))
	}

	extras := make([]*plan.Expr, 0)
	for _, colKey := range groupOrder {
		group := groups[colKey]
		if len(group.positions) < 2 {
			continue
		}
		merged, err := buildColumnDomainExpr(builder.GetContext(), group.colExpr, group.values.values)
		if err != nil {
			continue
		}
		for _, pos := range group.positions {
			skip[pos] = struct{}{}
		}
		extras = append(extras, merged)
		changed = true
	}
	if !changed {
		return expr, false
	}

	newDisjuncts := make([]*plan.Expr, 0, len(disjuncts)+len(extras)-len(skip))
	for idx, disjunct := range disjuncts {
		if _, ok := skip[idx]; ok {
			continue
		}
		newDisjuncts = append(newDisjuncts, disjunct)
	}
	newDisjuncts = append(newDisjuncts, extras...)
	if len(newDisjuncts) == 0 {
		return MakeFalseExpr(), true
	}
	newExpr, err := combinePlanDisjunction(builder.GetContext(), newDisjuncts)
	if err != nil {
		return expr, false
	}
	return newExpr, true
}

func extractEqualConst(expr *plan.Expr) (*plan.Expr, *plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 || fn.Func.ObjName != "=" {
		return nil, nil, false
	}
	if fn.Args[0].GetCol() != nil && fn.Args[1].GetLit() != nil {
		return fn.Args[0], fn.Args[1], true
	}
	if fn.Args[1].GetCol() != nil && fn.Args[0].GetLit() != nil {
		return fn.Args[1], fn.Args[0], true
	}
	return nil, nil, false
}

func extractNotEqualConstForDomain(expr *plan.Expr) (*domainFilterOperand, *plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 {
		return nil, nil, false
	}
	if fn.Func.ObjName != "!=" && fn.Func.ObjName != "<>" {
		return nil, nil, false
	}
	if operand, ok := extractDomainFilterOperand(fn.Args[0]); ok && fn.Args[1].GetLit() != nil {
		return operand, fn.Args[1], true
	}
	if operand, ok := extractDomainFilterOperand(fn.Args[1]); ok && fn.Args[0].GetLit() != nil {
		return operand, fn.Args[0], true
	}
	return nil, nil, false
}

func extractEqualConstForDomain(expr *plan.Expr) (*domainFilterOperand, *plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 || fn.Func.ObjName != "=" {
		return nil, nil, false
	}
	if operand, ok := extractDomainFilterOperand(fn.Args[0]); ok && fn.Args[1].GetLit() != nil {
		return operand, fn.Args[1], true
	}
	if operand, ok := extractDomainFilterOperand(fn.Args[1]); ok && fn.Args[0].GetLit() != nil {
		return operand, fn.Args[0], true
	}
	return nil, nil, false
}

func (builder *QueryBuilder) buildDomainDifferenceExpr(
	domain *inFilterDomain,
	excludeValues []*plan.Expr,
	operand *domainFilterOperand,
) *plan.Expr {
	excludeKeys, ok := constLiteralKeysForOperand(excludeValues, operand)
	if !ok {
		return nil
	}
	diffValues := make([]*plan.Expr, 0, len(domain.values))
	overlap := false
	seen := make(map[string]struct{}, len(domain.values))
	for _, value := range domain.values {
		key, ok := constLiteralKeyForOperand(value, operand)
		if !ok {
			return nil
		}
		if _, exists := seen[key]; exists {
			return nil
		}
		seen[key] = struct{}{}
		if _, excluded := excludeKeys[key]; excluded {
			overlap = true
			continue
		}
		diffValues = append(diffValues, DeepCopyExpr(value))
	}
	if !overlap {
		return nil
	}
	if len(diffValues) == 0 {
		return MakeFalseExpr()
	}
	expr, err := buildColumnDomainExpr(builder.GetContext(), domain.colExpr, diffValues)
	if err != nil {
		return nil
	}
	return expr
}

func (builder *QueryBuilder) buildDomainIntersectionExpr(
	domain *inFilterDomain,
	includeValues []*plan.Expr,
	operand *domainFilterOperand,
) *plan.Expr {
	includeKeys, ok := constLiteralKeysForOperand(includeValues, operand)
	if !ok {
		return nil
	}
	keepValues := make([]*plan.Expr, 0, len(domain.values))
	seen := make(map[string]struct{}, len(domain.values))
	for _, value := range domain.values {
		key, ok := constLiteralKeyForOperand(value, operand)
		if !ok {
			return nil
		}
		if _, exists := seen[key]; exists {
			return nil
		}
		seen[key] = struct{}{}
		if _, included := includeKeys[key]; included {
			keepValues = append(keepValues, DeepCopyExpr(value))
		}
	}
	if len(keepValues) == len(domain.values) {
		return nil
	}
	if len(keepValues) == 0 {
		return MakeFalseExpr()
	}
	expr, err := buildColumnDomainExpr(builder.GetContext(), domain.colExpr, keepValues)
	if err != nil {
		return nil
	}
	return expr
}

func classifyDomainConjunct(expr *plan.Expr) (*plan.Expr, domainKind, []*plan.Expr) {
	fn := expr.GetF()
	if fn == nil {
		return nil, domainOther, nil
	}

	switch fn.Func.ObjName {
	case "in":
		col, listValues, ok := extractInListFilter(expr)
		if !ok {
			return nil, domainOther, nil
		}
		unique, _, ok := constLiteralListValues(listValues)
		if !ok || len(unique) == 0 {
			return nil, domainOther, nil
		}
		return col, domainPointIn, unique
	case "not_in":
		col, listValues, ok := extractNotInListFilter(expr)
		if !ok {
			return nil, domainOther, nil
		}
		unique, _, ok := constLiteralListValues(listValues)
		if !ok || len(unique) == 0 {
			return nil, domainOther, nil
		}
		return col, domainPointOut, unique
	case "not":
		if len(fn.Args) != 1 {
			return nil, domainOther, nil
		}
		child := fn.Args[0].GetF()
		if child == nil || child.Func.ObjName != "in" {
			return nil, domainOther, nil
		}
		col, listValues, ok := extractInListFilter(fn.Args[0])
		if !ok {
			return nil, domainOther, nil
		}
		unique, _, ok := constLiteralListValues(listValues)
		if !ok || len(unique) == 0 {
			return nil, domainOther, nil
		}
		return col, domainPointOut, unique
	case "=":
		return classifyDomainEquality(fn, domainPointIn)
	case "!=", "<>":
		return classifyDomainEquality(fn, domainPointOut)
	case "isnull", "is_null":
		if len(fn.Args) == 1 && fn.Args[0].GetCol() != nil {
			return fn.Args[0], domainIsNull, nil
		}
	case "isnotnull", "is_not_null":
		if len(fn.Args) == 1 && fn.Args[0].GetCol() != nil {
			return fn.Args[0], domainIsNotNull, nil
		}
	case "between":
		if len(fn.Args) == 3 && fn.Args[0].GetCol() != nil {
			return fn.Args[0], domainBetween, nil
		}
	}
	return nil, domainOther, nil
}

func classifyDomainEquality(fn *plan.Function, kind domainKind) (*plan.Expr, domainKind, []*plan.Expr) {
	if len(fn.Args) != 2 {
		return nil, domainOther, nil
	}
	var colExpr, constExpr *plan.Expr
	if fn.Args[0].GetCol() != nil {
		colExpr, constExpr = fn.Args[0], fn.Args[1]
	} else if fn.Args[1].GetCol() != nil {
		colExpr, constExpr = fn.Args[1], fn.Args[0]
	} else {
		return nil, domainOther, nil
	}
	if _, ok := constLiteralKey(constExpr); !ok {
		return nil, domainOther, nil
	}
	return colExpr, kind, []*plan.Expr{DeepCopyExpr(constExpr)}
}

func extractInListFilter(expr *plan.Expr) (*plan.Expr, []*plan.Expr, bool) {
	return extractInLikeFilter(expr, "in")
}

func extractNotInListFilter(expr *plan.Expr) (*plan.Expr, []*plan.Expr, bool) {
	return extractInLikeFilter(expr, "not_in")
}

func extractInListFilterForDomain(expr *plan.Expr) (*domainFilterOperand, []*plan.Expr, bool) {
	return extractInLikeFilterForDomain(expr, "in")
}

func extractNotInListFilterForDomain(expr *plan.Expr) (*domainFilterOperand, []*plan.Expr, bool) {
	return extractInLikeFilterForDomain(expr, "not_in")
}

func extractInLikeFilter(expr *plan.Expr, opName string) (*plan.Expr, []*plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != opName || len(fn.Args) != 2 {
		return nil, nil, false
	}
	colExpr := fn.Args[0]
	if colExpr.GetCol() == nil {
		return nil, nil, false
	}
	list := fn.Args[1].GetList()
	if list == nil {
		return nil, nil, false
	}
	return colExpr, list.List, true
}

func extractInLikeFilterForDomain(expr *plan.Expr, opName string) (*domainFilterOperand, []*plan.Expr, bool) {
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != opName || len(fn.Args) != 2 {
		return nil, nil, false
	}
	operand, ok := extractDomainFilterOperand(fn.Args[0])
	if !ok {
		return nil, nil, false
	}
	list := fn.Args[1].GetList()
	if list == nil {
		return nil, nil, false
	}
	return operand, list.List, true
}

func extractDomainFilterOperand(expr *plan.Expr) (*domainFilterOperand, bool) {
	if col := expr.GetCol(); col != nil {
		return &domainFilterOperand{
			colExpr: expr,
			relPos:  col.RelPos,
			colPos:  col.ColPos,
		}, true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != "cast" || len(fn.Args) == 0 {
		return nil, false
	}
	col := fn.Args[0].GetCol()
	if col == nil || !isSupportedIntegralDomainCast(expr.Typ) {
		return nil, false
	}
	return &domainFilterOperand{
		colExpr: expr,
		relPos:  col.RelPos,
		colPos:  col.ColPos,
		castTyp: expr.Typ,
		hasCast: true,
	}, true
}

func constLiteralListValues(values []*plan.Expr) ([]*plan.Expr, map[string]struct{}, bool) {
	keys := make(map[string]struct{}, len(values))
	uniqueValues := make([]*plan.Expr, 0, len(values))
	for _, value := range values {
		key, ok := constLiteralKey(value)
		if !ok {
			return nil, nil, false
		}
		if _, exists := keys[key]; exists {
			continue
		}
		keys[key] = struct{}{}
		uniqueValues = append(uniqueValues, DeepCopyExpr(value))
	}
	return uniqueValues, keys, true
}

func constLiteralKeysForOperand(values []*plan.Expr, operand *domainFilterOperand) (map[string]struct{}, bool) {
	keys := make(map[string]struct{}, len(values))
	for _, value := range values {
		key, ok := constLiteralKeyForOperand(value, operand)
		if !ok {
			return nil, false
		}
		keys[key] = struct{}{}
	}
	return keys, true
}

func constLiteralKey(expr *plan.Expr) (string, bool) {
	lit, typ, ok := unwrapConstLiteral(expr)
	if !ok {
		return "", false
	}
	// Serialize the literal with proto binary Marshal rather than String(),
	// which goes through the reflection-driven TextMarshaler and can dominate
	// CPU when called per-value across large IN lists.
	litBytes, err := lit.Marshal()
	if err != nil {
		return "", false
	}
	var buf [16]byte
	n := 0
	appendInt32 := func(v int32) {
		buf[n] = byte(v)
		buf[n+1] = byte(v >> 8)
		buf[n+2] = byte(v >> 16)
		buf[n+3] = byte(v >> 24)
		n += 4
	}
	appendInt32(typ.Id)
	appendInt32(typ.Width)
	appendInt32(typ.Scale)
	key := make([]byte, 0, n+len(litBytes))
	key = append(key, buf[:n]...)
	key = append(key, litBytes...)
	return string(key), true
}

func constLiteralKeyForOperand(expr *plan.Expr, operand *domainFilterOperand) (string, bool) {
	if operand == nil || !operand.hasCast {
		return constLiteralKey(expr)
	}
	lit, typ, ok := unwrapConstLiteral(expr)
	if !ok {
		return "", false
	}
	return integralCastLiteralKey(lit, typ, operand.castTyp)
}

func integralCastLiteralKey(lit *plan.Literal, typ plan.Type, target plan.Type) (string, bool) {
	normalized, ok := normalizedIntegralLiteral(lit, typ, types.T(target.Id))
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%d/%s", target.Id, normalized), true
}

func normalizedIntegralLiteral(lit *plan.Literal, typ plan.Type, target types.T) (string, bool) {
	if lit == nil || lit.Isnull || !isSupportedIntegralType(target) {
		return "", false
	}
	if s, ok := literalStringValue(lit, typ); ok {
		return normalizeStringIntegralLiteral(s, target)
	}
	if v, ok := literalSignedValue(lit); ok {
		return normalizeSignedIntegralLiteral(v, target)
	}
	if v, ok := literalUnsignedValue(lit); ok {
		return normalizeUnsignedIntegralLiteral(v, target)
	}
	return "", false
}

func literalStringValue(lit *plan.Literal, typ plan.Type) (string, bool) {
	switch types.T(typ.Id) {
	case types.T_char, types.T_varchar, types.T_text:
	default:
		return "", false
	}
	if lit.IsBin {
		return "", false
	}
	if _, ok := lit.Value.(*plan.Literal_Sval); !ok {
		return "", false
	}
	return lit.GetSval(), true
}

func literalSignedValue(lit *plan.Literal) (int64, bool) {
	switch lit.Value.(type) {
	case *plan.Literal_I8Val:
		return int64(lit.GetI8Val()), true
	case *plan.Literal_I16Val:
		return int64(lit.GetI16Val()), true
	case *plan.Literal_I32Val:
		return int64(lit.GetI32Val()), true
	case *plan.Literal_I64Val:
		return lit.GetI64Val(), true
	default:
		return 0, false
	}
}

func literalUnsignedValue(lit *plan.Literal) (uint64, bool) {
	switch lit.Value.(type) {
	case *plan.Literal_U8Val:
		return uint64(lit.GetU8Val()), true
	case *plan.Literal_U16Val:
		return uint64(lit.GetU16Val()), true
	case *plan.Literal_U32Val:
		return uint64(lit.GetU32Val()), true
	case *plan.Literal_U64Val:
		return lit.GetU64Val(), true
	default:
		return 0, false
	}
}

func normalizeStringIntegralLiteral(s string, target types.T) (string, bool) {
	if s == "" {
		return "", false
	}
	offset := 0
	if s[0] == '-' {
		if !isSignedIntegralType(target) || len(s) == 1 {
			return "", false
		}
		offset = 1
	}
	if s[offset] == '0' && len(s)-offset > 1 {
		return "", false
	}
	for i := offset; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return "", false
		}
	}
	if isSignedIntegralType(target) {
		v, err := strconv.ParseInt(s, 10, integralBitSize(target))
		if err != nil {
			return "", false
		}
		return strconv.FormatInt(v, 10), true
	}
	v, err := strconv.ParseUint(s, 10, integralBitSize(target))
	if err != nil {
		return "", false
	}
	return strconv.FormatUint(v, 10), true
}

func normalizeSignedIntegralLiteral(v int64, target types.T) (string, bool) {
	if isSignedIntegralType(target) {
		parsed, err := strconv.ParseInt(strconv.FormatInt(v, 10), 10, integralBitSize(target))
		if err != nil {
			return "", false
		}
		return strconv.FormatInt(parsed, 10), true
	}
	if v < 0 {
		return "", false
	}
	parsed, err := strconv.ParseUint(strconv.FormatInt(v, 10), 10, integralBitSize(target))
	if err != nil {
		return "", false
	}
	return strconv.FormatUint(parsed, 10), true
}

func normalizeUnsignedIntegralLiteral(v uint64, target types.T) (string, bool) {
	if isSignedIntegralType(target) {
		parsed, err := strconv.ParseInt(strconv.FormatUint(v, 10), 10, integralBitSize(target))
		if err != nil {
			return "", false
		}
		return strconv.FormatInt(parsed, 10), true
	}
	parsed, err := strconv.ParseUint(strconv.FormatUint(v, 10), 10, integralBitSize(target))
	if err != nil {
		return "", false
	}
	return strconv.FormatUint(parsed, 10), true
}

func isSupportedIntegralDomainCast(typ plan.Type) bool {
	return isSupportedIntegralType(types.T(typ.Id))
}

func isSupportedIntegralType(t types.T) bool {
	return isSignedIntegralType(t) || isUnsignedIntegralType(t)
}

func isSignedIntegralType(t types.T) bool {
	switch t {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		return true
	default:
		return false
	}
}

func isUnsignedIntegralType(t types.T) bool {
	switch t {
	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		return true
	default:
		return false
	}
}

func integralBitSize(t types.T) int {
	switch t {
	case types.T_int8, types.T_uint8:
		return 8
	case types.T_int16, types.T_uint16:
		return 16
	case types.T_int32, types.T_uint32:
		return 32
	default:
		return 64
	}
}

// unwrapConstLiteral returns the underlying literal and its effective type,
// peeling off any trailing cast(...) wrappers produced by re-binding. Returns
// ok=false when the expression is not a constant literal.
func unwrapConstLiteral(expr *plan.Expr) (*plan.Literal, plan.Type, bool) {
	effectiveTyp := expr.Typ
	for {
		if lit := expr.GetLit(); lit != nil {
			if lit.Isnull {
				return nil, plan.Type{}, false
			}
			return lit, effectiveTyp, true
		}
		fn := expr.GetF()
		if fn == nil || fn.Func.ObjName != "cast" || len(fn.Args) == 0 {
			return nil, plan.Type{}, false
		}
		expr = fn.Args[0]
	}
}

func buildColumnDomainExpr(ctx context.Context, colExpr *plan.Expr, values []*plan.Expr) (*plan.Expr, error) {
	stripped := make([]*plan.Expr, len(values))
	for i, v := range values {
		stripped[i] = stripConstLiteralCasts(v)
	}
	if len(stripped) == 1 {
		return BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			DeepCopyExpr(colExpr),
			stripped[0],
		})
	}
	listExpr := &plan.Expr{
		Typ: colExpr.Typ,
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: stripped},
		},
	}
	return BindFuncExprImplByPlanExpr(ctx, "in", []*plan.Expr{
		DeepCopyExpr(colExpr),
		listExpr,
	})
}

func combinePlanDisjunction(ctx context.Context, exprs []*plan.Expr) (expr *plan.Expr, err error) {
	expr = exprs[0]

	for i := 1; i < len(exprs); i++ {
		expr, err = BindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{expr, exprs[i]})
		if err != nil {
			break
		}
	}

	return
}

// stripConstLiteralCasts unwraps trailing cast() wrappers from a constant
// literal so it can be re-bound through BindFuncExprImplByPlanExpr without
// triggering the IN-list expansion to OR of =. Non-literal expressions and
// null literals are returned as-is.
func stripConstLiteralCasts(expr *plan.Expr) *plan.Expr {
	cur := expr
	for {
		if cur.GetLit() != nil {
			return cur
		}
		fn := cur.GetF()
		if fn == nil || fn.Func.ObjName != "cast" || len(fn.Args) == 0 {
			return expr
		}
		cur = fn.Args[0]
	}
}

func hasNonNilFunctionArgs(fn *plan.Function, argCount int) bool {
	if fn == nil || fn.Func == nil || len(fn.Args) != argCount {
		return false
	}
	for _, arg := range fn.Args {
		if arg == nil {
			return false
		}
	}
	return true
}

func (builder *QueryBuilder) bindCompositeKeySerial(args []*plan.Expr) (*plan.Expr, bool) {
	expr, err := bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "serial", args)
	return expr, err == nil && expr != nil
}

func (builder *QueryBuilder) bindCompositeKeyPredicate(name string, args ...*plan.Expr) (*plan.Expr, bool) {
	expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), name, args)
	return expr, err == nil && expr != nil
}

func (builder *QueryBuilder) doMergeFiltersOnCompositeKey(tableDef *plan.TableDef, tableTag int32, filters ...*plan.Expr) []*plan.Expr {
	sortkeyIdx := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]
	col2filter := make(map[int32]int)
	Parts := tableDef.Pkey.Names
	numParts := len(Parts)
	if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
		sortkeyIdx = tableDef.Name2ColIndex[tableDef.ClusterBy.Name]
		Parts = util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
		numParts = len(Parts)
	}

	for i, expr := range filters {
		if expr == nil {
			continue
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil {
			continue
		}

		funcName := fn.Func.ObjName
		if funcName == "=" {
			if !hasNonNilFunctionArgs(fn, 2) {
				continue
			}
			if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
				fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
			}

			col := fn.Args[0].GetCol()
			if col == nil || !isRuntimeConstExpr(fn.Args[1]) {
				continue
			}

			col2filter[col.ColPos] = i
		} else if funcName == "between" {
			if !hasNonNilFunctionArgs(fn, 3) {
				continue
			}
			col := fn.Args[0].GetCol()
			if col == nil || !isRuntimeConstExpr(fn.Args[1]) || !isRuntimeConstExpr(fn.Args[2]) {
				continue
			}

			col2filter[col.ColPos] = i
		} else if funcName == "<" || funcName == "<=" || funcName == ">" || funcName == ">=" {
			if !hasNonNilFunctionArgs(fn, 2) {
				continue
			}
			col := fn.Args[0].GetCol()
			if col == nil || !isRuntimeConstExpr(fn.Args[1]) {
				continue
			}

			if _, ok := col2filter[col.ColPos]; !ok {
				col2filter[col.ColPos] = i
			}
		} else if funcName == "in_range" {
			if !hasNonNilFunctionArgs(fn, 4) {
				continue
			}
			col := fn.Args[0].GetCol()
			if col == nil || !isRuntimeConstExpr(fn.Args[1]) || !isRuntimeConstExpr(fn.Args[2]) {
				continue
			}

			if _, ok := col2filter[col.ColPos]; !ok {
				col2filter[col.ColPos] = i
			}
		} else if funcName == "in" {
			if !hasNonNilFunctionArgs(fn, 2) || fn.Args[0].GetCol() == nil {
				continue
			}

			col := fn.Args[0].GetCol()
			if _, ok := col2filter[col.ColPos]; ok {
				continue
			}

			col2filter[col.ColPos] = i
		} else if funcName == "or" {
			var orArgs []*plan.Expr
			flattenLogicalExpressions(expr, "or", &orArgs)

			newOrArgs := make([]*plan.Expr, 0, len(orArgs))
			var inArgs []*plan.Expr
			var mergeCandidates []*plan.Expr
			var firstEquiExpr *plan.Expr
			pkFnName := "in"
			hasPrefixEq := false
			hasEquality := false

			currColPos := int32(-1)
			for _, subExpr := range orArgs {
				if subExpr == nil {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}
				subFn := subExpr.GetF()
				if subFn == nil || subFn.Func == nil {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if subFn.Func.ObjName == "=" || subFn.Func.ObjName == "in" {
					if numParts > 1 {
						newArgs := builder.doMergeFiltersOnCompositeKey(tableDef, tableTag, subExpr)
						if len(newArgs) == 0 {
							newOrArgs = append(newOrArgs, subExpr)
							continue
						}
						subExpr = newArgs[0]
					}
				} else if subFn.Func.ObjName == "and" {
					var andArgs []*plan.Expr
					flattenLogicalExpressions(subExpr, "and", &andArgs)

					newArgs := builder.doMergeFiltersOnCompositeKey(tableDef, tableTag, andArgs...)
					if len(newArgs) == 1 {
						subExpr = newArgs[0]
					} else {
						subFn.Args = newArgs
					}
				}

				mergedFn := subExpr.GetF()
				if !hasNonNilFunctionArgs(mergedFn, 2) || mergedFn.Args[0].GetCol() == nil || !isRuntimeConstExpr(mergedFn.Args[1]) {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				var candidateValues []*plan.Expr
				switch mergedFn.Func.ObjName {
				case "=", "prefix_eq":
					candidateValues = []*plan.Expr{mergedFn.Args[1]}
				case "in":
					var ok bool
					candidateValues, ok = inRHSValues(mergedFn.Args[1], mergedFn.Args[0].Typ)
					if !ok || len(candidateValues) == 0 {
						// Preserve an unsupported or empty IN arm. It is safer to
						// forgo this optional OR merge than to change its semantics.
						newOrArgs = append(newOrArgs, subExpr)
						continue
					}
				default:
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if currColPos == -1 {
					currColPos = mergedFn.Args[0].GetCol().ColPos
				} else if currColPos != mergedFn.Args[0].GetCol().ColPos {
					newOrArgs = append(newOrArgs, subExpr)
					continue
				}

				if len(inArgs) == 0 {
					firstEquiExpr = subExpr
				}

				switch mergedFn.Func.ObjName {
				case "=":
					inArgs = append(inArgs, candidateValues...)
					mergeCandidates = append(mergeCandidates, subExpr)
					hasEquality = true

				case "prefix_eq":
					inArgs = append(inArgs, candidateValues...)
					mergeCandidates = append(mergeCandidates, subExpr)
					pkFnName = "prefix_in"
					hasPrefixEq = true

				case "in":
					inArgs = append(inArgs, candidateValues...)
					mergeCandidates = append(mergeCandidates, subExpr)
					hasEquality = true
				}
			}

			canMerge := !(hasPrefixEq && hasEquality)

			if len(inArgs) == 0 {
				// no merge candidates
			} else if !canMerge {
				newOrArgs = append(newOrArgs, mergeCandidates...)
			} else if len(inArgs) == 1 {
				newOrArgs = append(newOrArgs, firstEquiExpr)
			} else if len(inArgs) > 1 {
				leftExpr := firstEquiExpr.GetF().Args[0]
				leftType := makeTypeByPlan2Expr(leftExpr)
				argsType := []types.Type{leftType, leftType}
				fGet, err := function.GetFunctionByName(builder.GetContext(), pkFnName, argsType)
				if err != nil {
					newOrArgs = append(newOrArgs, mergeCandidates...)
				} else {
					funcID := fGet.GetEncodedOverloadID()
					returnType := fGet.GetReturnType()
					exprType := makePlan2Type(&returnType)
					args := []*plan.Expr{
						leftExpr,
						{
							Typ: leftExpr.Typ,
							Expr: &plan.Expr_List{
								List: &plan.ExprList{
									List: inArgs,
								},
							},
						},
					}
					exprType.NotNullable = function.DeduceNotNullable(funcID, args)
					inExpr := &plan.Expr{
						Typ: exprType,
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: getFunctionObjRef(funcID, pkFnName),
								Args: args,
							},
						},
					}

					newOrArgs = append(newOrArgs, inExpr)
				}
			}

			if len(newOrArgs) == 1 {
				filters[i] = newOrArgs[0]
				if firstEquiExpr != nil {
					colPos := firstEquiExpr.GetF().Args[0].GetCol().ColPos
					if colPos != sortkeyIdx {
						col2filter[colPos] = i
					}
				}
			} else {
				fn.Args = newOrArgs
			}
		}
	}

	if numParts == 1 {
		return filters
	}

	sortKeyPartCols := make(map[int32]struct{}, len(Parts))
	for _, part := range Parts {
		if colIdx, ok := tableDef.Name2ColIndex[part]; ok {
			sortKeyPartCols[colIdx] = struct{}{}
		}
	}

	// Pre-merge paired range conditions on composite sort-key parts
	// (e.g., pk_a = 1 AND pk_b > 3 AND pk_b < 10) into in_range.
	colLowerBounds := make(map[int32]int) // colPos -> filter index
	colUpperBounds := make(map[int32]int) // colPos -> filter index
	for i, expr := range filters {
		if expr == nil {
			continue
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil {
			continue
		}
		col, isLower := classifyRangeBound(fn)
		if col == nil {
			continue
		}
		if _, ok := sortKeyPartCols[col.ColPos]; !ok {
			continue
		}
		if isLower {
			colLowerBounds[col.ColPos] = i
		} else {
			colUpperBounds[col.ColPos] = i
		}
	}
	for colPos, lowerIdx := range colLowerBounds {
		upperIdx, hasUpper := colUpperBounds[colPos]
		if !hasUpper {
			continue
		}
		// Only merge if col2filter points to a range op (not =, between, in, in_range).
		if existingIdx, ok := col2filter[colPos]; ok {
			fn := filters[existingIdx].GetF()
			if fn != nil {
				switch fn.Func.ObjName {
				case "=", "between", "in", "in_range":
					continue
				}
			}
		}
		lowerFn := filters[lowerIdx].GetF()
		upperFn := filters[upperIdx].GetF()
		lowerOp := canonicalRangeOp(lowerFn)
		upperOp := canonicalRangeOp(upperFn)
		lowerVal := rangeFilterConstValue(lowerFn)
		upperVal := rangeFilterConstValue(upperFn)
		if lowerVal == nil || upperVal == nil {
			continue
		}

		colExpr := lowerFn.Args[0]
		if colExpr.GetCol() == nil {
			colExpr = lowerFn.Args[1]
		}

		lb := lowerVal
		ub := upperVal

		// Cast bounds to column type so between/in_range runtime gets uniform types.
		colTyp := colExpr.Typ
		if lb.Typ.Id != colTyp.Id || lb.Typ.Width != colTyp.Width || lb.Typ.Scale != colTyp.Scale {
			if casted, err := appendCastBeforeExpr(builder.GetContext(), lb, colTyp); err == nil {
				lb = casted
			}
		}
		if ub.Typ.Id != colTyp.Id || ub.Typ.Width != colTyp.Width || ub.Typ.Scale != colTyp.Scale {
			if casted, err := appendCastBeforeExpr(builder.GetContext(), ub, colTyp); err == nil {
				ub = casted
			}
		}

		compositeFilterSel := filters[lowerIdx].Selectivity * filters[upperIdx].Selectivity

		var merged *plan.Expr
		if lowerOp == ">=" && upperOp == "<=" {
			merged = makeBetweenExpr(colExpr, lb, ub)
		} else {
			var flag uint8
			if lowerOp == ">" {
				flag |= 1
			}
			if upperOp == "<" {
				flag |= 2
			}
			merged = makeInRangeExpr(colExpr, lb, ub, flag)
		}
		merged.Selectivity = compositeFilterSel

		filters[lowerIdx] = merged
		filters[upperIdx] = makePlan2BoolConstExprWithType(true)
		col2filter[colPos] = lowerIdx
	}

	filterIdx := make([]int, 0, numParts)
	for _, part := range Parts {
		colIdx := tableDef.Name2ColIndex[part]
		idx, ok := col2filter[colIdx]
		if !ok {
			break
		}

		filterIdx = append(filterIdx, idx)
		fn := filters[idx].GetF()
		if fn == nil || fn.Func == nil {
			return filters
		}
		funcName := fn.Func.ObjName
		if funcName == "in" || funcName == "between" ||
			funcName == "<" || funcName == "<=" || funcName == ">" || funcName == ">=" ||
			funcName == "in_range" {
			break
		}
	}

	if len(filterIdx) == 0 {
		return filters
	}

	var compositePKFilter *plan.Expr
	pkExpr := &plan.Expr{
		Typ: tableDef.Cols[sortkeyIdx].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: tableTag,
				ColPos: sortkeyIdx,
			},
		},
	}

	compositePKFilterSel := 1.0
	for i := range filterIdx {
		compositePKFilterSel *= (filters[filterIdx[i]]).Selectivity
	}

	lastFilter := filters[filterIdx[len(filterIdx)-1]]
	lastFn := lastFilter.GetF()
	if lastFn == nil || lastFn.Func == nil {
		return filters
	}
	lastFuncName := lastFn.Func.ObjName
	if lastFuncName == "in" {
		if !hasNonNilFunctionArgs(lastFn, 2) {
			return filters
		}
		lastArgs, ok := inRHSValues(lastFn.Args[1], lastFn.Args[0].Typ)
		if !ok {
			return filters
		}

		serialArgs := make([]*plan.Expr, len(filterIdx)-1)
		for i := 0; i < len(filterIdx)-1; i++ {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}

		inArgs := make([]*plan.Expr, len(lastArgs))
		for i, lastArg := range lastArgs {
			tmpSerialArgs := DeepCopyExprList(serialArgs)
			tmpSerialArgs = append(tmpSerialArgs, lastArg)
			rightArg, ok := builder.bindCompositeKeySerial(tmpSerialArgs)
			if !ok {
				return filters
			}
			inArgs[i] = rightArg
		}

		funcName := "in"
		if len(filterIdx) < numParts {
			funcName = "prefix_in"
		}

		compositePKFilter, ok = builder.bindCompositeKeyPredicate(funcName,
			pkExpr,
			&plan.Expr{
				Typ: pkExpr.Typ,
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: inArgs,
					},
				},
			},
		)
		if !ok {
			return filters
		}
	} else if lastFuncName == "between" {
		if !hasNonNilFunctionArgs(lastFn, 3) {
			return filters
		}
		serialArgs := make([]*plan.Expr, len(filterIdx)-1)
		for i := 0; i < len(filterIdx)-1; i++ {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}

		tmpSerialArgs := DeepCopyExprList(serialArgs)
		tmpSerialArgs = append(tmpSerialArgs, lastFn.Args[1])
		leftArg, ok := builder.bindCompositeKeySerial(tmpSerialArgs)
		if !ok {
			return filters
		}

		tmpSerialArgs = append(DeepCopyExprList(serialArgs), lastFn.Args[2])
		rightArg, ok := builder.bindCompositeKeySerial(tmpSerialArgs)
		if !ok {
			return filters
		}

		funcName := "between"
		if len(filterIdx) < numParts {
			funcName = "prefix_between"
		}

		compositePKFilter, ok = builder.bindCompositeKeyPredicate(funcName,
			pkExpr,
			leftArg,
			rightArg,
		)
		if !ok {
			return filters
		}
	} else if lastFuncName == "in_range" {
		if !hasNonNilFunctionArgs(lastFn, 4) {
			return filters
		}
		serialArgs := make([]*plan.Expr, len(filterIdx)-1)
		for i := 0; i < len(filterIdx)-1; i++ {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}

		if len(filterIdx) < numParts && len(serialArgs) == 0 {
			return filters
		}

		tmpSerialArgs := DeepCopyExprList(serialArgs)
		tmpSerialArgs = append(tmpSerialArgs, lastFn.Args[1])
		leftArg, ok := builder.bindCompositeKeySerial(tmpSerialArgs)
		if !ok {
			return filters
		}

		tmpSerialArgs = append(DeepCopyExprList(serialArgs), lastFn.Args[2])
		rightArg, ok := builder.bindCompositeKeySerial(tmpSerialArgs)
		if !ok {
			return filters
		}

		funcName := "in_range"
		if len(filterIdx) < numParts {
			funcName = "prefix_in_range"
		}

		compositePKFilter, ok = builder.bindCompositeKeyPredicate(funcName,
			pkExpr,
			leftArg,
			rightArg,
			lastFn.Args[3],
		)
		if !ok {
			return filters
		}
	} else if lastFuncName == "<" || lastFuncName == "<=" || lastFuncName == ">" || lastFuncName == ">=" {
		if !hasNonNilFunctionArgs(lastFn, 2) {
			return filters
		}
		serialArgs := make([]*plan.Expr, len(filterIdx)-1)
		for i := 0; i < len(filterIdx)-1; i++ {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}

		if len(filterIdx) < numParts && len(serialArgs) == 0 {
			return filters
		}

		tmpSerialArgs := append(DeepCopyExprList(serialArgs), lastFn.Args[1])
		boundArg, ok := builder.bindCompositeKeySerial(tmpSerialArgs)
		if !ok {
			return filters
		}

		prefixArg, ok := builder.bindCompositeKeySerial(DeepCopyExprList(serialArgs))
		if !ok {
			return filters
		}

		var flag byte
		var leftArg, rightArg *plan.Expr
		switch lastFuncName {
		case "<":
			leftArg = prefixArg
			rightArg = boundArg
			flag = 2 // RightOpen
		case "<=":
			leftArg = prefixArg
			rightArg = boundArg
			flag = 0 // both closed
		case ">":
			leftArg = boundArg
			rightArg = prefixArg
			flag = 1 // LeftOpen
		case ">=":
			leftArg = boundArg
			rightArg = prefixArg
			flag = 0 // both closed
		}

		flagExpr := makePlan2Uint8ConstExprWithType(flag)

		compositePKFilter, ok = builder.bindCompositeKeyPredicate("prefix_in_range",
			pkExpr,
			leftArg,
			rightArg,
			flagExpr,
		)
		if !ok {
			return filters
		}
	} else {
		serialArgs := make([]*plan.Expr, len(filterIdx))
		for i := range filterIdx {
			serialArgs[i] = filters[filterIdx[i]].GetF().Args[1]
		}
		rightArg, ok := builder.bindCompositeKeySerial(serialArgs)
		if !ok {
			return filters
		}

		funcName := "="
		if len(filterIdx) < numParts {
			funcName = "prefix_eq"
		}

		compositePKFilter, ok = builder.bindCompositeKeyPredicate(funcName,
			pkExpr,
			rightArg,
		)
		if !ok {
			return filters
		}
	}
	if compositePKFilter == nil {
		return filters
	}
	compositePKFilter.Selectivity = compositePKFilterSel

	hitFilterSet := make(map[int]bool)
	for i := range filterIdx {
		hitFilterSet[filterIdx[i]] = true
	}

	newFilterList := make([]*plan.Expr, 0, len(filters)-len(filterIdx)+1)
	newFilterList = append(newFilterList, compositePKFilter)
	for i, filter := range filters {
		if !hitFilterSet[i] {
			newFilterList = append(newFilterList, filter)
		}
	}

	return newFilterList
}

func flattenLogicalExpressions(expr *plan.Expr, opName string, args *[]*plan.Expr) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != opName {
		*args = append(*args, expr)
		return
	}

	for _, arg := range fn.Args {
		flattenLogicalExpressions(arg, opName, args)
	}
}

var (
	boolType       = plan.Type{Id: int32(types.T_bool), NotNullable: false}
	betweenFuncRef = getFunctionObjRef(int64(function.BETWEEN)<<32, "between")
	inRangeFuncRef = getFunctionObjRef(int64(function.IN_RANGE)<<32, "in_range")
)

func makeBetweenExpr(col, lb, ub *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: betweenFuncRef,
				Args: []*plan.Expr{col, lb, ub},
			},
		},
		Typ: boolType,
	}
}

func makeInRangeExpr(col, lb, ub *plan.Expr, flag uint8) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: inRangeFuncRef,
				Args: []*plan.Expr{col, lb, ub, MakePlan2Uint8ConstExprWithType(flag)},
			},
		},
		Typ: boolType,
	}
}
