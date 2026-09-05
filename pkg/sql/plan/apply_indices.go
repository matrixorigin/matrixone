// Copyright 2023 Matrix Origin
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
	"context"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	statspb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
)

const (
	UnsupportedIndexCondition = 0
	EqualIndexCondition       = 1
	NonEqualIndexCondition    = 2
	SpatialIndexCondition     = 3
	RangeIndexCondition       = 4

	// MaxOverFetchFactor is the maximum multiplier for over-fetching candidates
	// in auto mode vector search. This cap prevents excessive memory usage and
	// candidate processing when filter selectivity is very low (e.g., 0.001).
	// Value of 100 means we fetch at most 100x the original LIMIT value.
	MaxOverFetchFactor = 100.0
)

var spatialIndexPredicateNames = map[string]struct{}{
	"st_contains":   {},
	"st_coveredby":  {},
	"st_covers":     {},
	"st_crosses":    {},
	"st_disjoint":   {},
	"st_equals":     {},
	"st_intersects": {},
	"st_overlaps":   {},
	"st_touches":    {},
	"st_within":     {},
}

var spatialIndexDistanceComparisonNames = map[string]struct{}{
	"<":  {},
	"<=": {},
	"=":  {},
	">=": {},
	">":  {},
}

type specialIndexKind uint8

const (
	specialIndexKindFullText specialIndexKind = 1 << iota
	specialIndexKindVector
)

type specialIndexGuard struct {
	kinds       specialIndexKind
	scanNodeIDs []int32
}

type regularIndexTopSortContext struct {
	sortNode         *plan.Node
	sortProjectNode  *plan.Node
	scanNode         *plan.Node
	pushOrderedLimit bool
}

// overFetchDisplayLimit returns the plan-time over-fetched candidate budget for a
// LITERAL limit, for EXPLAIN display only (IndexReaderParam.OverFetchLimit) — 0
// for a prepared LIMIT ? (unknown at plan time) or when no over-fetch applies.
// It calls the same overfetch functions the TVF uses at EXECUTE, so the displayed
// value equals the runtime budget. filteredPostMode selects ivfflat's factor.
func overFetchDisplayLimit(limit *plan.Expr, overFetch bool, filteredPostMode bool) uint64 {
	if !overFetch || limit == nil {
		return 0
	}
	lit := limit.GetLit()
	if lit == nil {
		return 0
	}
	k := lit.GetU64Val()
	if filteredPostMode {
		return overfetch.FilteredPostModeLimit(k)
	}
	return overfetch.PostFilterLimit(k)
}

// BuildOverFetchLimitExpr returns an expression evaluating to the over-fetched
// candidate budget k' = overfetch.PostFilterLimit(k), for a k that may only be known
// at EXECUTE (a prepared LIMIT ?). A literal k is folded here; a parameterized one
// becomes an expression computing the same step function at runtime.
//
// Why the budget travels on node.Limit rather than on IndexReaderParam.Limit alone:
// node.Limit is the ONLY candidate-budget channel a pre-change CN understands. A
// vector provider child gives the FUNCTION_SCAN a child, so compileTableFunction
// attaches the search operator to already-compiled child scopes, which may be Remote
// — a new coordinator can therefore ship this operator to an old CN during a rolling
// upgrade. That CN's Prepare reads arg.Limit alone, so a nil there makes it default
// to one candidate and silently under-return before the post-filter JOIN.
//
// Carrying k' here needs no protocol capability and no low-version fallback, because
// every function used (case, greatest, cast, *, +) long predates any CN this can be
// mixed with, and evalLimitExpression has always evaluated a non-literal arg.Limit
// through a general expression executor. Old and new CNs compute the same k'.
//
// The formula mirrors overfetch.PostFilterLimit exactly:
//
//	greatest(cast(k * factor(k) as uint64), k + 10)
//
// so the two implementations cannot drift into disagreeing about the budget.
// TestOverFetchLimitExprMatchesGoFormula pins them together.
func BuildOverFetchLimitExpr(ctx context.Context, limit *plan.Expr, filteredPostMode bool) (*plan.Expr, error) {
	if limit == nil {
		return nil, nil
	}
	if lit := limit.GetLit(); lit != nil {
		k := lit.GetU64Val()
		if filteredPostMode {
			return makePlan2Uint64ConstExprWithType(overfetch.FilteredPostModeLimit(k)), nil
		}
		return makePlan2Uint64ConstExprWithType(overfetch.PostFilterLimit(k)), nil
	}

	// factor(k): the same bucketed step function the Go helpers use.
	bounds := overfetch.PostFilterFactorSteps()
	if filteredPostMode {
		bounds = overfetch.FilteredPostModeFactorSteps()
	}
	caseArgs := make([]*plan.Expr, 0, len(bounds)*2+1)
	for _, step := range bounds {
		cond, err := BindFuncExprImplByPlanExpr(ctx, "<", []*plan.Expr{
			DeepCopyExpr(limit), makePlan2Uint64ConstExprWithType(step.Below)})
		if err != nil {
			return nil, err
		}
		caseArgs = append(caseArgs, cond, makePlan2Float64ConstExprWithType(step.Factor))
	}
	caseArgs = append(caseArgs, makePlan2Float64ConstExprWithType(overfetch.DefaultFactor(filteredPostMode)))
	factor, err := BindFuncExprImplByPlanExpr(ctx, "case", caseArgs)
	if err != nil {
		return nil, err
	}

	scaled, err := BindFuncExprImplByPlanExpr(ctx, "*", []*plan.Expr{DeepCopyExpr(limit), factor})
	if err != nil {
		return nil, err
	}
	// floor() before the cast is required, not cosmetic: Go's uint64(product)
	// truncates while SQL CAST(... AS UNSIGNED) rounds half away from zero, so
	// k=51 would give 76 in overfetch.Limit and 77 here. Truncating explicitly
	// keeps the two definitions equal for every k.
	truncated, err := BindFuncExprImplByPlanExpr(ctx, "floor", []*plan.Expr{scaled})
	if err != nil {
		return nil, err
	}
	// SATURATION, HALF ONE: the product.
	//
	// overfetch.Limit clamps the product at MaxUint64 rather than overflowing.
	// The cast below cannot express that on its own -- CAST(... AS UNSIGNED)
	// raises "data out of range" for anything at or above 2^64, so a perfectly
	// valid large LIMIT would fail as a bound parameter while succeeding as a
	// literal.
	//
	// The clamp is applied with least() in FLOAT space, before the cast, rather
	// than by branching around the cast. A CASE cannot be relied on here: the
	// vectorized evaluator may evaluate both arms and only then select, so an
	// unguarded cast in the untaken arm would still raise. least() is
	// branch-free, so the cast never sees an out-of-range input.
	//
	// maxU64AsFloat is the largest float64 strictly below 2^64 (2^64 - 2048;
	// the ULP at that magnitude is 2^11). Clamping there is a no-op for every
	// value Go would truncate -- float64 cannot represent anything between it
	// and 2^64 -- so it only ever guards the cast.
	const maxU64AsFloat = 18446744073709549568.0 // 2^64 - 2048
	const twoPow64 = 18446744073709551616.0      // == float64(math.MaxUint64) in Go
	castable, err := BindFuncExprImplByPlanExpr(ctx, "least", []*plan.Expr{
		truncated, makePlan2Float64ConstExprWithType(maxU64AsFloat)})
	if err != nil {
		return nil, err
	}
	scaledU64, err := appendCastBeforeExpr(ctx, castable, plan.Type{
		Id: int32(types.T_uint64), NotNullable: true})
	if err != nil {
		return nil, err
	}
	// Go compares `product >= float64(math.MaxUint64)`, and that conversion
	// yields 2^64 exactly (MaxUint64 itself is not representable). Compare
	// against the same 2^64 so the two agree on which k saturates.
	overflows, err := BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{
		DeepCopyExpr(scaled), makePlan2Float64ConstExprWithType(twoPow64)})
	if err != nil {
		return nil, err
	}
	scaledU64, err = BindFuncExprImplByPlanExpr(ctx, "case", []*plan.Expr{
		overflows, makePlan2Uint64ConstExprWithType(math.MaxUint64), scaledU64})
	if err != nil {
		return nil, err
	}

	// SATURATION, HALF TWO: the additive floor.
	//
	// The +10 floor keeps a small k from over-fetching too little to survive the
	// filter, but k+10 wraps for k > MaxUint64-10, and unsigned addition raises
	// rather than wrapping silently. overfetch.Limit clamps to MaxUint64 there.
	//
	// Clamping the ADDEND instead of guarding the sum keeps this branch-free for
	// the same reason as above: least(k, MaxUint64-10) + 10 is at most MaxUint64
	// by construction, so no evaluation order can overflow it.
	clampedK, err := BindFuncExprImplByPlanExpr(ctx, "least", []*plan.Expr{
		DeepCopyExpr(limit),
		makePlan2Uint64ConstExprWithType(math.MaxUint64 - overfetch.MinExtraCandidates)})
	if err != nil {
		return nil, err
	}
	minCandidates, err := BindFuncExprImplByPlanExpr(ctx, "+", []*plan.Expr{
		clampedK, makePlan2Uint64ConstExprWithType(overfetch.MinExtraCandidates)})
	if err != nil {
		return nil, err
	}
	budget, err := BindFuncExprImplByPlanExpr(ctx, "greatest", []*plan.Expr{scaledU64, minCandidates})
	if err != nil {
		return nil, err
	}

	// overfetch.Limit short-circuits k == 0 to 0; without the same guard the floor
	// above would turn `LIMIT 0` into a 10-candidate search.
	isZero, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		DeepCopyExpr(limit), makePlan2Uint64ConstExprWithType(0)})
	if err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(ctx, "case", []*plan.Expr{
		isZero, makePlan2Uint64ConstExprWithType(0), budget})
}

func containsDynamicParam(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_P, *plan.Expr_V:
		return true
	case *plan.Expr_F:
		for _, subExpr := range exprImpl.F.Args {
			if containsDynamicParam(subExpr) {
				return true
			}
		}
	}
	return false
}

func isRuntimeConstExpr(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Vec, *plan.Expr_T:
		return true

	case *plan.Expr_F:
		for _, subExpr := range exprImpl.F.Args {
			if !isRuntimeConstExpr(subExpr) {
				return false
			}
		}

		return true

	case *plan.Expr_List:
		for _, subExpr := range exprImpl.List.List {
			if !isRuntimeConstExpr(subExpr) {
				return false
			}
		}

		return true

	default:
		return false
	}
}

func checkSpatialIndexFilter(expr *plan.Expr) *plan.ColRef {
	if expr == nil {
		return nil
	}
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 {
		return nil
	}
	if _, ok := spatialIndexPredicateNames[catalog.ToLower(fn.Func.ObjName)]; !ok {
		return checkSpatialIndexDistanceFilter(fn)
	}
	return checkSpatialIndexPredicateFilter(fn)
}

func checkSpatialIndexPredicateFilter(fn *plan.Function) *plan.ColRef {
	if fn == nil || len(fn.Args) != 2 {
		return nil
	}
	if col := fn.Args[0].GetCol(); col != nil && isRuntimeConstExpr(fn.Args[1]) {
		return col
	}
	if col := fn.Args[1].GetCol(); col != nil && isRuntimeConstExpr(fn.Args[0]) {
		return col
	}
	return nil
}

func checkSpatialIndexDistanceFilter(fn *plan.Function) *plan.ColRef {
	if fn == nil || len(fn.Args) != 2 {
		return nil
	}
	if _, ok := spatialIndexDistanceComparisonNames[fn.Func.ObjName]; !ok {
		return nil
	}
	if isRuntimeConstExpr(fn.Args[1]) {
		if col := checkSpatialDistanceExpr(fn.Args[0]); col != nil {
			return col
		}
	}
	if isRuntimeConstExpr(fn.Args[0]) {
		if col := checkSpatialDistanceExpr(fn.Args[1]); col != nil {
			return col
		}
	}
	return nil
}

func checkSpatialDistanceExpr(expr *plan.Expr) *plan.ColRef {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) != 2 || catalog.ToLower(fn.Func.ObjName) != "st_distance" {
		return nil
	}
	return checkSpatialIndexPredicateFilter(fn)
}

func findSpatialIndexFilter(idxDef *IndexDef, node *plan.Node) int32 {
	targetColPos, ok := node.TableDef.Name2ColIndex[indexPrimaryPartName(idxDef)]
	if !ok {
		return -1
	}
	for i := range node.FilterList {
		col := checkSpatialIndexFilter(node.FilterList[i])
		if col != nil && col.ColPos == targetColPos {
			return int32(i)
		}
	}
	return -1
}

func buildSpatialIndexColMap(idxDef *IndexDef, node *plan.Node, idxTag int32, idxTableDef *plan.TableDef) map[[2]int32]*plan.Expr {
	partColPos := node.TableDef.Name2ColIndex[indexPrimaryPartName(idxDef)]
	pkColPos := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
	partKey := [2]int32{node.BindingTags[0], partColPos}
	pkKey := [2]int32{node.BindingTags[0], pkColPos}
	return map[[2]int32]*plan.Expr{
		partKey: GetColExpr(idxTableDef.Cols[0].Typ, idxTag, 0),
		pkKey:   GetColExpr(idxTableDef.Cols[1].Typ, idxTag, 1),
	}
}

func exprUsesOnlyMappedCols(expr *plan.Expr, projMap map[[2]int32]*plan.Expr) bool {
	if expr == nil {
		return true
	}
	switch ne := expr.Expr.(type) {
	case *plan.Expr_Col:
		_, ok := projMap[[2]int32{ne.Col.RelPos, ne.Col.ColPos}]
		return ok
	case *plan.Expr_F:
		for _, arg := range ne.F.Args {
			if !exprUsesOnlyMappedCols(arg, projMap) {
				return false
			}
		}
		return true
	case *plan.Expr_List:
		for _, arg := range ne.List.List {
			if !exprUsesOnlyMappedCols(arg, projMap) {
				return false
			}
		}
		return true
	case *plan.Expr_W:
		if !exprUsesOnlyMappedCols(ne.W.WindowFunc, projMap) {
			return false
		}
		for _, arg := range ne.W.PartitionBy {
			if !exprUsesOnlyMappedCols(arg, projMap) {
				return false
			}
		}
		for _, order := range ne.W.OrderBy {
			if !exprUsesOnlyMappedCols(order.Expr, projMap) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func (builder *QueryBuilder) prepareSpecialIndexGuards(rootID int32) {
	if builder.protectedScans == nil {
		builder.protectedScans = make(map[int32]int)
	} else {
		for k := range builder.protectedScans {
			delete(builder.protectedScans, k)
		}
	}

	if builder.projectSpecialGuards == nil {
		builder.projectSpecialGuards = make(map[int32]*specialIndexGuard)
	} else {
		for k := range builder.projectSpecialGuards {
			delete(builder.projectSpecialGuards, k)
		}
	}

	clear(builder.projectAnchoredSorts)
	builder.collectSpecialIndexGuards(rootID)
}

func (builder *QueryBuilder) resetSpecialIndexGuards() {
	clear(builder.projectAnchoredSorts)
	if builder.protectedScans != nil {
		for k := range builder.protectedScans {
			delete(builder.protectedScans, k)
		}
	}
	if builder.projectSpecialGuards != nil {
		for k := range builder.projectSpecialGuards {
			delete(builder.projectSpecialGuards, k)
		}
	}
}

func (builder *QueryBuilder) collectSpecialIndexGuards(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	if node.NodeType == plan.Node_PROJECT {
		if scanIDs := builder.detectFullTextGuard(node); len(scanIDs) > 0 {
			builder.registerProjectGuard(node.NodeId, specialIndexKindFullText, scanIDs)
		}
		if scanIDs := builder.detectVectorGuard(node); len(scanIDs) > 0 {
			builder.registerProjectGuard(node.NodeId, specialIndexKindVector, scanIDs)
		}
		// This pre-pass visits a node before its children, so claiming the Top-K here
		// settles the anchor for both the guard below and applyIndicesForSort.
		if len(node.Children) == 1 && builder.qry.Nodes[node.Children[0]].NodeType == plan.Node_SORT {
			builder.markProjectAnchoredSort(node.Children[0])
		}
	}
	if node.NodeType == plan.Node_SORT {
		if _, anchored := builder.projectAnchoredSorts[node.NodeId]; !anchored {
			if scanIDs := builder.detectVectorGuardFromSort(node); len(scanIDs) > 0 {
				builder.registerProjectGuard(node.NodeId, specialIndexKindVector, scanIDs)
			}
		}
	}

	for _, childID := range node.Children {
		builder.collectSpecialIndexGuards(childID)
	}
}

func (builder *QueryBuilder) registerProjectGuard(projID int32, kind specialIndexKind, scanIDs []int32) {
	if len(scanIDs) == 0 {
		return
	}
	if builder.projectSpecialGuards == nil {
		builder.projectSpecialGuards = make(map[int32]*specialIndexGuard)
	}
	if builder.protectedScans == nil {
		builder.protectedScans = make(map[int32]int)
	}

	guard, ok := builder.projectSpecialGuards[projID]
	if !ok {
		guard = &specialIndexGuard{}
		builder.projectSpecialGuards[projID] = guard
	}
	guard.kinds |= kind
	for _, scanID := range scanIDs {
		if !containsInt32(guard.scanNodeIDs, scanID) {
			guard.scanNodeIDs = append(guard.scanNodeIDs, scanID)
		}
		builder.protectedScans[scanID]++
	}
}

func (builder *QueryBuilder) clearProjectGuard(projID int32) {
	if builder.projectSpecialGuards == nil {
		return
	}
	guard, ok := builder.projectSpecialGuards[projID]
	if !ok {
		return
	}

	if builder.protectedScans != nil {
		for _, scanID := range guard.scanNodeIDs {
			if cnt, ok := builder.protectedScans[scanID]; ok {
				if cnt <= 1 {
					delete(builder.protectedScans, scanID)
				} else {
					builder.protectedScans[scanID] = cnt - 1
				}
			}
		}
	}

	delete(builder.projectSpecialGuards, projID)
}

func (builder *QueryBuilder) isScanProtected(scanID int32) bool {
	if builder == nil {
		return false
	}
	if _, ok := builder.updateTargetScans[scanID]; ok {
		return true
	}
	return builder.protectedScans[scanID] > 0
}

func (builder *QueryBuilder) suspendScanProtection(scanID int32) func() {
	if builder == nil || builder.protectedScans == nil {
		return func() {}
	}

	originalCount, wasProtected := builder.protectedScans[scanID]
	if wasProtected {
		delete(builder.protectedScans, scanID)
	}

	return func() {
		currentCount, stillProtected := builder.protectedScans[scanID]
		switch {
		case wasProtected && stillProtected:
			builder.protectedScans[scanID] = originalCount + currentCount
		case wasProtected:
			builder.protectedScans[scanID] = originalCount
		case stillProtected:
			builder.protectedScans[scanID] = currentCount
		default:
			delete(builder.protectedScans, scanID)
		}
	}
}

func (builder *QueryBuilder) withSuspendedScanProtection(scanID int32, callback func()) {
	restore := builder.suspendScanProtection(scanID)
	defer restore()
	callback()
}

func containsInt32(list []int32, target int32) bool {
	for _, v := range list {
		if v == target {
			return true
		}
	}
	return false
}

func (builder *QueryBuilder) applyIndices(nodeID int32, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	var err error

	if builder.optimizerHints != nil && builder.optimizerHints.applyIndices != 0 {
		return nodeID, nil
	}

	node := builder.qry.Nodes[nodeID]
	for i, childID := range node.Children {
		if node.NodeType == plan.Node_JOIN && joinCanConsumeIndexHints(node) &&
			builder.qry.Nodes[childID].NodeType == plan.Node_TABLE_SCAN && builder.scanForcesJoinIndex(childID) {
			continue
		}
		node.Children[i], err = builder.applyIndices(childID, colRefCnt, idxColMap)
		if err != nil {
			return -1, err
		}
	}
	replaceColumnsForNode(node, idxColMap)

	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		return builder.applyIndicesForFilters(nodeID, node, colRefCnt, idxColMap), nil

	case plan.Node_JOIN:
		return builder.applyIndicesForJoins(nodeID, node, colRefCnt, idxColMap)

	case plan.Node_PROJECT:
		//NOTE: This is the entry point for vector index rule on SORT NODE.
		return builder.applyIndicesForProject(nodeID, node, colRefCnt, idxColMap)

	case plan.Node_SORT:
		// Second entry point for the vector rule: a Top-K SORT whose parent is not a
		// PROJECT (outer ORDER BY, or a join input) is invisible to the project-anchored
		// path above, and would otherwise fall back to a full scan + exact sort.
		return builder.applyIndicesForSort(nodeID, node, colRefCnt, idxColMap)

	case plan.Node_AGG:
		// Third fulltext anchor: an AGG -> SCAN(MATCH) whose parent is NOT a single-input
		// PROJECT. A flattened scalar subquery -- `select (select count(*) from t where
		// match(...) against(...))` -- becomes JOIN(SINGLE/LEFT) -> AGG -> SCAN, so the
		// project-anchored resolveFullTextIndexPath (which stops at the 2-input JOIN) never
		// reaches the AGG and the MATCH survives to execution as error 20105 (#27962).
		// Anchoring on the AGG itself rewrites it. When the AGG *does* sit under a PROJECT
		// (top-level `select count(*) ... where match`), this fires first during child
		// recursion and consumes the scan's MATCH filters, so the later PROJECT pass finds
		// none and no-ops -- no double rewrite.
		if scanNode := builder.resolveScanNodeWithIndex(node, 1); scanNode != nil {
			filterids, filterFTIdxs := builder.getFullTextMatchFiltersFromScanNode(scanNode)
			wrappedFTExprs, wrappedFTIdxs := builder.getWrappedFullTextMatches(nil, scanNode, filterids, nil)
			if len(filterids) > 0 || len(wrappedFTExprs) > 0 {
				return builder.applyIndicesForAggUsingFullTextIndex(nodeID, nil, node, scanNode,
					filterids, filterFTIdxs, wrappedFTExprs, wrappedFTIdxs, colRefCnt, idxColMap)
			}
		}
	}

	return nodeID, nil
}

// applyVectorIndicesEarly splices ANN access paths before statistics, join
// ordering and distribution are finalized. Other secondary-index rewrites stay
// in the established late pass; this traversal handles only the two vector
// anchors and propagates their column remaps to ancestors.
func (builder *QueryBuilder) applyVectorIndicesEarly(
	nodeID int32,
	colRefCnt map[[2]int32]int,
	idxColMap map[[2]int32]*plan.Expr,
) (int32, error) {
	node := builder.qry.Nodes[nodeID]
	for i, childID := range node.Children {
		newChild, err := builder.applyVectorIndicesEarly(childID, colRefCnt, idxColMap)
		if err != nil {
			return nodeID, err
		}
		node.Children[i] = newChild
	}
	replaceColumnsForNode(node, idxColMap)

	switch node.NodeType {
	case plan.Node_PROJECT:
		vecCtx := builder.buildVectorSortContext(node)
		if vecCtx == nil {
			vecCtx = builder.buildVectorSortContextThroughJoin(node)
		}
		if vecCtx == nil {
			return nodeID, nil
		}
		newNodeID, handled, err := builder.applyLogicalVectorIndexForSortContext(nodeID, vecCtx, colRefCnt, idxColMap)
		if handled || err != nil {
			return newNodeID, err
		}
	case plan.Node_SORT:
		if _, projectOwned := builder.projectAnchoredSorts[nodeID]; projectOwned {
			return nodeID, nil
		}
		vecCtx := builder.buildVectorSortContextFromSort(node)
		if vecCtx == nil {
			return nodeID, nil
		}
		newNodeID, _, err := builder.applyLogicalVectorIndexForSortContext(nodeID, vecCtx, colRefCnt, idxColMap)
		return newNodeID, err
	}
	return nodeID, nil
}

func (builder *QueryBuilder) applyLogicalVectorIndexForSortContext(
	nodeID int32,
	vecCtx *vectorSortContext,
	colRefCnt map[[2]int32]int,
	idxColMap map[[2]int32]*plan.Expr,
) (int32, bool, error) {
	if vecCtx == nil || vecCtx.scanNode == nil {
		return nodeID, false, nil
	}
	indexes, err := builder.collectVectorIndexes(vecCtx.scanNode)
	if err != nil {
		return nodeID, true, err
	}
	if len(indexes) == 0 {
		return nodeID, false, nil
	}
	opts := planplugin.ApplyForSortOpts{ColRefCnt: colRefCnt, IdxColMap: idxColMap}
	for _, multi := range indexes {
		p, ok := indexplugin.Get(multi.IndexAlgo)
		if !ok || !indexplugin.IsVectorIndexAlgo(multi.IndexAlgo) {
			continue
		}
		logical, ok := p.Plan().(planplugin.LogicalSearchHooks)
		if !ok {
			continue
		}
		if err := builder.recordPreparedPluginDependencies(vecCtx.scanNode); err != nil {
			return nodeID, true, err
		}
		vctxExt, mtiExt := toPlanplugin(vecCtx, multi)
		newNodeID, applied, err := logical.BuildLogicalSearch(builder, vctxExt, mtiExt, nodeID, opts)
		if err != nil || applied {
			return newNodeID, true, err
		}
	}
	return nodeID, false, nil
}

func joinCanConsumeIndexHints(node *plan.Node) bool {
	return node != nil && (node.JoinType == plan.Node_INNER || node.JoinType == plan.Node_RIGHT ||
		node.JoinType == plan.Node_SEMI || (node.JoinType == plan.Node_ANTI && node.IsRightJoin))
}

func (builder *QueryBuilder) applyIndicesForFilters(nodeID int32, node *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	if len(node.FilterList) == 0 || len(node.TableDef.Indexes) == 0 {
		return nodeID
	}
	if builder.scanHasMatchedFullTextFilter(node) {
		return nodeID
	}
	if builder.isScanProtected(node.NodeId) {
		return nodeID
	}

	// 1. Master Index Check
	{
		masterIndexes := make([]*plan.IndexDef, 0)
		for _, indexDef := range node.TableDef.Indexes {
			if indexDef != nil && indexDef.TableExist && !indexDef.Unique && catalog.IsMasterIndexAlgo(indexDef.IndexAlgo) {
				masterIndexes = append(masterIndexes, indexDef)
			}
		}
		masterIndexes = builder.filterIndexesByScanHints(node, masterIndexes)

		if len(masterIndexes) == 0 {
			goto END0
		}

		for _, expr := range node.FilterList {
			fn := expr.GetF()
			if fn == nil {
				goto END0
			}

			switch fn.Func.ObjName {
			case "=":
				if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
					fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
				}

				if !isRuntimeConstExpr(fn.Args[1]) {
					goto END0
				}
			case "between":
			case "in":

			default:
				goto END0
			}

			col := fn.Args[0].GetCol()
			if col == nil {
				goto END0
			}
		}
		for _, indexDef := range masterIndexes {
			isAllFilterColumnsIncluded := true
			for _, expr := range node.FilterList {
				fn := expr.GetF()
				col := fn.Args[0].GetCol()
				if !isKeyPresentInList(col.Name, indexDef.Parts) {
					isAllFilterColumnsIncluded = false
					break
				}
			}
			if isAllFilterColumnsIncluded {
				return builder.applyIndicesForFiltersUsingMasterIndex(nodeID, node, indexDef)
			}
		}

	}
END0:
	// 2. Regular Index Check
	{
		return builder.applyIndicesForFiltersRegularIndex(nodeID, node, colRefCnt, idxColMap)
	}
}

func getColSeqFromColDef(tblCol *plan.ColDef) string {
	return fmt.Sprintf("%d", tblCol.GetSeqnum())
}

type fullTextIndexPath struct {
	sortNode *plan.Node
	aggNode  *plan.Node
	scanNode *plan.Node
}

// resolveFullTextIndexPath finds the fulltext rewrite boundary. Projection
// rewrites retain their direct SCAN and SORT -> SCAN shapes. Aggregate rewrites
// locate the semantic AGG -> SCAN boundary through a single-input ancestor
// chain, so operators above that boundary do not affect index eligibility.
func (builder *QueryBuilder) resolveFullTextIndexPath(projNode *plan.Node) *fullTextIndexPath {
	if scanNode := builder.resolveScanNodeFromProject(projNode, 1); scanNode != nil {
		return &fullTextIndexPath{scanNode: scanNode}
	}

	if sortNode := builder.resolveSortNode(projNode, 1); sortNode != nil {
		if scanNode := builder.resolveScanNodeWithIndex(sortNode, 1); scanNode != nil {
			return &fullTextIndexPath{sortNode: sortNode, scanNode: scanNode}
		}
	}

	for node := projNode; node != nil && len(node.Children) == 1; node = builder.qry.Nodes[node.Children[0]] {
		if node.NodeType != plan.Node_AGG {
			continue
		}
		if scanNode := builder.resolveScanNodeWithIndex(node, 1); scanNode != nil {
			return &fullTextIndexPath{aggNode: node, scanNode: scanNode}
		}
	}
	return nil
}

func (builder *QueryBuilder) applyIndicesForProject(nodeID int32, projNode *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	defer builder.clearProjectGuard(projNode.NodeId)
	// FullText
	{
		// Rewrites either a direct projection path or the aggregate input/scan
		// boundary found through its single-input ancestors.
		path := builder.resolveFullTextIndexPath(projNode)
		// Turn json_extract comparisons on this scan into index probes BEFORE the
		// MATCH filters are collected below, so a probe is picked up by the very
		// same rewrite. Runs after scan-level index application, so adding a
		// fulltext filter here cannot suppress a master/regular index.
		if path != nil {
			builder.addJSONFulltextProbes(path.scanNode)
		}
		if path != nil && path.aggNode != nil {
			// agg node and scan node present
			// get the list of filter that is fulltext_match func
			filterids, filterFTIdxs := builder.getFullTextMatchFiltersFromScanNode(path.scanNode)

			// a MATCH wrapped in a larger expression drives the aggregate rewrite too:
			// `select count(*) from t where match(...) > 0.5` has no bare match at all.
			wrappedFTExprs, wrappedFTIdxs := builder.getWrappedFullTextMatches(
				nil, path.scanNode, filterids, nil)

			// apply the match indices (one unified pass handles a mix of MATCH + BM25)
			if len(filterids) > 0 || len(wrappedFTExprs) > 0 {
				return builder.applyIndicesForAggUsingFullTextIndex(nodeID, projNode, path.aggNode, path.scanNode,
					filterids, filterFTIdxs, wrappedFTExprs, wrappedFTIdxs, colRefCnt, idxColMap)
			}
		} else if path != nil {
			// get the list of project that is fulltext_match func
			projids, projFTIdxs := builder.getFullTextMatchFromProject(projNode, path.scanNode)

			// get the list of filter that is fulltext_match func
			filterids, filterFTIdxs := builder.getFullTextMatchFiltersFromScanNode(path.scanNode)

			// MATCHes nested inside a larger expression drive the rewrite too. Without this a
			// query whose ONLY match is wrapped -- `where match(...) > 0.5`, or a projected
			// `round(match(...),3)` -- never enters the rewrite at all and throws 20105.
			wrappedFTExprs, wrappedFTIdxs := builder.getWrappedFullTextMatches(
				projNode, path.scanNode, filterids, projids)

			// apply the match indices (one unified pass handles a mix of MATCH + BM25)
			if len(filterids) > 0 || len(projids) > 0 || len(wrappedFTExprs) > 0 {
				return builder.applyIndicesForProjectionUsingFullTextIndex(nodeID, projNode, path.sortNode, path.scanNode,
					filterids, filterFTIdxs, projids, projFTIdxs, wrappedFTExprs, wrappedFTIdxs, colRefCnt, idxColMap)
			}
		} else {
			// No single scan under this project: a JOIN in between takes the per-child route,
			// which has no project node and so never replaced a MATCH in the select list.
			// The children were rewritten before this node was visited, so resolve against the
			// scans they built.
			//
			// Deliberately does NOT return: this only rewrites expressions in place, it builds
			// no node, so the vector-index section below must still get its turn. Returning
			// here cost a query that both projects a MATCH over a join and orders by a vector
			// distance its vector index, silently falling back to brute force.
			builder.resolveProjectMatchesOverJoin(projNode, builder.resolveSortNode(projNode, 1))
		}
	}

	// 1. Vector Index Check
	// Handle Queries like
	// SELECT id,embedding FROM tbl ORDER BY l2_distance(embedding, "[1,2,3]") LIMIT 10;
	//
	// ANN index rewrites use LIMIT as the candidate-search budget. That is not
	// compatible with SQL_CALC_FOUND_ROWS, which must count the complete exact
	// result before the top-level LIMIT. Keep the exact scan+sort plan instead.
	if !builder.sqlCalcFoundRows {
		vecCtx := builder.buildVectorSortContext(projNode)
		if vecCtx == nil {
			vecCtx = builder.buildVectorSortContextThroughJoin(projNode)
		}
		if vecCtx != nil {
			newNodeID, handled, err := builder.applyVectorIndexForSortContext(nodeID, vecCtx, colRefCnt, idxColMap)
			if handled || err != nil {
				return newNodeID, err
			}
		}
	}
	// 2. Regular Index Check
	{
		if ctx := builder.buildRegularIndexTopSortContext(projNode); ctx != nil {
			builder.applyRegularIndexTopSort(ctx)
		}
	}

	return nodeID, nil
}

// applyVectorIndexForSortContext runs the plugin-mediated vector rewrite for an
// already-built context. Shared by both anchors: the PROJECT above a Top-K
// (applyIndicesForProject) and the Top-K SORT itself (applyIndicesForSort), so the two
// entry points cannot drift in which algorithms they dispatch to.
//
// handled=true means the caller must return immediately — either a plugin rewrote the
// tree, or this is a vector shape over a table with no vector index and there is nothing
// further to try.
func (builder *QueryBuilder) applyVectorIndexForSortContext(
	nodeID int32,
	vecCtx *vectorSortContext,
	colRefCnt map[[2]int32]int,
	idxColMap map[[2]int32]*plan.Expr,
) (int32, bool, error) {
	if vecCtx.projNode == nil && idxColMap == nil {
		// A sort-anchored rewrite publishes its column remap through idxColMap — that is
		// the only way ancestors learn the CTE's distance column became the index score.
		// With no map the rewritten tree would keep dangling references to a PROJECT that
		// is no longer in the plan, so decline instead of writing to a nil map (a panic)
		// or dropping the remap (a wrong plan). The planner always supplies one; the
		// plugin-facing ApplyForSortOpts.IdxColMap does not enforce it.
		return nodeID, false, nil
	}
	multiTableIndexes, err := builder.collectVectorIndexes(vecCtx.scanNode)
	if err != nil {
		return nodeID, true, err
	}
	if len(multiTableIndexes) == 0 {
		// Matches the original project-anchored behaviour: a vector Top-K over a
		// table with no vector index is done here, it is not a regular-index shape.
		return nodeID, true, nil
	}
	// Preserve the dependency closure before a plugin is allowed to rewrite
	// away the owning TABLE_SCAN. The final plan shape cannot be used as the
	// source of truth after an index-only rewrite.
	if err := builder.recordPreparedPluginDependencies(vecCtx.scanNode); err != nil {
		return nodeID, true, err
	}

	multiTableIndexKeys := make([]string, 0, len(multiTableIndexes))
	for key := range multiTableIndexes {
		multiTableIndexKeys = append(multiTableIndexKeys, key)
	}

	// Plugin-mediated dispatch — every plugin-registered vector
	// index exposes Hooks.ApplyForSort, which routes back into the
	// builder's per-algo redirect (plugin_builder.go) and then into
	// the real body in apply_indices_<algo>.go. The pluginless
	// hardcoded switch was the bug surface that let CAGRA / IVF-PQ
	// drift behind HNSW / IVF-FLAT; one loop here keeps the algo
	// set canonical.
	opts := planplugin.ApplyForSortOpts{ColRefCnt: colRefCnt, IdxColMap: idxColMap}
	for _, multiTableIndexKey := range multiTableIndexKeys {
		multiTableIndex := multiTableIndexes[multiTableIndexKey]
		// Defence in depth: collectVectorIndexes already filters
		// via IsVectorIndexAlgo, but the dispatch site re-checks so
		// a future change that loosens collectVectorIndexes can't
		// silently route fulltext (or any other non-vector
		// plugin-registered algo) through the vector ANN rewrite
		// path. indexplugin.Get alone is not sufficient — fulltext
		// is plugin-registered too.
		if !indexplugin.IsVectorIndexAlgo(multiTableIndex.IndexAlgo) {
			continue
		}
		p, ok := indexplugin.Get(multiTableIndex.IndexAlgo)
		if !ok {
			continue
		}
		vctxExt, mtiExt := toPlanplugin(vecCtx, multiTableIndex)
		newNodeID, applied, err := p.Plan().ApplyForSort(builder, vctxExt, mtiExt, nodeID, opts)
		if err != nil {
			return newNodeID, true, err
		}
		if applied {
			return newNodeID, true, nil
		}
	}

	builder.stabilizeExactVectorSort(vecCtx)
	return nodeID, false, nil
}

// markProjectAnchoredSort records that the PROJECT above this Top-K SORT will anchor the
// vector rewrite, so the SORT-anchored entry point must leave it alone.
func (builder *QueryBuilder) markProjectAnchoredSort(sortID int32) {
	if builder.projectAnchoredSorts == nil {
		builder.projectAnchoredSorts = make(map[int32]struct{})
	}
	builder.projectAnchoredSorts[sortID] = struct{}{}
}

// applyIndicesForSort is the SORT-anchored entry point for the vector rewrite. It fires
// for a Top-K whose parent is not a PROJECT — under an outer ORDER BY (#25967) or as a
// join input (#25974) — shapes the project-anchored path cannot see.
//
// It may return a DIFFERENT node id than it was given: the rewritten subtree replaces the
// Top-K, and applyIndices assigns the result back into the parent's Children. Dropping the
// return value orphans the rewrite.
func (builder *QueryBuilder) applyIndicesForSort(nodeID int32, sortNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	if builder.sqlCalcFoundRows {
		// The sort-anchored vector rewrite has the same ANN candidate cap as the
		// project-anchored path. Preserve the complete exact input stream.
		return nodeID, nil
	}
	if _, ok := builder.projectAnchoredSorts[nodeID]; ok {
		// The PROJECT above will anchor this Top-K with full column information.
		return nodeID, nil
	}
	defer builder.clearProjectGuard(nodeID)
	vecCtx := builder.buildVectorSortContextFromSort(sortNode)
	if vecCtx == nil {
		return nodeID, nil
	}
	newNodeID, _, err := builder.applyVectorIndexForSortContext(nodeID, vecCtx, colRefCnt, idxColMap)
	return newNodeID, err
}

func (builder *QueryBuilder) buildRegularIndexTopSortContext(projNode *plan.Node) *regularIndexTopSortContext {
	sortNode := builder.resolveSortNode(projNode, 1)
	if sortNode == nil || len(sortNode.OrderBy) != 1 || sortNode.Limit == nil || sortNode.Offset != nil || sortNode.RankOption != nil {
		return nil
	}

	scanNode := builder.resolveScanNodeWithIndex(sortNode, 1)
	if scanNode == nil || len(scanNode.OrderBy) != 0 {
		return nil
	}
	if !builder.regularIndexScanAllowedByOrderHints(scanNode) {
		return nil
	}

	if len(sortNode.Children) != 1 {
		return nil
	}
	sortProjectNode := builder.qry.Nodes[sortNode.Children[0]]
	if sortProjectNode.NodeType != plan.Node_PROJECT || len(sortProjectNode.BindingTags) == 0 {
		return nil
	}

	orderByCol := sortNode.OrderBy[0].Expr.GetCol()
	if orderByCol == nil || orderByCol.RelPos != sortProjectNode.BindingTags[0] || int(orderByCol.ColPos) >= len(sortProjectNode.ProjectList) {
		return nil
	}

	orderExpr := sortProjectNode.ProjectList[orderByCol.ColPos]
	if !encodedOrderMatchesSQLOrder(orderExpr) {
		return nil
	}
	orderExprCol := orderExpr.GetCol()
	if !canUseRegularIndexHiddenSortKey(scanNode, orderExprCol) {
		return nil
	}
	pushOrderedLimit := canPushRegularIndexOrderedLimit(scanNode)
	if !pushOrderedLimit && isPositiveLiteralLimit(sortNode.Limit) {
		pushOrderedLimit = builder.rewriteRegularIndexCursorRangeFilter(scanNode)
	}

	return &regularIndexTopSortContext{
		sortNode:         sortNode,
		sortProjectNode:  sortProjectNode,
		scanNode:         scanNode,
		pushOrderedLimit: pushOrderedLimit,
	}
}

func usableRegularHintIndex(idxDef *plan.IndexDef) bool {
	return idxDef != nil &&
		idxDef.TableExist &&
		catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) &&
		!isSpatialIndexDef(idxDef) &&
		regularIndexPrefixMetadataUsable(idxDef) &&
		!regularIndexHasDeclaredPrefix(idxDef)
}

func indexLeadingColumnsMatch(idxDef *plan.IndexDef, tableDef *plan.TableDef, colPositions []int32) bool {
	if idxDef == nil || tableDef == nil || len(colPositions) == 0 || len(idxDef.Parts) < len(colPositions) {
		return false
	}
	for i, colPos := range colPositions {
		if colPos < 0 || int(colPos) >= len(tableDef.Cols) || catalog.ResolveAlias(idxDef.Parts[i]) != tableDef.Cols[colPos].Name {
			return false
		}
	}
	return true
}

func indexOrderColumnsMatch(idxDef *plan.IndexDef, scanNode *plan.Node, colPositions []int32) bool {
	if idxDef == nil || scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 || len(colPositions) == 0 {
		return false
	}

	fixedPrefix := 0
	for fixedPrefix < len(idxDef.Parts) && indexPartFixedByEquality(idxDef.Parts[fixedPrefix], scanNode) {
		fixedPrefix++
	}

	nextPart := fixedPrefix
	for _, colPos := range colPositions {
		if colPos < 0 || int(colPos) >= len(scanNode.TableDef.Cols) {
			return false
		}
		colName := scanNode.TableDef.Cols[colPos].Name
		fixedOrderColumn := false
		for i := 0; i < fixedPrefix; i++ {
			if catalog.ResolveAlias(idxDef.Parts[i]) == colName {
				fixedOrderColumn = true
				break
			}
		}
		if fixedOrderColumn {
			continue
		}
		if nextPart >= len(idxDef.Parts) || catalog.ResolveAlias(idxDef.Parts[nextPart]) != colName {
			return false
		}
		nextPart++
	}
	return true
}

func indexPartFixedByEquality(part string, scanNode *plan.Node) bool {
	colPos, ok := scanNode.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
	if !ok {
		return false
	}
	tag := scanNode.BindingTags[0]
	for _, filter := range scanNode.FilterList {
		fn := filter.GetF()
		if fn == nil || fn.Func == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
			continue
		}
		leftCol := fn.Args[0].GetCol()
		rightCol := fn.Args[1].GetCol()
		if leftCol != nil && leftCol.RelPos == tag && leftCol.ColPos == colPos && isScanInvariantRuntimeConstExpr(fn.Args[1]) {
			return true
		}
		if rightCol != nil && rightCol.RelPos == tag && rightCol.ColPos == colPos && isScanInvariantRuntimeConstExpr(fn.Args[0]) {
			return true
		}
	}
	return false
}

// isScanInvariantRuntimeConstExpr is stricter than isRuntimeConstExpr: an
// expression can be independent of table columns while still producing a new
// value for every row. Such volatile expressions cannot fix an index prefix to
// one value for the duration of a scan.
func isScanInvariantRuntimeConstExpr(expr *plan.Expr) bool {
	return !containsVolatileFunction(expr) && isRuntimeConstExpr(expr)
}

func containsVolatileFunction(expr *plan.Expr) bool {
	if expr == nil {
		return true
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Sub:
		return true
	case *plan.Expr_F:
		if exprImpl.F == nil || exprImpl.F.Func == nil {
			return true
		}
		overload, ok := function.GetFunctionByIdWithoutError(exprImpl.F.Func.Obj)
		if !ok || overload.CannotFold() {
			return true
		}
		for _, arg := range exprImpl.F.Args {
			if containsVolatileFunction(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		if exprImpl.List == nil {
			return true
		}
		for _, item := range exprImpl.List.List {
			if containsVolatileFunction(item) {
				return true
			}
		}
	}

	return false
}

// ContainsVolatileFunction reports whether an expression can produce a new
// value on repeated evaluation. Storage-side filtering uses it to keep such
// predicates at the row-level execution boundary.
func ContainsVolatileFunction(expr *plan.Expr) bool {
	return containsVolatileFunction(expr)
}

type forceIndexScope int

const (
	forceIndexForOrder forceIndexScope = iota
	forceIndexForGroup
)

type forceIndexRequirement struct {
	scope      forceIndexScope
	columns    []*plan.Expr
	orderFlag  plan.OrderBySpec_OrderByFlag
	limit      *plan.Expr
	canPushLim bool
	block      *BindContext
}

func (builder *QueryBuilder) applyForceIndexHints(nodeID int32, requirements []forceIndexRequirement, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	return builder.applyForceIndexHintsWithMemo(nodeID, requirements, colRefCnt, idxColMap, make(map[forceIndexMemoKey]int32))
}

type forceIndexMemoKey struct {
	nodeID      int32
	requirement string
}

func forceIndexRequirementKey(requirements []forceIndexRequirement) string {
	var key strings.Builder
	for _, requirement := range requirements {
		fmt.Fprintf(&key, "%d:%p:%d:%t:", requirement.scope, requirement.block, requirement.orderFlag, requirement.canPushLim)
		if requirement.limit != nil {
			fmt.Fprintf(&key, "limit=%s:", requirement.limit.String())
		}
		for _, expr := range requirement.columns {
			if col := expr.GetCol(); col != nil {
				fmt.Fprintf(&key, "%d/%d,", col.RelPos, col.ColPos)
			} else {
				fmt.Fprintf(&key, "%s,", expr.String())
			}
		}
		key.WriteByte(';')
	}
	return key.String()
}

func (builder *QueryBuilder) applyForceIndexHintsWithMemo(nodeID int32, requirements []forceIndexRequirement, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, memo map[forceIndexMemoKey]int32) (int32, error) {
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return nodeID, nil
	}
	nodeBlock := builder.ctxByNode[nodeID]

	localRequirements := requirements
	switch node.NodeType {
	case plan.Node_SORT:
		if len(node.OrderBy) > 0 {
			columns := make([]*plan.Expr, len(node.OrderBy))
			flag := node.OrderBy[0].Flag
			sameDirection := true
			for i, orderBy := range node.OrderBy {
				columns[i] = DeepCopyExpr(orderBy.Expr)
				sameDirection = sameDirection && orderBy.Flag == flag
			}
			if sameDirection {
				localRequirements = append(slices.Clone(requirements), forceIndexRequirement{
					scope: forceIndexForOrder, columns: columns, orderFlag: flag,
					limit: node.Limit, canPushLim: node.Offset == nil && node.RankOption == nil, block: nodeBlock,
				})
			}
		}
	case plan.Node_AGG:
		if len(node.GroupBy) > 0 {
			columns := make([]*plan.Expr, len(node.GroupBy))
			for i, groupBy := range node.GroupBy {
				columns[i] = DeepCopyExpr(groupBy)
			}
			localRequirements = append(slices.Clone(requirements), forceIndexRequirement{
				scope: forceIndexForGroup, columns: columns, block: nodeBlock,
			})
		}
	}

	key := forceIndexMemoKey{nodeID: nodeID, requirement: forceIndexRequirementKey(localRequirements)}
	if rewritten, ok := memo[key]; ok {
		return rewritten, nil
	}

	if node.NodeType == plan.Node_TABLE_SCAN {
		rewritten, err := builder.applyForceIndexHintToScan(node, localRequirements, colRefCnt, idxColMap)
		if err == nil {
			memo[key] = rewritten
		}
		return rewritten, err
	}

	childRequirements := localRequirements
	if node.NodeType == plan.Node_PROJECT {
		childRequirements = translateForceIndexRequirementsThroughProject(node, localRequirements)
	}
	for i, childID := range node.Children {
		if node.NodeType == plan.Node_PROJECT && len(childRequirements) > 0 && childRequirements[0].block != nil &&
			builder.ctxByNode[childID] != childRequirements[0].block {
			newChildID, err := builder.applyForceIndexHintsWithMemo(childID, nil, colRefCnt, idxColMap, memo)
			if err != nil {
				return -1, err
			}
			node.Children[i] = newChildID
			continue
		}
		newChildID, err := builder.applyForceIndexHintsWithMemo(childID, childRequirements, colRefCnt, idxColMap, memo)
		if err != nil {
			return -1, err
		}
		node.Children[i] = newChildID
	}
	replaceColumnsForNode(node, idxColMap)
	memo[key] = nodeID
	return nodeID, nil
}

func translateForceIndexRequirementsThroughProject(projectNode *plan.Node, requirements []forceIndexRequirement) []forceIndexRequirement {
	if projectNode == nil || len(projectNode.BindingTags) == 0 {
		return requirements
	}
	translated := make([]forceIndexRequirement, 0, len(requirements))
	projectTag := projectNode.BindingTags[0]
	for _, requirement := range requirements {
		copyRequirement := requirement
		copyRequirement.columns = make([]*plan.Expr, len(requirement.columns))
		valid := true
		for i, expr := range requirement.columns {
			col := expr.GetCol()
			if col != nil && col.RelPos == projectTag {
				if col.ColPos < 0 || int(col.ColPos) >= len(projectNode.ProjectList) {
					valid = false
					break
				}
				copyRequirement.columns[i] = DeepCopyExpr(projectNode.ProjectList[col.ColPos])
			} else {
				copyRequirement.columns[i] = DeepCopyExpr(expr)
			}
		}
		if valid {
			translated = append(translated, copyRequirement)
		}
	}
	return translated
}

func (builder *QueryBuilder) applyForceIndexHintToScan(scanNode *plan.Node, requirements []forceIndexRequirement, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	if scanNode == nil {
		return -1, nil
	}
	if scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 || scanNode.IndexScanInfo.IsIndexScan {
		return scanNode.NodeId, nil
	}
	if builder.isScanProtected(scanNode.NodeId) {
		return scanNode.NodeId, nil
	}
	hintSet := builder.indexHintsByScan[scanNode.NodeId]
	if hintSet == nil {
		return scanNode.NodeId, nil
	}
	for _, requirement := range requirements {
		scope := hintSet.order
		if requirement.scope == forceIndexForGroup {
			scope = hintSet.group
		}
		colPositions := make([]int32, len(requirement.columns))
		matchesScan := true
		for i, expr := range requirement.columns {
			col := expr.GetCol()
			if col == nil || col.RelPos != scanNode.BindingTags[0] {
				matchesScan = false
				break
			}
			colPositions[i] = col.ColPos
		}
		if !matchesScan {
			continue
		}
		if !scope.forceSpecified {
			continue
		}

		// FORCE PRIMARY keeps the base-table access and blocks ordinary secondary-index rewrites.
		if _, forcePrimary := scope.force[strings.ToLower(PrimaryKeyName)]; forcePrimary &&
			(!hintSet.join.forceSpecified || indexAllowedByHintScope(PrimaryKeyName, hintSet.join)) {
			builder.protectedScans[scanNode.NodeId]++
			return scanNode.NodeId, nil
		}
		indexes := filterIndexesByHintScope(scanNode.TableDef.Indexes, scope)
		for _, idxDef := range indexes {
			if !usableRegularHintIndex(idxDef) ||
				(hintSet.join.forceSpecified && !indexAllowedByHintScope(idxDef.IndexName, hintSet.join)) {
				continue
			}
			columnsMatch := indexLeadingColumnsMatch(idxDef, scanNode.TableDef, colPositions)
			if requirement.scope == forceIndexForOrder {
				columnsMatch = indexOrderColumnsMatch(idxDef, scanNode, colPositions)
			}
			if !columnsMatch {
				continue
			}
			accessNodeID, idxNodeID, covering, err := builder.tryHintedIndexAccess(idxDef, scanNode, colRefCnt, idxColMap)
			if err != nil {
				return -1, err
			}
			if accessNodeID == -1 {
				continue
			}
			builder.protectedScans[scanNode.NodeId]++
			if requirement.scope == forceIndexForOrder && encodedOrderMatchesSQLOrder(requirement.columns...) {
				idxNode := builder.qry.Nodes[idxNodeID]
				idxNode.OrderBy = []*plan.OrderBySpec{{
					Expr: GetColExpr(idxNode.TableDef.Cols[0].Typ, idxNode.BindingTags[0], 0),
					Flag: requirement.orderFlag,
				}}
				if covering && len(idxNode.FilterList) == 0 && requirement.limit != nil && requirement.canPushLim {
					builder.applyRegularIndexOrderedLimitParam(idxNode, idxNode.OrderBy[0], requirement.limit)
				}
			}
			return accessNodeID, nil
		}
		if hintSet.join.forceSpecified {
			return builder.applyForcedHintAccessToScan(scanNode, hintSet.join, colRefCnt, idxColMap)
		}
		if requirement.scope == forceIndexForOrder && hintSet.scan.forceSpecified {
			// An unscoped FORCE INDEX constrains access as well as ordering. If the
			// named index cannot provide this ORDER BY, retain the forced access and
			// let the Sort enforce ordering. An explicit FORCE INDEX FOR ORDER BY has
			// no scan-scope fallback and therefore leaves base access unchanged.
			return builder.applyForcedHintAccessToScan(scanNode, hintSet.scan, colRefCnt, idxColMap)
		}
		// A FORCE hint that cannot provide the requested ordering/grouping still
		// excludes other secondary indexes from replacing the base scan.
		builder.protectedScans[scanNode.NodeId]++
		return scanNode.NodeId, nil
	}
	return scanNode.NodeId, nil
}

// applyForcedHintAccessToScan resolves a forced access scope before a covering
// access publishes hidden-column replacements to its ancestors. JOIN access
// has the same precedence that applyIndicesForJoins historically enforced, but
// choosing it here keeps the replacement atomic.
func (builder *QueryBuilder) applyForcedHintAccessToScan(scanNode *plan.Node, scope indexHintScopeSet,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	if indexAllowedByHintScope(PrimaryKeyName, scope) {
		builder.protectedScans[scanNode.NodeId]++
		return scanNode.NodeId, nil
	}
	for _, idxDef := range filterIndexesByHintScope(scanNode.TableDef.Indexes, scope) {
		if !usableRegularHintIndex(idxDef) {
			continue
		}
		accessNodeID, _, _, err := builder.tryHintedIndexAccess(idxDef, scanNode, colRefCnt, idxColMap)
		if err != nil {
			return -1, err
		}
		if accessNodeID == -1 {
			continue
		}
		builder.protectedScans[scanNode.NodeId]++
		return accessNodeID, nil
	}
	builder.protectedScans[scanNode.NodeId]++
	return scanNode.NodeId, nil
}

func (builder *QueryBuilder) tryHintedIndexAccess(idxDef *plan.IndexDef, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, int32, bool, error) {
	idxNodeID, err := builder.tryHintedCoveringIndexScan(idxDef, node, colRefCnt, idxColMap)
	if err != nil {
		return -1, -1, false, err
	}
	if idxNodeID != -1 {
		return idxNodeID, idxNodeID, true, nil
	}
	accessNodeID, idxNodeID, err := builder.buildHintedIndexBackfillJoin(idxDef, node)
	return accessNodeID, idxNodeID, false, err
}

func (builder *QueryBuilder) buildHintedIndexBackfillJoin(idxDef *plan.IndexDef, node *plan.Node) (int32, int32, error) {
	if !usableRegularHintIndex(idxDef) || node == nil || node.TableDef == nil || node.TableDef.Pkey == nil || len(node.BindingTags) == 0 {
		return -1, -1, nil
	}
	snapshot := node.ScanSnapshot
	if snapshot == nil {
		snapshot = &Snapshot{}
	}
	pkIdx, ok := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
	if !ok {
		return -1, -1, nil
	}
	idxTag := builder.genNewBindTag()
	idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(node.ObjRef, idxDef.IndexTableName, snapshot)
	if err != nil {
		return -1, -1, err
	}
	if idxObjRef == nil || idxTableDef == nil {
		return -1, -1, moerr.NewInternalErrorf(builder.GetContext(), "index table metadata for %s is unavailable", idxDef.IndexName)
	}
	if len(idxTableDef.Cols) < 2 {
		return -1, -1, moerr.NewInternalErrorf(builder.GetContext(), "index table metadata for %s has invalid columns", idxDef.IndexName)
	}
	pkExpr := GetColExpr(node.TableDef.Cols[pkIdx].Typ, node.BindingTags[0], pkIdx)
	joinCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(pkExpr),
		GetColExpr(pkExpr.Typ, idxTag, 1),
	})
	if err != nil {
		return -1, -1, err
	}
	builder.addNameByColRef(idxTag, idxTableDef)
	idxNodeID := builder.appendNode(&plan.Node{
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     idxTableDef,
		ObjRef:       idxObjRef,
		ParentObjRef: DeepCopyObjectRef(node.ObjRef),
		IndexScanInfo: plan.IndexScanInfo{
			IsIndexScan: true, IndexName: idxDef.IndexName, BelongToTable: node.ObjRef.ObjName,
			Parts: slices.Clone(idxDef.Parts), IsUnique: idxDef.Unique, IndexTableName: idxDef.IndexTableName,
		},
		BindingTags:  []int32{idxTag},
		ScanSnapshot: node.ScanSnapshot,
	}, builder.ctxByNode[node.NodeId])
	builder.inheritIndexHints(idxNodeID, node.NodeId)
	forceScanNodeStatsTP(idxNodeID, builder)
	joinNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{node.NodeId, idxNodeID},
		JoinType: plan.Node_INDEX,
		OnList:   []*plan.Expr{joinCond},
	}, builder.ctxByNode[node.NodeId])
	return joinNodeID, idxNodeID, nil
}

func (builder *QueryBuilder) tryHintedCoveringIndexScan(idxDef *plan.IndexDef, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	if !usableRegularHintIndex(idxDef) || node == nil || len(node.BindingTags) == 0 {
		return -1, nil
	}
	for i, col := range node.TableDef.Cols {
		if colRefCnt[[2]int32{node.BindingTags[0], int32(i)}] == 0 {
			continue
		}
		covered := false
		for _, part := range idxDef.Parts {
			if catalog.ResolveAlias(part) == col.Name {
				covered = true
				break
			}
		}
		if idxDef.Unique && col.Name == node.TableDef.Pkey.PkeyColName {
			covered = true
		}
		if !covered {
			return -1, nil
		}
	}

	snapshot := node.ScanSnapshot
	if snapshot == nil {
		snapshot = &Snapshot{}
	}
	idxTag := builder.genNewBindTag()
	idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(node.ObjRef, idxDef.IndexTableName, snapshot)
	if err != nil {
		return -1, err
	}
	if idxObjRef == nil || idxTableDef == nil {
		return -1, moerr.NewInternalErrorf(builder.GetContext(), "index table metadata for %s is unavailable", idxDef.IndexName)
	}
	if len(idxTableDef.Cols) < 2 {
		return -1, moerr.NewInternalErrorf(builder.GetContext(), "index table metadata for %s has invalid columns", idxDef.IndexName)
	}
	hiddenKey := GetColExpr(idxTableDef.Cols[0].Typ, idxTag, 0)
	colMap := make(map[[2]int32]*plan.Expr, len(idxDef.Parts))
	for i, part := range idxDef.Parts {
		colName := catalog.ResolveAlias(part)
		colIdx, ok := node.TableDef.Name2ColIndex[colName]
		if !ok {
			continue
		}
		if colName == node.TableDef.Pkey.PkeyColName {
			colMap[[2]int32{node.BindingTags[0], colIdx}] = GetColExpr(idxTableDef.Cols[1].Typ, idxTag, 1)
		} else if len(idxDef.Parts) == 1 {
			colMap[[2]int32{node.BindingTags[0], colIdx}] = DeepCopyExpr(hiddenKey)
		} else {
			mappedExpr, bindErr := MakeSerialExtractExpr(builder.GetContext(), DeepCopyExpr(hiddenKey), node.TableDef.Cols[colIdx].Typ, int64(i))
			if bindErr != nil {
				return -1, bindErr
			}
			colMap[[2]int32{node.BindingTags[0], colIdx}] = mappedExpr
		}
	}
	if idxDef.Unique {
		pkIdx := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
		colMap[[2]int32{node.BindingTags[0], pkIdx}] = GetColExpr(idxTableDef.Cols[1].Typ, idxTag, 1)
	}
	newFilters := make([]*plan.Expr, len(node.FilterList))
	for i, filter := range node.FilterList {
		if !exprUsesOnlyMappedCols(filter, colMap) {
			return -1, nil
		}
		newFilters[i] = replaceColumnsForExpr(DeepCopyExpr(filter), colMap)
	}
	for key, expr := range colMap {
		idxColMap[key] = expr
	}
	builder.addNameByColRef(idxTag, idxTableDef)

	idxNodeID := builder.appendNode(&plan.Node{
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     idxTableDef,
		ObjRef:       idxObjRef,
		ParentObjRef: DeepCopyObjectRef(node.ObjRef),
		IndexScanInfo: plan.IndexScanInfo{
			IsIndexScan: true, IndexName: idxDef.IndexName, BelongToTable: node.ObjRef.ObjName,
			Parts: slices.Clone(idxDef.Parts), IsUnique: idxDef.Unique, IndexTableName: idxDef.IndexTableName,
		},
		FilterList:   newFilters,
		BindingTags:  []int32{idxTag},
		ScanSnapshot: node.ScanSnapshot,
	}, builder.ctxByNode[node.NodeId])
	builder.inheritIndexHints(idxNodeID, node.NodeId)
	forceScanNodeStatsTP(idxNodeID, builder)
	return idxNodeID, nil
}

func canUseRegularIndexHiddenSortKey(scanNode *plan.Node, orderByCol *plan.ColRef) bool {
	if scanNode == nil || orderByCol == nil || !scanNode.IndexScanInfo.IsIndexScan || scanNode.IndexScanInfo.IsUnique || len(scanNode.BindingTags) == 0 {
		return false
	}

	// Non-unique regular secondary index tables are laid out as:
	//   col0 = hidden serialized key (index parts + base-table PK)
	//   col1 = base-table PK
	// Only under this layout can ORDER BY PK be rewritten to the hidden key safely.
	if len(scanNode.TableDef.Cols) < 2 ||
		scanNode.TableDef.Cols[0].Name != catalog.IndexTableIndexColName ||
		scanNode.TableDef.Cols[1].Name != catalog.IndexTablePrimaryColName {
		return false
	}

	if len(scanNode.IndexScanInfo.Parts) < 2 || len(scanNode.FilterList) == 0 {
		return false
	}

	if orderByCol.RelPos != scanNode.BindingTags[0] || orderByCol.ColPos != 1 {
		return false
	}

	numKeyParts := len(scanNode.IndexScanInfo.Parts) - 1
	return isRegularIndexFullPrefixEquality(scanNode.FilterList[0], numKeyParts)
}

func canPushRegularIndexOrderedLimit(scanNode *plan.Node) bool {
	if scanNode == nil || len(scanNode.IndexScanInfo.Parts) < 2 || len(scanNode.FilterList) != 1 {
		return false
	}
	// Static reader limit is valid only when index scan candidates exactly match the SQL filter.
	numKeyParts := len(scanNode.IndexScanInfo.Parts) - 1
	return isRegularIndexFullPrefixEquality(scanNode.FilterList[0], numKeyParts)
}

// encodedOrderMatchesSQLOrder reports whether encoded regular-index keys and
// storage metadata are proven to share the logical SQL ordering. Scalar float
// encoding has an identity order over NaN payloads, while SQL makes all NaNs
// peers and keeps them last in both directions, so encoded ordering cannot
// replace the logical sort or drive early truncation.
func encodedOrderMatchesSQLOrder(orderExprs ...*plan.Expr) bool {
	for _, expr := range orderExprs {
		if expr == nil {
			return false
		}
		switch types.T(expr.Typ.Id) {
		case types.T_float32, types.T_float64:
			return false
		}
	}
	return len(orderExprs) > 0
}

func isRegularIndexFullPrefixEquality(expr *plan.Expr, numKeyParts int) bool {
	if numKeyParts <= 0 || expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func.ObjName != "prefix_eq" || len(fn.Args) != 2 {
		return false
	}
	serialFn := fn.Args[1].GetF()
	return serialFn != nil && serialFn.Func.ObjName == indexTableComparisonSerialFunc() && len(serialFn.Args) == numKeyParts
}

func (builder *QueryBuilder) rewriteRegularIndexCursorRangeFilter(scanNode *plan.Node) bool {
	if scanNode == nil || scanNode.TableDef == nil || !scanNode.IndexScanInfo.IsIndexScan || scanNode.IndexScanInfo.IsUnique ||
		len(scanNode.BindingTags) == 0 || len(scanNode.IndexScanInfo.Parts) < 2 || len(scanNode.TableDef.Cols) < 2 ||
		len(scanNode.FilterList) != 2 || scanNode.TableDef.Cols[0].Name != catalog.IndexTableIndexColName ||
		scanNode.TableDef.Cols[1].Name != catalog.IndexTablePrimaryColName {
		return false
	}

	numKeyParts := len(scanNode.IndexScanInfo.Parts) - 1
	prefixFilter := scanNode.FilterList[0]
	if !isRegularIndexFullPrefixEquality(prefixFilter, numKeyParts) {
		return false
	}
	prefixFn := prefixFilter.GetF()
	prefixSerial := prefixFn.Args[1].GetF()

	cursorFilter := scanNode.FilterList[1]
	cursorFn := cursorFilter.GetF()
	if cursorFn == nil {
		return false
	}
	cursorCol, _ := classifyRangeBound(cursorFn)
	cursorValue := rangeFilterConstValue(cursorFn)
	if cursorCol == nil || cursorCol.RelPos != scanNode.BindingTags[0] || cursorCol.ColPos != 1 ||
		!isStableRegularIndexCursor(cursorValue) {
		return false
	}
	pkType := scanNode.TableDef.Cols[1].Typ
	if !regularIndexCursorTypeMatches(cursorValue.Typ, pkType) {
		return false
	}

	boundArgs := append(DeepCopyExprList(prefixSerial.Args), DeepCopyExpr(cursorValue))
	bound, err := BindFuncExprImplByPlanExpr(builder.GetContext(), indexTableComparisonSerialFunc(), boundArgs)
	if err != nil {
		return false
	}
	prefix := DeepCopyExpr(prefixFn.Args[1])

	var left, right *plan.Expr
	var flag uint8
	switch canonicalRangeOp(cursorFn) {
	case "<":
		left, right, flag = prefix, bound, 2
	case "<=":
		left, right = prefix, bound
	case ">":
		left, right, flag = bound, prefix, 1
	case ">=":
		left, right = bound, prefix
	default:
		return false
	}

	rangeFilter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_in_range", []*plan.Expr{
		DeepCopyExpr(prefixFn.Args[0]),
		left,
		right,
		MakePlan2Uint8ConstExprWithType(flag),
	})
	if err != nil {
		return false
	}
	rangeFilter.Selectivity = prefixFilter.Selectivity * cursorFilter.Selectivity
	scanNode.FilterList[0] = rangeFilter
	return true
}

func regularIndexCursorTypeMatches(cursorType, pkType plan.Type) bool {
	if cursorType.Id != pkType.Id {
		return false
	}
	switch types.T(pkType.Id) {
	case types.T_float32, types.T_float64:
		// NaN does not form the same total order under SQL comparison and serialized-key ordering.
		return false
	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		return cursorType.Width == pkType.Width && cursorType.Scale == pkType.Scale
	default:
		return true
	}
}

func isStableRegularIndexCursor(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V:
		return true
	case *plan.Expr_F:
		return exprImpl.F != nil && exprImpl.F.Func != nil &&
			(exprImpl.F.Func.ObjName == "cast" || exprImpl.F.Func.ObjName == "cast_strict") &&
			len(exprImpl.F.Args) > 0 && isStableRegularIndexCursor(exprImpl.F.Args[0])
	default:
		return false
	}
}

func hasTopValueMessage(node *plan.Node) bool {
	for i := range node.SendMsgList {
		if node.SendMsgList[i].MsgType == int32(message.MsgTopValue) {
			return true
		}
	}
	return false
}

func (builder *QueryBuilder) applyRegularIndexTopSort(ctx *regularIndexTopSortContext) {
	hiddenKeyName := builder.getColName(ctx.sortNode.OrderBy[0].Expr.GetCol())
	if hiddenKeyName == "" {
		hiddenKeyName = catalog.IndexTableIndexColName
	}

	projectHiddenKeyExpr := GetColExpr(ctx.scanNode.TableDef.Cols[0].Typ, ctx.scanNode.BindingTags[0], 0)
	projectHiddenKeyExpr.GetCol().Name = hiddenKeyName

	sortProjectTag := ctx.sortProjectNode.BindingTags[0]
	sortProjectColPos := int32(len(ctx.sortProjectNode.ProjectList))
	ctx.sortProjectNode.ProjectList = append(ctx.sortProjectNode.ProjectList, projectHiddenKeyExpr)
	builder.nameByColRef[[2]int32{sortProjectTag, sortProjectColPos}] = hiddenKeyName

	sortHiddenKeyExpr := GetColExpr(ctx.scanNode.TableDef.Cols[0].Typ, sortProjectTag, sortProjectColPos)
	sortHiddenKeyExpr.GetCol().Name = hiddenKeyName
	ctx.sortNode.OrderBy[0].Expr = sortHiddenKeyExpr

	scanHiddenKeyExpr := GetColExpr(ctx.scanNode.TableDef.Cols[0].Typ, ctx.scanNode.BindingTags[0], 0)
	scanHiddenKeyExpr.GetCol().Name = ctx.scanNode.TableDef.Cols[0].Name
	ctx.scanNode.OrderBy = append(ctx.scanNode.OrderBy, &plan.OrderBySpec{
		Expr: scanHiddenKeyExpr,
		Flag: ctx.sortNode.OrderBy[0].Flag,
	})
	if ctx.sortNode.Offset == nil && ctx.sortNode.RankOption == nil && ctx.pushOrderedLimit {
		builder.applyRegularIndexOrderedLimitParam(ctx.scanNode, ctx.scanNode.OrderBy[len(ctx.scanNode.OrderBy)-1], ctx.sortNode.Limit)
	}

	if !hasTopValueMessage(ctx.sortNode) {
		msgHeader := plan.MsgHeader{
			MsgTag:  builder.genNewMsgTag(),
			MsgType: int32(message.MsgTopValue),
		}
		ctx.sortNode.SendMsgList = append([]plan.MsgHeader{msgHeader}, ctx.sortNode.SendMsgList...)
		ctx.scanNode.RecvMsgList = append(ctx.scanNode.RecvMsgList, msgHeader)
	}
}

func applyRegularIndexOrderedLimitParam(scanNode *plan.Node, orderBy *plan.OrderBySpec, limit *plan.Expr) {
	if scanNode == nil || orderBy == nil || !isPositiveLiteralLimit(limit) {
		return
	}
	scanNode.IndexReaderParam = &plan.IndexReaderParam{
		OrderBy: []*plan.OrderBySpec{DeepCopyOrderBySpec(orderBy)},
		Limit:   DeepCopyExpr(limit),
	}
}

func (builder *QueryBuilder) applyRegularIndexOrderedLimitParam(scanNode *plan.Node, orderBy *plan.OrderBySpec, limit *plan.Expr) {
	if builder.sqlCalcFoundRows {
		return
	}
	applyRegularIndexOrderedLimitParam(scanNode, orderBy, limit)
}

func isPositiveLiteralLimit(limit *plan.Expr) bool {
	limitValue, literal := getLiteralUint64(limit)
	return literal && limitValue > 0 && limitValue <= maxVectorIndexTopPushdownLimit
}

// detectFullTextGuard reserves the scan that the fulltext rewrite is going to consume, so
// applyIndicesForFilters leaves it alone instead of turning it into a secondary-index scan
// first.
//
// Its predicate must stay identical to the one applyIndicesForProject rewrites on, wrapped
// MATCHes included. A scan whose only MATCH is wrapped -- a projected `round(match(...),3)`
// -- is just as much a fulltext scan as one with a bare MATCH, but it is invisible to
// getFullTextMatchFromProject. Left out, such a scan goes unprotected: with any ordinary
// index on a filtered column the regular-index rule rewrites it away, and by the time this
// project is visited resolveFullTextIndexPath no longer finds a base scan to serve the
// MATCH from -- so the MATCH survives into the executed plan and throws 20105. The bare
// form of the same query is protected and works, which is what makes the gap easy to miss.
func (builder *QueryBuilder) detectFullTextGuard(projNode *plan.Node) []int32 {
	path := builder.resolveFullTextIndexPath(projNode)
	if path == nil {
		return nil
	}

	if path.aggNode != nil {
		filterids, _ := builder.getFullTextMatchFiltersFromScanNode(path.scanNode)
		wrappedExprs, _ := builder.getWrappedFullTextMatches(nil, path.scanNode, filterids, nil)
		if len(filterids) > 0 || len(wrappedExprs) > 0 {
			return []int32{path.scanNode.NodeId}
		}
		return nil
	}

	projids, _ := builder.getFullTextMatchFromProject(projNode, path.scanNode)
	filterids, _ := builder.getFullTextMatchFiltersFromScanNode(path.scanNode)
	wrappedExprs, _ := builder.getWrappedFullTextMatches(projNode, path.scanNode, filterids, projids)
	if len(filterids) > 0 || len(projids) > 0 || len(wrappedExprs) > 0 {
		return []int32{path.scanNode.NodeId}
	}
	return nil
}

func (builder *QueryBuilder) detectVectorGuard(projNode *plan.Node) []int32 {
	vecCtx := builder.buildVectorSortContext(projNode)
	if vecCtx == nil {
		vecCtx = builder.buildVectorSortContextThroughJoin(projNode)
	}
	return builder.detectVectorGuardForContext(vecCtx)
}

// detectVectorGuardFromSort is the SORT-anchored counterpart of detectVectorGuard. A
// Top-K reached only through the sort anchor (outer ORDER BY, join input) still owns its
// TABLE_SCAN and must reserve it: applyIndices is post-order, so without a guard entry
// applyIndicesForFilters rewrites that scan into a secondary-index join first, and by the
// time the sort anchor runs resolveScanNodeWithIndex finds a JOIN instead of a scan and
// the ANN rewrite silently never fires — leaving exactly the full-scan fallback #25967 /
// #25974 are about, for any inner query that also has an indexed filter.
func (builder *QueryBuilder) detectVectorGuardFromSort(sortNode *plan.Node) []int32 {
	return builder.detectVectorGuardForContext(builder.buildVectorSortContextFromSort(sortNode))
}

func (builder *QueryBuilder) detectVectorGuardForContext(vecCtx *vectorSortContext) []int32 {
	if vecCtx == nil || vecCtx.scanNode == nil {
		return nil
	}

	multiTableIndexes, err := builder.collectVectorIndexes(vecCtx.scanNode)
	if err != nil {
		return nil
	}
	if len(multiTableIndexes) == 0 {
		return nil
	}

	// Same plugin dispatch as applyIndicesForSort above — the canonical
	// algo set lives in the plugin registry. Hooks.CanApply is the
	// non-destructive probe (it folds prepareXxxIndexContext into a
	// bool); a true answer claims this scan as a vector-index guard
	// site for downstream stat / cardinality decisions.
	//
	// IsVectorIndexAlgo gate: indexplugin.Get matches fulltext too
	// (it's plugin-registered), but fulltext has no ANN ORDER BY
	// concept and must not be claimed as a vector-index guard. The
	// explicit predicate keeps that boundary even if the upstream
	// collectVectorIndexes filter is ever loosened.
	for _, multi := range multiTableIndexes {
		if !indexplugin.IsVectorIndexAlgo(multi.IndexAlgo) {
			continue
		}
		p, ok := indexplugin.Get(multi.IndexAlgo)
		if !ok {
			continue
		}
		vctxExt, mtiExt := toPlanplugin(vecCtx, multi)
		applicable, err := p.Plan().CanApply(builder, vctxExt, mtiExt)
		if err != nil {
			return nil
		}
		if applicable {
			return []int32{vecCtx.scanNode.NodeId}
		}
	}
	return nil
}

func (builder *QueryBuilder) collectVectorIndexes(scanNode *plan.Node) (map[string]*MultiTableIndex, error) {
	multiTableIndexes := make(map[string]*MultiTableIndex)
	if scanNode == nil || scanNode.TableDef == nil {
		return multiTableIndexes, nil
	}

	for _, indexDef := range scanNode.TableDef.Indexes {
		if indexDef != nil && indexplugin.IsVectorIndexAlgo(indexDef.IndexAlgo) {
			if _, ok := multiTableIndexes[indexDef.IndexName]; !ok {
				multiTableIndexes[indexDef.IndexName] = &MultiTableIndex{
					IndexAlgo: catalog.ToLower(indexDef.IndexAlgo),
					IndexDefs: make(map[string]*plan.IndexDef),
				}
			}
			multiTableIndexes[indexDef.IndexName].IndexDefs[catalog.ToLower(indexDef.IndexAlgoTableType)] = indexDef
		}
	}

	for name, multiTableIndex := range multiTableIndexes {
		if err := validateVectorIndexDefGroup(builder.GetContext(), name, multiTableIndex); err != nil {
			return nil, err
		}
	}
	return multiTableIndexes, nil
}

func validateVectorIndexDefGroup(ctx context.Context, indexName string, multiTableIndex *MultiTableIndex) error {
	if multiTableIndex == nil || len(multiTableIndex.IndexDefs) == 0 {
		return nil
	}

	var reference *plan.IndexDef
	for _, indexDef := range multiTableIndex.IndexDefs {
		if indexDef == nil {
			continue
		}
		if reference == nil {
			reference = indexDef
			continue
		}
		if reference.IndexName != indexDef.IndexName ||
			catalog.ToLower(reference.IndexAlgo) != catalog.ToLower(indexDef.IndexAlgo) ||
			!slices.Equal(reference.Parts, indexDef.Parts) {
			return moerr.NewInternalErrorf(ctx, "inconsistent vector index metadata for index %s", indexName)
		}
		if catalog.ToLower(reference.IndexAlgo) == catalog.MoIndexIvfFlatAlgo.ToString() {
			referenceIncludedColumns, err := indexDefIncludedColumns(reference)
			if err != nil {
				return err
			}
			includedColumns, err := indexDefIncludedColumns(indexDef)
			if err != nil {
				return err
			}
			if !slices.Equal(referenceIncludedColumns, includedColumns) {
				return moerr.NewInternalErrorf(ctx, "inconsistent IVF-FLAT INCLUDE metadata for index %s", indexName)
			}
		}
	}
	if reference != nil {
		multiTableIndex.IndexAlgo = catalog.ToLower(reference.IndexAlgo)
	}
	return nil
}

func getVectorIndexIncludedColumns(multiTableIndex *MultiTableIndex) ([]string, error) {
	if multiTableIndex == nil || catalog.ToLower(multiTableIndex.IndexAlgo) != catalog.MoIndexIvfFlatAlgo.ToString() {
		return nil, nil
	}
	for _, tableType := range []string{
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		catalog.SystemSI_IVFFLAT_TblType_Metadata,
		catalog.SystemSI_IVFFLAT_TblType_Centroids,
	} {
		includedColumns, err := indexDefIncludedColumns(multiTableIndex.IndexDefs[tableType])
		if err != nil {
			return nil, err
		}
		if len(includedColumns) > 0 {
			return includedColumns, nil
		}
	}
	return nil, nil
}

// regularIndexPrefixMetadataUsable validates the relationship between persisted
// prefix metadata and the logical index parts. Optional index access must fail
// closed when older DDL left a stale column-name key behind, or when the
// metadata is malformed; probing with the complete value can otherwise miss a
// physically truncated key.
func regularIndexPrefixMetadataUsable(idxDef *IndexDef) bool {
	if idxDef == nil || len(idxDef.Parts) == 0 {
		return false
	}
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	if err != nil {
		return false
	}
	for prefixPart := range prefixLengths {
		matched := false
		for _, part := range idxDef.Parts {
			if prefixPart == catalog.ResolveAlias(part) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func validateRegularIndexPrefixMetadata(idxDef *IndexDef) error {
	if regularIndexPrefixMetadataUsable(idxDef) {
		return nil
	}
	indexName := ""
	if idxDef != nil {
		indexName = idxDef.IndexName
	}
	return moerr.NewInvalidInputNoCtxf(
		"invalid prefix metadata for index %q; rebuild the index before writing to the table",
		indexName,
	)
}

func validateTableRegularIndexPrefixMetadata(tableDef *TableDef) error {
	if tableDef == nil {
		return nil
	}
	for _, idxDef := range tableDef.Indexes {
		if idxDef == nil || !idxDef.TableExist || !catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) {
			continue
		}
		if err := validateRegularIndexPrefixMetadata(idxDef); err != nil {
			return err
		}
	}
	return nil
}

func regularIndexHasDeclaredPrefix(idxDef *IndexDef) bool {
	if idxDef == nil {
		return true
	}
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	return err != nil || len(prefixLengths) > 0
}

type regularIndexAccessCandidate struct {
	indexPos           int
	shape              encodedRegularIndexCostShape
	filterType         int
	filterIdx          []int32
	leadingEqual       bool
	residualLeadingPos []int32
	work               float64
}

func (builder *QueryBuilder) applyIndicesForFiltersRegularIndex(nodeID int32, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	if len(node.FilterList) == 0 || len(node.TableDef.Indexes) == 0 {
		return nodeID
	}

	forceIndex := builder.scanHintsForceIndexes(node)
	for i := range node.FilterList { // if already have filter on first pk column and have a good selectivity, no need to go index
		expr := node.FilterList[i]
		fn := expr.GetF()
		if fn == nil {
			continue
		}
		col := fn.Args[0].GetCol()
		if col == nil {
			continue
		}
		if !forceIndex && GetSortOrder(node.TableDef, col.ColPos) == 0 && node.FilterList[i].Selectivity <= 0.001 {
			return node.NodeId
		}
	}

	indexes := make([]*IndexDef, 0, len(node.TableDef.Indexes))
	spatialIndexes := make([]*IndexDef, 0, len(node.TableDef.Indexes))
	for i := range node.TableDef.Indexes {
		if node.TableDef.Indexes[i] == nil || !node.TableDef.Indexes[i].TableExist || !catalog.IsRegularIndexAlgo(node.TableDef.Indexes[i].IndexAlgo) {
			continue
		}
		if isSpatialIndexDef(node.TableDef.Indexes[i]) {
			spatialIndexes = append(spatialIndexes, node.TableDef.Indexes[i])
			continue
		}
		if !regularIndexPrefixMetadataUsable(node.TableDef.Indexes[i]) {
			continue
		}
		indexes = append(indexes, node.TableDef.Indexes[i])
	}
	indexes = builder.filterRegularIndexesByScanHints(node, indexes)
	spatialIndexes = builder.filterIndexesByScanHints(node, spatialIndexes)
	if len(indexes) == 0 && len(spatialIndexes) == 0 {
		return nodeID
	}

	scanSnapshot := node.ScanSnapshot
	if scanSnapshot == nil {
		scanSnapshot = &Snapshot{}
	}

	//small table means this table maybe not flushed yet, or it's not worse to go index
	ignoreStats := forceIndex || node.Stats.TableCnt < 50000
	allowBackfill := true
	if !ignoreStats {
		if catalog.IsFakePkName(node.TableDef.Pkey.PkeyColName) {
			// for cluster by table, make it less prone to go index
			if node.Stats.Selectivity >= InFilterSelectivityLimit/2 || node.Stats.Outcnt >= InFilterCardLimitNonPK {
				allowBackfill = false
			}
		}
		if node.Stats.Selectivity >= InFilterSelectivityLimit || node.Stats.Outcnt >= float64(GetInFilterCardLimitOnPK(builder.compCtx.GetProcess().GetService(), node.Stats.TableCnt)) {
			allowBackfill = false
		}
	}

	costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
	best := regularIndexAccessCandidate{indexPos: -1, work: math.Inf(1)}
	fallback := regularIndexAccessCandidate{indexPos: -1}
	sawScorable := false
	pointBlocked := costCtx.pointLookupBlockedByPrimaryKey()
	consider := func(candidate regularIndexAccessCandidate) {
		idxDef := indexes[candidate.indexPos]
		work, skip, valid := costCtx.score(idxDef, candidate.filterIdx, candidate.residualLeadingPos, candidate.shape)
		candidate.work = work
		if ignoreStats {
			// FORCE and small tables bypass cost rejection because their estimates
			// are not an automatic cross-shape selection oracle. Preserve the
			// established access-shape priority: any covering scan precedes every
			// backfill join, while the score still ranks candidates within one shape.
			if !valid {
				candidate.work = math.Inf(1)
			}
			if best.indexPos == -1 || candidate.shape < best.shape ||
				(candidate.shape == best.shape && candidate.work < best.work) {
				best = candidate
			}
			return
		}
		if !valid {
			if fallback.indexPos == -1 {
				fallback = candidate
			}
			return
		}
		sawScorable = true
		if skip || (best.indexPos != -1 && work >= best.work) {
			return
		}
		best = candidate
	}

	for idx, idxDef := range indexes {
		if match, ok := builder.matchRegularIndexOnlyScan(idxDef, node, costCtx); ok {
			consider(regularIndexAccessCandidate{
				indexPos: idx, shape: encodedRegularIndexCostIndexOnly,
				filterType: match.filterType, filterIdx: match.filterIdx,
				leadingEqual: match.leadingEqual, residualLeadingPos: match.residualLeadingPos,
			})
		}
		if !allowBackfill {
			continue
		}
		if !pointBlocked {
			if filterIdx := costCtx.matchPointBackfill(idxDef, ignoreStats); len(filterIdx) > 0 {
				consider(regularIndexAccessCandidate{
					indexPos: idx, shape: encodedRegularIndexCostBackfill,
					filterType: EqualIndexCondition, filterIdx: filterIdx,
				})
			}
		}
		if filterIdx := costCtx.matchRangeBackfill(idxDef); len(filterIdx) > 0 {
			filterType := NonEqualIndexCondition
			if len(filterIdx) == 2 {
				filterType = RangeIndexCondition
			}
			consider(regularIndexAccessCandidate{
				indexPos: idx, shape: encodedRegularIndexCostBackfill,
				filterType: filterType, filterIdx: filterIdx,
			})
		}
	}
	if best.indexPos == -1 && !sawScorable {
		best = fallback
	}

	if best.indexPos != -1 && best.shape == encodedRegularIndexCostIndexOnly {
		return builder.applyRegularIndexOnlyScan(
			indexes[best.indexPos], node, idxColMap, scanSnapshot,
			&regularIndexOnlyMatch{
				filterIdx: best.filterIdx, filterType: best.filterType,
				leadingEqual: best.leadingEqual, residualLeadingPos: best.residualLeadingPos,
			},
		)
	}

	// Preserve the established priority of a covering spatial access over a
	// regular backfill join. Spatial candidates have a different cost domain.
	for i := range spatialIndexes {
		ret := builder.trySpatialIndexOnlyScan(spatialIndexes[i], node, colRefCnt, idxColMap, scanSnapshot)
		if ret != -1 {
			return ret
		}
	}

	if !allowBackfill {
		return nodeID
	}
	if best.indexPos != -1 {
		idxDef := indexes[best.indexPos]
		retID, idxTableNodeID := builder.applyIndexJoin(idxDef, node, best.filterType, best.filterIdx, scanSnapshot)
		if idxTableNodeID != -1 {
			builder.applyExtraFiltersOnIndex(idxDef, node, builder.qry.Nodes[idxTableNodeID], best.filterIdx)
			return retID
		}
	}

	idxToChoose, filterIdx := builder.getIndexForSpatialCond(spatialIndexes, node)
	if idxToChoose != -1 {
		retID, _ := builder.applyIndexJoin(spatialIndexes[idxToChoose], node, SpatialIndexCondition, filterIdx, scanSnapshot)
		return retID
	}

	//no index applied
	return nodeID
}

func (builder *QueryBuilder) applyExtraFiltersOnIndex(idxDef *IndexDef, node *plan.Node, idxTableNode *plan.Node, filterIdx []int32) {
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	if err != nil {
		// Invalid metadata must not make an optional filter pushdown affect query
		// correctness. The original filters remain on the base-table scan.
		return
	}

	for i := range node.FilterList {
		// if already in filterIdx, continue
		if slices.Contains(filterIdx, int32(i)) {
			continue
		}

		fn := node.FilterList[i].GetF()
		if fn == nil || len(fn.Args) < 2 {
			continue
		}
		colArgIdx := 0
		col := fn.Args[0].GetCol()
		if col == nil {
			col = fn.Args[1].GetCol()
			colArgIdx = 1
		}
		if col == nil {
			continue
		}
		access := resolveRegularIndexBackfillResidualAccess(idxDef, node.TableDef, col.ColPos, prefixLengths, nil)
		switch access.source {
		case regularIndexResidualIndexKey:
			idxColExpr := GetColExpr(idxTableNode.TableDef.Cols[0].Typ, idxTableNode.BindingTags[0], 0)
			mappedExpr := idxColExpr
			if indexTableStoresSerializedKey(idxDef) {
				var err error
				mappedExpr, err = MakeSerialExtractExpr(builder.GetContext(), idxColExpr, fn.Args[colArgIdx].Typ, int64(access.position))
				if err != nil {
					// Extra-filter pushdown is optional. The original filter remains on
					// the base table, so skip an unsupported serialized-key mapping.
					continue
				}
			}
			newFilter := DeepCopyExpr(node.FilterList[i])
			newFilter.GetF().Args[colArgIdx] = mappedExpr
			idxTableNode.FilterList = append(idxTableNode.FilterList, newFilter)
		case regularIndexResidualPhysicalPK:
			idxColExpr := GetColExpr(idxTableNode.TableDef.Cols[1].Typ, idxTableNode.BindingTags[0], 1)
			newFilter := DeepCopyExpr(node.FilterList[i])
			newFilter.GetF().Args[colArgIdx] = idxColExpr
			idxTableNode.FilterList = append(idxTableNode.FilterList, newFilter)
		case regularIndexResidualCompoundPK:
			idxColExpr := GetColExpr(idxTableNode.TableDef.Cols[1].Typ, idxTableNode.BindingTags[0], 1)
			deserialExpr, err := MakeSerialExtractExpr(builder.GetContext(), idxColExpr, fn.Args[colArgIdx].Typ, int64(access.position))
			if err != nil {
				continue
			}
			newFilter := DeepCopyExpr(node.FilterList[i])
			newFilter.GetF().Args[colArgIdx] = deserialExpr
			idxTableNode.FilterList = append(idxTableNode.FilterList, newFilter)
		}
	}
}

func tryMatchMoreLeadingFilters(idxDef *IndexDef, node *plan.Node, pos int32) []int32 {
	leadingPos := []int32{pos}
	for i := range idxDef.Parts {
		if i == 0 {
			continue //already hit
		}
		currentPos, ok := node.TableDef.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[i])]
		if !ok {
			break
		}
		found := false
		for j := range node.FilterList {
			fn := node.FilterList[j].GetF()
			if fn == nil {
				continue
			}
			switch fn.Func.ObjName {
			case "=":
				col := fn.Args[0].GetCol()
				if col != nil && col.ColPos == currentPos && isRuntimeConstExpr(fn.Args[1]) {
					leadingPos = append(leadingPos, int32(j))
					found = true
				}
			}
			if found {
				break
			}
		}
		// Composite index filters must match a contiguous leading prefix.
		// If any intermediate part is missing, stop matching immediately.
		if !found {
			break
		}
	}
	return leadingPos
}

func checkIndexFilter(fn *plan.Function) (int, *plan.ColRef) {
	if fn == nil {
		return UnsupportedIndexCondition, nil
	}
	switch fn.Func.ObjName {
	case "=":
		if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
			fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
		}
		col := fn.Args[0].GetCol()
		if col != nil && isRuntimeConstExpr(fn.Args[1]) {
			return EqualIndexCondition, col
		}

	case "in", "between":
		col := fn.Args[0].GetCol()
		if col != nil {
			return NonEqualIndexCondition, col
		}

	case ">", ">=", "<", "<=":
		if fn.Args[0].GetCol() != nil && isRuntimeConstExpr(fn.Args[1]) {
			return NonEqualIndexCondition, fn.Args[0].GetCol()
		}
		if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
			return NonEqualIndexCondition, fn.Args[1].GetCol()
		}

	case "in_range":
		col := fn.Args[0].GetCol()
		if col != nil && isRuntimeConstExpr(fn.Args[1]) && isRuntimeConstExpr(fn.Args[2]) {
			return NonEqualIndexCondition, col
		}

	case "or":
		var col *plan.ColRef
		for i := range fn.Args {
			typ1, col1 := checkIndexFilter(fn.Args[i].GetF())
			if typ1 != NonEqualIndexCondition {
				return UnsupportedIndexCondition, nil
			}
			if col == nil {
				col = col1
			} else {
				if col.RelPos != col1.RelPos || col.ColPos != col1.ColPos {
					return UnsupportedIndexCondition, nil
				}
			}
		}
		return NonEqualIndexCondition, col
	}
	return UnsupportedIndexCondition, nil
}

func findLeadingFilter(idxDef *IndexDef, node *plan.Node) ([]int32, bool) {
	leadingPos := node.TableDef.Name2ColIndex[idxDef.Parts[0]]
	for i := range node.FilterList {
		filterType, col := checkIndexFilter(node.FilterList[i].GetF())
		switch filterType {
		case EqualIndexCondition:
			if col.ColPos == leadingPos {
				return []int32{int32(i)}, true
			}
		case NonEqualIndexCondition:
			if col.ColPos == leadingPos {
				return []int32{int32(i)}, false
			}
		}
		continue
	}
	return nil, false
}

func (builder *QueryBuilder) makeIndexLookupPartExpr(idxDef *IndexDef, partPos int, inputExpr *plan.Expr) (*plan.Expr, error) {
	prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
	if err != nil {
		return nil, err
	}
	if partPos < 0 || partPos >= len(idxDef.Parts) {
		return nil, moerr.NewInvalidInputNoCtxf("invalid index part position %d", partPos)
	}

	partName := catalog.ResolveAlias(idxDef.Parts[partPos])
	return builder.makeIndexPartExprFromInputExpr(inputExpr, partName, prefixLengths)
}

func (builder *QueryBuilder) replaceEqualCondition(idxDef *IndexDef, filterList []*plan.Expr, filterPos []int32, idxTag int32, idxTableDef *plan.TableDef) (*plan.Expr, error) {
	numParts := len(idxDef.Parts)
	if numParts == 1 { //directly equal
		expr := DeepCopyExpr(filterList[filterPos[0]])
		args := expr.GetF().Args
		args[0].GetCol().RelPos = idxTag
		args[0].GetCol().ColPos = 0
		var err error
		args[1], err = builder.makeIndexLookupPartExpr(idxDef, 0, args[1])
		if err != nil {
			return nil, err
		}
		return expr, nil
	}

	// a=1 and b=2, change to prefix_eq
	compositeFilterSel := 1.0
	serialArgs := make([]*plan.Expr, len(filterPos))
	for i := range filterPos {
		filter := filterList[filterPos[i]]
		var err error
		serialArgs[i], err = builder.makeIndexLookupPartExpr(idxDef, i, filter.GetF().Args[1])
		if err != nil {
			return nil, err
		}
		duplicate := false
		for prior := 0; prior < i; prior++ {
			if filterPos[prior] == filterPos[i] {
				duplicate = true
				break
			}
		}
		if !duplicate {
			compositeFilterSel *= filter.Selectivity
		}
	}
	rightArg, err := BindFuncExprImplByPlanExpr(builder.GetContext(), indexTableComparisonSerialFunc(), serialArgs)
	if err != nil {
		return nil, err
	}

	funcName := "="
	if len(filterPos) < numParts {
		funcName = "prefix_eq"
	}
	leadingColExpr := GetColExpr(idxTableDef.Cols[0].Typ, idxTag, 0)
	expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{leadingColExpr, rightArg})
	if err != nil {
		return nil, err
	}
	expr.Selectivity = compositeFilterSel
	return expr, nil
}

func (builder *QueryBuilder) replaceNonEqualCondition(idxDef *IndexDef, filter *plan.Expr, idxTag int32, idxTableDef *plan.TableDef) (*plan.Expr, error) {
	numParts := len(idxDef.Parts)
	expr := DeepCopyExpr(filter)
	fn := expr.GetF()
	if fn.Func.ObjName == "or" {
		for i := range expr.GetF().Args {
			var err error
			expr.GetF().Args[i], err = builder.replaceNonEqualCondition(idxDef, filter.GetF().Args[i], idxTag, idxTableDef)
			if err != nil {
				return nil, err
			}
		}
		return expr, nil
	}
	comparesByteStringColumn := indexFunctionComparesByteStringColumn(fn)

	switch fn.Func.ObjName {
	case ">", ">=", "<", "<=":
		// Canonicalize: ensure column is on the left
		if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
			fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
			switch fn.Func.ObjName {
			case ">":
				fn.Func.ObjName = "<"
			case ">=":
				fn.Func.ObjName = "<="
			case "<":
				fn.Func.ObjName = ">"
			case "<=":
				fn.Func.ObjName = ">="
			}
		}
	}

	indexedPartType := fn.Args[0].Typ
	fn.Args[0].GetCol().RelPos = idxTag
	fn.Args[0].GetCol().ColPos = 0
	fn.Args[0].Typ = idxTableDef.Cols[0].Typ
	if numParts > 1 {
		serialFunc := indexTableComparisonSerialFunc()
		var err error
		switch fn.Func.ObjName {
		case "between":
			fn.Args[1] = builder.normalizeDecimalIndexRangeBound(fn.Args[1], indexedPartType)
			fn.Args[2] = builder.normalizeDecimalIndexRangeBound(fn.Args[2], indexedPartType)
			fn.Args[1], err = BindFuncExprImplByPlanExpr(builder.GetContext(), serialFunc, []*plan.Expr{fn.Args[1]})
			if err != nil {
				return nil, err
			}
			fn.Args[2], err = BindFuncExprImplByPlanExpr(builder.GetContext(), serialFunc, []*plan.Expr{fn.Args[2]})
			if err != nil {
				return nil, err
			}
			expr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_between", fn.Args)
		case "in":
			fn.Args[1], err = BindFuncExprImplByPlanExpr(builder.GetContext(), serialFunc, []*plan.Expr{fn.Args[1]})
			if err != nil {
				return nil, err
			}
			expr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_in", fn.Args)
		case ">", ">=", "<", "<=":
			fn.Args[1] = builder.normalizeDecimalIndexRangeBound(fn.Args[1], indexedPartType)
			fn.Args[1], err = BindFuncExprImplByPlanExpr(builder.GetContext(), serialFunc, []*plan.Expr{fn.Args[1]})
			if err != nil {
				return nil, err
			}
			expr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), fn.Func.ObjName, fn.Args)
		case "in_range":
			fn.Args[1] = builder.normalizeDecimalIndexRangeBound(fn.Args[1], indexedPartType)
			fn.Args[2] = builder.normalizeDecimalIndexRangeBound(fn.Args[2], indexedPartType)
			fn.Args[1], err = BindFuncExprImplByPlanExpr(builder.GetContext(), serialFunc, []*plan.Expr{fn.Args[1]})
			if err != nil {
				return nil, err
			}
			fn.Args[2], err = BindFuncExprImplByPlanExpr(builder.GetContext(), serialFunc, []*plan.Expr{fn.Args[2]})
			if err != nil {
				return nil, err
			}
			if comparesByteStringColumn {
				// PrefixCompare cannot distinguish an encoded byte string from a
				// longer value for which that encoding is a prefix.  An open lower
				// bound would therefore drop valid longer values.  Widen the index
				// candidate range to a closed lower bound; the original predicate is
				// retained as an exact residual on index-only scans or the base scan.
				fn.Args[3] = closePrefixRangeLowerBound(fn.Args[3])
			}
			expr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_in_range", fn.Args)
		}
		if err != nil {
			return nil, err
		}
	}
	return expr, nil
}

func (builder *QueryBuilder) replaceLeadingFilter(idxDef *IndexDef, filterList []*plan.Expr, leadingPos []int32, leadingEqualCond bool, idxTag int32, idxTableDef *plan.TableDef) (*plan.Expr, error) {
	if !leadingEqualCond { // a IN (1, 2, 3), a BETWEEN 1 AND 2
		return builder.replaceNonEqualCondition(idxDef, filterList[leadingPos[0]], idxTag, idxTableDef)
	}
	return builder.replaceEqualCondition(idxDef, filterList, leadingPos, idxTag, idxTableDef)
}

func indexOnlyResidualLeadingFilterPositions(idxDef *IndexDef, tableDef *plan.TableDef, filterList []*plan.Expr, leadingPos []int32, lookupFilter *plan.Expr) []int32 {
	return indexOnlyResidualLeadingFilterPositionsForPrefix(
		idxDef, tableDef, filterList, leadingPos, indexLookupUsesPrefixComparison(lookupFilter),
	)
}

func indexOnlyResidualLeadingFilterPositionsForPrefix(idxDef *IndexDef, tableDef *plan.TableDef, filterList []*plan.Expr, leadingPos []int32, usesPrefixComparison bool) []int32 {
	if indexTableStoredKeySerialFunc(idxDef) != "serial_full" {
		return nil
	}
	residualPos := make([]int32, 0, len(leadingPos))
	for _, pos := range leadingPos {
		if pos < 0 || int(pos) >= len(filterList) {
			continue
		}
		if indexFilterNeedsDecodedNullResidual(filterList[pos]) && !slices.Contains(residualPos, pos) {
			residualPos = append(residualPos, pos)
		}
	}

	if len(leadingPos) == 0 || !usesPrefixComparison {
		return residualPos
	}
	if rangePairFilterPositions(filterList, leadingPos) {
		// A serialized range pair is widened for byte-string keys when needed.
		// Keep both SQL bounds as exact rechecks because either encoded endpoint
		// can share a prefix with a distinct stored value.
		for _, pos := range leadingPos {
			if pos >= 0 && int(pos) < len(filterList) &&
				indexFilterUsesPrefixAmbiguousStringColumn(tableDef, filterList[pos]) &&
				!slices.Contains(residualPos, pos) {
				residualPos = append(residualPos, pos)
			}
		}
		return residualPos
	}
	lastPos := leadingPos[len(leadingPos)-1]
	if lastPos < 0 || int(lastPos) >= len(filterList) {
		return residualPos
	}
	if !indexFilterUsesPrefixAmbiguousStringColumn(tableDef, filterList[lastPos]) {
		return residualPos
	}
	for _, pos := range residualPos {
		if pos == lastPos {
			return residualPos
		}
	}
	return append(residualPos, lastPos)
}

func rangePairFilterPositions(filterList []*plan.Expr, filterPos []int32) bool {
	if len(filterPos) != 2 {
		return false
	}
	var firstCol, secondCol *plan.ColRef
	var firstLower, secondLower bool
	if filterPos[0] >= 0 && int(filterPos[0]) < len(filterList) {
		firstCol, firstLower = classifyRangeBound(filterList[filterPos[0]].GetF())
	}
	if filterPos[1] >= 0 && int(filterPos[1]) < len(filterList) {
		secondCol, secondLower = classifyRangeBound(filterList[filterPos[1]].GetF())
	}
	return firstCol != nil && secondCol != nil && firstLower != secondLower &&
		firstCol.RelPos == secondCol.RelPos && firstCol.ColPos == secondCol.ColPos
}

// indexOnlyLookupWillUsePrefixComparison predicts the lookup shape before an
// index-table binding tag is allocated. Candidate costing must account for the
// same exact residuals that applyRegularIndexOnlyScan will later materialize.
func indexOnlyLookupWillUsePrefixComparison(idxDef *IndexDef, filterList []*plan.Expr, leadingPos []int32, filterType int, leadingEqual bool) bool {
	if idxDef == nil || len(idxDef.Parts) <= 1 || len(leadingPos) == 0 {
		return false
	}
	if filterType == RangeIndexCondition {
		return true
	}
	if leadingEqual {
		return len(leadingPos) < len(idxDef.Parts)
	}
	pos := leadingPos[0]
	if pos < 0 || int(pos) >= len(filterList) {
		return false
	}
	return indexFilterWillBecomePrefixComparison(filterList[pos])
}

func indexFilterWillBecomePrefixComparison(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	switch fn.Func.ObjName {
	case "between", "in", "in_range":
		return true
	case "or":
		for _, arg := range fn.Args {
			if indexFilterWillBecomePrefixComparison(arg) {
				return true
			}
		}
	}
	return false
}

func indexLookupUsesPrefixComparison(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	switch fn.Func.ObjName {
	case "prefix_eq", "prefix_in", "prefix_between", "prefix_in_range":
		return true
	}
	for _, arg := range fn.Args {
		if indexLookupUsesPrefixComparison(arg) {
			return true
		}
	}
	return false
}

func indexFilterUsesPrefixAmbiguousStringColumn(tableDef *plan.TableDef, expr *plan.Expr) bool {
	if tableDef == nil || expr == nil {
		return false
	}
	if col := expr.GetCol(); col != nil && col.ColPos >= 0 && int(col.ColPos) < len(tableDef.Cols) {
		colDef := tableDef.Cols[col.ColPos]
		if colDef == nil {
			return false
		}
		oid := types.T(colDef.Typ.Id)
		// CHAR, VARCHAR, BINARY, and VARBINARY use Packer.EncodeStringType.
		// Its 0x00 terminator is also the first byte of an escaped embedded NUL,
		// so an encoded lookup value can prefix a distinct stored encoding.
		return oid == types.T_char || oid == types.T_varchar ||
			oid == types.T_binary || oid == types.T_varbinary
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if indexFilterUsesPrefixAmbiguousStringColumn(tableDef, arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if indexFilterUsesPrefixAmbiguousStringColumn(tableDef, item) {
				return true
			}
		}
	}
	return false
}

func indexFunctionComparesByteStringColumn(fn *plan.Function) bool {
	if fn == nil {
		return false
	}
	for _, arg := range fn.Args {
		if arg.GetCol() == nil {
			continue
		}
		oid := types.T(arg.Typ.Id)
		return oid == types.T_char || oid == types.T_varchar ||
			oid == types.T_binary || oid == types.T_varbinary
	}
	return false
}

func closePrefixRangeLowerBound(flagExpr *plan.Expr) *plan.Expr {
	if flagExpr == nil || flagExpr.GetLit() == nil {
		return flagExpr
	}
	flag := uint8(flagExpr.GetLit().GetU8Val())
	if flag&1 == 0 {
		return flagExpr
	}
	return MakePlan2Uint8ConstExprWithType(flag &^ 1)
}

// indexFilterNeedsDecodedNullResidual reports row-side NULL cases that cannot
// be represented by the encoded access predicate. Comparison operands use the
// NULL-propagating serial function, and prefix_in ignores NULL needles. A
// nullable stored key under a strict upper bound still sorts before a non-NULL
// bound and therefore needs SQL-semantic row evaluation.
func indexFilterNeedsDecodedNullResidual(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	switch fn.Func.ObjName {
	case ">", ">=", "<", "<=":
		if len(fn.Args) < 2 {
			return false
		}
		if canonicalRangeOp(fn) != "<" {
			return false
		}
		if fn.Args[0].GetCol() != nil && isRuntimeConstExpr(fn.Args[1]) {
			return !fn.Args[0].Typ.NotNullable
		}
		return isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil && !fn.Args[1].Typ.NotNullable
	case "or":
		for _, arg := range fn.Args {
			if indexFilterNeedsDecodedNullResidual(arg) {
				return true
			}
		}
	}
	return false
}

const (
	regularIndexPredicateRowWork = 1.0
	regularIndexJoinFixedWork    = 64.0
	regularIndexUnknownRangeSel  = 0.1
)

type encodedIndexFilterFact struct {
	refs               []int32
	directCol          int32
	unknownRangeBounds bool
}

type encodedRegularIndexCostContext struct {
	builder   *QueryBuilder
	node      *plan.Node
	colRefCnt map[[2]int32]int
	relPos    int32
	valid     bool
	force     bool

	statsTableCnt float64
	baseRows      float64
	outputRows    float64
	baseWidth     float64
	outputWidth   float64
	outputCols    int
	baseWork      float64
	baseUpperWork float64

	columnWidths []float64
	serialWidths []float64
	filterFacts  []encodedIndexFilterFact
	filterRefs   []int
	equalFilters []int32
	firstEquals  []int32
	firstFilters []int32
	filterTypes  []int
	rangeFilters [][]int32
	lowerFilters []int32
	upperFilters []int32
	requiredCols []int32

	unknownRangeLowerSelectivities map[int32]float64

	leadingFilters []bool
	coveredCols    []bool
	partPositions  []int
	extractRefs    []int
	compoundRefs   []int
	partWidths     []float64
	touchedLeading []int32
	touchedCovered []int32
	touchedParts   []int32
	touchedExtract []int32
}

func regularIndexPartPosition(idxDef *IndexDef, tableDef *plan.TableDef, colPos int32) (int, bool) {
	position := 0
	found := false
	for partPos, part := range idxDef.Parts {
		if pos, ok := tableDef.Name2ColIndex[catalog.ResolveAlias(part)]; ok && pos == colPos {
			position = partPos
			found = true
		}
	}
	return position, found
}

type regularIndexBackfillResidualSource uint8

const (
	regularIndexResidualUnavailable regularIndexBackfillResidualSource = iota
	regularIndexResidualIndexKey
	regularIndexResidualPhysicalPK
	regularIndexResidualCompoundPK
)

type regularIndexBackfillResidualAccess struct {
	source   regularIndexBackfillResidualSource
	position int
}

// resolveRegularIndexBackfillResidualAccess is the single source of truth for
// both materializing an extra predicate on an index table and costing that
// predicate. A residual can read an exact index-key part, the separately stored
// physical primary key, or one component serialized inside a compound primary
// key. Prefix-index parts are deliberately unavailable because they are lossy.
func resolveRegularIndexBackfillResidualAccess(
	idxDef *IndexDef,
	tableDef *plan.TableDef,
	colPos int32,
	prefixLengths map[string]int,
	knownPartPositions []int,
) regularIndexBackfillResidualAccess {
	if idxDef == nil || tableDef == nil || tableDef.Pkey == nil || colPos < 0 {
		return regularIndexBackfillResidualAccess{}
	}
	partPos := -1
	if int(colPos) < len(knownPartPositions) {
		partPos = knownPartPositions[colPos]
	} else if resolvedPos, ok := regularIndexPartPosition(idxDef, tableDef, colPos); ok {
		partPos = resolvedPos
	}
	if partPos >= len(idxDef.Parts) {
		return regularIndexBackfillResidualAccess{}
	}
	if partPos >= 0 {
		partName := catalog.ResolveAlias(idxDef.Parts[partPos])
		if prefixLengths[partName] > 0 {
			return regularIndexBackfillResidualAccess{}
		}
		return regularIndexBackfillResidualAccess{source: regularIndexResidualIndexKey, position: partPos}
	}

	if len(tableDef.Pkey.Names) == 1 {
		if pkPos, ok := tableDef.Name2ColIndex[tableDef.Pkey.PkeyColName]; ok && pkPos == colPos {
			return regularIndexBackfillResidualAccess{source: regularIndexResidualPhysicalPK}
		}
		return regularIndexBackfillResidualAccess{}
	}
	for componentPos, name := range tableDef.Pkey.Names {
		if pkPos, ok := tableDef.Name2ColIndex[name]; ok && pkPos == colPos {
			return regularIndexBackfillResidualAccess{source: regularIndexResidualCompoundPK, position: componentPos}
		}
	}
	return regularIndexBackfillResidualAccess{}
}

type encodedRegularIndexCostShape uint8

const (
	encodedRegularIndexCostIndexOnly encodedRegularIndexCostShape = iota
	encodedRegularIndexCostBackfill
)

func collectEncodedIndexFilterFact(expr *plan.Expr, relPos int32, fact *encodedIndexFilterFact, refs []int) {
	if expr == nil {
		return
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if exprImpl.Col.RelPos == relPos && exprImpl.Col.ColPos >= 0 && int(exprImpl.Col.ColPos) < len(refs) {
			fact.refs = append(fact.refs, exprImpl.Col.ColPos)
			refs[exprImpl.Col.ColPos]++
		}
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			collectEncodedIndexFilterFact(arg, relPos, fact, refs)
		}
	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			collectEncodedIndexFilterFact(arg, relPos, fact, refs)
		}
	case *plan.Expr_W:
		collectEncodedIndexFilterFact(exprImpl.W.WindowFunc, relPos, fact, refs)
		for _, arg := range exprImpl.W.PartitionBy {
			collectEncodedIndexFilterFact(arg, relPos, fact, refs)
		}
		for _, orderBy := range exprImpl.W.OrderBy {
			collectEncodedIndexFilterFact(orderBy.Expr, relPos, fact, refs)
		}
	}
}

func directEncodedIndexFilterCol(expr *plan.Expr, relPos int32, numCols int) int32 {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) < 2 {
		return -1
	}
	col := fn.Args[0].GetCol()
	if col == nil {
		col = fn.Args[1].GetCol()
	}
	if col == nil || col.RelPos != relPos || col.ColPos < 0 || int(col.ColPos) >= numCols {
		return -1
	}
	return col.ColPos
}

func estimatedRegularIndexColumnBytes(col *plan.ColDef, sizeMap map[string]uint64, tableCnt float64) float64 {
	if col == nil {
		return 0
	}
	if tableCnt > 0 {
		if totalBytes := sizeMap[col.Name]; totalBytes > 0 {
			return max(1, float64(totalBytes)/tableCnt)
		}
	}

	oid := types.T(col.Typ.Id)
	width := float64(oid.TypeLen())
	if oid.FixedLength() < 0 && col.Typ.Width > 0 && float64(col.Typ.Width) < width {
		width = float64(col.Typ.Width)
	}
	return max(1, width)
}

func estimatedRegularIndexPartBytes(col *plan.ColDef, sizeMap map[string]uint64, tableCnt float64) float64 {
	if col == nil {
		return 0
	}
	oid := types.T(col.Typ.Id)
	if oid.FixedLength() >= 0 {
		if bound, ok := function.SerialEncodedTypeSizeBound(types.New(oid, col.Typ.Width, col.Typ.Scale)); ok {
			return float64(bound)
		}
	}
	// Variable-size tuple values add the string-type code, bytes code, and
	// terminator. SizeMap supplies the observed payload size; embedded-zero
	// escapes are intentionally left unknown rather than assuming worst case.
	return estimatedRegularIndexColumnBytes(col, sizeMap, tableCnt) + 3
}

func (builder *QueryBuilder) newEncodedRegularIndexCostContext(
	node *plan.Node,
	colRefCnt map[[2]int32]int,
) *encodedRegularIndexCostContext {
	ctx := &encodedRegularIndexCostContext{builder: builder, node: node, colRefCnt: colRefCnt}
	if node == nil || node.Stats == nil || node.TableDef == nil || node.TableDef.Pkey == nil || len(node.BindingTags) == 0 {
		return ctx
	}

	ctx.relPos = node.BindingTags[0]
	ctx.force = builder.scanHintsForceIndexes(node)
	ctx.statsTableCnt = node.Stats.TableCnt
	var statsInfo *statspb.StatsInfo
	var sizeMap map[string]uint64
	if wrapper := builder.getStatsInfoByTableID(node.TableDef.TblId); wrapper != nil && wrapper.GetStats() != nil {
		statsInfo = wrapper.GetStats()
		if finitePositive(statsInfo.TableCnt) {
			ctx.statsTableCnt = statsInfo.TableCnt
			sizeMap = statsInfo.SizeMap
		}
	}

	numCols := len(node.TableDef.Cols)
	ctx.columnWidths = make([]float64, numCols)
	ctx.serialWidths = make([]float64, numCols)
	ctx.filterFacts = make([]encodedIndexFilterFact, len(node.FilterList))
	ctx.filterRefs = make([]int, numCols)
	ctx.equalFilters = make([]int32, numCols)
	ctx.firstEquals = make([]int32, numCols)
	ctx.firstFilters = make([]int32, numCols)
	ctx.filterTypes = make([]int, len(node.FilterList))
	ctx.rangeFilters = make([][]int32, numCols)
	ctx.lowerFilters = make([]int32, numCols)
	ctx.upperFilters = make([]int32, numCols)
	ctx.leadingFilters = make([]bool, len(node.FilterList))
	ctx.coveredCols = make([]bool, numCols)
	ctx.partPositions = make([]int, numCols)
	ctx.extractRefs = make([]int, numCols)
	ctx.compoundRefs = make([]int, numCols)
	for colPos := 0; colPos < numCols; colPos++ {
		ctx.equalFilters[colPos] = -1
		ctx.firstEquals[colPos] = -1
		ctx.firstFilters[colPos] = -1
		ctx.lowerFilters[colPos] = -1
		ctx.upperFilters[colPos] = -1
		ctx.partPositions[colPos] = -1
		col := node.TableDef.Cols[colPos]
		ctx.columnWidths[colPos] = estimatedRegularIndexColumnBytes(col, sizeMap, ctx.statsTableCnt)
		ctx.serialWidths[colPos] = estimatedRegularIndexPartBytes(col, sizeMap, ctx.statsTableCnt)
	}
	for filterIdx, filter := range node.FilterList {
		fact := &ctx.filterFacts[filterIdx]
		fact.directCol = directEncodedIndexFilterCol(filter, ctx.relPos, numCols)
		if containsDynamicParam(filter) {
			lowerSelectivity, hasUnknownBounds := encodedRegularIndexRangeLowerSelectivity(filter, builder, statsInfo)
			fact.unknownRangeBounds = hasUnknownBounds
			if hasUnknownBounds {
				if ctx.unknownRangeLowerSelectivities == nil {
					ctx.unknownRangeLowerSelectivities = make(map[int32]float64)
				}
				ctx.unknownRangeLowerSelectivities[int32(filterIdx)] = lowerSelectivity
			}
		}
		collectEncodedIndexFilterFact(filter, ctx.relPos, fact, ctx.filterRefs)
		filterType, col := checkIndexFilter(filter.GetF())
		if col != nil && col.ColPos >= 0 && int(col.ColPos) < numCols {
			ctx.filterTypes[filterIdx] = filterType
			if filterType != UnsupportedIndexCondition && ctx.firstFilters[col.ColPos] < 0 {
				ctx.firstFilters[col.ColPos] = int32(filterIdx)
			}
			if filterType == EqualIndexCondition {
				ctx.equalFilters[col.ColPos] = int32(filterIdx)
				if ctx.firstEquals[col.ColPos] < 0 {
					ctx.firstEquals[col.ColPos] = int32(filterIdx)
				}
			} else if filterType == NonEqualIndexCondition {
				ctx.rangeFilters[col.ColPos] = append(ctx.rangeFilters[col.ColPos], int32(filterIdx))
			}
		}
		rangeCol, isLower := classifyRangeBound(filter.GetF())
		if rangeCol == nil || rangeCol.ColPos < 0 || int(rangeCol.ColPos) >= numCols {
			continue
		}
		if isLower {
			ctx.lowerFilters[rangeCol.ColPos] = int32(filterIdx)
		} else {
			ctx.upperFilters[rangeCol.ColPos] = int32(filterIdx)
		}
	}

	for colPos := 0; colPos < numCols; colPos++ {
		refs := max(colRefCnt[[2]int32{ctx.relPos, int32(colPos)}], ctx.filterRefs[colPos])
		if refs > 0 {
			ctx.baseWidth += ctx.columnWidths[colPos]
			ctx.requiredCols = append(ctx.requiredCols, int32(colPos))
		}
		if colRefCnt[[2]int32{ctx.relPos, int32(colPos)}] > ctx.filterRefs[colPos] {
			ctx.outputWidth += ctx.columnWidths[colPos]
			ctx.outputCols++
		}
	}
	ctx.baseRows = max(node.Stats.Outcnt, node.Stats.Cost)
	if node.Stats.BlockNum > 0 {
		// Cost is TableCnt * estimated block selectivity, while BlockNum is the
		// block-granular scan estimate. Charge selected blocks at the same
		// BlockMaxRows granularity used by scan pagination and runtime-filter stats,
		// without exceeding the table cardinality.
		selectedBlockRows := min(
			node.Stats.TableCnt,
			float64(node.Stats.BlockNum)*float64(objectio.BlockMaxRows),
		)
		ctx.baseRows = max(ctx.baseRows, selectedBlockRows)
	}
	ctx.outputRows = max(0, node.Stats.Outcnt)
	ctx.valid = finitePositive(node.Stats.TableCnt) && finitePositive(node.Stats.Cost) &&
		!math.IsNaN(node.Stats.Outcnt) && !math.IsInf(node.Stats.Outcnt, 0) && node.Stats.Outcnt >= 0 &&
		finitePositive(ctx.statsTableCnt) && finitePositive(ctx.baseRows) && finitePositive(ctx.baseWidth)
	if ctx.valid {
		// The control is the column-pruned storage read already represented by
		// Stats.Cost. Candidate-only stages below are additive work that the
		// control does not perform.
		baseRowWork := ctx.baseWidth + float64(len(node.FilterList))*regularIndexPredicateRowWork
		ctx.baseWork = ctx.baseRows * baseRowWork
		ctx.baseUpperWork = max(ctx.baseWork, node.Stats.TableCnt*baseRowWork)
		ctx.valid = finitePositive(ctx.baseWork) && finitePositive(ctx.baseUpperWork)
	}
	return ctx
}

func (ctx *encodedRegularIndexCostContext) pointLookupBlockedByPrimaryKey() bool {
	if ctx.node == nil || ctx.node.TableDef == nil || ctx.node.TableDef.Pkey == nil || len(ctx.node.TableDef.Pkey.Names) == 0 {
		return false
	}
	colPos, ok := ctx.node.TableDef.Name2ColIndex[ctx.node.TableDef.Pkey.Names[0]]
	return ok && colPos >= 0 && int(colPos) < len(ctx.equalFilters) && ctx.equalFilters[colPos] >= 0
}

func (ctx *encodedRegularIndexCostContext) matchPointBackfill(idxDef *IndexDef, ignoreStats bool) []int32 {
	if idxDef == nil || ctx.node == nil || ctx.node.TableDef == nil || ctx.node.Stats == nil {
		return nil
	}
	numKeyParts := len(idxDef.Parts)
	if !idxDef.Unique {
		numKeyParts--
	}
	if numKeyParts <= 0 {
		return nil
	}
	cardLimit := float64(GetInFilterCardLimitOnPK(
		ctx.builder.compCtx.GetProcess().GetService(), ctx.node.Stats.TableCnt,
	))
	filterIdx := make([]int32, 0, numKeyParts)
	usePartialIndex := false
	for partPos := 0; partPos < numKeyParts; partPos++ {
		colPos, ok := ctx.node.TableDef.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[partPos])]
		if !ok || colPos < 0 || int(colPos) >= len(ctx.equalFilters) {
			break
		}
		matchedFilter := ctx.equalFilters[colPos]
		if matchedFilter < 0 {
			break
		}
		filterIdx = append(filterIdx, matchedFilter)
		filter := ctx.node.FilterList[matchedFilter]
		if ignoreStats || (filter.Selectivity <= InFilterSelectivityLimit && ctx.node.Stats.TableCnt*filter.Selectivity <= cardLimit) {
			usePartialIndex = true
		}
	}
	if len(filterIdx) < len(idxDef.Parts) && (idxDef.Unique || !usePartialIndex) {
		return nil
	}
	return filterIdx
}

func (ctx *encodedRegularIndexCostContext) matchRangeBackfill(idxDef *IndexDef) []int32 {
	if idxDef == nil || ctx.node == nil || ctx.node.TableDef == nil {
		return nil
	}
	if regularIndexHasDeclaredPrefix(idxDef) {
		return nil
	}
	numKeyParts := len(idxDef.Parts)
	if !idxDef.Unique {
		numKeyParts--
	}
	if numKeyParts < 1 || (idxDef.Unique && numKeyParts != 1) {
		return nil
	}
	leadingColPos, ok := ctx.node.TableDef.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[0])]
	if !ok || leadingColPos < 0 || int(leadingColPos) >= len(ctx.node.TableDef.Cols) {
		return nil
	}
	for _, currentFilterPos := range ctx.rangeFilters[leadingColPos] {
		currentFilterIdx := int(currentFilterPos)
		filter := ctx.node.FilterList[currentFilterIdx]
		fn := filter.GetF()
		if rangeCol, _ := classifyRangeBound(fn); rangeCol != nil {
			lowerIdx := ctx.lowerFilters[leadingColPos]
			upperIdx := ctx.upperFilters[leadingColPos]
			if lowerIdx >= 0 && upperIdx >= 0 {
				if int32(currentFilterIdx) != min(lowerIdx, upperIdx) || shouldSkipLargeRangeIndexByStats(ctx.node) {
					continue
				}
				if !regularIndexRangeFunctionsUsable(
					idxDef,
					ctx.node.TableDef,
					ctx.node.FilterList[lowerIdx].GetF(),
					ctx.node.FilterList[upperIdx].GetF(),
					true,
				) {
					continue
				}
				return []int32{lowerIdx, upperIdx}
			}
		}
		if !regularIndexRangeFunctionsUsable(idxDef, ctx.node.TableDef, fn, nil, false) {
			continue
		}
		if isRangeOp(fn) && shouldSkipLargeRangeIndexByStats(ctx.node) {
			continue
		}
		return []int32{int32(currentFilterIdx)}
	}
	return nil
}

func (ctx *encodedRegularIndexCostContext) resetScratch() {
	for _, pos := range ctx.touchedLeading {
		ctx.leadingFilters[pos] = false
	}
	for _, pos := range ctx.touchedCovered {
		ctx.coveredCols[pos] = false
	}
	for _, pos := range ctx.touchedParts {
		ctx.partPositions[pos] = -1
	}
	for _, pos := range ctx.touchedExtract {
		ctx.extractRefs[pos] = 0
		ctx.compoundRefs[pos] = 0
	}
	ctx.touchedLeading = ctx.touchedLeading[:0]
	ctx.touchedCovered = ctx.touchedCovered[:0]
	ctx.touchedParts = ctx.touchedParts[:0]
	ctx.touchedExtract = ctx.touchedExtract[:0]
}

func (ctx *encodedRegularIndexCostContext) addExtractRef(colPos int32, compound bool) {
	if colPos < 0 || int(colPos) >= len(ctx.extractRefs) {
		return
	}
	if ctx.extractRefs[colPos] == 0 && ctx.compoundRefs[colPos] == 0 {
		ctx.touchedExtract = append(ctx.touchedExtract, colPos)
	}
	if compound {
		ctx.compoundRefs[colPos]++
	} else {
		ctx.extractRefs[colPos]++
	}
}

func encodedRegularIndexRangeLowerSelectivity(
	filter *plan.Expr,
	builder *QueryBuilder,
	statsInfo *statspb.StatsInfo,
) (lower float64, hasUnknownBounds bool) {
	if filter == nil {
		return 1, false
	}
	if !containsDynamicParam(filter) {
		return estimateExprSelectivity(filter, builder, statsInfo), false
	}
	fn := filter.GetF()
	if fn == nil || fn.Func == nil {
		return estimateExprSelectivity(filter, builder, statsInfo), false
	}

	switch fn.Func.ObjName {
	case ">", ">=", "<", "<=", "between", "in_range":
		return 0, true
	case "or":
		for _, arg := range fn.Args {
			argLower, argUnknown := encodedRegularIndexRangeLowerSelectivity(arg, builder, statsInfo)
			lower = max(lower, argLower)
			hasUnknownBounds = hasUnknownBounds || argUnknown
		}
		if hasUnknownBounds {
			// Every OR result contains each stable child result. The largest child
			// estimate is therefore conservative without assuming that known
			// branches are disjoint. Cap it at the parent estimate so stale child
			// statistics cannot make the lower estimate exceed the ranking point.
			return min(lower, estimateExprSelectivity(filter, builder, statsInfo)), true
		}
		return estimateExprSelectivity(filter, builder, statsInfo), false
	default:
		for _, arg := range fn.Args {
			if _, argUnknown := encodedRegularIndexRangeLowerSelectivity(arg, builder, statsInfo); argUnknown {
				// No non-OR parent guarantees that an unknown range preserves rows.
				return 0, true
			}
		}
	}
	return estimateExprSelectivity(filter, builder, statsInfo), false
}

func (ctx *encodedRegularIndexCostContext) score(
	idxDef *IndexDef,
	leadingPos []int32,
	residualLeadingPos []int32,
	shape encodedRegularIndexCostShape,
) (work float64, shouldReject bool, valid bool) {
	if idxDef == nil || len(idxDef.Parts) == 0 || !ctx.valid {
		return 0, false, false
	}
	ctx.resetScratch()
	if cap(ctx.partWidths) < len(idxDef.Parts) {
		ctx.partWidths = make([]float64, len(idxDef.Parts))
	} else {
		ctx.partWidths = ctx.partWidths[:len(idxDef.Parts)]
	}

	encodedWidth := 0.0
	for partPos, part := range idxDef.Parts {
		colPos, ok := ctx.node.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
		if !ok || colPos < 0 || int(colPos) >= len(ctx.columnWidths) {
			ctx.resetScratch()
			return 0, false, false
		}
		width := ctx.serialWidths[colPos]
		if len(idxDef.Parts) == 1 {
			width = ctx.columnWidths[colPos]
		}
		if !finitePositive(width) {
			ctx.resetScratch()
			return 0, false, false
		}
		ctx.partWidths[partPos] = width
		encodedWidth += width
		if ctx.partPositions[colPos] < 0 {
			ctx.touchedParts = append(ctx.touchedParts, colPos)
		}
		ctx.partPositions[colPos] = partPos
	}
	if !finitePositive(encodedWidth) {
		ctx.resetScratch()
		return 0, false, false
	}

	candidateSelectivity := 1.0
	lowerCandidateSelectivity := 1.0
	for _, pos := range leadingPos {
		if pos < 0 || int(pos) >= len(ctx.node.FilterList) {
			ctx.resetScratch()
			return 0, false, false
		}
		if ctx.leadingFilters[pos] {
			continue
		}
		selectivity := ctx.node.FilterList[pos].Selectivity
		lowerSelectivity := selectivity
		if ctx.filterFacts[pos].unknownRangeBounds {
			// PREPARE has no range-bound values. The stats layer's optimistic dynamic
			// estimate keeps parameterized ranges usable, but it is too speculative to
			// rank one physical index ahead of a sibling with an NDV-backed equality.
			// Use the same neutral fallback as an otherwise unknown range estimate.
			selectivity = max(selectivity, regularIndexUnknownRangeSel)
			// Without the bound values there is no comparable absolute cardinality
			// estimate for this access path. Keep the estimate for relative index
			// ranking, but do not use it to eliminate every index candidate.
			lowerSelectivity = ctx.unknownRangeLowerSelectivities[pos]
		}
		if math.IsNaN(selectivity) || math.IsInf(selectivity, 0) || selectivity < 0 || selectivity > 1 ||
			math.IsNaN(lowerSelectivity) || math.IsInf(lowerSelectivity, 0) || lowerSelectivity < 0 || lowerSelectivity > 1 {
			ctx.resetScratch()
			return 0, false, false
		}
		ctx.leadingFilters[pos] = true
		ctx.touchedLeading = append(ctx.touchedLeading, pos)
		candidateSelectivity *= selectivity
		lowerCandidateSelectivity *= lowerSelectivity
	}
	leadingCandidateRows := ctx.node.Stats.TableCnt * candidateSelectivity
	lowerCandidateRows := ctx.node.Stats.TableCnt * lowerCandidateSelectivity
	candidateRows := max(ctx.outputRows, leadingCandidateRows)
	if !finitePositive(candidateRows) {
		ctx.resetScratch()
		return 0, false, false
	}

	if shape == encodedRegularIndexCostIndexOnly && ctx.node.Limit != nil {
		hasResidual := len(residualLeadingPos) > 0
		for filterIdx := range ctx.node.FilterList {
			if !ctx.leadingFilters[filterIdx] {
				hasResidual = true
				break
			}
		}
		if !hasResidual {
			if candidateLimit, ok := buildCandidateLimit(ctx.node.Limit, ctx.node.Offset); ok {
				if literalLimit, ok := getLiteralUint64(candidateLimit); ok {
					candidateRows = min(candidateRows, float64(literalLimit))
				}
			}
		}
	}

	hiddenRows := ctx.outputRows
	lowerHiddenRows := 0.0
	hiddenFilterCount := 1
	if shape == encodedRegularIndexCostIndexOnly {
		for filterIdx, fact := range ctx.filterFacts {
			if ctx.leadingFilters[filterIdx] {
				if !slices.Contains(residualLeadingPos, int32(filterIdx)) {
					continue
				}
				hiddenFilterCount++
			} else {
				hiddenFilterCount++
			}
			for _, colPos := range fact.refs {
				ctx.addExtractRef(colPos, false)
			}
		}
		if len(ctx.unknownRangeLowerSelectivities) > 0 {
			lowerHiddenSelectivity := lowerCandidateSelectivity
			for filterIdx, fact := range ctx.filterFacts {
				if ctx.leadingFilters[filterIdx] {
					continue
				}
				selectivity := ctx.node.FilterList[filterIdx].Selectivity
				if fact.unknownRangeBounds {
					selectivity = ctx.unknownRangeLowerSelectivities[int32(filterIdx)]
				}
				if math.IsNaN(selectivity) || math.IsInf(selectivity, 0) || selectivity < 0 || selectivity > 1 {
					ctx.resetScratch()
					return 0, false, false
				}
				lowerHiddenSelectivity *= selectivity
			}
			lowerHiddenRows = ctx.node.Stats.TableCnt * lowerHiddenSelectivity
		}
	} else {
		prefixLengths, prefixErr := catalog.IndexPrefixLengthsFromParamsWithError(idxDef.IndexAlgoParams)
		hiddenSelectivity := candidateSelectivity
		lowerHiddenSelectivity := lowerCandidateSelectivity
		for filterIdx, fact := range ctx.filterFacts {
			if ctx.leadingFilters[filterIdx] || fact.directCol < 0 {
				continue
			}
			if prefixErr != nil {
				// Invalid optional-pushdown metadata has the same fail-safe
				// behavior as applyExtraFiltersOnIndex: leave every residual on
				// the base scan and do not credit its selectivity.
				continue
			}
			access := resolveRegularIndexBackfillResidualAccess(
				idxDef, ctx.node.TableDef, fact.directCol, prefixLengths, ctx.partPositions,
			)
			switch access.source {
			case regularIndexResidualIndexKey:
				if indexTableStoresSerializedKey(idxDef) {
					ctx.addExtractRef(fact.directCol, false)
				}
			case regularIndexResidualCompoundPK:
				ctx.addExtractRef(fact.directCol, true)
			case regularIndexResidualPhysicalPK:
				// The physical PK is stored directly in index-table column 1.
			default:
				continue
			}
			selectivity := ctx.node.FilterList[filterIdx].Selectivity
			lowerSelectivity := selectivity
			if fact.unknownRangeBounds {
				lowerSelectivity = ctx.unknownRangeLowerSelectivities[int32(filterIdx)]
			}
			if math.IsNaN(selectivity) || math.IsInf(selectivity, 0) || selectivity < 0 || selectivity > 1 ||
				math.IsNaN(lowerSelectivity) || math.IsInf(lowerSelectivity, 0) || lowerSelectivity < 0 || lowerSelectivity > 1 {
				ctx.resetScratch()
				return 0, false, false
			}
			hiddenSelectivity *= selectivity
			lowerHiddenSelectivity *= lowerSelectivity
			hiddenFilterCount++
		}
		hiddenRows = max(ctx.outputRows, ctx.node.Stats.TableCnt*hiddenSelectivity)
		lowerHiddenRows = ctx.node.Stats.TableCnt * lowerHiddenSelectivity
	}
	if math.IsNaN(hiddenRows) || math.IsInf(hiddenRows, 0) || hiddenRows < 0 {
		ctx.resetScratch()
		return 0, false, false
	}

	pkIdx, hasPK := ctx.node.TableDef.Name2ColIndex[ctx.node.TableDef.Pkey.PkeyColName]
	pkWidth := 0.0
	if hasPK && pkIdx >= 0 && int(pkIdx) < len(ctx.columnWidths) {
		pkWidth = ctx.columnWidths[pkIdx]
	}
	if shape == encodedRegularIndexCostBackfill && !finitePositive(pkWidth) {
		ctx.resetScratch()
		return 0, false, false
	}

	hiddenInputWidth := encodedWidth
	needsPhysicalPK := false
	if hasPK {
		totalPKRefs := max(ctx.colRefCnt[[2]int32{ctx.relPos, pkIdx}], ctx.filterRefs[pkIdx])
		needsPhysicalPK = max(0, totalPKRefs-ctx.filterRefs[pkIdx]) > 0 || ctx.extractRefs[pkIdx] > 0
	}
	if shape == encodedRegularIndexCostBackfill || needsPhysicalPK {
		hiddenInputWidth += pkWidth
	}
	hiddenOutputWidth := ctx.baseWidth
	if shape == encodedRegularIndexCostBackfill {
		hiddenOutputWidth = pkWidth
	}
	calculateWork := func(candidateRows, hiddenRows, outputRows float64) (float64, bool) {
		hiddenScanInput := candidateRows * hiddenInputWidth
		hiddenScanOutput := hiddenRows * hiddenOutputWidth
		hiddenPredicates := candidateRows * float64(hiddenFilterCount) * regularIndexPredicateRowWork
		indexWork := hiddenScanInput + hiddenScanOutput + hiddenPredicates

		prefixWidth := 0.0
		for partPos, part := range idxDef.Parts {
			prefixWidth += ctx.partWidths[partPos]
			colPos := ctx.node.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
			if ctx.partPositions[colPos] != partPos {
				continue
			}
			if shape == encodedRegularIndexCostIndexOnly {
				totalRefs := max(ctx.colRefCnt[[2]int32{ctx.relPos, colPos}], ctx.filterRefs[colPos])
				downstreamRefs := max(0, totalRefs-ctx.filterRefs[colPos])
				if len(idxDef.Parts) > 1 && colPos != pkIdx {
					extractWork := prefixWidth + ctx.columnWidths[colPos]
					indexWork += candidateRows * float64(ctx.extractRefs[colPos]) * extractWork
					indexWork += outputRows * float64(downstreamRefs) * extractWork
				}
			} else if refs := ctx.extractRefs[colPos]; refs > 0 {
				extractWork := prefixWidth + ctx.columnWidths[colPos]
				indexWork += candidateRows * float64(refs) * extractWork
			}
		}

		if shape == encodedRegularIndexCostBackfill {
			compoundPrefixWidth := 0.0
			for _, name := range ctx.node.TableDef.Pkey.Names {
				componentIdx, ok := ctx.node.TableDef.Name2ColIndex[name]
				if !ok || componentIdx < 0 || int(componentIdx) >= len(ctx.columnWidths) {
					return 0, false
				}
				compoundPrefixWidth += ctx.serialWidths[componentIdx]
				if refs := ctx.compoundRefs[componentIdx]; refs > 0 {
					extractWork := compoundPrefixWidth + ctx.columnWidths[componentIdx]
					indexWork += candidateRows * float64(refs) * extractWork
				}
			}

			// The hidden output already accounts for transferring each physical PK.
			// Charge the targeted base lookup once more as a per-key operation.
			pkBackfill := hiddenRows * regularIndexPredicateRowWork
			baseChildInput := hiddenRows * ctx.baseWidth
			baseChildPredicates := hiddenRows * float64(len(ctx.node.FilterList)) * regularIndexPredicateRowWork
			joinFixed := regularIndexJoinFixedWork + ctx.outputWidth + float64(ctx.outputCols)*regularIndexPredicateRowWork
			joinRows := outputRows * (ctx.outputWidth + regularIndexPredicateRowWork)
			indexWork += pkBackfill + baseChildInput + baseChildPredicates + joinFixed + joinRows
		}

		valid := !math.IsNaN(indexWork) && !math.IsInf(indexWork, 0) && indexWork >= 0
		return indexWork, valid
	}

	indexWork, validWork := calculateWork(candidateRows, hiddenRows, ctx.outputRows)
	rejectionWork := indexWork
	baseComparisonWork := ctx.baseWork
	if len(ctx.unknownRangeLowerSelectivities) > 0 && validWork {
		// Compare uncertainty intervals instead of the ranking point estimate. An
		// unbound leading range may be empty, while an unbound residual can reduce
		// hidden rows only when this candidate can evaluate it before backfill.
		// Retain every stage whose work is independent of the unknown values. The
		// base upper bound is a complete column-pruned table scan. Rejection is safe
		// only when the index lower bound still dominates that upper bound.
		lowerOutputRows := 0.0
		if shape == encodedRegularIndexCostIndexOnly {
			// The stable-branch lower estimate entails index-only output rows as
			// well as lookup rows. Keep the downstream decoding work for those rows
			// in the uncertainty lower bound.
			lowerOutputRows = lowerHiddenRows
		}
		rejectionWork, validWork = calculateWork(lowerCandidateRows, lowerHiddenRows, lowerOutputRows)
		baseComparisonWork = ctx.baseUpperWork
	}
	shouldReject = len(idxDef.Parts) >= 2 && ctx.node.Stats.TableCnt >= 50000 && !ctx.force &&
		validWork && rejectionWork >= baseComparisonWork
	ctx.resetScratch()
	if !validWork {
		return 0, false, false
	}
	return indexWork, shouldReject, true
}

// shouldSkipEncodedIndexOnlyScan is the covering-scan entry to the common
// automatic regular-index cost check.
func (builder *QueryBuilder) shouldSkipEncodedIndexOnlyScan(
	idxDef *IndexDef,
	node *plan.Node,
	colRefCnt map[[2]int32]int,
	leadingPos []int32,
	keepResidualLeading bool,
) bool {
	return builder.shouldSkipEncodedRegularIndex(idxDef, node, colRefCnt, leadingPos, keepResidualLeading, encodedRegularIndexCostIndexOnly)
}

// shouldSkipEncodedRegularIndex compares encoded-key scan, extraction, and
// (for an index join) base-row backfill work with the column-pruned base scan.
// Small tables and FORCE INDEX retain compatibility by bypassing rejection;
// their scores only order candidates within the same access shape.
func (builder *QueryBuilder) shouldSkipEncodedRegularIndex(
	idxDef *IndexDef,
	node *plan.Node,
	colRefCnt map[[2]int32]int,
	leadingPos []int32,
	keepResidualLeading bool,
	shape encodedRegularIndexCostShape,
	workOut ...*float64,
) bool {
	costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
	var residualLeadingPos []int32
	if keepResidualLeading {
		residualLeadingPos = leadingPos
	}
	work, skip, _ := costCtx.score(idxDef, leadingPos, residualLeadingPos, shape)
	if len(workOut) > 0 && workOut[0] != nil {
		*workOut[0] = work
	}
	return skip
}

type regularIndexOnlyMatch struct {
	filterIdx          []int32
	filterType         int
	leadingEqual       bool
	residualLeadingPos []int32
}

func (ctx *encodedRegularIndexCostContext) indexCoversRequiredColumns(idxDef *IndexDef) bool {
	for _, part := range idxDef.Parts {
		colPos, ok := ctx.node.TableDef.Name2ColIndex[catalog.ResolveAlias(part)]
		if !ok || colPos < 0 || int(colPos) >= len(ctx.coveredCols) {
			for _, touched := range ctx.touchedCovered {
				ctx.coveredCols[touched] = false
			}
			ctx.touchedCovered = ctx.touchedCovered[:0]
			return false
		}
		if !ctx.coveredCols[colPos] {
			ctx.coveredCols[colPos] = true
			ctx.touchedCovered = append(ctx.touchedCovered, colPos)
		}
	}
	covered := true
	for _, colPos := range ctx.requiredCols {
		if !ctx.coveredCols[colPos] {
			covered = false
			break
		}
	}
	for _, touched := range ctx.touchedCovered {
		ctx.coveredCols[touched] = false
	}
	ctx.touchedCovered = ctx.touchedCovered[:0]
	return covered
}

func (builder *QueryBuilder) matchRegularIndexOnlyScan(
	idxDef *IndexDef,
	node *plan.Node,
	costCtx *encodedRegularIndexCostContext,
) (*regularIndexOnlyMatch, bool) {
	if idxDef == nil || len(idxDef.Parts) == 0 || node == nil || node.TableDef == nil || node.TableDef.Pkey == nil || len(node.BindingTags) == 0 {
		return nil, false
	}
	if regularIndexHasDeclaredPrefix(idxDef) {
		return nil, false
	}
	if !costCtx.indexCoversRequiredColumns(idxDef) {
		return nil, false
	}

	leadingColPos, ok := node.TableDef.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[0])]
	if !ok || leadingColPos < 0 || int(leadingColPos) >= len(costCtx.firstFilters) {
		return nil, false
	}
	firstFilter := costCtx.firstFilters[leadingColPos]
	if firstFilter < 0 {
		return nil, false
	}
	leadingEqual := costCtx.filterTypes[firstFilter] == EqualIndexCondition
	leadingPos := []int32{firstFilter}
	filterType := NonEqualIndexCondition
	if leadingEqual {
		filterType = EqualIndexCondition
		for partPos := 1; partPos < len(idxDef.Parts); partPos++ {
			colPos, ok := node.TableDef.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[partPos])]
			if !ok || colPos < 0 || int(colPos) >= len(costCtx.firstEquals) {
				break
			}
			filterPos := costCtx.firstEquals[colPos]
			if filterPos < 0 {
				break
			}
			leadingPos = append(leadingPos, filterPos)
		}
	} else {
		lowerPos := costCtx.lowerFilters[leadingColPos]
		upperPos := costCtx.upperFilters[leadingColPos]
		if lowerPos >= 0 && upperPos >= 0 &&
			(firstFilter == lowerPos || firstFilter == upperPos) &&
			regularIndexRangeFunctionsUsable(
				idxDef, node.TableDef,
				node.FilterList[lowerPos].GetF(), node.FilterList[upperPos].GetF(), true,
			) {
			leadingPos = []int32{lowerPos, upperPos}
			filterType = RangeIndexCondition
		}
	}
	if !leadingEqual && node.Stats != nil && node.Stats.TableCnt >= 50000 {
		if node.Stats.Selectivity >= InFilterSelectivityLimit ||
			node.Stats.Outcnt >= float64(GetInFilterCardLimitOnPK(builder.compCtx.GetProcess().GetService(), node.Stats.TableCnt)) {
			return nil, false
		}
	}

	numParts := len(idxDef.Parts)
	if idxDef.Unique {
		if leadingEqual && len(leadingPos) < numParts {
			return nil, false
		}
		if !leadingEqual && numParts > 1 {
			return nil, false
		}
	}

	if !leadingEqual {
		firstFn := node.FilterList[leadingPos[0]].GetF()
		var secondFn *plan.Function
		allowUnsafeRange := false
		if filterType == RangeIndexCondition {
			secondFn = node.FilterList[leadingPos[1]].GetF()
			allowUnsafeRange = true
		}
		if !regularIndexRangeFunctionsUsable(idxDef, node.TableDef, firstFn, secondFn, allowUnsafeRange) {
			return nil, false
		}
	}
	numKeyParts := numParts
	if !idxDef.Unique {
		numKeyParts--
	}
	if numKeyParts == 0 {
		return nil, false
	}
	residualLeadingPos := indexOnlyResidualLeadingFilterPositionsForPrefix(
		idxDef, node.TableDef, node.FilterList, leadingPos,
		indexOnlyLookupWillUsePrefixComparison(idxDef, node.FilterList, leadingPos, filterType, leadingEqual),
	)
	return &regularIndexOnlyMatch{
		filterIdx: leadingPos, filterType: filterType, leadingEqual: leadingEqual,
		residualLeadingPos: residualLeadingPos,
	}, true
}

func (builder *QueryBuilder) tryIndexOnlyScan(idxDef *IndexDef, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, scanSnapshot *Snapshot) int32 {
	costCtx := builder.newEncodedRegularIndexCostContext(node, colRefCnt)
	match, ok := builder.matchRegularIndexOnlyScan(idxDef, node, costCtx)
	if !ok {
		return -1
	}
	if _, skip, _ := costCtx.score(idxDef, match.filterIdx, match.residualLeadingPos, encodedRegularIndexCostIndexOnly); skip {
		return -1
	}
	return builder.applyRegularIndexOnlyScan(idxDef, node, idxColMap, scanSnapshot, match)
}

func (builder *QueryBuilder) applyRegularIndexOnlyScan(
	idxDef *IndexDef,
	node *plan.Node,
	idxColMap map[[2]int32]*plan.Expr,
	scanSnapshot *Snapshot,
	match *regularIndexOnlyMatch,
) int32 {
	leadingPos := match.filterIdx
	numParts := len(idxDef.Parts)
	missFilterIdx := make([]int, 0, len(node.FilterList))
	for filterIdx := range node.FilterList {
		isLeading := false
		for _, leadingIdx := range leadingPos {
			if filterIdx == int(leadingIdx) {
				isLeading = true
				break
			}
		}
		if !isLeading {
			missFilterIdx = append(missFilterIdx, filterIdx)
		}
	}

	idxTag := builder.genNewBindTag()
	idxObjRef, idxTableDef, e := builder.compCtx.ResolveIndexTableByRef(node.ObjRef, idxDef.IndexTableName, scanSnapshot)
	if e != nil {
		panic(e)
	}
	leadingColExpr := GetColExpr(idxTableDef.Cols[0].Typ, idxTag, 0)
	var newLeadingFilter *plan.Expr
	var err error
	if match.filterType == RangeIndexCondition {
		newLeadingFilter, err = builder.replaceRangePairCondition(idxDef, node.FilterList, leadingPos, idxTag, idxTableDef)
	} else {
		newLeadingFilter, err = builder.replaceLeadingFilter(idxDef, node.FilterList, leadingPos, match.leadingEqual, idxTag, idxTableDef)
	}
	if err != nil {
		return -1
	}
	builder.addNameByColRef(idxTag, idxTableDef)

	if numParts == 1 {
		colIdx := node.TableDef.Name2ColIndex[idxDef.Parts[0]]
		idxColMap[[2]int32{node.BindingTags[0], colIdx}] = leadingColExpr
	} else {
		for i := 0; i < numParts; i++ {
			colName := catalog.ResolveAlias(idxDef.Parts[i])
			colIdx := node.TableDef.Name2ColIndex[colName]
			if colName == node.TableDef.Pkey.PkeyColName {
				idxColMap[[2]int32{node.BindingTags[0], colIdx}] = GetColExpr(idxTableDef.Cols[1].Typ, idxTag, 1)
			} else {
				origType := node.TableDef.Cols[colIdx].Typ
				mappedExpr, _ := MakeSerialExtractExpr(builder.GetContext(), DeepCopyExpr(leadingColExpr), origType, int64(i))
				idxColMap[[2]int32{node.BindingTags[0], colIdx}] = mappedExpr
			}
		}
	}

	residualLeadingPos := indexOnlyResidualLeadingFilterPositions(idxDef, node.TableDef, node.FilterList, leadingPos, newLeadingFilter)
	filterCapacity := 1 + len(missFilterIdx) + len(residualLeadingPos)
	newFilterList := make([]*plan.Expr, 0, filterCapacity)
	newFilterList = append(newFilterList, newLeadingFilter)
	if len(residualLeadingPos) > 0 {
		// Keep the original SQL predicate when serial_full lookup bytes are not
		// an exact semantic oracle: NULL is encoded as key bytes, and a byte-string
		// terminator can also begin an escaped embedded NUL.
		for _, idx := range residualLeadingPos {
			residual := replaceColumnsForExpr(DeepCopyExpr(node.FilterList[idx]), idxColMap)
			// The lookup predicate already carries the original selectivity. This
			// residual is an exact semantic recheck, not another reduction.
			residual.Selectivity = 1
			newFilterList = append(newFilterList, residual)
		}
	}
	for _, idx := range missFilterIdx {
		newFilterList = append(newFilterList, replaceColumnsForExpr(node.FilterList[idx], idxColMap))
	}

	// recod index table scan info
	idxScanInfo := plan.IndexScanInfo{
		IsIndexScan:    true,
		IndexName:      idxDef.IndexName,
		BelongToTable:  node.ObjRef.ObjName,
		Parts:          slices.Clone(idxDef.Parts),
		IsUnique:       idxDef.Unique,
		IndexTableName: idxDef.IndexTableName,
	}

	idxTableNodeID := builder.appendNode(&plan.Node{
		NodeType:      plan.Node_TABLE_SCAN,
		TableDef:      idxTableDef,
		IndexScanInfo: idxScanInfo,
		ObjRef:        idxObjRef,
		ParentObjRef:  node.ObjRef,
		FilterList:    newFilterList,
		Limit:         node.Limit,
		Offset:        node.Offset,
		BindingTags:   []int32{idxTag},
		ScanSnapshot:  node.ScanSnapshot,
	}, builder.ctxByNode[node.NodeId])
	builder.inheritIndexHints(idxTableNodeID, node.NodeId)

	forceScanNodeStatsTP(idxTableNodeID, builder)
	return idxTableNodeID
}

func (builder *QueryBuilder) trySpatialIndexOnlyScan(idxDef *IndexDef, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, scanSnapshot *Snapshot) int32 {
	if !isSpatialIndexDef(idxDef) || len(node.BindingTags) == 0 || len(node.FilterList) == 0 {
		return -1
	}

	filterIdx := findSpatialIndexFilter(idxDef, node)
	if filterIdx == -1 {
		return -1
	}

	partColIdx, ok := node.TableDef.Name2ColIndex[indexPrimaryPartName(idxDef)]
	if !ok {
		return -1
	}
	pkColIdx, ok := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
	if !ok {
		return -1
	}

	for i := range node.TableDef.Cols {
		if colRefCnt[[2]int32{node.BindingTags[0], int32(i)}] == 0 {
			continue
		}
		if int32(i) != partColIdx && int32(i) != pkColIdx {
			return -1
		}
	}

	idxTag := builder.genNewBindTag()
	idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(node.ObjRef, idxDef.IndexTableName, scanSnapshot)
	if err != nil {
		panic(err)
	}
	builder.addNameByColRef(idxTag, idxTableDef)

	spatialColMap := buildSpatialIndexColMap(idxDef, node, idxTag, idxTableDef)

	newFilterList := make([]*plan.Expr, 0, len(node.FilterList))
	for _, filter := range node.FilterList {
		if !exprUsesOnlyMappedCols(filter, spatialColMap) {
			return -1
		}
		newFilterList = append(newFilterList, replaceColumnsForExpr(DeepCopyExpr(filter), spatialColMap))
	}

	idxScanInfo := plan.IndexScanInfo{
		IsIndexScan:    true,
		IndexName:      idxDef.IndexName,
		BelongToTable:  node.ObjRef.ObjName,
		Parts:          slices.Clone(idxDef.Parts),
		IsUnique:       idxDef.Unique,
		IndexTableName: idxDef.IndexTableName,
	}

	idxTableNodeID := builder.appendNode(&plan.Node{
		NodeType:      plan.Node_TABLE_SCAN,
		TableDef:      idxTableDef,
		IndexScanInfo: idxScanInfo,
		ObjRef:        idxObjRef,
		ParentObjRef:  node.ObjRef,
		FilterList:    newFilterList,
		Limit:         node.Limit,
		Offset:        node.Offset,
		BindingTags:   []int32{idxTag},
		ScanSnapshot:  node.ScanSnapshot,
	}, builder.ctxByNode[node.NodeId])
	builder.inheritIndexHints(idxTableNodeID, node.NodeId)

	for key, expr := range spatialColMap {
		idxColMap[key] = expr
	}
	forceScanNodeStatsTP(idxTableNodeID, builder)
	return idxTableNodeID
}

func (builder *QueryBuilder) getIndexForSpatialCond(indexes []*IndexDef, node *plan.Node) (int, []int32) {
	for i, idxDef := range indexes {
		if !isSpatialIndexDef(idxDef) {
			continue
		}
		if filterIdx := findSpatialIndexFilter(idxDef, node); filterIdx != -1 {
			return i, []int32{filterIdx}
		}
	}
	return -1, nil
}

func isLowerBoundOp(name string) bool {
	return name == ">=" || name == ">"
}

func isUpperBoundOp(name string) bool {
	return name == "<=" || name == "<"
}

// classifyRangeBound returns the column and whether the filter is a lower bound.
// Returns nil if the filter is not a range comparison with a column and constant.
func classifyRangeBound(fn *plan.Function) (col *plan.ColRef, isLower bool) {
	if fn == nil || fn.Func == nil || len(fn.Args) < 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return nil, false
	}
	op := canonicalRangeOp(fn)
	if !isLowerBoundOp(op) && !isUpperBoundOp(op) {
		return nil, false
	}
	if fn.Args[0].GetCol() != nil && isRuntimeConstExpr(fn.Args[1]) {
		return fn.Args[0].GetCol(), isLowerBoundOp(op)
	}
	if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
		return fn.Args[1].GetCol(), isLowerBoundOp(op)
	}
	return nil, false
}

// regularIndexRangeFunctionsUsable is the shared semantic gate for a range
// lookup against one physical regular index. Direct unique keys compare values
// in their declared type and need no serialized-key restrictions. Composite
// keys must encode every DECIMAL bound losslessly; a lone <= or > is unsafe
// because the persisted key has a longer suffix. Opposing paired bounds may
// use those operators because replaceRangePairCondition emits one bounded
// prefix range.
func regularIndexRangeFunctionsUsable(
	idxDef *IndexDef,
	tableDef *plan.TableDef,
	first *plan.Function,
	second *plan.Function,
	allowUnsafeRange bool,
) bool {
	if idxDef == nil || tableDef == nil || len(idxDef.Parts) == 0 || first == nil {
		return false
	}
	if !indexTableStoresSerializedKey(idxDef) {
		return true
	}
	leadingColPos, ok := tableDef.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[0])]
	if !ok || leadingColPos < 0 || int(leadingColPos) >= len(tableDef.Cols) {
		return false
	}
	indexedPartType := tableDef.Cols[leadingColPos].Typ
	if !canSerializeDecimalIndexRangeBounds(first, indexedPartType) ||
		(second != nil && !canSerializeDecimalIndexRangeBounds(second, indexedPartType)) {
		return false
	}
	if !allowUnsafeRange && (hasUnsafeRangeOp(first) || (second != nil && hasUnsafeRangeOp(second))) {
		return false
	}
	return true
}

func (builder *QueryBuilder) getIndexForNonEquiCond(indexes []*IndexDef, node *plan.Node) (int, []int32) {
	type indexCandidates struct {
		preferred int
		direct    int
		hasDirect bool
	}
	colPos2Candidates := make(map[int32]indexCandidates)
	for i, idxDef := range indexes {
		// Prefix keys are lossy. IN and range operators can use block-level
		// pruning implementations that compare the untruncated probe with the
		// persisted prefix and under-fetch after flush. Keep prefix indexes for
		// equality candidate lookup only, where the probe is explicitly truncated.
		if regularIndexHasDeclaredPrefix(idxDef) {
			continue
		}
		numParts := len(idxDef.Parts)
		if !idxDef.Unique {
			numParts--
		}
		if (idxDef.Unique && numParts != 1) || (!idxDef.Unique && numParts < 1) {
			continue
		}

		colPos, ok := node.TableDef.Name2ColIndex[catalog.ResolveAlias(idxDef.Parts[0])]
		if !ok {
			continue
		}
		candidates := colPos2Candidates[colPos]
		candidates.preferred = i
		if !indexTableStoresSerializedKey(idxDef) {
			candidates.direct = i
			candidates.hasDirect = true
		}
		colPos2Candidates[colPos] = candidates
	}
	// Preserve the existing catalog-order preference for ordinary bounds. If a
	// serialized candidate cannot represent the bound losslessly or safely as
	// a single-ended range, use a direct unique key on the same column before
	// giving up on the index lookup altogether. Unsafe operators are allowed
	// while discovering, and after selecting, a paired range.
	selectIndex := func(candidates indexCandidates, first, second *plan.Function, allowUnsafeRange bool) (int, bool) {
		if regularIndexRangeFunctionsUsable(
			indexes[candidates.preferred], node.TableDef, first, second, allowUnsafeRange,
		) {
			return candidates.preferred, true
		}
		if candidates.hasDirect {
			return candidates.direct, true
		}
		return -1, false
	}

	// First pass: detect paired range conditions on index leading columns.
	colLowerBounds := make(map[int32]int32) // colPos -> filter index
	colUpperBounds := make(map[int32]int32) // colPos -> filter index

	for i := range node.FilterList {
		fn := node.FilterList[i].GetF()
		if fn == nil || len(fn.Args) < 2 {
			continue
		}
		col, isLower := classifyRangeBound(fn)
		if col == nil {
			continue
		}
		candidates, ok := colPos2Candidates[col.ColPos]
		if !ok {
			continue
		}
		if _, ok = selectIndex(candidates, fn, nil, true); !ok {
			continue
		}
		if isLower {
			colLowerBounds[col.ColPos] = int32(i)
		} else {
			colUpperBounds[col.ColPos] = int32(i)
		}
	}

	// Second pass: find non-equi conditions (in, between, in_range, or, single range ops)
	for i := range node.FilterList {
		fn := node.FilterList[i].GetF()
		filterType, col := checkIndexFilter(fn)
		if filterType == NonEqualIndexCondition {
			candidates, ok := colPos2Candidates[col.ColPos]
			if !ok {
				continue
			}
			if rangeCol, _ := classifyRangeBound(fn); rangeCol != nil {
				lowerIdx, hasLower := colLowerBounds[rangeCol.ColPos]
				upperIdx, hasUpper := colUpperBounds[rangeCol.ColPos]
				if hasLower && hasUpper && int32(i) == min(lowerIdx, upperIdx) {
					idxPos, usable := selectIndex(
						candidates,
						node.FilterList[lowerIdx].GetF(),
						node.FilterList[upperIdx].GetF(),
						true,
					)
					if !usable {
						continue
					}
					if shouldSkipLargeRangeIndexByStats(node) {
						continue
					}
					return idxPos, []int32{lowerIdx, upperIdx}
				}
			}
			idxPos, usable := selectIndex(candidates, fn, nil, false)
			if !usable {
				continue
			}
			if fn != nil && isRangeOp(fn) && shouldSkipLargeRangeIndexByStats(node) {
				continue
			}
			return idxPos, []int32{int32(i)}
		}
	}
	return -1, nil
}

func shouldSkipLargeRangeIndexByStats(node *plan.Node) bool {
	if node == nil || node.Stats == nil || node.Stats.TableCnt < 50000 {
		return false
	}
	return node.Stats.Selectivity >= InFilterSelectivityLimit || node.Stats.Outcnt >= float64(InFilterCardLimitNonPK)
}

// hasUnsafeRangeOp returns true if fn (or any OR arm within it) uses <= or >
// which are unsafe on serialized multi-part composite index keys.
// For prefix-encoded keys, serial(v, pk) is always > serial(v) because
// the full key is longer. Therefore:
//   - >= is safe: serial(v, pk) >= serial(bound) correctly matches v >= bound
//   - <  is safe: serial(v, pk) < serial(bound) correctly matches v < bound
//   - <= is UNSAFE: serial(v, pk) <= serial(v) is always FALSE (under-fetches)
//   - >  is UNSAFE: serial(v, pk) > serial(v) is always TRUE (over-fetches)
//
// Only recurses into OR arms; AND arms are safe because checkIndexFilter pre-rejects
// any AND-nested expression that isn't a simple comparison on an indexed column.
func hasUnsafeRangeOp(fn *plan.Function) bool {
	if fn == nil {
		return false
	}
	if fn.Func.ObjName == "or" {
		for _, arg := range fn.Args {
			if hasUnsafeRangeOp(arg.GetF()) {
				return true
			}
		}
		return false
	}
	op := canonicalRangeOp(fn)
	return op == "<=" || op == ">"
}

func isRangeOp(fn *plan.Function) bool {
	switch fn.Func.ObjName {
	case ">=", ">", "<=", "<", "in_range":
		return true
	}
	return false
}

func canonicalRangeOp(fn *plan.Function) string {
	if fn == nil || fn.Func == nil {
		return ""
	}
	if len(fn.Args) < 2 || fn.Args[0] == nil || fn.Args[1] == nil {
		return fn.Func.ObjName
	}
	if fn.Args[0].GetCol() != nil && isRuntimeConstExpr(fn.Args[1]) {
		return fn.Func.ObjName
	}
	if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
		switch fn.Func.ObjName {
		case ">":
			return "<"
		case ">=":
			return "<="
		case "<":
			return ">"
		case "<=":
			return ">="
		}
	}
	return fn.Func.ObjName
}

func rangeFilterConstValue(fn *plan.Function) *plan.Expr {
	if len(fn.Args) < 2 {
		return nil
	}
	if fn.Args[0].GetCol() != nil && isRuntimeConstExpr(fn.Args[1]) {
		return fn.Args[1]
	}
	if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
		return fn.Args[0]
	}
	return nil
}

func rangeFilterColumnType(fn *plan.Function) (plan.Type, bool) {
	if len(fn.Args) < 2 {
		return plan.Type{}, false
	}
	if fn.Args[0].GetCol() != nil && isRuntimeConstExpr(fn.Args[1]) {
		return fn.Args[0].Typ, true
	}
	if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
		return fn.Args[1].Typ, true
	}
	return plan.Type{}, false
}

func canSerializeDecimalIndexRangeBound(bound *plan.Expr, indexedPartType plan.Type) bool {
	indexedType := makeTypeByPlan2Type(indexedPartType)
	if !indexedType.Oid.IsDecimal() {
		return true
	}
	if bound == nil {
		return false
	}
	boundType := makeTypeByPlan2Expr(bound)
	if !boundType.Oid.IsDecimal() {
		return false
	}
	if boundType.Oid == indexedType.Oid && boundType.Scale == indexedType.Scale {
		return true
	}
	return checkNoNeedCast(boundType, indexedType, bound)
}

func canSerializeDecimalIndexRangeBounds(fn *plan.Function, indexedPartType plan.Type) bool {
	if fn == nil || fn.Func == nil {
		return false
	}
	if fn.Func.ObjName == "or" {
		for _, arg := range fn.Args {
			if !canSerializeDecimalIndexRangeBounds(arg.GetF(), indexedPartType) {
				return false
			}
		}
		return true
	}
	if !types.T(indexedPartType.Id).IsDecimal() {
		return true
	}
	switch fn.Func.ObjName {
	case "between", "in_range":
		return len(fn.Args) >= 3 &&
			canSerializeDecimalIndexRangeBound(fn.Args[1], indexedPartType) &&
			canSerializeDecimalIndexRangeBound(fn.Args[2], indexedPartType)
	case ">", ">=", "<", "<=":
		return canSerializeDecimalIndexRangeBound(rangeFilterConstValue(fn), indexedPartType)
	default:
		return true
	}
}

func (builder *QueryBuilder) normalizeDecimalIndexRangeBound(bound *plan.Expr, indexedPartType plan.Type) *plan.Expr {
	if bound == nil || !types.T(indexedPartType.Id).IsDecimal() ||
		(bound.Typ.Id == indexedPartType.Id && bound.Typ.Scale == indexedPartType.Scale) ||
		!canSerializeDecimalIndexRangeBound(bound, indexedPartType) {
		return bound
	}
	normalized, err := forceCastExpr(builder.GetContext(), bound, indexedPartType)
	if err != nil {
		return bound
	}
	return normalized
}

func (builder *QueryBuilder) replaceRangePairCondition(idxDef *IndexDef, filterList []*plan.Expr, filterIdx []int32, idxTag int32, idxTableDef *plan.TableDef) (*plan.Expr, error) {
	numParts := len(idxDef.Parts)
	lowerFn := filterList[filterIdx[0]].GetF()
	upperFn := filterList[filterIdx[1]].GetF()

	lowerOp := canonicalRangeOp(lowerFn)
	upperOp := canonicalRangeOp(upperFn)

	colExpr := GetColExpr(idxTableDef.Cols[0].Typ, idxTag, 0)
	lowerVal := DeepCopyExpr(rangeFilterConstValue(lowerFn))
	upperVal := DeepCopyExpr(rangeFilterConstValue(upperFn))

	compositeFilterSel := filterList[filterIdx[0]].Selectivity * filterList[filterIdx[1]].Selectivity

	if numParts > 1 {
		if indexedPartType, ok := rangeFilterColumnType(lowerFn); ok {
			lowerVal = builder.normalizeDecimalIndexRangeBound(lowerVal, indexedPartType)
		}
		if indexedPartType, ok := rangeFilterColumnType(upperFn); ok {
			upperVal = builder.normalizeDecimalIndexRangeBound(upperVal, indexedPartType)
		}
		serialFunc := indexTableComparisonSerialFunc()
		var err error
		lowerVal, err = BindFuncExprImplByPlanExpr(builder.GetContext(), serialFunc, []*plan.Expr{lowerVal})
		if err != nil {
			return nil, err
		}
		upperVal, err = BindFuncExprImplByPlanExpr(builder.GetContext(), serialFunc, []*plan.Expr{upperVal})
		if err != nil {
			return nil, err
		}
	}

	if lowerOp == ">=" && upperOp == "<=" {
		funcName := "between"
		if numParts > 1 {
			funcName = "prefix_between"
		}
		expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{colExpr, lowerVal, upperVal})
		if err != nil {
			return nil, err
		}
		expr.Selectivity = compositeFilterSel
		return expr, nil
	}

	var flag uint8
	if lowerOp == ">" {
		flag |= 1
	}
	if upperOp == "<" {
		flag |= 2
	}
	if numParts > 1 && indexFunctionComparesByteStringColumn(lowerFn) {
		// See replaceNonEqualCondition: make the prefix condition a candidate
		// superset when the original lower bound is open.
		flag &^= 1
	}
	funcName := "in_range"
	if numParts > 1 {
		funcName = "prefix_in_range"
	}
	expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{colExpr, lowerVal, upperVal, MakePlan2Uint8ConstExprWithType(flag)})
	if err != nil {
		return nil, err
	}
	expr.Selectivity = compositeFilterSel
	return expr, nil
}

func (builder *QueryBuilder) applyIndexJoin(idxDef *IndexDef, node *plan.Node, filterType int, filterIdx []int32, scanSnapshot *Snapshot) (int32, int32) {
	idxTag := builder.genNewBindTag()
	idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(node.ObjRef, idxDef.IndexTableName, scanSnapshot)
	if err != nil {
		panic(err)
	}

	var idxFilter *plan.Expr
	if filterType == EqualIndexCondition {
		idxFilter, err = builder.replaceEqualCondition(idxDef, node.FilterList, filterIdx, idxTag, idxTableDef)
		if err != nil {
			return node.NodeId, -1
		}
	} else if filterType == SpatialIndexCondition {
		spatialColMap := buildSpatialIndexColMap(idxDef, node, idxTag, idxTableDef)
		idxFilter = replaceColumnsForExpr(DeepCopyExpr(node.FilterList[filterIdx[0]]), spatialColMap)
	} else if filterType == RangeIndexCondition {
		idxFilter, err = builder.replaceRangePairCondition(idxDef, node.FilterList, filterIdx, idxTag, idxTableDef)
	} else {
		idxFilter, err = builder.replaceNonEqualCondition(idxDef, node.FilterList[filterIdx[0]], idxTag, idxTableDef)
	}
	if err != nil {
		return node.NodeId, -1
	}
	builder.addNameByColRef(idxTag, idxTableDef)

	// recod index table scan info
	idxScanInfo := plan.IndexScanInfo{
		IsIndexScan:    true,
		IndexName:      idxDef.IndexName,
		BelongToTable:  node.ObjRef.ObjName,
		Parts:          slices.Clone(idxDef.Parts),
		IsUnique:       idxDef.Unique,
		IndexTableName: idxDef.IndexTableName,
	}

	idxTableNode := &plan.Node{
		NodeType:      plan.Node_TABLE_SCAN,
		TableDef:      idxTableDef,
		ObjRef:        idxObjRef,
		IndexScanInfo: idxScanInfo,
		ParentObjRef:  DeepCopyObjectRef(node.ObjRef),
		FilterList:    []*plan.Expr{idxFilter},
		BindingTags:   []int32{idxTag},
		ScanSnapshot:  node.ScanSnapshot,
	}
	idxTableNodeID := builder.appendNode(idxTableNode, builder.ctxByNode[node.NodeId])
	builder.inheritIndexHints(idxTableNodeID, node.NodeId)
	forceScanNodeStatsTP(idxTableNodeID, builder)

	pkIdx := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
	pkExpr := GetColExpr(node.TableDef.Cols[pkIdx].Typ, node.BindingTags[0], pkIdx)

	joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		DeepCopyExpr(pkExpr),
		GetColExpr(pkExpr.Typ, idxTag, 1),
	})
	joinNode := &plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{node.NodeId, idxTableNodeID},
		JoinType: plan.Node_INDEX,
		OnList:   []*plan.Expr{joinCond},
	}
	joinNodeID := builder.appendNode(joinNode, builder.ctxByNode[node.NodeId])

	if len(node.FilterList) == 0 {
		idxTableNode.Limit, idxTableNode.Offset = node.Limit, node.Offset
	} else {
		joinNode.Limit, joinNode.Offset = node.Limit, node.Offset
	}
	node.Limit, node.Offset = nil, nil

	return joinNodeID, idxTableNodeID
}

func (builder *QueryBuilder) applyIndicesForJoins(nodeID int32, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	sid := builder.compCtx.GetProcess().GetService()

	if changed, err := builder.applyFullTextFiltersForJoinChildren(nodeID, node, colRefCnt, idxColMap); err != nil || changed {
		return nodeID, err
	}

	if node.JoinType != plan.Node_INNER && node.JoinType != plan.Node_RIGHT && node.JoinType != plan.Node_SEMI &&
		(node.JoinType != plan.Node_ANTI || !node.IsRightJoin) {
		return nodeID, nil
	}
	leftForcesJoin := builder.scanForcesJoinIndex(node.Children[0])
	rightForcesJoin := builder.scanForcesJoinIndex(node.Children[1])
	if leftForcesJoin && builder.qry.Nodes[node.Children[0]].NodeType != plan.Node_TABLE_SCAN {
		leftAccess, err := builder.applyForcedJoinAccess(node.Children[0])
		if err != nil {
			return -1, err
		}
		node.Children[0] = leftAccess
	}
	if node.JoinType == plan.Node_INNER && rightForcesJoin {
		rightAccess, err := builder.applyForcedJoinAccess(node.Children[1])
		if err != nil {
			return -1, err
		}
		node.Children[1] = rightAccess
	}
	if rightForcesJoin && !leftForcesJoin {
		return nodeID, nil
	}

	leftChild := builder.qry.Nodes[node.Children[0]]
	if leftChild.NodeType != plan.Node_TABLE_SCAN {
		return nodeID, nil
	}
	if builder.isScanProtected(leftChild.NodeId) {
		return nodeID, nil
	}

	//----------------------------------------------------------------------
	//ts2 := leftChild.GetScanTS()

	scanSnapshot := leftChild.ScanSnapshot
	if scanSnapshot == nil {
		scanSnapshot = &Snapshot{}
	}
	//----------------------------------------------------------------------

	rightChild := builder.qry.Nodes[node.Children[1]]
	forceJoinIndex := false
	if hintSet := builder.indexHintsByScan[leftChild.NodeId]; hintSet != nil {
		forceJoinIndex = hintSet.join.forceSpecified
	}

	if !forceJoinIndex && rightChild.Stats.Selectivity > 0.5 {
		return nodeID, nil
	}

	if !forceJoinIndex && (rightChild.Stats.Outcnt > float64(GetInFilterCardLimitOnPK(sid, leftChild.Stats.TableCnt)) || rightChild.Stats.Outcnt > leftChild.Stats.Cost*0.1) {
		return nodeID, nil
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}

	type indexJoinCondition struct {
		exprIdx  int
		hashSlot int
	}
	col2Cond := make(map[int32]indexJoinCondition)
	hashSlot := 0
	for i, expr := range node.OnList {
		if !isEquiCond(expr, leftTags, rightTags) {
			continue
		}

		col := expr.GetF().Args[0].GetCol()
		if col == nil {
			hashSlot++
			continue
		}

		col2Cond[col.ColPos] = indexJoinCondition{
			exprIdx:  i,
			hashSlot: hashSlot,
		}
		hashSlot++
	}

	if leftChild.TableDef == nil || leftChild.TableDef.Pkey == nil {
		return nodeID, nil
	}

	joinOnPK := true
	for _, part := range leftChild.TableDef.Pkey.Names {
		colIdx := leftChild.TableDef.Name2ColIndex[part]
		_, ok := col2Cond[colIdx]
		if !ok {
			joinOnPK = false
			break
		}
	}

	if joinOnPK {
		return nodeID, nil
	}

	indexes := builder.filterRegularIndexesByJoinHints(leftChild, leftChild.TableDef.Indexes)
	condIdx := make([]indexJoinCondition, 0, len(col2Cond))
	for _, idxDef := range indexes {
		if idxDef == nil || !idxDef.TableExist ||
			!catalog.IsRegularIndexAlgo(idxDef.IndexAlgo) ||
			isSpatialIndexDef(idxDef) ||
			!regularIndexPrefixMetadataUsable(idxDef) ||
			regularIndexHasDeclaredPrefix(idxDef) {
			continue
		}

		numParts := len(idxDef.Parts)
		numKeyParts := numParts
		if !idxDef.Unique {
			numKeyParts--
		}
		if numKeyParts == 0 || numKeyParts > len(col2Cond) {
			continue
		}

		condIdx = condIdx[:0]
		for i := 0; i < numKeyParts; i++ {
			tmpName := catalog.ResolveAlias(idxDef.Parts[i])
			colIdx := leftChild.TableDef.Name2ColIndex[tmpName]
			idx, ok := col2Cond[colIdx]
			if !ok {
				break
			}

			condIdx = append(condIdx, idx)
		}

		if len(condIdx) < numKeyParts {
			continue
		}

		idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(leftChild.ObjRef, idxDef.IndexTableName, scanSnapshot)
		if err != nil {
			return -1, err
		}
		if idxObjRef == nil || idxTableDef == nil || len(idxTableDef.Cols) < 2 || leftChild.ObjRef == nil ||
			leftChild.TableDef.Pkey == nil || len(leftChild.BindingTags) == 0 {
			return -1, moerr.NewInternalErrorf(builder.GetContext(), "invalid metadata for index %s", idxDef.IndexName)
		}
		pkIdx, ok := leftChild.TableDef.Name2ColIndex[leftChild.TableDef.Pkey.PkeyColName]
		if !ok || pkIdx < 0 || int(pkIdx) >= len(leftChild.TableDef.Cols) {
			return -1, moerr.NewInternalErrorf(builder.GetContext(), "invalid primary key metadata for index %s", idxDef.IndexName)
		}

		idxTag := builder.genNewBindTag()
		builder.addNameByColRef(idxTag, idxTableDef)

		rfTag := builder.genNewMsgTag()

		var rfBuildExpr *plan.Expr
		var componentProbeExprs []*plan.Expr
		if numParts == 1 {
			condition := node.OnList[condIdx[0].exprIdx].GetF()
			rfBuildExpr = &plan.Expr{
				Typ: condition.Args[1].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(condIdx[0].hashSlot),
					},
				},
			}
		} else {
			serialArgs := make([]*plan.Expr, len(condIdx))
			componentProbeExprs = make([]*plan.Expr, len(condIdx))
			for i := range condIdx {
				componentProbeExprs[i] =
					node.OnList[condIdx[i].exprIdx].GetF().Args[0]
				serialArgs[i] = &plan.Expr{
					Typ: node.OnList[condIdx[i].exprIdx].GetF().Args[1].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -1,
							ColPos: int32(condIdx[i].hashSlot),
						},
					},
				}
			}
			rfBuildExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), indexTableStoredKeySerialFunc(idxDef), serialArgs)
		}

		probeExpr := &plan.Expr{
			Typ: idxTableDef.Cols[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag,
					ColPos: 0,
				},
			},
		}
		var nodeProbeRuntimeFilter, nodeBuildRuntimeFilter *plan.RuntimeFilterSpec
		var hasRuntimeFilter bool
		if len(componentProbeExprs) == 0 {
			nodeProbeRuntimeFilter, nodeBuildRuntimeFilter, hasRuntimeFilter =
				builder.makeExactRuntimeFilterPair(
					rfTag,
					len(condIdx) < numParts,
					GetInFilterCardLimitOnPK(sid, leftChild.Stats.TableCnt),
					probeExpr,
					rfBuildExpr,
					false,
				)
		} else {
			nodeProbeRuntimeFilter, nodeBuildRuntimeFilter, hasRuntimeFilter =
				builder.makeSerializedExactRuntimeFilterPair(
					rfTag,
					len(condIdx) < numParts,
					GetInFilterCardLimitOnPK(sid, leftChild.Stats.TableCnt),
					probeExpr,
					rfBuildExpr,
					componentProbeExprs,
					false,
				)
		}
		if !hasRuntimeFilter && !forceJoinIndex {
			// The index lookup cost model assumes targeted runtime-filter
			// pruning.  Without a materializable exact pair this rewrite would
			// scan the index table under a no-op PASS dependency.
			continue
		}
		// FORCE INDEX remains an explicit request to use the index even when
		// its serialized lookup key has no proven exact-filter contract. Honor
		// the hint without publishing a no-op dependency or optimistic stats.

		// recod index table scan info
		idxScanInfo := plan.IndexScanInfo{
			IsIndexScan:    true,
			IndexName:      idxDef.IndexName,
			BelongToTable:  leftChild.ObjRef.ObjName,
			Parts:          slices.Clone(idxDef.Parts),
			IsUnique:       idxDef.Unique,
			IndexTableName: idxDef.IndexTableName,
		}

		idxTableNode := &plan.Node{
			NodeType:      plan.Node_TABLE_SCAN,
			TableDef:      idxTableDef,
			ObjRef:        idxObjRef,
			IndexScanInfo: idxScanInfo,
			ParentObjRef:  DeepCopyObjectRef(leftChild.ObjRef),
			BindingTags:   []int32{idxTag},
			ScanSnapshot:  leftChild.ScanSnapshot,
		}
		if hasRuntimeFilter {
			idxTableNode.RuntimeFilterProbeList =
				[]*plan.RuntimeFilterSpec{nodeProbeRuntimeFilter}
		}
		idxTableNodeID := builder.appendNode(idxTableNode, builder.ctxByNode[nodeID])
		builder.inheritIndexHints(idxTableNodeID, leftChild.NodeId)

		if hasRuntimeFilter {
			node.RuntimeFilterBuildList = append(
				node.RuntimeFilterBuildList, nodeBuildRuntimeFilter)
			recalcStatsByRuntimeFilter(builder.qry.Nodes[idxTableNodeID], node, builder)
		}

		pkExpr := &plan.Expr{
			Typ: leftChild.TableDef.Cols[pkIdx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: leftChild.BindingTags[0],
					ColPos: pkIdx,
				},
			},
		}
		pkJoinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			pkExpr,
			{
				Typ: pkExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag,
						ColPos: 1,
					},
				},
			},
		})

		idxJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{node.Children[0], idxTableNodeID},
			JoinType: plan.Node_INDEX,
			Limit:    leftChild.Limit,
			Offset:   leftChild.Offset,
			OnList:   []*plan.Expr{pkJoinCond},
		}, builder.ctxByNode[nodeID])

		leftChild.Limit, leftChild.Offset = nil, nil

		node.Children[0] = idxJoinNodeID

		break
	}

	return nodeID, nil
}

func (builder *QueryBuilder) scanForcesJoinIndex(nodeID int32) bool {
	scan := builder.baseScanForIndexAccess(nodeID)
	if scan == nil {
		return false
	}
	hints := builder.indexHintsByScan[scan.NodeId]
	return hints != nil && hints.join.forceSpecified
}

func (builder *QueryBuilder) baseScanForIndexAccess(nodeID int32) *plan.Node {
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return nil
	}
	if owner := builder.indexHintOwnerScan(nodeID); owner != nil {
		return owner
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return nil
	}
	if node.NodeType == plan.Node_TABLE_SCAN {
		return node
	}
	if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_INDEX && len(node.Children) > 0 {
		return builder.baseScanForIndexAccess(node.Children[0])
	}
	return nil
}

func (builder *QueryBuilder) indexHintOwnerScan(nodeID int32) *plan.Node {
	visited := make(map[int32]struct{})
	for {
		ownerID, ok := builder.indexHintOwnerByNode[nodeID]
		if !ok || ownerID == nodeID {
			if !ok {
				return nil
			}
			if ownerID < 0 || int(ownerID) >= len(builder.qry.Nodes) {
				return nil
			}
			owner := builder.qry.Nodes[ownerID]
			if owner != nil && owner.NodeType == plan.Node_TABLE_SCAN {
				return owner
			}
			return nil
		}
		if _, seen := visited[nodeID]; seen {
			return nil
		}
		visited[nodeID] = struct{}{}
		nodeID = ownerID
	}
}

func (builder *QueryBuilder) applyForcedJoinAccess(accessID int32) (int32, error) {
	scan := builder.baseScanForIndexAccess(accessID)
	if scan == nil || scan.TableDef == nil {
		return accessID, nil
	}
	// PRIMARY is represented by the base scan itself, not by TableDef.Indexes.
	primaryAllowed := false
	if !scan.IndexScanInfo.IsIndexScan {
		if hints := builder.indexHintsByScan[scan.NodeId]; hints != nil && hints.join.forceSpecified {
			primaryAllowed = indexAllowedByHintScope(PrimaryKeyName, hints.join)
		}
	}
	if accessID == scan.NodeId && primaryAllowed {
		builder.protectedScans[scan.NodeId]++
		return accessID, nil
	}
	indexes := builder.filterRegularIndexesByJoinHints(scan, scan.TableDef.Indexes)
	for _, idxDef := range indexes {
		if !usableRegularHintIndex(idxDef) {
			continue
		}
		if builder.indexAccessUsesIndex(accessID, idxDef.IndexName) {
			builder.protectedScans[scan.NodeId]++
			return accessID, nil
		}
	}
	if primaryAllowed {
		builder.protectedScans[scan.NodeId]++
		return scan.NodeId, nil
	}
	for _, idxDef := range indexes {
		if !usableRegularHintIndex(idxDef) {
			continue
		}
		forcedID, _, err := builder.buildHintedIndexBackfillJoin(idxDef, scan)
		if err != nil {
			return -1, err
		}
		if forcedID != -1 {
			builder.protectedScans[scan.NodeId]++
			return forcedID, nil
		}
	}
	return accessID, nil
}

func (builder *QueryBuilder) indexAccessUsesIndex(nodeID int32, indexName string) bool {
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return false
	}
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return false
	}
	if node.NodeType == plan.Node_TABLE_SCAN {
		return node.IndexScanInfo.IsIndexScan && IndexNamesEqual(node.IndexScanInfo.IndexName, indexName)
	}
	if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_INDEX {
		return false
	}
	for _, childID := range node.Children {
		if builder.indexAccessUsesIndex(childID, indexName) {
			return true
		}
	}
	return false
}
