// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// encodedRegularIndexEstimate is private, attempt-local cost input. It neither
// replaces an executable expression nor changes the bound plan's statistics.
type encodedRegularIndexEstimate struct {
	blockInput              bool
	filter                  int32
	selectivity, outputRows float64
	fixedWork               float64
}

func (e encodedRegularIndexEstimate) valid(rows float64) bool {
	return finitePositive(rows) && finiteNonnegative(e.selectivity) && e.selectivity <= 1 &&
		finiteNonnegative(e.outputRows) && e.outputRows <= rows &&
		finiteNonnegative(e.fixedWork)
}

func finiteNonnegative(v float64) bool {
	return v >= 0 && !math.IsInf(v, 0) && !math.IsNaN(v)
}

// estimateIndexRangeUnion is a bounded uniform/NDV cost approximation, never
// an executable filter. Exact literal rays and inverse necessary arms share it.
func estimateIndexRangeUnion(s engine.IndexCostStats, ranges [][2]float64) (float64, bool) {
	if len(ranges) > 4 || !finitePositive(s.Rows) || !finitePositive(s.NDV) || float64(s.Nulls) > s.Rows ||
		math.IsNaN(s.Min) || math.IsNaN(s.Max) || math.IsInf(s.Min, 0) || math.IsInf(s.Max, 0) || s.Min > s.Max {
		return 0, false
	}
	var spans [4][2]float64
	n := 0
	for _, r := range ranges {
		if math.IsNaN(r[0]) || math.IsNaN(r[1]) || r[0] > r[1] {
			return 0, false
		}
		lo, hi := max(s.Min, r[0]), min(s.Max, r[1])
		if lo <= hi {
			spans[n] = [2]float64{lo, hi}
			n++
		}
	}
	if n == 0 {
		return 0, true
	}
	for i := 1; i < n; i++ {
		for j := i; j > 0 && spans[j][0] < spans[j-1][0]; j-- {
			spans[j], spans[j-1] = spans[j-1], spans[j]
		}
	}
	width, points := 0., 0
	lo, hi := spans[0][0], spans[0][1]
	finish := func() {
		width += hi - lo
		if hi == lo {
			points++
		}
	}
	for _, span := range spans[1:n] {
		if span[0] <= hi {
			hi = max(hi, span[1])
		} else {
			finish()
			lo, hi = span[0], span[1]
		}
	}
	finish()
	nonnull := (s.Rows - float64(s.Nulls)) / s.Rows
	if s.Min == s.Max {
		return nonnull, true
	}
	ndv := min(s.NDV, max(1, s.Rows-float64(s.Nulls)))
	fraction := max(1/ndv, width/(s.Max-s.Min)+float64(points)/ndv)
	return min(1, fraction) * nonnull, true
}

// This is the ordinary block-granular input estimate, not a physical bound.
// Row residual and output work continue to use their matching-row estimate.
func indexBlockInputRows(rows, candidateRows float64) float64 {
	return min(rows, max(candidateRows, float64(estimatedBlocks(candidateRows/float64(objectio.BlockMaxRows)))*float64(objectio.BlockMaxRows)))
}

type indexCostScalar struct {
	typ   types.Type
	value []byte
	null  bool
}

func spendIndexCostWork(ctx context.Context, budget *int, work int) bool {
	if ctx == nil || budget == nil || work < 0 || *budget < work {
		if budget != nil {
			*budget = 0
		}
		return false
	}
	before := *budget
	*budget -= work
	if before>>6 != *budget>>6 && context.Cause(ctx) != nil {
		*budget = 0
		return false
	}
	return true
}

// Bound input payloads before invoking the scalar evaluator, which otherwise
// could allocate a large vector before an optional observation declines it.
func indexCostLiteralFits(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	literal := expr.GetLit()
	if literal == nil || literal.Src != nil {
		return false
	}
	if types.T(expr.Typ.Id) == types.T_json || types.T(expr.Typ.Id).IsArrayRelate() {
		return false
	}
	return len(literal.GetSval()) <= 4096 && len(literal.GetJsonval()) <= 4096 && len(literal.GetVecVal()) <= 4096
}

// The caller proves eager evaluation safe before this bounded copy. Warnings
// decline only the optional cost observation; real resource/stop errors retain
// the planner's established error ownership.
func evaluateIndexCostScalar(ctx context.Context, proc *process.Process, expr *plan.Expr, budget *int) (indexCostScalar, bool, error) {
	if ctx == nil || proc == nil || expr == nil || !spendIndexCostWork(ctx, budget, 1) {
		return indexCostScalar{}, false, indexCostStop(ctx)
	}
	if err := context.Cause(ctx); err != nil {
		return indexCostScalar{}, false, err
	}
	if expr.GetLit() != nil && !indexCostLiteralFits(expr) {
		return indexCostScalar{}, false, nil
	}
	probe := proc.NewNoContextChildProc(0)
	probe.Ctx = ctx
	vec, free, warned, err := rule.EvaluateConstantExpression(probe, DeepCopyExpr(expr), batch.EmptyForConstFoldBatch)
	if free != nil {
		defer free()
	}
	if stop := context.Cause(ctx); stop != nil {
		return indexCostScalar{}, false, stop
	}
	if err != nil {
		return indexCostScalar{}, false, indexCostEvaluationError(err)
	}
	if warned || vec == nil || vec.Length() != 1 {
		return indexCostScalar{}, false, nil
	}
	scalar := indexCostScalar{typ: *vec.GetType(), null: vec.IsConstNull() || vec.IsNull(0)}
	if scalar.null {
		return scalar, true, nil
	}
	value := vec.GetRawBytesAt(0)
	if len(value) > 4096 || !spendIndexCostWork(ctx, budget, len(value)) {
		return indexCostScalar{}, false, nil
	}
	scalar.value = append([]byte(nil), value...)
	return scalar, true, nil
}

func indexCostStop(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return context.Cause(ctx)
}

func indexCostEvaluationError(err error) error {
	if isStatementConversionError(err) {
		return nil
	}
	return err
}

// Only the normal late index pass follows the last scan-statistics refresh.
// Its later passes do not revisit leaves, but may add compound-key filters;
// compilation can also complete parameter-diagnostic block filters. Exclude
// both sources before treating the current block list as a complete selector.
// The caller separately proves the full unary reader shape and original owner.
func indexBaseBlockFiltersFinal(ctx context.Context, scan *plan.Node, lateBaseStatsFinalized bool, budget *int) bool {
	if !lateBaseStatsFinalized || scan == nil || scan.NodeType != plan.Node_TABLE_SCAN || scan.TableDef == nil || scan.TableDef.Pkey == nil ||
		len(scan.FilterList) == 0 || len(scan.FilterList) > 64 || scan.TableDef.Pkey.PkeyColName == "" ||
		len(scan.TableDef.Pkey.Names) > 1 || scan.TableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName ||
		(scan.TableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(scan.TableDef.ClusterBy.Name)) {
		return false
	}
	var nodes int
	for _, expr := range scan.FilterList {
		if !indexCostExpressionFits(ctx, expr, 0, &nodes, budget) {
			return false
		}
	}
	if !spendIndexCostWork(ctx, budget, nodes) {
		return false
	}
	for _, expr := range scan.FilterList {
		if containsStatementParameterDiagnostic(expr) {
			return false
		}
	}
	return true
}

// This proof is local to ordinary index costing. It does not change SQL or the
// existing candidate correctness/admission rules. The supported reader fully
// drains a normal persisted scan and has no pre-materialization row selector.
// Later composite-key filters cannot strengthen this subset: none of their
// key parts is filtered and no physical column occurs in two predicate leaves.
func indexBaseReaderShapeSafe(ctx context.Context, query *plan.Query, scanID int32, budget *int) bool {
	if ctx == nil || query == nil || len(query.Nodes) > 64 || scanID < 0 || int(scanID) >= len(query.Nodes) || query.StmtType != plan.Query_SELECT || len(query.Steps) != 1 || len(query.Params) != 0 {
		return false
	}
	scan := query.Nodes[scanID]
	if scan == nil || scan.TableDef == nil || len(scan.TableDef.Cols) > engine.MaxIndexPlanningColumns || len(scan.ProjectList) != 0 || len(scan.FilterList) == 0 || len(scan.FilterList) > 64 || len(scan.BlockFilterList) > 64 || scan.ExtraOptions == PreparedBlockFilterDisabledScanOption {
		return false
	}
	// Full ancestor validation precedes any clone or optional scalar evaluation.
	for id, steps := query.Steps[0], 0; ; steps++ {
		if steps >= 64 || !spendIndexCostWork(ctx, budget, 1) {
			return false
		}
		if id < 0 || int(id) >= len(query.Nodes) || query.Nodes[id] == nil {
			return false
		}
		node := query.Nodes[id]
		if node.Limit != nil || node.Offset != nil || node.RankOption != nil || len(node.OrderBy) != 0 {
			return false
		}
		if node.IndexReaderParam != nil || len(node.RuntimeFilterProbeList) != 0 || len(node.RuntimeFilterBuildList) != 0 || len(node.SendMsgList) != 0 || len(node.RecvMsgList) != 0 || node.SampleFunc != nil || node.Fuzzymessage != nil || node.IndexSearchScan != nil {
			return false
		}
		if id == scanID {
			break
		}
		if len(node.Children) != 1 || (node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_FILTER) || len(node.ProjectList) > 64 || len(node.FilterList) != 0 || len(node.BlockFilterList) != 0 {
			return false
		}
		for _, expr := range node.ProjectList {
			if expr == nil || (expr.GetCol() == nil && expr.GetLit() == nil) || !spendIndexCostWork(ctx, budget, 1) {
				return false
			}
		}
		id = node.Children[0]
	}
	var expressionNodes int
	for _, list := range [][]*plan.Expr{scan.FilterList, scan.BlockFilterList} {
		for _, expr := range list {
			if !indexCostExpressionFits(ctx, expr, 0, &expressionNodes, budget) {
				return false
			}
		}
	}
	if !indexCandidatePathSafe(query, scanID) {
		return false
	}
	forbidden := make(map[string]bool)
	if pk := scan.TableDef.Pkey; pk != nil {
		if len(pk.Names) > engine.MaxIndexPlanningParts || len(pk.PkeyColName) > engine.MaxIndexPlanningNameBytes {
			return false
		}
		forbidden[pk.PkeyColName] = true
		for _, name := range pk.Names {
			if len(name) > engine.MaxIndexPlanningNameBytes {
				return false
			}
			forbidden[name] = true
		}
	}
	if cb := scan.TableDef.ClusterBy; cb != nil {
		if len(cb.Name) > engine.MaxIndexPlanningNameBytes {
			return false
		}
		forbidden[cb.Name] = true
		if util.JudgeIsCompositeClusterByColumn(cb.Name) {
			// The shared decoder assumes a well-formed catalog name. Check its
			// length-prefixed envelope before optional costing invokes it.
			for at := len(catalog.PrefixCBColName); at < len(cb.Name); {
				if len(cb.Name)-at < 3 {
					return false
				}
				n, err := strconv.Atoi(cb.Name[at : at+3])
				if err != nil || n <= 0 || n > len(cb.Name)-at-3 {
					return false
				}
				at += 3 + n
			}
			for _, name := range util.SplitCompositeClusterByColumnName(cb.Name) {
				forbidden[name] = true
			}
		}
	}
	var used [maxIndexFilterColumns]uint32
	usedCount, nodes := 0, 0
	var visit func(*plan.Expr, int) bool
	visit = func(expr *plan.Expr, depth int) bool {
		nodes++
		if nodes > 64 || depth > 64 || expr == nil || !spendIndexCostWork(ctx, budget, 1) {
			return false
		}
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
			return false
		}
		if fn.Func.ObjName == "and" || fn.Func.ObjName == "or" {
			return visit(fn.Args[0], depth+1) && visit(fn.Args[1], depth+1)
		}
		switch fn.Func.ObjName {
		case "=", "<", "<=", ">", ">=":
		default:
			return false
		}
		column, bound := fn.Args[0], fn.Args[1]
		if column == nil || bound == nil {
			return false
		}
		if column.GetCol() == nil {
			column, bound = bound, column
		}
		ref := column.GetCol()
		if ref == nil || len(scan.BindingTags) != 1 || ref.RelPos != scan.BindingTags[0] || ref.ColPos < 0 || int(ref.ColPos) >= len(scan.TableDef.Cols) || !indexBoundEvaluationSafe(bound) {
			return false
		}
		def := scan.TableDef.Cols[ref.ColPos]
		if def == nil || forbidden[def.Name] || def.Primary || def.ClusterBy || types.IsUnicodeCollation(uint8(def.Typ.Charset)) || def.Typ.Id != column.Typ.Id || def.Typ.Scale != column.Typ.Scale {
			return false
		}
		for _, seq := range used[:usedCount] {
			if seq == def.Seqnum {
				return false
			}
		}
		if usedCount == maxIndexFilterColumns {
			return false
		}
		used[usedCount] = def.Seqnum
		usedCount++
		return true
	}
	for _, expr := range scan.FilterList {
		if !visit(expr, 0) {
			return false
		}
	}
	// calcScanStats copies filters and changes their statistics. Compare every
	// other field, including original source/provenance, collation and markers.
	for _, block := range scan.BlockFilterList {
		matched := false
		for _, filter := range scan.FilterList {
			if indexCostFiltersEqual(ctx, block, filter, budget) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return budget != nil && *budget > 0
}

func indexCostFiltersEqual(ctx context.Context, a, b *plan.Expr, budget *int) bool {
	// Positive shape validation bounds the scan tree, but an extra block tree
	// has not been proved yet. Bound both trees before comparing either one.
	var ca, cb int
	if !indexCostExpressionFits(ctx, a, 0, &ca, budget) || !indexCostExpressionFits(ctx, b, 0, &cb, budget) {
		return false
	}
	bytes := a.ProtoSize() + b.ProtoSize()
	if bytes > 32768 || !spendIndexCostWork(ctx, budget, bytes) {
		return false
	}

	var equal func(*plan.Expr, *plan.Expr) bool
	equal = func(a, b *plan.Expr) bool {
		x, y := *a, *b
		x.Selectivity, x.Ndv, y.Selectivity, y.Ndv = 0, 0, 0, 0
		fa, fb := a.GetF(), b.GetF()
		if fa == nil || fb == nil {
			return proto.Equal(&x, &y)
		}
		if len(fa.Args) != len(fb.Args) {
			return false
		}
		fx, fy := *fa, *fb
		fx.Args, fy.Args = nil, nil
		x.Expr, y.Expr = &plan.Expr_F{F: &fx}, &plan.Expr_F{F: &fy}
		if !proto.Equal(&x, &y) {
			return false
		}
		for i := range fa.Args {
			if !equal(fa.Args[i], fb.Args[i]) {
				return false
			}
		}
		return true
	}
	return equal(a, b)
}

// Only scalar leaves and functions are traversable here. Opaque vectors,
// folds, window specs and dependent trees can retain large or recursive
// payloads outside Function.Args and must be rejected before comparison.
func indexCostExpressionFits(ctx context.Context, expr *plan.Expr, depth int, count, budget *int) bool {
	*count++
	if expr == nil || depth > 64 || *count > 64 || expr.PreparedNumeric != nil || !spendIndexCostWork(ctx, budget, 1) {
		return false
	}
	switch value := expr.Expr.(type) {
	case *plan.Expr_Col, *plan.Expr_T:
	case *plan.Expr_Lit:
		if !indexCostLiteralFits(expr) {
			return false
		}
	case *plan.Expr_F:
		fn := value.F
		if fn == nil || fn.Func == nil || len(fn.Args) > 64 || len(fn.AggConfig) != 0 || fn.Func.PubInfo != nil || fn.Func.Snapshot != nil {
			return false
		}
		for _, arg := range fn.Args {
			if !indexCostExpressionFits(ctx, arg, depth+1, count, budget) {
				return false
			}
		}
	default:
		return false
	}
	// Size calculation is now over a bounded positive tree; it reads lengths
	// rather than copying retained strings/unknown fields.
	return expr.ProtoSize() <= 16384
}

const maxIndexFilterColumns = 4

// indexCostObservation is synchronous evidence owned by the ordinary selector.
// A complete observation can recost an exact or necessary covering comparison;
// it grants no authority over rows or catalog publication.
type indexCostObservation struct {
	provider   engine.IndexPlanningRelation
	full       *plan.TableDef
	borrow     *BorrowedTable
	scan       *plan.Node
	comparison indexComparison
	estimate   encodedRegularIndexEstimate
	baseWork   float64
	column     string
	budget     int
}

func (builder *QueryBuilder) indexObservationAllowed(scanID int32, finalized bool) bool {
	original := builder.originalIndexScan
	input := builder.planningInput
	if !finalized || !original.safe || original.scanID != scanID || builder.isReusablePlan() || builder.sessionSelectLimitMayStopEarly ||
		input == nil || !ProcessAllowsIndexObservation(builder.compCtx.GetProcess()) || !input.intent.Placement.AllowsPinnedLocalAccess() || GetForceScanOnMultiCN() {
		return false
	}
	return true
}

// observeIndexCost is called only after matching has found a covering index.
// Every input is rechecked after ordinary resolver reentry and after observation.
func (builder *QueryBuilder) observeIndexCost(scanID int32, cost *encodedRegularIndexCostContext) (access *indexCostObservation, err error) {
	ctx, proc := builder.GetContext(), builder.compCtx.GetProcess()
	defer func() {
		if cause := context.Cause(ctx); cause != nil {
			access, err = nil, cause
		}
	}()
	resolver := builder.borrowingCompiler()
	if proc == nil || resolver == nil || cost == nil || !cost.valid || len(cost.node.TableDef.Indexes) > engine.MaxIndexPlanningDefinitions ||
		!builder.planningInput.intent.Placement.AllowsLocalIndexPlacement() || GetForceScanOnMultiCN() {
		return nil, nil
	}
	budget := engine.MaxIndexPlanningWork
	if !indexBaseReaderShapeSafe(ctx, builder.qry, scanID, &budget) || builder.optimizerHints != nil && builder.optimizerHints.blockFilter == 2 {
		return nil, nil
	}
	scan := builder.qry.Nodes[scanID]
	if scan != cost.node || len(scan.FilterList) != 1 {
		return nil, nil
	}
	atom, ok := analyzeIndexComparison(ctx, scan.FilterList[0])
	if !ok || atom.column.GetCol().ColPos < 0 || int(atom.column.GetCol().ColPos) >= len(scan.TableDef.Cols) {
		return nil, nil
	}
	if !atom.observationMatches(builder.planningInput.intent, proc) ||
		atom.domain != indexComparisonInverse && (!makeTypeByPlan2Expr(atom.column).IsIntOrUint() || atom.domain != indexComparisonRaw || atom.bound.GetLit() == nil) {
		return nil, nil
	}
	column := scan.TableDef.Cols[atom.column.GetCol().ColPos]
	full, borrow, ok := builder.scanBorrow(scanID)
	if column == nil || !ok || !resolver.BorrowedTableStillCurrent(proc, borrow) {
		return nil, nil
	}
	provider, ok := borrow.Relation.(engine.IndexPlanningRelation)
	if !ok || borrow.Read.Op != proc.GetTxnOperator() {
		return nil, nil
	}
	stats := builder.getStatsInfoByTableID(scan.TableDef.TblId)
	if stats == nil || stats.GetStats() == nil {
		return nil, nil
	}
	observation, ok, err := provider.ReadIndexCostStats(ctx, borrow.Read, stats.GetStats(), column.Name)
	if err != nil || !ok || observation.Rows != cost.statsTableCnt || observation.Rows != scan.Stats.TableCnt {
		return nil, err
	}
	scalar, ok, err := evaluateIndexCostScalar(ctx, proc, atom.bound, &budget)
	if err != nil || !ok || scalar.null || scalar.typ.Oid != types.T(atom.bound.Typ.Id) || scalar.typ.Scale != atom.bound.Typ.Scale {
		return nil, err
	}
	selectivity, ok, err := indexComparisonSelectivity(ctx, observation, atom, scalar, proc.GetSessionInfo().TimeZone)
	if err != nil || !ok || !finitePositive(selectivity) || selectivity > 1 || len(cost.unknownRangeLowerSelectivities) != 0 ||
		!indexBaseBlockFiltersFinal(ctx, scan, true, &budget) {
		return nil, err
	}
	baseRows, readers := observation.Rows, 1.0
	if len(scan.BlockFilterList) != 0 {
		if len(scan.BlockFilterList) != 1 || observation.Blocks <= 0 {
			return nil, nil
		}
		nodes := 0
		if !indexCostExpressionFits(ctx, scan.FilterList[0], 0, &nodes, &budget) || !spendIndexCostWork(ctx, &budget, 8*nodes) ||
			!ScanStorageFilterEligible(proc, scan.BlockFilterList[0], false) {
			return nil, nil
		}
		request := engine.IndexBlockInputRequest{Column: column.Name, Observed: stats.GetStats(), Stats: observation,
			Bound: scalar.value, BoundType: scalar.typ, ComparisonScale: atom.scale, Op: atom.op, Zone: proc.GetSessionInfo().TimeZone}
		input, complete, cause := provider.ObserveIndexBlockInput(ctx, borrow.Read, request, &budget)
		if cause != nil || !complete || input.Rows == 0 || input.Blocks == 0 {
			return nil, cause
		}
		baseRows = float64(input.Rows)
		classify := spendIndexCostWork(ctx, &budget, 4*len(builder.qry.Nodes))
		for _, node := range builder.qry.Nodes {
			if node == nil || len(node.Children) > 1 || (node.NodeType != plan.Node_TABLE_SCAN && node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_FILTER) {
				classify = false
				break
			}
		}
		if classify && GetExecType(builder.qry, false, builder.isPrepareStatement) != ExecTypeTP {
			readers = min(cost.localScanReaders(), float64(input.Blocks))
		}
	}
	baseWork := baseRows * (cost.baseWidth + float64(len(scan.FilterList))*regularIndexPredicateRowWork) / readers
	if !finitePositive(baseWork) || budget <= 0 || builder.qry.Nodes[scanID] != scan || builder.borrowingCompiler() != resolver ||
		!resolver.BorrowedTableStillCurrent(proc, borrow) || !atom.observationMatches(builder.planningInput.intent, proc) {
		return nil, nil
	}
	return &indexCostObservation{provider: provider, full: full, borrow: borrow, scan: scan, comparison: atom, column: column.Name, budget: budget, baseWork: baseWork,
		estimate: encodedRegularIndexEstimate{blockInput: true, filter: 0, selectivity: selectivity, outputRows: selectivity * observation.Rows}}, nil
}

func indexComparisonSelectivity(ctx context.Context, stats engine.IndexCostStats, atom indexComparison, scalar indexCostScalar, zone *time.Location) (float64, bool, error) {
	if stats.Kind != types.T(atom.column.Typ.Id) {
		return 0, false, nil
	}
	var spans [4][2]float64
	count, ok, err := atom.costRanges(ctx, scalar, zone, &spans)
	if err != nil || !ok {
		return 0, false, err
	}
	sel, ok := estimateIndexRangeUnion(stats, spans[:count])
	return sel, ok, nil
}

// Cost the necessary predicate's actual runtime topology without substituting
// the observed scalar into the executable expression.
func necessaryIndexPredicateWork(ctx context.Context, index *plan.IndexDef, access *indexCostObservation) (float64, bool, error) {
	atom := access.comparison
	if !atom.prepayNecessaryPredicate(ctx, &access.budget) {
		return 0, false, context.Cause(ctx)
	}
	var physical *plan.Expr
	if indexTableStoresSerializedKey(index) {
		physical = GetColExpr(plan.Type{Id: int32(types.T_varchar)}, 0, 0)
	}
	hint, err := atom.necessaryPredicate(ctx, atom.column, physical)
	if err != nil || hint == nil {
		return 0, false, err
	}
	nodes, _, ok := indexPredicateStructure(hint, &access.budget, ctx)
	return float64(nodes) * regularIndexPredicateRowWork, ok, context.Cause(ctx)
}

func indexPredicateStructure(root *plan.Expr, budget *int, ctx context.Context) (int, int, bool) {
	nodes, predicates := 0, 0
	var visit func(*plan.Expr, int) bool
	visit = func(e *plan.Expr, depth int) bool {
		if e == nil || depth > 64 || !spendIndexCostWork(ctx, budget, 1) {
			return false
		}
		nodes++
		if f := e.GetF(); f != nil {
			if types.T(e.Typ.Id) == types.T_bool {
				predicates++
			}
			for _, arg := range f.Args {
				if !visit(arg, depth+1) {
					return false
				}
			}
		}
		return e.GetList() == nil && e.GetSub() == nil
	}
	ok := visit(root, 0)
	return nodes, predicates, ok
}
