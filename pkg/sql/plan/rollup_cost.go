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
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	containertypes "github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// The ROLLUP alternatives have different work shapes:
//
//	hash: (number of prefixes + grand total) independent scans and hash
//	      aggregates. The branches can overlap in wall time, but they still
//	      consume CPU, IO, and memory together.
//	sort: one scan, one global sort, and one streaming aggregate.
//
// These coefficients are deliberately dimensionless. ScanCost comes from the
// existing table statistics, while the CPU terms are stable relative weights.
// They are kept in one place so calibration from benchmark data does not leak
// into planner rules. The selector is conservative: unknown statistics never
// select sort. The session-level rollup_algorithm variable provides COST,
// SORT, and HASH modes; the old rollupSort optimizer hint remains a
// compatibility fallback for existing deployments.
const (
	rollupHashKeyCost           = 0.22
	rollupHashAggCost           = 0.12
	rollupSortCompareCost       = 0.15
	rollupSortBoundaryCost      = 0.02
	rollupSortAggCost           = 0.12
	rollupSortStartupCost       = 16.0
	rollupHashBranchStartupCost = 100.0
	rollupHashWorkWeight        = 0.70
	rollupHashLatencyWeight     = 0.30
	rollupSpillCostWeight       = 0.35
	rollupMaxKeyFactor          = 32.0
	rollupMaxRowWidth           = 1 << 20
	rollupSortSelectFactor      = 0.80
)

const rollupAlgorithmVariable = "rollup_algorithm"

type rollupAlgorithm uint8

const (
	rollupAlgorithmCost rollupAlgorithm = iota
	rollupAlgorithmSort
	rollupAlgorithmHash
)

func resolveRollupAlgorithm(ctx CompilerContext) (rollupAlgorithm, bool) {
	if ctx == nil {
		return rollupAlgorithmCost, false
	}
	value, err := ctx.ResolveVariable(rollupAlgorithmVariable, true, false)
	if err != nil {
		return rollupAlgorithmCost, false
	}
	mode, ok := value.(string)
	if !ok {
		return rollupAlgorithmCost, false
	}
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "COST":
		return rollupAlgorithmCost, true
	case "SORT":
		return rollupAlgorithmSort, true
	case "HASH":
		return rollupAlgorithmHash, true
	default:
		return rollupAlgorithmCost, false
	}
}

func (builder *QueryBuilder) legacyRollupAlgorithmMode() rollupAlgorithm {
	if builder == nil || builder.optimizerHints == nil {
		return rollupAlgorithmCost
	}
	switch builder.optimizerHints.rollupSort {
	case 1:
		return rollupAlgorithmSort
	case 2:
		return rollupAlgorithmHash
	default:
		return rollupAlgorithmCost
	}
}

// rollupAlgorithmMode mirrors window_partition_algorithm. An explicit session
// value wins; when the new variable is unavailable in an older compiler
// context, or remains COST, the legacy optimizer hint is honored for a
// compatibility transition.
func (builder *QueryBuilder) rollupAlgorithmMode() rollupAlgorithm {
	if builder == nil {
		return rollupAlgorithmCost
	}
	mode, resolved := resolveRollupAlgorithm(builder.compCtx)
	legacy := builder.legacyRollupAlgorithmMode()
	if !resolved || mode == rollupAlgorithmCost && legacy != rollupAlgorithmCost {
		return legacy
	}
	return mode
}

type sortRollupCostEstimate struct {
	Rows                float64
	ScanCost            float64
	HashCost            float64
	SortCost            float64
	HashMemory          float64
	SortMemory          float64
	SortAggMemory       float64
	SortOutputMemory    float64
	SortGroupUpperBound float64
	SortFeasible        bool
	BranchCount         int
	HashWork            float64
	SortWork            float64
	PrefixGroups        float64
	KeyWidth            float64
	AggregateCost       float64
	OrderedInput        bool
}

// chooseSortRollup is the only automatic selector. Physical eligibility is
// checked by the caller before this function, so this function is concerned
// only with uncertainty and relative cost.
func (builder *QueryBuilder) chooseSortRollup(
	ctx *BindContext,
	from tree.TableExprs,
	where *tree.Where,
	groupingExprs tree.Exprs,
	selectExprs tree.SelectExprs,
	having *tree.Where,
	orderBy tree.OrderBy,
	isRoot bool,
) bool {
	switch builder.rollupAlgorithmMode() {
	case rollupAlgorithmSort:
		// Force-sort mode is intentionally independent of statistics. It keeps
		// the existing experiment/benchmark switch useful on an unanalyzed table.
		return true
	case rollupAlgorithmHash:
		// Force-hash mode is useful as a stable baseline for comparisons.
		return false
	}

	probe, ok := builder.probeSortRollupInput(
		ctx, from, where, groupingExprs, isRoot)
	if !ok {
		return false
	}
	extraAggregateExprs := sortRollupAdditionalExprs(having, orderBy)
	estimate, ok := estimateSortRollupCost(probe, selectExprs, extraAggregateExprs...)
	if !ok || !estimate.SortFeasible {
		return false
	}
	return estimate.SortCost < estimate.HashCost*rollupSortSelectFactor
}

func estimateSortRollupCost(
	probe *sortRollupProbe,
	selectExprs tree.SelectExprs,
	extraAggregateExprs ...tree.Expr,
) (sortRollupCostEstimate, bool) {
	var estimate sortRollupCostEstimate
	if probe == nil || probe.source == nil || probe.source.Stats == nil ||
		probe.builder == nil || len(probe.groupExprs) == 0 {
		return estimate, false
	}

	stats := probe.source.Stats
	// DefaultStats is the planner's "we do not know" sentinel. Treating it as
	// real data would make the automatic path depend on arbitrary constants.
	if IsDefaultStats(stats) || !finiteRollupStat(stats.TableCnt) ||
		!finiteRollupStat(stats.Outcnt) || !finiteRollupStat(stats.Cost) {
		return estimate, false
	}
	// A derived-table/project node may retain the planner's generic table-count
	// default while carrying the real child output count. Its output is the
	// input to this ROLLUP, so do not reject that wrapper merely because
	// Outcnt > TableCnt. Keep the consistency check strict for leaf scans, where
	// TableCnt is a table cardinality invariant rather than a wrapper artifact.
	tableRows := stats.TableCnt
	leafScan := probe.source.NodeType == plan.Node_TABLE_SCAN ||
		probe.source.NodeType == plan.Node_EXTERNAL_SCAN
	if leafScan {
		if tableRows > 0 && stats.Outcnt > tableRows {
			// A filtered scan cannot produce more rows than its table-wide input.
			// Inconsistent statistics are not a reason to risk changing the plan.
			return estimate, false
		}
		if tableRows == 0 && stats.Outcnt > 0 {
			return estimate, false
		}
	} else if stats.Outcnt > tableRows {
		tableRows = stats.Outcnt
	}

	rows := math.Max(0, stats.Outcnt)
	rowsForCPU := math.Max(1, rows)
	scanCost := math.Max(1, stats.Cost)
	rowWidth := stats.Rowsize
	if (rowWidth <= 0 || math.IsNaN(rowWidth) || math.IsInf(rowWidth, 0)) &&
		probe.tableDef != nil {
		rowWidth = GetRowSizeFromTableDef(probe.tableDef, true) * 0.8
	}
	if rowWidth <= 0 || math.IsNaN(rowWidth) || math.IsInf(rowWidth, 0) {
		rowWidth = 64
	}
	rowWidth = math.Min(rowWidth, rollupMaxRowWidth)

	keyWidth := 0.0
	for _, expr := range probe.groupExprs {
		keyWidth += float64(getRowCarrierCost(expr, false).width)
	}
	if keyWidth < 1 || math.IsNaN(keyWidth) || math.IsInf(keyWidth, 0) {
		keyWidth = 1
	}
	keyFactor := math.Min(rollupMaxKeyFactor, math.Max(1, keyWidth/8))
	aggregateCost := sortRollupAggregateCost(selectExprs, extraAggregateExprs...)
	aggregateStateBytes, aggregateStateBounded := sortRollupAggregateStateBytes(
		probe, selectExprs, extraAggregateExprs...)
	levels := len(probe.groupExprs)
	branches := levels + 1
	if branches < 2 {
		return estimate, false
	}

	prefixGroups, maxPrefixGroups, ok := estimateRollupPrefixGroupStats(probe)
	if !ok {
		return estimate, false
	}
	// Hash aggregate state includes the equality key, aggregate state, and
	// allocator/hash-table overhead. This is intentionally an upper estimate;
	// it matters only when a spill limit is configured.
	hashStateBytes := math.Max(64, keyWidth+32+aggregateStateBytes)
	hashMemory := math.Max(1, prefixGroups*hashStateBytes)
	sortMemory := math.Max(1, rowsForCPU*(rowWidth+keyWidth))
	// Keep the theoretical output-group bound for diagnostics and overflow
	// detection. It is not the resident aggregate-state bound: the executor is
	// streaming even when it must insert an internal SORT before this node.
	capacityRows := math.Max(rows, tableRows)
	sortGroupUpperBound := capacityRows*float64(levels) + 1
	// groupIDs is one reusable UnitLimit-sized slice shared by all active
	// prefixes. The aggregate preflight scratch is stack-bounded by the same
	// unit and is not charged to the group mpool, so include this fixed heap
	// allocation explicitly in the admission estimate.
	scratchBytes := float64(hashmap.UnitLimit * bytesPerRollupGroupID)
	sortOutputMemory, sortOutputBounded := sortRollupOutputBatchMemory(
		probe, selectExprs, extraAggregateExprs...)
	sortAggMemory := math.Max(1,
		float64(branches)*hashStateBytes+scratchBytes+sortOutputMemory)
	if probe.orderedInput {
		// Streaming ROLLUP reuses an existing global order. Its resident
		// working set is the active prefix states and a bounded output batch,
		// not all input rows or all emitted groups.
		sortMemory = math.Max(1, float64(branches)*(rowWidth+keyWidth))
	}
	aggSpillMem := rollupEffectiveSpillLimit(probe.builder.aggSpillMem)
	sortSpillMem := rollupEffectiveSpillLimit(probe.builder.sortSpillMem)

	prefixKeyFactor := estimateRollupPrefixKeyFactor(probe.groupExprs)
	hashWork := float64(branches)*(scanCost+rollupHashBranchStartupCost) +
		rowsForCPU*(rollupHashKeyCost*prefixKeyFactor+
			rollupHashAggCost*aggregateCost*float64(branches))
	parallelism := float64(system.GoMaxProcs())
	if parallelism < 1 {
		parallelism = 1
	}
	if maxDop := probe.builder.qry.MaxDop; maxDop > 0 && float64(maxDop) < parallelism {
		parallelism = float64(maxDop)
	}
	if float64(branches) < parallelism {
		parallelism = float64(branches)
	}
	hashLatencyWork := scanCost + rollupHashBranchStartupCost +
		rowsForCPU*(rollupHashKeyCost*keyFactor+
			rollupHashAggCost*aggregateCost)
	hashLatency := math.Max(hashLatencyWork, hashWork/parallelism)
	hashCost := hashWork*rollupHashWorkWeight + hashLatency*rollupHashLatencyWeight

	logRows := math.Log2(rowsForCPU + 1)
	sortWork := scanCost +
		rowsForCPU*logRows*rollupSortCompareCost*keyFactor +
		rowsForCPU*rollupSortBoundaryCost*float64(levels) +
		rowsForCPU*rollupSortAggCost*aggregateCost*float64(branches) +
		rollupSortStartupCost
	if probe.orderedInput {
		// The comparison term belongs to the already executed producer. The
		// consumer still compares adjacent keys to find boundaries, but that is
		// linear and much cheaper than a second global sort.
		sortWork = scanCost +
			rowsForCPU*rollupSortBoundaryCost*float64(levels) +
			rowsForCPU*rollupSortAggCost*aggregateCost*float64(branches)
	}
	sortCost := sortWork
	sortFeasible := aggregateStateBounded && sortOutputBounded
	if aggSpillMem > 0 {
		// Both paths use the same streaming aggregate. The unordered path
		// additionally accounts for its independent sort workspace below.
		sortFeasible = sortFeasible && sortAggMemory < float64(aggSpillMem)
	}

	if aggSpillMem > 0 {
		if aggSpillMem < 10000 {
			hashCost += rollupSpillPenalty(hashCost, maxPrefixGroups, float64(aggSpillMem))
		} else {
			hashCost += rollupSpillPenalty(hashCost, hashMemory, float64(aggSpillMem))
		}
	}
	if sortSpillMem > 0 && !probe.orderedInput {
		// mergeorder always interprets its spill threshold as bytes. The
		// historical small-value group-count convention belongs only to hash
		// aggregation and must not be applied to the sort workspace.
		sortCost += rollupSpillPenalty(sortCost, sortMemory, float64(sortSpillMem))
	}
	if !finiteRollupCost(hashMemory) || !finiteRollupCost(sortMemory) ||
		!finiteRollupCost(sortAggMemory) || !finiteRollupCost(sortGroupUpperBound) ||
		!finiteRollupCost(hashCost) || !finiteRollupCost(sortCost) ||
		!finiteRollupCost(hashWork) || !finiteRollupCost(sortWork) {
		return estimate, false
	}

	estimate = sortRollupCostEstimate{
		Rows:                rows,
		ScanCost:            scanCost,
		HashCost:            hashCost,
		SortCost:            sortCost,
		HashMemory:          hashMemory,
		SortMemory:          sortMemory,
		SortAggMemory:       sortAggMemory,
		SortOutputMemory:    sortOutputMemory,
		SortGroupUpperBound: sortGroupUpperBound,
		SortFeasible:        sortFeasible,
		BranchCount:         branches,
		HashWork:            hashWork,
		SortWork:            sortWork,
		PrefixGroups:        prefixGroups,
		KeyWidth:            keyWidth,
		AggregateCost:       aggregateCost,
		OrderedInput:        probe.orderedInput,
	}
	return estimate, true
}

const (
	rollupOutputVectorOverhead = 64
	rollupOutputAggregateWidth = 32
	// hll_add_agg and its merge variants materialize a dense p=14 HLL state
	// when the aggregate result is flushed. Keep the encoded header in the
	// estimate as well as the 16 KiB register array. Underestimating this
	// vector could let COST select sort for a result batch that exceeds the
	// aggregate memory limit by more than 100 MiB.
	rollupHLLAggregateWidth = 8 + (1 << 14)
)

// sortRollupOutputBatchMemory accounts for the batch that the executor
// retains while its consumer processes a result. createNewGroupByBatch
// pre-extends every grouping vector to AggBatchSize, and aggregate result
// vectors grow to the same logical batch size as rows are appended. The
// previous model admitted only the active prefix state and scratch group IDs,
// so a small memory limit could select a plan that failed as soon as it
// materialized its first full result batch.
//
// The estimate is deliberately conservative. Fixed-width aggregate results
// use 32 bytes even when their concrete result is narrower, and both null and
// grouping bitmaps are charged for every grouping column. An unbounded key or
// aggregate returns a finite upper placeholder but marks the estimate
// infeasible; automatic selection remains fail-closed.
func sortRollupOutputBatchMemory(
	probe *sortRollupProbe,
	selectExprs tree.SelectExprs,
	extraAggregateExprs ...tree.Expr,
) (float64, bool) {
	if probe == nil {
		return math.Max(1, float64(aggexec.AggBatchSize)*rollupMaxRowWidth), false
	}

	rows := float64(aggexec.AggBatchSize)
	bitmapBytes := float64((aggexec.AggBatchSize + 7) / 8)
	bytes := 0.0
	bounded := true
	addVector := func(width float64, grouping bool) {
		if width <= 0 || math.IsNaN(width) || math.IsInf(width, 0) {
			width = rollupMaxRowWidth
			bounded = false
		}
		if width > rollupMaxRowWidth {
			width = rollupMaxRowWidth
			bounded = false
		}
		bytes += rows*width + bitmapBytes + rollupOutputVectorOverhead
		if grouping {
			bytes += bitmapBytes
		}
	}

	for _, expr := range probe.groupExprs {
		carrier := getRowCarrierCost(expr, false)
		if carrier.varlen {
			bounded = false
		}
		addVector(float64(carrier.width), true)
	}

	for _, selectExpr := range selectExprs {
		width, count, ok := sortRollupAggregateOutputProfile(selectExpr.Expr)
		if !ok {
			bounded = false
		}
		for range count {
			addVector(width, false)
		}
	}
	// HAVING and ORDER BY aggregates are also registered in ctx.aggregates and
	// therefore occupy result vectors even when they are not projected by the
	// SELECT list. Charge their AST expressions here so the admission check
	// covers the same hidden aggregate state as the executor.
	for _, expr := range extraAggregateExprs {
		width, count, ok := sortRollupAggregateOutputProfile(expr)
		if !ok {
			bounded = false
		}
		for range count {
			addVector(width, false)
		}
	}

	if bytes <= 0 || math.IsNaN(bytes) || math.IsInf(bytes, 0) {
		return math.Max(1, rows*rollupMaxRowWidth), false
	}
	// Vector growth and allocator rounding can exceed the logical fixed-width
	// payload. Keep admission comfortably above the exact payload estimate.
	bytes *= 1.25
	return bytes, bounded && finiteRollupCost(bytes)
}

func sortRollupAggregateOutputProfile(expr tree.Expr) (float64, int, bool) {
	width := 0.0
	count := 0
	bounded := true
	walkGroupingSetOrderByExpr(expr, func(candidate tree.Expr) bool {
		fn, ok := candidate.(*tree.FuncExpr)
		if !ok || fn.WindowSpec != nil {
			return true
		}
		name := strings.ToLower(sortRollupASTFunctionName(fn))
		if !function.GetFunctionIsAggregateByName(name) {
			return true
		}
		count++
		switch name {
		case "count", "count_if", "starcount", "avg", "sum", "bit_and", "bit_or",
			"bit_xor", "bitagg_and", "bitagg_or", "boolagg_and", "boolagg_or", "min",
			"max", "any_value", "std", "stddev", "stddev_pop", "stddev_samp", "var_pop",
			"var_samp", "variance", "corr", "covar_pop", "covar_sample", "approx_count",
			"approx_count_distinct":
			width += rollupOutputAggregateWidth
		case "hll_add_agg", "hll_add", "hll_merge":
			width += rollupHLLAggregateWidth
		default:
			bounded = false
			width += rollupMaxRowWidth
		}
		return true
	})
	if count == 0 {
		return 0, 0, bounded
	}
	return width / float64(count), count, bounded && finiteRollupCost(width)
}

func finiteRollupStat(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0
}

func finiteRollupCost(value float64) bool {
	return value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func rollupEffectiveSpillLimit(configured int64) int64 {
	if configured > 0 {
		return configured
	}
	if configured < 0 {
		// Keep the executor's explicit negative/unlimited convention intact.
		return configured
	}
	// Zero is the executor's auto setting, not unlimited memory. Use the same
	// CN-local resolution as the group and sort operators so a plan selected by
	// the model cannot exceed the runtime's materialization ceiling merely
	// because the session left the variable at its default.
	return colexec.ResolveSpillThreshold(0)
}

func rollupSpillPenalty(baseCost, bytes, limit float64) float64 {
	if baseCost <= 0 || bytes <= limit || limit <= 0 {
		return 0
	}
	ratio := math.Min(8, bytes/limit-1)
	if ratio <= 0 {
		return 0
	}
	return baseCost * rollupSpillCostWeight * ratio
}

const bytesPerRollupGroupID = 8

func estimateRollupPrefixGroupStats(probe *sortRollupProbe) (float64, float64, bool) {
	if probe == nil || probe.source == nil || probe.source.Stats == nil {
		return 0, 0, false
	}
	rows := math.Max(0, probe.source.Stats.Outcnt)
	if probe.source.Stats.Outcnt == 0 {
		return 1, 1, true
	}
	product := 1.0
	total := 0.0
	maxPrefix := 1.0
	for _, expr := range probe.groupExprs {
		if expr == nil {
			return 0, 0, false
		}
		ndv := expr.Ndv
		if ndv <= 0 || math.IsNaN(ndv) || math.IsInf(ndv, 0) {
			ndv = rollupExprNdv(probe, expr)
		}
		if ndv <= 0 || math.IsNaN(ndv) || math.IsInf(ndv, 0) {
			// NDV is needed for the hash-memory side of the comparison. A
			// guessed value can systematically choose sort for a high-NDV
			// input, so unknown NDV is a hash fallback rather than a heuristic.
			return 0, 0, false
		}
		ndv = math.Max(1, ndv)
		if product > rows/ndv {
			product = rows
		} else {
			product *= ndv
			product = math.Min(rows, product)
		}
		if !finiteRollupCost(product) {
			return 0, 0, false
		}
		maxPrefix = math.Max(maxPrefix, product)
		total += product
		if !finiteRollupCost(total) {
			return 0, 0, false
		}
	}
	if !finiteRollupCost(total + 1) {
		return 0, 0, false
	}
	return math.Max(1, total+1), maxPrefix, true // +1 is the grand-total group.
}

func rollupExprNdv(probe *sortRollupProbe, expr *Expr) float64 {
	if expr == nil {
		return -1
	}
	if expr.Ndv > 0 && !math.IsNaN(expr.Ndv) && !math.IsInf(expr.Ndv, 0) {
		return expr.Ndv
	}
	if probe != nil && probe.builder != nil {
		if ndv := getExprNdv(expr, probe.builder); ndv > 0 &&
			!math.IsNaN(ndv) && !math.IsInf(ndv, 0) {
			return ndv
		}
	}
	// The isolated probe may not have the same tag-to-table map as the real
	// bind context. For direct grouping columns, read the exact cached NDV by
	// table id as a final, still-statistics-backed lookup.
	if probe == nil || probe.builder == nil || probe.tableDef == nil {
		return -1
	}
	col := expr.GetCol()
	if col == nil || col.ColPos < 0 || int(col.ColPos) >= len(probe.tableDef.Cols) {
		return -1
	}
	name := col.Name
	if name == "" && probe.tableDef.Cols[col.ColPos] != nil {
		name = probe.tableDef.Cols[col.ColPos].Name
	}
	if name == "" {
		return -1
	}
	info := probe.builder.getStatsInfoByTableID(probe.tableDef.TblId)
	if info == nil || info.GetStats() == nil {
		return -1
	}
	ndv, ok := info.GetStats().NdvMap[name]
	if !ok || ndv <= 0 || math.IsNaN(ndv) || math.IsInf(ndv, 0) {
		return -1
	}
	return ndv
}

func estimateRollupPrefixKeyFactor(exprs []*Expr) float64 {
	if len(exprs) == 0 {
		return 1
	}
	total := 0.0
	levels := float64(len(exprs))
	for i, expr := range exprs {
		width := float64(getRowCarrierCost(expr, false).width)
		if width <= 0 || math.IsNaN(width) || math.IsInf(width, 0) {
			width = 8
		}
		// A hash branch for a shorter prefix hashes fewer keys. Include each
		// prefix once instead of charging every branch for the full key.
		total += math.Max(1, width/8) * (levels - float64(i))
	}
	return math.Max(1, total)
}

func sortRollupAdditionalExprs(having *tree.Where, orderBy tree.OrderBy) []tree.Expr {
	var exprs []tree.Expr
	if having != nil && having.Expr != nil {
		exprs = append(exprs, having.Expr)
	}
	for _, order := range orderBy {
		if order != nil && order.Expr != nil {
			exprs = append(exprs, order.Expr)
		}
	}
	return exprs
}

func sortRollupAggregateCost(selectExprs tree.SelectExprs, extraExprs ...tree.Expr) float64 {
	cost := 0.0
	for _, selectExpr := range selectExprs {
		cost += sortRollupExprAggregateCost(selectExpr.Expr)
	}
	for _, expr := range extraExprs {
		cost += sortRollupExprAggregateCost(expr)
	}
	if cost < 1 {
		return 1
	}
	return cost
}

func sortRollupExprAggregateCost(expr tree.Expr) float64 {
	cost, _, _ := sortRollupExprAggregateProfile(nil, expr)
	return cost
}

func sortRollupAggregateStateBytes(
	probe *sortRollupProbe,
	selectExprs tree.SelectExprs,
	extraExprs ...tree.Expr,
) (float64, bool) {
	stateBytes := 0.0
	bounded := true
	for _, selectExpr := range selectExprs {
		_, bytes, ok := sortRollupExprAggregateProfile(probe, selectExpr.Expr)
		stateBytes += bytes
		bounded = bounded && ok
	}
	for _, expr := range extraExprs {
		_, bytes, ok := sortRollupExprAggregateProfile(probe, expr)
		stateBytes += bytes
		bounded = bounded && ok
	}
	if stateBytes < 64 {
		stateBytes = 64
	}
	return stateBytes, bounded && finiteRollupCost(stateBytes)
}

func sortRollupExprAggregateProfile(probe *sortRollupProbe, expr tree.Expr) (float64, float64, bool) {
	cost := 0.25 // projection/filter evaluation around the aggregate, if any
	stateBytes := 0.0
	bounded := true
	foundAggregate := false
	walkGroupingSetOrderByExpr(expr, func(candidate tree.Expr) bool {
		fn, ok := candidate.(*tree.FuncExpr)
		if !ok || fn.WindowSpec != nil {
			return true
		}
		name := strings.ToLower(sortRollupASTFunctionName(fn))
		if !function.GetFunctionIsAggregateByName(name) {
			return true
		}
		foundAggregate = true
		if fn.Type == tree.FUNC_TYPE_DISTINCT {
			bounded = false
			return false
		}
		switch name {
		case "count", "count_if", "starcount":
			cost += 1
			stateBytes += 64
		case "avg", "sum", "bit_and", "bit_or", "bit_xor", "bitagg_and", "bitagg_or",
			"boolagg_and", "boolagg_or":
			cost += 2
			stateBytes += 64
		case "min", "max", "any_value":
			// A base row width is not a maximum for an expression such as
			// REPEAT or a variable-width column. Bind the arguments in the
			// isolated probe and accept only fixed-width result types.
			if probe != nil && !sortRollupFixedAggregateArgs(fn, probe) {
				bounded = false
				return false
			}
			cost += 2
			stateBytes += 64
		case "std", "stddev", "stddev_pop", "stddev_samp", "var_pop", "var_samp", "variance",
			"corr", "covar_pop", "covar_sample":
			cost += 4
			stateBytes += 128
		case "approx_count", "approx_count_distinct", "hll_add_agg", "hll_add", "hll_merge":
			cost += 5
			stateBytes += 16 * 1024
		case "group_concat", "json_arrayagg", "json_objectagg", "array_agg", "median",
			"percentile_cont", "percentile_disc", "approx_percentile", "bitmap_construct_agg",
			"bitmap_or":
			// These states can grow with input values or cardinality. The sort
			// executor materializes all groups, so a fixed slot estimate is not
			// a safe admission bound.
			bounded = false
			return false
		default:
			// Keep the automatic path fail-closed when a new aggregate is added
			// without a memory contract here. Forced sort remains a semantic
			// experiment, while auto mode retains hash compatibility.
			bounded = false
			return false
		}
		return true
	})
	if !foundAggregate {
		return 0.25, 0, true
	}
	if !finiteRollupCost(cost) || !finiteRollupCost(stateBytes) {
		return 0, math.Inf(1), false
	}
	return cost, stateBytes, bounded
}

func sortRollupFixedAggregateArgs(fn *tree.FuncExpr, probe *sortRollupProbe) bool {
	if fn == nil || probe == nil || probe.builder == nil || probe.ctx == nil || len(fn.Exprs) == 0 {
		return false
	}
	binder := NewWhereBinder(probe.builder, probe.ctx)
	for _, arg := range fn.Exprs {
		qualified, err := probe.ctx.qualifyColumnNames(cloneTreeExpr(arg), AliasAfterColumn)
		if err != nil {
			return false
		}
		bound, err := binder.BindExpr(qualified, 0, true)
		if err != nil || bound == nil || !sortRollupFixedValueType(bound.Typ) {
			return false
		}
	}
	return true
}

func sortRollupFixedValueType(typ Type) bool {
	switch containertypes.T(typ.Id) {
	case containertypes.T_char, containertypes.T_varchar, containertypes.T_text,
		containertypes.T_blob, containertypes.T_datalink, containertypes.T_json,
		containertypes.T_binary, containertypes.T_varbinary,
		containertypes.T_array_float32, containertypes.T_array_float64,
		containertypes.T_array_bf16, containertypes.T_array_float16,
		containertypes.T_array_int8, containertypes.T_array_uint8,
		containertypes.T_geometry, containertypes.T_geometry32:
		return false
	case containertypes.T_bool, containertypes.T_bit,
		containertypes.T_int8, containertypes.T_int16, containertypes.T_int32,
		containertypes.T_int64, containertypes.T_int128,
		containertypes.T_uint8, containertypes.T_uint16, containertypes.T_uint32,
		containertypes.T_uint64, containertypes.T_uint128,
		containertypes.T_float32, containertypes.T_float64,
		containertypes.T_decimal64, containertypes.T_decimal128, containertypes.T_decimal256,
		containertypes.T_date, containertypes.T_time, containertypes.T_datetime,
		containertypes.T_timestamp, containertypes.T_year, containertypes.T_uuid,
		containertypes.T_enum:
		return true
	default:
		return false
	}
}
