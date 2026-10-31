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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type temporalCBOProposal struct {
	index  *plan.IndexDef
	filter int
	work   float64
}

func (builder *QueryBuilder) applyCostedTemporalIndex(scanID int32, indexes []*plan.IndexDef, remap map[[2]int32]*plan.Expr, cost *encodedRegularIndexCostContext) (result int32, applied bool, err error) {
	ctx, proc := builder.GetContext(), builder.compCtx.GetProcess()
	// Before publication the caller's selected cause wins over helper ctx.Err.
	// Once the plan is published, a later stop does not undo that result.
	defer func() {
		if !applied && context.Cause(ctx) != nil {
			result, err = scanID, context.Cause(ctx)
		}
	}()
	scope := builder.temporalPlanningScope
	if proc == nil || !scope.Usable() || len(indexes) > engine.MaxTemporalIndexDefinitions || !cost.valid {
		return scanID, false, context.Cause(ctx)
	}
	if !builder.temporalPlanningIntent.Placement.AllowsNamedTemporalCandidate() || GetForceScanOnMultiCN() {
		return scanID, false, nil
	}
	budget := engine.MaxTemporalBindingWork
	if !temporalBaseReaderShapeSafe(ctx, builder.qry, scanID, &budget) || builder.optimizerHints != nil && builder.optimizerHints.blockFilter == 2 {
		return scanID, false, context.Cause(ctx)
	}
	scan := builder.qry.Nodes[scanID]
	if len(scan.FilterList) != 1 {
		return scanID, false, nil
	}
	atom, admitted := temporalCandidateAtomForExpr(scan.FilterList[0])
	if !admitted || atom.column.GetCol().ColPos < 0 || int(atom.column.GetCol().ColPos) >= len(scan.TableDef.Cols) {
		return scanID, false, nil
	}
	column := scan.TableDef.Cols[atom.column.GetCol().ColPos]
	if column == nil {
		return scanID, false, nil
	}
	relation, full, read, ok := scope.NativeRelation(scan.ObjRef, scan.TableDef)
	provider, supported := relation.(engine.TemporalIndexRelation)
	if !ok || !supported || read.Op != proc.GetTxnOperator() {
		return scanID, false, nil
	}
	stats := builder.getStatsInfoByTableID(scan.TableDef.TblId)
	if stats == nil || stats.GetStats() == nil {
		return scanID, false, nil
	}
	observation, ok, err := provider.ReadTemporalCostStats(ctx, read, stats.GetStats(), column.Name)
	if err != nil || !ok || observation.Rows != cost.statsTableCnt || observation.Rows != scan.Stats.TableCnt {
		return scanID, false, err
	}
	scalar, ok, err := evaluateTemporalCostScalar(ctx, proc, atom.bound, &budget)
	if err != nil || !ok || scalar.null || len(scalar.value) != 8 || scalar.typ.Oid != types.T(atom.bound.Typ.Id) || scalar.typ.Scale != atom.bound.Typ.Scale {
		return scanID, false, err
	}
	var bound int64
	if scalar.typ.Oid == types.T_timestamp {
		bound = int64(types.DecodeTimestamp(scalar.value))
	} else {
		bound = int64(types.DecodeDatetime(scalar.value))
	}
	candidate, err := types.BuildTemporalCandidate(ctx, types.T(atom.column.Typ.Id), atom.scale, bound, false, atom.op, proc.GetSessionInfo().TimeZone)
	if err != nil {
		return scanID, false, err
	}
	if scalar.typ.Oid == types.T_timestamp && !function.TemporalTimestampBoundNotEmpty(types.Timestamp(bound), int32(atom.op), atom.scale) {
		return scanID, false, nil
	}
	selectivity, ok := estimateTemporalRange(observation, candidate, types.T(atom.column.Typ.Id))
	if !ok || selectivity <= 0 {
		return scanID, false, nil
	}
	if len(cost.unknownRangeLowerSelectivities) != 0 {
		return scanID, false, nil
	}
	baseWork, ok := builder.temporalCBOBaseWork(scan, observation, selectivity, cost, &budget)
	if !ok {
		return scanID, false, context.Cause(ctx)
	}
	attemptCost := *cost
	attemptCost.baseWork = baseWork
	best := temporalCBOProposal{work: math.Inf(1)}
	for _, index := range indexes {
		if index == nil {
			continue
		}
		if !spendTemporalCostWork(ctx, &budget, len(full.Cols)+len(index.Parts)+len(builder.qry.Nodes)) {
			return scanID, false, context.Cause(ctx)
		}
		if len(index.Parts) > engine.MaxTemporalIndexParts || len(index.IndexTableName) > engine.MaxTemporalNameBytes || len(index.IndexName) > engine.MaxTemporalNameBytes {
			return scanID, false, nil
		}
		for _, part := range index.Parts {
			if len(part) > engine.MaxTemporalNameBytes {
				return scanID, false, nil
			}
		}
		if !cost.indexCoversRequiredColumns(index) {
			continue
		}
		prefix, filter, matched, cause := findTemporalIndexMatch(ctx, builder.qry, scanID, true, index)
		if cause != nil {
			return scanID, false, cause
		}
		if !matched || len(prefix) != 0 || filter != 0 {
			continue
		}
		// Build the actual runtime hint for structure charging, never with the
		// observed scalar. Full hidden metadata is resolved only for the winner.
		if !prepayTemporalCBOConstruction(ctx, atom.bound, &budget) {
			return scanID, false, context.Cause(ctx)
		}
		var hint *plan.Expr
		if indexTableStoresSerializedKey(index) {
			physical := GetColExpr(plan.Type{Id: int32(types.T_varchar)}, 0, 0)
			hint, _, cause = makeEncodedTemporalCandidate(ctx, physical, atom.column, atom.bound, nil, len(index.Parts) > 1, atom.op, atom.scale)
		} else {
			hint, _, cause = makeDirectTemporalCandidate(ctx, atom.column, atom.bound, atom.op, atom.scale)
		}
		if cause != nil {
			return scanID, false, cause
		}
		nodes, _, bounded := temporalCBOStructure(hint, &budget, ctx)
		if !bounded {
			return scanID, false, context.Cause(ctx)
		}
		estimate := &encodedRegularIndexEstimate{blockInput: true, filter: int32(filter), selectivity: selectivity, outputRows: selectivity * observation.Rows,
			fixedWork: float64(nodes) * regularIndexPredicateRowWork}
		work, rejected, valid := attemptCost.scoreWithEstimate(index, []int32{int32(filter)}, []int32{int32(filter)}, encodedRegularIndexCostIndexOnly, estimate)
		if valid && !rejected && work < baseWork && work < best.work {
			best = temporalCBOProposal{index: index, filter: filter, work: work}
		}
	}
	if best.index == nil || budget <= 0 || !scope.Usable() || !builder.temporalPlanningIntent.MatchesProcess(proc) || context.Cause(ctx) != nil {
		return scanID, false, context.Cause(ctx)
	}
	ref, table, err := builder.compCtx.ResolveIndexTableByRef(scan.ObjRef, best.index.IndexTableName, scan.ScanSnapshot)
	if err != nil {
		return scanID, false, err
	}
	hidden, fullHidden, hiddenRead, ok := scope.NativeRelation(ref, table)
	// The selected scan must use this exact resolution, rather than an older
	// same-identity binding whose optional capture happened to succeed.
	if !ok || fullHidden != table {
		return scanID, false, nil
	}
	resolved := &temporalResolvedIndex{ref: ref, table: table, verify: func() (bool, error) {
		valid, cause := provider.VerifyTemporalIndexPair(ctx, read, engine.TemporalPairCheck{BaseRef: scan.ObjRef, HiddenRef: ref, BaseFull: full, BaseScan: scan.TableDef, HiddenFull: fullHidden, Index: best.index, Column: column.Name, Hidden: hidden, HiddenRead: hiddenRead}, &budget)
		return valid && read.Op == proc.GetTxnOperator() && budget > 0 && scope.Usable() && builder.temporalPlanningIntent.MatchesProcess(proc) && context.Cause(ctx) == nil, cause
	}}
	id, applied, err := builder.applyTemporalIndexOnlyScanResolved(scanID, best.index, true, nil, best.filter, remap, cost, resolved)
	if applied && err == nil {
		builder.qry.Nodes[id].Stats.ForceOneCN = true
		scope.recordTemporalCacheIndex(scan.TableDef, table, best.index)
	}
	return id, applied, err
}

func temporalCBOStructure(root *plan.Expr, budget *int, ctx context.Context) (int, int, bool) {
	nodes, predicates := 0, 0
	var visit func(*plan.Expr, int) bool
	visit = func(e *plan.Expr, depth int) bool {
		if e == nil || depth > 64 || !spendTemporalCostWork(ctx, budget, 1) {
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

// The fixed topology has at most seven bound endpoints plus its guard. The
// encoded form also copies endpoint trees into serial leaves. Reserve twice
// those copies and their bounded wrapper nodes before any construction. The
// subsequent walk independently charges inspection of the resulting tree.
func prepayTemporalCBOConstruction(ctx context.Context, bound *plan.Expr, budget *int) bool {
	nodes := 0
	if !temporalCostExpressionFits(ctx, bound, 0, &nodes, budget) {
		return false
	}
	return spendTemporalCostWork(ctx, budget, 16*(nodes+64)+16*bound.ProtoSize()) && *budget > 0
}

// Only the actual finalized reader predicate earns base block-pruning credit.
// The cost-only range never becomes a base filter or changes shared stats.
func (builder *QueryBuilder) temporalCBOBaseWork(scan *plan.Node, stats TemporalRangeStatistics, selectivity float64, cost *encodedRegularIndexCostContext, budget *int) (float64, bool) {
	ctx := builder.GetContext()
	if !finitePositive(selectivity) || selectivity > 1 || !finitePositive(stats.Rows) || !temporalBaseBlockFiltersFinal(ctx, scan, true, budget) {
		return 0, false
	}
	rows := stats.Rows
	if len(scan.BlockFilterList) != 0 {
		if len(scan.BlockFilterList) != 1 || len(scan.FilterList) != 1 || !stats.HasOverlap || stats.Blocks <= 0 ||
			!finiteNonnegative(stats.Overlap) || stats.Overlap > 1 ||
			!temporalCostFiltersEqual(ctx, scan.BlockFilterList[0], scan.FilterList[0], budget) {
			return 0, false
		}
		nodes := 0
		if !temporalCostExpressionFits(ctx, scan.FilterList[0], 0, &nodes, budget) || !spendTemporalCostWork(ctx, budget, 8*nodes) ||
			!ScanStorageFilterEligible(builder.compCtx.GetProcess(), scan.BlockFilterList[0], false) {
			return 0, false
		}
		atom, admitted := temporalCandidateAtomForExpr(scan.FilterList[0])
		if !admitted {
			return 0, false
		}
		column := scan.TableDef.Cols[atom.column.GetCol().ColPos].Name
		observation := &statsinfo.StatsInfo{ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{column: {Overlap: stats.Overlap}}}
		header := *scan.FilterList[0]
		header.Selectivity = selectivity
		blockSel := estimateFilterBlockSelectivity(ctx, &header, scan.TableDef, observation)
		if !finiteNonnegative(blockSel) || blockSel > 1 {
			return 0, false
		}
		blockRows := min(stats.Rows, float64(estimatedBlocks(float64(stats.Blocks)*blockSel))*float64(objectio.BlockMaxRows))
		rows = max(stats.Rows*selectivity, stats.Rows*blockSel, blockRows)
	}
	work := rows * (cost.baseWidth + float64(len(scan.FilterList))*regularIndexPredicateRowWork)
	return work, finitePositive(work) && budget != nil && *budget > 0 && context.Cause(ctx) == nil
}
