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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func (builder *QueryBuilder) enumerateTemporalIndexCandidates(scanID int32, indexes []*plan.IndexDef, cost *encodedRegularIndexCostContext, consider func(regularIndexAccessCandidate)) (access temporalIndexAccess, err error) {
	ctx, proc := builder.GetContext(), builder.compCtx.GetProcess()
	// Before publication the caller's selected cause wins over helper ctx.Err.
	// Once the plan is published, a later stop does not undo that result.
	defer func() {
		if context.Cause(ctx) != nil {
			access, err = temporalIndexAccess{}, context.Cause(ctx)
		}
	}()
	resolver := builder.temporalResolver
	if proc == nil || resolver == nil || !resolver.IsSameCompiler(builder.compCtx) || len(indexes) > engine.MaxTemporalIndexDefinitions || !cost.valid {
		return temporalIndexAccess{}, context.Cause(ctx)
	}
	if !builder.temporalPlanningIntent.Placement.AllowsNamedTemporalCandidate() || GetForceScanOnMultiCN() {
		return temporalIndexAccess{}, nil
	}
	budget := engine.MaxTemporalBindingWork
	if !temporalBaseReaderShapeSafe(ctx, builder.qry, scanID, &budget) || builder.optimizerHints != nil && builder.optimizerHints.blockFilter == 2 {
		return temporalIndexAccess{}, context.Cause(ctx)
	}
	scan := builder.qry.Nodes[scanID]
	if len(scan.FilterList) != 1 {
		return temporalIndexAccess{}, nil
	}
	atom, admitted := temporalCandidateAtomForExpr(scan.FilterList[0])
	if !admitted || atom.column.GetCol().ColPos < 0 || int(atom.column.GetCol().ColPos) >= len(scan.TableDef.Cols) {
		return temporalIndexAccess{}, nil
	}
	column := scan.TableDef.Cols[atom.column.GetCol().ColPos]
	if column == nil {
		return temporalIndexAccess{}, nil
	}
	full, borrow, ok := builder.temporalScanBorrow(scanID)
	if !ok || !resolver.BorrowStillCurrent(builder.compCtx, proc, borrow) {
		return temporalIndexAccess{}, nil
	}
	provider, supported := borrow.Relation.(engine.TemporalIndexRelation)
	read := borrow.Read
	if !supported || read.Op != proc.GetTxnOperator() {
		return temporalIndexAccess{}, nil
	}
	stats := builder.getStatsInfoByTableID(scan.TableDef.TblId)
	if stats == nil || stats.GetStats() == nil {
		return temporalIndexAccess{}, nil
	}
	observation, ok, err := provider.ReadTemporalCostStats(ctx, read, stats.GetStats(), column.Name)
	if err != nil || !ok || observation.Rows != cost.statsTableCnt || observation.Rows != scan.Stats.TableCnt {
		return temporalIndexAccess{}, err
	}
	scalar, ok, err := evaluateTemporalCostScalar(ctx, proc, atom.bound, &budget)
	if err != nil || !ok || scalar.null || len(scalar.value) != 8 || scalar.typ.Oid != types.T(atom.bound.Typ.Id) || scalar.typ.Scale != atom.bound.Typ.Scale {
		return temporalIndexAccess{}, err
	}
	var bound int64
	if scalar.typ.Oid == types.T_timestamp {
		bound = int64(types.DecodeTimestamp(scalar.value))
	} else {
		bound = int64(types.DecodeDatetime(scalar.value))
	}
	candidate, err := types.BuildTemporalCandidate(ctx, types.T(atom.column.Typ.Id), atom.scale, bound, false, atom.op, proc.GetSessionInfo().TimeZone)
	if err != nil {
		return temporalIndexAccess{}, err
	}
	if scalar.typ.Oid == types.T_timestamp && !function.TemporalTimestampBoundNotEmpty(types.Timestamp(bound), int32(atom.op), atom.scale) {
		return temporalIndexAccess{}, nil
	}
	selectivity, ok := estimateTemporalRange(observation, candidate, types.T(atom.column.Typ.Id))
	if !ok || selectivity <= 0 {
		return temporalIndexAccess{}, nil
	}
	if len(cost.unknownRangeLowerSelectivities) != 0 {
		return temporalIndexAccess{}, nil
	}
	// The reader-shape check above already proved block/filter correspondence.
	// Check the remaining finalized-reader conditions before observing blocks.
	if !finitePositive(selectivity) || selectivity > 1 || !finitePositive(observation.Rows) ||
		!temporalBaseBlockFiltersFinal(ctx, scan, true, &budget) {
		return temporalIndexAccess{}, context.Cause(ctx)
	}
	if len(scan.BlockFilterList) != 0 {
		if len(scan.BlockFilterList) != 1 || len(scan.FilterList) != 1 || observation.Blocks <= 0 {
			return temporalIndexAccess{}, context.Cause(ctx)
		}
		nodes := 0
		if !temporalCostExpressionFits(ctx, scan.FilterList[0], 0, &nodes, &budget) || !spendTemporalCostWork(ctx, &budget, 8*nodes) ||
			!ScanStorageFilterEligible(proc, scan.BlockFilterList[0], false) {
			return temporalIndexAccess{}, context.Cause(ctx)
		}
	}
	if budget <= 0 || context.Cause(ctx) != nil {
		return temporalIndexAccess{}, context.Cause(ctx)
	}
	needsBaseInput := len(scan.BlockFilterList) != 0
	baseWork := 0.0
	if !needsBaseInput {
		baseWork = observation.Rows * (cost.baseWidth + float64(len(scan.FilterList))*regularIndexPredicateRowWork)
		if !finitePositive(baseWork) {
			return temporalIndexAccess{}, nil
		}
	}
	for indexPos, index := range indexes {
		if index == nil {
			continue
		}
		if !spendTemporalCostWork(ctx, &budget, len(full.Cols)+len(index.Parts)+len(builder.qry.Nodes)) {
			return temporalIndexAccess{}, context.Cause(ctx)
		}
		if len(index.Parts) > engine.MaxTemporalIndexParts || len(index.IndexTableName) > engine.MaxTemporalNameBytes || len(index.IndexName) > engine.MaxTemporalNameBytes {
			return temporalIndexAccess{}, nil
		}
		for _, part := range index.Parts {
			if len(part) > engine.MaxTemporalNameBytes {
				return temporalIndexAccess{}, nil
			}
		}
		if !cost.indexCoversRequiredColumns(index) {
			continue
		}
		_, matched, cause := temporalIndexOperands(ctx, builder.qry, scanID, true, index, 0)
		if cause != nil {
			return temporalIndexAccess{}, cause
		}
		if !matched {
			continue
		}
		// Observe once, only after a usable covering candidate exists.
		if needsBaseInput {
			request := engine.TemporalBlockInputRequest{Column: column.Name, Observed: stats.GetStats(), Stats: observation,
				BoundType: scalar.typ.Oid, Scale: atom.scale, Op: atom.op, Zone: proc.GetSessionInfo().TimeZone}
			copy(request.Bound[:], scalar.value)
			input, complete, cause := provider.ObserveTemporalBlockInput(ctx, read, request, &budget)
			if cause != nil || !complete || input.Rows == 0 || input.Blocks == 0 || !builder.temporalPlanningIntent.MatchesProcess(proc) {
				return temporalIndexAccess{}, cause
			}
			// Compare the optimistic parallel BASE latency with the index's
			// serial work. The normal scan owner supplies its local reader
			// estimate; no more readers can run than there are matching blocks.
			readers := 1.0
			// GetExecType visits the whole query, including unreachable nodes.
			// Only call it when that traversal is bounded by the simple shape.
			classify := spendTemporalCostWork(ctx, &budget, 4*len(builder.qry.Nodes))
			for _, node := range builder.qry.Nodes {
				if node == nil || len(node.Children) > 1 ||
					(node.NodeType != plan.Node_TABLE_SCAN && node.NodeType != plan.Node_PROJECT && node.NodeType != plan.Node_FILTER) {
					classify = false
					break
				}
			}
			if classify && GetExecType(builder.qry, false, builder.isPrepareStatement) != ExecTypeTP {
				readers = min(cost.localScanReaders(), float64(input.Blocks))
			}
			baseWork = float64(input.Rows) * (cost.baseWidth + float64(len(scan.FilterList))*regularIndexPredicateRowWork) / readers
			if !finitePositive(baseWork) || budget <= 0 {
				return temporalIndexAccess{}, context.Cause(ctx)
			}
			needsBaseInput = false
		}
		// Build the actual runtime hint for structure charging, never with the
		// observed scalar. Full hidden metadata is resolved only for the winner.
		if !prepayTemporalCBOConstruction(ctx, atom.bound, &budget) {
			return temporalIndexAccess{}, context.Cause(ctx)
		}
		var hint *plan.Expr
		if indexTableStoresSerializedKey(index) {
			physical := GetColExpr(plan.Type{Id: int32(types.T_varchar)}, 0, 0)
			hint, _, cause = makeEncodedTemporalCandidate(ctx, physical, atom.column, atom.bound, atom.op, atom.scale)
		} else {
			hint, _, cause = makeDirectTemporalCandidate(ctx, atom.column, atom.bound, atom.op, atom.scale)
		}
		if cause != nil {
			return temporalIndexAccess{}, cause
		}
		nodes, _, bounded := temporalCBOStructure(hint, &budget, ctx)
		if !bounded {
			return temporalIndexAccess{}, context.Cause(ctx)
		}
		estimate := encodedRegularIndexEstimate{blockInput: true, filter: 0, selectivity: selectivity, outputRows: selectivity * observation.Rows,
			fixedWork: float64(nodes) * regularIndexPredicateRowWork}
		consider(regularIndexAccessCandidate{indexPos: indexPos, shape: encodedRegularIndexCostIndexOnly,
			filterIdx: []int32{0}, residualLeadingPos: []int32{0}, temporal: true, estimate: estimate, baseWork: baseWork})
	}
	return temporalIndexAccess{provider: provider, full: full, borrow: borrow, column: column.Name, budget: budget}, nil
}

// temporalIndexAccess borrows one planning attempt's verified metadata. The
// ordinary selector owns candidate ranking; only its winner resolves a hidden table.
type temporalIndexAccess struct {
	provider engine.TemporalIndexRelation
	full     *plan.TableDef
	borrow   *BorrowedTable
	column   string
	budget   int
}

func (builder *QueryBuilder) applyTemporalIndexCandidate(scanID int32, index *plan.IndexDef, remap map[[2]int32]*plan.Expr, cost *encodedRegularIndexCostContext, access temporalIndexAccess) (result int32, applied bool, err error) {
	ctx, proc := builder.GetContext(), builder.compCtx.GetProcess()
	defer func() {
		if !applied && context.Cause(ctx) != nil {
			result, err = scanID, context.Cause(ctx)
		}
	}()
	resolver := builder.temporalResolver
	if access.budget <= 0 || resolver == nil || !resolver.IsSameCompiler(builder.compCtx) || !resolver.BorrowStillCurrent(builder.compCtx, proc, access.borrow) || !builder.temporalPlanningIntent.MatchesProcess(proc) || context.Cause(ctx) != nil {
		return scanID, false, context.Cause(ctx)
	}
	scan := builder.qry.Nodes[scanID]
	ref, table, hiddenBorrow, err := resolver.ResolveIndexForPlanning(scan.ObjRef, index.IndexTableName, scan.ScanSnapshot)
	if err != nil {
		return scanID, false, err
	}
	if ref == nil || table == nil || ref.Obj <= 0 || uint64(ref.Obj) != table.TblId || ref.PubInfo != nil ||
		ref.SubscriptionName != "" || IsSnapshotValid(ref.Snapshot) || table.IsTemporary || hiddenBorrow == nil ||
		!resolver.BorrowStillCurrent(builder.compCtx, proc, hiddenBorrow) {
		return scanID, false, nil
	}
	resolved := &temporalResolvedIndex{ref: ref, table: table, verify: func() (bool, error) {
		valid, cause := access.provider.VerifyTemporalIndexPair(ctx, access.borrow.Read, engine.TemporalPairCheck{BaseRef: scan.ObjRef, HiddenRef: ref, BaseFull: access.full, BaseScan: scan.TableDef, HiddenFull: table, Index: index, Column: access.column, Hidden: hiddenBorrow.Relation, HiddenRead: hiddenBorrow.Read}, &access.budget)
		return valid && access.borrow.Read.Op == proc.GetTxnOperator() && access.budget > 0 && resolver.BorrowStillCurrent(builder.compCtx, proc, access.borrow) && resolver.BorrowStillCurrent(builder.compCtx, proc, hiddenBorrow) && builder.temporalPlanningIntent.MatchesProcess(proc) && context.Cause(ctx) == nil, cause
	}}
	return builder.applyTemporalIndexOnlyScanResolved(scanID, index, remap, cost, resolved)
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
