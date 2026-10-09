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
	"math"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
)

const (
	groupingSetExpandOptionPrefix = "grouping_set_expand:"
	// This is a planner ceiling, not an execution allowance. The bounded
	// materialized source still charges every spill byte and file descriptor to
	// the statement/CN owner before writing.
	groupingSetEstimatedSpillBytesLimit = float64(8 * mpool.GB)
	// Group cardinality is especially fragile here: the rewrite runs before the
	// normal optimizer passes, while joins and correlated multi-column keys can
	// make the estimate miss by orders of magnitude. Inflate the estimate, but
	// never beyond the relational one-output-row-per-input-row ceiling.
	groupingSetCardinalitySafetyFactor = float64(32)
	// Require a twofold modeled advantage because row counts and producer costs
	// are estimates while the output write and every branch scan are certain.
	groupingSetCostSafetyFactor = float64(2)
)

// DecodeGroupingSetExpandOption exposes planner-owned expand metadata to the
// compiler without adding SQL-visible syntax or a new logical node kind.
func DecodeGroupingSetExpandOption(option string) (int, bool) {
	if !strings.HasPrefix(option, groupingSetExpandOptionPrefix) {
		return 0, false
	}
	count, err := strconv.Atoi(strings.TrimPrefix(option, groupingSetExpandOptionPrefix))
	return count, err == nil && count > 1
}

func groupingSetExpandOutputType(
	typ planpb.Type,
	flags []bool,
	setCount int,
	projectPos int32,
) planpb.Type {
	if setCount < 2 || len(flags) == 0 || len(flags)%setCount != 0 {
		return typ
	}
	groupCount := len(flags) / setCount
	if projectPos < 0 || int(projectPos) >= groupCount {
		return typ
	}
	for set := 0; set < setCount; set++ {
		if !flags[set*groupCount+int(projectPos)] {
			typ.NotNullable = false
			break
		}
	}
	return typ
}

type groupingSetBranch struct {
	rootID int32
	aggID  int32
	agg    *planpb.Node
	ctx    *BindContext
}

type groupingSetCandidate struct {
	nodes    []int32
	contexts []*BindContext
}

type directGroupingSetOutput struct {
	unionRootID int32
	projectList []*planpb.Expr
}

func (builder *QueryBuilder) registerGroupingSetInput(nodes []int32, contexts []*BindContext) {
	builder.groupingSetCandidates = append(builder.groupingSetCandidates, groupingSetCandidate{
		nodes:    append([]int32(nil), nodes...),
		contexts: append([]*BindContext(nil), contexts...),
	})
}

// groupingSetInputSharingMayApply is the cheap common precondition for the
// protocol 49+ grouping-set sharing rewrite. It is deliberately only a
// possibility check: shape, determinism, drain, and capacity checks remain in
// shareGroupingSetInput, and a failed detailed rewrite still falls back to the
// independent branch model. The rollup cost probe uses this same possibility
// check as a conservative lower-envelope baseline; it must not claim that the
// detailed rewrite is guaranteed to succeed.
func (builder *QueryBuilder) groupingSetInputSharingMayApply() bool {
	if builder == nil || builder.sharedComputationDisabled() ||
		builder.sessionSelectLimitMayStopEarly {
		return false
	}
	if builder.compCtx == nil {
		return false
	}
	proc := builder.compCtx.GetProcess()
	if proc == nil {
		return false
	}
	version, _ := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.MOProtocolVersion)
	protocolVersion, ok := version.(int64)
	return ok && protocolVersion >= defines.MORPCVersion49
}

func (builder *QueryBuilder) sharePendingGroupingSetInputs(rootID int32) int32 {
	if !builder.groupingSetInputSharingMayApply() {
		return rootID
	}
	parents := builder.groupingSetConsumerParents()
	for _, candidate := range builder.groupingSetCandidates {
		reachable := make(map[int32]bool)
		builder.collectCTEParents(rootID, make(map[int32][]int32), reachable)
		for _, stepRootID := range builder.qry.Steps {
			builder.collectCTEParents(stepRootID, make(map[int32][]int32), reachable)
		}
		allReachable := len(candidate.nodes) > 1
		for _, nodeID := range candidate.nodes {
			if !reachable[nodeID] {
				allReachable = false
				break
			}
		}
		if allReachable && !builder.groupingSetCandidateHasGroupingAncestor(candidate, parents) {
			builder.shareGroupingSetInput(rootID, candidate.nodes, candidate.contexts)
		}
	}
	return rootID
}

// groupingSetConsumerParents describes both ordinary child consumers and
// materialized-source consumers. It is captured before any grouping candidate
// is rewritten so nested candidates cannot disappear behind a newly inserted
// SINK_SCAN edge while admission is in progress.
func (builder *QueryBuilder) groupingSetConsumerParents() map[int32][]int32 {
	parents := make(map[int32][]int32)
	for parentID, node := range builder.qry.Nodes {
		if node == nil {
			continue
		}
		for _, childID := range node.Children {
			parents[childID] = append(parents[childID], int32(parentID))
		}
		for _, sourceStep := range node.SourceStep {
			if sourceStep >= 0 && int(sourceStep) < len(builder.qry.Steps) {
				sourceRootID := builder.qry.Steps[sourceStep]
				parents[sourceRootID] = append(parents[sourceRootID], int32(parentID))
			}
		}
	}
	return parents
}

func (builder *QueryBuilder) groupingSetCandidateHasGroupingAncestor(
	candidate groupingSetCandidate,
	parents map[int32][]int32,
) bool {
	queue := append([]int32(nil), candidate.nodes...)
	seen := make(map[int32]bool, len(queue))
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		if seen[nodeID] {
			continue
		}
		seen[nodeID] = true
		for _, parentID := range parents[nodeID] {
			if parentID < 0 || int(parentID) >= len(builder.qry.Nodes) || builder.qry.Nodes[parentID] == nil {
				return true
			}
			parent := builder.qry.Nodes[parentID]
			if parent.NodeType == planpb.Node_AGG && len(parent.GroupingFlag) > 0 {
				return true
			}
			queue = append(queue, parentID)
		}
	}
	return false
}

// shareGroupingSetInput replaces the independently bound aggregate branches of
// one internally generated grouping-set UNION with the canonical
// expand-aggregate shape:
//
//	common input -> vector-level grouping-set expand
//	             -> one aggregate keyed by (groups, set id)
//	             -> bounded materialized fanout to branch projects
//
// The rewrite is deliberately fail-closed. The internal UNION marker proves a
// common FROM/WHERE AST; typed aggregate shape, determinism, bounded output,
// and a conservative byte-work comparison prove that sharing is safe and
// useful. Only the reduced aggregate output is materialized; detailed input is
// never retained. Independent readers prevent a lazy UNION ALL branch from
// backpressuring the producer while another branch is still draining.
func (builder *QueryBuilder) shareGroupingSetInput(
	rootID int32,
	nodes []int32,
	contexts []*BindContext,
) bool {
	if len(nodes) < 2 || len(nodes) != len(contexts) {
		return false
	}

	branches := make([]groupingSetBranch, len(nodes))
	for i := range nodes {
		ctx := contexts[i]
		if ctx == nil || ctx.isCorrelated {
			return false
		}
		aggID, ok := builder.findGroupingSetAggregate(nodes[i], ctx)
		if !ok {
			return false
		}
		agg := builder.qry.Nodes[aggID]
		if len(agg.Children) != 1 || len(agg.BindingTags) < 2 ||
			len(agg.GroupBy) == 0 || len(agg.GroupingFlag) != len(agg.GroupBy) {
			return false
		}
		branches[i] = groupingSetBranch{rootID: nodes[i], aggID: aggID, agg: agg, ctx: ctx}
	}

	first := branches[0].agg
	groupCount := len(first.GroupBy)
	aggCount := len(first.AggList)
	for i := 1; i < len(branches); i++ {
		if !sameGroupingSetAggregateShape(first, branches[i].agg) {
			return false
		}
	}
	// The shared producer is an eager query step. Prove that the legacy plan
	// already had to consume at least one complete grouping branch; otherwise
	// an outer LIMIT or a conditionally skipped join/APPLY input could make work
	// (and errors) observable that the lazy UNION ALL never reached. Reuse the
	// same path proof as shared CTEs and preserve any join build-side contract
	// on the replacement scan below.
	drainOccurrences := make([]cteOccurrence, len(branches))
	for i := range branches {
		drainOccurrences[i] = cteOccurrence{rootID: branches[i].rootID}
	}
	drainProof := builder.proveCTEConsumerDrainRequirements(rootID, drainOccurrences)
	if !drainProof.hasWitness {
		return false
	}
	hashBuildBranches := drainProof.hashBuildOccurrences
	allBranchesDrained := len(drainProof.drainedOccurrences) == len(branches)
	directOutput, directOutputOK := builder.directGroupingSetOutput(nodes, branches)
	// Collapsing the UNION changes when rows from individual grouping sets become
	// visible. Keep that form for consumers which already have to drain every
	// legacy branch. The direct form also has no SINK_SCAN on which to carry a
	// required hash-build marker, so retain materialization when the drain proof
	// depends on a fixed build side.
	directOutputOK = directOutputOK && allBranchesDrained && len(hashBuildBranches) == 0
	if !builder.subtreeIsDeterministic(first.Children[0], make(map[int32]bool), true) {
		return false
	}
	for _, expr := range first.GroupBy {
		if expr == nil || !exprCanRemoveProject(expr) {
			return false
		}
		if !allBranchesDrained && !builder.cteExprIsTotal(
			first.Children[0], expr, branches[0].ctx, make(map[[2]int32]bool),
		) {
			return false
		}
	}
	for _, agg := range first.AggList {
		fn := agg.GetF()
		if fn == nil || fn.AggConfigType != planpb.AggregateConfigType_AGG_CONFIG_NONE || len(fn.AggConfig) != 0 {
			return false
		}
		// A legacy grouping branch may leave some grouping expressions inactive,
		// and an outer consumer may stop before other branches. Dynamic expansion
		// evaluates every key and aggregate for every set. If every legacy branch
		// is already guaranteed to drain, every deterministic expression and
		// aggregate is evaluated on the same complete input domain and no totality
		// proof is needed. Otherwise retain the conservative totality guard.
		if !allBranchesDrained && !builder.cteAggregateIsTotal(
			first.Children[0], agg, branches[0].ctx, make(map[[2]int32]bool),
		) {
			return false
		}
		for _, arg := range fn.Args {
			if arg == nil || !exprCanRemoveProject(arg) {
				return false
			}
		}
	}

	// Build the exact value schemas used by the shared aggregate before changing
	// the plan. Declared variable-width capacities are deliberately included:
	// an unknown or unbounded materialized row keeps the historical plan.
	inputProject := make([]*planpb.Expr, 0, groupCount+aggCount+2)
	for groupPos, group := range first.GroupBy {
		group = DeepCopyExpr(group)
		for _, branch := range branches {
			if !branch.agg.GroupingFlag[groupPos] {
				group.Typ.NotNullable = false
				break
			}
		}
		inputProject = append(inputProject, group)
	}
	aggArgPositions := make([][]int32, aggCount)
	for i, agg := range first.AggList {
		for _, arg := range agg.GetF().Args {
			aggArgPositions[i] = append(aggArgPositions[i], int32(len(inputProject)))
			inputProject = append(inputProject, DeepCopyExpr(arg))
		}
	}
	inputTypes := make([]planpb.Type, len(inputProject))
	for i := range inputProject {
		inputTypes[i] = inputProject[i].Typ
	}
	outputTypes := make([]planpb.Type, 0, groupCount+aggCount+1)
	for i := 0; i < groupCount; i++ {
		outputTypes = append(outputTypes, inputProject[i].Typ)
	}
	for _, agg := range first.AggList {
		outputTypes = append(outputTypes, agg.Typ)
	}
	outputTypes = append(outputTypes,
		planpb.Type{Id: int32(types.T_int64), NotNullable: true})
	inputRowSize, inputSizeKnown := materializedDeclaredRowSize(inputTypes)
	outputRowSize, outputSizeKnown := materializedDeclaredRowSize(outputTypes)
	if !inputSizeKnown || !outputSizeKnown {
		return false
	}

	// The old plan repeats the producer once per branch. The new plan runs it
	// once, then writes the complete aggregate output once and makes every
	// branch scan that complete output. Bound the optimistic group-NDV estimate
	// with a safety margin and the relational ceiling (each aggregate can emit at
	// most one row per input row). Multi-column NDVs are often correlated or
	// unavailable at this early rewrite point; using their raw result can turn a
	// supposedly small shared result into a multi-GB spill stage. Compare
	// byte-work, branch count and the bounded-spill ceiling; unknown or marginal
	// estimates fail closed.
	// Cost the joined relation, not the binder's Cartesian product with WHERE
	// join predicates still in a FILTER above it. Normalize only the original
	// aggregate inputs: the branch output/drain proofs above remain unchanged,
	// and a genuine Cartesian join retains its multiplicative row estimate.
	for i := range branches {
		inputID, remaining := builder.pushdownFilters(branches[i].agg.Children[0], nil, false)
		if len(remaining) > 0 {
			inputID = builder.appendNode(&planpb.Node{
				NodeType:   planpb.Node_FILTER,
				Children:   []int32{inputID},
				FilterList: remaining,
			}, nil)
		}
		branches[i].agg.Children[0] = inputID
	}
	producerID := first.Children[0]
	// A grouping sentinel belongs to the grouping extension that created it.
	// Legacy outer aggregates intentionally hash an inherited sentinel like SQL
	// NULL when the corresponding outer key is active. Dynamic grouping must
	// distinguish its own sentinel from SQL NULL, so feeding it an inherited
	// sentinel would split one SQL NULL group into two. Until relational
	// boundaries normalize grouping provenance, keep the legacy plan whenever a
	// producer can expose one. Follow materialized source steps as well as child
	// edges because an already-shared inner grouping set lives behind SINK_SCAN.
	if builder.subtreeMayExposeGroupingSentinel(producerID, make(map[int32]bool)) {
		return false
	}
	// Unlike the normal optimization pipeline, this rewrite runs before the
	// final leaf-statistics pass. Recalculate table leaves explicitly; costing a
	// materialization from the binder's tiny defaults defeats both the spill
	// ceiling and the profitability test on large fact tables.
	ReCalcNodeStats(producerID, builder, true, true, true)
	producerCost := builder.cteProducerCost(producerID, make(map[int32]bool))
	producerStats := builder.qry.Nodes[producerID].Stats
	if producerStats == nil || !finitePositive(producerStats.Outcnt) {
		return false
	}
	totalAggregateRows := float64(0)
	for i := range branches {
		ReCalcNodeStats(branches[i].aggID, builder, true, true, true)
		stats := branches[i].agg.Stats
		if stats == nil || !finitePositive(stats.Outcnt) {
			return false
		}
		totalAggregateRows += stats.Outcnt
	}
	estimatedMaterializedRows, ok := groupingSetMaterializedRowsForAdmission(
		producerStats.Outcnt, totalAggregateRows, len(branches))
	if !ok {
		return false
	}
	materializedSharingOK := groupingSetSharingFitsCostAndStorage(
		producerCost,
		inputRowSize,
		estimatedMaterializedRows,
		outputRowSize,
		len(branches),
	)
	if !directOutputOK && !materializedSharingOK {
		return false
	}

	// Validate every branch path before mutating the plan.
	for i := range branches {
		if builder.countReachableNode(branches[i].rootID, branches[i].aggID, make(map[int32]bool)) != 1 ||
			!builder.groupingBranchExpressionsRewritable(branches[i].rootID, branches[i].aggID, branches[i].agg) {
			return false
		}
	}
	if directOutputOK && builder.shareDirectGroupingSetSplit(
		branches, directOutput.unionRootID, producerStats, outputRowSize,
	) {
		return true
	}
	if !directOutputOK && !builder.reserveSharedMaterialization(
		estimatedMaterializedRows*outputRowSize,
		estimatedMaterializedRows,
		outputTypes,
	) {
		return false
	}

	inputTag := builder.genNewBindTag()
	if builder.syntheticNDVCols == nil {
		builder.syntheticNDVCols = make(map[[2]int32]struct{}, groupCount+1)
	}
	for i := 0; i < groupCount; i++ {
		builder.syntheticNDVCols[[2]int32{inputTag, int32(i)}] = struct{}{}
	}
	// The penultimate column is an execution-only marker. Normal expanded rows
	// carry false; on runtime-empty input the projection emits one true row for
	// each empty grouping set. Group inserts that row's key but skips aggregate
	// filling, preserving COUNT(*) = 0 and NULL aggregate states.
	inputProject = append(inputProject, MakePlan2BoolConstExprWithType(false))
	setIDPos := int32(len(inputProject))
	inputProject = append(inputProject, MakePlan2Int64ConstExprWithType(0))
	builder.syntheticNDVCols[[2]int32{inputTag, setIDPos}] = struct{}{}
	flattenedFlags := make([]bool, 0, len(branches)*groupCount)
	for _, branch := range branches {
		flattenedFlags = append(flattenedFlags, branch.agg.GroupingFlag...)
	}
	expandedID := builder.appendNode(&planpb.Node{
		NodeType:     planpb.Node_PROJECT,
		Children:     []int32{producerID},
		ProjectList:  inputProject,
		BindingTags:  []int32{inputTag},
		GroupingFlag: flattenedFlags,
		ExtraOptions: groupingSetExpandOptionPrefix + strconv.Itoa(len(branches)),
	}, branches[0].ctx)

	sharedGroupTag := builder.genNewBindTag()
	sharedAggTag := builder.genNewBindTag()
	sharedGroups := make([]*planpb.Expr, 0, groupCount+1)
	for i := 0; i < groupCount; i++ {
		group := groupingSetCol(inputProject[i].Typ, inputTag, int32(i))
		group.Ndv = first.GroupBy[i].Ndv
		sharedGroups = append(sharedGroups, group)
	}
	setIDGroup := groupingSetCol(
		planpb.Type{Id: int32(types.T_int64), NotNullable: true}, inputTag, setIDPos)
	setIDGroup.Ndv = float64(len(branches))
	sharedGroups = append(sharedGroups, setIDGroup)
	sharedAggs := DeepCopyExprList(first.AggList)
	for i, agg := range sharedAggs {
		for j, pos := range aggArgPositions[i] {
			agg.GetF().Args[j] = groupingSetCol(inputProject[pos].Typ, inputTag, pos)
		}
	}
	sharedAggID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_AGG,
		Children:    []int32{expandedID},
		GroupBy:     sharedGroups,
		AggList:     sharedAggs,
		BindingTags: []int32{sharedGroupTag, sharedAggTag},
		SpillMem:    first.SpillMem,
	}, branches[0].ctx)

	sharedOutputTag := builder.genNewBindTag()
	sharedOutput := make([]*planpb.Expr, 0, groupCount+aggCount+1)
	for i := 0; i < groupCount; i++ {
		sharedOutput = append(sharedOutput, groupingSetCol(sharedGroups[i].Typ, sharedGroupTag, int32(i)))
	}
	for i, agg := range sharedAggs {
		sharedOutput = append(sharedOutput, groupingSetCol(agg.Typ, sharedAggTag, int32(i)))
	}
	sharedOutput = append(sharedOutput,
		groupingSetCol(sharedGroups[groupCount].Typ, sharedGroupTag, int32(groupCount)))
	sharedOutputID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{sharedAggID},
		ProjectList: sharedOutput,
		BindingTags: []int32{sharedOutputTag},
	}, branches[0].ctx)
	if directOutputOK {
		projectList := DeepCopyExprList(directOutput.projectList)
		for i, expr := range projectList {
			rewriteGroupingSetExpr(expr, first, sharedOutputTag, groupCount)
			// Preserve the set-operation's common output contract. The source is
			// nullable whenever any grouping set omits a key, even if the first
			// legacy branch happened to make that key active.
			expr.Typ = builder.qry.Nodes[directOutput.unionRootID].ProjectList[i].Typ
		}
		unionRoot := builder.qry.Nodes[directOutput.unionRootID]
		unionRoot.NodeType = planpb.Node_PROJECT
		unionRoot.Children = []int32{sharedOutputID}
		unionRoot.ProjectList = projectList
		unionRoot.PhysicalEqualityKeyList = nil
		unionRoot.Stats = nil
		return true
	}
	sinkID := appendSinkNodeWithTag(builder, branches[0].ctx, sharedOutputID, sharedOutputTag)
	builder.qry.Nodes[sinkID].ExtraOptions = materialized.CTESinkOption
	sourceStep := builder.appendStep(sinkID)

	for i := range branches {
		scanTag := builder.genNewBindTag()
		scanProject := make([]*planpb.Expr, len(sharedOutput))
		cols := make([]*planpb.ColDef, len(sharedOutput))
		for j, output := range sharedOutput {
			scanProject[j] = groupingSetCol(output.Typ, scanTag, int32(j))
			cols[j] = &planpb.ColDef{Typ: output.Typ}
		}
		scanID := builder.appendNode(&planpb.Node{
			NodeType:    planpb.Node_SINK_SCAN,
			SourceStep:  []int32{sourceStep},
			ProjectList: scanProject,
			BindingTags: []int32{scanTag},
			TableDef:    &planpb.TableDef{Name: "__mo_grouping_sets", Cols: cols},
		}, branches[i].ctx)
		if hashBuildBranches[branches[i].rootID] {
			builder.qry.Nodes[scanID].ExtraOptions = materialized.CTEHashBuildScanOption
		}
		setID := groupingSetCol(sharedOutput[len(sharedOutput)-1].Typ, scanTag, int32(len(sharedOutput)-1))
		matches, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
			setID, MakePlan2Int64ConstExprWithType(int64(i)),
		})
		if err != nil {
			return false
		}
		filterID := builder.appendNode(&planpb.Node{
			NodeType:   planpb.Node_FILTER,
			Children:   []int32{scanID},
			FilterList: []*planpb.Expr{matches},
		}, branches[i].ctx)

		builder.rewriteGroupingBranch(branches[i].rootID, branches[i].aggID, filterID,
			branches[i].agg, scanTag, groupCount)
	}
	return true
}

// A large strict-prefix ROLLUP can be cheaper as two independent direct
// aggregates. The finest level often retains almost one group per input row;
// mixing it with every coarser level in one expanded hash table multiplies
// spill traffic. Keep that level's original producer and aggregate, and share
// only the remaining levels over their own original producer. Unlike prefix
// state reuse, every SUM still sees raw input values in its original branch.
func (builder *QueryBuilder) shareDirectGroupingSetSplit(
	branches []groupingSetBranch,
	unionRootID int32,
	producerStats *planpb.Stats,
	outputRowSize float64,
) bool {
	if !directGroupingSetSplitFits(branches, producerStats, outputRowSize) {
		return false
	}
	// TOP's comparator schema is fixed by its first input batch. A direct
	// UNION can deliver a later grouping branch whose ORDER BY expression
	// evaluates into an additional vector, changing that schema mid-stream.
	// Keep the existing single-aggregate path until TOP can handle that case.
	parents := builder.groupingSetConsumerParents()
	queue := []int32{unionRootID}
	seen := make(map[int32]bool)
	for len(queue) > 0 {
		childID := queue[0]
		queue = queue[1:]
		if seen[childID] {
			continue
		}
		seen[childID] = true
		for _, parentID := range parents[childID] {
			parent := builder.qry.Nodes[parentID]
			switch parent.NodeType {
			case planpb.Node_SORT:
				return false
			case planpb.Node_PROJECT, planpb.Node_FILTER:
				queue = append(queue, parentID)
			}
		}
	}

	coarse := branches[1:]
	first := coarse[0].agg
	groupCount := len(first.GroupBy)
	aggCount := len(first.AggList)
	producerID := first.Children[0]
	unionRoot := builder.qry.Nodes[unionRootID]
	finestRoot := builder.qry.Nodes[branches[0].rootID]
	if unionRoot == nil || finestRoot == nil || len(finestRoot.BindingTags) != 1 ||
		len(unionRoot.ProjectList) != len(finestRoot.ProjectList) {
		return false
	}
	// Validate the surviving UNION projection before appending any nodes.
	unionProject := DeepCopyExprList(unionRoot.ProjectList)
	for i, expr := range unionProject {
		col := expr.GetCol()
		if col == nil || col.ColPos != int32(i) {
			return false
		}
		col.RelPos = finestRoot.BindingTags[0]
	}

	// The last key is inactive in every remaining ROLLUP level. Do not
	// evaluate or hash it in this branch; restore its SQL NULL output below.
	inputProject := make([]*planpb.Expr, 0, groupCount+aggCount+1)
	for pos, group := range first.GroupBy[:groupCount-1] {
		group = DeepCopyExpr(group)
		for _, branch := range coarse {
			if !branch.agg.GroupingFlag[pos] {
				group.Typ.NotNullable = false
				break
			}
		}
		inputProject = append(inputProject, group)
	}
	aggArgPositions := make([][]int32, aggCount)
	for i, agg := range first.AggList {
		for _, arg := range agg.GetF().Args {
			aggArgPositions[i] = append(aggArgPositions[i], int32(len(inputProject)))
			inputProject = append(inputProject, DeepCopyExpr(arg))
		}
	}
	inputTag := builder.genNewBindTag()
	if builder.syntheticNDVCols == nil {
		builder.syntheticNDVCols = make(map[[2]int32]struct{}, groupCount)
	}
	for i := 0; i < groupCount-1; i++ {
		builder.syntheticNDVCols[[2]int32{inputTag, int32(i)}] = struct{}{}
	}
	inputProject = append(inputProject, MakePlan2BoolConstExprWithType(false))
	setIDPos := int32(len(inputProject))
	inputProject = append(inputProject, MakePlan2Int64ConstExprWithType(0))
	builder.syntheticNDVCols[[2]int32{inputTag, setIDPos}] = struct{}{}
	flattenedFlags := make([]bool, 0, len(coarse)*(groupCount-1))
	for _, branch := range coarse {
		flattenedFlags = append(flattenedFlags, branch.agg.GroupingFlag[:groupCount-1]...)
	}
	expandedID := builder.appendNode(&planpb.Node{
		NodeType:     planpb.Node_PROJECT,
		Children:     []int32{producerID},
		ProjectList:  inputProject,
		BindingTags:  []int32{inputTag},
		GroupingFlag: flattenedFlags,
		ExtraOptions: groupingSetExpandOptionPrefix + strconv.Itoa(len(coarse)),
	}, coarse[0].ctx)

	sharedGroupTag := builder.genNewBindTag()
	sharedAggTag := builder.genNewBindTag()
	sharedGroups := make([]*planpb.Expr, 0, groupCount)
	for i := 0; i < groupCount-1; i++ {
		group := groupingSetCol(inputProject[i].Typ, inputTag, int32(i))
		group.Ndv = first.GroupBy[i].Ndv
		sharedGroups = append(sharedGroups, group)
	}
	sharedGroups = append(sharedGroups, groupingSetCol(
		planpb.Type{Id: int32(types.T_int64), NotNullable: true}, inputTag, setIDPos))
	sharedGroups[groupCount-1].Ndv = float64(len(coarse))
	sharedAggs := DeepCopyExprList(first.AggList)
	for i, agg := range sharedAggs {
		for j, pos := range aggArgPositions[i] {
			agg.GetF().Args[j] = groupingSetCol(inputProject[pos].Typ, inputTag, pos)
		}
	}
	sharedAggID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_AGG,
		Children:    []int32{expandedID},
		GroupBy:     sharedGroups,
		AggList:     sharedAggs,
		BindingTags: []int32{sharedGroupTag, sharedAggTag},
		SpillMem:    first.SpillMem,
	}, coarse[0].ctx)
	if builder.splitGroupingSetCoarseAggs == nil {
		builder.splitGroupingSetCoarseAggs = make(map[*planpb.Node]struct{})
	}
	builder.splitGroupingSetCoarseAggs[builder.qry.Nodes[sharedAggID]] = struct{}{}

	sharedOutputTag := builder.genNewBindTag()
	sharedOutput := make([]*planpb.Expr, 0, groupCount+aggCount+1)
	for i := 0; i < groupCount-1; i++ {
		sharedOutput = append(sharedOutput, groupingSetCol(sharedGroups[i].Typ, sharedGroupTag, int32(i)))
	}
	rolledUpKey := &planpb.Expr{Typ: first.GroupBy[groupCount-1].Typ,
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	rolledUpKey.Typ.NotNullable = false
	sharedOutput = append(sharedOutput, rolledUpKey)
	for i, agg := range sharedAggs {
		sharedOutput = append(sharedOutput, groupingSetCol(agg.Typ, sharedAggTag, int32(i)))
	}
	sharedOutput = append(sharedOutput,
		groupingSetCol(sharedGroups[groupCount-1].Typ, sharedGroupTag, int32(groupCount-1)))
	sharedOutputID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{sharedAggID},
		ProjectList: sharedOutput,
		BindingTags: []int32{sharedOutputTag},
	}, coarse[0].ctx)

	coarseTag := builder.genNewBindTag()
	coarseProject := DeepCopyExprList(builder.qry.Nodes[coarse[0].rootID].ProjectList)
	for i, expr := range coarseProject {
		rewriteGroupingSetExpr(expr, first, sharedOutputTag, groupCount)
		expr.Typ = unionProject[i].Typ
	}
	coarseRootID := builder.appendNode(&planpb.Node{
		NodeType:    planpb.Node_PROJECT,
		Children:    []int32{sharedOutputID},
		ProjectList: coarseProject,
		BindingTags: []int32{coarseTag},
	}, coarse[0].ctx)

	unionRoot.Children = []int32{branches[0].rootID, coarseRootID}
	unionRoot.ProjectList = unionProject
	unionRoot.PhysicalEqualityKeyList = nil
	unionRoot.Stats = nil
	return true
}

// This is a performance admission, not a correctness proof. Single-column NDV
// only supplies an opportunity signal; all grouping/aggregate semantics above
// remain exact even when the estimate is wrong.
func directGroupingSetSplitFits(
	branches []groupingSetBranch,
	producerStats *planpb.Stats,
	outputRowSize float64,
) bool {
	if len(branches) < 4 || producerStats == nil ||
		!finitePositive(producerStats.Outcnt) || !finitePositive(outputRowSize) {
		return false
	}
	finest := branches[0].agg
	if finest == nil || finest.Stats == nil || !finitePositive(finest.Stats.Outcnt) {
		return false
	}
	groupCount := len(finest.GroupBy)
	if len(branches) != groupCount+1 || groupCount < 3 ||
		len(finest.GroupingFlag) != groupCount ||
		finest.Stats.Outcnt < producerStats.Outcnt/2 ||
		math.Min(finest.Stats.Outcnt, producerStats.Outcnt)*outputRowSize < float64(128*mpool.MB) ||
		finest.GroupBy[groupCount-1] == nil || finest.GroupBy[groupCount-1].Ndv < 128 {
		return false
	}
	for branchIndex, branch := range branches {
		if branch.agg == nil || len(branch.agg.GroupingFlag) != groupCount {
			return false
		}
		for keyIndex, active := range branch.agg.GroupingFlag {
			if active != (keyIndex < groupCount-branchIndex) {
				return false
			}
		}
	}
	return true
}

// directGroupingSetOutput recognizes the narrow case where the generated
// UNION ALL does no branch-specific work above the aggregates. Dynamic
// grouping already emits every grouping set, so retaining the UNION would only
// force a large aggregate result through a materialized fanout. The returned
// project is still expressed against the first legacy aggregate and is rebound
// to the shared output after that output exists.
func (builder *QueryBuilder) directGroupingSetOutput(
	nodes []int32,
	branches []groupingSetBranch,
) (directGroupingSetOutput, bool) {
	if len(nodes) < 2 || len(nodes) != len(branches) {
		return directGroupingSetOutput{}, false
	}
	candidates := make(map[int32]bool, len(nodes))
	for _, nodeID := range nodes {
		if candidates[nodeID] {
			return directGroupingSetOutput{}, false
		}
		candidates[nodeID] = true
	}

	unionRootID := int32(-1)
	for nodeID, node := range builder.qry.Nodes {
		if node == nil || node.NodeType != planpb.Node_UNION_ALL {
			continue
		}
		leaves, ok := builder.directGroupingSetUnionLeaves(int32(nodeID), candidates, make(map[int32]bool))
		if !ok || len(leaves) != len(nodes) {
			continue
		}
		leafCounts := make(map[int32]int, len(leaves))
		for _, leafID := range leaves {
			leafCounts[leafID]++
		}
		exactLeaves := true
		for _, candidateID := range nodes {
			if leafCounts[candidateID] != 1 {
				exactLeaves = false
				break
			}
		}
		if !exactLeaves {
			continue
		}
		if unionRootID >= 0 {
			return directGroupingSetOutput{}, false
		}
		unionRootID = int32(nodeID)
	}
	if unionRootID < 0 {
		return directGroupingSetOutput{}, false
	}
	unionRoot := builder.qry.Nodes[unionRootID]
	if len(unionRoot.BindingTags) != 1 || len(unionRoot.ProjectList) == 0 ||
		len(unionRoot.FilterList) != 0 || len(unionRoot.OnList) != 0 ||
		len(unionRoot.OrderBy) != 0 || len(unionRoot.WinSpecList) != 0 ||
		len(unionRoot.BlockFilterList) != 0 ||
		len(unionRoot.RuntimeFilterProbeList) != 0 ||
		len(unionRoot.RuntimeFilterBuildList) != 0 ||
		len(unionRoot.PhysicalEqualityKeyList) != 0 ||
		unionRoot.Limit != nil || unionRoot.Offset != nil || unionRoot.ExtraOptions != "" {
		return directGroupingSetOutput{}, false
	}

	const comparisonTag = int32(math.MinInt32)
	var commonProject []*planpb.Expr
	for i := range branches {
		branchRoot := builder.qry.Nodes[branches[i].rootID]
		if branchRoot == nil || branchRoot.NodeType != planpb.Node_PROJECT ||
			len(branchRoot.Children) != 1 || branchRoot.Children[0] != branches[i].aggID ||
			len(branchRoot.ProjectList) != len(unionRoot.ProjectList) ||
			len(branchRoot.FilterList) != 0 || len(branchRoot.OnList) != 0 ||
			len(branchRoot.OrderBy) != 0 || len(branchRoot.WinSpecList) != 0 ||
			len(branchRoot.BlockFilterList) != 0 ||
			len(branchRoot.RuntimeFilterProbeList) != 0 ||
			len(branchRoot.RuntimeFilterBuildList) != 0 ||
			len(branchRoot.PhysicalEqualityKeyList) != 0 ||
			branchRoot.Limit != nil || branchRoot.Offset != nil || branchRoot.ExtraOptions != "" {
			return directGroupingSetOutput{}, false
		}
		projectList := DeepCopyExprList(branchRoot.ProjectList)
		for pos, expr := range projectList {
			if expr == nil || !exprCanRemoveProject(expr) ||
				!groupingSetExprRewritable(expr, branches[i].agg) {
				return directGroupingSetOutput{}, false
			}
			rewriteGroupingSetExpr(expr, branches[i].agg, comparisonTag, len(branches[i].agg.GroupBy))
			expr.Typ = unionRoot.ProjectList[pos].Typ
		}
		if commonProject == nil {
			commonProject = projectList
			continue
		}
		if len(commonProject) != len(projectList) {
			return directGroupingSetOutput{}, false
		}
		for pos := range commonProject {
			if !proto.Equal(commonProject[pos], projectList[pos]) {
				return directGroupingSetOutput{}, false
			}
		}
	}
	return directGroupingSetOutput{
		unionRootID: unionRootID,
		projectList: DeepCopyExprList(builder.qry.Nodes[branches[0].rootID].ProjectList),
	}, true
}

func (builder *QueryBuilder) directGroupingSetUnionLeaves(
	nodeID int32,
	candidates map[int32]bool,
	seen map[int32]bool,
) ([]int32, bool) {
	if candidates[nodeID] {
		return []int32{nodeID}, true
	}
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) || seen[nodeID] {
		return nil, false
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	if node == nil || node.NodeType != planpb.Node_UNION_ALL || len(node.Children) != 2 {
		return nil, false
	}
	left, ok := builder.directGroupingSetUnionLeaves(node.Children[0], candidates, seen)
	if !ok {
		return nil, false
	}
	right, ok := builder.directGroupingSetUnionLeaves(node.Children[1], candidates, seen)
	if !ok {
		return nil, false
	}
	return append(left, right...), true
}

func (builder *QueryBuilder) subtreeMayExposeGroupingSentinel(nodeID int32, seen map[int32]bool) bool {
	if nodeID < 0 || int(nodeID) >= len(builder.qry.Nodes) {
		return true
	}
	if seen[nodeID] {
		return false
	}
	seen[nodeID] = true
	node := builder.qry.Nodes[nodeID]
	if node == nil {
		return true
	}
	if node.NodeType == planpb.Node_AGG && hasInactiveGroupingColumn(node.GroupingFlag) {
		return true
	}
	if node.NodeType == planpb.Node_PROJECT {
		if _, expandsGroupingSets := DecodeGroupingSetExpandOption(node.ExtraOptions); expandsGroupingSets &&
			hasInactiveGroupingColumn(node.GroupingFlag) {
			return true
		}
	}
	for _, childID := range node.Children {
		if builder.subtreeMayExposeGroupingSentinel(childID, seen) {
			return true
		}
	}
	for _, sourceStep := range node.SourceStep {
		if sourceStep < 0 || int(sourceStep) >= len(builder.qry.Steps) ||
			builder.subtreeMayExposeGroupingSentinel(builder.qry.Steps[sourceStep], seen) {
			return true
		}
	}
	return false
}

// materializedDeclaredRowSize returns a conservative retained-byte estimate for
// a materialized row. Fixed-width values use their vector element width;
// variable-width values include both the Varlena cell and declared payload
// capacity. Missing capacities and unsupported/future types fail closed.
func materializedDeclaredRowSize(rowTypes []planpb.Type) (float64, bool) {
	if len(rowTypes) == 0 {
		return 0, false
	}
	rowSize := float64(0)
	for _, typ := range rowTypes {
		if typ.Id < 0 || typ.Id > int32(^uint8(0)) {
			return 0, false
		}
		oid := types.T(typ.Id)
		valueSize, fixed := fixedTypeRetainedBytes(oid)
		if !fixed {
			if typ.Width <= 0 {
				return 0, false
			}
			payloadSize := float64(typ.Width)
			switch oid {
			case types.T_char, types.T_varchar:
				// SQL width counts characters; UTF-8 can retain four bytes per
				// declared character.
				payloadSize *= 4
			case types.T_array_float32, types.T_array_float64,
				types.T_array_bf16, types.T_array_float16,
				types.T_array_int8, types.T_array_uint8:
				payloadSize *= float64(types.New(oid, typ.Width, typ.Scale).GetArrayElementSize())
			case types.T_json, types.T_blob, types.T_text,
				types.T_binary, types.T_varbinary, types.T_datalink,
				types.T_geometry, types.T_geometry32:
			default:
				return 0, false
			}
			valueSize = float64(types.VarlenaSize) + payloadSize
		}
		if !typ.NotNullable {
			valueSize++
		}
		if !finitePositive(valueSize) || rowSize > math.MaxFloat64-valueSize {
			return 0, false
		}
		rowSize += valueSize
	}
	return rowSize, finitePositive(rowSize)
}

func groupingSetSharingFitsCostAndStorage(
	producerCost float64,
	inputRowSize float64,
	materializedRows float64,
	outputRowSize float64,
	branchCount int,
) bool {
	if branchCount < 2 || !finitePositive(producerCost) ||
		!finitePositive(inputRowSize) || !finitePositive(materializedRows) ||
		!finitePositive(outputRowSize) ||
		outputRowSize > float64(materialized.MaxSpillBatchBytes)/2 {
		return false
	}
	materializedBytes := materializedRows * outputRowSize
	if !finitePositive(materializedBytes) ||
		materializedBytes > groupingSetEstimatedSpillBytesLimit {
		return false
	}
	savedProducerWork := producerCost * inputRowSize * float64(branchCount-1)
	materializedTraffic := materializedBytes * float64(branchCount+1) *
		groupingSetCostSafetyFactor
	return finitePositive(savedProducerWork) && finitePositive(materializedTraffic) &&
		savedProducerWork > materializedTraffic
}

func groupingSetMaterializedRowsForAdmission(
	producerRows float64,
	aggregateRows float64,
	branchCount int,
) (float64, bool) {
	if branchCount < 2 || !finitePositive(producerRows) || !finitePositive(aggregateRows) {
		return 0, false
	}
	structuralCeiling := producerRows * float64(branchCount)
	inflatedEstimate := aggregateRows * groupingSetCardinalitySafetyFactor
	if !finitePositive(structuralCeiling) || !finitePositive(inflatedEstimate) {
		return 0, false
	}
	return max(aggregateRows, min(structuralCeiling, inflatedEstimate)), true
}

func groupingSetCol(typ planpb.Type, tag, pos int32) *planpb.Expr {
	return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: tag, ColPos: pos}}}
}

func sameGroupingSetAggregateShape(left, right *planpb.Node) bool {
	if left == nil || right == nil || len(left.GroupBy) != len(right.GroupBy) ||
		len(left.GroupingFlag) != len(right.GroupingFlag) || len(left.AggList) != len(right.AggList) {
		return false
	}
	for i := range left.GroupBy {
		if !samePlanType(left.GroupBy[i].Typ, right.GroupBy[i].Typ) {
			return false
		}
	}
	for i := range left.AggList {
		lf, rf := left.AggList[i].GetF(), right.AggList[i].GetF()
		if lf == nil || rf == nil || lf.Func == nil || rf.Func == nil ||
			lf.Func.Obj != rf.Func.Obj || lf.Func.ObjName != rf.Func.ObjName ||
			lf.AggConfigType != rf.AggConfigType || !bytes.Equal(lf.AggConfig, rf.AggConfig) ||
			len(lf.Args) != len(rf.Args) || !samePlanType(left.AggList[i].Typ, right.AggList[i].Typ) {
			return false
		}
		for j := range lf.Args {
			if !samePlanType(lf.Args[j].Typ, rf.Args[j].Typ) {
				return false
			}
		}
	}
	return true
}

func (builder *QueryBuilder) findGroupingSetAggregate(rootID int32, ctx *BindContext) (int32, bool) {
	found := int32(-1)
	seen := make(map[int32]bool)
	var visit func(int32) bool
	visit = func(nodeID int32) bool {
		if seen[nodeID] {
			return true
		}
		seen[nodeID] = true
		node := builder.qry.Nodes[nodeID]
		if node.NodeType == planpb.Node_AGG && len(node.BindingTags) >= 2 &&
			node.BindingTags[0] == ctx.groupTag && node.BindingTags[1] == ctx.aggregateTag {
			if found >= 0 {
				return false
			}
			found = nodeID
		}
		for _, childID := range node.Children {
			if !visit(childID) {
				return false
			}
		}
		return true
	}
	return found, visit(rootID) && found >= 0
}

func (builder *QueryBuilder) countReachableNode(rootID, targetID int32, seen map[int32]bool) int {
	if rootID == targetID {
		return 1
	}
	if seen[rootID] {
		return 0
	}
	seen[rootID] = true
	count := 0
	for _, childID := range builder.qry.Nodes[rootID].Children {
		count += builder.countReachableNode(childID, targetID, seen)
	}
	return count
}

func (builder *QueryBuilder) groupingBranchExpressionsRewritable(rootID, aggID int32, agg *planpb.Node) bool {
	seen := make(map[int32]bool)
	var visit func(int32) bool
	visit = func(nodeID int32) bool {
		if nodeID == aggID || seen[nodeID] {
			return true
		}
		seen[nodeID] = true
		node := builder.qry.Nodes[nodeID]
		for _, expr := range groupingSetNodeExprs(node) {
			if !groupingSetExprRewritable(expr, agg) {
				return false
			}
		}
		for _, order := range node.OrderBy {
			if order != nil && !groupingSetExprRewritable(order.Expr, agg) {
				return false
			}
		}
		for _, childID := range node.Children {
			if !visit(childID) {
				return false
			}
		}
		return true
	}
	return visit(rootID)
}

func groupingSetExprRewritable(expr *planpb.Expr, agg *planpb.Node) bool {
	if expr == nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && fn.Func.ObjName == "grouping" {
			for _, arg := range fn.Args {
				col := arg.GetCol()
				if col == nil || col.RelPos != agg.BindingTags[0] || col.ColPos < 0 || int(col.ColPos) >= len(agg.GroupingFlag) {
					return false
				}
			}
			return true
		}
		for _, arg := range fn.Args {
			if !groupingSetExprRewritable(arg, agg) {
				return false
			}
		}
	}
	if win := expr.GetW(); win != nil {
		if !groupingSetExprRewritable(win.WindowFunc, agg) {
			return false
		}
		for _, part := range win.PartitionBy {
			if !groupingSetExprRewritable(part, agg) {
				return false
			}
		}
		for _, order := range win.OrderBy {
			if order != nil && !groupingSetExprRewritable(order.Expr, agg) {
				return false
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if !groupingSetExprRewritable(item, agg) {
				return false
			}
		}
	}
	return true
}

func groupingSetNodeExprs(node *planpb.Node) []*planpb.Expr {
	result := make([]*planpb.Expr, 0,
		len(node.ProjectList)+len(node.FilterList)+len(node.OnList)+len(node.WinSpecList)+4)
	result = append(result, node.ProjectList...)
	result = append(result, node.FilterList...)
	result = append(result, node.OnList...)
	result = append(result, node.WinSpecList...)
	result = append(result, node.Limit, node.Offset, node.Interval, node.Sliding)
	return result
}

func (builder *QueryBuilder) rewriteGroupingBranch(
	rootID, aggID, replacementID int32,
	agg *planpb.Node,
	scanTag int32,
	groupCount int,
) {
	seen := make(map[int32]bool)
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID == aggID || seen[nodeID] {
			return
		}
		seen[nodeID] = true
		node := builder.qry.Nodes[nodeID]
		for _, expr := range groupingSetNodeExprs(node) {
			rewriteGroupingSetExpr(expr, agg, scanTag, groupCount)
		}
		for _, order := range node.OrderBy {
			if order != nil {
				rewriteGroupingSetExpr(order.Expr, agg, scanTag, groupCount)
			}
		}
		for i, childID := range node.Children {
			if childID == aggID {
				node.Children[i] = replacementID
				continue
			}
			visit(childID)
		}
	}
	visit(rootID)
}

func rewriteGroupingSetExpr(expr *planpb.Expr, agg *planpb.Node, scanTag int32, groupCount int) {
	if expr == nil {
		return
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && fn.Func.ObjName == "grouping" {
			value := int64(0)
			for i := 0; i < len(fn.Args); i++ {
				value <<= 1
				col := fn.Args[i].GetCol()
				if col != nil && !agg.GroupingFlag[col.ColPos] {
					value++
				}
			}
			*expr = *MakePlan2Int64ConstExprWithType(value)
			return
		}
		for _, arg := range fn.Args {
			rewriteGroupingSetExpr(arg, agg, scanTag, groupCount)
		}
	}
	if col := expr.GetCol(); col != nil {
		switch col.RelPos {
		case agg.BindingTags[0]:
			col.RelPos = scanTag
		case agg.BindingTags[1]:
			col.RelPos = scanTag
			col.ColPos += int32(groupCount)
		}
	}
	if win := expr.GetW(); win != nil {
		rewriteGroupingSetExpr(win.WindowFunc, agg, scanTag, groupCount)
		for _, part := range win.PartitionBy {
			rewriteGroupingSetExpr(part, agg, scanTag, groupCount)
		}
		for _, order := range win.OrderBy {
			if order != nil {
				rewriteGroupingSetExpr(order.Expr, agg, scanTag, groupCount)
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			rewriteGroupingSetExpr(item, agg, scanTag, groupCount)
		}
	}
}
