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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

const (
	InFilterCardLimitNonPK   = 10000
	InFilterCardLimitPK      = 1000000
	BloomFilterCardLimit     = 100 * InFilterCardLimitNonPK
	InFilterSelectivityLimit = 0.3
)

func GetInFilterCardLimit(sid string) int32 {
	v, ok := runtime.ServiceRuntime(sid).GetGlobalVariables("runtime_filter_limit_in")
	if ok {
		return int32(v.(int64))
	}
	return InFilterCardLimitNonPK
}

func GetInFilterCardLimitOnPK(
	sid string,
	tableCnt float64,
) int32 {
	upper := tableCnt * InFilterSelectivityLimit
	if upper > InFilterCardLimitPK {
		upper = InFilterCardLimitPK
	}
	lower := float64(GetInFilterCardLimit(sid))
	if upper < lower {
		upper = lower
	}
	return int32(upper)
}

func mustRuntimeFilter(node *plan.Node) bool {
	switch node.JoinType {
	case plan.Node_INDEX:
		return true

	case plan.Node_DEDUP:
		return !node.IsRightJoin
	}
	return false
}

// runtimeFilterJoinPolicy is the semantic and delivery contract for a
// broadcast-join runtime filter.  RuntimeFilterSpec describes the filter
// payload, but it does not describe which side may be discarded or where the
// payload can be delivered.  Keep those decisions together so candidate
// generation and physical placement cannot drift apart.
type runtimeFilterJoinPolicy struct {
	eligible              bool
	requiresLocalDelivery bool
}

func analyzeRuntimeFilterJoinPolicy(node *plan.Node) runtimeFilterJoinPolicy {
	if node == nil || node.NodeType != plan.Node_JOIN {
		return runtimeFilterJoinPolicy{}
	}

	switch node.JoinType {
	case plan.Node_LEFT, plan.Node_OUTER, plan.Node_MARK:
		return runtimeFilterJoinPolicy{}

	case plan.Node_SINGLE:
		// A left SINGLE preserves its physical probe side.  Filtering that side
		// would remove the rows which must produce the scalar-subquery NULL for
		// a missing match.  After right conversion and the physical child swap,
		// child 0 is discardable and child 1 remains the preserved side.
		return runtimeFilterJoinPolicy{
			eligible:              node.IsRightJoin,
			requiresLocalDelivery: node.IsRightJoin,
		}

	case plan.Node_ANTI:
		return runtimeFilterJoinPolicy{
			eligible:              node.IsRightJoin,
			requiresLocalDelivery: node.IsRightJoin,
		}

	case plan.Node_DEDUP:
		return runtimeFilterJoinPolicy{eligible: !node.IsRightJoin}

	case plan.Node_RIGHT:
		return runtimeFilterJoinPolicy{eligible: true, requiresLocalDelivery: true}

	case plan.Node_SEMI:
		return runtimeFilterJoinPolicy{eligible: true, requiresLocalDelivery: node.IsRightJoin}

	case plan.Node_INDEX:
		return runtimeFilterJoinPolicy{eligible: true, requiresLocalDelivery: true}

	default:
		return runtimeFilterJoinPolicy{eligible: true}
	}
}

func (builder *QueryBuilder) canSatisfyRuntimeFilterDelivery(node *plan.Node, policy runtimeFilterJoinPolicy) bool {
	if node.JoinType != plan.Node_SINGLE || !policy.requiresLocalDelivery {
		return true
	}
	// forceOneCN is an optimizer diagnostic override which suppresses the
	// automatic placement pass.  A current-CN-only runtime filter must not be
	// generated when that placement guarantee has been disabled.
	return builder.optimizerHints == nil || builder.optimizerHints.forceOneCN == 0
}

func rightSingleLocalDeliveryIsSafe(node, build *plan.Node, upperLimit int32) bool {
	if node.JoinType != plan.Node_SINGLE {
		return true
	}
	if upperLimit <= 0 || build == nil || build.NodeType != plan.Node_TABLE_SCAN || build.Stats == nil {
		return false
	}
	// DefaultStats is an unavailable-statistics sentinel, not evidence that the
	// complete build contains 1,000 rows.  Treating it as an exact upper bound
	// can serialize an arbitrarily large scan only for hashbuild to send PASS.
	if IsDefaultStats(build.Stats) {
		return false
	}
	// LOCAL_COLOCATED applies to the whole preserved/build subtree. Phase 1
	// accepts only a direct scan whose full table cardinality and filtered
	// output both fit exact IN. Looking only at Outcnt would incorrectly force a
	// scan/aggregate of a large input onto one CN merely because its final output
	// was estimated to be small.
	limit := float64(upperLimit)
	return build.Stats.Outcnt <= limit && build.Stats.TableCnt <= limit
}

func localProtocolEnablesVersionedExactKeyContract(sid string) bool {
	// MOProtocolVersion is a service-local compatibility gate written by the
	// deployment control plane. This helper does not discover peers or infer
	// their capabilities. Deployment orchestration is responsible for raising
	// participating services consistently after rollout, and lowering them
	// before rollback introduces an older producer.
	rt := runtime.ServiceRuntime(sid)
	if rt == nil {
		return false
	}
	value, ok := rt.GetGlobalVariables(runtime.MOProtocolVersion)
	if !ok {
		return false
	}
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion7
}

func (builder *QueryBuilder) exactRuntimeFilterPlanEncoding(
	probeType, buildType types.Type,
	matchPrefix bool,
) (keycodec.ExactRuntimeFilterEncoding, bool) {
	encoding := keycodec.ExactRuntimeFilterEncodingForPair(probeType, buildType)
	if encoding == keycodec.ExactRuntimeFilterUnsupported {
		return encoding, false
	}
	if (encoding != keycodec.ExactRuntimeFilterRaw ||
		!keycodec.LegacyExactRawProducerSafe(buildType.Oid)) &&
		!localProtocolEnablesVersionedExactKeyContract(
			builder.compCtx.GetProcess().GetService(),
		) {
		// Only metadata-independent raw types can keep their exact filters
		// below v7. Metadata-dependent raw contracts and contracts which promise
		// a new closure remain disabled until every producer understands their
		// versioned metadata. Guarded BuildExpr still makes an unexpected older
		// producer fail open.
		return keycodec.ExactRuntimeFilterUnsupported, false
	}
	filterFunction := function.InFunctionName
	if matchPrefix {
		filterFunction = function.PrefixInFunctionName
	}
	if _, err := function.GetFunctionByName(
		builder.GetContext(), filterFunction, []types.Type{probeType, probeType}); err != nil {
		return keycodec.ExactRuntimeFilterUnsupported, false
	}
	return encoding, true
}

// makeExactRuntimeFilterPair is the single construction boundary for vector-
// backed exact runtime filters. Both specs must be published together, and
// unsupported contracts must leave no consumer dependency, placement
// constraint, or optimistic selectivity behind.
func (builder *QueryBuilder) makeExactRuntimeFilterPair(
	tag int32,
	matchPrefix bool,
	upperLimit int32,
	probeExpr, buildExpr *plan.Expr,
	notOnPk bool,
) (probeSpec, buildSpec *plan.RuntimeFilterSpec, ok bool) {
	if probeExpr == nil || probeExpr.GetCol() == nil ||
		buildExpr == nil || buildExpr.GetCol() == nil ||
		buildExpr.GetCol().ColPos < 0 {
		// Scan consumers require a column probe. Function/composite build
		// payloads require a separate codec-and-component contract; validating
		// only their final VARCHAR type would admit false negatives for decimal
		// scale, floating signed zero, and other non-raw equality domains.
		return nil, nil, false
	}
	encoding, ok := builder.exactRuntimeFilterPlanEncoding(
		makeTypeByPlan2Expr(probeExpr),
		makeTypeByPlan2Expr(buildExpr),
		matchPrefix,
	)
	if !ok {
		return nil, nil, false
	}

	probeSpec = MakeRuntimeFilter(
		tag, matchPrefix, 0, DeepCopyExpr(probeExpr), notOnPk)
	buildSpec = MakeRuntimeFilter(
		tag, matchPrefix, upperLimit, DeepCopyExpr(buildExpr), notOnPk)
	buildSpec.BuildExpr = DeepCopyExpr(buildSpec.Expr)
	buildSpec.ProbeType = DeepCopyType(&probeExpr.Typ)
	switch encoding {
	case keycodec.ExactRuntimeFilterRaw:
		// Metadata-independent RAW_V1 needs no producer-side transformation.
		// Retain the legacy expression only for those types so an older
		// HashBuild/IndexBuild can keep its established filter during rolling
		// upgrade. Decimal scale requires the versioned triangle and therefore
		// deliberately makes an older producer PASS.
		if !keycodec.LegacyExactRawProducerSafe(
			types.T(buildExpr.Typ.Id),
		) {
			buildSpec.Expr = nil
		}
		buildSpec.KeyEncoding = plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1
	case keycodec.ExactRuntimeFilterFloatZeroClosed:
		// An older producer cannot close signed zero. Hide the expression it
		// understands so it publishes PASS rather than unsafe raw bytes.
		buildSpec.Expr = nil
		buildSpec.KeyEncoding = plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1
	default:
		return nil, nil, false
	}
	return probeSpec, buildSpec, true
}

func (builder *QueryBuilder) exactRuntimeFilterPairContractValid(
	probeSpec, buildSpec *plan.RuntimeFilterSpec,
) bool {
	if probeSpec == nil || buildSpec == nil ||
		probeSpec.Expr == nil || probeSpec.BuildExpr != nil ||
		buildSpec.BuildExpr == nil ||
		buildSpec.ProbeType == nil ||
		probeSpec.Tag != buildSpec.Tag ||
		probeSpec.MatchPrefix != buildSpec.MatchPrefix ||
		probeSpec.NotOnPk != buildSpec.NotOnPk ||
		buildSpec.BuildExpr.GetCol() == nil ||
		buildSpec.BuildExpr.GetCol().ColPos != 0 ||
		len(buildSpec.KeyComponentProbeTypes) != 0 {
		return false
	}
	probeExprType := makeTypeByPlan2Expr(probeSpec.Expr)
	advertisedProbeType := makeTypeByPlan2Type(
		*buildSpec.ProbeType)
	if probeExprType != advertisedProbeType {
		return false
	}
	encoding, ok := builder.exactRuntimeFilterPlanEncoding(
		advertisedProbeType,
		makeTypeByPlan2Expr(buildSpec.BuildExpr),
		buildSpec.MatchPrefix,
	)
	if !ok {
		return false
	}
	switch encoding {
	case keycodec.ExactRuntimeFilterRaw:
		if buildSpec.KeyEncoding !=
			plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_RAW_V1 {
			return false
		}
		if keycodec.LegacyExactRawProducerSafe(
			types.T(buildSpec.BuildExpr.Typ.Id),
		) {
			return buildSpec.Expr != nil &&
				exprStructuralEqual(buildSpec.Expr, buildSpec.BuildExpr)
		}
		return buildSpec.Expr == nil
	case keycodec.ExactRuntimeFilterFloatZeroClosed:
		return buildSpec.KeyEncoding ==
			plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_FLOAT_ZERO_CLOSED_V1 &&
			buildSpec.Expr == nil
	default:
		return false
	}
}

// makeSerializedExactRuntimeFilterPair constructs the component contract for
// an index tuple key. finalProbeExpr is the encoded index-table column;
// componentProbeExprs are the original indexed columns in encoder order; and
// buildExpr must be serial/serial_full over the corresponding build slots.
//
// A tuple encoder is safe only when every component is raw-compatible. Float
// signed-zero closure cannot be expressed by adding one final tuple value
// without a combinatorial expansion, so float components remain unsupported.
func (builder *QueryBuilder) makeSerializedExactRuntimeFilterPair(
	tag int32,
	matchPrefix bool,
	upperLimit int32,
	finalProbeExpr, buildExpr *plan.Expr,
	componentProbeExprs []*plan.Expr,
	notOnPk bool,
) (probeSpec, buildSpec *plan.RuntimeFilterSpec, ok bool) {
	if !localProtocolEnablesVersionedExactKeyContract(
		builder.compCtx.GetProcess().GetService(),
	) || finalProbeExpr == nil || finalProbeExpr.GetCol() == nil ||
		buildExpr == nil || len(componentProbeExprs) == 0 ||
		types.T(finalProbeExpr.Typ.Id) != types.T_varchar ||
		types.T(buildExpr.Typ.Id) != types.T_varchar {
		return nil, nil, false
	}
	fn := buildExpr.GetF()
	if fn == nil || fn.Func == nil ||
		len(fn.Args) != len(componentProbeExprs) {
		return nil, nil, false
	}
	encodingMarker := plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_UNSPECIFIED
	switch fn.Func.ObjName {
	case function.SerialFunctionName:
		if matchPrefix ||
			fn.Func.Obj != function.SerialFunctionEncodeID {
			return nil, nil, false
		}
		encodingMarker =
			plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_V1
	case function.SerialFullFunctionName:
		if !matchPrefix ||
			fn.Func.Obj != function.SerialFullFunctionEncodeID {
			return nil, nil, false
		}
		encodingMarker =
			plan.RuntimeFilterKeyEncoding_RUNTIME_FILTER_KEY_SERIAL_FULL_V1
	default:
		return nil, nil, false
	}

	finalEncoding, ok := builder.exactRuntimeFilterPlanEncoding(
		makeTypeByPlan2Expr(finalProbeExpr),
		makeTypeByPlan2Expr(buildExpr),
		matchPrefix,
	)
	if !ok || finalEncoding != keycodec.ExactRuntimeFilterRaw {
		return nil, nil, false
	}

	componentProbeTypes := make([]plan.Type, len(componentProbeExprs))
	for i := range componentProbeExprs {
		probeComponent := componentProbeExprs[i]
		buildComponent := fn.Args[i]
		if probeComponent == nil || probeComponent.GetCol() == nil ||
			buildComponent == nil || buildComponent.GetCol() == nil ||
			buildComponent.GetCol().ColPos < 0 ||
			!function.SerialTypeSupported(
				types.T(probeComponent.Typ.Id),
			) ||
			!function.SerialTypeSupported(
				types.T(buildComponent.Typ.Id),
			) ||
			keycodec.ExactRuntimeFilterEncodingForPair(
				makeTypeByPlan2Expr(probeComponent),
				makeTypeByPlan2Expr(buildComponent),
			) != keycodec.ExactRuntimeFilterRaw {
			return nil, nil, false
		}
		componentProbeTypes[i] = *DeepCopyType(&probeComponent.Typ)
	}

	probeSpec = MakeRuntimeFilter(
		tag, matchPrefix, 0, DeepCopyExpr(finalProbeExpr), notOnPk)
	buildSpec = MakeRuntimeFilter(
		tag, matchPrefix, upperLimit, DeepCopyExpr(buildExpr), notOnPk)
	buildSpec.BuildExpr = buildSpec.Expr
	buildSpec.Expr = nil
	buildSpec.ProbeType = DeepCopyType(&finalProbeExpr.Typ)
	buildSpec.KeyComponentProbeTypes = componentProbeTypes
	buildSpec.KeyEncoding = encodingMarker
	return probeSpec, buildSpec, true
}

func (builder *QueryBuilder) generateRuntimeFilters(nodeID int32) {
	node := builder.qry.Nodes[nodeID]
	sid := builder.compCtx.GetProcess().GetService()

	for _, childID := range node.Children {
		builder.generateRuntimeFilters(childID)
	}

	if node.NodeType == plan.Node_FUZZY_FILTER {
		builder.finalizeFuzzyRuntimeFilter(node)
		return
	}

	if builder.isMasterIndexInnerJoin(node) {
		return
	}

	// Build runtime filters only for broadcast join
	if node.NodeType != plan.Node_JOIN {
		return
	}

	// if this node has already pushed runtime filter, just return
	if len(node.RuntimeFilterBuildList) > 0 {
		return
	}

	if node.Stats.HashmapStats.Shuffle {
		rfTag := builder.genNewMsgTag()
		node.RuntimeFilterProbeList = append(node.RuntimeFilterProbeList, MakeRuntimeFilter(rfTag, false, 0, nil, false))
		node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, MakeRuntimeFilter(rfTag, false, 0, nil, false))
		return
	}

	policy := analyzeRuntimeFilterJoinPolicy(node)
	if !policy.eligible || !builder.canSatisfyRuntimeFilterDelivery(node, policy) {
		return
	}
	if node.JoinType == plan.Node_SINGLE && builder.optimizerHints != nil && builder.optimizerHints.disableRightSingleRF != 0 {
		return
	}

	leftChild := builder.qry.Nodes[node.Children[0]]

	// TODO: build runtime filters deeper than 1 level
	if leftChild.NodeType != plan.Node_TABLE_SCAN || leftChild.Limit != nil || leftChild.Offset != nil {
		return
	}

	rightChild := builder.qry.Nodes[node.Children[1]]
	if !mustRuntimeFilter(node) && rightChild.Stats.Outcnt > 5000000 {
		return
	}
	if node.Stats.HashmapStats.HashOnPK && rightChild.Stats.Outcnt > 320000 {
		return
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
	}

	var probeExprs, buildExprs []*plan.Expr

	for _, expr := range node.OnList {
		if isEquiCond(expr, leftTags, rightTags) {
			args := expr.GetF().Args
			if !ExprIsZonemappable(builder.GetContext(), args[0]) {
				return
			}
			probeExprs = append(probeExprs, args[0])
			buildExprs = append(buildExprs, args[1])

		}
	}

	// No equi condition found
	if probeExprs == nil {
		return
	}

	for i := range probeExprs {
		probeType := makeTypeByPlan2Expr(probeExprs[i])
		buildType := makeTypeByPlan2Expr(buildExprs[i])
		// Exact runtime-filter payloads eventually reach consumers which compare
		// physical bytes (notably persistent Bloom filters). Generate one only
		// when the producer can close every SQL-equal physical representation
		// for both operands.
		if _, ok := builder.exactRuntimeFilterPlanEncoding(probeType, buildType, false); !ok {
			return
		}
	}

	// HashBuild currently falls back to PASS for function/composite exact
	// payloads. Do not publish a dependency, alter scan placement, or reduce
	// statistics for a filter which cannot be materialized.
	if len(probeExprs) == 1 {
		convertToCPKey := false
		tableDef := leftChild.TableDef
		if tableDef == nil || tableDef.Pkey == nil {
			return
		}
		probeCol := probeExprs[0].GetCol()
		if probeCol == nil {
			return
		}
		sortOrder := GetSortOrder(tableDef, probeCol.ColPos)
		// LOCAL_COLOCATED gives up multi-CN scan bandwidth.  In phase 1 only
		// enable right-SINGLE on the leading PK/cluster key, where exact IN can
		// prune ranges predictably.  A scattered non-key filter may still scan
		// nearly every block and would be a performance regression on one CN.
		if node.JoinType == plan.Node_SINGLE && policy.requiresLocalDelivery && sortOrder != 0 {
			return
		}
		if node.JoinType != plan.Node_INDEX {
			probeNdv := getExprNdv(probeExprs[0], builder)
			if probeNdv <= 1 {
				//maybe not flushed yet, set at least 100 to continue calculation
				probeNdv = 100
			}
			if node.Stats.HashmapStats.HashmapSize/probeNdv >= 0.1 {
				return
			}
			if sortOrder != 0 {
				if node.Stats.HashmapStats.HashmapSize/probeNdv >= 0.1*probeNdv/leftChild.Stats.TableCnt {
					return
				}
			} else {
				if len(tableDef.Pkey.Names) > 1 && probeCol.Name != catalog.CPrimaryKeyColName {
					convertToCPKey = true
				}
			}
			//todo: need to fix this in the future
			//if probeCol.Name != tableDef.Pkey.PkeyColName && builder.getColOverlap(probeCol) > overlapThreshold {
			//	return
			//}
		}

		if builder.optimizerHints != nil && builder.optimizerHints.runtimeFilter != 0 && !mustRuntimeFilter(node) {
			return
		}

		notOnPk := probeCol.Name != tableDef.Pkey.PkeyColName
		inLimit := GetInFilterCardLimit(sid)
		if sortOrder == 0 {
			inLimit = GetInFilterCardLimitOnPK(sid, leftChild.Stats.TableCnt)
		}
		// Placement is decided before hashbuild knows the actual number of
		// unique keys.  If the planner already knows the build cannot fit in an
		// exact IN, hashbuild would send PASS after both scans were forced onto
		// one CN.  Reject that no-benefit topology up front for right-SINGLE.
		if !rightSingleLocalDeliveryIsSafe(node, rightChild, inLimit) {
			return
		}
		if convertToCPKey {
			return
		}

		buildExpr := &plan.Expr{
			Typ: buildExprs[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -1,
					ColPos: 0,
				},
			},
		}
		rfTag := builder.genNewMsgTag()
		probeSpec, buildSpec, ok := builder.makeExactRuntimeFilterPair(
			rfTag, false, inLimit, probeExprs[0], buildExpr, notOnPk)
		if !ok {
			return
		}
		leftChild.RuntimeFilterProbeList = append(leftChild.RuntimeFilterProbeList, probeSpec)
		node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, buildSpec)
		// SINGLE output cardinality belongs to its semantic preserved side.  Do
		// not feed the existing probe-side RF heuristic into SINGLE stats here;
		// preserved-side cardinality and cost selection are a separate phase.
		if node.JoinType != plan.Node_SINGLE {
			recalcStatsByRuntimeFilter(leftChild, node, builder)
		}
		return
	}
}

// finalizeFuzzyRuntimeFilter makes build-side selection, delivery, and
// selectivity one planner decision. The candidate is constructed with the
// fuzzy node, but no stats or placement are changed until final costs are
// available here. Compile treats a surviving versioned pair as the explicit
// build-on-sink decision.
func (builder *QueryBuilder) finalizeFuzzyRuntimeFilter(node *plan.Node) {
	if node == nil ||
		node.FuzzyBuildSide !=
			plan.Node_FUZZY_BUILD_SIDE_UNSPECIFIED {
		// The explicit decision is the idempotency boundary. DML planning can
		// traverse the same step more than once, after the first traversal has
		// already rewritten scan statistics.
		return
	}
	if len(node.RuntimeFilterBuildList) == 0 {
		return
	}
	clearPair := func(scan *plan.Node) {
		node.RuntimeFilterBuildList = nil
		if scan != nil {
			scan.RuntimeFilterProbeList = nil
		}
	}
	if len(node.Children) != 2 {
		clearPair(nil)
		return
	}
	tableScan := builder.qry.Nodes[node.Children[0]]
	sinkScan := builder.qry.Nodes[node.Children[1]]
	if tableScan == nil || sinkScan == nil ||
		tableScan.NodeType != plan.Node_TABLE_SCAN ||
		tableScan.Stats == nil || sinkScan.Stats == nil ||
		len(tableScan.RuntimeFilterProbeList) != 1 ||
		len(node.RuntimeFilterBuildList) != 1 ||
		!builder.exactRuntimeFilterPairContractValid(
			tableScan.RuntimeFilterProbeList[0],
			node.RuntimeFilterBuildList[0],
		) {
		clearPair(tableScan)
		return
	}
	if !localProtocolEnablesVersionedExactKeyContract(
		builder.compCtx.GetProcess().GetService(),
	) {
		// RuntimeFilterSpec is a new FuzzyFilter pipeline field. An older CN
		// drops it entirely and therefore cannot publish terminal PASS. Remove
		// both ends before any placement/stats side effect while such a peer may
		// participate.
		clearPair(tableScan)
		return
	}

	if safeRatio(tableScan.Stats.Cost, sinkScan.Stats.Cost, 1) < 0.3 {
		// Build-on-table has no supported delivery direction. Leave the
		// original scan statistics and multi-CN placement untouched.
		node.FuzzyBuildSide = plan.Node_FUZZY_BUILD_SIDE_TABLE
		clearPair(tableScan)
		return
	}

	node.FuzzyBuildSide = plan.Node_FUZZY_BUILD_SIDE_SINK
	tableScan.Stats.ForceOneCN = true
	recalcStatsByRuntimeFilter(tableScan, node, builder)
}

func (builder *QueryBuilder) isMasterIndexInnerJoin(node *plan.Node) bool {
	// In Master Index, INNER Joins in the query plan should not have runtime filters, as it sets
	// input rows to 0 for right child, which is not expected.
	// https://github.com/matrixorigin/matrixone/issues/14876#issuecomment-2148824892
	if !(node.JoinType == plan.Node_INNER && len(node.Children) == 2) {
		return false
	}

	leftChild := builder.qry.Nodes[node.Children[0]]
	rightChild := builder.qry.Nodes[node.Children[1]]

	if leftChild.TableDef == nil || leftChild.TableDef.Cols == nil || len(leftChild.TableDef.Cols) != 3 {
		return false
	}

	if rightChild.TableDef == nil || rightChild.TableDef.Cols == nil || len(rightChild.TableDef.Cols) != 3 {
		return false
	}

	// In Master Index, both the children are from the same master index table.
	if leftChild.TableDef.Name != rightChild.TableDef.Name {
		return false
	}

	// Check if left child is a master/secondary index table
	//TODO: verify if Cols will contain  __mo_cpkey
	for _, column := range leftChild.TableDef.Cols {
		if column.Name == catalog.MasterIndexTablePrimaryColName {
			continue
		}
		if column.Name == catalog.MasterIndexTableIndexColName {
			continue
		}
		if column.Name == catalog.Row_ID {
			continue
		}
		return false
	}

	// Check if right child is a master/secondary index table
	for _, column := range rightChild.TableDef.Cols {
		if column.Name == catalog.MasterIndexTablePrimaryColName {
			continue
		}
		if column.Name == catalog.MasterIndexTableIndexColName {
			continue
		}
		if column.Name == catalog.Row_ID {
			continue
		}
		return false
	}

	return true

}
