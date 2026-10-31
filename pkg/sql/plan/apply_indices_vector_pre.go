// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// vectorPreFilterInput is the index search a PRE plan restricts and the scan it reads.
type vectorPreFilterInput struct {
	ctx       *BindContext
	scanNode  *plan.Node
	colRefCnt map[[2]int32]int
	idxColMap map[[2]int32]*plan.Expr
	pkPos     int32
	pkType    plan.Type
	// tableFuncTag and tableFuncNodeID are the index search scan; candidateNodeID is it
	// or the APPLY over it.
	tableFuncTag    int32
	tableFuncNodeID int32
	candidateNodeID int32
	// producerFilters, bound to scanNode, are added to the copied scan's filters: the
	// filters the index search evaluates, which narrow the membership key set.
	producerFilters []*plan.Expr
	// trimProducerFilters removes the filters of the copied scan, bound to tag, that the
	// index candidates imply; nil keeps every filter.
	trimProducerFilters func(filters []*plan.Expr, tag int32) []*plan.Expr
	// annotateRequiredDomain describes the index search scan once its membership runtime
	// filter is placed; requiredDomain reports MustApply. Nil does nothing.
	annotateRequiredDomain func(tableFuncNode *plan.Node, requiredDomain bool) error
}

// vectorPreFilterReplaySafe reports whether the PRE plan may evaluate the scan's
// filters and the membership join twice: in the membership producer and on the
// row-fetch side. A sequence or volatile expression is evaluated once.
func (builder *QueryBuilder) vectorPreFilterReplaySafe(vecCtx *vectorSortContext) bool {
	root := vecCtx.scanNode.NodeId
	if vecCtx.hasMembership {
		root = vecCtx.membershipNodeID
	}
	return builder.adaptiveVectorReplaySafe(root)
}

// membershipJoinBuildable reports whether the membership join of vecCtx is a SEMI join
// whose left child is the scan, the shape buildVectorPreFilterJoin copies.
func (builder *QueryBuilder) membershipJoinBuildable(vecCtx *vectorSortContext) bool {
	node := builder.qry.Nodes[vecCtx.membershipNodeID]
	return node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_SEMI &&
		len(node.Children) == 2 && node.Children[0] == vecCtx.scanNode.NodeId
}

// buildVectorPreFilterJoin builds the PRE plan of an index search:
//
//	JOIN(scan, SEMI(index search, PROJECT pk(copied filtered scan [JOIN membership])))
//
// with a membership runtime filter from the copied scan's primary keys to the index search.
// It returns the outer join, or false when the plan cannot be built and the caller keeps
// its original plan.
func (builder *QueryBuilder) buildVectorPreFilterJoin(vecCtx *vectorSortContext, in vectorPreFilterInput) (int32, bool, error) {
	scanNode := in.scanNode
	tableFuncNode := builder.qry.Nodes[in.tableFuncNodeID]
	rowFetchFilters, membershipOnlyFilters := builder.ivfMembershipRowFetchFilters(scanNode)
	// secondScanNode: copy original scanNode for JOIN(ivf, table)
	secondScanNodeID := builder.copyNode(in.ctx, scanNode.NodeId)
	secondScanNode := builder.qry.Nodes[secondScanNodeID]
	oldTag := secondScanNode.BindingTags[0]
	builder.rebindScanNode(secondScanNode)
	newTag := secondScanNode.BindingTags[0]
	for _, expr := range in.producerFilters {
		producerFilter := DeepCopyExpr(expr)
		replaceColRefTag(producerFilter, oldTag, newTag)
		secondScanNode.FilterList = append(secondScanNode.FilterList, producerFilter)
	}

	// The copied scan is an internal producer. Keep its regular-index remaps local so
	// they cannot affect the original row-fetch side, while preserving any mappings
	// already published for the original scan tag.
	secondIdxColMap := make(map[[2]int32]*plan.Expr)
	if oldTag != newTag {
		for key, value := range in.idxColMap {
			if key[0] == oldTag {
				secondIdxColMap[[2]int32{newTag, key[1]}] = DeepCopyExpr(value)
			}
		}
	}

	var originalMembershipNode *plan.Node
	if vecCtx.hasMembership {
		if !builder.membershipJoinBuildable(vecCtx) {
			return 0, false, nil
		}
		originalMembershipNode = builder.qry.Nodes[vecCtx.membershipNodeID]
	}

	if builder.canApplyRegularIndex(secondScanNode) {
		// Membership is enforced before candidate Top-K. Only predicates implied
		// by the index candidates may be removed from its producer.
		if in.trimProducerFilters != nil {
			secondScanNode.FilterList = in.trimProducerFilters(secondScanNode.FilterList, newTag)
		}

		// Retained predicates must participate in column-use accounting.
		secondColRefCnt := make(map[[2]int32]int)
		secondColRefCnt[[2]int32{newTag, in.pkPos}] = 1
		for _, expr := range secondScanNode.FilterList {
			extractColRefs(expr, newTag, secondColRefCnt)
		}
		// A covering-index decision must account for every left-side column that
		// the copied membership JOIN will consume after this scan is optimized.
		if originalMembershipNode != nil {
			for _, expr := range originalMembershipNode.OnList {
				rebound := DeepCopyExpr(expr)
				replaceColRefTag(rebound, oldTag, newTag)
				extractColRefs(rebound, newTag, secondColRefCnt)
			}
		}
		optimizedSecondScanID, err := builder.applyIndicesForFilters(
			secondScanNodeID, secondScanNode, secondColRefCnt, secondIdxColMap)
		if err != nil {
			return 0, false, err
		}
		secondScanNodeID = optimizedSecondScanID
	}

	// Otherwise the runtime filter will only see the truncated primary key set, causing data loss.
	// Clear candidate limits only from the copied indexed-table side. A membership subquery may
	// have its own semantic LIMIT/OFFSET, which must remain intact.
	clearLimitOffsetInSubtree(builder.qry, secondScanNodeID)

	membershipProducerID := secondScanNodeID
	if originalMembershipNode != nil {
		membershipNode := DeepCopyNode(originalMembershipNode)
		membershipNode.Children[0] = secondScanNodeID
		membershipNode.Limit = nil
		membershipNode.Offset = nil
		for _, expr := range membershipNode.OnList {
			replaceColRefTag(expr, oldTag, newTag)
		}
		// An index-only rewrite replaces the copied scan tag with hidden-index
		// output expressions. Rebind every membership expression to that output.
		replaceColumnsForNode(membershipNode, secondIdxColMap)
		membershipProducerID = builder.appendNode(membershipNode, in.ctx)
	}

	// Add a PROJECT above the filtered relation to output only the primary key column.
	secondProjectTag := builder.genNewBindTag()
	secondPkExpr := builder.buildPkExprFromNode(membershipProducerID, in.pkType, scanNode.TableDef.Pkey.PkeyColName)
	if secondPkExpr == nil {
		// If an optimized second-scan subtree can't provide a stable PK expression,
		// skip IVF rewrite to avoid wiring stale bindings into join/runtime-filter paths.
		return 0, false, nil
	}
	secondProjectNodeID := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{membershipProducerID},
		ProjectList: []*plan.Expr{secondPkExpr},
		BindingTags: []int32{secondProjectTag},
	}, in.ctx)

	// membership SEMI join: (vector scan SEMI JOIN second table project)
	innerJoinOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: in.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: in.tableFuncTag,
					ColPos: 0, // tf.pkid
				},
			},
		},
		{
			Typ: in.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: secondProjectTag,
					ColPos: 0, // only pk column from second scan
				},
			},
		},
	})

	innerJoinNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{in.candidateNodeID, secondProjectNodeID},
		// The filtered relation is only a membership producer; none of its
		// columns escape this node. SEMI is both the exact relational shape
		// and an optimizer barrier that prevents the runtime-filter producer
		// from being flattened into a three-way INNER join with its consumer.
		JoinType: plan.Node_SEMI,
		OnList:   []*Expr{innerJoinOn},
		// Don't set Limit/Offset on JOIN - they should be applied after SORT
	}, in.ctx)

	// Construct BloomFilter type runtime filter for inner join + table function
	rfTag := builder.genNewMsgTag()

	// build side: primary key from secondScanNode (consistent with BloomFilter build column)
	buildExpr := &plan.Expr{
		Typ: in.pkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: secondProjectTag,
				ColPos: 0,
			},
		},
	}
	buildSpec := MakeRuntimeFilter(rfTag, false, 0, buildExpr, false)
	buildSpec.UseMembershipFilter = true
	requiredDomain := vecCtx.hasMembership || in.candidateNodeID == in.tableFuncNodeID
	buildSpec.MustApply = requiredDomain
	innerJoinNode := builder.qry.Nodes[innerJoinNodeID]
	innerJoinNode.RuntimeFilterBuildList = []*plan.RuntimeFilterSpec{buildSpec}

	// probe side: pkid column from table function
	probeExpr := &plan.Expr{
		Typ: in.pkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: in.tableFuncTag,
				ColPos: 0,
			},
		},
	}
	probeSpec := MakeRuntimeFilter(rfTag, false, 0, probeExpr, false)
	probeSpec.UseMembershipFilter = true
	probeSpec.MustApply = requiredDomain
	tableFuncNode.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{probeSpec}
	// The final placement proof decides whether every CN can receive the
	// complete domain. Unproved shapes retain the local execution restriction.
	tableFuncNode.Stats.ForceOneCN = true
	if in.annotateRequiredDomain != nil {
		if err := in.annotateRequiredDomain(tableFuncNode, requiredDomain); err != nil {
			return 0, false, err
		}
	}

	// The original scan was guarded during the recursive planner pass so the vector rewrite
	// could see the raw table scan shape. Once the IVF subtree is constructed, we can
	// temporarily suspend that protection and apply regular secondary-index optimization
	// to the row-fetch side of the outer join.
	outerScanNodeID := scanNode.NodeId
	if builder.canApplyRegularIndex(scanNode) {
		var err error
		builder.withSuspendedScanProtection(scanNode.NodeId, func() {
			outerScanNodeID, err = builder.applyIndicesForFilters(scanNode.NodeId, scanNode, in.colRefCnt, in.idxColMap)
		})
		if err != nil {
			return 0, false, err
		}
	}

	outerPkExpr := builder.buildPkExprFromNode(outerScanNodeID, in.pkType, scanNode.TableDef.Pkey.PkeyColName)
	if outerPkExpr == nil && outerScanNodeID != scanNode.NodeId {
		// If a future regular-index rewrite produces an unsupported subtree shape,
		// fall back to the original scan instead of wiring stale bindings into the IVF join.
		logutil.Debugf("IVF outer PK fallback: optimized node %d -> original scan %d", outerScanNodeID, scanNode.NodeId)
		outerScanNodeID = scanNode.NodeId
		outerPkExpr = builder.buildPkExprFromNode(outerScanNodeID, in.pkType, scanNode.TableDef.Pkey.PkeyColName)
	}
	if outerPkExpr == nil {
		return 0, false, nil
	}

	// outer join: optimized outer subtree JOIN (inner ivf join)
	outerOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		DeepCopyExpr(outerPkExpr),
		{
			Typ: in.pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: in.tableFuncTag, // tf pkid from inner join subtree
					ColPos: 0,
				},
			},
		},
	})

	outerJoinNodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{outerScanNodeID, innerJoinNodeID},
		JoinType: plan.Node_INNER,
		OnList:   []*Expr{outerOn},
		// Don't set Limit/Offset on JOIN - they should be applied after SORT
	}, in.ctx)

	// Every surviving PK passed the complete predicate on the copied scan.
	// Commit this only after both exact PK joins have been built, so a declined
	// rewrite retains its MATCH filters. The membership producer keeps them.
	if len(membershipOnlyFilters) > 0 {
		scanNode.FilterList = rowFetchFilters
		if in.colRefCnt != nil {
			increaseRefCntForExprList(membershipOnlyFilters, -1, in.colRefCnt)
		}
	}

	// Outer join doesn't add extra project, let global column pruning optimizer handle it
	return outerJoinNodeID, true, nil
}
