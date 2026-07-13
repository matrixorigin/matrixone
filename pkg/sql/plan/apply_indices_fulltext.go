// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package plan

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// The idea is as follows:
// 1. Find fulltext_match() function from projection (projNode) and filters (ScanNode)
// and then convert fulltext_match() to table function fulltext_index_scan().
// 2. Do INNER JOIN fulltext_index_scan tables and source table with key doc_id
// 3. Add SORT node with score key DESC if SORT node does not exist.  If SORT node exists,
// Add the JOIN node to SORT node.
// 4. Replace the fulltext_match() in project list with ColRef score in fulltext_index_scan()
//
// explain  select *, match(body, title) against('red')  from src where match(body) against('red');
// +------------------------------------------------------------------------------------------+
// | TP QURERY PLAN                                                                           |
// +------------------------------------------------------------------------------------------+
// | Project                                                                                  |
// |   ->  Sort                                                                               |
// |         Sort Key: mo_fulltext_alias_0.score DESC, mo_fulltext_alias_1.score DESC         |
// |         ->  Join                                                                         |
// |               Join Type: INNER                                                           |
// |               Join Cond: (src.doc_id = mo_fulltext_alias_1.doc_id)                       |
// |               ->  Table Scan on eric.src                                                 |
// |               ->  Join                                                                   |
// |                     Join Type: INNER                                                     |
// |                     Join Cond: (mo_fulltext_alias_1.doc_id = mo_fulltext_alias_0.doc_id) |
// |                     ->  Table Function on fulltext_index_scan                            |
// |                           ->  Values Scan "*VALUES*"                                     |
// |                     ->  Table Function on fulltext_index_scan                            |
// |                           ->  Values Scan "*VALUES*"                                     |
// +------------------------------------------------------------------------------------------+
func (builder *QueryBuilder) applyIndicesForProjectionUsingFullTextIndex(nodeID int32, projNode *plan.Node, sortNode *plan.Node, scanNode *plan.Node,
	filterids []int32, filterIndexDefs []*plan.IndexDef, projids []int32, projIndexDef []*plan.IndexDef,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	ctx := builder.ctxByNode[nodeID]

	// check equal fulltext_match func and only compute once for equal function()
	eqmap := builder.findEqualMatchFunc(projNode, scanNode, projids, filterids)
	var paginationLimit, paginationOffset *plan.Expr
	if projNode.Limit != nil || projNode.Offset != nil {
		paginationLimit, paginationOffset = projNode.Limit, projNode.Offset
	}
	if scanNode.Limit != nil || scanNode.Offset != nil {
		paginationLimit, paginationOffset = scanNode.Limit, scanNode.Offset
	}
	if sortNode != nil && (sortNode.Limit != nil || sortNode.Offset != nil) {
		paginationLimit, paginationOffset = sortNode.Limit, sortNode.Offset
	}
	internalLimit, internalOffset := paginationLimit, paginationOffset
	if sortNode != nil {
		// The fulltext function returns relevance order. It is not safe to
		// truncate that stream before an explicit sort on another expression.
		internalLimit, internalOffset = nil, nil
	}

	idxID, filter_node_ids, proj_node_ids, err := builder.applyJoinFullTextIndices(nodeID, projNode, scanNode,
		internalLimit, internalOffset, filterids, filterIndexDefs, projids, projIndexDef, eqmap, colRefCnt, idxColMap)
	if err != nil {
		return -1, err
	}
	// Pagination belongs to the result of the base-table/fulltext join, never
	// to the base scan and fulltext stream independently. Both inputs are
	// unordered, so paginating them separately can select disjoint subsets.
	scanNode.Limit = nil
	scanNode.Offset = nil

	if sortNode != nil {
		sortNode.Children[0] = idxID
		sortNode.Limit = DeepCopyExpr(paginationLimit)
		sortNode.Offset = DeepCopyExpr(paginationOffset)

	} else {

		// create sort node with order by score DESC

		var orderByScore []*OrderBySpec
		for _, id := range filter_node_ids {
			ftnode := builder.qry.Nodes[id]
			orderByScore = append(orderByScore, &OrderBySpec{
				Expr: &Expr{
					Typ: ftnode.TableDef.Cols[1].Typ, // score column
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: ftnode.BindingTags[0],
							ColPos: 1, // score column
						},
					},
				},
				Flag: plan.OrderBySpec_DESC,
			})
		}

		for i, id := range proj_node_ids {
			if _, ok := eqmap[int32(i)]; ok {
				// duplicate fulltext_match() found and skip it
				continue
			}

			ftnode := builder.qry.Nodes[id]
			orderByScore = append(orderByScore, &OrderBySpec{
				Expr: &Expr{
					Typ: ftnode.TableDef.Cols[1].Typ, // score column
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: ftnode.BindingTags[0],
							ColPos: 1, // score column
						},
					},
				},
				Flag: plan.OrderBySpec_DESC,
			})
		}

		sortByID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{idxID},
			OrderBy:  orderByScore,
			Limit:    DeepCopyExpr(paginationLimit),
			Offset:   DeepCopyExpr(paginationOffset),
			SpillMem: builder.sortSpillMem,
		}, ctx)

		projNode.Children[0] = sortByID
	}

	// replace the project with ColRef
	for i, id := range proj_node_ids {
		idx := projids[i]
		ftnode := builder.qry.Nodes[id]
		projNode.ProjectList[idx] = &Expr{
			Typ: ftnode.TableDef.Cols[1].Typ, // score column
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: ftnode.BindingTags[0],
					ColPos: 1, // score column
				},
			},
		}
	}
	return nodeID, nil
}

// mysql> explain select count(*) from src where match(title, body) against('d');
// +----------------------------------------------------------------+
// | TP QURERY PLAN                                                 |
// +----------------------------------------------------------------+
// | Project                                                        |
// |   ->  Aggregate                                                |
// |         Aggregate Functions: starcount(1)                      |
// |         ->  Join                                               |
// |               Join Type: INNER                                 |
// |               Join Cond: (src.id = mo_fulltext_alias_0.doc_id) |
// |               ->  Table Scan on eric.src                       |
// |               ->  Table Function on fulltext_index_scan        |
// |                     ->  Values Scan "*VALUES*"                 |
// +----------------------------------------------------------------+
func (builder *QueryBuilder) applyIndicesForAggUsingFullTextIndex(nodeID int32, projNode *plan.Node, aggNode *plan.Node, scanNode *plan.Node,
	filterids []int32, filterIndexDefs []*plan.IndexDef,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	var err error

	projids := make([]int32, 0)
	projIndexDefs := make([]*plan.IndexDef, 0)

	eqmap := make(map[int32]int32)

	idxID, _, _, err := builder.applyJoinFullTextIndices(nodeID, projNode, scanNode,
		scanNode.Limit, scanNode.Offset, filterids, filterIndexDefs, projids, projIndexDefs, eqmap, colRefCnt, idxColMap)
	if err != nil {
		return -1, err
	}
	joinNode := builder.qry.Nodes[idxID]
	joinNode.Limit = DeepCopyExpr(scanNode.Limit)
	joinNode.Offset = DeepCopyExpr(scanNode.Offset)
	scanNode.Limit = nil
	scanNode.Offset = nil

	aggNode.Children[0] = idxID

	return nodeID, nil
}

func (builder *QueryBuilder) applyJoinFullTextIndices(nodeID int32, projNode *plan.Node, scanNode *plan.Node,
	paginationLimit, paginationOffset *plan.Expr,
	filterids []int32, filter_indexDefs []*plan.IndexDef,
	projids []int32, proj_indexDefs []*plan.IndexDef, eqmap map[int32]int32,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, []int32, []int32, error) {

	ctx := builder.ctxByNode[nodeID]

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	var pkType = scanNode.TableDef.Cols[pkPos].Typ

	var ret_filter_node_ids = make([]int32, len(filterids))
	var ret_proj_node_ids = make([]int32, len(projids))

	indexDefs := make([]*plan.IndexDef, 0)
	// copy filters and then delete the fulltext_match from scanNode.FilterList
	ft_filters := make([]*plan.Expr, 0)
	for _, id := range filterids {
		ft_filters = append(ft_filters, scanNode.FilterList[id])
	}

	// remove the fulltext_match filter from TABLE_SCAN
	for i := len(filterids) - 1; i >= 0; i-- {
		ftid := filterids[i]
		scanNode.FilterList = append(scanNode.FilterList[:ftid], scanNode.FilterList[ftid+1:]...)
	}

	indexDefs = append(indexDefs, filter_indexDefs...)

	// Check Equal fulltext_match function
	ret_proj_node_ids_map := make(map[int32]int32)

	proj_offset := len(filterids)
	for i, id := range projids {

		eqfid, ok := eqmap[int32(i)]
		if ok {
			// equivalent filter found
			ret_proj_node_ids_map[eqfid] = int32(i)
		} else {
			// proj filter not match with filter list so add to ft_filters
			// and append the corresponding ret_proj_node_ids position
			ftlen := len(ft_filters)
			ret_proj_node_ids_map[int32(ftlen)] = int32(i)
			ft_filters = append(ft_filters, projNode.ProjectList[id])
			indexDefs = append(indexDefs, proj_indexDefs[i])
		}
	}

	// A single fulltext stream can safely keep LIMIT+OFFSET candidates. With
	// multiple streams, limiting each input before their intersection can drop
	// documents that belong to the final top page, so leave those inputs
	// unbounded until a joint top-k implementation exists.
	var limitExpr *plan.Expr
	if len(scanNode.FilterList) == 0 && len(ft_filters) == 1 {
		limitExpr, _ = buildCandidateLimit(paginationLimit, paginationOffset)
	}

	// buildFullTextIndexScan
	var last_node_id int32
	var last_ftnode_pkcol *Expr

	for i := 0; i < len(ft_filters); i++ {
		ftidxscan := ft_filters[i]
		idxdef := indexDefs[i]
		fn := ftidxscan.GetF()
		pattern := fn.Args[0].GetLit().GetSval()
		mode := fn.Args[1].GetLit().GetI64Val()

		alias_name := fmt.Sprintf("mo_fulltext_alias_%d", i)
		if projNode == nil {
			alias_name = fmt.Sprintf("mo_fulltext_alias_%d_%d", scanNode.NodeId, i)
		}

		// One unified loop over ALL match predicates on this scan, so a mix of
		// BM25(...) (bm25_match -> bm25_search) and MATCH(...) (fulltext_match ->
		// fulltext_index_scan) on the same query chains into ONE join structure with
		// ONE combined score sort. Dispatch the per-match TVF by the resolved index's
		// algo; both TVFs emit the same (doc_id, score) shape so the rest is identical.
		var tmpTableFunc *tree.AliasedTableExpr
		if idxdef.IndexAlgo == catalog.MoIndexBm25Algo.ToString() {
			var berr error
			tmpTableFunc, berr = builder.buildBm25SearchTableFunc(scanNode, idxdef, pattern, alias_name)
			if berr != nil {
				return -1, nil, nil, berr
			}
		} else {
			idxtblname := fmt.Sprintf("`%s`.`%s`", scanNode.ObjRef.SchemaName, idxdef.IndexTableName)
			srctblname := fmt.Sprintf("`%s`.`%s`", scanNode.ObjRef.SchemaName, scanNode.TableDef.Name)
			fulltext_func := tree.NewCStr(fulltext_index_scan_func_name, 1)
			params := idxdef.IndexAlgoParams

			var exprs tree.Exprs
			exprs = append(exprs, tree.NewNumVal[string](params, params, false, tree.P_char))
			exprs = append(exprs, tree.NewNumVal[string](srctblname, srctblname, false, tree.P_char))
			exprs = append(exprs, tree.NewNumVal[string](idxtblname, idxtblname, false, tree.P_char))
			exprs = append(exprs, tree.NewNumVal[string](pattern, pattern, false, tree.P_char))
			exprs = append(exprs, tree.NewNumVal[int64](mode, strconv.FormatInt(mode, 10), false, tree.P_int64))

			name := tree.NewUnresolvedName(fulltext_func)
			tmpTableFunc = &tree.AliasedTableExpr{
				Expr: &tree.TableFunction{
					Func: &tree.FuncExpr{
						Func:     tree.FuncName2ResolvableFunctionReference(name),
						FuncName: fulltext_func,
						Exprs:    exprs,
						Type:     tree.FUNC_TYPE_TABLE,
					},
				},
				As: tree.AliasClause{
					Alias: tree.Identifier(alias_name),
				},
			}
		}

		curr_ftnode_id, err := builder.buildTable(tmpTableFunc, ctx, -1, nil)
		if err != nil {
			return -1, nil, nil, err
		}

		// save the created fulltext node to either filter or projection
		// check equal fulltext_match() and return node id to correct project position
		if i < proj_offset {
			v, ok := ret_proj_node_ids_map[int32(i)]
			if ok {
				// equal fulltext_match() in proj and filter
				ret_proj_node_ids[v] = curr_ftnode_id
			}
			ret_filter_node_ids[i] = curr_ftnode_id
		} else {
			v, ok := ret_proj_node_ids_map[int32(i)]
			if ok {
				// remaining fulltext_match() should have its position from reverse_eqmap too
				ret_proj_node_ids[v] = curr_ftnode_id

			} else {
				return -1, nil, nil, moerr.NewInternalError(builder.GetContext(), "Invalid ret_proj_node_ids_map")
			}
		}

		curr_ftnode := builder.qry.Nodes[curr_ftnode_id]
		curr_ftnode_tag := curr_ftnode.BindingTags[0]
		curr_ftnode_pkcol := &Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: curr_ftnode_tag, // last idxTbl (may be join) relPos
					ColPos: 0,               // idxTbl.pk
				},
			},
		}

		// pushdown limit
		if limitExpr != nil {
			curr_ftnode.Limit = DeepCopyExpr(limitExpr)
		}

		// change doc_id type to the primary type here
		curr_ftnode.TableDef.Cols[0].Typ.Id = pkType.Id
		curr_ftnode.TableDef.Cols[0].Typ.Width = pkType.Width
		curr_ftnode.TableDef.Cols[0].Typ.Scale = pkType.Scale

		if i > 0 {
			// JOIN last_node_id and curr_ftnode_id
			// JOIN INNER with children (curr_ftnode_id, last_node_id)

			// oncond
			wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
				curr_ftnode_pkcol, last_ftnode_pkcol,
			})

			last_node_id = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				Children: []int32{curr_ftnode_id, last_node_id},
				JoinType: plan.Node_INNER,
				OnList:   []*Expr{wherePkEqPk},
			}, ctx)

		} else {
			last_node_id = curr_ftnode_id
		}
		last_ftnode_pkcol = DeepCopyExpr(curr_ftnode_pkcol)

	}

	// Determine join structure based on whether scanNode still has non-fulltext filters.
	// When filters remain, use pre-filter pushdown (nested JOIN + runtime filter)
	// to reduce the number of doc_ids that fulltext_index_scan must process.
	pushdownEnabled := len(scanNode.FilterList) > 0
	if pushdownEnabled {
		if val, err := builder.compCtx.ResolveVariable("fulltext_bloom_filter_pushdown", true, false); err == nil {
			if v, ok := val.(int8); ok && v == 0 {
				pushdownEnabled = false
			}
		}
	}

	var joinnodeID int32

	if pushdownEnabled {
		// Pre-filter pushdown mode:
		//   outerJoin( scanNode, innerJoin( ft_func_chain, secondScanProject ) )
		//
		// 1) Copy scanNode as secondScanNode with only PK output
		// 2) Inner join ft_func_chain with secondScanProject + BloomFilter runtime filter
		// 3) Outer join original scanNode with inner join result + IN-list runtime filter

		// secondScanNode: copy original scanNode for pre-filter
		secondScanNodeID := builder.copyNode(ctx, scanNode.NodeId)
		secondScanNode := builder.qry.Nodes[secondScanNodeID]
		baseSecondScan := secondScanNode

		oldTag := secondScanNode.BindingTags[0]
		builder.rebindScanNode(secondScanNode)
		newTag := secondScanNode.BindingTags[0]

		if oldTag != newTag {
			for key, value := range colRefCnt {
				if key[0] == oldTag {
					colRefCnt[[2]int32{newTag, key[1]}] = value
				}
			}
			for key, value := range idxColMap {
				if key[0] == oldTag {
					idxColMap[[2]int32{newTag, key[1]}] = DeepCopyExpr(value)
				}
			}
		}

		if builder.canApplyRegularIndex(secondScanNode) {
			secondScanNodeID = builder.applyIndicesForFilters(secondScanNodeID, secondScanNode, colRefCnt, idxColMap)
			secondScanNode = builder.qry.Nodes[secondScanNodeID]
		}

		secondScanNode.Limit = nil
		secondScanNode.Offset = nil

		// PROJECT node above secondScanNode: output only PK column
		secondProjectTag := builder.genNewBindTag()
		secondPkExpr := builder.buildPkExprFromNode(secondScanNodeID, pkType, scanNode.TableDef.Pkey.PkeyColName)
		if secondPkExpr == nil {
			secondPkExpr = &plan.Expr{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: baseSecondScan.BindingTags[0],
						ColPos: pkPos,
						Name:   scanNode.TableDef.Cols[pkPos].Name,
					},
				},
			}
		}
		secondProjectNodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{secondScanNodeID},
			ProjectList: []*plan.Expr{secondPkExpr},
			BindingTags: []int32{secondProjectTag},
		}, ctx)

		// Inner join: (ft_func_chain JOIN secondScanProject)
		innerJoinOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: last_ftnode_pkcol.GetCol().RelPos,
						ColPos: 0,
					},
				},
			},
			{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: secondProjectTag,
						ColPos: 0,
					},
				},
			},
		})

		innerJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{last_node_id, secondProjectNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{innerJoinOn},
		}, ctx)

		// BloomFilter runtime filter: secondScan(build) -> all ft_func(probe)
		innerJoinNode := builder.qry.Nodes[innerJoinNodeID]

		// Collect all unique ft_func node IDs for BF probe
		allFtNodeIDs := make([]int32, 0, len(ret_filter_node_ids)+len(ret_proj_node_ids))
		seen := make(map[int32]bool)
		for _, id := range ret_filter_node_ids {
			if !seen[id] {
				seen[id] = true
				allFtNodeIDs = append(allFtNodeIDs, id)
			}
		}
		for _, id := range ret_proj_node_ids {
			if !seen[id] {
				seen[id] = true
				allFtNodeIDs = append(allFtNodeIDs, id)
			}
		}

		// For each ft_func node, create an independent BF build/probe pair
		for _, ftNodeID := range allFtNodeIDs {
			tag := builder.genNewMsgTag()
			ftNode := builder.qry.Nodes[ftNodeID]

			bExpr := &plan.Expr{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: secondProjectTag,
						ColPos: 0,
					},
				},
			}
			bSpec := MakeRuntimeFilter(tag, false, 0, bExpr, false)
			bSpec.UseMembershipFilter = true
			innerJoinNode.RuntimeFilterBuildList = append(innerJoinNode.RuntimeFilterBuildList, bSpec)

			pExpr := &plan.Expr{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ftNode.BindingTags[0],
						ColPos: 0,
					},
				},
			}
			pSpec := MakeRuntimeFilter(tag, false, 0, pExpr, false)
			pSpec.UseMembershipFilter = true
			ftNode.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{pSpec}
		}

		// Outer join: (scanNode JOIN innerJoin)
		outerOn, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: pkPos,
					},
				},
			},
			{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: last_ftnode_pkcol.GetCol().RelPos,
						ColPos: 0,
					},
				},
			},
		})

		outerJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{scanNode.NodeId, innerJoinNodeID},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{outerOn},
		}, ctx)

		// IN-list runtime filter: innerJoin(build) -> scanNode(probe)
		rfTag2 := builder.genNewMsgTag()

		probeExpr2 := &plan.Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: pkPos,
				},
			},
		}
		probeSpec2 := MakeRuntimeFilter(rfTag2, false, 0, DeepCopyExpr(probeExpr2), false)
		scanNode.RuntimeFilterProbeList = append(scanNode.RuntimeFilterProbeList, probeSpec2)

		buildExpr2 := &plan.Expr{
			Typ: pkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -1,
					ColPos: 0,
				},
			},
		}
		const unlimitedInFilterCard = int32(1<<31 - 1)
		buildSpec2 := MakeRuntimeFilter(rfTag2, false, unlimitedInFilterCard, buildExpr2, false)

		outerJoinNode := builder.qry.Nodes[outerJoinNodeID]
		outerJoinNode.RuntimeFilterBuildList = append(outerJoinNode.RuntimeFilterBuildList, buildSpec2)

		joinnodeID = outerJoinNodeID
	} else {
		// No extra filters: simple JOIN (scanNode, ft_func_chain)
		wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
			{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: scanNode.BindingTags[0],
						ColPos: pkPos, // tbl.pk
					},
				},
			},
			{
				Typ: pkType,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: last_ftnode_pkcol.GetCol().RelPos, // last idxTbl (may be join) relPos
						ColPos: 0,                                 // idxTbl.pk
					},
				},
			},
		})

		joinnodeID = builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{scanNode.NodeId, last_node_id},
			JoinType: plan.Node_INNER,
			OnList:   []*Expr{wherePkEqPk},
		}, ctx)
	}

	// Clear Limit/Offset from scanNode when pushdown is enabled
	// (they should be applied after SORT)
	// Note: caller (applyIndicesForProjectionUsingFullTextIndex) may still need
	// scanNode.Limit to set up the SORT node, so we don't clear it here.
	// The caller is responsible for clearing scanNode.Limit/Offset after using it.

	return joinnodeID, ret_filter_node_ids, ret_proj_node_ids, nil
}

func (builder *QueryBuilder) scanHasMatchedFullTextFilter(node *plan.Node) bool {
	filterids, _ := builder.getFullTextMatchFiltersFromScanNode(node)
	return len(filterids) > 0
}

func (builder *QueryBuilder) applyFullTextFiltersForJoinChildren(nodeID int32, joinNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (bool, error) {
	if joinNode == nil || joinNode.JoinType != plan.Node_INNER {
		return false, nil
	}

	changed := false
	for i, childID := range joinNode.Children {
		child := builder.qry.Nodes[childID]
		if child == nil || child.NodeType != plan.Node_TABLE_SCAN {
			continue
		}

		newNodeID, ok, err := builder.applyFullTextFiltersForScanInJoin(nodeID, child, colRefCnt, idxColMap)
		if err != nil {
			return false, err
		}
		if ok {
			joinNode.Children[i] = newNodeID
			changed = true
		}
	}

	return changed, nil
}

func (builder *QueryBuilder) applyFullTextFiltersForScanInJoin(nodeID int32, scanNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, bool, error) {
	filterids, filterIndexDefs := builder.getFullTextMatchFiltersFromScanNode(scanNode)
	if len(filterids) == 0 {
		return scanNode.NodeId, false, nil
	}

	ctxNodeID := builder.matchRewriteContextNodeID(nodeID, scanNode)
	newNodeID, _, _, err := builder.applyJoinFullTextIndices(
		ctxNodeID,
		nil,
		scanNode,
		scanNode.Limit,
		scanNode.Offset,
		filterids,
		filterIndexDefs,
		nil,
		nil,
		map[int32]int32{},
		colRefCnt,
		idxColMap,
	)
	if err != nil {
		return -1, false, err
	}
	joinNode := builder.qry.Nodes[newNodeID]
	joinNode.Limit = DeepCopyExpr(scanNode.Limit)
	joinNode.Offset = DeepCopyExpr(scanNode.Offset)
	scanNode.Limit = nil
	scanNode.Offset = nil

	return newNodeID, true, nil
}

func (builder *QueryBuilder) findMatchFullTextIndex(fn *plan.Function, scanNode *plan.Node) *plan.IndexDef {
	if fn == nil || scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return nil
	}
	if len(fn.Args) < 3 || fn.Args[0].GetLit() == nil || fn.Args[1].GetLit() == nil {
		return nil
	}
	if scanNode.TableDef.Pkey == nil || scanNode.TableDef.Pkey.PkeyColName == "" {
		return nil
	}

	scanTag := scanNode.BindingTags[0]
	argColNames := make([]string, 0, len(fn.Args)-2)
	for j := 2; j < len(fn.Args); j++ {
		col := fn.Args[j].GetCol()
		if col == nil || col.RelPos != scanTag {
			return nil
		}

		colName := col.Name
		if colName == "" {
			if col.ColPos < 0 || int(col.ColPos) >= len(scanNode.TableDef.Cols) {
				return nil
			}
			colName = scanNode.TableDef.Cols[col.ColPos].Name
		}
		argColNames = append(argColNames, colName)
	}

	nargs := len(fn.Args) - 2
	for _, idx := range scanNode.TableDef.Indexes {
		if idx == nil || !idx.TableExist || !catalog.IsFullTextIndexAlgo(idx.IndexAlgo) {
			continue
		}
		if len(idx.Parts) != nargs {
			continue
		}

		nfound := 0
		for _, p := range idx.Parts {
			partName := catalog.ResolveAlias(p)
			for _, colName := range argColNames {
				if strings.EqualFold(partName, colName) || strings.EqualFold(p, colName) {
					// found
					nfound++
					break
				}
			}
		}

		if nfound == nargs && nfound == len(idx.Parts) {
			return idx
		}
	}
	return nil
}

// Get the match filters in ScanNode — both fulltext_match (classic MATCH) and
// bm25_match (BM25). Collecting both here lets applyJoinFullTextIndices chain a mix
// of the two into one join structure with one combined score sort.
func (builder *QueryBuilder) getFullTextMatchFiltersFromScanNode(node *plan.Node) ([]int32, []*plan.IndexDef) {

	filterids := make([]int32, 0)
	ftidxs := make([]*plan.IndexDef, 0)

	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		var idx *plan.IndexDef
		switch fn.Func.ObjName {
		case "fulltext_match":
			idx = builder.findMatchFullTextIndex(fn, node)
		case "bm25_match":
			idx = builder.findMatchBm25Index(fn, node)
		default:
		}
		if idx != nil {
			ftidxs = append(ftidxs, idx)
			filterids = append(filterids, int32(i))
		}
	}

	return filterids, ftidxs
}

// Get the match projections in ProjectList — both fulltext_match (classic MATCH) and
// bm25_match (BM25). An unmatched classic fulltext_match is rewritten to
// fulltext_match_score (float32); an unmatched bm25_match is left as-is (BM25 requires
// a bm25 index — it errors at execution rather than silently scoring).
func (builder *QueryBuilder) getFullTextMatchFromProject(projNode *plan.Node, scanNode *plan.Node) ([]int32, []*plan.IndexDef) {
	projids := make([]int32, 0)
	ftidxs := make([]*plan.IndexDef, 0)

	for i, expr := range projNode.ProjectList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		switch fn.Func.ObjName {
		case "fulltext_match":

			idx := builder.findMatchFullTextIndex(fn, scanNode)
			if idx != nil {
				ftidxs = append(ftidxs, idx)
				projids = append(projids, int32(i))
			} else {
				// change the fulltext_match function to fulltext_match_score
				// which has return type float32 instead of bool
				projNode.ProjectList[i] = builder.getFullTextMatchScoreExpr(expr)
			}
		case "bm25_match":

			idx := builder.findMatchBm25Index(fn, scanNode)
			if idx != nil {
				ftidxs = append(ftidxs, idx)
				projids = append(projids, int32(i))
			}
		default:
		}
	}

	return projids, ftidxs
}

func (builder *QueryBuilder) getFullTextMatchScoreExpr(expr *plan.Expr) *plan.Expr {
	fn := expr.GetF()

	// get args(exprs) & types
	argsLength := len(fn.Args)
	argsType := make([]types.Type, argsLength)
	for idx, expr := range fn.Args {
		argsType[idx] = makeTypeByPlan2Expr(expr)
	}

	var funcID int64
	var returnType types.Type

	funcname := "fulltext_match_score"

	// get function definition
	fGet, err := function.GetFunctionByName(builder.GetContext(), funcname, argsType)
	if err != nil {
		panic(err)
	}

	funcID = fGet.GetEncodedOverloadID()
	returnType = fGet.GetReturnType()
	exprType := makePlan2Type(&returnType)
	exprType.NotNullable = function.DeduceNotNullable(funcID, fn.Args)
	newExpr := &Expr{
		Typ: exprType,
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcID, funcname),
				Args: fn.Args,
			},
		},
	}

	return newExpr
}

func (builder *QueryBuilder) resolveAggNode(node *plan.Node, depth int32) *plan.Node {
	if depth == 0 {
		if node.NodeType == plan.Node_AGG {
			return node
		}
		return nil
	}

	if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		return builder.resolveAggNode(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}
