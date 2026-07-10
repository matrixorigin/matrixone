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

// This file is the bm25 ranked-retrieval counterpart of apply_indices_fulltext.go.
// bm25 shares the MATCH(...) AGAINST(...) query surface with classic fulltext but is
// a first-class, position-free index type with its own storage/metadata hidden tables
// and its own bm25_search TVF. To keep the two decoupled, the MATCH->JOIN planner
// machinery is duplicated here (rather than shared) and stripped of everything bm25
// does not need: no fulltext_index_scan branch, no fulltext_match_score projection
// fallback, and only the ranked bag-of-words modes (default / natural language) — the
// position-free index rejects boolean/phrase/query-expansion.
package plan

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const bm25_search_func_name = "bm25_search"

// buildBm25SearchTableFunc builds the bm25_search TVF AST for a MATCH resolved
// to a bm25 ranked-retrieval index. Args: [param="", cfg{db,index,metadata}
// JSON, pattern]. The TVF tokenizes the pattern as bag-of-words and answers a
// BM25 top-K walk, emitting (doc_id, score) — the same shape fulltext_index_scan
// emits, so the downstream join/projection/limit is unchanged.
func (builder *QueryBuilder) buildBm25SearchTableFunc(scanNode *plan.Node, idxdef *plan.IndexDef, pattern string, mode int64, aliasName string) (*tree.AliasedTableExpr, error) {
	// bm25 is a position-free bag-of-words BM25 index: it implements only ranked
	// retrieval (DEFAULT / NATURAL LANGUAGE / RETRIEVAL). Boolean (+/-/~/phrase)
	// and query-expansion need term positions bm25 does not store — reject them
	// with a clear message rather than silently returning bag-of-words results.
	switch mode {
	case int64(tree.FULLTEXT_DEFAULT), int64(tree.FULLTEXT_NL):
		// ranked retrieval — supported (both tokenize the pattern as bag-of-words)
	default:
		return nil, moerr.NewNotSupported(builder.GetContext(),
			"a bm25 index only supports ranked retrieval (default or natural language mode); boolean and query-expansion need a classic fulltext index")
	}

	storeTbl, metaTbl, ok := builder.findBm25IndexTables(scanNode, idxdef)
	if !ok {
		return nil, moerr.NewInternalErrorf(builder.GetContext(),
			"bm25 index %q: storage/metadata tables not found (index may be partially materialized); reindex required",
			idxdef.IndexName)
	}

	// json.Marshal so a schema name containing a double-quote/backslash is escaped
	// (the search side sonic.Unmarshal's it).
	cfgBytes, err := json.Marshal(map[string]string{
		"db":       scanNode.ObjRef.SchemaName,
		"index":    storeTbl,
		"metadata": metaTbl,
	})
	if err != nil {
		return nil, err
	}
	cfg := string(cfgBytes)

	bm25Func := tree.NewCStr(bm25_search_func_name, 1)
	var exprs tree.Exprs
	exprs = append(exprs, tree.NewNumVal[string]("", "", false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal[string](cfg, cfg, false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal[string](pattern, pattern, false, tree.P_char))
	name := tree.NewUnresolvedName(bm25Func)

	return &tree.AliasedTableExpr{
		Expr: &tree.TableFunction{
			Func: &tree.FuncExpr{
				Func:     tree.FuncName2ResolvableFunctionReference(name),
				FuncName: bm25Func,
				Exprs:    exprs,
				Type:     tree.FUNC_TYPE_TABLE,
			},
		},
		As: tree.AliasClause{Alias: tree.Identifier(aliasName)},
	}, nil
}

// findBm25IndexTables resolves the storage + metadata hidden tables of a bm25
// index — the two sibling defs sharing the storage def's IndexName. ok is false
// if either is missing (partial/restored catalog).
func (builder *QueryBuilder) findBm25IndexTables(scanNode *plan.Node, idxdef *plan.IndexDef) (storeTbl string, metaTbl string, ok bool) {
	if scanNode == nil || scanNode.TableDef == nil || idxdef == nil {
		return "", "", false
	}
	for _, idx := range scanNode.TableDef.Indexes {
		if idx == nil || idx.IndexName != idxdef.IndexName {
			continue
		}
		switch idx.IndexAlgoTableType {
		case catalog.Bm25Index_TblType_Storage:
			storeTbl = idx.IndexTableName
		case catalog.Bm25Index_TblType_Metadata:
			metaTbl = idx.IndexTableName
		}
	}
	return storeTbl, metaTbl, storeTbl != "" && metaTbl != ""
}

// applyIndicesForProjectionUsingBm25Index rewrites MATCH(...) AGAINST(...) over a
// bm25 index into: bm25_search TVF chain INNER JOINed to the source on pk, a SORT by
// score DESC (with limit pushdown), and the project's MATCH replaced by the score
// ColRef. Mirrors applyIndicesForProjectionUsingFullTextIndex for the bm25 path.
func (builder *QueryBuilder) applyIndicesForProjectionUsingBm25Index(nodeID int32, projNode *plan.Node, sortNode *plan.Node, scanNode *plan.Node,
	filterids []int32, filterIndexDefs []*plan.IndexDef, projids []int32, projIndexDef []*plan.IndexDef,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {

	ctx := builder.ctxByNode[nodeID]

	// check equal bm25 match func and only compute once for equal function()
	eqmap := builder.findEqualMatchFunc(projNode, scanNode, projids, filterids)

	idxID, filter_node_ids, proj_node_ids, err := builder.applyJoinBm25Indices(nodeID, projNode, scanNode,
		filterids, filterIndexDefs, projids, projIndexDef, eqmap, colRefCnt, idxColMap)
	if err != nil {
		return -1, err
	}

	if sortNode != nil {
		sortNode.Children[0] = idxID

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
				// duplicate bm25 match() found and skip it
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
			Limit:    DeepCopyExpr(scanNode.Limit),
			Offset:   DeepCopyExpr(scanNode.Offset),
			SpillMem: builder.sortSpillMem,
		}, ctx)

		// move scanNode.Limit to sortNode
		scanNode.Limit = nil
		scanNode.Offset = nil

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

// applyIndicesForAggUsingBm25Index handles `select count(*) ... where MATCH(...)` over
// a bm25 index: the bm25_search chain is INNER JOINed to the source and fed to the agg.
func (builder *QueryBuilder) applyIndicesForAggUsingBm25Index(nodeID int32, projNode *plan.Node, aggNode *plan.Node, scanNode *plan.Node,
	filterids []int32, filterIndexDefs []*plan.IndexDef,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, error) {
	var err error

	projids := make([]int32, 0)
	projIndexDefs := make([]*plan.IndexDef, 0)

	eqmap := make(map[int32]int32)

	idxID, _, _, err := builder.applyJoinBm25Indices(nodeID, projNode, scanNode,
		filterids, filterIndexDefs, projids, projIndexDefs, eqmap, colRefCnt, idxColMap)
	if err != nil {
		return -1, err
	}

	aggNode.Children[0] = idxID

	return nodeID, nil
}

// applyJoinBm25Indices builds the bm25_search TVF chain for each MATCH, joins them to
// the source on pk (with optional bloom-filter pre-filter pushdown when other filters
// remain), and pushes down the limit. bm25 counterpart of applyJoinFullTextIndices,
// with the fulltext_index_scan branch removed — every MATCH here routes to bm25_search.
func (builder *QueryBuilder) applyJoinBm25Indices(nodeID int32, projNode *plan.Node, scanNode *plan.Node,
	filterids []int32, filter_indexDefs []*plan.IndexDef,
	projids []int32, proj_indexDefs []*plan.IndexDef, eqmap map[int32]int32,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, []int32, []int32, error) {

	ctx := builder.ctxByNode[nodeID]

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	var pkType = scanNode.TableDef.Cols[pkPos].Typ

	var ret_filter_node_ids = make([]int32, len(filterids))
	var ret_proj_node_ids = make([]int32, len(projids))

	indexDefs := make([]*plan.IndexDef, 0)
	// copy filters and then delete the bm25 match from scanNode.FilterList
	ft_filters := make([]*plan.Expr, 0)
	for _, id := range filterids {
		ft_filters = append(ft_filters, scanNode.FilterList[id])
	}

	// remove the bm25 match filter from TABLE_SCAN
	for i := len(filterids) - 1; i >= 0; i-- {
		ftid := filterids[i]
		scanNode.FilterList = append(scanNode.FilterList[:ftid], scanNode.FilterList[ftid+1:]...)
	}

	// apply pushdown limit when no filter
	var limitExpr *plan.Expr
	if len(scanNode.FilterList) == 0 && scanNode.Limit != nil {
		limitExpr = scanNode.Limit
	}

	indexDefs = append(indexDefs, filter_indexDefs...)

	// Check Equal bm25 match function
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

	// build bm25_search TVF for each MATCH
	var last_node_id int32
	var last_ftnode_pkcol *Expr

	for i := 0; i < len(ft_filters); i++ {
		ftidxscan := ft_filters[i]
		idxdef := indexDefs[i]
		fn := ftidxscan.GetF()
		pattern := fn.Args[0].GetLit().GetSval()
		mode := fn.Args[1].GetLit().GetI64Val()

		alias_name := fmt.Sprintf("mo_bm25_alias_%d", i)
		if projNode == nil {
			alias_name = fmt.Sprintf("mo_bm25_alias_%d_%d", scanNode.NodeId, i)
		}

		tmpTableFunc, berr := builder.buildBm25SearchTableFunc(scanNode, idxdef, pattern, mode, alias_name)
		if berr != nil {
			return -1, nil, nil, berr
		}

		curr_ftnode_id, err := builder.buildTable(tmpTableFunc, ctx, -1, nil)
		if err != nil {
			return -1, nil, nil, err
		}

		// save the created bm25 node to either filter or projection
		// check equal bm25 match() and return node id to correct project position
		if i < proj_offset {
			v, ok := ret_proj_node_ids_map[int32(i)]
			if ok {
				// equal bm25 match() in proj and filter
				ret_proj_node_ids[v] = curr_ftnode_id
			}
			ret_filter_node_ids[i] = curr_ftnode_id
		} else {
			v, ok := ret_proj_node_ids_map[int32(i)]
			if ok {
				// remaining bm25 match() should have its position from reverse_eqmap too
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

	// Determine join structure based on whether scanNode still has non-bm25 filters.
	// When filters remain, use pre-filter pushdown (nested JOIN + runtime filter)
	// to reduce the number of doc_ids that bm25_search must process.
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

	return joinnodeID, ret_filter_node_ids, ret_proj_node_ids, nil
}

func (builder *QueryBuilder) scanHasMatchedBm25Filter(node *plan.Node) bool {
	filterids, _ := builder.getBm25MatchFiltersFromScanNode(node)
	return len(filterids) > 0
}

func (builder *QueryBuilder) applyBm25FiltersForJoinChildren(nodeID int32, joinNode *plan.Node,
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

		newNodeID, ok, err := builder.applyBm25FiltersForScanInJoin(nodeID, child, colRefCnt, idxColMap)
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

func (builder *QueryBuilder) applyBm25FiltersForScanInJoin(nodeID int32, scanNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, bool, error) {
	filterids, filterIndexDefs := builder.getBm25MatchFiltersFromScanNode(scanNode)
	if len(filterids) == 0 {
		return scanNode.NodeId, false, nil
	}

	ctxNodeID := builder.matchRewriteContextNodeID(nodeID, scanNode)
	newNodeID, _, _, err := builder.applyJoinBm25Indices(
		ctxNodeID,
		nil,
		scanNode,
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

	return newNodeID, true, nil
}

// findMatchBm25Index resolves a MATCH(...) function to a bm25 storage index def on the
// scan's columns. It returns non-nil ONLY for the ranked bag-of-words modes bm25
// implements (default / natural language). For boolean and query-expansion it returns
// nil so that, when a column also carries a classic fulltext index (coexistence), the
// planner falls through to the fulltext path which implements those modes.
func (builder *QueryBuilder) findMatchBm25Index(fn *plan.Function, scanNode *plan.Node) *plan.IndexDef {
	if fn == nil || scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return nil
	}
	if len(fn.Args) < 3 || fn.Args[0].GetLit() == nil || fn.Args[1].GetLit() == nil {
		return nil
	}
	if scanNode.TableDef.Pkey == nil || scanNode.TableDef.Pkey.PkeyColName == "" {
		return nil
	}

	// Only the ranked bag-of-words modes route to bm25; other modes are left for a
	// classic fulltext index (buildBm25SearchTableFunc also enforces this as a hard
	// error for a bm25-only column with no fulltext fallback).
	mode := fn.Args[1].GetLit().GetI64Val()
	switch mode {
	case int64(tree.FULLTEXT_DEFAULT), int64(tree.FULLTEXT_NL):
	default:
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
		if idx == nil || !idx.TableExist {
			continue
		}
		// Match the bm25 storage def as the single representative — the metadata
		// sibling is resolved later in buildBm25SearchTableFunc.
		if idx.GetIndexAlgo() != catalog.MoIndexBm25Algo.ToString() ||
			idx.IndexAlgoTableType != catalog.Bm25Index_TblType_Storage {
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

// getBm25MatchFiltersFromScanNode returns the fulltext_match() filters in a scan node
// that resolve to a bm25 index (and their storage defs).
func (builder *QueryBuilder) getBm25MatchFiltersFromScanNode(node *plan.Node) ([]int32, []*plan.IndexDef) {

	filterids := make([]int32, 0)
	ftidxs := make([]*plan.IndexDef, 0)

	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		switch fn.Func.ObjName {
		case "fulltext_match":

			idx := builder.findMatchBm25Index(fn, node)
			if idx != nil {
				ftidxs = append(ftidxs, idx)
				filterids = append(filterids, int32(i))
			}
		default:
		}
	}

	return filterids, ftidxs
}

// getBm25MatchFromProject returns the fulltext_match() projections that resolve to a
// bm25 index. Unlike the fulltext path it does NOT rewrite non-matching MATCH
// projections into fulltext_match_score — a MATCH that is not a bm25 match is left
// untouched so the fulltext path can claim it (coexistence / classic-only columns).
func (builder *QueryBuilder) getBm25MatchFromProject(projNode *plan.Node, scanNode *plan.Node) ([]int32, []*plan.IndexDef) {
	projids := make([]int32, 0)
	ftidxs := make([]*plan.IndexDef, 0)

	for i, expr := range projNode.ProjectList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		switch fn.Func.ObjName {
		case "fulltext_match":

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
