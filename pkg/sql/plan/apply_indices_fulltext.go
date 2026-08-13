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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	eqmap := builder.findEqualFullTextMatchFunc(projNode, scanNode, projids, filterids)
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

	// Covered fast path (Phase 6): a fully-covered SELECT over a single fulltext2 index
	// WITH include columns can DROP the base-table JOIN and serve pk/score/include-cols
	// straight from the fulltext2_search TVF. Purely additive: when not covered, falls
	// through to the existing JOIN path below byte-for-byte.
	if handled, herr := builder.tryApplyCoveredFulltext2(nodeID, projNode, sortNode, scanNode,
		filterids, filterIndexDefs, projids, projIndexDef, eqmap,
		paginationLimit, paginationOffset); herr != nil {
		return -1, herr
	} else if handled {
		return nodeID, nil
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

	// The loop above only rewrites projections that ARE a bare fulltext_match, because
	// projids holds the positions getFullTextMatchFromProject recognised. A MATCH WRAPPED by
	// a scalar -- `round(match(...), 3)`, `cast(match(...) as double)`, `match(...) * 100` --
	// is not at one of those positions, so it was left untouched, reached execution and threw
	// the 20105 "cannot be replaced by FULLTEXT INDEX" error even though the index scan had
	// just been built for the accompanying WHERE MATCH.
	//
	// Sweep whatever is left. This is the projection twin of the wrapped-predicate lift in
	// buildFullTextIndexScan, and the same defect the vector side fixed for wrapped distances
	// in #26961 -- which is why the walk (replaceScoreFnInExpr) is shared with it.
	//
	// Single served MATCH only, matching the filter side: with several, choosing WHICH index
	// scan's score a wrapped projection refers to needs argument matching this does not
	// attempt, and guessing would silently report the wrong score.
	if len(filter_node_ids)+len(proj_node_ids) == 1 {
		id := append(append([]int32{}, filter_node_ids...), proj_node_ids...)[0]
		if ftnode := builder.qry.Nodes[id]; ftnode != nil && len(ftnode.BindingTags) > 0 {
			scoreExpr := func() *plan.Expr {
				return &Expr{
					Typ: ftnode.TableDef.Cols[1].Typ, // score column
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{RelPos: ftnode.BindingTags[0], ColPos: 1},
					},
				}
			}
			for i := range projNode.ProjectList {
				projNode.ProjectList[i] = replaceScoreFnInExpr(
					projNode.ProjectList[i], "fulltext_match", scoreExpr)
			}
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

	// Predicates that WRAP a MATCH rather than being one -- `MATCH(...) > 0`, or a filter on a
	// projected score alias that inlining substituted back into its definition -- are invisible
	// to getFullTextMatchFiltersFromScanNode, which only recognises a bare fulltext_match. Left
	// on the scan they survive the rewrite and throw at execution, even though the index scan
	// was built right beside them:
	//
	//	Join
	//	  Table Scan   Filter Cond: (fulltext_match('hello', 0, body) > 0)   <- throws
	//	  Table Function on fulltext_index_scan                              <- index IS used
	//
	// Take them off the scan here; they are re-attached above the join once the score column
	// exists, with the inner MATCH rewritten to reference it. Removing them before the
	// pushdown decision below is deliberate: a predicate needing the score can never be
	// pushed into the index scan, so it must not make the pre-filter path look worthwhile.
	//
	// Same defect the vector side fixed for wrapped distances in #26961; the recursive
	// replacement is shared with it (replaceScoreFnInExpr).
	wrappedMatchFilters := make([]*plan.Expr, 0)
	if len(ft_filters) == 1 {
		// Single served MATCH only. With several, deciding WHICH index scan's score a wrapped
		// predicate refers to needs argument matching this does not attempt; leaving those
		// alone keeps today's behaviour rather than risking the wrong score.
		kept := scanNode.FilterList[:0]
		for _, expr := range scanNode.FilterList {
			if exprCallsFunc(expr, "fulltext_match") {
				wrappedMatchFilters = append(wrappedMatchFilters, expr)
				continue
			}
			kept = append(kept, expr)
		}
		scanNode.FilterList = kept
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

	// fulltext2 INCLUDE/pk prefilter pushdown: when the driving index is a fulltext2 index
	// WITH INCLUDE columns, peel the WHERE predicates on those INCLUDE columns (and the pk)
	// out of scanNode.FilterList into the ivfpq-aligned predicate JSON, which fulltext2_search
	// evaluates against the stored per-doc values inside the WAND walk (bounding the pushed
	// LIMIT to the filtered set). Reusing buildFilterPredicateJSON keeps this correctness-safe:
	// it peels only NUMERIC + pk predicates exactly and leaves varchar/others residual (so a
	// collation-sensitive string filter stays on the existing scan path — no wrong results).
	// Removing the peeled predicates lets the pre-filter second-scan below be skipped when no
	// residual filter remains.
	var ft2PredsJSON string
	if len(indexDefs) > 0 && catalog.IsFullText2IndexAlgo(indexDefs[0].IndexAlgo) {
		if incCols := indexDefIncludedColumnsBestEffort(indexDefs[0]); len(incCols) > 0 {
			pkColName := fulltext2PeelablePkColName(scanNode) // "" for a non-evaluable pk type
			preds, serialized, residual, perr := buildFilterPredicateJSON(scanNode.FilterList, scanNode, incCols, pkColName, true)
			if perr == nil && len(serialized) > 0 {
				ft2PredsJSON = preds
				scanNode.FilterList = residual
			}
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
		idxObjRef, idxTableDef, err := builder.compCtx.ResolveIndexTableByRef(
			scanNode.ObjRef, idxdef.IndexTableName, scanNode.ScanSnapshot)
		if err != nil {
			return -1, nil, nil, err
		}
		if idxObjRef == nil || idxTableDef == nil {
			return -1, nil, nil, moerr.NewInternalErrorf(
				builder.GetContext(), "resolved fulltext index table %q without catalog metadata", idxdef.IndexTableName)
		}

		idxtblname := sqlquote.QualifiedIdent(idxObjRef.SchemaName, idxObjRef.ObjName)
		srctblname := sqlquote.QualifiedIdent(scanNode.ObjRef.SchemaName, scanNode.ObjRef.ObjName)
		fn := ftidxscan.GetF()
		params := idxdef.IndexAlgoParams
		aliasName := fmt.Sprintf("mo_fulltext_alias_%d", i)

		modeLit := fn.Args[1].GetLit()
		if modeLit == nil {
			return -1, nil, nil, moerr.NewInvalidInput(builder.GetContext(), "fulltext search mode must be a constant")
		}
		mode := modeLit.GetI64Val()

		// Dispatch the per-match TVF by the resolved index's algo: fulltext2 ->
		// fulltext2_search, classic fulltext -> fulltext_index_scan. Both emit the same
		// (doc_id, score) shape and take the search pattern as an EXPRESSION (fn.Args[0]),
		// so a prepared-statement '?' pattern flows through either path unchanged. This
		// 2-member dispatch stays inline (not an index-plugin hook) because building the
		// node needs QueryBuilder internals a plugin sub-package must not import.
		var curr_ftnode_id int32
		if catalog.IsFullText2IndexAlgo(idxdef.IndexAlgo) {
			cfg, cfgErr := builder.buildFulltext2SearchCfg(scanNode, idxdef, mode)
			if cfgErr != nil {
				return -1, nil, nil, cfgErr
			}
			exprs := []*plan.Expr{
				makePlan2StringConstExprWithType(cfg),
				DeepCopyExpr(fn.Args[0]), // pattern (may be a bound '?' parameter)
				DeepCopyExpr(fn.Args[1]), // mode (a constant)
			}
			// Attach the peeled INCLUDE/pk predicate JSON to the DRIVING TVF (i==0). With
			// multiple MATCHes the JOIN #1 doc_id intersection propagates the filter, so one
			// filtered stream constrains the whole result — the predicate need only ride the
			// driving stream.
			if i == 0 && ft2PredsJSON != "" {
				exprs = append(exprs, makePlan2StringConstExprWithType(ft2PredsJSON))
			}
			curr_ftnode_id, err = builder.buildFulltext2SearchNode(ctx, exprs, nil)
			if err != nil {
				return -1, nil, nil, err
			}
		} else {
			// A literal pattern is pre-compiled to the index-scan SQL now; a runtime
			// pattern expression (prepared '?') leaves sql empty and compiles at exec.
			sql := ""
			if patternLit := fn.Args[0].GetLit(); patternLit != nil {
				fullTextSQL, sqlErr := builder.getFullTextIndexScanSql(params, idxtblname, patternLit.GetSval(), mode)
				if sqlErr != nil {
					return -1, nil, nil, sqlErr
				}
				sql = fullTextSQL
			}

			exprs := []*plan.Expr{
				makePlan2StringConstExprWithType(srctblname),
				makePlan2StringConstExprWithType(idxtblname),
				DeepCopyExpr(fn.Args[0]),
				DeepCopyExpr(fn.Args[1]),
			}
			curr_ftnode_id, err = builder.buildFullTextIndexScanNode(ctx, exprs, nil, params, sql)
			if err != nil {
				return -1, nil, nil, err
			}
		}
		if scanNode.ObjRef.PubInfo != nil {
			fulltextFunc := builder.qry.Nodes[curr_ftnode_id].TableDef.TblFunc
			fulltextFunc.FulltextSourceRef = DeepCopyObjectRef(scanNode.ObjRef)
			fulltextFunc.FulltextIndexRef = DeepCopyObjectRef(idxObjRef)
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
		for colPos, colDef := range curr_ftnode.TableDef.Cols {
			builder.nameByColRef[[2]int32{curr_ftnode_tag, int32(colPos)}] = aliasName + "." + colDef.Name
		}
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
		curr_ftnode.TableDef.Cols[0].Typ.Charset = pkType.Charset

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
	if pushdownEnabled && types.T(pkType.Id).IsInteger() &&
		!localProtocolEnablesSortedMembershipFilter(
			builder.compCtx.GetProcess().GetService()) {
		pushdownEnabled = false
	}
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
		outerJoinNode := builder.qry.Nodes[outerJoinNodeID]
		probeSpec2, buildSpec2, hasRuntimeFilter := builder.makeExactRuntimeFilterPair(
			rfTag2,
			false,
			unlimitedInFilterCard,
			probeExpr2,
			buildExpr2,
			false,
		)
		if hasRuntimeFilter {
			scanNode.RuntimeFilterProbeList = append(
				scanNode.RuntimeFilterProbeList, probeSpec2)
			outerJoinNode.RuntimeFilterBuildList = append(
				outerJoinNode.RuntimeFilterBuildList, buildSpec2)
		}

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

	// Re-attach the wrapped-MATCH predicates lifted off the scan, now that the index scan's
	// score column exists. They go on the JOIN: the score is produced by the table function, so
	// the predicate cannot be evaluated below the join that brings the two together.
	if len(wrappedMatchFilters) > 0 && len(ret_filter_node_ids) == 1 {
		ftnode := builder.qry.Nodes[ret_filter_node_ids[0]]
		joinNode := builder.qry.Nodes[joinnodeID]
		if ftnode != nil && joinNode != nil && len(ftnode.BindingTags) > 0 {
			scoreExpr := func() *plan.Expr {
				return &plan.Expr{
					Typ: ftnode.TableDef.Cols[1].Typ, // score column
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{RelPos: ftnode.BindingTags[0], ColPos: 1},
					},
				}
			}
			rewritten := make([]*plan.Expr, 0, len(wrappedMatchFilters))
			for _, expr := range wrappedMatchFilters {
				rewritten = append(rewritten,
					replaceScoreFnInExpr(expr, "fulltext_match", scoreExpr))
			}
			// A FILTER node above the join, NOT joinNode.FilterList. Nothing in the planner
			// puts a FilterList on a Node_JOIN and nothing evaluates one: EXPLAIN happily
			// prints `Filter Cond:` for it, so the plan LOOKS right while the predicate is
			// never applied. That silently returned rows the predicate excludes -- strictly
			// worse than the error this lift replaced, and invisible to any test whose
			// threshold every matching row passes (`sc > 0`). Use the node type the executor
			// actually runs.
			joinnodeID = builder.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{joinnodeID},
				FilterList: rewritten,
			}, ctx)
		} else {
			// Could not resolve the score column: put them back rather than drop predicates,
			// which would silently widen the result set.
			scanNode.FilterList = append(scanNode.FilterList, wrappedMatchFilters...)
		}
	} else if len(wrappedMatchFilters) > 0 {
		scanNode.FilterList = append(scanNode.FilterList, wrappedMatchFilters...)
	}

	if err := builder.recordPreparedPluginDependencies(scanNode); err != nil {
		return -1, nil, nil, err
	}
	return joinnodeID, ret_filter_node_ids, ret_proj_node_ids, nil
}

// tryApplyCoveredFulltext2 implements the covered fast path (Phase 6): a fully-covered
// projection over a SINGLE fulltext2 index WITH include columns drops the base-table JOIN
// and reads pk/score/include-cols straight from the fulltext2_search TVF. It returns
// (handled=true) only when EVERY guard holds:
//
//	(a) a single MATCH driving a single fulltext2 index (len(filterids)==1, and any
//	    projection MATCH is the SAME func — i.e. eqmap-equal, no independent extra MATCH);
//	(b) the index is fulltext2 (not classic) with >=1 include column;
//	(c) AFTER peeling numeric/pk predicates into the TVF prefilter JSON, scanNode.FilterList
//	    is EMPTY (no residual varchar/complex predicate left on the base scan);
//	(d) every base-scan column the outer query projects is the pk or an include column
//	    (no non-covered base column; the only MATCH is the one replaced by score).
//
// When handled it builds the include-coldef TVF, keeps the score-DESC SORT above it, and
// remaps the projection (pk -> TVF col 0, MATCH -> score col 1, include col -> col 2+j).
// On any guard miss it returns (false, nil) and mutates NOTHING, so the caller runs the
// existing JOIN path unchanged.
func (builder *QueryBuilder) tryApplyCoveredFulltext2(nodeID int32, projNode, sortNode, scanNode *plan.Node,
	filterids []int32, filterIndexDefs []*plan.IndexDef, projids []int32, projIndexDef []*plan.IndexDef,
	eqmap map[int32]int32, paginationLimit, paginationOffset *plan.Expr) (bool, error) {

	// (a) exactly one MATCH filter, driving one index.
	if len(filterids) != 1 || len(filterIndexDefs) != 1 {
		return false, nil
	}
	// Every projection MATCH must be eqmap-equal to the single filter MATCH — otherwise
	// there is an independent extra MATCH (multi-stream), which the covered path can't serve.
	for i := range projids {
		if _, ok := eqmap[int32(i)]; !ok {
			return false, nil
		}
	}

	idxdef := filterIndexDefs[0]
	// (b) fulltext2 index with >=1 include column.
	if !catalog.IsFullText2IndexAlgo(idxdef.IndexAlgo) {
		return false, nil
	}
	incCols := indexDefIncludedColumnsBestEffort(idxdef)
	if len(incCols) == 0 {
		return false, nil
	}
	if scanNode.TableDef == nil || scanNode.TableDef.Pkey == nil || scanNode.TableDef.Pkey.PkeyColName == "" {
		return false, nil
	}
	if len(scanNode.BindingTags) == 0 {
		return false, nil
	}
	scanTag := scanNode.BindingTags[0]
	pkColName := scanNode.TableDef.Pkey.PkeyColName
	var pkColPos int32 = -1
	for i, c := range scanNode.TableDef.Cols {
		if strings.EqualFold(c.Name, pkColName) {
			pkColPos = int32(i)
			break
		}
	}
	if pkColPos < 0 {
		return false, nil
	}

	// The single MATCH func + its mode.
	ftFilterExpr := scanNode.FilterList[filterids[0]]
	fn := ftFilterExpr.GetF()
	if fn == nil || len(fn.Args) < 2 {
		return false, nil
	}
	modeLit := fn.Args[1].GetLit()
	if modeLit == nil {
		return false, nil
	}
	mode := modeLit.GetI64Val()

	// (c) peel numeric/pk predicates into the TVF prefilter; residual MUST be empty. Do the
	// peel on a COPY of FilterList so a residual miss leaves scanNode untouched (still covered
	// only when peel consumes ALL non-MATCH filters). Exclude the MATCH filter itself.
	nonMatchFilters := make([]*plan.Expr, 0, len(scanNode.FilterList))
	for i, f := range scanNode.FilterList {
		if int32(i) == filterids[0] {
			continue
		}
		nonMatchFilters = append(nonMatchFilters, f)
	}
	// Gate the pk peel on an evaluable pk type: a pk predicate on a non-evaluable type
	// (uuid/decimal/date/...) is left RESIDUAL (pkColName ""), which makes residual non-empty
	// below → not covered → safe 2-JOIN fallback, instead of peeling a predicate the TVF
	// would reject at runtime. (pkColName above stays the real name for the coverage check.)
	ft2PredsJSON, _, residual, perr := buildFilterPredicateJSON(nonMatchFilters, scanNode, incCols, fulltext2PeelablePkColName(scanNode), true)
	if perr != nil {
		return false, nil // a peel error → fall back to the safe JOIN path
	}
	if len(residual) != 0 {
		return false, nil // residual varchar/complex predicate stays on the base scan → not covered
	}

	// (d) coverage: every projected base-scan ColRef must be the pk or an include column
	// (the MATCH funcs are replaced by score, so skip them). Build the include-name set.
	incSet := make(map[string]int, len(incCols))
	for j, n := range incCols {
		incSet[strings.ToLower(n)] = j
	}
	covered := true
	for i, expr := range projNode.ProjectList {
		if isProjIndexPosition(projids, int32(i)) {
			continue // this projection is the MATCH → becomes score, handled below
		}
		coveredBaseColRefs(expr, scanTag, func(colPos int32, name string) bool {
			if colPos == pkColPos {
				return true
			}
			cn := name
			if cn == "" && colPos >= 0 && int(colPos) < len(scanNode.TableDef.Cols) {
				cn = scanNode.TableDef.Cols[colPos].Name
			}
			if _, ok := incSet[strings.ToLower(cn)]; ok {
				return true
			}
			covered = false
			return false
		})
		if !covered {
			return false, nil
		}
	}

	// ---- All guards passed: build the covered plan. From here on we mutate. ----
	ctx := builder.ctxByNode[nodeID]

	// Remove the MATCH filter from the base scan (it is fully served by the TVF now). This is
	// the last filter standing (residual is empty), so FilterList becomes empty.
	scanNode.FilterList = append(scanNode.FilterList[:filterids[0]], scanNode.FilterList[filterids[0]+1:]...)

	cfg, cfgErr := builder.buildFulltext2SearchCfg(scanNode, idxdef, mode)
	if cfgErr != nil {
		return false, cfgErr
	}
	exprs := []*plan.Expr{
		makePlan2StringConstExprWithType(cfg),
		DeepCopyExpr(fn.Args[0]), // pattern (may be a bound '?' parameter)
		DeepCopyExpr(fn.Args[1]), // mode (a constant)
	}
	if ft2PredsJSON != "" {
		exprs = append(exprs, makePlan2StringConstExprWithType(ft2PredsJSON))
	}
	ftnodeID, err := builder.buildFulltext2SearchNodeCovered(ctx, exprs, nil, scanNode, incCols)
	if err != nil {
		return false, err
	}
	ftnode := builder.qry.Nodes[ftnodeID]
	ftTag := ftnode.BindingTags[0]

	// The TVF's doc_id (col 0) carries the source pk; retype it to the pk's real type, as the
	// JOIN path does (change doc_id type to the primary type).
	pkType := scanNode.TableDef.Cols[pkColPos].Typ
	ftnode.TableDef.Cols[0].Typ.Id = pkType.Id
	ftnode.TableDef.Cols[0].Typ.Width = pkType.Width
	ftnode.TableDef.Cols[0].Typ.Scale = pkType.Scale

	// Register the TVF's output column names for EXPLAIN/name resolution.
	for colPos, colDef := range ftnode.TableDef.Cols {
		builder.nameByColRef[[2]int32{ftTag, int32(colPos)}] = "mo_fulltext_alias_0." + colDef.Name
	}

	// Keep the score-DESC SORT above the TVF (heap tie order is unspecified, so the Sort
	// stays). No JOIN: the SORT/PROJECT reads directly from the TVF.
	//
	// On an EXPLICIT ORDER BY (sortNode != nil) we simply repoint its child at the TVF and do
	// NOT remap sortNode.OrderBy here — that is correct, not a bug. A review flagged that the
	// sort's OrderBy would dangle on the removed base scan; it does not, because:
	//   - EVERY ORDER BY sort-key rides in projNode.ProjectList — the binder projects each as a
	//     hidden INTERNAL column (that is how the SORT gets its key) EVEN when it is not in the
	//     user's SELECT — so guard (d) above already vets sort-keys identically to selected cols:
	//     an uncovered ORDER BY column/expr (incl. one not in SELECT, e.g. `ORDER BY b` / `b+1`)
	//     is a non-pk/non-include base ref that makes the whole rewrite decline to the 2-JOIN
	//     path (verified live: such an ORDER BY produces the JOIN plan, not this one);
	//   - a COVERED ORDER BY column (score / pk / include) is remapped to its TVF output by the
	//     projection remap below, and the final remapAllColRefs pass binds the SORT's OrderBy to
	//     those TVF outputs. Verified live: ORDER BY <include col> — even unprojected, even with
	//     base/TVF ordinals differing — returns correct rows on this 0-JOIN plan.
	scoreOrderBy := []*OrderBySpec{{
		Expr: &Expr{
			Typ:  ftnode.TableDef.Cols[1].Typ,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: ftTag, ColPos: 1}},
		},
		Flag: plan.OrderBySpec_DESC,
	}}
	if sortNode != nil {
		sortNode.Children[0] = ftnodeID
		sortNode.Limit = DeepCopyExpr(paginationLimit)
		sortNode.Offset = DeepCopyExpr(paginationOffset)
	} else {
		sortByID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{ftnodeID},
			OrderBy:  scoreOrderBy,
			Limit:    DeepCopyExpr(paginationLimit),
			Offset:   DeepCopyExpr(paginationOffset),
			SpillMem: builder.sortSpillMem,
		}, ctx)
		projNode.Children[0] = sortByID
	}

	// Pagination now belongs to the SORT above the TVF, not the base scan.
	scanNode.Limit = nil
	scanNode.Offset = nil

	// Remap the projection off the TVF: MATCH -> score (col 1); pk base ColRef -> col 0;
	// include base ColRef -> col 2+j.
	scoreType := ftnode.TableDef.Cols[1].Typ
	for i := range projNode.ProjectList {
		if isProjIndexPosition(projids, int32(i)) {
			projNode.ProjectList[i] = &Expr{
				Typ:  scoreType,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: ftTag, ColPos: 1}},
			}
			continue
		}
		projNode.ProjectList[i] = remapCoveredBaseColRefs(projNode.ProjectList[i], scanTag, ftTag,
			pkColPos, incSet, scanNode)
	}

	if err := builder.recordPreparedPluginDependencies(scanNode); err != nil {
		return false, err
	}
	return true, nil
}

// isProjIndexPosition reports whether projList index i is one of the MATCH projection
// positions (projids holds ProjectList indices of the fulltext MATCH funcs).
func isProjIndexPosition(projids []int32, i int32) bool {
	for _, p := range projids {
		if p == i {
			return true
		}
	}
	return false
}

// coveredBaseColRefs walks expr and invokes visit(colPos, name) for every base-scan ColRef
// (RelPos == scanTag). visit returns false to stop the walk early (coverage failed).
func coveredBaseColRefs(expr *plan.Expr, scanTag int32, visit func(colPos int32, name string) bool) bool {
	if expr == nil {
		return true
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos == scanTag {
			return visit(impl.Col.ColPos, impl.Col.Name)
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			if !coveredBaseColRefs(arg, scanTag, visit) {
				return false
			}
		}
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if !coveredBaseColRefs(sub, scanTag, visit) {
				return false
			}
		}
	}
	return true
}

// remapCoveredBaseColRefs rewrites, in place, every base-scan ColRef (RelPos == scanTag) in
// expr to the equivalent TVF output column: the pk -> TVF col 0, an include column -> TVF
// col 2+j (j = its include index). All such ColRefs are guaranteed pk/include by the (d)
// coverage guard, so no default branch is needed. Returns expr for convenience.
func remapCoveredBaseColRefs(expr *plan.Expr, scanTag, ftTag, pkColPos int32,
	incSet map[string]int, scanNode *plan.Node) *plan.Expr {
	if expr == nil {
		return nil
	}
	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if impl.Col.RelPos == scanTag {
			if impl.Col.ColPos == pkColPos {
				impl.Col.RelPos = ftTag
				impl.Col.ColPos = 0
			} else {
				name := impl.Col.Name
				if name == "" && impl.Col.ColPos >= 0 && int(impl.Col.ColPos) < len(scanNode.TableDef.Cols) {
					name = scanNode.TableDef.Cols[impl.Col.ColPos].Name
				}
				if j, ok := incSet[strings.ToLower(name)]; ok {
					impl.Col.RelPos = ftTag
					impl.Col.ColPos = int32(2 + j)
				}
			}
		}
	case *plan.Expr_F:
		for _, arg := range impl.F.Args {
			remapCoveredBaseColRefs(arg, scanTag, ftTag, pkColPos, incSet, scanNode)
		}
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			remapCoveredBaseColRefs(sub, scanTag, ftTag, pkColPos, incSet, scanNode)
		}
	}
	return expr
}

func (builder *QueryBuilder) scanHasMatchedFullTextFilter(node *plan.Node) bool {
	filterids, _ := builder.getFullTextMatchFiltersFromScanNode(node)
	return len(filterids) > 0
}

func (builder *QueryBuilder) applyFullTextFiltersForJoinChildren(nodeID int32, joinNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (bool, error) {
	// IN subqueries are flattened into SEMI joins. Filters on either input of
	// an INNER or SEMI join can be replaced by an equivalent fulltext index
	// scan without changing the join's row-preservation semantics.
	if joinNode == nil || (joinNode.JoinType != plan.Node_INNER && joinNode.JoinType != plan.Node_SEMI) {
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

	ctxNodeID := builder.fullTextRewriteContextNodeID(nodeID, scanNode)
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

func (builder *QueryBuilder) fullTextRewriteContextNodeID(preferredNodeID int32, scanNode *plan.Node) int32 {
	if preferredNodeID >= 0 && int(preferredNodeID) < len(builder.ctxByNode) && builder.ctxByNode[preferredNodeID] != nil {
		return preferredNodeID
	}
	if scanNode != nil && scanNode.NodeId >= 0 && int(scanNode.NodeId) < len(builder.ctxByNode) && builder.ctxByNode[scanNode.NodeId] != nil {
		return scanNode.NodeId
	}
	return preferredNodeID
}

func (builder *QueryBuilder) equalsFullTextMatchFunc(fn1 *plan.Function, fn2 *plan.Function) bool {

	nargs1 := len(fn1.Args)
	nargs2 := len(fn2.Args)

	if nargs1 != nargs2 {
		return false
	}

	// Pattern arguments may be bound parameters, so compare the bound
	// expression tree instead of dereferencing literal strings.
	if !exprStructuralEqual(fn1.Args[0], fn2.Args[0]) || !exprStructuralEqual(fn1.Args[1], fn2.Args[1]) {
		return false
	}

	// check index parts
	for i := 2; i < nargs1; i++ {
		if !strings.EqualFold(fn1.Args[i].GetCol().GetName(), fn2.Args[i].GetCol().GetName()) {
			return false
		}
	}

	return true
}

// return map[projid]fiter_id -- position of the projids and filterids but NOT position of ProjectList and FilterList
func (builder *QueryBuilder) findEqualFullTextMatchFunc(projNode *plan.Node, scanNode *plan.Node, projids, filterids []int32) map[int32]int32 {

	eqmap := make(map[int32]int32)

	for i, projid := range projids {
		prexpr := projNode.ProjectList[projid]
		for j, fid := range filterids {
			fexpr := scanNode.FilterList[fid]
			eq := builder.equalsFullTextMatchFunc(prexpr.GetF(), fexpr.GetF())
			if eq {
				eqmap[int32(i)] = int32(j)
			}
		}
	}

	return eqmap
}

func (builder *QueryBuilder) findMatchFullTextIndex(fn *plan.Function, scanNode *plan.Node) *plan.IndexDef {
	if fn == nil || scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return nil
	}
	if len(fn.Args) < 3 || fn.Args[1].GetLit() == nil {
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
		if idx == nil || !idx.TableExist {
			continue
		}
		// A fulltext2 index has two hidden-table defs (storage + metadata) sharing the
		// IndexName; resolve against the STORAGE def so IndexTableName/Parts are the ones
		// the fulltext2_search TVF expects. Classic fulltext has a single def.
		if catalog.IsFullText2IndexAlgo(idx.IndexAlgo) {
			if idx.IndexAlgoTableType != catalog.FullText2Index_TblType_Storage {
				continue
			}
		} else if !catalog.IsFullTextIndexAlgo(idx.IndexAlgo) {
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

// Get the filters that are fulltext_match() in ScanNode
func (builder *QueryBuilder) getFullTextMatchFiltersFromScanNode(node *plan.Node) ([]int32, []*plan.IndexDef) {

	filterids := make([]int32, 0)
	ftidxs := make([]*plan.IndexDef, 0)

	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		switch fn.Func.ObjName {
		case "fulltext_match":

			idx := builder.findMatchFullTextIndex(fn, node)
			if idx != nil {
				ftidxs = append(ftidxs, idx)
				filterids = append(filterids, int32(i))
			}
		default:
		}
	}

	return filterids, ftidxs
}

// Get the projection that are fulltext_match() in ProjectList
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
