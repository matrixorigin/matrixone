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
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	ctx := builder.ctxByNode[nodeID]

	idxID, filter_node_ids, proj_node_ids := builder.applyJoinFullTextIndices(nodeID, projNode, scanNode,
		filterids, filterIndexDefs, projids, projIndexDef, colRefCnt, idxColMap)

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

		for _, id := range proj_node_ids {
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
	return nodeID
}

func (builder *QueryBuilder) applyJoinFullTextIndices(nodeID int32, projNode *plan.Node, scanNode *plan.Node,
	filterids []int32, filter_indexDefs []*plan.IndexDef,
	projids []int32, proj_indexDefs []*plan.IndexDef,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) (int32, []int32, []int32) {

	ctx := builder.ctxByNode[nodeID]

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	var pkType = scanNode.TableDef.Cols[pkPos].Typ
	//var colDefs = scanNode.TableDef.Cols
	//pkJson := fmt.Sprintf("{\"type\":%d}", pkType.Id)

	var ret_filter_node_ids = make([]int32, 0)
	var ret_proj_node_ids = make([]int32, 0)

	indexDefs := make([]*plan.IndexDef, 0)
	// copy filters and then delete the fulltext_match from scanNode.FilterList
	ft_filters := make([]*plan.Expr, len(filterids)+len(projids))
	for i, id := range filterids {
		ft_filters[i] = scanNode.FilterList[id]
	}

	// remove the fulltext_match filter from TABLE_SCAN
	for i := len(filterids) - 1; i >= 0; i-- {
		ftid := filterids[i]
		scanNode.FilterList = append(scanNode.FilterList[:ftid], scanNode.FilterList[ftid+1:]...)
	}

	indexDefs = append(indexDefs, filter_indexDefs...)

	proj_offset := len(filterids)
	for i, id := range projids {
		ft_filters[proj_offset+i] = projNode.ProjectList[id]
	}
	indexDefs = append(indexDefs, proj_indexDefs...)

	// buildFullTextIndexScan
	var last_node_id int32
	var last_ftnode_pkcol *Expr

	for i := 0; i < len(ft_filters); i++ {
		ftidxscan := ft_filters[i]
		idxdef := indexDefs[i]
		idxtblname := fmt.Sprintf("`%s`", idxdef.IndexTableName)
		fn := ftidxscan.GetF()
		pattern := fn.Args[0].GetLit().GetSval()
		mode := fn.Args[1].GetLit().GetI64Val()

		fulltext_func := tree.NewCStr(fulltext_index_scan_func_name, 1)
		alias_name := fmt.Sprintf("mo_fulltext_alias_%d", i)

		var exprs tree.Exprs
		exprs = append(exprs, tree.NewNumVal[string](idxtblname, idxtblname, false, tree.P_char))
		exprs = append(exprs, tree.NewNumVal[string](pattern, pattern, false, tree.P_char))
		exprs = append(exprs, tree.NewNumVal[int64](mode, strconv.FormatInt(mode, 10), false, tree.P_int64))

		name := tree.NewUnresolvedName(fulltext_func)

		// TableFuncion AST
		tmpTableFunc := &tree.AliasedTableExpr{
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

		curr_ftnode_id, err := builder.buildTable(tmpTableFunc, ctx, -1, nil)
		if err != nil {
			panic(err.Error())
		}

		// save the created fulltext node to either filter or projection
		if i < proj_offset {
			ret_filter_node_ids = append(ret_filter_node_ids, curr_ftnode_id)
		} else {
			ret_proj_node_ids = append(ret_proj_node_ids, curr_ftnode_id)
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

	// JOIN INNER with children (nodeId, FullTextIndexScanId)

	// oncond
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

	joinnodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{scanNode.NodeId, last_node_id},
		JoinType: plan.Node_INNER,
		OnList:   []*Expr{wherePkEqPk},
		Limit:    DeepCopyExpr(scanNode.Limit),
		Offset:   DeepCopyExpr(scanNode.Offset),
	}, ctx)

	scanNode.Limit = nil
	scanNode.Offset = nil

	return joinnodeID, ret_filter_node_ids, ret_proj_node_ids
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
			for _, idx := range node.TableDef.Indexes {
				nfound := 0
				for _, p := range idx.Parts {
					for j := 2; j < len(fn.Args); j++ {
						if strings.EqualFold(p, fn.Args[j].GetCol().GetName()) {
							// found
							nfound++
							break
						}
					}
				}

				if nfound == len(idx.Parts) {
					ftidxs = append(ftidxs, idx)
					filterids = append(filterids, int32(i))
					break
				}
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

			for _, idx := range scanNode.TableDef.Indexes {
				nfound := 0
				for _, p := range idx.Parts {
					for j := 2; j < len(fn.Args); j++ {
						if strings.EqualFold(p, fn.Args[j].GetCol().GetName()) {
							// found
							nfound++
							break
						}
					}
				}

				if nfound == len(idx.Parts) {
					ftidxs = append(ftidxs, idx)
					projids = append(projids, int32(i))
					break
				}
			}

			// change the fulltext_match function to fulltext_match_score
			// which has return type float32 instead of bool
			projNode.ProjectList[i] = builder.getFullTextMatchScoreExpr(expr)

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

func (builder *QueryBuilder) resolveFullTextMatchFromScanNode(node *plan.Node) *plan.Node {

	if node.NodeType == plan.Node_TABLE_SCAN && len(node.TableDef.Indexes) > 0 {

		for _, f := range node.FilterList {
			fn := f.GetF()
			if fn == nil {
				continue
			}

			if fn.Func.ObjName == "fulltext_match" {
				return node
			}
		}
	}

	for _, nodeId := range node.Children {
		node = builder.resolveFullTextMatchFromScanNode(builder.qry.Nodes[nodeId])
		if node != nil {
			return node
		}
	}

	return nil
}

func (builder *QueryBuilder) resolveFullTextIndexScanNode(node *plan.Node) *plan.Node {

	if node.NodeType == plan.Node_FUNCTION_SCAN && node.TableDef.TableType == "func_table" &&
		node.TableDef.TblFunc.Name == fulltext_index_scan_func_name {
		return node
	}

	for _, nodeId := range node.Children {
		node = builder.resolveFullTextIndexScanNode(builder.qry.Nodes[nodeId])
		if node != nil {
			return node
		}
	}

	return nil
}
