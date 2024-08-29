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
	"go/constant"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Convert the table_scan node to the following
// - JOIN_INNER
//   - TABLE_SCAN
//     -- FilterList but remove the fulltext_match func expr
//   - TABLE_FUNCTION_SCAN (fulltext_index_scan)
//     -- Node_Value_Scan
func (builder *QueryBuilder) applyIndicesForFiltersUsingFullTextIndex(nodeID int32, scanNode *plan.Node, filterids []int32, indexDefs []*plan.IndexDef) int32 {

	//idxScanTag := builder.genNewTag()
	ctx := builder.ctxByNode[nodeID]

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	var pkType = scanNode.TableDef.Cols[pkPos].Typ
	//var colDefs = scanNode.TableDef.Cols

	pkJson := fmt.Sprintf("{\"type\":%d}", pkType.Id)

	// copy filters and then delete the fulltext_match from scanNode.FilterList
	ft_filters := make([]*plan.Expr, len(filterids))
	for i, id := range filterids {
		ft_filters[i] = scanNode.FilterList[id]
	}

	// remove the fulltext_match filter from TABLE_SCAN
	for i := len(filterids) - 1; i >= 0; i-- {
		ftid := filterids[i]
		scanNode.FilterList = append(scanNode.FilterList[:ftid], scanNode.FilterList[ftid+1:]...)
	}

	// buildFullTextIndexScan
	// rewrite sltStmt to select distinct * from (sltStmt) a

	var last_node_id int32
	var last_ftnode_pkcol *Expr

	for i := 0; i < len(filterids); i++ {
		ftidxscan := ft_filters[i]
		idxdef := indexDefs[i]
		idxtblname := fmt.Sprintf("`%s`", idxdef.IndexTableName)
		keyparts := strings.Join(idxdef.Parts, ",")
		fn := ftidxscan.GetF()
		pattern := fn.Args[0].GetLit().GetSval()
		mode := fn.Args[1].GetLit().GetI64Val()

		fulltext_func := tree.NewCStr("fulltext_index_scan", 1)

		var exprs tree.Exprs
		exprs = append(exprs, tree.NewNumValWithType(constant.MakeString(idxtblname), idxtblname, false, tree.P_char))
		exprs = append(exprs, tree.NewNumValWithType(constant.MakeString(pkJson), pkJson, false, tree.P_char))
		exprs = append(exprs, tree.NewNumValWithType(constant.MakeString(keyparts), keyparts, false, tree.P_char))
		exprs = append(exprs, tree.NewNumValWithType(constant.MakeString(pattern), pattern, false, tree.P_char))
		exprs = append(exprs, tree.NewNumValWithType(constant.MakeInt64(mode), strconv.FormatInt(mode, 10), false, tree.P_int64))

		name := tree.NewUnresolvedName(fulltext_func)

		/*
			tmpSltStmt := &tree.Select{
				Select: &tree.SelectClause{
					Distinct: true,

					Exprs: []tree.SelectExpr{
						{Expr: tree.StarExpr()},
						//{Expr: tree.NewUnresolvedColName("fulltext_alias_inner.doc_id")},
						//{Expr: tree.NewUnresolvedColName("fulltext_alias_table_name.doc_id")},
					},
					From: &tree.From{
						Tables: tree.TableExprs{
							&tree.AliasedTableExpr{
								Expr: &tree.AliasedTableExpr{
									Expr: &tree.TableFunction{
										Func: &tree.FuncExpr{
											Func:     tree.FuncName2ResolvableFunctionReference(name),
											FuncName: fulltext_func,
											Exprs:    exprs,
											Type:     tree.FUNC_TYPE_TABLE,
										},
									},
									As: tree.AliasClause{
										Alias: "fulltext_alias_inner",
									},
								},
								As: tree.AliasClause{
									Alias: "fulltext_alias_table_name",
								},
							},
						},
					},
				},
			}

			isRoot := false
			curr_ftnode_id, err := builder.buildSelect(tmpSltStmt, ctx, isRoot)
			if err != nil {
				panic(err.Error())
			}

		*/

		// SELECT doc_id from fulltext_index_scan('a', 'b', 'c', 'd') as f;
		/*
			tmpTableFunc := &tree.TableFunction{
				Func: &tree.FuncExpr{
					Func:     tree.FuncName2ResolvableFunctionReference(name),
					FuncName: fulltext_func,
					Exprs:    exprs,
					Type:     tree.FUNC_TYPE_TABLE,
				},
			}

			curr_ftnode_id, err := builder.buildTableFunction(tmpTableFunc, ctx, -1, nil)
		*/

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
				Alias: "fulltext_alias",
			},
		}
		curr_ftnode_id, err := builder.buildTable(tmpTableFunc, ctx, nodeID, ctx)
		if err != nil {
			panic(err.Error())
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

		// add the column label
		builder.addNameByColRef(curr_ftnode_tag, curr_ftnode.TableDef)

		logutil.Infof("TABLE_FUNCTION %v", curr_ftnode)

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
		Children: []int32{nodeID, last_node_id},
		JoinType: plan.Node_INNER,
		OnList:   []*Expr{wherePkEqPk},
		Limit:    DeepCopyExpr(scanNode.Limit),
		Offset:   DeepCopyExpr(scanNode.Offset),
		//FilterList: scanNode.FilterList,
	}, ctx)

	scanNode.Limit = nil
	scanNode.Offset = nil
	//scanNode.FilterList = nil

	joinnode := builder.qry.Nodes[joinnodeID]

	logutil.Infof("JOIN INNER %v", joinnode)

	return joinnodeID
}
