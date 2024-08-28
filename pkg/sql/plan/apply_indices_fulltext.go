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
	"go/constant"

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
func (builder *QueryBuilder) applyIndicesForFiltersUsingFullTextIndex(nodeID int32, scanNode *plan.Node, indexDef *plan.IndexDef) (int32, error) {

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	var pkType = scanNode.TableDef.Cols[pkPos].Typ
	//var colDefs = scanNode.TableDef.Cols

	//idxScanTag := builder.genNewTag()
	ctx := builder.ctxByNode[nodeID]

	// remove the fulltext_match filter from TABLE_SCAN

	// buildFullTextIndexScan
	// rewrite sltStmt to select distinct * from (sltStmt) a

	fulltext_func := tree.NewCStr("fulltext_index_scan", 1)

	var exprs tree.Exprs
	/*
		exprs = append(exprs, tree.NewStrVal("idx"))
		exprs = append(exprs, tree.NewStrVal("pk_type"))
		exprs = append(exprs, tree.NewStrVal("keyparts"))
		exprs = append(exprs, tree.NewStrVal("pattern"))

	*/
	exprs = append(exprs, tree.NewNumVal(constant.MakeString("idx"), "idx", false))
	exprs = append(exprs, tree.NewNumVal(constant.MakeString("pk_type"), "pk_type", false))
	exprs = append(exprs, tree.NewNumVal(constant.MakeString("keyparts"), "keyparts", false))
	exprs = append(exprs, tree.NewNumVal(constant.MakeString("pattern"), "pattern", false))

	// SELECT doc_id from fulltext_index_scan('a', 'b', 'c', 'd') as f;
	name := tree.NewUnresolvedName(fulltext_func)
	tmpSltStmt := &tree.Select{
		Select: &tree.SelectClause{
			Distinct: true,

			Exprs: []tree.SelectExpr{
				//{Expr: tree.StarExpr()},
				{Expr: tree.NewUnresolvedColName("fulltext_alias_table_name.doc_id")},
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
	tblfn_nodeId, err := builder.buildSelect(tmpSltStmt, ctx, isRoot)
	if err != nil {
		return 0, err
	}
	tblfn_node := builder.qry.Nodes[tblfn_nodeId]

	logutil.Infof("TABLE_FUNCTION %v", tblfn_node)

	// JOIN INNER with children (nodeId, FullTextIndexScanId)
	joinnodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{nodeID, tblfn_nodeId},
		JoinType: plan.Node_INNER,
	}, ctx)

	joinnode := builder.qry.Nodes[joinnodeID]

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
					RelPos: tblfn_node.BindingTags[0], // last idxTbl (may be join) relPos
					ColPos: 0,                         // idxTbl.pk
				},
			},
		},
	})

	joinnode.OnList = []*Expr{wherePkEqPk}

	logutil.Infof("JOIN INNER %v", joinnode)

	return joinnodeID, nil
}
