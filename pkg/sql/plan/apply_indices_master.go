// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	varcharType = types.T_varchar.ToType()
)

func (builder *QueryBuilder) applyIndicesForFiltersUsingMasterIndex(nodeID int32, scanNode *plan.Node,
	cnt map[[2]int32]int, colMap map[[2]int32]*plan.Expr, indexDef *plan.IndexDef) int32 {

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	var pkType = scanNode.TableDef.Cols[pkPos].Typ

	var prevIndexPkCol *Expr
	var prevLastNodeId int32
	var lastNodeId int32
	for i, filterExp := range scanNode.FilterList {
		idxObjRef, idxTableDef := builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, indexDef.IndexTableName)

		// 1. SELECT pk from idx WHERE prefix_eq(`__mo_index_idx_col`,serial_full("a","value"))
		currIdxScanTag, currScanId := makeIndexTblScan(builder, builder.ctxByNode[nodeID], filterExp, idxTableDef, idxObjRef)

		// 2. (SELECT pk from idx1 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("a","value1")) )
		//    	INNER JOIN
		//    (SELECT pk from idx2 WHERE prefix_eq(`__mo_index_idx_col` =  serial_full("b","value2")) )
		//    	ON idx1.pk = idx2.pk
		//    ...
		lastNodeId = currScanId
		currIndexPkCol := &Expr{
			Typ: DeepCopyType(pkType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: currIdxScanTag,
					ColPos: 1, // __mo_index_pk_col
				},
			},
		}
		if i != 0 {
			wherePrevPkEqCurrPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
				currIndexPkCol,
				prevIndexPkCol,
			})
			lastNodeId = builder.appendNode(&plan.Node{
				NodeType: plan.Node_JOIN,
				JoinType: plan.Node_INNER,
				Children: []int32{currScanId, prevLastNodeId},
				OnList:   []*Expr{wherePrevPkEqCurrPk},
			}, builder.ctxByNode[nodeID])
		}

		prevIndexPkCol = DeepCopyExpr(currIndexPkCol)
		prevLastNodeId = lastNodeId
	}

	// 3. SELECT * from tbl INNER JOIN (
	//    	(SELECT pk from idx1 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("a","value1")) )
	//    		INNER JOIN
	//    	(SELECT pk from idx2 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("b","value2")) )
	//    		ON idx1.pk = idx2.pk
	//    ) ON tbl.pk = idx1.pk
	wherePkEqPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: DeepCopyType(pkType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: pkPos, // tbl.pk
				},
			},
		},
		{
			Typ: DeepCopyType(pkType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: prevIndexPkCol.GetCol().RelPos, // last idxTbl (may be join) relPos
					ColPos: 1,                              // idxTbl.pk
				},
			},
		},
	})
	lastNodeId = builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INDEX,
		Children: []int32{scanNode.NodeId, lastNodeId},
		OnList:   []*Expr{wherePkEqPk},
	}, builder.ctxByNode[nodeID])

	return lastNodeId
}

func makeIndexTblScan(builder *QueryBuilder, bindCtx *BindContext, filterExp *plan.Expr,
	idxTableDef *TableDef, idxObjRef *ObjectRef) (int32, int32) {

	idxScanTag := builder.genNewTag()
	args := filterExp.GetF().Args

	var filterList *plan.Expr
	indexKeyCol := &plan.Expr{
		Typ: makePlan2Type(&varcharType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: idxScanTag, //__mo_index_idx_col
				ColPos: 0,
			},
		},
	}

	switch filterExp.GetF().Func.ObjName {
	case "=":
		serialExpr1, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full",
			[]*plan.Expr{
				makePlan2StringConstExprWithType(args[0].GetCol().Name), // "a"
				args[1], // value
			})

		filterList, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_eq", []*Expr{
			indexKeyCol, // __mo_index_idx_col
			serialExpr1, // serial_full("a","value")
		})
	case "between":
		serialExpr1, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", []*plan.Expr{
			makePlan2StringConstExprWithType(args[0].GetCol().Name), // "a"
			args[1], // value1
		})
		serialExpr2, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", []*plan.Expr{
			makePlan2StringConstExprWithType(args[0].GetCol().Name), // "a"
			args[2], // value2
		})
		filterList, _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_between", []*Expr{
			indexKeyCol, // __mo_index_idx_col
			serialExpr1, // serial_full("a","value1")
			serialExpr2, // serial_full("a","value2")
		})

		//TODO: need to convert []Expr{"a", args[1]} --> []Expr{"a", "value1", "value2", "value3"...}
		// Will be done later. args[1] is LiteralVec
		//case "in":
		//	serialExpr1, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full",
		//		[]*plan.Expr{
		//			makePlan2StringConstExprWithType(args[0].GetCol().Name), // "a"
		//			args[1], // value
		//		})
		//	filterList, _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_in", []*Expr{
		//		indexKeyCol, // __mo_index_idx_col
		//		serialExpr1, // serial_full("a","value")
		//	})

	}

	scanId := builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    idxTableDef,
		ObjRef:      idxObjRef,
		FilterList:  []*plan.Expr{filterList},
		BindingTags: []int32{idxScanTag},
	}, bindCtx)
	return idxScanTag, scanId
}

func isKeyPresentInList(key string, list []string) bool {
	for _, item := range list {
		if key == item {
			return true
		}
	}
	return false
}
