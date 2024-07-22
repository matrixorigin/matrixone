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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

var (
	varcharType = types.T_varchar.ToType()
)

func (builder *QueryBuilder) applyIndicesForFiltersUsingMasterIndex(nodeID int32, scanNode *plan.Node, indexDef *plan.IndexDef) int32 {

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	var pkType = scanNode.TableDef.Cols[pkPos].Typ
	var colDefs = scanNode.TableDef.Cols

	var prevIndexPkCol *Expr
	var prevLastNodeId int32
	var lastNodeId int32

	//ts1 := scanNode.ScanTS
	for i, filterExp := range scanNode.FilterList {
		// TODO: node should hold snapshot info and account info
		//idxObjRef, idxTableDef := builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, indexDef.IndexTableName, timestamp.Timestamp{})
		idxObjRef, idxTableDef := builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, indexDef.IndexTableName, Snapshot{TS: &timestamp.Timestamp{}})

		// 1. SELECT pk from idx WHERE prefix_eq(`__mo_index_idx_col`,serial_full("0","value"))
		currIdxScanTag, currScanId := makeIndexTblScan(builder, builder.ctxByNode[nodeID], filterExp, idxTableDef, idxObjRef, scanNode.ScanSnapshot, colDefs)

		// 2. (SELECT pk from idx1 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("0","value1")) )
		//    	INNER JOIN
		//    (SELECT pk from idx2 WHERE prefix_eq(`__mo_index_idx_col` =  serial_full("1","value2")) )
		//    	ON idx1.pk = idx2.pk
		//    ...
		lastNodeId = currScanId
		currIndexPkCol := &Expr{
			Typ: pkType,
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
	lastNodeFromIndexTbl := builder.qry.Nodes[lastNodeId]
	lastNodeFromIndexTbl.Limit = DeepCopyExpr(scanNode.Limit)
	lastNodeFromIndexTbl.Offset = DeepCopyExpr(scanNode.Offset)
	scanNode.Limit, scanNode.Offset = nil, nil

	// 3. SELECT * from tbl INNER JOIN (
	//    	(SELECT pk from idx1 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("0","value1")) )
	//    		INNER JOIN
	//    	(SELECT pk from idx2 WHERE prefix_eq(`__mo_index_idx_col`,serial_full("1","value2")) )
	//    		ON idx1.pk = idx2.pk
	//    ) ON tbl.pk = idx1.pk
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
		Limit:    DeepCopyExpr(lastNodeFromIndexTbl.Limit),
		Offset:   DeepCopyExpr(lastNodeFromIndexTbl.Offset),
	}, builder.ctxByNode[nodeID])

	return lastNodeId
}

func makeIndexTblScan(builder *QueryBuilder, bindCtx *BindContext, filterExp *plan.Expr,
	idxTableDef *TableDef, idxObjRef *ObjectRef, scanSnapshot *Snapshot, colDefs []*plan.ColDef) (int32, int32) {

	// a. Scan * WHERE prefix_eq(`__mo_index_idx_col`,serial_full("0","value"))
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
				makePlan2StringConstExprWithType(getColSeqFromColDef(colDefs[args[0].GetCol().GetColPos()])), // "0"
				args[1], // value
			})

		filterList, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_eq", []*Expr{
			indexKeyCol, // __mo_index_idx_col
			serialExpr1, // serial_full("0","value")
		})
	case "between":
		serialExpr1, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", []*plan.Expr{
			makePlan2StringConstExprWithType(getColSeqFromColDef(colDefs[args[0].GetCol().GetColPos()])), // "0"
			args[1], // value1
		})
		serialExpr2, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", []*plan.Expr{
			makePlan2StringConstExprWithType(getColSeqFromColDef(colDefs[args[0].GetCol().GetColPos()])), // "0"
			args[2], // value2
		})
		filterList, _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_between", []*Expr{
			indexKeyCol, // __mo_index_idx_col
			serialExpr1, // serial_full("0","value1")
			serialExpr2, // serial_full("0","value2")
		})

	case "in":
		// Since this master index specifically for varchar, we assume the `IN` to contain only varchar values.
		inVecType := types.T_varchar.ToType()

		// a. varchar vector ("value1", "value2", "value3")
		arg1AsColValuesVec := vector.NewVec(inVecType)
		_ = arg1AsColValuesVec.UnmarshalBinary(args[1].GetVec().GetData())
		inExprListLen := arg1AsColValuesVec.Length()

		// b. const vector "0"
		mp := mpool.MustNewZero()
		arg0AsColNameVec, _ := vector.NewConstBytes(inVecType, []byte(getColSeqFromColDef(colDefs[args[0].GetCol().GetColPos()])), inExprListLen, mp)

		// c. (serial_full("0","value1"), serial_full("0","value2"), serial_full("0","value3"))
		ps := types.NewPackerArray(inExprListLen, mp)
		defer func() {
			for _, p := range ps {
				p.FreeMem()
			}
		}()
		function.SerialHelper(arg0AsColNameVec, nil, ps, true)
		function.SerialHelper(arg1AsColValuesVec, nil, ps, true)
		arg1ForPrefixInVec := vector.NewVec(inVecType)
		for i := 0; i < inExprListLen; i++ {
			_ = vector.AppendBytes(arg1ForPrefixInVec, ps[i].Bytes(), false, mp)
		}

		// d. convert result vector to LiteralVec
		arg1ForPrefixInBytes, _ := arg1ForPrefixInVec.MarshalBinary()
		arg1ForPrefixInLitVec := &plan.Expr{
			Typ: makePlan2Type(&varcharType),
			Expr: &plan.Expr_Vec{
				Vec: &plan.LiteralVec{
					Len:  int32(len(arg1ForPrefixInBytes)),
					Data: arg1ForPrefixInBytes,
				},
			},
		}

		// e. free memory for arg0, arg1 vector. Packer's memory is freed in defer.
		arg1ForPrefixInVec.Free(mp)
		arg0AsColNameVec.Free(mp)

		filterList, _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_in", []*Expr{
			indexKeyCol,           // __mo_index_idx_col
			arg1ForPrefixInLitVec, // (serial_full("0","value1"), serial_full("0","value2"), serial_full("0","value3"))
		})

	default:
		panic("unsupported filter expression")
	}

	//NOTE: very important. You need to set ColName for the ColExpr to be pushed down to
	// the Storage Engine layer. Otherwise, we will end up scanning all the rows.
	builder.addNameByColRef(idxScanTag, idxTableDef)

	scanId := builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    idxTableDef,
		ObjRef:      idxObjRef,
		FilterList:  []*plan.Expr{filterList},
		BindingTags: []int32{idxScanTag},
		//ScanTS:       &scanTs,
		ScanSnapshot: scanSnapshot,
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
