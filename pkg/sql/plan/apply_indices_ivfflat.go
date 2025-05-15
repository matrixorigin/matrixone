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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

func (builder *QueryBuilder) checkValidIvfflatDistFn(nodeID int32, projNode, sortNode, scanNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, multiTableIndex *MultiTableIndex) bool {

	if len(sortNode.OrderBy) != 1 {
		return false
	}

	distFnExpr := sortNode.OrderBy[0].Expr.GetF()
	if distFnExpr == nil {
		return false
	}
	if _, ok := metric.DistFuncOpTypes[distFnExpr.Func.ObjName]; !ok {
		return false
	}

	var limit *plan.Expr
	if sortNode.Limit != nil {
		limit = sortNode.Limit
	} else if scanNode.Limit != nil {
		limit = scanNode.Limit
	} else if projNode.Limit != nil {
		limit = projNode.Limit
	}
	if limit == nil {
		return false
	}

	idxdef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]

	params, err := catalog.IndexParamsStringToMap(idxdef.IndexAlgoParams)
	if err != nil {
		return false
	}

	optype, ok := params[catalog.IndexAlgoParamOpType]
	if !ok {
		return false
	}

	if optype != metric.DistFuncOpTypes[distFnExpr.Func.ObjName] {
		return false
	}

	_, value, ok := builder.getArgsFromDistFn(scanNode, distFnExpr, idxdef.Parts[0])
	if !ok {
		return false
	}

	if value.Typ.GetId() != int32(types.T_array_float32) && value.Typ.GetId() != int32(types.T_array_float64) {
		return false
	}

	if value.GetF() != nil {
		fnexpr := value.GetF()
		if fnexpr.Func.ObjName != "cast" {
			return false
		}
	} else {
		return false
	}

	return true
}

func (builder *QueryBuilder) applyIndicesForSortUsingIvfflat(nodeID int32, projNode, sortNode, scanNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, multiTableIndex *MultiTableIndex) (int32, error) {

	ctx := builder.ctxByNode[nodeID]
	metadef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
	idxdef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids]
	entriesdef := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries]
	pkPos := scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName]
	pkType := scanNode.TableDef.Cols[pkPos].Typ
	keypart := idxdef.Parts[0]
	partPos := scanNode.TableDef.Name2ColIndex[keypart]
	partType := scanNode.TableDef.Cols[partPos].Typ
	params := idxdef.IndexAlgoParams

	var limit *plan.Expr
	if sortNode.Limit != nil {
		limit = sortNode.Limit
	} else if scanNode.Limit != nil {
		limit = scanNode.Limit
	} else if projNode.Limit != nil {
		limit = projNode.Limit
	}

	val, err := builder.compCtx.ResolveVariable("ivf_threads_search", true, false)
	if err != nil {
		return nodeID, err
	}

	nprobe := int64(5)
	nprobeif, err := builder.compCtx.ResolveVariable("probe_limit", true, false)
	if err != nil {
		return nodeID, err
	}
	if nprobeif != nil {
		var ok bool
		nprobe, ok = (nprobeif.(int64))
		if !ok {
			return nodeID, moerr.NewInternalErrorNoCtx("ResolveVariable: probe_limit is not int64")
		}
	}

	tblcfg := vectorindex.IndexTableConfig{DbName: scanNode.ObjRef.SchemaName,
		SrcTable:      scanNode.TableDef.Name,
		MetadataTable: metadef.IndexTableName,
		IndexTable:    idxdef.IndexTableName,
		ThreadsSearch: val.(int64),
		EntriesTable:  entriesdef.IndexTableName,
		Nprobe:        uint(nprobe),
		PKeyType:      pkType.Id,
		PKey:          scanNode.TableDef.Pkey.PkeyColName,
		KeyPart:       keypart,
		KeyPartType:   partType.Id}

	cfgbytes, err := json.Marshal(tblcfg)
	if err != nil {
		return nodeID, err
	}
	tblcfgstr := string(cfgbytes)

	distFnExpr := sortNode.OrderBy[0].Expr.GetF()
	sortDirection := sortNode.OrderBy[0].Flag // For the most part, it is ASC

	_, value, ok := builder.getArgsFromDistFn(scanNode, distFnExpr, keypart)
	if !ok {
		return nodeID, moerr.NewInternalErrorNoCtx("invalid distance function")
	}

	//distFnName := distFuncInternalDistFunc[distFnExpr.Func.ObjName]
	// fp32vstr := distFnExpr.Args[1].GetLit().GetSval() // fp32vec

	// JOIN between source table and hnsw_search table function
	var exprs tree.Exprs

	exprs = append(exprs, tree.NewNumVal[string](params, params, false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal[string](tblcfgstr, tblcfgstr, false, tree.P_char))

	fnexpr := value.GetF()
	vecsval := fnexpr.Args[0].GetLit().GetSval()
	family := "vecf32"
	if value.Typ.GetId() == int32(types.T_array_float64) {
		family = "vecf64"
	}
	valExpr := &tree.CastExpr{Expr: tree.NewNumVal[string](vecsval, vecsval, false, tree.P_char),
		Type: &tree.T{InternalType: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_VAR_STRING),
			FamilyString: family, Family: tree.ArrayFamily, DisplayWith: partType.Width}}}
	exprs = append(exprs, valExpr)

	ivf_func := tree.NewCStr(ivf_search_func_name, 1)
	alias_name := "mo_ivf_alias_0"
	name := tree.NewUnresolvedName(ivf_func)

	tmpTableFunc := &tree.AliasedTableExpr{
		Expr: &tree.TableFunction{
			Func: &tree.FuncExpr{
				Func:     tree.FuncName2ResolvableFunctionReference(name),
				FuncName: ivf_func,
				Exprs:    exprs,
				Type:     tree.FUNC_TYPE_TABLE,
			},
		},
		As: tree.AliasClause{
			Alias: tree.Identifier(alias_name),
		},
	}

	curr_node_id, err := builder.buildTable(tmpTableFunc, ctx, -1, nil)
	if err != nil {
		return nodeID, err
	}

	curr_node := builder.qry.Nodes[curr_node_id]

	curr_node_tag := curr_node.BindingTags[0]

	curr_node_pkcol := &Expr{
		Typ: pkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: curr_node_tag,
				ColPos: 0,
			},
		},
	}

	// change doc_id type to the primary type here
	curr_node.TableDef.Cols[0].Typ.Id = pkType.Id
	curr_node.TableDef.Cols[0].Typ.Width = pkType.Width
	curr_node.TableDef.Cols[0].Typ.Scale = pkType.Scale

	// pushdown limit
	if limit != nil {
		curr_node.Limit = DeepCopyExpr(limit)
	}

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
					RelPos: curr_node_pkcol.GetCol().RelPos, // last idxTbl (may be join) relPos
					ColPos: 0,                               // idxTbl.pk
				},
			},
		},
	})

	joinnodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{scanNode.NodeId, curr_node_id},
		JoinType: plan.Node_INNER,
		OnList:   []*Expr{wherePkEqPk},
		Limit:    DeepCopyExpr(scanNode.Limit),
		Offset:   DeepCopyExpr(scanNode.Offset),
	}, ctx)

	scanNode.Limit = nil
	scanNode.Offset = nil

	// Create SortBy with distance column from table function
	orderByScore := make([]*OrderBySpec, 0, 1)
	orderByScore = append(orderByScore, &OrderBySpec{
		Expr: &Expr{
			Typ: curr_node.TableDef.Cols[1].Typ, // score column
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: curr_node.BindingTags[0],
					ColPos: 1, // score column
				},
			},
		},
		Flag: sortDirection,
	})

	sortByID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{joinnodeID},
		OrderBy:  orderByScore,
	}, ctx)

	projNode.Children[0] = sortByID

	return nodeID, nil
}
