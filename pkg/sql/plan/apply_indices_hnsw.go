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
	"fmt"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

func (builder *QueryBuilder) checkValidHnswDistFn(nodeID int32, projNode, sortNode, scanNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, multiTableIndex *MultiTableIndex) bool {

	if len(sortNode.OrderBy) != 1 {
		return false
	}

	orderExpr := sortNode.OrderBy[0].Expr
	distFnExpr := orderExpr.GetF()
	if distFnExpr == nil {
		childNode := builder.qry.Nodes[sortNode.Children[0]]
		if childNode.NodeType == plan.Node_PROJECT {
			distFnExpr = childNode.ProjectList[orderExpr.GetCol().ColPos].GetF()
		}

		if distFnExpr == nil {
			return false
		}
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

	idxdef := multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Metadata]

	val, err := sonic.Get([]byte(idxdef.IndexAlgoParams), catalog.IndexAlgoParamOpType)
	if err != nil {
		return false
	}
	optype, err := val.StrictString()
	if err != nil {
		return false
	}

	if optype != metric.DistFuncOpTypes[distFnExpr.Func.ObjName] {
		return false
	}

	_, value, ok := builder.getArgsFromDistFn(scanNode, distFnExpr, idxdef.Parts[0])
	if !ok {
		return false
	}

	// check valid vector type
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

func (builder *QueryBuilder) applyIndicesForSortUsingHnsw(nodeID int32, projNode, sortNode, scanNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, multiTableIndex *MultiTableIndex) (int32, error) {

	ctx := builder.ctxByNode[nodeID]
	metadef := multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Metadata]
	idxdef := multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Storage]
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

	val, err := builder.compCtx.ResolveVariable("hnsw_threads_search", true, false)
	if err != nil {
		return nodeID, err
	}
	/*
		tblcfg := vectorindex.IndexTableConfig{DbName: scanNode.ObjRef.SchemaName,
			SrcTable:      scanNode.TableDef.Name,
			MetadataTable: metadef.IndexTableName,
			IndexTable:    idxdef.IndexTableName,
			ThreadsSearch: val.(int64)}

		cfgbytes, err := sonic.Marshal(tblcfg)
		if err != nil {
			return nodeID, err
		}
		tblcfgstr := string(cfgbytes)
	*/

	// generate JSON by fmt.Sprintf instead of sonic.Marshal for performance
	tblcfgstr := fmt.Sprintf(`{"db": "%s", "src": "%s", "metadata":"%s", "index":"%s", "threads_search": %d}`,
		scanNode.ObjRef.SchemaName,
		scanNode.TableDef.Name,
		metadef.IndexTableName,
		idxdef.IndexTableName,
		val.(int64))

	var childNode *plan.Node
	orderExpr := sortNode.OrderBy[0].Expr
	distFnExpr := orderExpr.GetF()
	if distFnExpr == nil {
		childNode = builder.qry.Nodes[sortNode.Children[0]]
		distFnExpr = childNode.ProjectList[orderExpr.GetCol().ColPos].GetF()
	}
	sortDirection := sortNode.OrderBy[0].Flag // For the most part, it is ASC

	_, value, ok := builder.getArgsFromDistFn(scanNode, distFnExpr, keypart)
	if !ok {
		return nodeID, moerr.NewInternalErrorNoCtx("invalid distance function")
	}

	//distFnName := distFuncInternalDistFunc[distFnExpr.Func.ObjName]
	// fp32vstr := distFnExpr.Args[1].GetLit().GetSval() // fp32vec

	// JOIN between source table and hnsw_search table function
	var exprs tree.Exprs

	exprs = append(exprs, tree.NewNumVal(params, params, false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal(tblcfgstr, tblcfgstr, false, tree.P_char))

	fnexpr := value.GetF()
	fvec := fnexpr.Args[0].GetLit().GetSval()

	// no need to check valid vector type here. type already checked.
	var vecfamily string
	if partType.GetId() == int32(types.T_array_float32) {
		vecfamily = "vecf32"
	} else if partType.GetId() == int32(types.T_array_float64) {
		vecfamily = "vecf64"
	}

	valExpr := &tree.CastExpr{Expr: tree.NewNumVal(fvec, fvec, false, tree.P_char),
		Type: &tree.T{InternalType: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_VAR_STRING),
			FamilyString: vecfamily, Family: tree.ArrayFamily, DisplayWith: partType.Width}}}

	exprs = append(exprs, valExpr)

	hnsw_func := tree.NewCStr(hnsw_search_func_name, 1)
	alias_name := "mo_hnsw_alias_0"
	name := tree.NewUnresolvedName(hnsw_func)

	tmpTableFunc := &tree.AliasedTableExpr{
		Expr: &tree.TableFunction{
			Func: &tree.FuncExpr{
				Func:     tree.FuncName2ResolvableFunctionReference(name),
				FuncName: hnsw_func,
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

	// pushdown limit to Table Function
	// When there are filters, over-fetch to get more candidates
	// This ensures we have enough candidates after filtering
	if limit != nil {
		if len(scanNode.FilterList) > 0 {
			// Over-fetch strategy: dynamically adjust factor based on limit size
			// Smaller limits need more over-fetching due to higher variance
			if limitConst := limit.GetLit(); limitConst != nil {
				originalLimit := limitConst.GetU64Val()

				// Use shared function to calculate over-fetch factor
				overFetchFactor := calculatePostFilterOverFetchFactor(originalLimit)

				newLimit := uint64(float64(originalLimit) * overFetchFactor)
				// Ensure at least original limit + some buffer
				if newLimit < originalLimit+10 {
					newLimit = originalLimit + 10
				}

				curr_node.Limit = &Expr{
					Typ: limit.Typ,
					Expr: &plan.Expr_Lit{
						Lit: &plan.Literal{
							Isnull: false,
							Value: &plan.Literal_U64Val{
								U64Val: newLimit,
							},
						},
					},
				}
			} else {
				// If limit is not a constant, just copy it
				curr_node.Limit = DeepCopyExpr(limit)
			}
		} else {
			// No filters, use original limit
			curr_node.Limit = DeepCopyExpr(limit)
		}
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

	// Preserve non-vector filters from scanNode
	// These filters (e.g., "score >= 4.0") should be applied after the JOIN
	var filterList []*Expr
	if len(scanNode.FilterList) > 0 {
		filterList = make([]*Expr, len(scanNode.FilterList))
		for i, filter := range scanNode.FilterList {
			filterList[i] = DeepCopyExpr(filter)
		}
	}

	joinnodeID := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{scanNode.NodeId, curr_node_id},
		JoinType: plan.Node_INNER,
		OnList:   []*Expr{wherePkEqPk},
		// Don't set Limit/Offset on JOIN - they should be applied after SORT
		// Don't set FilterList on JOIN - move to SORT for better execution
	}, ctx)

	scanNode.Limit = nil
	scanNode.Offset = nil
	scanNode.FilterList = nil // Clear filters from scanNode since they're moved to SORT

	// Create SortBy with distance column from table function
	orderByScore := []*OrderBySpec{
		{
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
		},
	}

	sortByID := builder.appendNode(&plan.Node{
		NodeType:   plan.Node_SORT,
		Children:   []int32{joinnodeID},
		OrderBy:    orderByScore,
		FilterList: filterList,                    // Apply filters before limit
		Limit:      DeepCopyExpr(scanNode.Limit),  // Apply LIMIT after filtering and sorting
		Offset:     DeepCopyExpr(scanNode.Offset), // Apply OFFSET after filtering and sorting
	}, ctx)

	projNode.Children[0] = sortByID

	if childNode != nil {
		sortIdx := orderExpr.GetCol().ColPos
		projMap := make(map[[2]int32]*plan.Expr)
		for i, proj := range childNode.ProjectList {
			if i == int(sortIdx) {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = DeepCopyExpr(orderByScore[0].Expr)
			} else {
				projMap[[2]int32{childNode.BindingTags[0], int32(i)}] = proj
			}
		}

		replaceColumnsForNode(projNode, projMap)
	}

	return nodeID, nil
}

func (builder *QueryBuilder) getArgsFromDistFn(scanNode *plan.Node, distfn *plan.Function, keypart string) (key *plan.Expr, value *plan.Expr, found bool) {

	found = false
	keyid := -1
	key = nil
	value = nil

	for i, a := range distfn.Args {
		if a.GetCol() != nil {
			colPosOrderBy := a.GetCol().ColPos
			if colPosOrderBy == scanNode.TableDef.Name2ColIndex[keypart] {
				found = true
				keyid = i
				break
			}
		}
	}

	if found {
		key = distfn.Args[keyid]
		if keyid == 0 {
			value = distfn.Args[1]
		} else {
			value = distfn.Args[0]
		}
	}

	return key, value, found
}
