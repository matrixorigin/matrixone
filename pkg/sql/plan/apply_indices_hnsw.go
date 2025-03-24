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

func (builder *QueryBuilder) checkValidHnswDistFn(nodeID int32, projNode, sortNode, scanNode *plan.Node,
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

	idxdef := multiTableIndex.IndexDefs[catalog.Hnsw_TblType_Metadata]

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

	if value.Typ.GetId() != int32(types.T_array_float32) {
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
	tblcfg := vectorindex.IndexTableConfig{DbName: scanNode.ObjRef.SchemaName,
		SrcTable:      scanNode.TableDef.Name,
		MetadataTable: metadef.IndexTableName,
		IndexTable:    idxdef.IndexTableName,
		ThreadsSearch: val.(int64)}

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
	f32vec := fnexpr.Args[0].GetLit().GetSval()

	valExpr := &tree.CastExpr{Expr: tree.NewNumVal[string](f32vec, f32vec, false, tree.P_char),
		Type: &tree.T{InternalType: tree.InternalType{Oid: uint32(defines.MYSQL_TYPE_VAR_STRING),
			FamilyString: "vecf32", Family: tree.ArrayFamily, DisplayWith: partType.Width}}}

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

	/*
		// check equal distFn and only compute once for equal function()
		projids := builder.findEqualDistFnFromProject(projNode, distFnExpr)

		// replace the project with ColRef (same distFn as the order by)
		for _, id := range projids {
			projNode.ProjectList[id] = &Expr{
				Typ: curr_node.TableDef.Cols[1].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: curr_node.BindingTags[0],
						ColPos: 1, // score
					},
				},
			}
		}
	*/

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

/*
func (builder *QueryBuilder) findPkFromProject(projNode *plan.Node, pkPos int32) []int32 {

	projids := make([]int32, 0)
	for i, expr := range projNode.ProjectList {
		if expr.GetCol() != nil {
			if expr.GetCol().ColPos == pkPos {
				projids = append(projids, int32(i))
			}
		}
	}
	return projids
}

// e.g. SELECT a, L2_DISTANCE(b, '[0, ..]') FROM SRC ORDER BY L2_DISTANCE(b, '[0,..]') limit 4
// the plan is 'project -> sort -> project -> tablescan'
func (builder *QueryBuilder) findEqualDistFnFromProject(projNode *plan.Node, distfn *plan.Function) []int32 {

	projids := make([]int32, 0)
	optype := distfn.Func.ObjName

	for i, expr := range projNode.ProjectList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		if fn.Func.ObjName != optype {
			continue
		}

		// check args

		equal := false
		for j := range distfn.Args {
			targ := distfn.Args[j]
			arg := fn.Args[j]

			if targ.GetCol() != nil && arg.GetCol() != nil && targ.GetCol().ColPos == arg.GetCol().ColPos {
				equal = true
				continue
			}
			if targ.GetF() != nil && arg.GetF() != nil && targ.GetF().Func.ObjName == "cast" && arg.GetF().Func.ObjName == "cast" {
				tv32 := targ.GetF().Args[0].GetLit().GetSval()
				v32 := arg.GetF().Args[0].GetLit().GetSval()
				if tv32 == v32 {
					equal = true
					continue
				}
			}

			equal = false
		}

		if equal {
			projids = append(projids, int32(i))
		}
	}

	return projids
}
*/
