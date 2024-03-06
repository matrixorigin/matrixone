// Copyright 2021 - 2022 Matrix Origin
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
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

func buildInsert(stmt *tree.Insert, ctx CompilerContext, isReplace bool, isPrepareStmt bool) (p *Plan, err error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildInsertHistogram.Observe(time.Since(start).Seconds())
	}()
	if isReplace {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "Not support replace statement")
	}

	tbl := stmt.Table.(*tree.TableName)
	dbName := string(tbl.SchemaName)
	tblName := string(tbl.ObjectName)
	if len(dbName) == 0 {
		dbName = ctx.DefaultDatabase()
	}
	_, t := ctx.Resolve(dbName, tblName)
	if t == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}
	if t.TableType == catalog.SystemStreamRel {
		return nil, moerr.NewNYI(ctx.GetContext(), "insert stream %s", tblName)
	}

	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, nil, nil, "insert")
	if err != nil {
		return nil, err
	}
	rewriteInfo := &dmlSelectInfo{
		typ:     "insert",
		rootId:  -1,
		tblInfo: tblInfo,
	}
	tableDef := tblInfo.tableDefs[0]
	// clusterTable, err := getAccountInfoOfClusterTable(ctx, stmt.Accounts, tableDef, tblInfo.isClusterTable[0])
	// if err != nil {
	// 	return nil, err
	// }
	// if len(stmt.OnDuplicateUpdate) > 0 && clusterTable.IsClusterTable {
	// 	return nil, moerr.NewNotSupported(ctx.GetContext(), "INSERT ... ON DUPLICATE KEY UPDATE ... for cluster table")
	// }

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt)
	builder.haveOnDuplicateKey = len(stmt.OnDuplicateUpdate) > 0

	bindCtx := NewBindContext(builder, nil)
	checkInsertPkDup, pkPosInValues, isInsertWithoutAutoPkCol, insertWithoutUniqueKeyMap, err := initInsertStmt(builder, bindCtx, stmt, rewriteInfo)
	if err != nil {
		return nil, err
	}
	lastNodeId := rewriteInfo.rootId
	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	var pkFilterExprs []*Expr
	var newPartitionExpr *Expr
	if CNPrimaryCheck && len(pkPosInValues) > 0 {
		pkFilterExprs = getPkValueExpr(builder, ctx, tableDef, pkPosInValues)
		// The insert statement subplan with a primary key has undergone manual column pruning in advance,
		// so the partition expression needs to be remapped and judged whether partition pruning can be performed
		newPartitionExpr = remapPartitionExpr(builder, tableDef, pkPosInValues)
	}
	builder.qry.Steps = append(builder.qry.Steps[:sourceStep], builder.qry.Steps[sourceStep+1:]...)

	objRef := tblInfo.objRef[0]
	if len(rewriteInfo.onDuplicateIdx) > 0 {
		// append on duplicate key node
		tableDef = DeepCopyTableDef(tableDef, true)
		if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
			tableDef.Cols = append(tableDef.Cols, tableDef.Pkey.CompPkeyCol)
		}
		if tableDef.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
			tableDef.Cols = append(tableDef.Cols, tableDef.ClusterBy.CompCbkeyCol)
		}

		dupProjection := getProjectionByLastNode(builder, lastNodeId)
		// if table have pk & unique key. we need append an agg node before on_duplicate_key
		if rewriteInfo.onDuplicateNeedAgg {
			colLen := len(tableDef.Cols)
			aggGroupBy := make([]*Expr, 0, colLen)
			aggList := make([]*Expr, 0, len(dupProjection)-colLen)
			aggProject := make([]*Expr, 0, len(dupProjection))
			for i := 0; i < len(dupProjection); i++ {
				if i < colLen {
					aggGroupBy = append(aggGroupBy, &Expr{
						Typ: dupProjection[i].Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								ColPos: int32(i),
							},
						},
					})
					aggProject = append(aggProject, &Expr{
						Typ: dupProjection[i].Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								RelPos: -1,
								ColPos: int32(i),
							},
						},
					})
				} else {
					aggExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "any_value", []*Expr{
						{
							Typ: dupProjection[i].Typ,
							Expr: &plan.Expr_Col{
								Col: &ColRef{
									ColPos: int32(i),
								},
							},
						},
					})
					if err != nil {
						return nil, err
					}
					aggList = append(aggList, aggExpr)
					aggProject = append(aggProject, &Expr{
						Typ: dupProjection[i].Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								RelPos: -2,
								ColPos: int32(i),
							},
						},
					})
				}
			}

			aggNode := &Node{
				NodeType:    plan.Node_AGG,
				Children:    []int32{lastNodeId},
				GroupBy:     aggGroupBy,
				AggList:     aggList,
				ProjectList: aggProject,
			}
			lastNodeId = builder.appendNode(aggNode, bindCtx)
		}

		onDuplicateKeyNode := &Node{
			NodeType:    plan.Node_ON_DUPLICATE_KEY,
			Children:    []int32{lastNodeId},
			ProjectList: dupProjection,
			OnDuplicateKey: &plan.OnDuplicateKeyCtx{
				TableDef:        tableDef,
				OnDuplicateIdx:  rewriteInfo.onDuplicateIdx,
				OnDuplicateExpr: rewriteInfo.onDuplicateExpr,
				IsIgnore:        rewriteInfo.onDuplicateIsIgnore,
			},
		}
		lastNodeId = builder.appendNode(onDuplicateKeyNode, bindCtx)

		// append project node to make batch like update logic, not insert
		updateColLength := 0
		updateColPosMap := make(map[string]int)
		var insertColPos []int
		var projectProjection []*Expr
		tableDef = DeepCopyTableDef(tableDef, true)
		tableDef.Cols = append(tableDef.Cols, MakeRowIdColDef())
		colLength := len(tableDef.Cols)
		rowIdPos := colLength - 1
		for _, col := range tableDef.Cols {
			if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
				continue
			}
			updateColLength++
		}
		for i, col := range tableDef.Cols {
			projectProjection = append(projectProjection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(i + updateColLength),
						Name:   col.Name,
					},
				},
			})
		}
		for i := 0; i < updateColLength; i++ {
			col := tableDef.Cols[i]
			projectProjection = append(projectProjection, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						ColPos: int32(i),
						Name:   col.Name,
					},
				},
			})
			updateColPosMap[col.Name] = colLength + i
			insertColPos = append(insertColPos, colLength+i)
		}
		projectNode := &Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeId},
			ProjectList: projectProjection,
		}
		lastNodeId = builder.appendNode(projectNode, bindCtx)

		// append sink node
		lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
		sourceStep = builder.appendStep(lastNodeId)

		// append plans like update
		updateBindCtx := NewBindContext(builder, nil)
		upPlanCtx := getDmlPlanCtx()
		upPlanCtx.objRef = objRef
		upPlanCtx.tableDef = tableDef
		upPlanCtx.beginIdx = 0
		upPlanCtx.sourceStep = sourceStep
		upPlanCtx.isMulti = false
		upPlanCtx.updateColLength = updateColLength
		upPlanCtx.rowIdPos = rowIdPos
		upPlanCtx.insertColPos = insertColPos
		upPlanCtx.updateColPosMap = updateColPosMap
		upPlanCtx.checkInsertPkDup = checkInsertPkDup

		err = buildUpdatePlans(ctx, builder, updateBindCtx, upPlanCtx)
		if err != nil {
			return nil, err
		}
		putDmlPlanCtx(upPlanCtx)

		query.StmtType = plan.Query_UPDATE
	} else {
		err = buildInsertPlans(ctx, builder, bindCtx, objRef, tableDef, rewriteInfo.rootId, checkInsertPkDup, pkFilterExprs, newPartitionExpr, isInsertWithoutAutoPkCol, insertWithoutUniqueKeyMap)
		if err != nil {
			return nil, err
		}
		query.StmtType = plan.Query_INSERT
	}
	reduceSinkSinkScanNodes(query)
	ReCalcQueryStats(builder, query)
	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

func getPkValueExpr(builder *QueryBuilder, ctx CompilerContext, tableDef *TableDef, pkPosInValues map[int]int) []*Expr {
	if builder.qry.Nodes[0].NodeType != plan.Node_VALUE_SCAN {
		return nil
	}
	pkPos, pkTyp := getPkPos(tableDef, true)
	if pkPos == -1 {
		if tableDef.Pkey.PkeyColName != catalog.CPrimaryKeyColName {
			return nil
		}
	} else if pkTyp.AutoIncr {
		return nil
	}

	node := builder.qry.Nodes[0]

	proc := ctx.GetProcess()
	var bat *batch.Batch
	var err error
	if builder.isPrepareStatement {
		bat = proc.GetPrepareBatch()
	} else {
		bat = proc.GetValueScanBatch(uuid.UUID(node.Uuid))
	}
	rowsCount := bat.RowCount()

	colExprs := make([][]*Expr, len(pkPosInValues))
	pkColLength := len(pkPosInValues)
	var colTyp *Type
	var insertRowIdx int
	var pkColIdx int

	for insertRowIdx, pkColIdx = range pkPosInValues {
		valExprs := make([]*Expr, rowsCount)
		rowTyp := bat.Vecs[insertRowIdx].GetType()
		colTyp = makePlan2Type(rowTyp)

		var varcharTyp *Type
		if rowTyp.Oid == types.T_uuid {
			typ := types.T_varchar.ToType()
			varcharTyp = MakePlan2Type(&typ)
		}

		for _, data := range node.RowsetData.Cols[insertRowIdx].Data {
			rowExpr := DeepCopyExpr(data.Expr)
			e, err := forceCastExpr(builder.GetContext(), rowExpr, colTyp)
			if err != nil {
				return nil
			}
			valExprs[data.RowPos] = e
		}

		for i := 0; i < rowsCount; i++ {
			if valExprs[i] == nil {
				if bat.Vecs[insertRowIdx].GetType().Oid == types.T_uuid {
					// we have not uuid type in plan.Const. so use string & cast string to uuid
					val := vector.MustFixedCol[types.Uuid](bat.Vecs[insertRowIdx])[i]
					constExpr := &plan.Expr{
						Typ: varcharTyp,
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_Sval{
									Sval: val.ToString(),
								},
							},
						},
					}
					valExprs[i], err = appendCastBeforeExpr(proc.Ctx, constExpr, colTyp, false)
					if err != nil {
						return nil
					}
				} else {
					constExpr := rule.GetConstantValue(bat.Vecs[insertRowIdx], true, uint64(i))
					if constExpr == nil {
						return nil
					}
					valExprs[i] = &plan.Expr{
						Typ: colTyp,
						Expr: &plan.Expr_Lit{
							Lit: constExpr,
						},
					}
				}
			}
		}
		colExprs[pkColIdx] = valExprs
	}

	if pkColLength == 1 {
		if rowsCount > 1 {
			// args in list must be constant
			expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "in", []*Expr{{
				Typ: colTyp,
				Expr: &plan.Expr_Col{
					Col: &ColRef{
						ColPos: int32(pkColIdx),
						Name:   tableDef.Pkey.PkeyColName,
					},
				},
			}, {
				Expr: &plan.Expr_List{
					List: &plan.ExprList{
						List: colExprs[0],
					},
				},
				Typ: &plan.Type{
					Id: int32(types.T_tuple),
				},
			}})
			if err != nil {
				return nil
			}
			expr, err = ConstantFold(batch.EmptyForConstFoldBatch, expr, proc, false)
			if err != nil {
				return nil
			}
			return []*Expr{expr}
		} else {
			var orExpr *Expr
			for i := 0; i < rowsCount; i++ {
				expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{{
					Typ: colTyp,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							ColPos: int32(pkColIdx),
							Name:   tableDef.Pkey.PkeyColName,
						},
					},
				}, colExprs[0][i]})
				if err != nil {
					return nil
				}

				if i == 0 {
					orExpr = expr
				} else {
					orExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*Expr{orExpr, expr})
					if err != nil {
						return nil
					}
				}
			}
			return []*Expr{orExpr}
		}
	} else {
		// multi cols pk & one row for insert
		if rowsCount == 1 {
			filterExprs := make([]*Expr, pkColLength)
			for insertRowIdx, pkColIdx = range pkPosInValues {
				expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{{
					Typ: tableDef.Cols[insertRowIdx].Typ,
					Expr: &plan.Expr_Col{
						Col: &ColRef{
							ColPos: int32(pkColIdx),
							Name:   tableDef.Cols[insertRowIdx].Name,
						},
					},
				}, colExprs[pkColIdx][0]})
				if err != nil {
					return nil
				}
				filterExprs[pkColIdx] = expr
			}
			return filterExprs
		} else {
			// seems serial function have poor performance. we have to use or function
			var orExpr *Expr
			for i := 0; i < rowsCount; i++ {
				var andExpr *Expr
				for insertRowIdx, pkColIdx = range pkPosInValues {
					eqExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{{
						Typ: tableDef.Cols[insertRowIdx].Typ,
						Expr: &plan.Expr_Col{
							Col: &ColRef{
								ColPos: int32(pkColIdx),
								Name:   tableDef.Cols[insertRowIdx].Name,
							},
						},
					}, colExprs[pkColIdx][i]})
					if err != nil {
						return nil
					}

					if andExpr == nil {
						andExpr = eqExpr
					} else {
						andExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*Expr{andExpr, eqExpr})
						if err != nil {
							return nil
						}
					}
				}

				if i == 0 {
					orExpr = andExpr
				} else {
					orExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*Expr{orExpr, andExpr})
					if err != nil {
						return nil
					}
				}
			}
			return []*Expr{orExpr}
		}
	}
}

// remapPartitionExpr Remap partition expression column references
func remapPartitionExpr(builder *QueryBuilder, tableDef *TableDef, pkPosInValues map[int]int) *Expr {
	if builder.qry.Nodes[0].NodeType != plan.Node_VALUE_SCAN {
		return nil
	}

	if tableDef.Partition == nil {
		return nil
	} else {
		partitionExpr := DeepCopyExpr(tableDef.Partition.PartitionExpression)
		if remapPartExprColRef(partitionExpr, pkPosInValues, tableDef) {
			return partitionExpr
		}
		return nil
	}
}

// remapPartExprColRef Remap partition expression column references
func remapPartExprColRef(expr *Expr, colMap map[int]int, tableDef *TableDef) bool {
	switch ne := expr.Expr.(type) {
	case *plan.Expr_Col:
		cPos := ne.Col.ColPos
		if ids, ok := colMap[int(cPos)]; ok {
			ne.Col.RelPos = 0
			ne.Col.ColPos = int32(ids)
			ne.Col.Name = tableDef.Cols[cPos].Name
		} else {
			return false
		}

	case *plan.Expr_F:
		for _, arg := range ne.F.GetArgs() {
			if res := remapPartExprColRef(arg, colMap, tableDef); !res {
				return false
			}
		}

	case *plan.Expr_W:
		if res := remapPartExprColRef(ne.W.WindowFunc, colMap, tableDef); !res {
			return false
		}

		for _, arg := range ne.W.PartitionBy {
			if res := remapPartExprColRef(arg, colMap, tableDef); !res {
				return false
			}
		}
		for _, order := range ne.W.OrderBy {
			if res := remapPartExprColRef(order.Expr, colMap, tableDef); !res {
				return false
			}
		}
	}
	return true
}
