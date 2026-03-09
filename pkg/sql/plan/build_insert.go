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
	"context"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

	_, t, err := ctx.Resolve(dbName, tblName, nil)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}
	if t.TableType == catalog.SystemSourceRel {
		return nil, moerr.NewNYIf(ctx.GetContext(), "insert stream %s", tblName)
	}

	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, stmt.With, nil, "insert")
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

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt, false)
	builder.haveOnDuplicateKey = len(stmt.OnDuplicateUpdate) > 0
	if stmt.IsRestore {
		builder.isRestore = true
		if stmt.IsRestoreByTs {
			builder.isRestoreByTs = true
		}
		oldSnapshot := builder.compCtx.GetSnapshot()
		builder.compCtx.SetSnapshot(&Snapshot{
			Tenant: &plan.SnapshotTenant{
				TenantName: "xxx",
				TenantID:   stmt.FromDataTenantID,
			},
		})
		defer func() {
			builder.compCtx.SetSnapshot(oldSnapshot)
		}()
	}

	bindCtx := NewBindContext(builder, nil)

	// Pass WITH clause from INSERT to SELECT if present
	if stmt.With != nil && stmt.Rows != nil {
		stmt.Rows.With = stmt.With
	}

	ifExistAutoPkCol, insertWithoutUniqueKeyMap, ifInsertFromUniqueColMap, err := initInsertStmt(builder, bindCtx, stmt, rewriteInfo)
	if err != nil {
		return nil, err
	}
	replaceStmt := getRewriteToReplaceStmt(tableDef, stmt, rewriteInfo, isPrepareStmt)
	if replaceStmt != nil {
		return buildReplace(replaceStmt, ctx, isPrepareStmt, true)
	}
	lastNodeId := rewriteInfo.rootId
	sourceStep := builder.appendStep(lastNodeId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
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
				SpillMem:    builder.aggSpillMem,
			}
			lastNodeId = builder.appendNode(aggNode, bindCtx)
		}
		// construct the attrs and insertColCount for on_duplicate_key node
		attrs := make([]string, 0)
		insertColCount := int32(0)
		for _, col := range tableDef.Cols {
			if col.Hidden && col.Name != catalog.FakePrimaryKeyColName {
				continue
			}
			attrs = append(attrs, col.GetOriginCaseName())
			insertColCount++
		}
		for _, col := range tableDef.Cols {
			attrs = append(attrs, col.GetOriginCaseName())
		}
		attrs = append(attrs, catalog.Row_ID)
		uniqueColWithIdx, _ := GetUniqueColAndIdxFromTableDef(tableDef)
		uniqueColCheckExpr, err := GenUniqueColCheckExpr(ctx.GetContext(), tableDef, uniqueColWithIdx, int(insertColCount))
		if err != nil {
			return nil, err
		}
		uniqueCol := make([]string, len(uniqueColWithIdx))
		for i := range uniqueColWithIdx {
			keys := make([]string, 0)
			for k := range uniqueColWithIdx[i] {
				keys = append(keys, k)
			}
			uniqueCol[i] = strings.Join(keys, ",")
		}
		onDuplicateKeyNode := &Node{
			NodeType:    plan.Node_ON_DUPLICATE_KEY,
			Children:    []int32{lastNodeId},
			ProjectList: dupProjection,
			OnDuplicateKey: &plan.OnDuplicateKeyCtx{
				Attrs:              attrs,
				InsertColCount:     insertColCount,
				UniqueColCheckExpr: uniqueColCheckExpr,
				UniqueCols:         uniqueCol,
				OnDuplicateIdx:     rewriteInfo.onDuplicateIdx,
				OnDuplicateExpr:    rewriteInfo.onDuplicateExpr,
				IsIgnore:           rewriteInfo.onDuplicateIsIgnore,
				TableName:          tableDef.Name,
				TableId:            tableDef.TblId,
				TableVersion:       tableDef.Version,
			},
		}
		lastNodeId = builder.appendNode(onDuplicateKeyNode, bindCtx)

		// append project node to make batch like update logic, not insert
		updateColLength := 0
		updateColPosMap := make(map[string]int)
		updatePkCol := false
		var insertColPos []int
		var projectProjection []*Expr
		tableDef = DeepCopyTableDef(tableDef, true)
		tableDef.Cols = append(tableDef.Cols, MakeRowIdColDef())
		colLength := len(tableDef.Cols)
		rowIdPos := colLength - 1
		if tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
			for _, name := range tableDef.Pkey.Names {
				if _, ok := rewriteInfo.onDuplicateExpr[name]; ok {
					updatePkCol = true
				}
			}
		}
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
		upPlanCtx.updatePkCol = updatePkCol

		err = buildUpdatePlans(ctx, builder, updateBindCtx, upPlanCtx, true)
		if err != nil {
			return nil, err
		}
		putDmlPlanCtx(upPlanCtx)

		query.StmtType = plan.Query_UPDATE
	} else {
		err = buildInsertPlans(ctx, builder, bindCtx, stmt, objRef, tableDef, rewriteInfo.rootId, ifExistAutoPkCol, insertWithoutUniqueKeyMap, ifInsertFromUniqueColMap)
		if err != nil {
			return nil, err
		}
		query.StmtType = plan.Query_INSERT
	}
	sqls, err := genSqlsForCheckFKSelfRefer(ctx.GetContext(),
		dbName, tableDef.Name, tableDef.Cols, tableDef.Fkeys)
	if err != nil {
		return nil, err
	}
	query.DetectSqls = sqls
	reduceSinkSinkScanNodes(query)
	builder.tempOptimizeForDML()
	reCheckifNeedLockWholeTable(builder)

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}

// ------------------- pk filter relatived -------------------

// getInsertColsFromStmt retrieves the list of column names to be inserted into a table
// based on the given INSERT statement and table definition.
// If the INSERT statement does not specify the columns, all columns except the fake primary key column
// will be included in the list.
// If the INSERT statement specifies the columns, it validates the column names against the table definition
// and returns an error if any of the column names are invalid.
// The function returns the list of insert columns and an error, if any.
func getInsertColsFromStmt(ctx context.Context, stmt *tree.Insert, tableDef *TableDef) ([]string, error) {
	var insertColsName []string
	colToIdx := make(map[string]int32)
	for i, col := range tableDef.Cols {
		colToIdx[col.Name] = int32(i)
	}
	tableDef.Name2ColIndex = colToIdx
	if stmt.Columns == nil {
		for _, col := range tableDef.Cols {
			if col.Name != catalog.FakePrimaryKeyColName {
				insertColsName = append(insertColsName, col.Name)
			}
		}
	} else {
		for _, column := range stmt.Columns {
			colName := strings.ToLower(string(column))
			if _, ok := colToIdx[colName]; !ok {
				return nil, moerr.NewBadFieldError(ctx, colName, tableDef.Name)
			}
			insertColsName = append(insertColsName, colName)
		}
	}
	return insertColsName, nil
}

// canUsePkFilter checks if the primary key filter can be used for the given insert statement.
// It returns true if the primary key filter can be used, otherwise it returns false.
// The primary key filter can be used if the following conditions are met:
// NOTE : For hidden tables created by UNIQUE INDEX, the situation is more subtle.
//  0. CNPrimaryCheck is true.
//  1. The insert statement is INSERT VALUES type
//  2. table contains primary key
//  3. for auto-incr primary key, must contain corresponding columns, and values must not contain nil.
//  4. performance constraints: (maybe outdated)
//     4.1 for single priamry key and the type of pk is number type, the number of rows being inserted is less than or equal to 20_000
//     4.2 otherwise : the number of rows being inserted is less than or equal to defaultmaxRowThenUnusePkFilterExpr
//
// NOTE : For hidden tables created by UNIQUE INDEX, the situation is more subtle.
//  5. for hidden table created by unique index, need to contain the inserted data column
//     for 3 and 5, after valuescan refactor, insert batch is construct in compile , so we can't get batch  here, return false directly
//
// Otherwise, the primary key filter cannot be used.
func canUsePkFilter(builder *QueryBuilder, ctx CompilerContext, stmt *tree.Insert, tableDef *TableDef, insertColsName []string, uniqueIndexDef *IndexDef) bool {
	var isCompound bool
	var used4UniqueIndex bool // mark if this pkfilter is used for hidden table created by unique index

	if uniqueIndexDef != nil {
		if !uniqueIndexDef.Unique {
			panic("should never happen")
		}
		used4UniqueIndex = true
	}

	if used4UniqueIndex {
		isCompound = len(uniqueIndexDef.Parts) > 1
	} else {
		isCompound = len(tableDef.Pkey.Names) > 1
	}

	if !config.CNPrimaryCheck.Load() {
		return false // break condition 0
	}

	if builder.qry.Nodes[0].NodeType != plan.Node_VALUE_SCAN {
		return false // break condition 1
	}

	// hack, should be removed soon
	if builder.qry.Nodes[0].NodeType == plan.Node_VALUE_SCAN && builder.qry.Nodes[1].NodeType == plan.Node_FUNCTION_SCAN {
		return false // break condition 1
	}

	if used4UniqueIndex {
		return false
	} else {
		// check for auto increment primary key
		pkPos, pkTyp := getPkPos(tableDef, true)
		if pkPos == -1 {
			if tableDef.Pkey.PkeyColName != catalog.CPrimaryKeyColName {
				return false // break condition 2
			}

			pkNameMap := make(map[string]int)
			for pkIdx, pkName := range tableDef.Pkey.Names {
				pkNameMap[pkName] = pkIdx
			}

			autoIncIdx := -1
			for _, col := range tableDef.Cols {
				if _, ok := pkNameMap[col.Name]; ok {
					if col.Typ.AutoIncr {
						foundInStmt := false
						for i, name := range insertColsName {
							if name == col.Name {
								foundInStmt = true
								autoIncIdx = i
								break
							}
						}
						if !foundInStmt {
							// one of pk cols is auto incr col and this col was not in values, break condition 3
							return false
						}
					}
				}
			}

			if autoIncIdx != -1 {
				return false
			}
		} else if pkTyp.AutoIncr { // single auto incr primary key
			return false
		}
	}

	switch slt := stmt.Rows.Select.(type) {
	case *tree.ValuesClause:
		if !isCompound {

			var toCheckColName string
			if !used4UniqueIndex {
				toCheckColName = tableDef.Pkey.PkeyColName
			} else {
				toCheckColName = uniqueIndexDef.Parts[0]
			}

			for i, col := range tableDef.Cols {
				if col.Name == toCheckColName {
					typ := tableDef.Cols[i].Typ
					switch typ.Id {
					case int32(types.T_int8), int32(types.T_int16), int32(types.T_int32), int32(types.T_int64), int32(types.T_int128):
						if len(slt.Rows) > 20_000 {
							return false // break condition 4.1
						}
					case int32(types.T_uint8), int32(types.T_uint16), int32(types.T_uint32), int32(types.T_uint64), int32(types.T_uint128), int32(types.T_bit):
						if len(slt.Rows) > 20_000 {
							return false // break condition 4.1
						}
					default:
						if len(slt.Rows) > defaultmaxRowThenUnusePkFilterExpr {
							return false // break condition 4.2
						}
					}
				}
			}
		} else {
			if len(slt.Rows) > defaultmaxRowThenUnusePkFilterExpr {
				return false // break condition 4.2
			}
		}
	default:
		// TODO(jensenojs):need to support more type, such as load or update ?
		return false
	}

	return true
}

type orderAndIdx struct {
	order int // pkOrder is the order(ignore non-pk cols) in tableDef.Pkey.Names
	index int // pkIndex is the index of the primary key columns in tableDef.Cols
}

type locationMap struct {
	m        map[string]orderAndIdx
	isUnique bool
}

// need to check if the primary key filter can be used before calling this function.
// also need to consider both origin table and hidden table for unique key
func newLocationMap(tableDef *TableDef, uniqueIndexDef *IndexDef) *locationMap {
	if uniqueIndexDef != nil && !uniqueIndexDef.Unique {
		panic("uniqueIndexDef.Unique must be true")
	}

	m := make(map[string]orderAndIdx)
	name2Order := make(map[string]int)
	name2Indx := make(map[string]int)

	if uniqueIndexDef != nil {
		for o, n := range uniqueIndexDef.Parts {
			name2Order[n] = o
		}
	} else {
		for o, n := range tableDef.Pkey.Names {
			name2Order[n] = o
		}
	}

	for i, col := range tableDef.Cols {
		if _, ok := name2Order[col.Name]; ok {
			name2Indx[col.Name] = i
		}
	}
	for name := range name2Indx {
		m[name] = orderAndIdx{name2Order[name], name2Indx[name]}
	}
	return &locationMap{
		m:        m,
		isUnique: uniqueIndexDef != nil,
	}
}

func getPkValueExpr(builder *QueryBuilder, ctx CompilerContext, tableDef *TableDef, lmap *locationMap, insertColsNameFromStmt []string) (pkFilterExprs []*Expr, err error) {
	var pkLocationInfo orderAndIdx
	var ok bool
	var col *ColDef
	proc := ctx.GetProcess()
	node := builder.qry.Nodes[0]
	isCompound := len(lmap.m) > 1
	forUniqueHiddenTable := lmap.isUnique

	// insert pk col with default value, skip build pk filter expr
	insertColMap := make(map[string]bool)
	for _, name := range insertColsNameFromStmt {
		insertColMap[name] = true
	}
	for col := range lmap.m {
		if _, ok := insertColMap[col]; !ok {
			return nil, nil
		}
	}

	// colExprs will store the constant value expressions (or UUID value) for each primary key column by the order in insert value SQL
	// that is, the key part of pkPosInValues, more info see the comment of func getPkOrderInValues
	colExprs := make([][]*Expr, len(lmap.m))
	rowsCount := len(node.RowsetData.Cols[0].Data)
	// If the expression is nil, it creates a constant expression with either the UUID value or a constant value.
	for idx, name := range insertColsNameFromStmt {
		if pkLocationInfo, ok = lmap.m[name]; !ok {
			continue
		}

		col = tableDef.Cols[tableDef.Name2ColIndex[name]]
		valExprs := make([]*Expr, rowsCount)

		for i, data := range node.RowsetData.Cols[idx].Data {
			rowExpr := DeepCopyExpr(data.Expr)
			e, err := forceCastExpr(builder.GetContext(), rowExpr, col.Typ)
			if err != nil {
				return nil, err
			}
			valExprs[i] = e
		}

		colExprs[pkLocationInfo.order] = valExprs
	}

	var filterExpr *plan.Expr

	if !isCompound {
		var colName string
		for n := range lmap.m {
			colName = n
			break
		}
		if forUniqueHiddenTable {
			colName = catalog.IndexTableIndexColName
		}

		pkExpr := &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &ColRef{
					ColPos: 0,
					Name:   colName,
				},
			},
		}

		if rowsCount == 1 {
			// pk = a1 or pk = a2 or pk = a3
			filterExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
				pkExpr,
				colExprs[0][0],
			})
		} else {
			// pk in (a1, a2, a3)
			// args in list must be constant
			filterExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "in", []*Expr{
				pkExpr,
				{
					Typ: pkExpr.Typ,
					Expr: &plan.Expr_List{
						List: &plan.ExprList{
							List: colExprs[0],
						},
					},
				},
			})
		}
	} else {
		var colName string
		var colPos int32
		if forUniqueHiddenTable {
			colName = catalog.IndexTableIndexColName
			colPos = 0
		} else {
			colName = catalog.CPrimaryKeyColName
			colPos = int32(len(tableDef.Pkey.Names))
		}

		pkExpr := &plan.Expr{
			Typ: makeHiddenColTyp(),
			Expr: &plan.Expr_Col{
				Col: &ColRef{
					ColPos: colPos,
					Name:   colName,
				},
			},
		}

		if rowsCount == 1 {
			// ppk1 = a1 and ppk2 = a2 or ppk1 = b1 and ppk2 = b2 or ppk1 = c1 and ppk2 = c2
			serialArgs := make([]*plan.Expr, len(colExprs))
			for i := range colExprs {
				serialArgs[i] = colExprs[i][0]
			}

			serialExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)
			filterExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
				pkExpr,
				serialExpr,
			})
		} else {
			inArgs := make([]*plan.Expr, rowsCount)
			for i := range inArgs {
				serialArgs := make([]*plan.Expr, len(colExprs))
				for j := range colExprs {
					serialArgs[j] = colExprs[j][i]
				}

				inArgs[i], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)
			}
			filterExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "in", []*Expr{
				pkExpr,
				{
					Typ: pkExpr.Typ,
					Expr: &plan.Expr_List{
						List: &plan.ExprList{
							List: inArgs,
						},
					},
				},
			})
		}
	}

	filterExpr, err = ConstantFold(batch.EmptyForConstFoldBatch, filterExpr, proc, false, true)
	if err != nil {
		return nil, nil
	}

	return []*Expr{filterExpr}, nil
}

func getRewriteToReplaceStmt(tableDef *TableDef, stmt *tree.Insert, info *dmlSelectInfo, isPrepareStmt bool) *tree.Replace {
	if len(info.onDuplicateIdx) == 0 {
		return nil
	}
	if _, ok := stmt.Rows.Select.(*tree.ValuesClause); !ok {
		return nil
	}
	if isPrepareStmt {
		return nil
	}
	canUpdateCols := make([]string, 0, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if col.Hidden {
			continue
		}
		canUpdateCols = append(canUpdateCols, col.Name)
	}
	if len(info.onDuplicateExpr) != len(canUpdateCols) {
		return nil
	}

	for colName, colExpr := range info.onDuplicateExpr {
		isUpdateSelf := false
		if expr, ok := colExpr.Expr.(*plan.Expr_F); ok {
			if expr.F.Func.GetObjName() == "values" {
				if cExpr, ok := expr.F.Args[0].Expr.(*plan.Expr_Col); ok {
					if cExpr.Col.Name == colName {
						isUpdateSelf = true
					}
				}
			}
		}
		if !isUpdateSelf {
			return nil
		}
	}

	replaceStmt := &tree.Replace{
		Table:          stmt.Table,
		PartitionNames: stmt.PartitionNames,
		Columns:        stmt.Columns,
		Rows:           stmt.Rows,
	}
	return replaceStmt
}
