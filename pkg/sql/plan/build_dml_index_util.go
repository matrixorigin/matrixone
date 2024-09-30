// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// buildInsertPlansWithRelatedHiddenTable build insert plan recursively for origin table
func buildInsertPlansWithRelatedHiddenTable(
	stmt *tree.Insert, ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef,
	tableDef *TableDef, updateColLength int, sourceStep int32, addAffectedRows bool, isFkRecursionCall bool,
	updatePkCol bool, pkFilterExprs []*Expr, partitionExpr *Expr, ifExistAutoPkCol bool,
	checkInsertPkDupForHiddenIndexTable bool, indexSourceColTypes []*plan.Type, fuzzymessage *OriginTableMessageForFuzzy,
	insertWithoutUniqueKeyMap map[string]bool, ifInsertFromUniqueColMap map[string]bool,
) error {
	//var lastNodeId int32
	var err error

	if builder.isRestore {
		checkInsertPkDupForHiddenIndexTable = false
	}

	multiTableIndexes := make(map[string]*MultiTableIndex)
	if updateColLength == 0 {
		for idx, indexdef := range tableDef.Indexes {
			if indexdef.GetUnique() && (insertWithoutUniqueKeyMap != nil && insertWithoutUniqueKeyMap[indexdef.IndexName]) {
				continue
			}

			// append plan for the hidden tables of unique/secondary keys
			if indexdef.TableExist && catalog.IsRegularIndexAlgo(indexdef.IndexAlgo) {
				/********
				  NOTE: make sure to make the major change applied to secondary index, to IVFFLAT index as well.
				  Else IVFFLAT index would fail
				  ********/

				err = buildPreInsertRegularIndex(stmt, ctx, builder, bindCtx, objRef, tableDef, sourceStep, ifInsertFromUniqueColMap, indexdef, idx)
				if err != nil {
					return err
				}

			} else if indexdef.TableExist && catalog.IsIvfIndexAlgo(indexdef.IndexAlgo) {

				// IVF indexDefs are aggregated and handled later
				if _, ok := multiTableIndexes[indexdef.IndexName]; !ok {
					multiTableIndexes[indexdef.IndexName] = &MultiTableIndex{
						IndexAlgo: catalog.ToLower(indexdef.IndexAlgo),
						IndexDefs: make(map[string]*IndexDef),
					}
				}
				multiTableIndexes[indexdef.IndexName].IndexDefs[catalog.ToLower(indexdef.IndexAlgoTableType)] = indexdef
			} else if indexdef.TableExist && catalog.IsMasterIndexAlgo(indexdef.IndexAlgo) {

				err = buildPreInsertMasterIndex(stmt, ctx, builder, bindCtx, objRef, tableDef, sourceStep, ifInsertFromUniqueColMap, indexdef, idx)
				if err != nil {
					return err
				}
			}
		}
	}

	// build multitable indexes
	buildPreInsertMultiTableIndexes(ctx, builder, bindCtx, objRef, tableDef, sourceStep, multiTableIndexes)

	ifInsertFromUnique := false
	if tableDef.Pkey != nil && ifInsertFromUniqueColMap != nil {
		for _, colName := range tableDef.Pkey.Names {
			if _, exists := ifInsertFromUniqueColMap[colName]; exists {
				ifInsertFromUnique = true
				break
			}
		}
	}

	err = makeOneInsertPlan(ctx, builder, bindCtx, objRef, tableDef,
		updateColLength, sourceStep, addAffectedRows, isFkRecursionCall, updatePkCol,
		pkFilterExprs, partitionExpr, ifExistAutoPkCol, checkInsertPkDupForHiddenIndexTable,
		ifInsertFromUnique, indexSourceColTypes, fuzzymessage)
	if err != nil {
		return err
	}

	return nil
}

// make delete index plans here
func buildDeleteIndexPlans(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, delCtx *dmlPlanCtx) error {

	isUpdate := delCtx.updateColLength > 0

	// delete unique/secondary index table
	// Refer to this PR:https://github.com/matrixorigin/matrixone/pull/12093
	// we have build SK using UK code path. So we might see UK in function signature even thought it could be for
	// both UK and SK. To handle SK case, we will have flags to indicate if it's UK or SK.
	hasUniqueKey := haveUniqueKey(delCtx.tableDef)
	hasSecondaryKey := haveSecondaryKey(delCtx.tableDef)
	canTruncate := delCtx.isDeleteWithoutFilters

	accountId, err := ctx.GetAccountId()
	if err != nil {
		return err
	}

	enabled, err := IsForeignKeyChecksEnabled(ctx)
	if err != nil {
		return err
	}

	if enabled && len(delCtx.tableDef.RefChildTbls) > 0 ||
		delCtx.tableDef.ViewSql != nil ||
		(util.TableIsClusterTable(delCtx.tableDef.GetTableType()) && accountId != catalog.System_Account) ||
		delCtx.objRef.PubInfo != nil {
		canTruncate = false
	}

	if (hasUniqueKey || hasSecondaryKey) && !canTruncate {
		typMap := make(map[string]plan.Type)
		posMap := make(map[string]int)
		colMap := make(map[string]*ColDef)
		for idx, col := range delCtx.tableDef.Cols {
			posMap[col.Name] = idx
			typMap[col.Name] = col.Typ
			colMap[col.Name] = col
		}
		multiTableIndexes := make(map[string]*MultiTableIndex)
		for idx, indexdef := range delCtx.tableDef.Indexes {

			if isUpdate {
				pkeyName := delCtx.tableDef.Pkey.PkeyColName

				// Check if primary key is being updated.
				isPrimaryKeyUpdated := func() bool {
					if pkeyName == catalog.CPrimaryKeyColName {
						// Handle compound primary key.
						for _, pkPartColName := range delCtx.tableDef.Pkey.Names {
							if _, exists := delCtx.updateColPosMap[pkPartColName]; exists || colMap[pkPartColName].OnUpdate != nil {
								return true
							}
						}
					} else if pkeyName == catalog.FakePrimaryKeyColName {
						// Handle programmatically generated primary key.
						if _, exists := delCtx.updateColPosMap[pkeyName]; exists || colMap[pkeyName].OnUpdate != nil {
							return true
						}
					} else {
						// Handle single primary key.
						if _, exists := delCtx.updateColPosMap[pkeyName]; exists || colMap[pkeyName].OnUpdate != nil {
							return true
						}
					}
					return false
				}

				// Check if secondary key is being updated.
				isSecondaryKeyUpdated := func() bool {
					for _, colName := range indexdef.Parts {
						resolvedColName := catalog.ResolveAlias(colName)
						if colIdx, ok := posMap[resolvedColName]; ok {
							col := delCtx.tableDef.Cols[colIdx]
							if _, exists := delCtx.updateColPosMap[resolvedColName]; exists || col.OnUpdate != nil {
								return true
							}
						}
					}
					return false
				}

				if !isPrimaryKeyUpdated() && !isSecondaryKeyUpdated() {
					continue
				}
			}

			if indexdef.TableExist && catalog.IsRegularIndexAlgo(indexdef.IndexAlgo) {

				err = buildDeleteRegularIndex(ctx, builder, bindCtx, delCtx, indexdef, idx, typMap, posMap)
				if err != nil {
					return err
				}

			} else if indexdef.TableExist && catalog.IsIvfIndexAlgo(indexdef.IndexAlgo) {
				// IVF indexDefs are aggregated and handled later
				if _, ok := multiTableIndexes[indexdef.IndexName]; !ok {
					multiTableIndexes[indexdef.IndexName] = &MultiTableIndex{
						IndexAlgo: catalog.ToLower(indexdef.IndexAlgo),
						IndexDefs: make(map[string]*IndexDef),
					}
				}
				multiTableIndexes[indexdef.IndexName].IndexDefs[catalog.ToLower(indexdef.IndexAlgoTableType)] = indexdef
			} else if indexdef.TableExist && catalog.IsMasterIndexAlgo(indexdef.IndexAlgo) {
				err = buildDeleteMasterIndex(ctx, builder, bindCtx, delCtx, indexdef, idx, typMap, posMap)
				if err != nil {
					return err
				}
			}
		}

		buildDeleteMultiTableIndexes(ctx, builder, bindCtx, delCtx, multiTableIndexes)
	}

	return nil
}

// appendPreInsertSkMasterPlan  append preinsert node
func appendPreInsertSkMasterPlan(builder *QueryBuilder,
	bindCtx *BindContext,
	tableDef *TableDef,
	indexIdx int,
	isUpdate bool,
	indexTableDef *TableDef,
	genLastNodeIdFn func() int32) (int32, error) {

	// 1. init details
	idxDef := tableDef.Indexes[indexIdx]
	originPkPos, originPkType := getPkPos(tableDef, false)
	//var rowIdPos int
	//var rowIdType *Type

	colsPos := make(map[string]int)
	colsType := make(map[string]*Type)
	for i, colVal := range tableDef.Cols {
		//if colVal.Name == catalog.Row_ID {
		//      rowIdPos = i
		//      rowIdType = colVal.Typ
		//}
		colsPos[colVal.Name] = i
		colsType[colVal.Name] = &tableDef.Cols[i].Typ
	}

	var lastNodeId int32

	// 2. build single project or union based on the number of index parts.
	// NOTE: Union with single child will cause panic.
	if len(idxDef.Parts) == 0 {
		return -1, moerr.NewInternalErrorNoCtx("index parts is empty. file a bug")
	} else if len(idxDef.Parts) == 1 {
		// 2.a build single project
		projectNode, err := buildSerialFullAndPKColsProjMasterIndex(builder, bindCtx, tableDef, genLastNodeIdFn, originPkPos, idxDef.Parts[0], colsType, colsPos, originPkType)
		if err != nil {
			return -1, err
		}
		lastNodeId = builder.appendNode(projectNode, bindCtx)
	} else {
		// 2.b build union in pairs. ie union(c, union(b,a))

		// a) build all the projects
		var unionChildren []int32
		for _, part := range idxDef.Parts {
			// 2.b.i build project
			projectNode, err := buildSerialFullAndPKColsProjMasterIndex(builder, bindCtx, tableDef, genLastNodeIdFn, originPkPos, part, colsType, colsPos, originPkType)
			if err != nil {
				return -1, err
			}
			// 2.b.ii add to union's list
			unionChildren = append(unionChildren, builder.appendNode(projectNode, bindCtx))
		}

		// b) get projectList
		outputProj := getProjectionByLastNode(builder, unionChildren[0])

		// c) build union in pairs
		lastNodeId = unionChildren[0]
		for _, nextProjectId := range unionChildren[1:] { // NOTE: we start from the 2nd item
			lastNodeId = builder.appendNode(&plan.Node{
				NodeType:    plan.Node_UNION,
				Children:    []int32{nextProjectId, lastNodeId},
				ProjectList: outputProj,
			}, bindCtx)
		}

		// NOTE: we could merge the len==1 and len>1 cases, but keeping it separate to make help understand how the
		// union works (ie it works in pairs)
	}
	// 3. add lock
	if lockNodeId, ok := appendLockNode(
		builder,
		bindCtx,
		lastNodeId,
		indexTableDef,
		false,
		false,
		-1,
		nil,
		isUpdate,
	); ok {
		lastNodeId = lockNodeId
	}

	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	newSourceStep := builder.appendStep(lastNodeId)

	return newSourceStep, nil
}

func recomputeMoCPKeyViaProjection(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, lastNodeId int32, posOriginPk int) int32 {
	if tableDef.Pkey != nil && tableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
		lastProject := builder.qry.Nodes[lastNodeId].ProjectList

		projectProjection := make([]*Expr, len(lastProject))
		for i := 0; i < len(lastProject); i++ {
			projectProjection[i] = &plan.Expr{
				Typ: lastProject[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						//Name:   "col" + strconv.FormatInt(int64(i), 10),
					},
				},
			}
		}

		if tableDef.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
			// pkNamesMap := make(map[string]int)
			prikeyPos := make([]int, 0)
			for _, name := range tableDef.Pkey.Names {
				for i, coldef := range tableDef.Cols {
					if coldef.Name == name {
						prikeyPos = append(prikeyPos, i)
						break
					}
				}
			}

			serialArgs := make([]*plan.Expr, len(prikeyPos))
			for i, position := range prikeyPos {
				serialArgs[i] = &plan.Expr{
					Typ: lastProject[position].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: int32(position),
							Name:   tableDef.Cols[position].Name,
						},
					},
				}
			}
			compkey, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)
			projectProjection[posOriginPk] = compkey
		} else {
			pkPos := -1
			for i, coldef := range tableDef.Cols {
				if tableDef.Pkey.PkeyColName == coldef.Name {
					pkPos = i
					break
				}
			}
			if pkPos != -1 {
				projectProjection[posOriginPk] = &plan.Expr{
					Typ: lastProject[pkPos].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: int32(pkPos),
							Name:   tableDef.Pkey.PkeyColName,
						},
					},
				}
			}
		}
		projectNode := &Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{lastNodeId},
			ProjectList: projectProjection,
		}
		lastNodeId = builder.appendNode(projectNode, bindCtx)
	}
	return lastNodeId
}

func buildSerialFullAndPKColsProjMasterIndex(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, genLastNodeIdFn func() int32, originPkPos int, part string, colsType map[string]*Type, colsPos map[string]int, originPkType Type) (*Node, error) {
	var err error
	// 1. get new source sink
	var currLastNodeId = genLastNodeIdFn()

	//2. recompute CP PK.
	currLastNodeId = recomputeMoCPKeyViaProjection(builder, bindCtx, tableDef, currLastNodeId, originPkPos)

	//3. add a new project for < serial_full("0", a, pk), pk >
	projectProjection := make([]*Expr, 2)

	//3.i build serial_full("0", a, pk)
	serialArgs := make([]*plan.Expr, 3)
	serialArgs[0] = makePlan2StringConstExprWithType(getColSeqFromColDef(tableDef.Cols[colsPos[part]]))
	serialArgs[1] = &Expr{
		Typ: *colsType[part],
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: int32(colsPos[part]),
				Name:   part,
			},
		},
	}
	serialArgs[2] = &Expr{
		Typ: originPkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: int32(originPkPos),
				Name:   tableDef.Cols[originPkPos].Name,
			},
		},
	}
	projectProjection[0], err = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", serialArgs)
	if err != nil {
		return nil, err
	}

	//3.ii build pk
	projectProjection[1] = &Expr{
		Typ: originPkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: int32(originPkPos),
				Name:   tableDef.Cols[originPkPos].Name,
			},
		},
	}

	// TODO: verify this with Feng, Ouyuanning and Qingx (not reusing the row_id)
	//if isUpdate {
	//      // 2.iii add row_id if Update
	//      projectProjection = append(projectProjection, &plan.Expr{
	//              Typ: rowIdType,
	//              Expr: &plan.Expr_Col{
	//                      Col: &plan.ColRef{
	//                              RelPos: 0,
	//                              ColPos: int32(rowIdPos),
	//                              Name:   catalog.Row_ID,
	//                      },
	//              },
	//      })
	//}

	projectNode := &Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{currLastNodeId},
		ProjectList: projectProjection,
	}
	return projectNode, nil
}

func appendPreInsertSkVectorPlan(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef, lastNodeId int32, multiTableIndex *MultiTableIndex, isUpdate bool, idxRefs []*ObjectRef, indexTableDefs []*TableDef) (int32, error) {

	//1.a get vector & pk column details
	var posOriginPk, posOriginVecColumn int
	var typeOriginPk, typeOriginVecColumn Type
	{
		colsMap := make(map[string]int)
		colTypes := make([]Type, len(tableDef.Cols))
		for i, col := range tableDef.Cols {
			colsMap[col.Name] = i
			colTypes[i] = tableDef.Cols[i].Typ
		}

		for _, part := range multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].Parts {
			if i, ok := colsMap[part]; ok {
				posOriginVecColumn = i
				typeOriginVecColumn = tableDef.Cols[i].Typ
				break
			}
		}

		posOriginPk, typeOriginPk = getPkPos(tableDef, false)
	}

	//1.b Handle mo_cp_key
	lastNodeId = recomputeMoCPKeyViaProjection(builder, bindCtx, tableDef, lastNodeId, posOriginPk)

	// 2. scan meta table to find the `current version` number
	metaCurrVersionRow, err := makeMetaTblScanWhereKeyEqVersion(builder, bindCtx, indexTableDefs, idxRefs)
	if err != nil {
		return -1, err
	}

	// 3. create a scan node for centroids x meta on centroids.version = cast (meta.version as bigint)
	currVersionCentroids, err := makeCrossJoinCentroidsMetaForCurrVersion(builder, bindCtx,
		indexTableDefs, idxRefs, metaCurrVersionRow)
	if err != nil {
		return -1, err
	}

	// 4. create "CrossJoinL2" on tbl x centroids
	joinTblAndCentroidsUsingCrossL2Join := makeTblCrossJoinL2Centroids(builder, bindCtx, tableDef, lastNodeId, currVersionCentroids, typeOriginPk, posOriginPk, typeOriginVecColumn, posOriginVecColumn)

	// 5. Create a Project with CP Key for LockNode
	projectWithCpKey, err := makeFinalProject(builder, bindCtx, joinTblAndCentroidsUsingCrossL2Join)
	if err != nil {
		return -1, err
	}

	lastNodeId = projectWithCpKey

	if lockNodeId, ok := appendLockNode(
		builder,
		bindCtx,
		lastNodeId,
		indexTableDefs[2],
		false,
		false,
		-1,
		nil,
		isUpdate,
	); ok {
		lastNodeId = lockNodeId
	}

	lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
	sourceStep := builder.appendStep(lastNodeId)

	return sourceStep, nil
}

func appendDeleteIndexTablePlan(
	builder *QueryBuilder,
	bindCtx *BindContext,
	uniqueObjRef *ObjectRef,
	uniqueTableDef *TableDef,
	indexdef *IndexDef,
	typMap map[string]plan.Type,
	posMap map[string]int,
	baseNodeId int32,
	isUK bool,
) (int32, error) {
	/********
	  NOTE: make sure to make the major change applied to secondary index, to IVFFLAT index as well.
	  Else IVFFLAT index would fail
	  ********/
	lastNodeId := baseNodeId
	var err error
	projectList := getProjectionByLastNodeForRightJoin(builder, lastNodeId)
	rfTag := builder.genNewMsgTag()

	var rightRowIdPos int32 = -1
	var rightPkPos int32 = -1
	scanNodeProject := make([]*Expr, len(uniqueTableDef.Cols))
	for colIdx, col := range uniqueTableDef.Cols {
		if col.Name == catalog.Row_ID {
			rightRowIdPos = int32(colIdx)
		} else if col.Name == catalog.IndexTableIndexColName {
			rightPkPos = int32(colIdx)
		}
		scanNodeProject[colIdx] = &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(colIdx),
					Name:   col.Name,
				},
			},
		}
	}
	pkTyp := uniqueTableDef.Cols[rightPkPos].Typ

	probeExpr := &plan.Expr{
		Typ: pkTyp,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				Name: uniqueTableDef.Pkey.PkeyColName,
			},
		},
	}

	leftscan := &plan.Node{
		NodeType:               plan.Node_TABLE_SCAN,
		Stats:                  &plan.Stats{},
		ObjRef:                 uniqueObjRef,
		TableDef:               uniqueTableDef,
		ProjectList:            scanNodeProject,
		RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{MakeRuntimeFilter(rfTag, false, 0, probeExpr)},
	}
	leftId := builder.appendNode(leftscan, bindCtx)
	leftscan.Stats.ForceOneCN = true //to avoid bugs ,maybe refactor in the future

	// append projection
	projectList = append(projectList, &plan.Expr{
		Typ: uniqueTableDef.Cols[rightRowIdPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: rightRowIdPos,
				Name:   catalog.Row_ID,
			},
		},
	}, &plan.Expr{
		Typ: uniqueTableDef.Cols[rightPkPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: rightPkPos,
				Name:   catalog.IndexTableIndexColName,
			},
		},
	})

	rightExpr := &plan.Expr{
		Typ: uniqueTableDef.Cols[rightPkPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: rightPkPos,
				Name:   catalog.IndexTableIndexColName,
			},
		},
	}

	// append join node
	var joinConds []*Expr
	var leftExpr *Expr
	partsLength := len(indexdef.Parts)
	if partsLength == 1 {
		orginIndexColumnName := indexdef.Parts[0]
		typ := typMap[orginIndexColumnName]
		leftExpr = &Expr{
			Typ: typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 1,
					ColPos: int32(posMap[orginIndexColumnName]),
					Name:   orginIndexColumnName,
				},
			},
		}
	} else {
		args := make([]*Expr, partsLength)
		for i, column := range indexdef.Parts {
			column = catalog.ResolveAlias(column)
			typ := typMap[column]
			args[i] = &plan.Expr{
				Typ: typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: int32(posMap[column]),
						Name:   column,
					},
				},
			}
		}
		if isUK {
			// use for UK
			// 0: serial(part1, part2) <----
			// 1: serial(pk1, pk2)
			leftExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", args)
		} else {
			// only used for regular secondary index's 0'th column
			// 0: serial_full(part1, part2, serial(pk1, pk2)) <----
			// 1: serial(pk1, pk2)
			leftExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", args)
		}
		if err != nil {
			return -1, err
		}
	}

	condExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{rightExpr, leftExpr})
	if err != nil {
		return -1, err
	}
	joinConds = []*Expr{condExpr}

	buildExpr := &plan.Expr{
		Typ: pkTyp,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 0,
			},
		},
	}

	/*
	   For the hidden table of the secondary index, there will be no null situation, so there is no need to use right join
	   For why right join is needed, you can consider the following SQL :

	   create table t1(a int, b int, c int, unique key(a));
	   insert into t1 values(null, 1, 1);
	   explain verbose update t1 set a = 1 where a is null;
	*/
	joinType := plan.Node_INNER
	if isUK {
		joinType = plan.Node_RIGHT
	}

	sid := builder.compCtx.GetProcess().GetService()
	lastNodeId = builder.appendNode(&plan.Node{
		NodeType:               plan.Node_JOIN,
		Children:               []int32{leftId, lastNodeId},
		JoinType:               joinType,
		OnList:                 joinConds,
		ProjectList:            projectList,
		RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{MakeRuntimeFilter(rfTag, false, GetInFilterCardLimitOnPK(sid, builder.qry.Nodes[leftId].Stats.TableCnt), buildExpr)},
	}, bindCtx)
	recalcStatsByRuntimeFilter(builder.qry.Nodes[leftId], builder.qry.Nodes[lastNodeId], builder)
	return lastNodeId, nil
}

func appendDeleteMasterTablePlan(builder *QueryBuilder, bindCtx *BindContext,
	masterObjRef *ObjectRef, masterTableDef *TableDef,
	baseNodeId int32, tableDef *TableDef, indexDef *plan.IndexDef,
	typMap map[string]plan.Type, posMap map[string]int) (int32, error) {

	originPkColumnPos, originPkType := getPkPos(tableDef, false)

	lastNodeId := baseNodeId
	projectList := getProjectionByLastNode(builder, lastNodeId)

	var rightRowIdPos int32 = -1
	var rightPkPos int32 = -1
	scanNodeProject := make([]*Expr, len(masterTableDef.Cols))
	for colIdx, colVal := range masterTableDef.Cols {

		if colVal.Name == catalog.Row_ID {
			rightRowIdPos = int32(colIdx)
		} else if colVal.Name == catalog.MasterIndexTableIndexColName {
			rightPkPos = int32(colIdx)
		}

		scanNodeProject[colIdx] = &plan.Expr{
			Typ: colVal.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(colIdx),
					Name:   colVal.Name,
				},
			},
		}
	}

	rightId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      masterObjRef,
		TableDef:    masterTableDef,
		ProjectList: scanNodeProject,
	}, bindCtx)

	// join conditions
	// Example :-
	//  ( (serial_full('1', a, c) = __mo_index_idx_col) or (serial_full('1', b, c) = __mo_index_idx_col) )
	var joinConds *Expr
	for idx, part := range indexDef.Parts {
		// serial_full("colPos", col1, pk)
		var leftExpr *Expr
		leftExprArgs := make([]*Expr, 3)
		leftExprArgs[0] = makePlan2StringConstExprWithType(getColSeqFromColDef(tableDef.Cols[posMap[part]]))
		leftExprArgs[1] = &Expr{
			Typ: typMap[part],
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(posMap[part]),
					Name:   part,
				},
			},
		}
		leftExprArgs[2] = &Expr{
			Typ: originPkType,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(originPkColumnPos),
					Name:   tableDef.Pkey.PkeyColName,
				},
			},
		}
		leftExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", leftExprArgs)
		if err != nil {
			return -1, err
		}

		var rightExpr = &plan.Expr{
			Typ: masterTableDef.Cols[rightPkPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 1,
					ColPos: rightPkPos,
					Name:   catalog.MasterIndexTableIndexColName,
				},
			},
		}
		currCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
		if err != nil {
			return -1, err
		}
		if idx == 0 {
			joinConds = currCond
		} else {
			joinConds, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*plan.Expr{joinConds, currCond})
			if err != nil {
				return -1, err
			}
		}
	}

	projectList = append(projectList, &plan.Expr{
		Typ: masterTableDef.Cols[rightRowIdPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: rightRowIdPos,
				Name:   catalog.Row_ID,
			},
		},
	}, &plan.Expr{
		Typ: masterTableDef.Cols[rightPkPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: rightPkPos,
				Name:   catalog.MasterIndexTableIndexColName,
			},
		},
	})
	lastNodeId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_LEFT,
		Children:    []int32{lastNodeId, rightId},
		OnList:      []*Expr{joinConds},
		ProjectList: projectList,
	}, bindCtx)

	return lastNodeId, nil
}

func appendDeleteIvfTablePlan(builder *QueryBuilder, bindCtx *BindContext,
	entriesObjRef *ObjectRef, entriesTableDef *TableDef,
	baseNodeId int32, tableDef *TableDef) (int32, error) {

	originPkColumnPos, originPkType := getPkPos(tableDef, false)

	lastNodeId := baseNodeId
	var err error
	projectList := getProjectionByLastNode(builder, lastNodeId)

	var entriesRowIdPos int32 = -1
	var entriesFkPkColPos int32 = -1
	var entriesCpPkColPos int32 = -1
	var cpPkType = types.T_varchar.ToType()
	scanNodeProject := make([]*Expr, len(entriesTableDef.Cols))
	for colIdx, col := range entriesTableDef.Cols {
		if col.Name == catalog.Row_ID {
			entriesRowIdPos = int32(colIdx)
		} else if col.Name == catalog.SystemSI_IVFFLAT_TblCol_Entries_pk {
			entriesFkPkColPos = int32(colIdx)
		} else if col.Name == catalog.CPrimaryKeyColName {
			entriesCpPkColPos = int32(colIdx)
		}
		scanNodeProject[colIdx] = &plan.Expr{
			Typ: col.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(colIdx),
					Name:   col.Name,
				},
			},
		}
	}
	rightId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      entriesObjRef,
		TableDef:    entriesTableDef,
		ProjectList: scanNodeProject,
	}, bindCtx)

	// append projection
	projectList = append(projectList,
		&plan.Expr{
			Typ: entriesTableDef.Cols[entriesRowIdPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 1,
					ColPos: entriesRowIdPos,
					Name:   catalog.Row_ID,
				},
			},
		},
		&plan.Expr{
			Typ: makePlan2Type(&cpPkType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 1,
					ColPos: entriesCpPkColPos,
					Name:   catalog.CPrimaryKeyColName,
				},
			},
		},
	)

	rightExpr := &plan.Expr{
		Typ: entriesTableDef.Cols[entriesFkPkColPos].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: entriesFkPkColPos,
				Name:   catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
			},
		},
	}
	// append join node
	var joinConds []*Expr
	var leftExpr = &plan.Expr{
		Typ: originPkType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: int32(originPkColumnPos),
				Name:   tableDef.Cols[originPkColumnPos].Name,
			},
		},
	}

	/*
	   Some notes:
	   1. Primary key of entries table is a <version,origin_pk> pair.
	   2. In the Join condition we are only using origin_pk and not serial(version,origin_pk). For this reason,
	      we will be deleting older version entries as well, so keep in mind that older version entries are stale.
	   3. The same goes with inserts as well. We only update the current version. Due to this reason, updates will cause
	      older versions of the entries to be stale.
	*/

	condExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{leftExpr, rightExpr})
	if err != nil {
		return -1, err
	}
	joinConds = []*Expr{condExpr}

	lastNodeId = builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_LEFT,
		Children:    []int32{lastNodeId, rightId},
		OnList:      joinConds,
		ProjectList: projectList,
	}, bindCtx)
	return lastNodeId, nil
}

func buildPreInsertMultiTableIndexes(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef,
	sourceStep int32, multiTableIndexes map[string]*MultiTableIndex) error {
	var lastNodeId int32

	for _, multiTableIndex := range multiTableIndexes {

		switch multiTableIndex.IndexAlgo {
		case catalog.MoIndexIvfFlatAlgo.ToString():
			lastNodeId = appendSinkScanNode(builder, bindCtx, sourceStep)
			var idxRefs = make([]*ObjectRef, 3)
			var idxTableDefs = make([]*TableDef, 3)
			// TODO: node should hold snapshot and account info
			//idxRefs[0], idxTableDefs[0] = ctx.Resolve(objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName, timestamp.Timestamp{})
			//idxRefs[1], idxTableDefs[1] = ctx.Resolve(objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName, timestamp.Timestamp{})
			//idxRefs[2], idxTableDefs[2] = ctx.Resolve(objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, timestamp.Timestamp{})

			idxRefs[0], idxTableDefs[0] = ctx.Resolve(objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName, nil)
			idxRefs[1], idxTableDefs[1] = ctx.Resolve(objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName, nil)
			idxRefs[2], idxTableDefs[2] = ctx.Resolve(objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, nil)

			// remove row_id
			for i := range idxTableDefs {
				idxTableDefs[i].Cols = RemoveIf[*ColDef](idxTableDefs[i].Cols, func(column *ColDef) bool {
					return column.Name == catalog.Row_ID
				})
			}

			newSourceStep, err := appendPreInsertSkVectorPlan(builder, bindCtx, tableDef, lastNodeId, multiTableIndex, false, idxRefs, idxTableDefs)
			if err != nil {
				return err
			}

			//TODO: verify with zengyan1 if colType should read from original table.
			// It is mainly used for retaining decimal datatype precision in error messages.
			colTypes := make([]*plan.Type, len(tableDef.Cols))
			for i := range tableDef.Cols {
				colTypes[i] = &tableDef.Cols[i].Typ
			}

			updateColLength := 0
			addAffectedRows := false
			isFkRecursionCall := false
			updatePkCol := true
			ifExistAutoPkCol := false
			ifCheckPkDup := false
			var pkFilterExprs []*Expr
			var partitionExpr *Expr
			var fuzzymessage *OriginTableMessageForFuzzy
			var ifInsertFromUnique bool
			err = makeOneInsertPlan(ctx, builder, bindCtx, idxRefs[2], idxTableDefs[2],
				updateColLength, newSourceStep, addAffectedRows, isFkRecursionCall, updatePkCol,
				pkFilterExprs, partitionExpr, ifExistAutoPkCol, ifCheckPkDup, ifInsertFromUnique,
				colTypes, fuzzymessage)

			if err != nil {
				return err
			}
		default:
			return moerr.NewInvalidInputNoCtxf("Unsupported index algorithm: %s", multiTableIndex.IndexAlgo)
		}
	}
	return nil

}

func buildDeleteMultiTableIndexes(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, delCtx *dmlPlanCtx, multiTableIndexes map[string]*MultiTableIndex) error {
	isUpdate := delCtx.updateColLength > 0

	for _, multiTableIndex := range multiTableIndexes {
		switch multiTableIndex.IndexAlgo {
		case catalog.MoIndexIvfFlatAlgo.ToString():

			// Used by pre-insert vector index.
			var idxRefs = make([]*ObjectRef, 3)
			var idxTableDefs = make([]*TableDef, 3)
			// TODO: plan node should hold snapshot and account info
			//idxRefs[0], idxTableDefs[0] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName, timestamp.Timestamp{})
			//idxRefs[1], idxTableDefs[1] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName, timestamp.Timestamp{})
			//idxRefs[2], idxTableDefs[2] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, timestamp.Timestamp{})

			idxRefs[0], idxTableDefs[0] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName, nil)
			idxRefs[1], idxTableDefs[1] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName, nil)
			idxRefs[2], idxTableDefs[2] = ctx.Resolve(delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, nil)

			entriesObjRef, entriesTableDef := idxRefs[2], idxTableDefs[2]
			if entriesTableDef == nil {
				return moerr.NewNoSuchTable(builder.GetContext(), delCtx.objRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexName)
			}

			var lastNodeId int32
			var err error
			var entriesDeleteIdx int
			var entriesTblPkPos int
			var entriesTblPkTyp Type

			if delCtx.isDeleteWithoutFilters {
				lastNodeId, err = appendDeleteIndexTablePlanWithoutFilters(builder, bindCtx, entriesObjRef, entriesTableDef)
				entriesDeleteIdx = getRowIdPos(entriesTableDef)
				entriesTblPkPos, entriesTblPkTyp = getPkPos(entriesTableDef, false)
			} else {
				lastNodeId = appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
				lastNodeId, err = appendDeleteIvfTablePlan(builder, bindCtx, entriesObjRef, entriesTableDef, lastNodeId, delCtx.tableDef)
				entriesDeleteIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength // eg:- <id, embedding, row_id, <... update_col> > + 0/1
				entriesTblPkPos = entriesDeleteIdx + 1                                // this is the compound primary key of the entries table
				entriesTblPkTyp = entriesTableDef.Cols[4].Typ                         // 4'th column is the compound primary key <version,id, org_pk,org_embedding, cp_pk, row_id>
			}

			if err != nil {
				return err
			}

			if isUpdate {
				// do it like simple update
				lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
				newSourceStep := builder.appendStep(lastNodeId)
				// delete uk plan
				{
					//sink_scan -> lock -> delete
					lastNodeId = appendSinkScanNode(builder, bindCtx, newSourceStep)
					delNodeInfo := makeDeleteNodeInfo(builder.compCtx, entriesObjRef, entriesTableDef, entriesDeleteIdx, -1, false, entriesTblPkPos, entriesTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
					lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, true, false)
					putDeleteNodeInfo(delNodeInfo)
					if err != nil {
						return err
					}
					builder.appendStep(lastNodeId)
				}
				// insert ivf_sk plan
				{
					//TODO: verify with ouyuanning, if this is correct
					lastNodeId = appendSinkScanNode(builder, bindCtx, newSourceStep)
					lastNodeIdForTblJoinCentroids := appendSinkScanNode(builder, bindCtx, newSourceStep)

					lastProject := builder.qry.Nodes[lastNodeId].ProjectList
					lastProjectForTblJoinCentroids := builder.qry.Nodes[lastNodeIdForTblJoinCentroids].ProjectList

					projectProjection := make([]*Expr, len(delCtx.tableDef.Cols))
					projectProjectionForTblJoinCentroids := make([]*Expr, len(delCtx.tableDef.Cols))
					for j, uCols := range delCtx.tableDef.Cols {
						if nIdx, ok := delCtx.updateColPosMap[uCols.Name]; ok {
							projectProjection[j] = lastProject[nIdx]
							projectProjectionForTblJoinCentroids[j] = lastProjectForTblJoinCentroids[nIdx]
						} else {
							if uCols.Name == catalog.Row_ID {
								// replace the origin table's row_id with entry table's row_id
								// it is the 2nd last column in the entry table join
								projectProjection[j] = lastProject[len(lastProject)-2]
								projectProjectionForTblJoinCentroids[j] = lastProjectForTblJoinCentroids[len(lastProjectForTblJoinCentroids)-2]
							} else {
								projectProjection[j] = lastProject[j]
								projectProjectionForTblJoinCentroids[j] = lastProjectForTblJoinCentroids[j]
							}
						}
					}
					projectNode := &Node{
						NodeType:    plan.Node_PROJECT,
						Children:    []int32{lastNodeId},
						ProjectList: projectProjection,
					}
					lastNodeId = builder.appendNode(projectNode, bindCtx)

					preUKStep, err := appendPreInsertSkVectorPlan(builder, bindCtx, delCtx.tableDef, lastNodeId, multiTableIndex, true, idxRefs, idxTableDefs)
					if err != nil {
						return err
					}

					insertEntriesTableDef := DeepCopyTableDef(entriesTableDef, false)
					for _, col := range entriesTableDef.Cols {
						if col.Name != catalog.Row_ID {
							insertEntriesTableDef.Cols = append(insertEntriesTableDef.Cols, DeepCopyColDef(col))
						}
					}
					updateColLength := 1
					addAffectedRows := false
					isFkRecursionCall := false
					updatePkCol := true
					ifExistAutoPkCol := false
					ifCheckPkDup := false
					ifInsertFromUnique := false
					var pkFilterExprs []*Expr
					var partitionExpr *Expr
					var indexSourceColTypes []*Type
					var fuzzymessage *OriginTableMessageForFuzzy
					err = makeOneInsertPlan(ctx, builder, bindCtx, entriesObjRef, insertEntriesTableDef,
						updateColLength, preUKStep, addAffectedRows, isFkRecursionCall, updatePkCol,
						pkFilterExprs, partitionExpr, ifExistAutoPkCol, ifCheckPkDup, ifInsertFromUnique,
						indexSourceColTypes, fuzzymessage)

					if err != nil {
						return err
					}
				}

			} else {
				// it's more simple for delete hidden unique table .so we append nodes after the plan. not recursive call buildDeletePlans
				delNodeInfo := makeDeleteNodeInfo(builder.compCtx, entriesObjRef, entriesTableDef, entriesDeleteIdx, -1, false, entriesTblPkPos, entriesTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
				lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, true, false)
				putDeleteNodeInfo(delNodeInfo)
				if err != nil {
					return err
				}
				builder.appendStep(lastNodeId)
			}
		default:
			return moerr.NewNYINoCtxf("unsupported index algorithm %s", multiTableIndex.IndexAlgo)
		}
	}
	return nil
}

func buildPreInsertRegularIndex(stmt *tree.Insert, ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef,
	sourceStep int32, ifInsertFromUniqueColMap map[string]bool, indexdef *plan.IndexDef, idx int) error {

	idxRef, idxTableDef := ctx.Resolve(objRef.SchemaName, indexdef.IndexTableName, nil)
	// remove row_id
	idxTableDef.Cols = RemoveIf[*ColDef](idxTableDef.Cols, func(col *ColDef) bool {
		return col.Name == catalog.Row_ID
	})

	lastNodeId := appendSinkScanNode(builder, bindCtx, sourceStep)
	newSourceStep, err := appendPreInsertPlan(builder, bindCtx, tableDef, lastNodeId, idx, false, idxTableDef, indexdef.Unique)
	if err != nil {
		return err
	}

	needCheckPkDupForHiddenTable := indexdef.Unique // only check PK uniqueness for UK. SK will not check PK uniqueness.
	var insertColsNameFromStmt []string
	var pkFilterExprForHiddenTable []*Expr
	var originTableMessageForFuzzy *OriginTableMessageForFuzzy
	var ifInsertFromUnique bool

	// The way to guarantee the uniqueness of the unique key is to create a hidden table,
	// with the primary key of the hidden table as the unique key.
	// package contains some information needed by the fuzzy filter to run background SQL.
	if indexdef.GetUnique() {
		_, idxTableDef := ctx.Resolve(objRef.SchemaName, indexdef.IndexTableName, nil)
		// remove row_id
		idxTableDef.Cols = RemoveIf[*ColDef](idxTableDef.Cols, func(colVal *ColDef) bool {
			return colVal.Name == catalog.Row_ID
		})
		originTableMessageForFuzzy = &OriginTableMessageForFuzzy{
			ParentTableName: tableDef.Name,
		}

		uniqueCols := make([]*plan.ColDef, len(indexdef.Parts))
		uniqueColsMap := make(map[string]int)

		for i, n := range indexdef.Parts {
			uniqueColsMap[n] = i

			if _, exists := ifInsertFromUniqueColMap[n]; exists {
				ifInsertFromUnique = true
				break
			}
		}

		for _, c := range tableDef.Cols { // sort
			if i, ok := uniqueColsMap[c.Name]; ok {
				uniqueCols[i] = c
			}
		}
		originTableMessageForFuzzy.ParentUniqueCols = uniqueCols
		uniqueColLocationMap := newLocationMap(tableDef, indexdef)
		if stmt != nil {
			insertColsNameFromStmt, err = getInsertColsFromStmt(ctx.GetContext(), stmt, tableDef)
			if err != nil {
				return err
			}

			// check if this unique key need to check dup
			for name, oi := range uniqueColLocationMap.m {
				if tableDef.Cols[oi.index].Typ.AutoIncr {
					found := false
					for _, inserted := range insertColsNameFromStmt {
						if inserted == name {
							found = true
						}
					}
					if !found { // still need to check dup for auto incr unique if contains value, else no need
						needCheckPkDupForHiddenTable = false
					}
				}
			}

			// try to build pk filter epxr for hidden table created by unique key
			if needCheckPkDupForHiddenTable && canUsePkFilter(builder, ctx, stmt, tableDef, insertColsNameFromStmt, indexdef) {
				pkFilterExprForHiddenTable, err = getPkValueExpr(builder, ctx, tableDef, uniqueColLocationMap, insertColsNameFromStmt)
				if err != nil {
					return err
				}
			}
		}
	}

	colTypes := make([]*plan.Type, len(tableDef.Cols))
	for i := range tableDef.Cols {
		colTypes[i] = &tableDef.Cols[i].Typ
	}

	updateColLength := 0
	addAffectedRows := false
	isFkRecursionCall := false
	updatePkCol := true
	ifExistAutoPkCol := false
	needCheckPkDupForHiddenTable = true
	var partitionExpr *Expr
	err = makeOneInsertPlan(ctx, builder, bindCtx, idxRef, idxTableDef,
		updateColLength, newSourceStep, addAffectedRows, isFkRecursionCall, updatePkCol,
		pkFilterExprForHiddenTable, partitionExpr, ifExistAutoPkCol, needCheckPkDupForHiddenTable, ifInsertFromUnique,
		colTypes, originTableMessageForFuzzy)

	return err
}

func buildPreInsertMasterIndex(stmt *tree.Insert, ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, objRef *ObjectRef, tableDef *TableDef,
	sourceStep int32, ifInsertFromUniqueColMap map[string]bool, indexdef *plan.IndexDef, idx int) error {
	idxRef, idxTableDef := ctx.Resolve(objRef.SchemaName, indexdef.IndexTableName, nil)
	// remove row_id
	idxTableDef.Cols = RemoveIf[*ColDef](idxTableDef.Cols, func(colVal *ColDef) bool {
		return colVal.Name == catalog.Row_ID
	})
	genLastNodeIdFn := func() int32 {
		return appendSinkScanNode(builder, bindCtx, sourceStep)
	}
	newSourceStep, err := appendPreInsertSkMasterPlan(builder, bindCtx, tableDef, idx, false, idxTableDef, genLastNodeIdFn)
	if err != nil {
		return err
	}

	//TODO: verify with zengyan1 if colType should read from original table.
	// It is mainly used for retaining decimal datatype precision in error messages.
	colTypes := make([]*plan.Type, len(tableDef.Cols))
	for i := range tableDef.Cols {
		colTypes[i] = &tableDef.Cols[i].Typ
	}

	updateColLength := 0
	addAffectedRows := false
	isFkRecursionCall := false
	updatePkCol := true
	ifExistAutoPkCol := false
	ifCheckPkDup := false
	ifInsertFromUnique := false
	var pkFilterExprs []*Expr
	var partitionExpr *Expr
	var fuzzymessage *OriginTableMessageForFuzzy
	err = makeOneInsertPlan(ctx, builder, bindCtx, idxRef, idxTableDef,
		updateColLength, newSourceStep, addAffectedRows, isFkRecursionCall, updatePkCol,
		pkFilterExprs, partitionExpr, ifExistAutoPkCol, ifCheckPkDup, ifInsertFromUnique,
		colTypes, fuzzymessage)

	return err
}

func buildDeleteRegularIndex(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, delCtx *dmlPlanCtx,
	indexdef *plan.IndexDef, idx int, typMap map[string]plan.Type, posMap map[string]int) error {

	/********
	NOTE: make sure to make the major change applied to secondary index, to IVFFLAT index as well.
	Else IVFFLAT index would fail
	********/
	isUpdate := delCtx.updateColLength > 0

	var isUk = indexdef.Unique
	var isSK = !isUk && catalog.IsRegularIndexAlgo(indexdef.IndexAlgo)

	uniqueObjRef, uniqueTableDef := builder.compCtx.Resolve(delCtx.objRef.SchemaName, indexdef.IndexTableName, nil)
	if uniqueTableDef == nil {
		return moerr.NewNoSuchTable(builder.GetContext(), delCtx.objRef.SchemaName, indexdef.IndexTableName)
	}
	var lastNodeId int32
	var err error
	var uniqueDeleteIdx int
	var uniqueTblPkPos int
	var uniqueTblPkTyp Type

	if delCtx.isDeleteWithoutFilters {
		lastNodeId, err = appendDeleteIndexTablePlanWithoutFilters(builder, bindCtx, uniqueObjRef, uniqueTableDef)
		uniqueDeleteIdx = getRowIdPos(uniqueTableDef)
		uniqueTblPkPos, uniqueTblPkTyp = getPkPos(uniqueTableDef, false)
	} else {
		lastNodeId = appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
		lastNodeId, err = appendDeleteIndexTablePlan(builder, bindCtx, uniqueObjRef, uniqueTableDef, indexdef, typMap, posMap, lastNodeId, isUk)
		uniqueDeleteIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
		uniqueTblPkPos = uniqueDeleteIdx + 1
		uniqueTblPkTyp = uniqueTableDef.Cols[0].Typ
	}
	if err != nil {
		return err
	}
	if isUpdate {
		// do it like simple update
		lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
		newSourceStep := builder.appendStep(lastNodeId)
		// delete uk plan
		{
			//sink_scan -> lock -> delete
			lastNodeId = appendSinkScanNode(builder, bindCtx, newSourceStep)
			delNodeInfo := makeDeleteNodeInfo(builder.compCtx, uniqueObjRef, uniqueTableDef, uniqueDeleteIdx, -1, false, uniqueTblPkPos, uniqueTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
			lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, isUk, isSK, false)
			putDeleteNodeInfo(delNodeInfo)
			if err != nil {
				return err
			}
			builder.appendStep(lastNodeId)
		}
		// insert uk plan
		{
			lastNodeId = appendSinkScanNode(builder, bindCtx, newSourceStep)
			lastProject := builder.qry.Nodes[lastNodeId].ProjectList
			projectProjection := make([]*Expr, len(delCtx.tableDef.Cols))
			for j, uCols := range delCtx.tableDef.Cols {
				if nIdx, ok := delCtx.updateColPosMap[uCols.Name]; ok {
					projectProjection[j] = lastProject[nIdx]
				} else {
					if uCols.Name == catalog.Row_ID {
						// replace the origin table's row_id with unique table's row_id
						projectProjection[j] = lastProject[len(lastProject)-2]
					} else {
						projectProjection[j] = lastProject[j]
					}
				}
			}
			projectNode := &Node{
				NodeType:    plan.Node_PROJECT,
				Children:    []int32{lastNodeId},
				ProjectList: projectProjection,
			}
			lastNodeId = builder.appendNode(projectNode, bindCtx)
			preUKStep, err := appendPreInsertPlan(builder, bindCtx, delCtx.tableDef, lastNodeId, idx, true, uniqueTableDef, isUk)
			if err != nil {
				return err
			}

			insertUniqueTableDef := DeepCopyTableDef(uniqueTableDef, false)
			for _, col := range uniqueTableDef.Cols {
				if col.Name != catalog.Row_ID {
					insertUniqueTableDef.Cols = append(insertUniqueTableDef.Cols, DeepCopyColDef(col))
				}
			}
			_checkPKDupForHiddenIndexTable := indexdef.Unique // only check PK uniqueness for UK. SK will not check PK uniqueness.
			updateColLength := 1
			addAffectedRows := false
			isFkRecursionCall := false
			updatePkCol := true
			ifExistAutoPkCol := false
			ifInsertFromUnique := false
			var pkFilterExprs []*Expr
			var partitionExpr *Expr
			var indexSourceColTypes []*Type
			var fuzzymessage *OriginTableMessageForFuzzy
			err = makeOneInsertPlan(ctx, builder, bindCtx, uniqueObjRef, insertUniqueTableDef,
				updateColLength, preUKStep, addAffectedRows, isFkRecursionCall, updatePkCol,
				pkFilterExprs, partitionExpr, ifExistAutoPkCol, _checkPKDupForHiddenIndexTable, ifInsertFromUnique,
				indexSourceColTypes, fuzzymessage)
			if err != nil {
				return err
			}
		}
	} else {
		// it's more simple for delete hidden unique table .so we append nodes after the plan. not recursive call buildDeletePlans
		delNodeInfo := makeDeleteNodeInfo(builder.compCtx, uniqueObjRef, uniqueTableDef, uniqueDeleteIdx, -1, false, uniqueTblPkPos, uniqueTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
		lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, isUk, isSK, false)
		putDeleteNodeInfo(delNodeInfo)
		if err != nil {
			return err
		}
		builder.appendStep(lastNodeId)
	}

	return nil
}

func buildDeleteMasterIndex(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, delCtx *dmlPlanCtx,
	indexdef *plan.IndexDef, idx int, typMap map[string]plan.Type, posMap map[string]int) error {
	isUpdate := delCtx.updateColLength > 0
	// Used by pre-insert vector index.
	masterObjRef, masterTableDef := ctx.Resolve(delCtx.objRef.SchemaName, indexdef.IndexTableName, nil)
	if masterTableDef == nil {
		return moerr.NewNoSuchTable(builder.GetContext(), delCtx.objRef.SchemaName, indexdef.IndexName)
	}

	var lastNodeId int32
	var err error
	var masterDeleteIdx int
	var masterTblPkPos int
	var masterTblPkTyp Type

	if delCtx.isDeleteWithoutFilters {
		lastNodeId, err = appendDeleteIndexTablePlanWithoutFilters(builder, bindCtx, masterObjRef, masterTableDef)
		masterDeleteIdx = getRowIdPos(masterTableDef)
		masterTblPkPos, masterTblPkTyp = getPkPos(masterTableDef, false)
	} else {
		lastNodeId = appendSinkScanNode(builder, bindCtx, delCtx.sourceStep)
		lastNodeId, err = appendDeleteMasterTablePlan(builder, bindCtx, masterObjRef, masterTableDef, lastNodeId, delCtx.tableDef, indexdef, typMap, posMap)
		masterDeleteIdx = len(delCtx.tableDef.Cols) + delCtx.updateColLength
		masterTblPkPos = masterDeleteIdx + 1
		masterTblPkTyp = masterTableDef.Cols[0].Typ
	}

	if err != nil {
		return err
	}

	if isUpdate {
		// do it like simple update
		lastNodeId = appendSinkNode(builder, bindCtx, lastNodeId)
		newSourceStep := builder.appendStep(lastNodeId)
		// delete uk plan
		{
			//sink_scan -> lock -> delete
			lastNodeId = appendSinkScanNode(builder, bindCtx, newSourceStep)
			delNodeInfo := makeDeleteNodeInfo(builder.compCtx, masterObjRef, masterTableDef, masterDeleteIdx, -1, false, masterTblPkPos, masterTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
			lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, true, false)
			putDeleteNodeInfo(delNodeInfo)
			if err != nil {
				return err
			}
			builder.appendStep(lastNodeId)
		}
		// insert master sk plan
		{
			// This function creates new SinkScanNode for each of Union's inside appendPreInsertSkMasterPlan
			genLastNodeIdFn := func() int32 {
				//TODO: verify if this will cause memory leak.
				newLastNodeId := appendSinkScanNode(builder, bindCtx, newSourceStep)
				lastProject := builder.qry.Nodes[newLastNodeId].ProjectList
				projectProjection := make([]*Expr, len(delCtx.tableDef.Cols))
				for j, uCols := range delCtx.tableDef.Cols {
					if nIdx, ok := delCtx.updateColPosMap[uCols.Name]; ok {
						projectProjection[j] = lastProject[nIdx]
					} else {
						if uCols.Name == catalog.Row_ID {
							// NOTE:
							// 1. In the case of secondary index, we are reusing the row_id values
							// that were deleted.
							// 2. But in the master index case, we are using the row_id values from
							// the original table. Row_id associated with say 2 rows (one row for each column)
							// in the index table will be same value (ie that from the original table).
							// So, when we do UNION it automatically removes the duplicate values.
							// ie
							//  <"a_arjun_1",1, 1> -->  (select serial_full("0", a, c),__mo_pk_key, __mo_row_id)
							//  <"a_arjun_1",1, 1>
							//  <"b_sunil_1",1, 1> -->  (select serial_full("2", b,c),__mo_pk_key, __mo_row_id)
							//  <"b_sunil_1",1, 1>
							//  when we use UNION, we remove the duplicate values
							// 3. RowID is added here: https://github.com/arjunsk/matrixone/blob/d7db178e1c7298e2a3e4f99e7292425a7ef0ef06/pkg/vm/engine/disttae/txn.go#L95
							// TODO: verify this with Feng, Ouyuanning and Qingx (not reusing the row_id)
							projectProjection[j] = lastProject[j]
						} else {
							projectProjection[j] = lastProject[j]
						}
					}
				}
				projectNode := &Node{
					NodeType:    plan.Node_PROJECT,
					Children:    []int32{newLastNodeId},
					ProjectList: projectProjection,
				}
				return builder.appendNode(projectNode, bindCtx)
			}

			preUKStep, err := appendPreInsertSkMasterPlan(builder, bindCtx, delCtx.tableDef, idx, true, masterTableDef, genLastNodeIdFn)
			if err != nil {
				return err
			}

			insertEntriesTableDef := DeepCopyTableDef(masterTableDef, false)
			for _, col := range masterTableDef.Cols {
				if col.Name != catalog.Row_ID {
					insertEntriesTableDef.Cols = append(insertEntriesTableDef.Cols, DeepCopyColDef(col))
				}
			}
			updateColLength := 1
			addAffectedRows := false
			isFkRecursionCall := false
			updatePkCol := true
			ifExistAutoPkCol := false
			ifCheckPkDup := false
			ifInsertFromUnique := false
			var pkFilterExprs []*Expr
			var partitionExpr *Expr
			var indexSourceColTypes []*Type
			var fuzzymessage *OriginTableMessageForFuzzy
			err = makeOneInsertPlan(ctx, builder, bindCtx, masterObjRef, insertEntriesTableDef,
				updateColLength, preUKStep, addAffectedRows, isFkRecursionCall, updatePkCol,
				pkFilterExprs, partitionExpr, ifExistAutoPkCol, ifCheckPkDup, ifInsertFromUnique,
				indexSourceColTypes, fuzzymessage)

			if err != nil {
				return err
			}
		}

	} else {
		// it's more simple for delete hidden unique table .so we append nodes after the plan. not recursive call buildDeletePlans
		delNodeInfo := makeDeleteNodeInfo(builder.compCtx, masterObjRef, masterTableDef, masterDeleteIdx, -1, false, masterTblPkPos, masterTblPkTyp, delCtx.lockTable, delCtx.partitionInfos)
		lastNodeId, err = makeOneDeletePlan(builder, bindCtx, lastNodeId, delNodeInfo, false, true, false)
		putDeleteNodeInfo(delNodeInfo)
		if err != nil {
			return err
		}
		builder.appendStep(lastNodeId)
	}

	return nil
}
