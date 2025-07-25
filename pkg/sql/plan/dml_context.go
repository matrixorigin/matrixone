// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

type DMLContext struct {
	objRefs        []*plan.ObjectRef
	tableDefs      []*plan.TableDef
	aliases        []string
	isClusterTable []bool

	updateCol2Expr []map[string]tree.Expr // This slice index correspond to tableDefs
	updatePartCol  []bool                 //If update cols contains col that Partition expr used
	//oldColPosMap   []map[string]int       // origin table values to their position in derived table
	//newColPosMap   []map[string]int       // insert/update values to their position in derived table
	//nameToIdx      map[string]int         // Mapping of table full path name to tableDefs index，such as： 'tpch.nation -> 0'
	//idToName       map[uint64]string      // Mapping of tableId to full path name of table
	aliasMap map[string]int // Mapping of table aliases to tableDefs array index,If there is no alias, replace it with the original name of the table
}

func NewDMLContext() *DMLContext {
	return &DMLContext{
		//nameToIdx: make(map[string]int),
		//idToName:  make(map[uint64]string),
		aliasMap: make(map[string]int),
	}
}

func (dmlCtx *DMLContext) ResolveUpdateTables(ctx CompilerContext, stmt *tree.Update) error {
	err := dmlCtx.ResolveTables(ctx, stmt.Tables, stmt.With, nil, false)
	if err != nil {
		return err
	}

	// check update field and set updateKeys
	usedTbl := make(map[string]map[string]tree.Expr)
	allColumns := make(map[string]map[string]bool)
	for alias, idx := range dmlCtx.aliasMap {
		allColumns[alias] = make(map[string]bool)
		for _, col := range dmlCtx.tableDefs[idx].Cols {
			allColumns[alias][col.Name] = true
		}
	}

	appendToTbl := func(table, column string, expr tree.Expr) {
		if _, exists := usedTbl[table]; !exists {
			usedTbl[table] = make(map[string]tree.Expr)
		}
		usedTbl[table][column] = expr
	}

	for _, updateExpr := range stmt.Exprs {
		if len(updateExpr.Names) > 1 {
			return moerr.NewNYI(ctx.GetContext(), "unsupport expr")
		}

		parts := updateExpr.Names[0]
		expr := updateExpr.Expr
		if parts.NumParts > 1 {
			colName := parts.ColName()
			tblName := parts.TblName()
			if _, tblExists := dmlCtx.aliasMap[tblName]; tblExists {
				if allColumns[tblName][colName] {
					appendToTbl(tblName, colName, expr)
				} else {
					return moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' not found in table %s", parts.ColNameOrigin(), parts.TblNameOrigin())
				}
			} else {
				return moerr.NewNoSuchTable(ctx.GetContext(), "", parts.TblNameOrigin())
			}
		} else {
			colName := parts.ColName()
			found := false
			for alias, columns := range allColumns {
				if columns[colName] {
					if found {
						return moerr.NewInternalErrorf(ctx.GetContext(), "Column '%v' in field list is ambiguous", parts.ColNameOrigin())
					}
					found = true
					appendToTbl(alias, colName, expr)
				}
			}
			if !found && stmt.With != nil {
				var str string
				for i, c := range stmt.With.CTEs {
					if i > 0 {
						str += ", "
					}
					str += string(c.Name.Alias)
				}
				return moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' not found in table or the target table %s of the UPDATE is not updatable", parts.ColNameOrigin(), str)
			} else if !found {
				return moerr.NewInternalErrorf(ctx.GetContext(), "column '%v' not found in table", parts.ColNameOrigin())
			}
		}
	}

	if len(usedTbl) > 1 {
		return moerr.NewUnsupportedDML(ctx.GetContext(), "multi-table update")
	}

	dmlCtx.updateCol2Expr = make([]map[string]tree.Expr, len(dmlCtx.tableDefs))
	for alias, columnMap := range usedTbl {
		idx := dmlCtx.aliasMap[alias]
		dmlCtx.updateCol2Expr[idx] = columnMap
	}

	dmlCtx.updatePartCol = make([]bool, len(dmlCtx.tableDefs))

	return nil
}

func (dmlCtx *DMLContext) ResolveTables(ctx CompilerContext, tableExprs tree.TableExprs, with *tree.With, aliasMap map[string][2]string, respectFKCheck bool) error {
	cteMap := make(map[string]bool)
	if with != nil {
		for _, cte := range with.CTEs {
			cteMap[string(cte.Name.Alias)] = true
		}
	}

	for _, tbl := range tableExprs {
		err := dmlCtx.ResolveSingleTable(ctx, tbl, aliasMap, cteMap, respectFKCheck)
		if err != nil {
			return err
		}
	}

	return nil
}

func (dmlCtx *DMLContext) ResolveSingleTable(ctx CompilerContext, tbl tree.TableExpr, aliasMap map[string][2]string, withMap map[string]bool, respectFKCheck bool) error {
	var tblName, dbName, alias string

	if aliasTbl, ok := tbl.(*tree.AliasedTableExpr); ok {
		alias = string(aliasTbl.As.Alias)
		tbl = aliasTbl.Expr
	}

	for {
		if baseTbl, ok := tbl.(*tree.ParenTableExpr); ok {
			tbl = baseTbl.Expr
		} else {
			break
		}
	}

	//if joinTbl, ok := tbl.(*tree.JoinTableExpr); ok {
	//	dmlCtx.needAggFilter = true
	//	err := setTableExprToDmlTableInfo(ctx, joinTbl.Left, dmlCtx, aliasMap, withMap)
	//	if err != nil {
	//		return err
	//	}
	//	if joinTbl.Right != nil {
	//		return setTableExprToDmlTableInfo(ctx, joinTbl.Right, dmlCtx, aliasMap, withMap)
	//	}
	//	return nil
	//}

	if baseTbl, ok := tbl.(*tree.TableName); ok {
		dbName = string(baseTbl.SchemaName)
		tblName = string(baseTbl.ObjectName)
	} else {
		return moerr.NewUnsupportedDML(ctx.GetContext(), "unsupported table type")
	}

	if withMap[tblName] {
		return nil
	}

	if aliasNames, exist := aliasMap[tblName]; exist {
		alias = tblName // work in delete statement
		dbName = aliasNames[0]
		tblName = aliasNames[1]
	}

	if len(tblName) == 0 {
		return moerr.NewUnsupportedDML(ctx.GetContext(), "empty table name")
	}

	if len(dbName) == 0 {
		dbName = ctx.DefaultDatabase()
	}

	objRef, tableDef := ctx.Resolve(dbName, tblName, nil)
	if tableDef == nil {
		return moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}

	if tableDef.TableType == catalog.SystemSourceRel {
		return moerr.NewInvalidInput(ctx.GetContext(), "cannot insert/update/delete from source")
	} else if tableDef.TableType == catalog.SystemExternalRel {
		return moerr.NewInvalidInput(ctx.GetContext(), "cannot insert/update/delete from external table")
	} else if tableDef.TableType == catalog.SystemViewRel {
		return moerr.NewInvalidInput(ctx.GetContext(), "cannot insert/update/delete from view")
	} else if tableDef.TableType == catalog.SystemSequenceRel && ctx.GetContext().Value(defines.BgKey{}) == nil {
		return moerr.NewInvalidInput(ctx.GetContext(), "Cannot insert/update/delete from sequence")
	}

	var err error
	checkFK := true
	if respectFKCheck {
		checkFK, err = IsForeignKeyChecksEnabled(ctx)
		if err != nil {
			return err
		}
	}

	if checkFK && (len(tableDef.Fkeys) > 0 || len(tableDef.RefChildTbls) > 0) {
		return moerr.NewUnsupportedDML(ctx.GetContext(), "foreign key constraint")
	}

	isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return err
	}
	if isClusterTable && accountId != catalog.System_Account {
		return moerr.NewInternalError(ctx.GetContext(), "only the sys account can insert/update/delete the cluster table")
	}

	if util.TableIsClusterTable(tableDef.GetTableType()) && accountId != catalog.System_Account {
		return moerr.NewInternalErrorf(ctx.GetContext(), "only the sys account can insert/update/delete the cluster table %s", tableDef.GetName())
	}
	if objRef.PubInfo != nil {
		return moerr.NewInternalError(ctx.GetContext(), "cannot insert/update/delete from public table")
	}

	if len(tableDef.Name2ColIndex) == 0 {
		tableDef.Name2ColIndex = make(map[string]int32)
		for colIdx, col := range tableDef.Cols {
			tableDef.Name2ColIndex[col.Name] = int32(colIdx)
		}
	}

	nowIdx := len(dmlCtx.tableDefs)
	dmlCtx.isClusterTable = append(dmlCtx.isClusterTable, isClusterTable)
	dmlCtx.objRefs = append(dmlCtx.objRefs, objRef)
	dmlCtx.tableDefs = append(dmlCtx.tableDefs, tableDef)
	//key := dbName + "." + tblName
	//dmlCtx.nameToIdx[key] = nowIdx
	//dmlCtx.idToName[tableDef.TblId] = key
	if alias == "" {
		alias = tblName
	}
	dmlCtx.aliases = append(dmlCtx.aliases, alias)
	dmlCtx.aliasMap[alias] = nowIdx

	return nil
}
