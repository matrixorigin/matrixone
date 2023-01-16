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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/operator"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func genViewTableDef(ctx CompilerContext, stmt *tree.Select) (*plan.TableDef, error) {
	var tableDef plan.TableDef

	// check view statement
	stmtPlan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, stmt)
	if err != nil {
		return nil, err
	}

	query := stmtPlan.GetQuery()
	cols := make([]*plan.ColDef, len(query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList))
	for idx, expr := range query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList {
		cols[idx] = &plan.ColDef{
			Name: query.Headings[idx],
			Alg:  plan.CompressType_Lz4,
			Typ:  expr.Typ,
			Default: &plan.Default{
				NullAbility:  true,
				Expr:         nil,
				OriginString: "",
			},
		}
	}
	tableDef.Cols = cols

	viewData, err := json.Marshal(ViewData{
		Stmt:            ctx.GetRootSql(),
		DefaultDatabase: ctx.DefaultDatabase(),
	})
	if err != nil {
		return nil, err
	}
	tableDef.ViewSql = &plan.ViewDef{
		View: string(viewData),
	}
	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemViewRel,
		},
	}
	tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})

	return &tableDef, nil
}

func buildCreateView(stmt *tree.CreateView, ctx CompilerContext) (*Plan, error) {
	viewName := stmt.Name.ObjectName
	createTable := &plan.CreateTable{
		IfNotExists: stmt.IfNotExists,
		Temporary:   stmt.Temporary,
		TableDef: &TableDef{
			Name: string(viewName),
		},
	}

	// get database name
	if len(stmt.Name.SchemaName) == 0 {
		createTable.Database = ""
	} else {
		createTable.Database = string(stmt.Name.SchemaName)
	}

	tableDef, err := genViewTableDef(ctx, stmt.AsSource)
	if err != nil {
		return nil, err
	}

	createTable.TableDef.Cols = tableDef.Cols
	createTable.TableDef.ViewSql = tableDef.ViewSql
	createTable.TableDef.Defs = tableDef.Defs

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_TABLE,
				Definition: &plan.DataDefinition_CreateTable{
					CreateTable: createTable,
				},
			},
		},
	}, nil
}

func buildCreateTable(stmt *tree.CreateTable, ctx CompilerContext) (*Plan, error) {
	createTable := &plan.CreateTable{
		IfNotExists: stmt.IfNotExists,
		Temporary:   stmt.Temporary,
		TableDef: &TableDef{
			Name: string(stmt.Table.ObjectName),
		},
	}

	// get database name
	if len(stmt.Table.SchemaName) == 0 {
		createTable.Database = ctx.DefaultDatabase()
	} else {
		createTable.Database = string(stmt.Table.SchemaName)
	}

	// set tableDef
	err := buildTableDefs(stmt, ctx, createTable)
	if err != nil {
		return nil, err
	}

	// set option
	for _, option := range stmt.Options {
		switch opt := option.(type) {
		case *tree.TableOptionProperties:
			properties := make([]*plan.Property, len(opt.Preperties))
			for idx, property := range opt.Preperties {
				properties[idx] = &plan.Property{
					Key:   property.Key,
					Value: property.Value,
				}
			}
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		// todo confirm: option data store like this?
		case *tree.TableOptionComment:
			if getNumOfCharacters(opt.Comment) > maxLengthOfTableComment {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "comment for field '%s' is too long", createTable.TableDef.Name)
			}

			properties := []*plan.Property{
				{
					Key:   catalog.SystemRelAttr_Comment,
					Value: opt.Comment,
				},
			}
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		// these table options is not support in plan
		// case *tree.TableOptionEngine, *tree.TableOptionSecondaryEngine, *tree.TableOptionCharset,
		// 	*tree.TableOptionCollate, *tree.TableOptionAutoIncrement, *tree.TableOptionComment,
		// 	*tree.TableOptionAvgRowLength, *tree.TableOptionChecksum, *tree.TableOptionCompression,
		// 	*tree.TableOptionConnection, *tree.TableOptionPassword, *tree.TableOptionKeyBlockSize,
		// 	*tree.TableOptionMaxRows, *tree.TableOptionMinRows, *tree.TableOptionDelayKeyWrite,
		// 	*tree.TableOptionRowFormat, *tree.TableOptionStatsPersistent, *tree.TableOptionStatsAutoRecalc,
		// 	*tree.TableOptionPackKeys, *tree.TableOptionTablespace, *tree.TableOptionDataDirectory,
		// 	*tree.TableOptionIndexDirectory, *tree.TableOptionStorageMedia, *tree.TableOptionStatsSamplePages,
		// 	*tree.TableOptionUnion, *tree.TableOptionEncryption:
		// 	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
		default:
			return nil, moerr.NewNotSupported(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
		}
	}

	// After handleTableOptions, so begin the partitions processing depend on TableDef
	if stmt.Param != nil {
		for i := 0; i < len(stmt.Param.Option); i += 2 {
			switch strings.ToLower(stmt.Param.Option[i]) {
			case "endpoint", "region", "access_key_id", "secret_access_key", "bucket", "filepath", "compression", "format", "jsondata":
			default:
				return nil, moerr.NewBadConfig(ctx.GetContext(), "the keyword '%s' is not support", strings.ToLower(stmt.Param.Option[i]))
			}
		}
		json_byte, err := json.Marshal(stmt.Param)
		if err != nil {
			return nil, err
		}
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemExternalRel,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: string(json_byte),
			},
		}
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	} else {
		kind := catalog.SystemOrdinaryRel
		if stmt.IsClusterTable {
			kind = catalog.SystemClusterRel
		}
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: kind,
			},
			{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: ctx.GetRootSql(),
			},
		}
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Properties{
				Properties: &plan.PropertiesDef{
					Properties: properties,
				},
			}})
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
	bindContext := NewBindContext(builder, nil)

	// set partition(unsupport now)
	if stmt.PartitionOption != nil {
		nodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			Stats:       nil,
			ObjRef:      nil,
			TableDef:    createTable.TableDef,
			BindingTags: []int32{builder.genNewTag()},
		}, bindContext)

		err = builder.addBinding(nodeID, tree.AliasClause{}, bindContext)
		if err != nil {
			return nil, err
		}
		partitionBinder := NewPartitionBinder(builder, bindContext)
		err = buildPartitionByClause(partitionBinder, stmt, createTable.TableDef)
		if err != nil {
			return nil, err
		}
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_TABLE,
				Definition: &plan.DataDefinition_CreateTable{
					CreateTable: createTable,
				},
			},
		},
	}, nil
}

// buildPartitionByClause build partition by clause info and semantic check.
// Currently, sub partition and partition value verification are not supported
func buildPartitionByClause(partitionBinder *PartitionBinder, stmt *tree.CreateTable, tableDef *TableDef) (err error) {
	switch stmt.PartitionOption.PartBy.PType.(type) {
	case *tree.HashType:
		err = buildHashPartition(partitionBinder, stmt, tableDef)
	case *tree.KeyType:
		err = buildKeyPartition(partitionBinder, stmt, tableDef)
	case *tree.RangeType:
		err = buildRangePartition(partitionBinder, stmt, tableDef)
	case *tree.ListType:
		err = buildListPartitiion(partitionBinder, stmt, tableDef)
	}
	return err
}

func buildTableDefs(stmt *tree.CreateTable, ctx CompilerContext, createTable *plan.CreateTable) error {
	var primaryKeys []string
	var indexs []string
	colMap := make(map[string]*ColDef)
	uniqueIndexInfos := make([]*tree.UniqueIndex, 0)
	secondaryIndexInfos := make([]*tree.Index, 0)
	for _, item := range stmt.Defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colType, err := getTypeFromAst(ctx.GetContext(), def.Type)
			if err != nil {
				return err
			}
			if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) {
				if colType.GetWidth() > types.MaxStringSize {
					return moerr.NewInvalidInput(ctx.GetContext(), "string width (%d) is too long", colType.GetWidth())
				}
			}
			var pks []string
			var comment string
			var auto_incr bool
			for _, attr := range def.Attributes {
				switch attribute := attr.(type) {
				case *tree.AttributePrimaryKey:
					if colType.GetId() == int32(types.T_blob) {
						return moerr.NewNotSupported(ctx.GetContext(), "blob type in primary key")
					}
					if colType.GetId() == int32(types.T_text) {
						return moerr.NewNotSupported(ctx.GetContext(), "text type in primary key")
					}
					if colType.GetId() == int32(types.T_json) {
						return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("JSON column '%s' cannot be in primary key", def.Name.Parts[0]))
					}
					pks = append(pks, def.Name.Parts[0])
				case *tree.AttributeComment:
					comment = attribute.CMT.String()
					if getNumOfCharacters(comment) > maxLengthOfColumnComment {
						return moerr.NewInvalidInput(ctx.GetContext(), "comment for column '%s' is too long", def.Name.Parts[0])
					}
				case *tree.AttributeAutoIncrement:
					auto_incr = true
					if !operator.IsInteger(types.T(colType.GetId())) {
						return moerr.NewNotSupported(ctx.GetContext(), "the auto_incr column is only support integer type now")
					}
				case *tree.AttributeUnique, *tree.AttributeUniqueKey:
					uniqueIndexInfos = append(uniqueIndexInfos, &tree.UniqueIndex{
						KeyParts: []*tree.KeyPart{
							{
								ColName: def.Name,
							},
						},
					})
					indexs = append(indexs, def.Name.Parts[0])
				}
			}
			if len(pks) > 0 {
				if len(primaryKeys) > 0 {
					return moerr.NewInvalidInput(ctx.GetContext(), "more than one primary key defined")
				}
				primaryKeys = pks
			}

			defaultValue, err := buildDefaultExpr(def, colType, ctx.GetProcess())
			if err != nil {
				return err
			}
			if auto_incr && defaultValue.Expr != nil {
				return moerr.NewInvalidInput(ctx.GetContext(), "invalid default value for '%s'", def.Name.Parts[0])
			}

			onUpdateExpr, err := buildOnUpdate(def, colType, ctx.GetProcess())
			if err != nil {
				return err
			}

			colType.AutoIncr = auto_incr
			col := &ColDef{
				Name:     def.Name.Parts[0],
				Alg:      plan.CompressType_Lz4,
				Typ:      colType,
				Default:  defaultValue,
				OnUpdate: onUpdateExpr,
				Comment:  comment,
			}
			colMap[col.Name] = col
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, col)
		case *tree.PrimaryKeyIndex:
			if len(primaryKeys) > 0 {
				return moerr.NewInvalidInput(ctx.GetContext(), "more than one primary key defined")
			}
			pksMap := map[string]bool{}
			for _, key := range def.KeyParts {
				name := key.ColName.Parts[0] // name of primary key column
				if _, ok := pksMap[name]; ok {
					return moerr.NewInvalidInput(ctx.GetContext(), "duplicate column name '%s' in primary key", name)
				}
				primaryKeys = append(primaryKeys, name)
				pksMap[name] = true
				indexs = append(indexs, name)
			}
		case *tree.Index:
			secondaryIndexInfos = append(secondaryIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.Parts[0]
				indexs = append(indexs, name)
			}

		case *tree.UniqueIndex:
			uniqueIndexInfos = append(uniqueIndexInfos, def)
			for _, key := range def.KeyParts {
				name := key.ColName.Parts[0]
				indexs = append(indexs, name)
			}
		case *tree.ForeignKey:
			refer := def.Refer
			fkDef := &plan.ForeignKeyDef{
				Name:        def.Name,
				Cols:        make([]uint64, len(def.KeyParts)),
				OnDelete:    getRefAction(refer.OnDelete),
				OnUpdate:    getRefAction(refer.OnUpdate),
				ForeignCols: make([]uint64, len(refer.KeyParts)),
			}

			// get fk columns of create table
			fkCols := &plan.CreateTable_FkColName{
				Cols: make([]string, len(def.KeyParts)),
			}
			fkColTyp := make(map[int]*plan.Type)
			fkColName := make(map[int]string)
			for i, keyPart := range def.KeyParts {
				getCol := false
				colName := keyPart.ColName.Parts[0]
				for _, col := range createTable.TableDef.Cols {
					if col.Name == colName {
						// need to reset to ColId after created.
						fkDef.Cols[i] = 0
						fkCols.Cols[i] = colName
						fkColTyp[i] = col.Typ
						fkColName[i] = colName
						getCol = true
						break
					}
				}
				if !getCol {
					return moerr.NewInternalError(ctx.GetContext(), "column '%v' no exists in the creating table '%v'", colName, createTable.TableDef.Name)
				}
			}
			createTable.FkCols = append(createTable.FkCols, fkCols)

			// get foreign table & their columns
			fkTableName := string(refer.TableName.ObjectName)
			fkDbName := string(refer.TableName.SchemaName)
			if fkDbName == "" {
				fkDbName = ctx.DefaultDatabase()
			}
			createTable.FkDbs = append(createTable.FkDbs, fkDbName)
			createTable.FkTables = append(createTable.FkTables, fkTableName)

			_, tableRef := ctx.Resolve(fkDbName, fkTableName)
			if tableRef == nil {
				return moerr.NewNoSuchTable(ctx.GetContext(), ctx.DefaultDatabase(), fkTableName)
			}

			fkDef.ForeignTbl = tableRef.TblId
			columnNames := make(map[string]uint64)
			uniqueColumn := make(map[uint64]bool)
			for _, col := range tableRef.Cols {
				columnNames[col.Name] = col.ColId
				if col.Primary {
					uniqueColumn[col.ColId] = true
				}
			}

			/// now tableRef.Indices is empty, you can not test it
			for _, index := range tableRef.Indices {
				if index.Unique {
					if len(index.Parts) == 1 {
						if colId, ok := getColIdFromExpr(index.Parts[0]); ok {
							uniqueColumn[colId] = true
						}
					}
				}
			}
			for i, keyPart := range refer.KeyParts {
				getCol := false
				colName := keyPart.ColName.Parts[0]
				for _, col := range tableRef.Cols {
					if col.Name == colName {
						if _, ok := uniqueColumn[col.ColId]; ok {
							// check column type
							if col.Typ.Id != fkColTyp[i].Id {
								return moerr.NewInternalError(ctx.GetContext(), "type of reference column '%v' is not match for column '%v'", colName, fkColName[i])
							}
							fkDef.ForeignCols[i] = col.ColId
							getCol = true
						} else {
							return moerr.NewInternalError(ctx.GetContext(), "reference column '%v' is not unique constraint(Unique index or Primary Key)", colName)
						}
						break
					}
				}
				if !getCol {
					return moerr.NewInternalError(ctx.GetContext(), "column '%v' no exists in table '%v'", colName, fkTableName)
				}
			}
			createTable.TableDef.Fkeys = append(createTable.TableDef.Fkeys, fkDef)

		case *tree.CheckIndex, *tree.FullTextIndex:
			// unsupport in plan. will support in next version.
			return moerr.NewNYI(ctx.GetContext(), "table def: '%v'", def)
		default:
			return moerr.NewNYI(ctx.GetContext(), "table def: '%v'", def)
		}
	}

	//add cluster table attribute
	if stmt.IsClusterTable {
		if _, ok := colMap[util.GetClusterTableAttributeName()]; ok {
			return moerr.NewInvalidInput(ctx.GetContext(), "the attribute account_id in the cluster table can not be defined directly by the user")
		}
		colType, err := getTypeFromAst(ctx.GetContext(), util.GetClusterTableAttributeType())
		if err != nil {
			return err
		}
		colDef := &ColDef{
			Name:    util.GetClusterTableAttributeName(),
			Alg:     plan.CompressType_Lz4,
			Typ:     colType,
			NotNull: true,
			Default: &plan.Default{
				Expr: &Expr{
					Expr: &plan.Expr_C{
						C: &Const{
							Isnull: false,
							Value:  &plan.Const_U32Val{U32Val: catalog.System_Account},
						},
					},
					Typ: &plan.Type{
						Id:          colType.Id,
						NotNullable: true,
					},
				},
				NullAbility: false,
			},
			Comment: "the account_id added by the mo",
		}
		colMap[util.GetClusterTableAttributeName()] = colDef
		createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
	}

	pkeyName := ""
	if len(primaryKeys) > 0 {
		for _, primaryKey := range primaryKeys {
			if _, ok := colMap[primaryKey]; !ok {
				return moerr.NewInvalidInput(ctx.GetContext(), "column '%s' doesn't exist in table", primaryKey)
			}
		}
		if len(primaryKeys) == 1 {
			pkeyName = primaryKeys[0]
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Pk{
					Pk: &plan.PrimaryKeyDef{
						Names: primaryKeys,
					},
				},
			})
		} else {
			pkeyName = util.BuildCompositePrimaryKeyColumnName(primaryKeys)
			colDef := &ColDef{
				Name: pkeyName,
				Alg:  plan.CompressType_Lz4,
				Typ: &Type{
					Id:    int32(types.T_varchar),
					Size:  types.VarlenaSize,
					Width: types.MaxVarcharLen,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[pkeyName] = colDef
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Pk{
					Pk: &plan.PrimaryKeyDef{
						Names: []string{pkeyName},
					},
				},
			})
		}
	}

	//handle cluster by keys
	if stmt.ClusterByOption != nil {
		if len(primaryKeys) > 0 {
			return moerr.NewNotSupported(ctx.GetContext(), "cluster by with primary key is not support")
		}
		lenClusterBy := len(stmt.ClusterByOption.ColumnList)
		var clusterByKeys []string
		for i := 0; i < lenClusterBy; i++ {
			colName := stmt.ClusterByOption.ColumnList[i].Parts[0]
			if _, ok := colMap[colName]; !ok {
				return moerr.NewInvalidInput(ctx.GetContext(), "column '%s' doesn't exist in table", colName)
			}
			clusterByKeys = append(clusterByKeys, colName)
		}

		clusterByColName := clusterByKeys[0]
		if lenClusterBy == 1 {
			for _, col := range createTable.TableDef.Cols {
				if col.Name == clusterByColName {
					col.ClusterBy = true
				}
			}
		} else {
			clusterByColName = util.BuildCompositeClusterByColumnName(clusterByKeys)
			colDef := &ColDef{
				Name:      clusterByColName,
				Alg:       plan.CompressType_Lz4,
				ClusterBy: true,
				Typ: &Type{
					Id:    int32(types.T_varchar),
					Size:  types.VarlenaSize,
					Width: types.MaxVarcharLen,
				},
				Default: &plan.Default{
					NullAbility:  true,
					Expr:         nil,
					OriginString: "",
				},
			}
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, colDef)
			colMap[clusterByColName] = colDef
		}
		createTable.TableDef.ClusterBy = &plan.ClusterByDef{
			Name: clusterByColName,
		}
	}

	// check index invalid on the type
	// for example, the text type don't support index
	for _, str := range indexs {
		if _, ok := colMap[str]; !ok {
			return moerr.NewInvalidInput(ctx.GetContext(), "column '%s' is not exist", str)
		}
		if colMap[str].Typ.Id == int32(types.T_blob) {
			return moerr.NewNotSupported(ctx.GetContext(), "blob type in index")
		}
		if colMap[str].Typ.Id == int32(types.T_text) {
			return moerr.NewNotSupported(ctx.GetContext(), "text type in index")
		}
		if colMap[str].Typ.Id == int32(types.T_json) {
			return moerr.NewNotSupported(ctx.GetContext(), fmt.Sprintf("JSON column '%s' cannot be in index", str))
		}
	}

	// build index table
	if len(uniqueIndexInfos) != 0 {
		err := buildUniqueIndexTable(createTable, uniqueIndexInfos, colMap, pkeyName, ctx)
		if err != nil {
			return err
		}
	}
	if len(secondaryIndexInfos) != 0 {
		err := buildSecondaryIndexDef(createTable, secondaryIndexInfos, colMap, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func getColIdFromExpr(e *plan.Expr) (uint64, bool) {
	switch exprImpl := e.Expr.(type) {
	case *plan.Expr_F:
		for _, expr := range exprImpl.F.Args {
			if colId, ok := getColIdFromExpr(expr); ok {
				return colId, ok
			}
		}
		return 0, false
	case *plan.Expr_Col:
		return uint64(exprImpl.Col.ColPos), true
	default:
		return 0, false
	}
}

func getRefAction(typ tree.ReferenceOptionType) plan.ForeignKeyDef_RefAction {
	switch typ {
	case tree.REFERENCE_OPTION_CASCADE:
		return plan.ForeignKeyDef_CASCADE
	case tree.REFERENCE_OPTION_NO_ACTION:
		return plan.ForeignKeyDef_NO_ACTION
	case tree.REFERENCE_OPTION_RESTRICT:
		return plan.ForeignKeyDef_RESTRICT
	case tree.REFERENCE_OPTION_SET_NULL:
		return plan.ForeignKeyDef_SET_NULL
	case tree.REFERENCE_OPTION_SET_DEFAULT:
		return plan.ForeignKeyDef_SET_DEFAULT
	default:
		return plan.ForeignKeyDef_RESTRICT
	}
}

func buildUniqueIndexTable(createTable *plan.CreateTable, indexInfos []*tree.UniqueIndex, colMap map[string]*ColDef, pkeyName string, ctx CompilerContext) error {
	def := &plan.UniqueIndexDef{
		Fields: make([]*plan.Field, 0),
	}
	nameCount := make(map[string]int)

	for _, indexInfo := range indexInfos {
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), true)

		if err != nil {
			return err
		}
		tableDef := &TableDef{
			Name: indexTableName,
		}
		field := &plan.Field{
			Parts: make([]string, 0),
			Cols:  make([]*ColDef, 0),
		}
		for _, keyPart := range indexInfo.KeyParts {
			name := keyPart.ColName.Parts[0]
			if _, ok := colMap[name]; !ok {
				return moerr.NewInvalidInput(ctx.GetContext(), "column '%s' is not exist", name)
			}
			field.Parts = append(field.Parts, name)
		}

		var keyName string
		if len(indexInfo.KeyParts) == 1 {
			keyName = catalog.IndexTableIndexColName
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ: &Type{
					Id:    colMap[indexInfo.KeyParts[0].ColName.Parts[0]].Typ.Id,
					Size:  colMap[indexInfo.KeyParts[0].ColName.Parts[0]].Typ.Size,
					Width: colMap[indexInfo.KeyParts[0].ColName.Parts[0]].Typ.Width,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Pk{
					Pk: &plan.PrimaryKeyDef{
						Names: []string{keyName},
					},
				},
			})
			field.Cols = append(field.Cols, colDef)
		} else {
			keyName = catalog.IndexTableIndexColName
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ: &Type{
					Id:    int32(types.T_varchar),
					Size:  types.VarlenaSize,
					Width: types.MaxVarcharLen,
				},
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Pk{
					Pk: &plan.PrimaryKeyDef{
						Names: []string{keyName},
					},
				},
			})
			field.Cols = append(field.Cols, colDef)
		}
		if pkeyName != "" {
			colDef := &ColDef{
				Name: catalog.IndexTablePrimaryColName,
				Alg:  plan.CompressType_Lz4,
				Typ:  colMap[pkeyName].Typ,
				Default: &plan.Default{
					NullAbility:  false,
					Expr:         nil,
					OriginString: "",
				},
			}
			tableDef.Cols = append(tableDef.Cols, colDef)
			field.Cols = append(field.Cols, colDef)
		}
		if indexInfo.Name == "" {
			firstPart := indexInfo.KeyParts[0].ColName.Parts[0]
			nameCount[firstPart]++
			count := nameCount[firstPart]
			indexName := firstPart
			if count > 1 {
				indexName = firstPart + "_" + strconv.Itoa(count)
			}
			def.IndexNames = append(def.IndexNames, indexName)
		} else {
			def.IndexNames = append(def.IndexNames, indexInfo.Name)
		}
		def.TableNames = append(def.TableNames, indexTableName)
		def.Fields = append(def.Fields, field)
		def.TableExists = append(def.TableExists, true)
		if indexInfo.IndexOption != nil {
			def.Comments = append(def.Comments, indexInfo.IndexOption.Comment)
		} else {
			def.Comments = append(def.Comments, "")
		}
		createTable.IndexTables = append(createTable.IndexTables, tableDef)
	}
	createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_UIdx{
			UIdx: def,
		},
	})
	return nil
}

func buildSecondaryIndexDef(createTable *plan.CreateTable, indexInfos []*tree.Index, colMap map[string]*ColDef, ctx CompilerContext) error {
	def := &plan.SecondaryIndexDef{
		Fields: make([]*plan.Field, 0),
	}
	nameCount := make(map[string]int)

	for _, indexInfo := range indexInfos {
		field := &plan.Field{
			Parts: make([]string, 0),
			Cols:  make([]*ColDef, 0),
		}
		for _, keyPart := range indexInfo.KeyParts {
			name := keyPart.ColName.Parts[0]
			if _, ok := colMap[name]; !ok {
				return moerr.NewInvalidInput(ctx.GetContext(), "column '%s' is not exist", name)
			}
			field.Parts = append(field.Parts, name)
		}

		if indexInfo.Name == "" {
			firstPart := indexInfo.KeyParts[0].ColName.Parts[0]
			nameCount[firstPart]++
			count := nameCount[firstPart]
			indexName := firstPart
			if count > 1 {
				indexName = firstPart + "_" + strconv.Itoa(count)
			}
			def.IndexNames = append(def.IndexNames, indexName)
		} else {
			def.IndexNames = append(def.IndexNames, indexInfo.Name)
		}
		def.TableNames = append(def.TableNames, "")
		def.Fields = append(def.Fields, field)
		def.TableExists = append(def.TableExists, false)
		if indexInfo.IndexOption != nil {
			def.Comments = append(def.Comments, indexInfo.IndexOption.Comment)
		} else {
			def.Comments = append(def.Comments, "")
		}
	}
	createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_SIdx{
			SIdx: def,
		},
	})
	return nil
}

func buildTruncateTable(stmt *tree.TruncateTable, ctx CompilerContext) (*Plan, error) {
	truncateTable := &plan.TruncateTable{}

	truncateTable.Database = string(stmt.Name.SchemaName)
	if truncateTable.Database == "" {
		truncateTable.Database = ctx.DefaultDatabase()
	}
	truncateTable.Table = string(stmt.Name.ObjectName)
	_, tableDef := ctx.Resolve(truncateTable.Database, truncateTable.Table)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
	} else {
		if len(tableDef.RefChildTbls) > 0 {
			return nil, moerr.NewInternalError(ctx.GetContext(), "can not truncate table '%v' referenced by some foreign key constraint", truncateTable.Table)
		}

		if tableDef.ViewSql != nil {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
		}

		truncateTable.TableId = tableDef.TblId
		if tableDef.Fkeys != nil {
			for _, fk := range tableDef.Fkeys {
				truncateTable.ForeignTbl = append(truncateTable.ForeignTbl, fk.ForeignTbl)
			}
		}

		truncateTable.ClusterTable = &plan.ClusterTable{
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		//non-sys account can not truncate the cluster table
		if truncateTable.GetClusterTable().GetIsClusterTable() && ctx.GetAccountId() != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can truncate the cluster table")
		}

		uDef, sDef := buildIndexDefs(tableDef.Defs)
		truncateTable.IndexTableNames = make([]string, 0)
		if uDef != nil {
			for i := 0; i < len(uDef.TableNames); i++ {
				if uDef.TableExists[i] {
					truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, uDef.TableNames[i])
				}
			}
		}
		if sDef != nil {
			for i := 0; i < len(sDef.TableNames); i++ {
				if sDef.TableExists[i] {
					truncateTable.IndexTableNames = append(truncateTable.IndexTableNames, sDef.TableNames[i])
				}
			}
		}
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_TRUNCATE_TABLE,
				Definition: &plan.DataDefinition_TruncateTable{
					TruncateTable: truncateTable,
				},
			},
		},
	}, nil
}

func buildDropTable(stmt *tree.DropTable, ctx CompilerContext) (*Plan, error) {
	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "drop multiple (%d) tables in one statement", len(stmt.Names))
	}
	dropTable.Database = string(stmt.Names[0].SchemaName)
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	dropTable.Table = string(stmt.Names[0].ObjectName)

	_, tableDef := ctx.Resolve(dropTable.Database, dropTable.Table)
	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
	} else {
		if len(tableDef.RefChildTbls) > 0 {
			return nil, moerr.NewInternalError(ctx.GetContext(), "can not drop table '%v' referenced by some foreign key constraint", dropTable.Table)
		}

		isView := (tableDef.ViewSql != nil)

		if isView && !dropTable.IfExists {
			// drop table v0, v0 is view
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		} else if isView {
			// drop table if exists v0, v0 is view
			dropTable.Table = ""
		}

		dropTable.ClusterTable = &plan.ClusterTable{
			IsClusterTable: util.TableIsClusterTable(tableDef.GetTableType()),
		}

		//non-sys account can not drop the cluster table
		if dropTable.GetClusterTable().GetIsClusterTable() && ctx.GetAccountId() != catalog.System_Account {
			return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can drop the cluster table")
		}

		dropTable.TableId = tableDef.TblId
		if tableDef.Fkeys != nil {
			for _, fk := range tableDef.Fkeys {
				dropTable.ForeignTbl = append(dropTable.ForeignTbl, fk.ForeignTbl)
			}
		}

		uDef, sDef := buildIndexDefs(tableDef.Defs)
		dropTable.IndexTableNames = make([]string, 0)
		if uDef != nil {
			for i := 0; i < len(uDef.TableNames); i++ {
				if uDef.TableExists[i] {
					dropTable.IndexTableNames = append(dropTable.IndexTableNames, uDef.TableNames[i])
				}
			}
		}
		if sDef != nil {
			for i := 0; i < len(sDef.TableNames); i++ {
				if sDef.TableExists[i] {
					dropTable.IndexTableNames = append(dropTable.IndexTableNames, sDef.TableNames[i])
				}
			}
		}
	}
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_TABLE,
				Definition: &plan.DataDefinition_DropTable{
					DropTable: dropTable,
				},
			},
		},
	}, nil
}

func buildDropView(stmt *tree.DropView, ctx CompilerContext) (*Plan, error) {
	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "drop multiple (%d) view", len(stmt.Names))
	}
	dropTable.Database = string(stmt.Names[0].SchemaName)
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	dropTable.Table = string(stmt.Names[0].ObjectName)

	_, tableDef := ctx.Resolve(dropTable.Database, dropTable.Table)
	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewBadView(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
	} else {
		if tableDef.ViewSql == nil {
			return nil, moerr.NewBadView(ctx.GetContext(), dropTable.Database, dropTable.Table)
		}
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_TABLE,
				Definition: &plan.DataDefinition_DropTable{
					DropTable: dropTable,
				},
			},
		},
	}, nil
}

func buildCreateDatabase(stmt *tree.CreateDatabase, ctx CompilerContext) (*Plan, error) {
	if string(stmt.Name) == defines.TEMPORARY_DBNAME {
		return nil, moerr.NewInternalError(ctx.GetContext(), "this database name is used by mo temporary engine")
	}
	createDB := &plan.CreateDatabase{
		IfNotExists: stmt.IfNotExists,
		Database:    string(stmt.Name),
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_DATABASE,
				Definition: &plan.DataDefinition_CreateDatabase{
					CreateDatabase: createDB,
				},
			},
		},
	}, nil
}

func buildDropDatabase(stmt *tree.DropDatabase, ctx CompilerContext) (*Plan, error) {
	dropDB := &plan.DropDatabase{
		IfExists: stmt.IfExists,
		Database: string(stmt.Name),
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_DATABASE,
				Definition: &plan.DataDefinition_DropDatabase{
					DropDatabase: dropDB,
				},
			},
		},
	}, nil
}

func buildCreateIndex(stmt *tree.CreateIndex, ctx CompilerContext) (*Plan, error) {
	createIndex := &plan.CreateIndex{}
	if len(stmt.Table.SchemaName) == 0 {
		createIndex.Database = ctx.DefaultDatabase()
	} else {
		createIndex.Database = string(stmt.Table.SchemaName)
	}
	// check table
	tableName := string(stmt.Table.ObjectName)
	_, tableDef := ctx.Resolve(createIndex.Database, tableName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), createIndex.Database, tableName)
	}
	// check index
	indexName := string(stmt.Name)
	for _, def := range tableDef.Defs {
		switch idx := def.Def.(type) {
		case *plan.TableDef_DefType_UIdx:
			for _, name := range idx.UIdx.IndexNames {
				if indexName == name {
					return nil, moerr.NewDuplicateKey(ctx.GetContext(), indexName)
				}
			}
		case *plan.TableDef_DefType_SIdx:
			for _, name := range idx.SIdx.IndexNames {
				if indexName == name {
					return nil, moerr.NewDuplicateKey(ctx.GetContext(), indexName)
				}
			}
		}
	}
	// build index
	var uIdx *tree.UniqueIndex
	var sIdx *tree.Index
	switch stmt.IndexCat {
	case tree.INDEX_CATEGORY_UNIQUE:
		uIdx = &tree.UniqueIndex{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
		}
	case tree.INDEX_CATEGORY_NONE:
		sIdx = &tree.Index{
			Name:        indexName,
			KeyParts:    stmt.KeyParts,
			IndexOption: stmt.IndexOption,
		}
	default:
		return nil, moerr.NewNotSupported(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	}
	colMap := make(map[string]*ColDef)
	for _, col := range tableDef.Cols {
		colMap[col.Name] = col
	}
	// index.TableDef.Defs store info of index need to be modified
	// index.IndexTables store index table need to be created
	oriPriKeyName := GetTablePriKeyName(tableDef.Cols, tableDef.CompositePkey)
	createIndex.OriginTablePrimaryKey = oriPriKeyName

	index := &plan.CreateTable{TableDef: &TableDef{}}
	if uIdx != nil {
		if err := buildUniqueIndexTable(index, []*tree.UniqueIndex{uIdx}, colMap, oriPriKeyName, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = true
	}
	if sIdx != nil {
		if err := buildSecondaryIndexDef(index, []*tree.Index{sIdx}, colMap, ctx); err != nil {
			return nil, err
		}
		createIndex.TableExist = false
	}
	createIndex.Index = index
	createIndex.Table = tableName

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_INDEX,
				Definition: &plan.DataDefinition_CreateIndex{
					CreateIndex: createIndex,
				},
			},
		},
	}, nil
}

func buildDropIndex(stmt *tree.DropIndex, ctx CompilerContext) (*Plan, error) {
	dropIndex := &plan.DropIndex{}
	if len(stmt.TableName.SchemaName) == 0 {
		dropIndex.Database = ctx.DefaultDatabase()
	} else {
		dropIndex.Database = string(stmt.TableName.SchemaName)
	}

	// check table
	dropIndex.Table = string(stmt.TableName.ObjectName)
	_, tableDef := ctx.Resolve(dropIndex.Database, dropIndex.Table)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropIndex.Database, dropIndex.Table)
	}

	// check index
	dropIndex.IndexName = string(stmt.Name)
	found := false
	for _, def := range tableDef.Defs {
		switch idx := def.Def.(type) {
		case *plan.TableDef_DefType_UIdx:
			for i, name := range idx.UIdx.IndexNames {
				if dropIndex.IndexName == name {
					dropIndex.IndexTableName = idx.UIdx.TableNames[i]
					found = true
					break
				}
			}
		case *plan.TableDef_DefType_SIdx:
			for i, name := range idx.SIdx.IndexNames {
				if dropIndex.IndexName == name {
					dropIndex.IndexTableName = idx.SIdx.TableNames[i]
					found = true
					break
				}
			}
		}
	}
	if !found {
		return nil, moerr.NewInternalError(ctx.GetContext(), "not found index: %s", dropIndex.IndexName)
	}

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_DROP_INDEX,
				Definition: &plan.DataDefinition_DropIndex{
					DropIndex: dropIndex,
				},
			},
		},
	}, nil
}

// Get tabledef(col, viewsql, properties) for alterview.
func buildAlterView(stmt *tree.AlterView, ctx CompilerContext) (*Plan, error) {
	viewName := stmt.Name.ObjectName
	alterView := &plan.AlterView{
		IfExists:  stmt.IfExists,
		Temporary: stmt.Temporary,
		TableDef: &plan.TableDef{
			Name: string(viewName),
		},
	}
	// get database name
	if len(stmt.Name.SchemaName) == 0 {
		alterView.Database = ""
	} else {
		alterView.Database = string(stmt.Name.SchemaName)
	}

	tableDef, err := genViewTableDef(ctx, stmt.AsSource)
	if err != nil {
		return nil, err
	}

	alterView.TableDef.Cols = tableDef.Cols
	alterView.TableDef.ViewSql = tableDef.ViewSql
	alterView.TableDef.Defs = tableDef.Defs

	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_VIEW,
				Definition: &plan.DataDefinition_AlterView{
					AlterView: alterView,
				},
			},
		},
	}, nil
}
