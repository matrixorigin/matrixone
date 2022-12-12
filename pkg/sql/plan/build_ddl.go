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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/operator"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

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

	// check view statement
	stmtPlan, err := runBuildSelectByBinder(plan.Query_SELECT, ctx, stmt.AsSource)
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
	createTable.TableDef.Cols = cols

	viewData, err := json.Marshal(ViewData{
		Stmt:            ctx.GetRootSql(),
		DefaultDatabase: ctx.DefaultDatabase(),
	})
	if err != nil {
		return nil, err
	}
	createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_View{
			View: &plan.ViewDef{
				View: string(viewData),
			},
		},
	})
	properties := []*plan.Property{
		{
			Key:   catalog.SystemRelAttr_Kind,
			Value: catalog.SystemViewRel,
		},
	}
	createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_Properties{
			Properties: &plan.PropertiesDef{
				Properties: properties,
			},
		},
	})

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
	err := buildTableDefs(stmt.Defs, ctx, createTable)
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
		properties := []*plan.Property{
			{
				Key:   catalog.SystemRelAttr_Kind,
				Value: catalog.SystemOrdinaryRel,
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

	if stmt.ClusterByOption != nil {
		if util.FindPrimaryKey(createTable.TableDef) {
			return nil, moerr.NewNotSupported(ctx.GetContext(), "cluster by with primary key is not support")
		}
		if len(stmt.ClusterByOption.ColumnList) > 1 {
			return nil, moerr.NewNYI(ctx.GetContext(), "cluster by multi columns")
		}
		colName := stmt.ClusterByOption.ColumnList[0].Parts[0]
		var found bool
		for _, col := range createTable.TableDef.Cols {
			if col.Name == colName {
				found = true
				col.ClusterBy = true
			}
		}
		if !found {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "column '%s' doesn't exist in table", colName)
		}
		createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Cb{
				Cb: &plan.ClusterByDef{
					Name: colName,
				},
			},
		})
	}

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

func buildTableDefs(defs tree.TableDefs, ctx CompilerContext, createTable *plan.CreateTable) error {
	var primaryKeys []string
	var indexs []string
	colMap := make(map[string]*ColDef)
	indexInfos := make([]*tree.UniqueIndex, 0)
	for _, item := range defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colType, err := getTypeFromAst(def.Type)
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
				if _, ok := attr.(*tree.AttributePrimaryKey); ok {
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
				}

				if attrComment, ok := attr.(*tree.AttributeComment); ok {
					comment = attrComment.CMT.String()
					if getNumOfCharacters(comment) > maxLengthOfColumnComment {
						return moerr.NewInvalidInput(ctx.GetContext(), "comment for column '%s' is too long", def.Name.Parts[0])
					}
				}

				if _, ok := attr.(*tree.AttributeAutoIncrement); ok {
					auto_incr = true
					if !operator.IsInteger(types.T(colType.GetId())) {
						return moerr.NewNotSupported(ctx.GetContext(), "the auto_incr column is only support integer type now")
					}
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
			// TODO
			nameMap := map[string]bool{}
			for _, key := range def.KeyParts {
				name := key.ColName.Parts[0] // name of index column
				if _, ok := nameMap[name]; ok {
					return moerr.NewInvalidInput(ctx.GetContext(), "duplicate column name '%s' in primary key", name)
				}
				nameMap[name] = true
				indexs = append(indexs, name)
			}
		case *tree.UniqueIndex:
			indexInfos = append(indexInfos, def)
		case *tree.CheckIndex, *tree.ForeignKey, *tree.FullTextIndex:
			// unsupport in plan. will support in next version.
			return moerr.NewNYI(ctx.GetContext(), "table def: '%v'", def)
		default:
			return moerr.NewNYI(ctx.GetContext(), "table def: '%v'", def)
		}
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
			createTable.TableDef.Cols = append(createTable.TableDef.Cols, &ColDef{
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
			})
			createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Pk{
					Pk: &plan.PrimaryKeyDef{
						Names: []string{pkeyName},
					},
				},
			})
		}
	}

	// check index invalid on the type
	// for example, the text type don't support index
	for _, str := range indexs {
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
	if len(indexInfos) != 0 {
		err := buildUniqueIndexTable(createTable, indexInfos, colMap, pkeyName, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func buildUniqueIndexTable(createTable *plan.CreateTable, indexInfos []*tree.UniqueIndex, colMap map[string]*ColDef, pkeyName string, ctx CompilerContext) error {
	def := &plan.UniqueIndexDef{
		Fields: make([]*plan.Field, 0),
	}

	for _, indexInfo := range indexInfos {
		indexTableName, err := util.BuildIndexTableName(ctx.GetContext(), true, indexInfo.Name)

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
			keyName = field.Parts[0]
			colDef := &ColDef{
				Name: keyName,
				Alg:  plan.CompressType_Lz4,
				Typ: &Type{
					Id:   colMap[keyName].Typ.Id,
					Size: colMap[keyName].Typ.Size,
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
			keyName = util.BuildCompositePrimaryKeyColumnName(field.Parts)
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
		def.IndexNames = append(def.IndexNames, indexInfo.Name)
		def.TableNames = append(def.TableNames, indexTableName)
		def.Fields = append(def.Fields, field)
		def.TableExists = append(def.TableExists, true)
		createTable.IndexTables = append(createTable.IndexTables, tableDef)
	}
	createTable.TableDef.Defs = append(createTable.TableDef.Defs, &plan.TableDef_DefType{
		Def: &plan.TableDef_DefType_UIdx{
			UIdx: def,
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
		isView := false
		for _, def := range tableDef.Defs {
			if _, ok := def.Def.(*plan.TableDef_DefType_View); ok {
				isView = true
				break
			}
		}
		if isView {
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), truncateTable.Database, truncateTable.Table)
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
		isView := false
		for _, def := range tableDef.Defs {
			if _, ok := def.Def.(*plan.TableDef_DefType_View); ok {
				isView = true
				break
			}
		}
		if isView && !dropTable.IfExists {
			// drop table v0, v0 is view
			return nil, moerr.NewNoSuchTable(ctx.GetContext(), dropTable.Database, dropTable.Table)
		} else if isView {
			// drop table if exists v0, v0 is view
			dropTable.Table = ""
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
		isView := false
		for _, def := range tableDef.Defs {
			if _, ok := def.Def.(*plan.TableDef_DefType_View); ok {
				isView = true
				break
			}
		}
		if !isView {
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
	return nil, moerr.NewNotSupported(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	// todo unsupport now
	// createIndex := &plan.CreateIndex{}
	// return &Plan{
	// 	Plan: &plan.Plan_Ddl{
	// 		Ddl: &plan.DataDefinition{
	// 			DdlType: plan.DataDefinition_CREATE_INDEX,
	// 			Definition: &plan.DataDefinition_CreateIndex{
	// 				CreateIndex: createIndex,
	// 			},
	// 		},
	// 	},
	// }, nil
}

func buildDropIndex(stmt *tree.DropIndex, ctx CompilerContext) (*Plan, error) {
	return nil, moerr.NewNotSupported(ctx.GetContext(), "statement: '%v'", tree.String(stmt, dialect.MYSQL))
	// todo unsupport now
	// dropIndex := &plan.DropIndex{
	// 	IfExists: stmt.IfExists,
	// 	Index:    string(stmt.Name),
	// }
	// return &Plan{
	// 	Plan: &plan.Plan_Ddl{
	// 		Ddl: &plan.DataDefinition{
	// 			DdlType: plan.DataDefinition_DROP_INDEX,
	// 			Definition: &plan.DataDefinition_DropIndex{
	// 				DropIndex: dropIndex,
	// 			},
	// 		},
	// 	},
	// }, nil
}
