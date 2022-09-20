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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
	cols := make([]*plan.ColDef, len(query.Headings))
	for idx, expr := range query.Nodes[query.Steps[len(query.Steps)-1]].ProjectList {
		cols[idx] = &plan.ColDef{
			Name: query.Headings[idx],
			Alg:  plan.CompressType_Lz4,
			Typ:  expr.Typ,
			Default: &plan.Default{
				NullAbility:  false,
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
		createTable.Database = ""
	} else {
		createTable.Database = string(stmt.Table.SchemaName)
	}

	// set tableDef
	err := buildTableDefs(stmt.Defs, ctx, createTable.TableDef)
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
				return nil, moerr.NewInvalidInput("comment for field '%s' is too long", createTable.TableDef.Name)
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
			return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
		}
	}

	// After handleTableOptions, so begin the partitions processing depend on TableDef
	if stmt.Param != nil {
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
			},
		})
	}

	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
	bindContext := NewBindContext(builder, nil)

	// set partition(unsupport now)
	if stmt.PartitionOption != nil {
		nodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			Cost:        nil,
			ObjRef:      nil,
			TableDef:    createTable.TableDef,
			BindingTags: []int32{builder.genNewTag()},
		}, bindContext)

		err = builder.addBinding(nodeID, tree.AliasClause{}, bindContext)
		if err != nil {
			return nil, err
		}
		partitionBinder := NewPartitionBinder(builder, bindContext)
		err = buildPartitionByClause(partitionBinder, stmt.PartitionOption, createTable.TableDef)
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
func buildPartitionByClause(partitionBinder *PartitionBinder, partitionOp *tree.PartitionOption, tableDef *TableDef) (err error) {
	switch partitionOp.PartBy.PType.(type) {
	case *tree.HashType:
		err = buildHashPartition(partitionBinder, partitionOp, tableDef)
	case *tree.KeyType:
		err = buildKeyPartition(partitionBinder, partitionOp, tableDef)
	case *tree.RangeType:
		err = buildRangePartition(partitionBinder, partitionOp, tableDef)
	case *tree.ListType:
		err = buildListPartitiion(partitionBinder, partitionOp, tableDef)
	}
	return err
}

func buildTableDefs(defs tree.TableDefs, ctx CompilerContext, tableDef *TableDef) error {
	var primaryKeys []string
	var indexs []string
	colNameMap := make(map[string]int32)
	for _, item := range defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colType, err := getTypeFromAst(def.Type)
			if err != nil {
				return err
			}
			if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) {
				if colType.GetWidth() > types.MaxStringSize {
					return moerr.NewInvalidInput("string width (%d) is too long", colType.GetWidth())
				}
			}

			var pks []string
			var comment string
			var auto_incr bool
			for _, attr := range def.Attributes {
				if _, ok := attr.(*tree.AttributePrimaryKey); ok {
					if colType.GetId() == int32(types.T_blob) {
						return moerr.NewNotSupported("text type in primary key")
					}
					pks = append(pks, def.Name.Parts[0])
				}

				if attrComment, ok := attr.(*tree.AttributeComment); ok {
					comment = attrComment.CMT.String()
					if getNumOfCharacters(comment) > maxLengthOfColumnComment {
						return moerr.NewInvalidInput("comment for column '%s' is too long", def.Name.Parts[0])
					}
				}

				if _, ok := attr.(*tree.AttributeAutoIncrement); ok {
					auto_incr = true
					if colType.GetId() != int32(types.T_int32) && colType.GetId() != int32(types.T_int64) {
						return moerr.NewNotSupported("auto_incr column type is not int32/int64")
					}
				}
			}
			if len(pks) > 0 {
				if len(primaryKeys) > 0 {
					return moerr.NewInvalidInput("more than one primary key defined")
				}
				primaryKeys = pks
			}

			defaultValue, err := buildDefaultExpr(def, colType)
			if err != nil {
				return err
			}
			if auto_incr && defaultValue.Expr != nil {
				return moerr.NewInvalidInput("invalid default value for '%s'", def.Name.Parts[0])
			}

			onUpdateExpr, err := buildOnUpdate(def, colType)
			if err != nil {
				return err
			}

			col := &ColDef{
				Name:          def.Name.Parts[0],
				Alg:           plan.CompressType_Lz4,
				Typ:           colType,
				Default:       defaultValue,
				OnUpdate:      onUpdateExpr,
				Comment:       comment,
				AutoIncrement: auto_incr,
			}
			colNameMap[col.Name] = col.Typ.GetId()
			tableDef.Cols = append(tableDef.Cols, col)
		case *tree.PrimaryKeyIndex:
			if len(primaryKeys) > 0 {
				return moerr.NewInvalidInput("more than one primary key defined")
			}
			pksMap := map[string]bool{}
			for _, key := range def.KeyParts {
				name := key.ColName.Parts[0] // name of primary key column
				if _, ok := pksMap[name]; ok {
					return moerr.NewInvalidInput("duplicate column name '%s' in primary key", name)
				}
				primaryKeys = append(primaryKeys, name)
				pksMap[name] = true
				indexs = append(indexs, name)
			}
		case *tree.Index:
			var idxType plan.IndexDef_IndexType
			switch def.KeyType {
			case tree.INDEX_TYPE_BSI:
				idxType = plan.IndexDef_BSI
			case tree.INDEX_TYPE_ZONEMAP:
				idxType = plan.IndexDef_ZONEMAP
			default:
				idxType = plan.IndexDef_ZONEMAP //default
			}

			idxDef := &plan.IndexDef{
				Typ:      idxType,
				Name:     def.Name,
				ColNames: make([]string, len(def.KeyParts)),
			}

			nameMap := map[string]bool{}
			for i, key := range def.KeyParts {
				name := key.ColName.Parts[0] // name of index column
				if _, ok := nameMap[name]; ok {
					return moerr.NewInvalidInput("duplicate column name '%s' in primary key", name)
				}
				idxDef.ColNames[i] = name
				nameMap[name] = true
				indexs = append(indexs, name)
			}

			tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Idx{
					Idx: idxDef,
				},
			})
		case *tree.UniqueIndex, *tree.CheckIndex, *tree.ForeignKey, *tree.FullTextIndex:
			// unsupport in plan. will support in next version.
			return moerr.NewNYI("table def: '%v'", def)
		default:
			return moerr.NewNYI("table def: '%v'", def)
		}
	}

	if len(primaryKeys) > 0 {
		tableDef.Defs = append(tableDef.Defs, &plan.TableDef_DefType{
			Def: &plan.TableDef_DefType_Pk{
				Pk: &plan.PrimaryKeyDef{
					Names: primaryKeys,
				},
			},
		})
	}

	// check index invalid on the type
	// for example, the text type don't support index
	for _, str := range indexs {
		if colNameMap[str] == int32(types.T_blob) {
			return moerr.NewNotSupported("text type in index")
		}
	}
	return nil
}

func buildDropTable(stmt *tree.DropTable, ctx CompilerContext) (*Plan, error) {
	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, moerr.NewNotSupported("drop multiple (%d) tables in one statement", len(stmt.Names))
	}
	dropTable.Database = string(stmt.Names[0].SchemaName)
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	dropTable.Table = string(stmt.Names[0].ObjectName)

	_, tableDef := ctx.Resolve(dropTable.Database, dropTable.Table)
	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewNoSuchTable(dropTable.Database, dropTable.Table)
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
			return nil, moerr.NewNoSuchTable(dropTable.Database, dropTable.Table)
		} else if isView {
			// drop table if exists v0, v0 is view
			dropTable.Table = ""
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
		return nil, moerr.NewNotSupported("drop multiple (%d) view", len(stmt.Names))
	}
	dropTable.Database = string(stmt.Names[0].SchemaName)
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	dropTable.Table = string(stmt.Names[0].ObjectName)

	_, tableDef := ctx.Resolve(dropTable.Database, dropTable.Table)
	if tableDef == nil {
		if !dropTable.IfExists {
			return nil, moerr.NewBadView(dropTable.Database, dropTable.Table)
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
			return nil, moerr.NewBadView(dropTable.Database, dropTable.Table)
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
	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
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
	return nil, moerr.NewNotSupported("statement: '%v'", tree.String(stmt, dialect.MYSQL))
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
