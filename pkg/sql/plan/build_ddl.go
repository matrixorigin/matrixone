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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

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
				errmsg := fmt.Sprintf("Comment for table '%s' is too long (max = %d)", createTable.TableDef.Name, maxLengthOfTableComment)
				return nil, errors.New(errno.InvalidOptionValue, errmsg)
			}

			properties := []*plan.Property{
				{
					Key:   "Comment",
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
		// 	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
		default:
			return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected options; statement: '%v'", tree.String(stmt, dialect.MYSQL)))
		}
	}

	// set partition(unsupport now)
	if stmt.PartitionOption != nil {
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("partition unsupport now; statement: '%v'", tree.String(stmt, dialect.MYSQL)))
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

func buildTableDefs(defs tree.TableDefs, ctx CompilerContext, tableDef *TableDef) error {
	var primaryKeys []string
	var indexs []string
	colNameMap := make(map[string]plan.Type_TypeId)
	for _, item := range defs {
		switch def := item.(type) {
		case *tree.ColumnTableDef:
			colType, err := getTypeFromAst(def.Type)
			if err != nil {
				return err
			}
			if colType.Id == plan.Type_CHAR || colType.Id == plan.Type_VARCHAR {
				if colType.GetWidth() > types.MaxStringSize {
					return errors.New(errno.DataException, "width out of 1GB is unexpected for char/varchar type")
				}
			}
			defultValue, err := getDefaultExprFromColumn(def, colType)
			if err != nil {
				return err
			}

			var pks []string
			var comment string
			for _, attr := range def.Attributes {
				if _, ok := attr.(*tree.AttributePrimaryKey); ok {
					if colType.GetId() == plan.Type_BLOB {
						return errors.New(errno.InvalidColumnDefinition, "Type text don't support primary key")
					}
					pks = append(pks, def.Name.Parts[0])
				}

				if attrComment, ok := attr.(*tree.AttributeComment); ok {
					comment = attrComment.CMT.String()
					if getNumOfCharacters(comment) > maxLengthOfColumnComment {
						errmsg := fmt.Sprintf("Comment for field '%s' is too long (max = %d)", def.Name.Parts[0], maxLengthOfColumnComment)
						return errors.New(errno.InvalidOptionValue, errmsg)
					}
				}
			}
			if len(pks) > 0 {
				if len(primaryKeys) > 0 {
					return errors.New(errno.SyntaxErrororAccessRuleViolation, "Multiple primary key defined")
				}
				primaryKeys = pks
			}
			col := &ColDef{
				Name:    def.Name.Parts[0],
				Alg:     plan.CompressType_Lz4,
				Typ:     colType,
				Default: defultValue,
				Comment: comment,
			}
			colNameMap[col.Name] = col.Typ.GetId()
			tableDef.Cols = append(tableDef.Cols, col)
		case *tree.PrimaryKeyIndex:
			if len(primaryKeys) > 0 {
				return errors.New(errno.SyntaxErrororAccessRuleViolation, "Multiple primary key defined")
			}
			pksMap := map[string]bool{}
			for _, key := range def.KeyParts {
				name := key.ColName.Parts[0] // name of primary key column
				if _, ok := pksMap[name]; ok {
					return errors.New(errno.InvalidTableDefinition, fmt.Sprintf("Duplicate column name '%s'", name))
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
				// return errors.New(errno.InvalidTableDefinition, fmt.Sprintf("Invaild index type '%s'", def.KeyType.ToString()))
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
					return errors.New(errno.InvalidTableDefinition, fmt.Sprintf("Duplicate column name '%s'", key.ColName.Parts[0]))
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
			return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table def: '%v'", def))
		default:
			return errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unsupport table def: '%v'", def))
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
		if colNameMap[str] == plan.Type_BLOB {
			return errors.New(errno.InvalidColumnDefinition, "Type text don't support index")
		}
	}
	return nil
}

func buildDropTable(stmt *tree.DropTable, ctx CompilerContext) (*Plan, error) {
	dropTable := &plan.DropTable{
		IfExists: stmt.IfExists,
	}
	if len(stmt.Names) != 1 {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, "support drop one table now")
	}
	dropTable.Database = string(stmt.Names[0].SchemaName)
	if dropTable.Database == "" {
		dropTable.Database = ctx.DefaultDatabase()
	}
	dropTable.Table = string(stmt.Names[0].ObjectName)

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
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
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
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", tree.String(stmt, dialect.MYSQL)))
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
