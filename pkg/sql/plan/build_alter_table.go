// Copyright 2023 Matrix Origin
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
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func buildAlterTableCopy(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
	// 1. get origin table name and Schema name
	schemaName, tableName := string(stmt.Table.Schema()), string(stmt.Table.Name())
	if schemaName == "" {
		schemaName = ctx.DefaultDatabase()
	}

	var snapshot *Snapshot
	_, tableDef, err := ctx.Resolve(schemaName, tableName, snapshot)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
	}

	isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if isClusterTable && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can alter the cluster table")
	}

	// 2. split alter_option list
	copyTableDef, err := buildCopyTableDef(ctx.GetContext(), tableDef)
	if err != nil {
		return nil, err
	}
	alterTableCtx := initAlterTableContext(tableDef, copyTableDef, schemaName)

	// 3. check alter_option list
	// set name for anonymous foreign key.
	tmpForeignKeyId := 0
	validAlterSpecs := stmt.Options
	for _, spec := range validAlterSpecs {
		if alterOpt, ok := spec.(*tree.AlterOptionAdd); ok {
			if foreignKey, ok2 := alterOpt.Def.(*tree.ForeignKey); ok2 && foreignKey.Name == "" {
				foreignKey.Name = fmt.Sprintf("fk_%d", tmpForeignKeyId)
			}
		}
	}

	// 4. traverse and handle alter options
	alterTablePlan := &plan.AlterTable{
		Database:       schemaName,
		TableDef:       tableDef,
		CopyTableDef:   copyTableDef,
		IsClusterTable: isClusterTable,
		AlgorithmType:  plan.AlterTable_COPY,
	}

	for _, spec := range validAlterSpecs {
		switch option := spec.(type) {
		case *tree.AlterOptionAdd:
			switch optionAdd := option.Def.(type) {
			case *tree.PrimaryKeyIndex:
				err = AddPrimaryKey(ctx, alterTablePlan, optionAdd, alterTableCtx)
			case *tree.ForeignKey:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", optionAdd)
			case *tree.UniqueIndex:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", optionAdd)
			case *tree.Index:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", optionAdd)
			case *tree.ColumnTableDef:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", optionAdd)
			default:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", optionAdd)
			}
		case *tree.AlterOptionDrop:
			switch option.Typ {
			case tree.AlterTableDropColumn:
				//return nil, moerr.NewInvalidInput(ctx.GetContext(), "Do not support this stmt now. %v", option)
				err = DropColumn(ctx, alterTablePlan, string(option.Name), alterTableCtx)
			case tree.AlterTableDropIndex:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", option)
			case tree.AlterTableDropKey:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", option)
			case tree.AlterTableDropPrimaryKey:
				err = DropPrimaryKey(ctx, alterTablePlan, alterTableCtx)
			case tree.AlterTableDropForeignKey:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", option)
			default:
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", option)
			}
		case *tree.AlterOptionAlterIndex:
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", spec)
		case *tree.AlterOptionAlterReIndex:
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", spec)
		case *tree.TableOptionComment:
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", spec)
		case *tree.AlterOptionTableName:
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", spec)
		case *tree.AlterAddCol:
			err = AddColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.AlterTableModifyColumnClause:
			err = ModifyColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.AlterTableChangeColumnClause:
			err = ChangeColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.AlterTableRenameColumnClause:
			err = RenameColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.AlterTableAlterColumnClause:
			err = AlterColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.AlterTableOrderByColumnClause:
			err = OrderByColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.TableOptionAutoIncrement:
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "Do not support this stmt now. %v", spec)
		default:
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "Do not support this stmt now.")
		}
		if err != nil {
			return nil, err
		}
	}

	createTmpDdl, _, err := ConstructCreateTableSQL(ctx, copyTableDef, snapshot, true)
	if err != nil {
		return nil, err
	}
	alterTablePlan.CreateTmpTableSql = createTmpDdl

	insertTmpDml, err := buildAlterInsertDataSQL(ctx, alterTableCtx)
	if err != nil {
		return nil, err
	}
	alterTablePlan.InsertTmpDataSql = insertTmpDml

	alterTablePlan.ChangeTblColIdMap = alterTableCtx.changColDefMap
	alterTablePlan.UpdateFkSqls = append(alterTablePlan.UpdateFkSqls, alterTableCtx.UpdateSqls...)
	//delete copy table records from mo_catalog.mo_foreign_keys
	alterTablePlan.UpdateFkSqls = append(alterTablePlan.UpdateFkSqls, getSqlForDeleteTable(schemaName, alterTableCtx.copyTableName))
	return &Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_ALTER_TABLE,
				Definition: &plan.DataDefinition_AlterTable{
					AlterTable: alterTablePlan,
				},
			},
		},
	}, nil
}

func buildAlterInsertDataSQL(ctx CompilerContext, alterCtx *AlterTableContext) (string, error) {
	schemaName := alterCtx.schemaName
	originTableName := alterCtx.originTableName
	copyTableName := alterCtx.copyTableName

	insertBuffer := bytes.NewBufferString("")
	selectBuffer := bytes.NewBufferString("")

	isFirst := true
	for key, value := range alterCtx.alterColMap {
		if isFirst {
			insertBuffer.WriteString("`" + key + "`")
			if value.sexprType == columnName {
				selectBuffer.WriteString("`" + value.sexprStr + "`")
			} else {
				selectBuffer.WriteString(value.sexprStr)
			}
			isFirst = false
		} else {
			insertBuffer.WriteString(", " + "`" + key + "`")

			if value.sexprType == columnName {
				selectBuffer.WriteString(", " + "`" + value.sexprStr + "`")
			} else {
				selectBuffer.WriteString(", " + value.sexprStr)
			}
		}
	}

	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) SELECT %s FROM `%s`.`%s`",
		formatStr(schemaName), formatStr(copyTableName), insertBuffer.String(),
		selectBuffer.String(), formatStr(schemaName), formatStr(originTableName))
	return insertSQL, nil
}

const UnKnownColId uint64 = math.MaxUint64

type AlterTableContext struct {
	// key   --> Copy table column name, letter case: lower
	// value --> Original table column name
	alterColMap     map[string]selectExpr
	schemaName      string
	originTableName string
	copyTableName   string
	// key oldColId -> new ColDef
	changColDefMap map[uint64]*ColDef
	UpdateSqls     []string
}

type exprType int

const (
	constValue exprType = iota
	columnName
)

type selectExpr struct {
	sexprType exprType
	sexprStr  string
}

func initAlterTableContext(originTableDef *TableDef, copyTableDef *TableDef, schemaName string) *AlterTableContext {
	alterTblColMap := make(map[string]selectExpr)
	changTblColIdMap := make(map[uint64]*ColDef)
	for _, coldef := range originTableDef.Cols {
		if coldef.Hidden {
			continue
		}

		alterTblColMap[coldef.Name] = selectExpr{
			sexprType: columnName,
			sexprStr:  coldef.Name,
		}

		changTblColIdMap[coldef.ColId] = &plan.ColDef{
			ColId:      UnKnownColId,
			Name:       coldef.Name,
			OriginName: coldef.OriginName,
		}
	}
	return &AlterTableContext{
		alterColMap:     alterTblColMap,
		schemaName:      schemaName,
		originTableName: originTableDef.Name,
		copyTableName:   copyTableDef.Name,
		changColDefMap:  changTblColIdMap,
	}
}

func buildCopyTableDef(ctx context.Context, tableDef *TableDef) (*TableDef, error) {
	replicaTableDef := DeepCopyTableDef(tableDef, true)

	id, err := uuid.NewV7()
	if err != nil {
		return nil, moerr.NewInternalError(ctx, "new uuid failed")
	}
	replicaTableDef.Name = replicaTableDef.Name + "_copy_" + id.String()
	return replicaTableDef, nil
}

func buildAlterTable(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
	// ALTER TABLE tbl_name
	//		[alter_option [, alter_option] ...]
	//		[partition_options]
	schemaName, tableName := string(stmt.Table.Schema()), string(stmt.Table.Name())
	if schemaName == "" {
		schemaName = ctx.DefaultDatabase()
	}
	objRef, tableDef, err := ctx.Resolve(schemaName, tableName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
	}

	if tableDef.IsTemporary {
		return nil, moerr.NewNYI(ctx.GetContext(), "alter table for temporary table")
	}

	if tableDef.ViewSql != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "you should use alter view statemnt for View")
	}
	if objRef.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter table in subscription database")
	}
	isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
	accountId, err := ctx.GetAccountId()
	if err != nil {
		return nil, err
	}
	if isClusterTable && accountId != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can alter the cluster table")
	}

	if stmt.PartitionOption != nil {
		if stmt.Options != nil {
			return nil, moerr.NewParseError(ctx.GetContext(), "Unsupported multi schema change")
		}
		return buildAlterTableInplace(stmt, ctx)
	}

	algorithm := ResolveAlterTableAlgorithm(ctx.GetContext(), stmt.Options, tableDef)
	if algorithm == plan.AlterTable_COPY {
		return buildAlterTableCopy(stmt, ctx)
	} else {
		return buildAlterTableInplace(stmt, ctx)
	}
}

func ResolveAlterTableAlgorithm(ctx context.Context, validAlterSpecs []tree.AlterTableOption, tableDef *TableDef) (algorithm plan.AlterTable_AlgorithmType) {
	algorithm = plan.AlterTable_COPY
	for _, spec := range validAlterSpecs {
		switch option := spec.(type) {
		case *tree.AlterOptionAdd:
			switch option.Def.(type) {
			case *tree.PrimaryKeyIndex:
				algorithm = plan.AlterTable_COPY
			case *tree.ForeignKey:
				algorithm = plan.AlterTable_INPLACE
			case *tree.UniqueIndex:
				algorithm = plan.AlterTable_INPLACE
			case *tree.Index:
				algorithm = plan.AlterTable_INPLACE
			case *tree.ColumnTableDef:
				algorithm = plan.AlterTable_INPLACE
			default:
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterOptionDrop:
			switch option.Typ {
			case tree.AlterTableDropColumn:
				algorithm = plan.AlterTable_COPY
			case tree.AlterTableDropIndex:
				algorithm = plan.AlterTable_INPLACE
			case tree.AlterTableDropKey:
				algorithm = plan.AlterTable_INPLACE
			case tree.AlterTableDropPrimaryKey:
				algorithm = plan.AlterTable_COPY
			case tree.AlterTableDropForeignKey:
				algorithm = plan.AlterTable_INPLACE
			default:
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterOptionAlterIndex:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterOptionAlterReIndex:
			algorithm = plan.AlterTable_INPLACE
		case *tree.TableOptionComment:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterOptionTableName:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterAddCol:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableModifyColumnClause:
			algorithm = plan.AlterTable_COPY
			if isVarcharLengthModified(ctx, option, tableDef) {
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterTableChangeColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableRenameColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableAlterColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableOrderByColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.TableOptionAutoIncrement:
			algorithm = plan.AlterTable_INPLACE
		default:
			algorithm = plan.AlterTable_INPLACE
		}
		if algorithm != plan.AlterTable_COPY {
			return algorithm
		}
	}
	return algorithm
}

func isVarcharLengthModified(ctx context.Context, spec *tree.AlterTableModifyColumnClause, tableDef *TableDef) bool {
	specNewColumn := spec.NewColumn
	colName := specNewColumn.Name.ColName()

	logutil.Infof("DEBUG isVarcharLengthModified: Starting function for table %s, column %s", tableDef.Name, colName)

	// Find the original column
	oldCol := FindColumn(tableDef.Cols, colName)
	if oldCol == nil || oldCol.Hidden {
		logutil.Infof("DEBUG isVarcharLengthModified: Column %s not found or hidden in table %s", colName, tableDef.Name)
		return false
	}

	logutil.Infof("DEBUG isVarcharLengthModified: Found old column %s - Type ID: %d, Width: %d, Scale: %d, AutoIncr: %t, NotNull: %t",
		colName, oldCol.Typ.Id, oldCol.Typ.Width, oldCol.Typ.Scale, oldCol.Typ.AutoIncr, oldCol.Default != nil && !oldCol.Default.NullAbility)

	// Parse the new column type
	newColType, err := getTypeFromAst(ctx, specNewColumn.Type)
	if err != nil {
		logutil.Infof("DEBUG isVarcharLengthModified: Failed to parse new column type for %s: %v", colName, err)
		return false
	}

	logutil.Infof("DEBUG isVarcharLengthModified: New column type - ID: %d, Width: %d, Scale: %d, AutoIncr: %t",
		newColType.Id, newColType.Width, newColType.Scale, newColType.AutoIncr)

	// Check if both old and new are varchar types
	oldIsVarchar := oldCol.Typ.Id == int32(types.T_varchar)
	newIsVarchar := newColType.Id == int32(types.T_varchar)

	logutil.Infof("DEBUG isVarcharLengthModified: Old is varchar: %t, New is varchar: %t", oldIsVarchar, newIsVarchar)

	if !oldIsVarchar || !newIsVarchar {
		logutil.Infof("DEBUG isVarcharLengthModified: Not both varchar types, returning false")
		return false // Not a varchar type modification
	}

	// Check if only the width (length) is different
	// All other properties should remain the same, including column name
	typeIdMatch := oldCol.Typ.Id == newColType.Id
	scaleMatch := oldCol.Typ.Scale == newColType.Scale
	autoIncrMatch := oldCol.Typ.AutoIncr == newColType.AutoIncr
	widthIncrease := oldCol.Typ.Width < newColType.Width
	nameMatch := strings.EqualFold(oldCol.Name, colName)

	logutil.Infof("DEBUG isVarcharLengthModified: Type ID match: %t (%d == %d)", typeIdMatch, oldCol.Typ.Id, newColType.Id)
	logutil.Infof("DEBUG isVarcharLengthModified: Scale match: %t (%d == %d)", scaleMatch, oldCol.Typ.Scale, newColType.Scale)
	logutil.Infof("DEBUG isVarcharLengthModified: AutoIncr match: %t (%t == %t)", autoIncrMatch, oldCol.Typ.AutoIncr, newColType.AutoIncr)
	logutil.Infof("DEBUG isVarcharLengthModified: Width increase: %t (%d <= %d)", widthIncrease, oldCol.Typ.Width, newColType.Width)
	logutil.Infof("DEBUG isVarcharLengthModified: Name match: %t (%s == %s)", nameMatch, oldCol.Name, colName)

	if typeIdMatch && scaleMatch && autoIncrMatch && widthIncrease && nameMatch {
		// Check if any attributes are specified in the new column definition
		if len(specNewColumn.Attributes) > 0 {
			logutil.Infof("DEBUG isVarcharLengthModified: Found %d attributes in new column definition", len(specNewColumn.Attributes))
			// Check each attribute - only be lenient with NULL attributes
			for i, attr := range specNewColumn.Attributes {
				logutil.Infof("DEBUG isVarcharLengthModified: Checking attribute %d: %T", i, attr)
				switch a := attr.(type) {
				case *tree.AttributeNull:
					// Strict check for NOT NULL/NULL constraint modifications
					oldIsNotNull := oldCol.Default != nil && !oldCol.Default.NullAbility
					newIsNotNull := !a.Is

					logutil.Infof("DEBUG isVarcharLengthModified: AttributeNull - old NotNull: %t, new NotNull: %t (a.Is: %t)",
						oldIsNotNull, newIsNotNull, a.Is)

					// Restore the strict check for NULL attributes
					if oldIsNotNull != newIsNotNull {
						logutil.Infof("DEBUG isVarcharLengthModified: NOT NULL/NULL constraint modification detected, returning false")
						// NOT NULL/NULL constraint modification is not allowed
						return false
					}
					// If constraints are consistent, this is acceptable for varchar length modification
					logutil.Infof("DEBUG isVarcharLengthModified: NOT NULL constraint is consistent, continuing")
				case *tree.AttributeDefault:
					logutil.Infof("DEBUG isVarcharLengthModified: AttributeDefault detected, returning false")
					// Default value modification is not allowed
					return false
				case *tree.AttributeAutoIncrement:
					logutil.Infof("DEBUG isVarcharLengthModified: AttributeAutoIncrement detected, returning false")
					// Auto increment modification is not allowed
					return false
				case *tree.AttributeComment:
					logutil.Infof("DEBUG isVarcharLengthModified: AttributeComment detected, returning false")
					// Comment modification is not allowed
					return false
				case *tree.AttributePrimaryKey, *tree.AttributeUniqueKey, *tree.AttributeUnique, *tree.AttributeKey:
					logutil.Infof("DEBUG isVarcharLengthModified: Key constraint attribute detected, returning false")
					// Key constraint modifications are not allowed
					return false
				case *tree.AttributeCollate:
					logutil.Infof("DEBUG isVarcharLengthModified: AttributeCollate detected, returning false")
					// Collation modification is not allowed
					return false
				default:
					logutil.Infof("DEBUG isVarcharLengthModified: Unknown attribute type %T detected, returning false", attr)
					// Any other attribute modification is not allowed
					return false
				}
			}
		} else {
			logutil.Infof("DEBUG isVarcharLengthModified: No attributes specified in new column definition")
		}

		// This is a varchar length modification with only NULL constraint changes allowed
		logutil.Infof("DEBUG isVarcharLengthModified: All checks passed, returning true for varchar length modification")
		return true
	}

	logutil.Infof("DEBUG isVarcharLengthModified: Basic type/property checks failed, returning false")
	return false
}

func buildNotNullColumnVal(col *ColDef) string {
	var defaultValue string
	if col.Typ.Id == int32(types.T_int8) ||
		col.Typ.Id == int32(types.T_int16) ||
		col.Typ.Id == int32(types.T_int32) ||
		col.Typ.Id == int32(types.T_int64) ||
		col.Typ.Id == int32(types.T_uint8) ||
		col.Typ.Id == int32(types.T_uint16) ||
		col.Typ.Id == int32(types.T_uint32) ||
		col.Typ.Id == int32(types.T_uint64) ||
		col.Typ.Id == int32(types.T_float32) ||
		col.Typ.Id == int32(types.T_float64) ||
		col.Typ.Id == int32(types.T_decimal64) ||
		col.Typ.Id == int32(types.T_decimal128) ||
		col.Typ.Id == int32(types.T_decimal256) ||
		col.Typ.Id == int32(types.T_bool) ||
		col.Typ.Id == int32(types.T_bit) {
		defaultValue = "0"
	} else if col.Typ.Id == int32(types.T_varchar) ||
		col.Typ.Id == int32(types.T_char) ||
		col.Typ.Id == int32(types.T_text) ||
		col.Typ.Id == int32(types.T_datalink) ||
		col.Typ.Id == int32(types.T_binary) ||
		col.Typ.Id == int32(types.T_blob) {
		defaultValue = "''"
	} else if col.Typ.Id == int32(types.T_date) {
		defaultValue = "'0001-01-01'"
	} else if col.Typ.Id == int32(types.T_datetime) {
		defaultValue = "'0001-01-01 00:00:00'"
	} else if col.Typ.Id == int32(types.T_time) {
		defaultValue = "'00:00:00'"
	} else if col.Typ.Id == int32(types.T_timestamp) {
		defaultValue = "'0001-01-01 00:00:00'"
	} else if col.Typ.Id == int32(types.T_json) {
		//defaultValue = "null"
		defaultValue = "'{}'"
	} else if col.Typ.Id == int32(types.T_enum) {
		enumvalues := strings.Split(col.Typ.Enumvalues, ",")
		defaultValue = enumvalues[0]
	} else if col.Typ.Id == int32(types.T_array_float32) || col.Typ.Id == int32(types.T_array_float64) {
		if col.Typ.Width > 0 {
			zerosWithCommas := strings.Repeat("0,", int(col.Typ.Width)-1)
			arrayAsString := zerosWithCommas + "0" // final zero
			defaultValue = fmt.Sprintf("'[%s]'", arrayAsString)
		} else {
			defaultValue = "'[]'"
		}
	} else {
		defaultValue = "null"
	}
	return defaultValue
}
