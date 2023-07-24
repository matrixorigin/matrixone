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
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"strings"
)

func buildAlterTableCopy(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
	// ALTER TABLE tbl_name
	//		[alter_option [, alter_option] ...]
	//		[partition_options]

	// 1. get origin table name and Schema name
	schemaName, tableName := string(stmt.Table.Schema()), string(stmt.Table.Name())
	if schemaName == "" {
		schemaName = ctx.DefaultDatabase()
	}
	objRef, tableDef := ctx.Resolve(schemaName, tableName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
	}
	//-----------------------------------------<<<<<< Abstract a function ------------------------------------------
	if tableDef.ViewSql != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "you should use alter view statemnt for View")
	}
	if objRef.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter table in subscription database")
	}
	isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
	if isClusterTable && ctx.GetAccountId() != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can alter the cluster table")
	}
	if tableDef.Partition != nil {
		return nil, moerr.NewNotSupported(ctx.GetContext(), "Currently, partition table does not support alter table")
	}
	//-------------------------------------------------------------------------------------------------------->>>>>
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
		Actions:        make([]*plan.AlterTable_Action, len(validAlterSpecs)),
		AlgorithmType:  plan.AlterTable_COPY,
	}

	for _, spec := range validAlterSpecs {
		switch option := spec.(type) {
		case *tree.AlterOptionAdd:
			switch optionAdd := option.Def.(type) {
			case *tree.PrimaryKeyIndex:
				// TODO
			case *tree.ForeignKey:
				// TODO
			case *tree.UniqueIndex:
				// TODO
			case *tree.Index:
				// TODO
			case *tree.ColumnTableDef:
				// TODO
			default:
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "Do not support this stmt now. %v", optionAdd)
			}
		case *tree.AlterOptionDrop:
			// TODO
		case *tree.AlterOptionAlterIndex:
			// TODO
		case *tree.TableOptionComment:
			// TODO
		case *tree.AlterTableName:
			// TODO
		case *tree.AlterAddCol:
			// TODO
			err = AddColumn(ctx, alterTablePlan, option)
		case *tree.AlterTableModifyColumnClause:
			err = ModifyColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.AlterTableChangeColumnClause:
			err = ChangeColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.AlterTableRenameColumnClause:
			err = RenameColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.AlterTableAlterColumnClause:
			err = AlterColumn(ctx, alterTablePlan, option, alterTableCtx)
		case *tree.TableOptionAutoIncrement:
			// TODO
		default:
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "Do not support this stmt now.")
		}
		if err != nil {
			return nil, err
		}
	}

	createDdl, err := restoreDDL(ctx, alterTablePlan.CopyTableDef, schemaName)
	if err != nil {
		return nil, err
	}
	alterTablePlan.CreateTableSql = createDdl

	insertDml, err := buildAlterInsertDataSQL(ctx, alterTableCtx)
	if err != nil {
		return nil, err
	}
	alterTablePlan.InsertDataSql = insertDml

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

func restoreDDL(ctx CompilerContext, tableDef *TableDef, schemaName string) (string, error) {
	tblName := tableDef.Name
	var createStr string
	if tableDef.TableType == catalog.SystemOrdinaryRel {
		createStr = fmt.Sprintf("CREATE TABLE `%s`.`%s` (", formatStr(schemaName), formatStr(tblName))
	} else if tableDef.TableType == catalog.SystemExternalRel {
		createStr = fmt.Sprintf("CREATE EXTERNAL TABLE `%s`.`%s` (", formatStr(schemaName), formatStr(tblName))
	} else if tableDef.TableType == catalog.SystemClusterRel {
		createStr = fmt.Sprintf("CREATE CLUSTER TABLE `%s`.`%s` (", formatStr(schemaName), formatStr(tblName))
	} else if tblName == catalog.MO_DATABASE || tblName == catalog.MO_TABLES || tblName == catalog.MO_COLUMNS {
		createStr = fmt.Sprintf("CREATE TABLE `%s`.`%s` (", formatStr(schemaName), formatStr(tblName))
	}

	rowCount := 0
	var pkDefs []string
	isClusterTable := util.TableIsClusterTable(tableDef.TableType)

	colIdToName := make(map[uint64]string)
	for _, col := range tableDef.Cols {
		if col.Hidden {
			continue
		}
		colName := col.Name
		colIdToName[col.ColId] = col.Name
		if colName == catalog.Row_ID {
			continue
		}
		//the non-sys account skips the column account_id of the cluster table
		if util.IsClusterTableAttribute(colName) &&
			isClusterTable &&
			ctx.GetAccountId() != catalog.System_Account {
			continue
		}
		nullOrNot := "NOT NULL"
		// col.Default must be not nil
		if len(col.Default.OriginString) > 0 {
			nullOrNot = "DEFAULT " + formatStr(col.Default.OriginString)
		} else if col.Default.NullAbility {
			nullOrNot = "DEFAULT NULL"
		}

		if col.Typ.AutoIncr {
			nullOrNot = "NOT NULL AUTO_INCREMENT"
		}

		var hasAttrComment string
		if col.Comment != "" {
			hasAttrComment = " COMMENT '" + col.Comment + "'"
		}

		if rowCount == 0 {
			createStr += "\n"
		} else {
			createStr += ",\n"
		}
		typ := types.T(col.Typ.Id).ToType()
		typeStr := typ.String()
		if typ.Oid.IsDecimal() { //after decimal fix,remove this
			typeStr = fmt.Sprintf("DECIMAL(%d,%d)", col.Typ.Width, col.Typ.Scale)
		}
		if typ.Oid == types.T_varchar || typ.Oid == types.T_char ||
			typ.Oid == types.T_binary || typ.Oid == types.T_varbinary {
			typeStr += fmt.Sprintf("(%d)", col.Typ.Width)
		}
		if typ.Oid.IsFloat() && col.Typ.Scale != -1 {
			typeStr += fmt.Sprintf("(%d,%d)", col.Typ.Width, col.Typ.Scale)
		}

		updateOpt := ""
		if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
			updateOpt = " ON UPDATE " + col.OnUpdate.OriginString
		}
		createStr += fmt.Sprintf("`%s` %s %s%s%s", formatStr(colName), typeStr, nullOrNot, updateOpt, hasAttrComment)
		rowCount++
		if col.Primary {
			pkDefs = append(pkDefs, colName)
		}
	}

	// If it is a composite primary key, get the component columns of the composite primary key
	if tableDef.Pkey != nil && len(tableDef.Pkey.Names) > 1 {
		pkDefs = append(pkDefs, tableDef.Pkey.Names...)
	}

	if len(pkDefs) != 0 {
		pkStr := "PRIMARY KEY ("
		for i, def := range pkDefs {
			if i == len(pkDefs)-1 {
				pkStr += fmt.Sprintf("`%s`", formatStr(def))
			} else {
				pkStr += fmt.Sprintf("`%s`,", formatStr(def))
			}
		}
		pkStr += ")"
		if rowCount != 0 {
			createStr += ",\n"
		}
		createStr += pkStr
	}

	if tableDef.Indexes != nil {
		for _, indexdef := range tableDef.Indexes {
			var indexStr string
			if indexdef.Unique {
				indexStr = "UNIQUE KEY "
			} else {
				indexStr = "KEY "
			}
			indexStr += fmt.Sprintf("`%s` (", formatStr(indexdef.IndexName))
			for num, part := range indexdef.Parts {
				if num == len(indexdef.Parts)-1 {
					indexStr += fmt.Sprintf("`%s`", formatStr(part))
				} else {
					indexStr += fmt.Sprintf("`%s`,", formatStr(part))
				}
			}
			indexStr += ")"
			if indexdef.Comment != "" {
				indexdef.Comment = strings.Replace(indexdef.Comment, "'", "\\'", -1)
				indexStr += fmt.Sprintf(" COMMENT '%s'", formatStr(indexdef.Comment))
			}
			if rowCount != 0 {
				createStr += ",\n"
			}
			createStr += indexStr
		}
	}

	for _, fk := range tableDef.Fkeys {
		colNames := make([]string, len(fk.Cols))
		for i, colId := range fk.Cols {
			colNames[i] = colIdToName[colId]
		}
		_, fkTableDef := ctx.ResolveById(fk.ForeignTbl)
		fkColIdToName := make(map[uint64]string)
		for _, col := range fkTableDef.Cols {
			fkColIdToName[col.ColId] = col.Name
		}
		fkColNames := make([]string, len(fk.ForeignCols))
		for i, colId := range fk.ForeignCols {
			fkColNames[i] = fkColIdToName[colId]
		}

		if rowCount != 0 {
			createStr += ",\n"
		}

		if fk.Name == "" {
			createStr += fmt.Sprintf("CONSTRAINT FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`) ON DELETE %s ON UPDATE %s",
				strings.Join(colNames, "`,`"), formatStr(fkTableDef.Name), strings.Join(fkColNames, "`,`"), fk.OnDelete.String(), fk.OnUpdate.String())
		} else {
			createStr += fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`) ON DELETE %s ON UPDATE %s",
				formatStr(fk.Name), strings.Join(colNames, "`,`"), formatStr(fkTableDef.Name), strings.Join(fkColNames, "`,`"), fk.OnDelete.String(), fk.OnUpdate.String())
		}
	}

	if rowCount != 0 {
		createStr += "\n"
	}
	createStr += ")"

	if tableDef.ClusterBy != nil {
		clusterby := " CLUSTER BY ("
		if util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
			//multi column clusterby
			cbNames := util.SplitCompositeClusterByColumnName(tableDef.ClusterBy.Name)
			for i, cbName := range cbNames {
				if i != 0 {
					clusterby += fmt.Sprintf(", `%s`", formatStr(cbName))
				} else {
					clusterby += fmt.Sprintf("`%s`", formatStr(cbName))
				}
			}
		} else {
			//single column cluster by
			clusterby += fmt.Sprintf("`%s`", formatStr(tableDef.ClusterBy.Name))
		}
		clusterby += ")"
		createStr += clusterby
	}

	var comment string
	var partition string
	for _, def := range tableDef.Defs {
		if proDef, ok := def.Def.(*plan.TableDef_DefType_Properties); ok {
			for _, kv := range proDef.Properties.Properties {
				if kv.Key == catalog.SystemRelAttr_Comment {
					comment = " COMMENT='" + kv.Value + "'"
				}
			}
		}
	}

	if tableDef.Partition != nil {
		partition = ` ` + tableDef.Partition.PartitionMsg
	}

	createStr += comment
	createStr += partition

	if tableDef.TableType == catalog.SystemExternalRel {
		param := tree.ExternParam{}
		err := json.Unmarshal([]byte(tableDef.Createsql), &param)
		if err != nil {
			return "", err
		}
		createStr += fmt.Sprintf(" INFILE{'FILEPATH'='%s','COMPRESSION'='%s','FORMAT'='%s','JSONDATA'='%s'}", param.Filepath, param.CompressType, param.Format, param.JsonData)

		escapedby := ""
		if param.Tail.Fields.EscapedBy != byte(0) {
			escapedby = fmt.Sprintf(" ESCAPED BY '%c'", param.Tail.Fields.EscapedBy)
		}

		line := ""
		if param.Tail.Lines.StartingBy != "" {
			line = fmt.Sprintf(" LINE STARTING BY '%s'", param.Tail.Lines.StartingBy)
		}
		lineEnd := ""
		if param.Tail.Lines.TerminatedBy == "\n" || param.Tail.Lines.TerminatedBy == "\r\n" {
			lineEnd = " TERMINATED BY '\\\\n'"
		} else {
			lineEnd = fmt.Sprintf(" TERMINATED BY '%s'", param.Tail.Lines.TerminatedBy)
		}
		if len(line) > 0 {
			line += lineEnd
		} else {
			line = " LINES" + lineEnd
		}

		createStr += fmt.Sprintf(" FIELDS TERMINATED BY '%s' ENCLOSED BY '%c'%s", param.Tail.Fields.Terminated, rune(param.Tail.Fields.EnclosedBy), escapedby)
		createStr += line
		if param.Tail.IgnoredLines > 0 {
			createStr += fmt.Sprintf(" IGNORE %d LINES", param.Tail.IgnoredLines)
		}
	}

	var buf bytes.Buffer
	for _, ch := range createStr {
		if ch == '"' {
			buf.WriteRune('"')
		}
		buf.WriteRune(ch)
	}
	sql := buf.String()
	_, err := getRewriteSQLStmt(ctx, sql)
	if err != nil {
		return "", err
	}
	return sql, nil
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
			insertBuffer.WriteString(key)
			selectBuffer.WriteString(value)
			isFirst = false
		} else {
			insertBuffer.WriteString(", " + key)
			selectBuffer.WriteString(", " + value)
		}
	}

	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) SELECT %s FROM `%s`.`%s`",
		formatStr(schemaName), formatStr(copyTableName), insertBuffer.String(),
		selectBuffer.String(), formatStr(schemaName), formatStr(originTableName))
	return insertSQL, nil
}

type AlterTableContext struct {
	// key   --> Replica Table Column Names
	// value --> Original Table Column Name
	alterColMap     map[string]string
	schemaName      string
	originTableName string
	copyTableName   string
}

func initAlterTableContext(originTableDef *TableDef, copyTableDef *TableDef, schemaName string) *AlterTableContext {
	alterTableMap := make(map[string]string)
	for _, coldef := range originTableDef.Cols {
		if coldef.Hidden {
			continue
		}
		alterTableMap[coldef.Name] = coldef.Name
	}
	return &AlterTableContext{
		alterColMap:     alterTableMap,
		schemaName:      schemaName,
		originTableName: originTableDef.Name,
		copyTableName:   copyTableDef.Name,
	}
}

func buildCopyTableDef(ctx context.Context, tableDef *TableDef) (*TableDef, error) {
	replicaTableDef := DeepCopyTableDef(tableDef)

	id, err := uuid.NewUUID()
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
	objRef, tableDef := ctx.Resolve(schemaName, tableName)
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), schemaName, tableName)
	}

	if tableDef.ViewSql != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "you should use alter view statemnt for View")
	}
	if objRef.PubInfo != nil {
		return nil, moerr.NewInternalError(ctx.GetContext(), "cannot alter table in subscription database")
	}
	isClusterTable := util.TableIsClusterTable(tableDef.GetTableType())
	if isClusterTable && ctx.GetAccountId() != catalog.System_Account {
		return nil, moerr.NewInternalError(ctx.GetContext(), "only the sys account can alter the cluster table")
	}
	if tableDef.Partition != nil {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "can't add/drop column for partition table now")
	}

	algorithm := ResolveAlterTableAlgorithm(ctx.GetContext(), stmt.Options)
	if algorithm == plan.AlterTable_COPY {
		return buildAlterTableCopy(stmt, ctx)
	} else {
		return buildAlterTableInplace(stmt, ctx)
	}
}

func ResolveAlterTableAlgorithm(ctx context.Context, validAlterSpecs []tree.AlterTableOption) (algorithm plan.AlterTable_AlgorithmType) {
	algorithm = plan.AlterTable_COPY
	for _, spec := range validAlterSpecs {
		switch option := spec.(type) {
		case *tree.AlterOptionAdd:
			switch option.Def.(type) {
			case *tree.PrimaryKeyIndex:
				// TODO
				algorithm = plan.AlterTable_INPLACE
			case *tree.ForeignKey:
				// TODO
				algorithm = plan.AlterTable_INPLACE
			case *tree.UniqueIndex:
				// TODO
				algorithm = plan.AlterTable_INPLACE
			case *tree.Index:
				// TODO
				algorithm = plan.AlterTable_INPLACE
			case *tree.ColumnTableDef:
				// TODO
				algorithm = plan.AlterTable_INPLACE
			default:
				algorithm = plan.AlterTable_INPLACE
			}
		case *tree.AlterOptionDrop:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterOptionAlterIndex:
			algorithm = plan.AlterTable_INPLACE
		case *tree.TableOptionComment:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterTableName:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterAddCol:
			algorithm = plan.AlterTable_INPLACE
		case *tree.AlterTableModifyColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableChangeColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableRenameColumnClause:
			algorithm = plan.AlterTable_COPY
		case *tree.AlterTableAlterColumnClause:
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
