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
	"math"
	"strings"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func buildAlterTableCopy(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
	// 1. get origin table name and Schema name
	schemaName, tableName := string(stmt.Table.Schema()), string(stmt.Table.Name())
	if schemaName == "" {
		schemaName = ctx.DefaultDatabase()
	}

	_, tableDef := ctx.Resolve(schemaName, tableName, Snapshot{TS: &timestamp.Timestamp{}})
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

	createTmpDdl, err := restoreDDL(ctx, alterTablePlan.CopyTableDef, schemaName, alterTableCtx.copyTableName)
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

// restoreDDL Get the DDL statement for the corresponding table based on tableDef,
// skipConstraint: Skip foreign key and index constraints
func restoreDDL(ctx CompilerContext, tableDef *TableDef, schemaName string, tblName string) (string, error) {
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
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return "", err
		}
		if util.IsClusterTableAttribute(colName) &&
			isClusterTable &&
			accountId != catalog.System_Account {
			continue
		}

		buf := bytes.NewBuffer(make([]byte, 0, 64))

		if rowCount == 0 {
			buf.WriteString("\n")
		} else {
			buf.WriteString(",\n")
		}

		typeStr := FormatColType(col.Typ)
		fmt.Fprintf(buf, "  `%s` %s", formatStr(colName), typeStr)

		if col.Typ.AutoIncr {
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if !col.Default.NullAbility {
				buf.WriteString(" NOT NULL")
			}

			if strings.EqualFold(col.Default.OriginString, "null") ||
				len(col.Default.OriginString) == 0 {
				if col.Default.NullAbility {
					if col.Typ.Id == int32(types.T_timestamp) {
						buf.WriteString(" NULL")
					}
					buf.WriteString(" DEFAULT NULL")
				}
			} else if len(col.Default.OriginString) > 0 {
				buf.WriteString(" DEFAULT " + formatStr(col.Default.OriginString))
			}

			if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
				buf.WriteString(" ON UPDATE " + col.OnUpdate.OriginString)
			}
		}

		if col.Comment != "" {
			buf.WriteString(" COMMENT '" + col.Comment + "'")
		}

		createStr += buf.String()
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
		pkStr := "  PRIMARY KEY ("
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

		// We only print distinct index names. This is used to avoid printing the same index multiple times for IVFFLAT or
		// other multi-table indexes.
		indexNames := make(map[string]bool)

		for _, indexdef := range tableDef.Indexes {
			if _, ok := indexNames[indexdef.IndexName]; ok {
				continue
			} else {
				indexNames[indexdef.IndexName] = true
			}

			var indexStr string
			if indexdef.Unique {
				indexStr = "  UNIQUE KEY "
			} else {
				indexStr = "  KEY "
			}
			indexStr += fmt.Sprintf("`%s` ", formatStr(indexdef.IndexName))
			if !catalog.IsNullIndexAlgo(indexdef.IndexAlgo) {
				indexStr += fmt.Sprintf("USING %s ", indexdef.IndexAlgo)
			}
			indexStr += "("
			i := 0
			for _, part := range indexdef.Parts {
				if catalog.IsAlias(part) {
					continue
				}
				if i > 0 {
					indexStr += ","
				}

				indexStr += fmt.Sprintf("`%s`", formatStr(part))
				i++
			}

			indexStr += ")"
			if indexdef.IndexAlgoParams != "" {
				var paramList string
				paramList, err := catalog.IndexParamsToStringList(indexdef.IndexAlgoParams)
				if err != nil {
					return "", err
				}
				indexStr += paramList
			}
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

		var fkTableDef *TableDef

		//fk self reference
		if fk.ForeignTbl == 0 {
			fkTableDef = tableDef
		} else {
			if ctx.GetQueryingSubscription() != nil {
				_, fkTableDef = ctx.ResolveSubscriptionTableById(fk.ForeignTbl, ctx.GetQueryingSubscription())
			} else {
				_, fkTableDef = ctx.ResolveById(fk.ForeignTbl, Snapshot{TS: &timestamp.Timestamp{}})
			}
		}

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
		createStr += fmt.Sprintf("  CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`) ON DELETE %s ON UPDATE %s",
			formatStr(fk.Name), strings.Join(colNames, "`,`"), formatStr(fkTableDef.Name), strings.Join(fkColNames, "`,`"), fk.OnDelete.String(), fk.OnUpdate.String())
	}

	if rowCount != 0 {
		createStr += "\n"
	}
	createStr += ")"

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

	/**
	Fix issue: https://github.com/matrixorigin/MO-Cloud/issues/1028#issuecomment-1667642384
	Based on the grammar of the 'create table' in the file pkg/sql/parsers/dialect/mysql/mysql_sql.y
		https://github.com/matrixorigin/matrixone/blob/68db7260e411e5a4541eaccf78ca9bb57e810f24/pkg/sql/parsers/dialect/mysql/mysql_sql.y#L6076C7-L6076C7
		https://github.com/matrixorigin/matrixone/blob/68db7260e411e5a4541eaccf78ca9bb57e810f24/pkg/sql/parsers/dialect/mysql/mysql_sql.y#L6097
	The 'cluster by' is after the 'partition by' and the 'table options', so we need to add the 'cluster by' string after the 'partition by' and the 'table options'.
	*/
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

	if tableDef.TableType == catalog.SystemExternalRel {
		param := tree.ExternParam{}
		err := json.Unmarshal([]byte(tableDef.Createsql), &param)
		if err != nil {
			return "", err
		}
		createStr += fmt.Sprintf(" INFILE{'FILEPATH'='%s','COMPRESSION'='%s','FORMAT'='%s','JSONDATA'='%s'}", param.Filepath, param.CompressType, param.Format, param.JsonData)

		fields := ""
		if param.Tail.Fields.Terminated != nil {
			if param.Tail.Fields.Terminated.Value == "" {
				fields += " TERMINATED BY \"\""
			} else {
				fields += fmt.Sprintf(" TERMINATED BY '%s'", param.Tail.Fields.Terminated.Value)
			}
		}
		if param.Tail.Fields.EnclosedBy != nil {
			if param.Tail.Fields.EnclosedBy.Value == byte(0) {
				fields += " ENCLOSED BY ''"
			} else if param.Tail.Fields.EnclosedBy.Value == byte('\\') {
				fields += " ENCLOSED BY '\\\\'"
			} else {
				fields += fmt.Sprintf(" ENCLOSED BY '%c'", param.Tail.Fields.EnclosedBy.Value)
			}
		}
		if param.Tail.Fields.EscapedBy != nil {
			if param.Tail.Fields.EscapedBy.Value == byte(0) {
				fields += " ESCAPED BY ''"
			} else if param.Tail.Fields.EscapedBy.Value == byte('\\') {
				fields += " ESCAPED BY '\\\\'"
			} else {
				fields += fmt.Sprintf(" ESCAPED BY '%c'", param.Tail.Fields.EscapedBy.Value)
			}
		}

		line := ""
		if param.Tail.Lines.StartingBy != "" {
			line += fmt.Sprintf(" STARTING BY '%s'", param.Tail.Lines.StartingBy)
		}
		if param.Tail.Lines.TerminatedBy != nil {
			if param.Tail.Lines.TerminatedBy.Value == "\n" || param.Tail.Lines.TerminatedBy.Value == "\r\n" {
				line += " TERMINATED BY '\\\\n'"
			} else {
				line += fmt.Sprintf(" TERMINATED BY '%s'", param.Tail.Lines.TerminatedBy)
			}
		}

		if len(fields) > 0 {
			fields = " FIELDS" + fields
			createStr += fields
		}
		if len(line) > 0 {
			line = " LINES" + line
			createStr += line
		}

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
	stmt, err := getRewriteSQLStmt(ctx, sql)
	defer func() {
		if stmt != nil {
			stmt.Free()
		}
	}()

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
	// key   --> Copy table column name
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

		if !coldef.Hidden {
			changTblColIdMap[coldef.ColId] = &plan.ColDef{
				ColId: UnKnownColId,
				Name:  coldef.Name,
			}
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
	objRef, tableDef := ctx.Resolve(schemaName, tableName, Snapshot{TS: &timestamp.Timestamp{}})
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

	if tableDef.Partition != nil && stmt.Options != nil {
		return nil, moerr.NewInvalidInput(ctx.GetContext(), "can't add/drop column for partition table now")
	}

	if stmt.PartitionOption != nil {
		if stmt.Options != nil {
			return nil, moerr.NewParseError(ctx.GetContext(), "Unsupported multi schema change")
		}
		return buildAlterTableInplace(stmt, ctx)
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
