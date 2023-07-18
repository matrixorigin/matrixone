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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"strings"
)

func buildAlterTable3(stmt *tree.AlterTable, ctx CompilerContext) (*Plan, error) {
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
	//-------------------------------------------------------------------------------------------------------->>>>>

	// 2. split alter_option list
	copyTableDef := buildCopyTableDef(tableDef)
	alterTableCtx := initAlterTableContext(tableDef, copyTableDef, schemaName)

	validAlterSpecs, err := ResolveAlterTableSpec(ctx.GetContext(), stmt.Options)
	if err != nil {
		return nil, err
	}

	// 3. check alter_option list
	// set name for anonymous foreign key.
	tmpForeignKeyId := 0
	for _, spec := range validAlterSpecs {
		if alterOpt, ok := spec.(*tree.AlterOptionAdd); ok {
			if foreignKey, ok2 := alterOpt.Def.(*tree.ForeignKey); ok2 && foreignKey.Name == "" {
				foreignKey.Name = fmt.Sprintf("fk_%d", tmpForeignKeyId)
			}
		}
	}

	// 4. traverse and handle alter options
	alterTablePlan := &plan.AlterTable{
		Database:        schemaName,
		TableDef:        tableDef,
		ReplicaTableDef: copyTableDef,
		IsClusterTable:  isClusterTable,
		Actions:         make([]*plan.AlterTable_Action, len(validAlterSpecs)),
		AlgorithmType:   plan.AlterTable_COPY,
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
		case *tree.TableOptionAutoIncrement:
			// TODO
		default:
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "Do not support this stmt now.")
		}
	}

	createDdl, err := restoreDDL(ctx, alterTablePlan.ReplicaTableDef, schemaName)
	if err != nil {
		return nil, err
	}
	alterTablePlan.NewDdlSql = createDdl
	fmt.Printf("--------------------->restore alter table ddl:\n %s \n", createDdl)

	insertDml, err := buildAlterMoveDataSQL(ctx, alterTableCtx)
	alterTablePlan.RemoveDataSql = insertDml
	fmt.Printf("---------------------------------------------------------------")
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

func ResolveAlterTableSpec(ctx context.Context, specs []tree.AlterTableOption) ([]tree.AlterTableOption, error) {
	return specs, nil
}

// AddColumn will add a new column to the table.
func AddColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterAddCol) error {
	newColumn := spec.Column
	if err := checkAddColumnTooManyColumns(ctx.GetContext(), len(alterPlan.ReplicaTableDef.Cols)+1); err != nil {
		return err
	}
	_, err := checkAndCreateNewColumn(ctx, spec, alterPlan, newColumn)
	if err != nil {
		return err
	}
	return nil
}

func checkAndCreateNewColumn(ctx CompilerContext, spec *tree.AlterAddCol, alterPlan *plan.AlterTable, specNewColumn *tree.ColumnTableDef) (*ColDef, error) {
	tableDef := alterPlan.ReplicaTableDef
	err := checkUnsupportedColumnConstraint(ctx.GetContext(), specNewColumn, tableDef.Name)
	if err != nil {
		return nil, err
	}

	// Check whether added column has existed.
	colName := specNewColumn.Name.Parts[0]
	col := FindColumn(tableDef.Cols, colName)
	if col != nil {
		return nil, moerr.NewErrDupFieldName(ctx.GetContext(), colName)
	}

	colType, err := getTypeFromAst(ctx.GetContext(), specNewColumn.Type)
	if err != nil {
		return nil, err
	}

	if err = checkAddColumnType(ctx.GetContext(), colType, specNewColumn.Name.Parts[0]); err != nil {
		return nil, err
	}

	return CreateNewColumn(ctx, alterPlan, spec, specNewColumn, colType)
}

// CreateNewColumn creates a new column according to the column information.
func CreateNewColumn(ctx CompilerContext, alterPlan *plan.AlterTable, spec *tree.AlterAddCol, specNewColumn *tree.ColumnTableDef, colType *plan.Type) (*ColDef, error) {
	isPrimaryKey := false
	hasNullFlag := false
	hasDefaultValue := false
	auto_incr := false
	var err error
	for _, attr := range specNewColumn.Attributes {
		switch attribute := attr.(type) {
		case *tree.AttributePrimaryKey, *tree.AttributeKey:
			pkeyColName := specNewColumn.Name.Parts[0]
			err = checkPrimaryKeyPartType(ctx.GetContext(), colType, specNewColumn.Name.Parts[0])
			if err != nil {
				return nil, err
			}
			// If the table already contains a primary key, an `ErrMultiplePriKey` error is reported
			if alterPlan.ReplicaTableDef.Pkey != nil && alterPlan.ReplicaTableDef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				return nil, moerr.NewErrMultiplePriKey(ctx.GetContext())
			}
			alterPlan.ReplicaTableDef.Pkey = &PrimaryKeyDef{
				Names:       []string{pkeyColName},
				PkeyColName: pkeyColName,
			}
			isPrimaryKey = true
			fmt.Printf("isPrimaryKey: %v \n", isPrimaryKey)
		case *tree.AttributeComment:
			comment := attribute.CMT.String()
			if getNumOfCharacters(comment) > maxLengthOfColumnComment {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "comment for column '%s' is too long", specNewColumn.Name.Parts[0])
			}
		case *tree.AttributeAutoIncrement:
			auto_incr = true
			if !types.T(colType.GetId()).IsInteger() {
				return nil, moerr.NewNotSupported(ctx.GetContext(), "the auto_incr column is only support integer type now")
			}
		case *tree.AttributeUnique, *tree.AttributeUniqueKey:
			err = checkUniqueKeyPartType(ctx.GetContext(), colType, specNewColumn.Name.Parts[0])
			if err != nil {
				return nil, err
			}
			uniqueIndex := &tree.UniqueIndex{
				KeyParts: []*tree.KeyPart{
					{
						ColName: specNewColumn.Name,
					},
				},
				Name: specNewColumn.Name.Parts[0],
			}
			indexDef, err2 := checkAddColumWithUniqueKey(ctx.GetContext(), alterPlan.ReplicaTableDef, uniqueIndex)
			if err2 != nil {
				return nil, err2
			}
			alterPlan.ReplicaTableDef.Indexes = append(alterPlan.ReplicaTableDef.Indexes, indexDef)
		case *tree.AttributeDefault:
			_, err = buildDefaultExpr(specNewColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			hasDefaultValue = true
		case *tree.AttributeNull:
			hasNullFlag = true
			fmt.Printf("hasNullFlag: %v \n", hasNullFlag)
		default:
			return nil, moerr.NewNotSupported(ctx.GetContext(), "unsupport column definition %v", attribute)
		}
	}

	if auto_incr && hasDefaultValue {
		return nil, moerr.NewErrInvalidDefault(ctx.GetContext(), specNewColumn.Name.Parts[0])
	}

	colDef, err := buildColumnAndConstraint(ctx, 0, specNewColumn, colType)
	if err != nil {
		return nil, err
	}
	alterPlan.ReplicaTableDef.Cols = append(alterPlan.ReplicaTableDef.Cols, colDef)

	return colDef, nil
}

func buildColumnAndConstraint(ctx CompilerContext, offset int, newColumn *tree.ColumnTableDef, colType *plan.Type) (*ColDef, error) {
	colName := newColumn.Name.Parts[0]
	if _, ok := catalog.InternalColumns[colName]; ok {
		return nil, moerr.NewErrWrongColumnName(ctx.GetContext(), colName)
	}

	colDef, err := columnDefToCol(ctx, offset, newColumn, colType)
	if err != nil {
		return nil, err
	}
	return colDef, nil
}

func columnDefToCol(ctx CompilerContext, offset int, newColumn *tree.ColumnTableDef, colType *plan.Type) (*ColDef, error) {
	colDef := &ColDef{
		Name: newColumn.Name.Parts[0],
		Alg:  plan.CompressType_Lz4,
		Typ:  colType,
	}

	//isPrimaryKey := false
	hasNullFlag := false
	//auto_incr := false
	var defaultValue *plan.Default
	var onUpdateExpr *plan.OnUpdate
	var err error
	for _, attr := range newColumn.Attributes {
		switch attribute := attr.(type) {
		case *tree.AttributePrimaryKey, *tree.AttributeKey:
			colDef.Primary = true
			//isPrimaryKey = true
		case *tree.AttributeComment:
			colDef.Comment = attribute.CMT.String()
		case *tree.AttributeAutoIncrement:
			//auto_incr = true
			colDef.Typ.AutoIncr = true
		case *tree.AttributeDefault, *tree.AttributeNull:
			defaultValue, err = buildDefaultExpr(newColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			colDef.Default = defaultValue
			hasNullFlag = true
			fmt.Printf("hasNullFlag: %v\n", hasNullFlag)
		case *tree.AttributeOnUpdate:
			onUpdateExpr, err = buildOnUpdate(newColumn, colType, ctx.GetProcess())
			if err != nil {
				return nil, err
			}
			colDef.OnUpdate = onUpdateExpr
		}
	}

	if defaultValue == nil {
		defaultValue, err = buildDefaultExpr(newColumn, colType, ctx.GetProcess())
		if err != nil {
			return nil, err
		}
		colDef.Default = defaultValue
	}

	return colDef, nil
}

func checkAddColumnTooManyColumns(ctx context.Context, colNum int) error {
	if uint32(colNum) > TableColumnCountLimit {
		return moerr.NewErrTooManyFields(ctx)
	}
	return nil
}

// checkAddColumnType check type for add single column.
func checkAddColumnType(ctx context.Context, colType *plan.Type, columnName string) error {
	if colType.Id == int32(types.T_char) || colType.Id == int32(types.T_varchar) ||
		colType.Id == int32(types.T_binary) || colType.Id == int32(types.T_varbinary) {
		if colType.GetWidth() > types.MaxStringSize {
			return moerr.NewInvalidInput(ctx, "string width (%d) is too long", colType.GetWidth())
		}
	}
	return nil
}

func checkUnsupportedColumnConstraint(ctx context.Context, column *tree.ColumnTableDef, tableName string) error {
	for _, constraint := range column.Attributes {
		switch constraint.(type) {
		case *tree.AttributeAutoIncrement:
			return moerr.NewNotSupported(ctx, "unsupported add column '%s' constraint AUTO_INCREMENT when alter table '%s'", column.Name.Parts[0], tableName)
		case *tree.AttributeKey:
			return moerr.NewNotSupported(ctx, "unsupported add column '%s' index when alter table '%s'", column.Name.Parts[0], tableName)
		case *tree.AttributeUniqueKey, *tree.AttributeUnique:
			return moerr.NewNotSupported(ctx, "unsupported add column '%s' constraint UNIQUE KEY when alter table '%s'", column.Name.Parts[0], tableName)
		case *tree.AttributePrimaryKey:
			return moerr.NewNotSupported(ctx, "unsupported add column '%s' constraint PRIMARY KEY when alter table '%s'", column.Name.Parts[0], tableName)
		}
	}
	return nil
}

func checkPrimaryKeyPartType(ctx context.Context, colType *plan.Type, columnName string) error {
	if colType.GetId() == int32(types.T_blob) {
		return moerr.NewNotSupported(ctx, "blob type in primary key")
	}
	if colType.GetId() == int32(types.T_text) {
		return moerr.NewNotSupported(ctx, "text type in primary key")
	}
	if colType.GetId() == int32(types.T_json) {
		return moerr.NewNotSupported(ctx, fmt.Sprintf("JSON column '%s' cannot be in primary key", columnName))
	}
	return nil
}

func checkUniqueKeyPartType(ctx context.Context, colType *plan.Type, columnName string) error {
	if colType.GetId() == int32(types.T_blob) {
		return moerr.NewNotSupported(ctx, "blob type in primary key")
	}
	if colType.GetId() == int32(types.T_text) {
		return moerr.NewNotSupported(ctx, "text type in primary key")
	}
	if colType.GetId() == int32(types.T_json) {
		return moerr.NewNotSupported(ctx, fmt.Sprintf("JSON column '%s' cannot be in primary key", columnName))
	}
	return nil
}

func checkAddColumWithUniqueKey(ctx context.Context, tableDef *TableDef, uniKey *tree.UniqueIndex) (*plan.IndexDef, error) {
	indexName := uniKey.GetIndexName()
	if strings.EqualFold(indexName, PrimaryKeyName) {
		return nil, moerr.NewErrWrongNameForIndex(ctx, uniKey.GetIndexName())
	}

	indexTableName, err := util.BuildIndexTableName(ctx, true)
	if err != nil {
		return nil, err
	}

	indexParts := make([]string, 0)
	for _, keyPart := range uniKey.KeyParts {
		name := keyPart.ColName.Parts[0]
		indexParts = append(indexParts, name)
	}
	if len(indexParts) > MaxKeyParts {
		return nil, moerr.NewErrTooManyKeyParts(ctx, MaxKeyParts)
	}

	indexDef := &plan.IndexDef{
		IndexName:      indexName,
		Unique:         true,
		Parts:          indexParts,
		IndexTableName: indexTableName,
		TableExist:     true,
		Comment:        "",
	}

	if uniKey.IndexOption != nil {
		indexDef.Comment = uniKey.IndexOption.Comment
	}
	return indexDef, nil
}

func restoreDDL(ctx CompilerContext, tableDef *TableDef, schemaName string) (string, error) {
	tblName := tableDef.Name
	var createStr string
	if tableDef.TableType == catalog.SystemOrdinaryRel {
		createStr = fmt.Sprintf("CREATE TABLE `%s`.`%s` (", formatStr(schemaName), formatStr(tblName+"_copy"))
	} else if tableDef.TableType == catalog.SystemExternalRel {
		createStr = fmt.Sprintf("CREATE EXTERNAL TABLE `%s`.`%s` (", formatStr(schemaName), formatStr(tblName+"_copy"))
	} else if tableDef.TableType == catalog.SystemClusterRel {
		createStr = fmt.Sprintf("CREATE CLUSTER TABLE `%s`.`%s` (", formatStr(schemaName), formatStr(tblName+"_copy"))
	} else if tblName == catalog.MO_DATABASE || tblName == catalog.MO_TABLES || tblName == catalog.MO_COLUMNS {
		createStr = fmt.Sprintf("CREATE TABLE `%s`.`%s` (", formatStr(schemaName), formatStr(tblName+"_copy"))
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
		createStr += fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`) ON DELETE %s ON UPDATE %s",
			formatStr(fk.Name), strings.Join(colNames, "`,`"), formatStr(fkTableDef.Name), strings.Join(fkColNames, "`,`"), fk.OnDelete.String(), fk.OnUpdate.String())
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

func buildAlterMoveDataSQL(ctx CompilerContext, alterCtx *AlterTableContext) (string, error) {
	schemaName := alterCtx.schemaName
	originTableName := alterCtx.originTableName
	copyTableName := alterCtx.copyTableName

	insertBuffer := bytes.NewBufferString("")
	selectBuffer := bytes.NewBufferString("")

	fmtCtx := tree.NewFmtCtx(dialect.MYSQL)
	isFirst := true
	for key, value := range alterCtx.alterColMap {
		fmtCtx.Reset()
		value.Format(fmtCtx)
		if isFirst {
			insertBuffer.WriteString(key)
			selectBuffer.WriteString(fmtCtx.String())
			isFirst = false
		} else {
			insertBuffer.WriteString(", " + key)
			selectBuffer.WriteString(", " + fmtCtx.String())
		}
	}

	insertSQL := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) SELECT %s FROM `%s`.`%s`",
		formatStr(schemaName), formatStr(copyTableName), insertBuffer.String(),
		formatStr(schemaName), formatStr(originTableName), selectBuffer.String())
	fmt.Printf("------wuxiliang-----alter table insert SQL----> [%s]\n", insertSQL)
	return insertSQL, nil
}

type AlterTableContext struct {
	// key   --> 副本表列名，
	// value --> 变换方式
	alterColMap     map[string]tree.Expr
	schemaName      string
	originTableName string
	copyTableName   string
}

func initAlterTableContext(originTableDef *TableDef, copyTableDef *TableDef, schemaName string) *AlterTableContext {
	alterTableMap := make(map[string]tree.Expr)
	for _, coldef := range originTableDef.Cols {
		if coldef.Hidden {
			continue
		}
		alterTableMap[coldef.Name] = &tree.UnresolvedName{
			NumParts: 1,
			Star:     false,
			Parts:    [4]string{coldef.Name, "", "", ""},
		}
	}
	return &AlterTableContext{
		alterColMap:     alterTableMap,
		schemaName:      schemaName,
		originTableName: originTableDef.Name,
		copyTableName:   copyTableDef.Name,
	}
}

func buildCopyTableDef(tableDef *TableDef) *TableDef {
	replicaTableDef := DeepCopyTableDef(tableDef)
	replicaTableDef.Name = replicaTableDef.Name + "_copy"
	return replicaTableDef
}
