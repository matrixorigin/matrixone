// Copyright 2024 Matrix Origin
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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// ConstructCreateTableSQL used to build CREATE Table statement
func ConstructCreateTableSQL(ctx CompilerContext, tableDef *plan.TableDef, snapshot *Snapshot, useDbName bool) (string, tree.Statement, error) {
	var err error
	var createStr string

	tblName := tableDef.Name
	schemaName := tableDef.DbName
	dbTblName := fmt.Sprintf("`%s`", formatStr(tblName))
	if useDbName {
		dbTblName = fmt.Sprintf("`%s`.`%s`", formatStr(schemaName), formatStr(tblName))
	}

	if tableDef.TableType == catalog.SystemOrdinaryRel {
		createStr = fmt.Sprintf("CREATE TABLE %s (", dbTblName)
	} else if tableDef.TableType == catalog.SystemExternalRel {
		createStr = fmt.Sprintf("CREATE EXTERNAL TABLE %s (", dbTblName)
	} else if tableDef.TableType == catalog.SystemClusterRel {
		createStr = fmt.Sprintf("CREATE CLUSTER TABLE %s (", dbTblName)
	} else if tblName == catalog.MO_DATABASE || tblName == catalog.MO_TABLES || tblName == catalog.MO_COLUMNS {
		createStr = fmt.Sprintf("CREATE TABLE %s (", dbTblName)
	}

	rowCount := 0
	var pkDefs []string
	isClusterTable := util.TableIsClusterTable(tableDef.TableType)

	// col.Name -> col.OriginName
	colNameToOriginName := make(map[string]string)
	colIdToOriginName := make(map[uint64]string)
	for _, col := range tableDef.Cols {
		if col.Hidden {
			continue
		}
		colNameOrigin := col.GetOriginCaseName()
		colNameToOriginName[col.Name] = colNameOrigin
		colIdToOriginName[col.ColId] = colNameOrigin
		if colNameOrigin == catalog.Row_ID {
			continue
		}
		//the non-sys account skips the column account_id of the cluster table
		accountId, err := ctx.GetAccountId()
		if err != nil {
			return "", nil, err
		}

		if util.IsClusterTableAttribute(colNameOrigin) && isClusterTable &&
			(accountId != catalog.System_Account || IsSnapshotValid(snapshot)) {
			continue
		}

		//-------------------------------------------------------------------------------------------------------------
		buf := bytes.NewBuffer(make([]byte, 0, 64))

		if rowCount == 0 {
			buf.WriteString("\n")
		} else {
			buf.WriteString(",\n")
		}

		typeStr := FormatColType(col.Typ)
		fmt.Fprintf(buf, "  `%s` %s", formatStr(colNameOrigin), typeStr)

		//-------------------------------------------------------------------------------------------------------------
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
			pkDefs = append(pkDefs, col.Name)
		}
	}

	// If it is a composite primary key, get the component columns of the composite primary key
	if tableDef.Pkey != nil && len(tableDef.Pkey.Names) > 1 {
		pkDefs = append(pkDefs, tableDef.Pkey.Names...)
	}

	if len(pkDefs) != 0 {
		pkStr := "  PRIMARY KEY ("
		for i, def := range pkDefs {
			def = colNameToOriginName[def]
			if i == len(pkDefs)-1 {
				pkStr += fmt.Sprintf("`%s`)", formatStr(def))
			} else {
				pkStr += fmt.Sprintf("`%s`,", formatStr(def))
			}
		}
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

				part = colNameToOriginName[part]
				indexStr += fmt.Sprintf("`%s`", formatStr(part))
				i++
			}

			indexStr += ")"
			if indexdef.IndexAlgoParams != "" {
				var paramList string
				paramList, err = catalog.IndexParamsToStringList(indexdef.IndexAlgoParams)
				if err != nil {
					return "", nil, err
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

	dedupFkName := make(UnorderedSet[string])
	for _, fk := range tableDef.Fkeys {
		if len(fk.Name) != 0 {
			if dedupFkName.Find(fk.Name) {
				continue
			}
			dedupFkName.Insert(fk.Name)
		}

		colOriginNames := make([]string, len(fk.Cols))
		for i, colId := range fk.Cols {
			colOriginNames[i] = colIdToOriginName[colId]
		}

		var fkTableDef *TableDef
		//fk self reference
		if fk.ForeignTbl == 0 {
			fkTableDef = tableDef
		} else {
			if ctx.GetQueryingSubscription() != nil {
				_, fkTableDef = ctx.ResolveSubscriptionTableById(fk.ForeignTbl, ctx.GetQueryingSubscription())
			} else {
				_, fkTableDef = ctx.ResolveById(fk.ForeignTbl, snapshot)
			}
		}

		// fkTable may not exist in snapshot restoration
		if fkTableDef == nil {
			return "", nil, moerr.NewInternalErrorNoCtx("can't find fkTable from fk %s.(%s) {%s}", tableDef.Name, strings.Join(colOriginNames, ","), snapshot.String())
		}

		fkColIdToOriginName := make(map[uint64]string)
		for _, col := range fkTableDef.Cols {
			fkColIdToOriginName[col.ColId] = col.GetOriginCaseName()
		}
		fkColOriginNames := make([]string, len(fk.ForeignCols))
		for i, colId := range fk.ForeignCols {
			fkColOriginNames[i] = fkColIdToOriginName[colId]
		}

		if rowCount != 0 {
			createStr += ",\n"
		}

		fkRefDbTblName := fmt.Sprintf("`%s`", formatStr(fkTableDef.Name))
		if tableDef.DbName != fkTableDef.DbName {
			fkRefDbTblName = fmt.Sprintf("`%s`.`%s`", formatStr(fkTableDef.DbName), formatStr(fkTableDef.Name))
		}
		createStr += fmt.Sprintf("  CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES %s (`%s`) ON DELETE %s ON UPDATE %s",
			formatStr(fk.Name), strings.Join(colOriginNames, "`,`"), fkRefDbTblName, strings.Join(fkColOriginNames, "`,`"), fk.OnDelete.String(), fk.OnUpdate.String())
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
		param := &tree.ExternParam{}
		if err = json.Unmarshal([]byte(tableDef.Createsql), param); err != nil {
			return "", nil, err
		}
		if param.ScanType == tree.S3 {
			if err = InitS3Param(param); err != nil {
				return "", nil, err
			}
		} else {
			if err = InitInfileParam(param); err != nil {
				return "", nil, err
			}
		}
		// hide file path
		createStr += fmt.Sprintf(" INFILE{'FILEPATH'='','COMPRESSION'='%s','FORMAT'='%s','JSONDATA'='%s'}", param.CompressType, param.Format, param.JsonData)

		fields := ""
		if param.Tail != nil && param.Tail.Fields != nil {
			if param.Tail.Fields.Terminated != nil {
				if param.Tail.Fields.Terminated.Value == "" {
					fields += " TERMINATED BY \"\""
				} else {
					fields += fmt.Sprintf(" TERMINATED BY '%s'", param.Tail.Fields.Terminated.Value)
				}
			}

			escape := func(value byte) string {
				if value == byte(0) {
					return ""
				} else if value == byte('\\') {
					return "\\\\"
				}
				return fmt.Sprintf("%c", value)
			}
			if param.Tail.Fields.EnclosedBy != nil {
				fields += " ENCLOSED BY '" + escape(param.Tail.Fields.EnclosedBy.Value) + "'"
			}
			if param.Tail.Fields.EscapedBy != nil {
				fields += " ESCAPED BY '" + escape(param.Tail.Fields.EscapedBy.Value) + "'"
			}
		}

		line := ""
		if param.Tail != nil && param.Tail.Lines != nil {
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
		}

		if len(fields) > 0 {
			fields = " FIELDS" + fields
			createStr += fields
		}
		if len(line) > 0 {
			line = " LINES" + line
			createStr += line
		}

		if param.Tail != nil && param.Tail.IgnoredLines > 0 {
			createStr += fmt.Sprintf(" IGNORE %d LINES", param.Tail.IgnoredLines)
		}
	}

	stmt, err := getRewriteSQLStmt(ctx, createStr)
	return createStr, stmt, err
}

// FormatColType Get the formatted description of the column type.
func FormatColType(colType plan.Type) string {
	typ := types.T(colType.Id).ToType()

	ts := typ.String()
	// after decimal fix, remove this
	if typ.Oid.IsDecimal() {
		ts = "DECIMAL"
	}

	suffix := ""
	switch types.T(colType.Id) {
	case types.T_enum: //types.T_set:
		elements := strings.Split(colType.GetEnumvalues(), ",")
		// format enum as ENUM ('e1', 'e2')
		elems := make([]string, 0, len(elements))
		for _, e := range elements {
			e = EscapeFormat(e)
			elems = append(elems, e)
		}
		suffix = fmt.Sprintf("('%s')", strings.Join(elems, "','"))

	case types.T_timestamp, types.T_datetime, types.T_time:
		if colType.Width > 0 {
			suffix = fmt.Sprintf("(%d)", colType.Width)
		}

	case types.T_float64, types.T_float32:
		if colType.Width > 0 && colType.Scale != -1 {
			suffix = fmt.Sprintf("(%d,%d)", colType.Width, colType.Scale)
		}

	case types.T_decimal64, types.T_decimal128, types.T_decimal256:
		suffix = fmt.Sprintf("(%d,%d)", colType.Width, colType.Scale)

	case types.T_bit, types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		suffix = fmt.Sprintf("(%d)", colType.Width)

	case types.T_array_float32, types.T_array_float64:
		suffix = fmt.Sprintf("(%d)", colType.Width)

	}
	return ts + suffix
}

// Character replace mapping maps certain special characters to their escape sequences.
var replaceMap = map[rune]string{
	'\000': "\\0",
	'\'':   "''",
	'\n':   "\\n",
	'\r':   "\\r",
}

// EscapeFormat output escape character with backslash.
func EscapeFormat(s string) string {
	var buf bytes.Buffer
	for _, old := range s {
		if newVal, ok := replaceMap[old]; ok {
			buf.WriteString(newVal)
			continue
		}
		buf.WriteRune(old)
	}
	return buf.String()
}

func formatStr(str string) string {
	tmp := strings.Replace(str, "`", "``", -1)
	strLen := len(tmp)
	if strLen < 2 {
		return tmp
	}
	if tmp[0] == '\'' && tmp[strLen-1] == '\'' {
		return "'" + strings.Replace(tmp[1:strLen-1], "'", "''", -1) + "'"
	}
	return strings.Replace(tmp, "'", "''", -1)
}

func getTimeStampByTsHint(ctx CompilerContext, AtTsExpr *tree.AtTimeStamp) (snapshot *plan.Snapshot, err error) {
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	return builder.resolveTsHint(AtTsExpr)
}
