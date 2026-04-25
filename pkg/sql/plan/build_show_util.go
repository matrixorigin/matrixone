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

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// ConstructCreateTableSQL used to build CREATE Table statement
func ConstructCreateTableSQL(
	ctx CompilerContext,
	tableDef *plan.TableDef,
	snapshot *Snapshot,
	useDbName bool,
	cloneStmt *tree.CloneTable,
) (string, tree.Statement, error) {

	var err error
	var createStr string
	rewritePairs := make([]struct {
		display string
		rewrite string
	}, 0)
	checkDefs := extractTopLevelCheckDefs(tableDef)

	tblName := tableDef.Name
	schemaName := tableDef.DbName
	dbTblName := fmt.Sprintf("`%s`", formatStr(tblName))
	if useDbName {
		dbTblName = fmt.Sprintf("`%s`.`%s`", formatStr(schemaName), formatStr(tblName))
	}

	if tableDef.TableType == catalog.SystemExternalRel {
		createStr = fmt.Sprintf("CREATE EXTERNAL TABLE %s (", dbTblName)
	} else if tableDef.TableType == catalog.SystemClusterRel {
		createStr = fmt.Sprintf("CREATE CLUSTER TABLE %s (", dbTblName)
	} else if tableDef.IsTemporary {
		createStr = fmt.Sprintf("CREATE TEMPORARY TABLE %s (", dbTblName)
	} else {
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

		if util.IsClusterTableAttribute(colNameOrigin) && isClusterTable {
			accountId, err := ctx.GetAccountId()
			if err != nil {
				return "", nil, err
			}
			if accountId != catalog.System_Account || IsSnapshotValid(snapshot) || useDbName {
				// useDbName reuse in build alter table sql
				// if use in other place, need to check or add a new parameter
				continue
			}
		}

		//-------------------------------------------------------------------------------------------------------------
		buf := bytes.NewBuffer(make([]byte, 0, 64))

		if rowCount == 0 {
			buf.WriteString("\n")
		} else {
			buf.WriteString(",\n")
		}

		typeStr := FormatColType(col.Typ)
		if strings.HasPrefix(typeStr, "ENUM") {
			typeStr = strings.ToLower(typeStr[:4]) + typeStr[4:]
		} else {
			typeStr = strings.ToLower(typeStr)
		}
		fmt.Fprintf(buf, "  `%s` %s", formatStr(colNameOrigin), typeStr)

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
			// Index Name can be empty string when CREATE TABLE with index
			// avoid duplicate only work when index name is not empty
			if len(indexdef.IndexName) > 0 {
				if _, ok := indexNames[indexdef.IndexName]; ok {
					continue
				} else {
					indexNames[indexdef.IndexName] = true
				}
			}

			var indexStr string
			if !indexdef.Unique && catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo) {
				indexStr += " FULLTEXT "

				if len(indexdef.IndexName) > 0 {
					indexStr += fmt.Sprintf("`%s`", formatStr(indexdef.IndexName))
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
					val, err := sonic.Get([]byte(indexdef.IndexAlgoParams), "parser")
					// ignore err != nil --> value not found
					if err == nil {
						parser, err := val.StrictString()
						if err != nil {
							// value exists but not string type
							return "", nil, err
						}

						if len(parser) > 0 {
							indexStr += " WITH PARSER " + parser
						}
					}

					val, err = sonic.Get([]byte(indexdef.IndexAlgoParams), "async")
					// ignore err != nil --> value not found
					if err == nil {
						async, err := val.StrictString()
						if err != nil {
							// value exists but not string type
							return "", nil, err
						}

						if async == "true" {
							indexStr += " ASYNC"
						}
					}

				}

			} else {
				rewriteIndexStr := ""
				if catalog.IsRTreeIndexAlgo(indexdef.IndexAlgo) {
					indexStr = "  SPATIAL KEY "
					rewriteIndexStr = "  KEY "
				} else if indexdef.Unique {
					indexStr = "  UNIQUE KEY "
					rewriteIndexStr = "  UNIQUE KEY "
				} else {
					indexStr = "  KEY "
					rewriteIndexStr = "  KEY "
				}
				indexStr += fmt.Sprintf("`%s` ", formatStr(indexdef.IndexName))
				rewriteIndexStr += fmt.Sprintf("`%s` ", formatStr(indexdef.IndexName))
				if !catalog.IsNullIndexAlgo(indexdef.IndexAlgo) && !catalog.IsRTreeIndexAlgo(indexdef.IndexAlgo) {
					indexStr += fmt.Sprintf("USING %s ", indexdef.IndexAlgo)
				}
				if !catalog.IsNullIndexAlgo(indexdef.IndexAlgo) {
					rewriteIndexStr += fmt.Sprintf("USING %s ", indexdef.IndexAlgo)
				}
				indexStr += "("
				rewriteIndexStr += "("
				i := 0
				for _, part := range indexdef.Parts {
					if catalog.IsAlias(part) {
						continue
					}
					if i > 0 {
						indexStr += ","
						rewriteIndexStr += ","
					}

					part = colNameToOriginName[part]
					indexStr += fmt.Sprintf("`%s`", formatStr(part))
					rewriteIndexStr += fmt.Sprintf("`%s`", formatStr(part))
					i++
				}

				indexStr += ")"
				rewriteIndexStr += ")"
				if indexdef.IndexAlgoParams != "" {
					var paramList string
					paramList, err = catalog.IndexParamsToStringList(indexdef.IndexAlgoParams)
					if err != nil {
						return "", nil, err
					}
					indexStr += paramList
					rewriteIndexStr += paramList
				}
				if indexStr != rewriteIndexStr {
					rewritePairs = append(rewritePairs, struct {
						display string
						rewrite string
					}{display: indexStr, rewrite: rewriteIndexStr})
				}
			}
			if indexdef.Comment != "" {
				indexdef.Comment = strings.Replace(indexdef.Comment, "'", "\\'", -1)
				indexStr += fmt.Sprintf(" COMMENT '%s'", formatStr(indexdef.Comment))
				if len(rewritePairs) > 0 && rewritePairs[len(rewritePairs)-1].display != rewritePairs[len(rewritePairs)-1].rewrite &&
					strings.HasPrefix(indexStr, rewritePairs[len(rewritePairs)-1].display) {
					rewritePairs[len(rewritePairs)-1] = struct {
						display string
						rewrite string
					}{
						display: indexStr,
						rewrite: rewritePairs[len(rewritePairs)-1].rewrite + fmt.Sprintf(" COMMENT '%s'", formatStr(indexdef.Comment)),
					}
				}
			}
			if rowCount != 0 {
				createStr += ",\n"
			}
			createStr += indexStr
		}
	}

	updateFKTableDef := func(fkDef *TableDef) (*TableDef, error) {
		if cloneStmt == nil || cloneStmt.StmtType == tree.NoClone {
			return fkDef, nil
		}

		if fkDef == nil || tableDef == fkDef {
			// self refer
			return fkDef, nil
		}

		var (
			referType    int
			tempTableDef *TableDef
		)

		update := func(snap *Snapshot) error {
			if _, tempTableDef, err = ctx.Resolve(schemaName, fkDef.Name, snap); err != nil {
				return err
			}

			fkDef = tempTableDef
			return err
		}

		if cloneStmt.SrcTable.SchemaName.String() == fkDef.DbName {
			// within db refer
			referType = 1
		} else {
			// between db refer
			referType = 2
		}

		switch cloneStmt.StmtType {
		case tree.CloneCluster, tree.CloneAccount, tree.WithinDBCloneTable, tree.WithinAccBetweenDBCloneTable:
			return fkDef, nil
		case tree.WithinAccCloneDB:
			if referType == 1 {
				err = update(nil)
			}
			return fkDef, err
		case tree.BetweenAccCloneDB:
			if referType == 1 {
				err = update(nil)
			} else {
				err = moerr.NewInternalErrorNoCtx(
					"cannot clone a db to another account when it has foreign key reference on another db",
				)
			}
			return fkDef, err
		case tree.BetweenAccCloneTable:
			return nil, moerr.NewInternalErrorNoCtx(
				"cannot clone a table to another account when it has foreign key reference on another table",
			)
		default:
			return fkDef, nil
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
				if _, fkTableDef, err = ctx.ResolveSubscriptionTableById(fk.ForeignTbl, ctx.GetQueryingSubscription()); err != nil {
					return "", nil, err
				}
				if fkTableDef, err = updateFKTableDef(fkTableDef); err != nil {
					return "", nil, err
				}
			} else {
				if _, fkTableDef, err = ctx.ResolveById(fk.ForeignTbl, snapshot); err != nil {
					return "", nil, err
				}
				if fkTableDef, err = updateFKTableDef(fkTableDef); err != nil {
					return "", nil, err
				}
			}
		}

		// fkTable may not exist in snapshot restoration
		if fkTableDef == nil {
			return "", nil, moerr.NewInternalErrorNoCtxf(
				"can't find fkTable from fk %s.%s.(%s) {%s}",
				tableDef.DbName, tableDef.Name,
				strings.Join(colOriginNames, ","),
				snapshot.String(),
			)
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
		if cloneStmt != nil || tableDef.DbName != fkTableDef.DbName {
			fkRefDbTblName = fmt.Sprintf("`%s`.`%s`", formatStr(fkTableDef.DbName), formatStr(fkTableDef.Name))
		}
		createStr += fmt.Sprintf("  CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES %s (`%s`) ON DELETE %s ON UPDATE %s",
			formatStr(fk.Name), strings.Join(colOriginNames, "`,`"), fkRefDbTblName, strings.Join(fkColOriginNames, "`,`"), strings.ReplaceAll(fk.OnDelete.String(), "_", " "), strings.ReplaceAll(fk.OnUpdate.String(), "_", " "))
	}

	for _, checkDef := range checkDefs {
		createStr += ",\n  " + checkDef
	}

	if rowCount != 0 {
		createStr += "\n"
	}
	createStr += ")"

	var comment string
	for _, def := range tableDef.Defs {
		if proDef, ok := def.Def.(*plan.TableDef_DefType_Properties); ok {
			for _, kv := range proDef.Properties.Properties {
				if kv.Key == catalog.SystemRelAttr_Comment {
					comment = " COMMENT='" + kv.Value + "'"
				}
			}
		}
	}

	createStr += comment

	if tableDef.Partition != nil {
		ps := ctx.GetProcess().GetPartitionService()
		if ps.Enabled() {
			partitionBy := " partition by "

			txn := ctx.GetProcess().GetTxnOperator()
			newCtx := ctx.GetContext()
			if snapshot != nil && snapshot.TS != nil {
				txn = txn.CloneSnapshotOp(*snapshot.TS)

				if snapshot.Tenant != nil {
					newCtx = defines.AttachAccountId(newCtx, snapshot.Tenant.TenantID)
				}
			}

			meta, err := ps.GetPartitionMetadata(newCtx, tableDef.GetTblId(), txn)
			if err != nil {
				return "", nil, err
			}

			partitionBy += meta.Description

			switch meta.Method {
			case partition.PartitionMethod_Hash,
				partition.PartitionMethod_LinearHash,
				partition.PartitionMethod_Key,
				partition.PartitionMethod_LinearKey:
				partitionBy += fmt.Sprintf(" partitions %d", len(meta.Partitions))
			default:
				partitionBy += " ("
				for i, p := range meta.Partitions {
					if i > 0 {
						partitionBy += ", "
					}
					partitionBy += "partition" + " " + p.Name + " " + p.ExprStr
				}
				partitionBy += ")"
			}

			createStr += partitionBy
		}
	}

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
	var stmt tree.Statement
	if ctx != nil {
		rewriteStr := createStr
		for _, pair := range rewritePairs {
			rewriteStr = strings.Replace(rewriteStr, pair.display, pair.rewrite, 1)
		}
		stmt, err = getRewriteSQLStmt(ctx, rewriteStr)
	}
	return createStr, stmt, err
}

func extractTopLevelCheckDefs(tableDef *plan.TableDef) []string {
	if tableDef == nil || tableDef.Createsql == "" || tableDef.TableType == catalog.SystemExternalRel {
		return nil
	}
	if !containsKeywordOutsideQuotes(tableDef.Createsql, "CHECK") {
		return nil
	}

	defsSection, ok := extractCreateTableDefsSection(tableDef.Createsql)
	if !ok {
		return nil
	}

	segments := splitTopLevelDefs(defsSection)
	checks := make([]string, 0, len(segments))
	for _, segment := range segments {
		segment = strings.TrimSpace(segment)
		if isTopLevelCheckDef(segment) {
			checks = append(checks, segment)
		}
	}
	return checks
}

func extractCreateTableDefsSection(createSQL string) (string, bool) {
	start := findTopLevelByte(createSQL, '(')
	if start == -1 {
		return "", false
	}

	end := findMatchingParen(createSQL, start)
	if end == -1 || end <= start {
		return "", false
	}
	return createSQL[start+1 : end], true
}

func splitTopLevelDefs(defs string) []string {
	parts := make([]string, 0, 8)
	start := 0
	depth := 0
	for i := 0; i < len(defs); i++ {
		switch defs[i] {
		case '\'', '"', '`':
			i = skipQuoted(defs, i)
		case '#':
			i = skipLineComment(defs, i)
		case '-':
			if i+1 < len(defs) && defs[i+1] == '-' {
				i = skipLineComment(defs, i+1)
			}
		case '/':
			if i+1 < len(defs) && defs[i+1] == '*' {
				i = skipBlockComment(defs, i)
			}
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, defs[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, defs[start:])
	return parts
}

func isTopLevelCheckDef(def string) bool {
	if def == "" {
		return false
	}

	trimmed := strings.TrimSpace(def)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, "CHECK") {
		return true
	}
	if !strings.HasPrefix(upper, "CONSTRAINT") {
		return false
	}
	return containsKeywordOutsideQuotes(trimmed, "CHECK")
}

func containsKeywordOutsideQuotes(s string, keyword string) bool {
	upper := strings.ToUpper(s)
	for i := 0; i < len(upper); i++ {
		switch upper[i] {
		case '\'', '"', '`':
			i = skipQuoted(upper, i)
		case '#':
			i = skipLineComment(upper, i)
		case '-':
			if i+1 < len(upper) && upper[i+1] == '-' {
				i = skipLineComment(upper, i+1)
			}
		case '/':
			if i+1 < len(upper) && upper[i+1] == '*' {
				i = skipBlockComment(upper, i)
			}
		default:
			if hasKeywordAt(upper, keyword, i) {
				return true
			}
		}
	}
	return false
}

func hasKeywordAt(s string, keyword string, pos int) bool {
	end := pos + len(keyword)
	if end > len(s) || s[pos:end] != keyword {
		return false
	}
	prevIsIdent := pos > 0 && isIdentChar(s[pos-1])
	nextIsIdent := end < len(s) && isIdentChar(s[end])
	return !prevIsIdent && !nextIsIdent
}

func isIdentChar(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

func findTopLevelByte(s string, target byte) int {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"', '`':
			i = skipQuoted(s, i)
		case '#':
			i = skipLineComment(s, i)
		case '-':
			if i+1 < len(s) && s[i+1] == '-' {
				i = skipLineComment(s, i+1)
			}
		case '/':
			if i+1 < len(s) && s[i+1] == '*' {
				i = skipBlockComment(s, i)
			}
		default:
			if s[i] == target {
				return i
			}
		}
	}
	return -1
}

func findMatchingParen(s string, start int) int {
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '\'', '"', '`':
			i = skipQuoted(s, i)
		case '#':
			i = skipLineComment(s, i)
		case '-':
			if i+1 < len(s) && s[i+1] == '-' {
				i = skipLineComment(s, i+1)
			}
		case '/':
			if i+1 < len(s) && s[i+1] == '*' {
				i = skipBlockComment(s, i)
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func skipQuoted(s string, start int) int {
	quote := s[start]
	for i := start + 1; i < len(s); i++ {
		if s[i] == '\\' && quote != '`' {
			i++
			continue
		}
		if s[i] != quote {
			continue
		}
		if i+1 < len(s) && s[i+1] == quote && quote != '`' {
			i++
			continue
		}
		return i
	}
	return len(s) - 1
}

func skipLineComment(s string, start int) int {
	for i := start + 1; i < len(s); i++ {
		if s[i] == '\n' {
			return i
		}
	}
	return len(s) - 1
}

func skipBlockComment(s string, start int) int {
	for i := start + 2; i < len(s); i++ {
		if s[i-1] == '*' && s[i] == '/' {
			return i
		}
	}
	return len(s) - 1
}

// FormatColType Get the formatted description of the column type.
func FormatColType(colType plan.Type) string {
	typ := types.T(colType.Id).ToType()

	ts := typ.String()
	// after decimal fix, remove this
	if typ.Oid.IsDecimal() {
		ts = "DECIMAL"
	}
	if isSetPlanType(&colType) {
		ts = "SET"
	}
	if subtype := geometrySubtypeName(&colType); subtype != "" {
		ts = subtype
	}
	if srid, ok := geometrySRIDValue(&colType); ok {
		ts = fmt.Sprintf("%s SRID %d", ts, srid)
	}

	suffix := ""
	switch types.T(colType.Id) {
	case types.T_enum:
		fallthrough
	case types.T_uint64:
		if !isEnumOrSetPlanType(&colType) {
			break
		}
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
	return builder.ResolveTsHint(AtTsExpr)
}
