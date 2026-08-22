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
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	sqlmongodb "github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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
	// This formatter is also used by context-free consumers that do not own a
	// source catalog snapshot (CDC, publication, and dump). Planner entry points
	// reconcile index visibility before calling here. Subscription-aware clone
	// planning additionally passes its scoped publisher identity explicitly.
	var sourceSubscription *SubscriptionMeta
	if ctx != nil {
		sourceSubscription = ctx.GetQueryingSubscription()
	}
	return constructCreateTableSQL(
		ctx, tableDef, snapshot, useDbName, cloneStmt, true,
		sourceSubscription,
	)
}

func createTableIndexVisible(indexDef *plan.IndexDef) bool {
	if visible, isSet := catalog.GetIndexVisibility(indexDef); isSet {
		return visible
	}
	// A context-free caller cannot query mo_indexes. Preserve proto3's
	// historical default rather than treating an omitted bool as INVISIBLE.
	return true
}

func constructCreateTableSQL(
	ctx CompilerContext,
	tableDef *plan.TableDef,
	snapshot *Snapshot,
	useDbName bool,
	cloneStmt *tree.CloneTable,
	includeChecks bool,
	sourceSubscription *SubscriptionMeta,
) (string, tree.Statement, error) {
	var err error
	var createStr string
	sqlMode := ""
	if ctx != nil {
		sqlMode = *parserSQLModeFromContext(ctx)
	}
	rewritePairs := make([]struct {
		display string
		rewrite string
	}, 0)
	var checkDefs []string
	if includeChecks {
		checkDefs = constructCheckDefs(tableDef)
	}
	var mongoEnvelope sqlmongodb.CreateSQLEnvelope
	mongoColumns := make(map[string]sqlmongodb.ColumnMapping)
	if tableDef.TableType == catalog.SystemExternalRel {
		var isMongoDB bool
		isMongoDB, err = IsMongoDBTableDef(ctx.GetContext(), tableDef)
		if err != nil {
			return "", nil, err
		}
		if isMongoDB {
			mongoEnvelope, _, err = sqlmongodb.ParseCreateSQLEnvelope(ctx.GetContext(), tableDef.Createsql)
			if err != nil {
				return "", nil, err
			}
			for _, column := range mongoEnvelope.Columns {
				mongoColumns[strings.ToLower(column.Name)] = column
			}
		}
	}

	tblName := tableDef.Name
	schemaName := tableDef.DbName
	dbTblName := sqlquote.Ident(tblName)
	if useDbName {
		dbTblName = sqlquote.QualifiedIdent(schemaName, tblName)
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
	displayTableCharset := effectiveTableCharsetForShowCreate(tableDef)
	columnTableCharset := displayTableCharset
	if tableDef.TableType == catalog.SystemExternalRel {
		// External-table grammar has no table charset option. Use a sentinel
		// that makes every text column emit its own replay-safe collation instead
		// of relying on a table clause that would make the DDL unparsable.
		columnTableCharset = uint32(types.CharsetLegacy)
	}

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
		fmt.Fprintf(buf, "  %s %s", sqlquote.Ident(colNameOrigin), typeStr)
		appendTextCharsetForShowCreate(buf, col.Typ, columnTableCharset)

		//-------------------------------------------------------------------------------------------------------------
		if col.GeneratedCol != nil && col.GeneratedCol.Expr != nil {
			// Generated column: output GENERATED ALWAYS AS (expr) STORED/VIRTUAL
			if !col.Default.NullAbility {
				buf.WriteString(" NOT NULL")
			}
			buf.WriteString(" GENERATED ALWAYS AS (" + col.GeneratedCol.OriginString + ")")
			if col.GeneratedCol.IsStored {
				buf.WriteString(" STORED")
			} else {
				buf.WriteString(" VIRTUAL")
			}
		} else if col.Typ.AutoIncr {
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
				buf.WriteString(" DEFAULT " + formatDefaultExpr(col.Default.OriginString, col.Default.Expr))
			}

			if col.OnUpdate != nil && col.OnUpdate.Expr != nil {
				buf.WriteString(" ON UPDATE " + col.OnUpdate.OriginString)
			}
		}

		if col.Comment != "" {
			buf.WriteString(" COMMENT '" + col.Comment + "'")
		}
		if mapping, ok := mongoColumns[strings.ToLower(col.Name)]; ok {
			buf.WriteString(" MONGODB_PATH '")
			buf.WriteString(formatStrInSingleQuotesForSQLMode(mapping.Path, sqlMode))
			buf.WriteString("' MONGODB_CONVERT '")
			buf.WriteString(formatStrInSingleQuotesForSQLMode(mapping.Conversion, sqlMode))
			buf.WriteString("'")
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
				pkStr += fmt.Sprintf("%s)", sqlquote.Ident(def))
			} else {
				pkStr += fmt.Sprintf("%s,", sqlquote.Ident(def))
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
			if indexdef == nil {
				continue
			}
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
			indexVisible := createTableIndexVisible(indexdef)
			if !indexdef.Unique && (catalog.IsFullTextIndexAlgo(indexdef.IndexAlgo) || catalog.IsFullText2IndexAlgo(indexdef.IndexAlgo)) {
				if catalog.IsFullText2IndexAlgo(indexdef.IndexAlgo) {
					indexStr += " FULLTEXT2 "
				} else {
					indexStr += " FULLTEXT "
				}

				if len(indexdef.IndexName) > 0 {
					indexStr += sqlquote.Ident(indexdef.IndexName)
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
					indexStr += sqlquote.Ident(part)
					i++
				}

				indexStr += ")"

				// INCLUDE columns: render so SHOW CREATE round-trips — a rebuild from
				// the clause-less DDL would silently drop the covering/prefilter columns.
				// Uses the same helper as the vector-index branch below (INCLUDE is an
				// order-flexible index_option, so it may precede WITH PARSER).
				includedColumns, incErr := indexDefIncludedColumns(indexdef)
				if incErr != nil {
					return "", nil, incErr
				}
				indexStr += indexIncludeColumnsToString(includedColumns, colNameToOriginName)

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

					if catalog.IsFullText2IndexAlgo(indexdef.IndexAlgo) {
						// fulltext2 carries persisted build options (position_free,
						// max_index_capacity, max_postings_capacity) plus async / cron
						// scheduling. Render the FULL set via the shared list so SHOW CREATE
						// round-trips — a rebuild from parser-only DDL would silently drop
						// POSITION_FREE and the capacities and build a different index.
						paramStr, err := catalog.IndexParamsToStringList(indexdef.IndexAlgoParams)
						if err != nil {
							return "", nil, err
						}
						indexStr += paramStr
					} else {
						val, err = sonic.Get([]byte(indexdef.IndexAlgoParams), catalog.Async)
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
				}
				if !indexVisible {
					indexStr += " INVISIBLE"
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
				indexStr += fmt.Sprintf("%s ", sqlquote.Ident(indexdef.IndexName))
				rewriteIndexStr += fmt.Sprintf("%s ", sqlquote.Ident(indexdef.IndexName))
				if !catalog.IsNullIndexAlgo(indexdef.IndexAlgo) && !catalog.IsRTreeIndexAlgo(indexdef.IndexAlgo) {
					indexStr += fmt.Sprintf("USING %s ", indexdef.IndexAlgo)
				}
				if !catalog.IsNullIndexAlgo(indexdef.IndexAlgo) {
					rewriteIndexStr += fmt.Sprintf("USING %s ", indexdef.IndexAlgo)
				}
				prefixLengths, err := catalog.IndexPrefixLengthsFromParamsWithError(indexdef.IndexAlgoParams)
				if err != nil {
					return "", nil, err
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

					originPart := colNameToOriginName[part]
					indexStr += sqlquote.Ident(originPart)
					rewriteIndexStr += sqlquote.Ident(originPart)
					if length, ok := prefixLengths[part]; ok {
						prefixLength := fmt.Sprintf("(%d)", length)
						indexStr += prefixLength
						rewriteIndexStr += prefixLength
					}
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
				includedColumns, err := indexDefIncludedColumns(indexdef)
				if err != nil {
					return "", nil, err
				}
				includeList := indexIncludeColumnsToString(includedColumns, colNameToOriginName)
				indexStr += includeList
				rewriteIndexStr += includeList
				if !indexVisible {
					indexStr += " INVISIBLE"
					rewriteIndexStr += " INVISIBLE"
				}
				if indexStr != rewriteIndexStr {
					rewritePairs = append(rewritePairs, struct {
						display string
						rewrite string
					}{display: indexStr, rewrite: rewriteIndexStr})
				}
			}
			if indexdef.Comment != "" {
				formattedComment := formatStrInSingleQuotesForSQLMode(indexdef.Comment, sqlMode)
				indexStr += fmt.Sprintf(" COMMENT '%s'", formattedComment)
				if len(rewritePairs) > 0 && rewritePairs[len(rewritePairs)-1].display != rewritePairs[len(rewritePairs)-1].rewrite &&
					strings.HasPrefix(indexStr, rewritePairs[len(rewritePairs)-1].display) {
					rewritePairs[len(rewritePairs)-1] = struct {
						display string
						rewrite string
					}{
						display: indexStr,
						rewrite: rewritePairs[len(rewritePairs)-1].rewrite + fmt.Sprintf(" COMMENT '%s'", formattedComment),
					}
				}
			}
			if rowCount != 0 {
				createStr += ",\n"
			}
			createStr += indexStr
		}
	}

	sourceDatabaseName := ""
	if cloneStmt != nil {
		sourceDatabaseName = cloneStmt.SrcTable.SchemaName.String()
		if sourceSubscription != nil {
			sourceDatabaseName = sourceSubscription.DbName
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
			if tempTableDef == nil {
				enabled, resolveErr := IsForeignKeyChecksEnabled(ctx)
				if resolveErr != nil {
					return resolveErr
				}
				if !enabled {
					return nil
				}
			}
			fkDef = tempTableDef
			return err
		}

		if sourceDatabaseName == fkDef.DbName {
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
			if sourceSubscription != nil {
				if _, fkTableDef, err = ctx.ResolveSubscriptionTableById(fk.ForeignTbl, sourceSubscription); err != nil {
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

		fkRefDbName := fkTableDef.DbName
		if cloneStmt == nil && sourceSubscription != nil && fkRefDbName == sourceSubscription.DbName {
			fkRefDbName = sourceSubscription.SubName
		}
		if cloneStmt != nil && (cloneStmt.StmtType == tree.WithinAccCloneDB || cloneStmt.StmtType == tree.BetweenAccCloneDB) &&
			sourceDatabaseName == fkTableDef.DbName {
			fkRefDbName = schemaName
		}
		fkRefDbTblName := sqlquote.Ident(fkTableDef.Name)
		if cloneStmt != nil || tableDef.DbName != fkTableDef.DbName {
			fkRefDbTblName = sqlquote.QualifiedIdent(fkRefDbName, fkTableDef.Name)
		}
		createStr += fmt.Sprintf("  CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s ON UPDATE %s",
			sqlquote.Ident(fk.Name), joinQuotedIdentifiers(colOriginNames), fkRefDbTblName, joinQuotedIdentifiers(fkColOriginNames), strings.ReplaceAll(fk.OnDelete.String(), "_", " "), strings.ReplaceAll(fk.OnUpdate.String(), "_", " "))
	}

	for _, checkDef := range checkDefs {
		createStr += ",\n  " + checkDef
	}

	if rowCount != 0 {
		createStr += "\n"
	}
	createStr += ")"
	if tableDef.TableType != catalog.SystemExternalRel {
		createStr += tableCharsetForShowCreate(ctx, displayTableCharset)
	}

	var comment string
	var properties []*plan.Property // Collect non-system properties for PROPERTIES clause
	for _, def := range tableDef.Defs {
		if proDef, ok := def.Def.(*plan.TableDef_DefType_Properties); ok {
			for _, kv := range proDef.Properties.Properties {
				if kv.Key == catalog.SystemRelAttr_Comment {
					comment = " COMMENT='" + kv.Value + "'"
				} else if kv.Key != catalog.SystemRelAttr_Kind &&
					kv.Key != catalog.SystemRelAttr_CreateSQL &&
					kv.Key != catalog.PropSchemaExtra {
					// Collect non-system properties (excluding Comment, Kind, CreateSQL, SchemaExtra)
					// These will be included in PROPERTIES clause
					properties = append(properties, kv)
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

	// Add PROPERTIES clause if there are any non-system properties
	// PROPERTIES is a table option and should be before CLUSTER BY
	if len(properties) > 0 {
		propsStr := " PROPERTIES("
		for i, prop := range properties {
			if i > 0 {
				propsStr += ", "
			}
			propsStr += fmt.Sprintf("%s = %s", formatStrLitForSQLMode(prop.Key, sqlMode), formatStrLitForSQLMode(prop.Value, sqlMode))
		}
		propsStr += ")"
		createStr += propsStr
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
					clusterby += fmt.Sprintf(", %s", sqlquote.Ident(cbName))
				} else {
					clusterby += sqlquote.Ident(cbName)
				}
			}
		} else {
			//single column cluster by
			clusterby += sqlquote.Ident(tableDef.ClusterBy.Name)
		}
		clusterby += ")"
		createStr += clusterby
	}

	if tableDef.TableType == catalog.SystemExternalRel {
		if env, found, parseErr := sqliceberg.ParseCreateSQLEnvelope(ctx.GetContext(), tableDef.Createsql); parseErr != nil {
			return "", nil, parseErr
		} else if found {
			createStr += formatIcebergTableOptionsForShowCreate(env, sqlMode)
			var stmt tree.Statement
			if ctx != nil {
				stmt, err = getRewriteSQLStmtWithSQLMode(ctx, createStr, sqlMode)
			}
			return createStr, stmt, err
		}
		if len(mongoColumns) > 0 {
			createStr += formatMongoDBTableOptionsForShowCreate(mongoEnvelope, sqlMode)
			var stmt tree.Statement
			if ctx != nil {
				stmt, err = getRewriteSQLStmtWithSQLMode(ctx, createStr, sqlMode)
			}
			return createStr, stmt, err
		}

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
		createStr += formatExternalTableOptionsForShowCreate(param, sqlMode)

		fields := ""
		if param.Tail != nil && param.Tail.Fields != nil {
			if param.Tail.Fields.Terminated != nil {
				if param.Tail.Fields.Terminated.Value == "" {
					fields += " TERMINATED BY \"\""
				} else {
					fields += fmt.Sprintf(" TERMINATED BY '%s'", formatStrInSingleQuotesForSQLMode(param.Tail.Fields.Terminated.Value, sqlMode))
				}
			}

			escape := func(value byte) string {
				if value == 0 {
					return ""
				}
				return formatStrInSingleQuotesForSQLMode(string([]byte{value}), sqlMode)
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
				line += fmt.Sprintf(" STARTING BY '%s'", formatStrInSingleQuotesForSQLMode(param.Tail.Lines.StartingBy, sqlMode))
			}
			if param.Tail.Lines.TerminatedBy != nil {
				line += fmt.Sprintf(" TERMINATED BY '%s'", formatLinesTerminatedBy(param.Tail.Lines.TerminatedBy.Value, sqlMode))
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
		stmt, err = getRewriteSQLStmtWithSQLMode(ctx, rewriteStr, sqlMode)
	}
	return createStr, stmt, err
}

func appendTextCharsetForShowCreate(buf *bytes.Buffer, typ plan.Type, tableCharset uint32) {
	switch types.T(typ.Id) {
	case types.T_char, types.T_varchar, types.T_text:
	default:
		return
	}

	switch typ.Charset {
	case uint32(types.CharsetLegacy):
		// A migrated table default can coexist with a text column whose old
		// catalog row still has no charset metadata. Preserve that column's
		// historical bytewise ordering even when it cannot inherit the table
		// display default.
		if tableCharset != uint32(types.CharsetUTF8MB4Bin) {
			buf.WriteString(" COLLATE utf8mb4_bin")
		}
	case uint32(types.CharsetUTF8MB4Bin):
		buf.WriteString(" COLLATE utf8mb4_bin")
	case uint32(types.CharsetBinary):
		// Packed binary values can deliberately use a VARCHAR container. COLLATE
		// binary is the lossless MO spelling for that representation; CHARACTER
		// SET binary would instead change the physical type to VARBINARY/BLOB.
		buf.WriteString(" COLLATE binary")
	case uint32(types.CharsetUTF8):
		if tableCharset != uint32(types.CharsetUTF8) {
			buf.WriteString(" COLLATE utf8mb4_general_ci")
		}
	}
}

func effectiveTableCharsetForShowCreate(tableDef *plan.TableDef) uint32 {
	if tableDef.DefaultCharset != uint32(types.CharsetLegacy) {
		return tableDef.DefaultCharset
	}
	hasTextColumn := false
	for _, col := range tableDef.Cols {
		switch types.T(col.Typ.Id) {
		case types.T_char, types.T_varchar, types.T_text:
			hasTextColumn = true
			if col.Typ.Charset == uint32(types.CharsetLegacy) {
				// Legacy text was ordered bytewise before charset metadata became
				// meaningful. There is no SQL spelling for CharsetLegacy, so use
				// utf8mb4_bin as its replay-safe, nonbinary text identity. Using
				// COLLATE binary here would incorrectly advertise VARCHAR as binary
				// protocol data.
				return uint32(types.CharsetUTF8MB4Bin)
			}
		}
	}
	if !hasTextColumn {
		return tableDef.DefaultCharset
	}
	// Program-authored system definitions predate the table-default field but
	// now carry explicit UTF-8 on every text column. Treat UTF-8 as their display
	// default so SHOW CREATE stays concise. A genuinely legacy column above uses
	// the bytewise display default, causing explicit general_ci peers to be shown.
	return uint32(types.CharsetUTF8)
}

func tableCharsetForShowCreate(ctx CompilerContext, charset uint32) string {
	switch charset {
	case uint32(types.CharsetUTF8):
		// collation_server is runtime-configurable. Spell general_ci whenever it
		// differs from the effective runtime default. Callers such as CDC and
		// table dump have no compiler context, so they must also spell it: an
		// unknown target default is not safe to inherit during DDL replay.
		if ctx == nil {
			return " COLLATE=utf8mb4_general_ci"
		}
		serverCharset, err := tableDefaultCharset(ctx, nil)
		if err == nil && serverCharset == uint32(types.CharsetUTF8) {
			return ""
		}
		return " COLLATE=utf8mb4_general_ci"
	case uint32(types.CharsetUTF8MB4Bin):
		return " COLLATE=utf8mb4_bin"
	case uint32(types.CharsetBinary):
		return " CHARACTER SET=binary"
	default:
		return ""
	}
}

func indexIncludeColumnsToString(includedColumns []string, colNameToOriginName map[string]string) string {
	if len(includedColumns) == 0 {
		return ""
	}

	names := make([]string, 0, len(includedColumns))
	for _, colName := range includedColumns {
		resolvedName := catalog.ResolveAlias(colName)
		if originName := colNameToOriginName[resolvedName]; originName != "" {
			resolvedName = originName
		}
		names = append(names, fmt.Sprintf("`%s`", formatStr(resolvedName)))
	}
	return fmt.Sprintf(" INCLUDE (%s)", strings.Join(names, ", "))
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

func constructCheckDefs(tableDef *plan.TableDef) []string {
	if tableDef == nil || tableDef.TableType == catalog.SystemExternalRel {
		return nil
	}
	if len(tableDef.Checks) == 0 {
		return extractTopLevelCheckDefs(tableDef)
	}

	checks := make([]string, 0, len(tableDef.Checks))
	for _, check := range tableDef.Checks {
		if check == nil || check.OriginSql == "" {
			continue
		}
		checks = append(
			checks,
			fmt.Sprintf(
				"CONSTRAINT `%s` CHECK (%s)",
				formatStr(check.Name),
				check.OriginSql,
			),
		)
	}
	if len(checks) == 0 {
		return extractTopLevelCheckDefs(tableDef)
	}
	return checks
}

func joinQuotedIdentifiers(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = sqlquote.Ident(name)
	}
	return strings.Join(quoted, ",")
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
	if hasKeywordAt(upper, "CHECK", 0) {
		return true
	}
	if !hasKeywordAt(upper, "CONSTRAINT", 0) {
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
	return ch == '_' || ch == '$' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
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
	if arrayType := arrayPlanTypeString(&colType); arrayType != "" {
		return strings.ToUpper(arrayType[:len("array")]) + arrayType[len("array"):]
	}

	typ := types.T(colType.Id).ToType()

	ts := typ.String()
	if typ.Oid == types.T_text && colType.Width == types.MaxTinyTextLen {
		ts = "TINYTEXT"
	}
	// after decimal fix, remove this
	if typ.Oid.IsDecimal() {
		ts = "DECIMAL"
	}
	if isSetPlanType(&colType) {
		ts = "SET"
	}
	if subtype := geometrySubtypeName(&colType); subtype != "" {
		ts = subtype
		// A GEOMETRY32 subtype column renders with the "32" suffix (e.g.
		// POINT32) so SHOW CREATE round-trips the float32 family.
		if types.T(colType.Id) == types.T_geometry32 {
			ts += "32"
		}
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

	case types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
		suffix = fmt.Sprintf("(%d)", colType.Width)

	}
	return ts + suffix
}

// formatStrInSingleQuotes returns the contents of a default-mode SQL string
// literal. Use formatStrInSingleQuotesForSQLMode when the generated DDL will be
// reparsed under a specific session SQL mode.
func formatStrInSingleQuotes(s string) string {
	return formatStrInSingleQuotesForSQLMode(s, "")
}

// formatStrInSingleQuotesForSQLMode returns the contents of a string literal
// that reparses to s under sqlMode. In NO_BACKSLASH_ESCAPES, backslashes must
// remain single because they are data, while quote doubling remains valid.
func formatStrInSingleQuotesForSQLMode(s, sqlMode string) string {
	literal := formatStrLitForSQLMode(s, sqlMode)
	return literal[1 : len(literal)-1]
}

// formatLinesTerminatedBy renders a LINES TERMINATED BY value for SHOW CREATE
// using the same SQL-mode contract as the surrounding generated DDL. In the
// default mode, LF and CRLF are emitted as \n and \r\n source escapes; under
// NO_BACKSLASH_ESCAPES their raw bytes are emitted instead.
func formatLinesTerminatedBy(value, sqlMode string) string {
	return formatStrInSingleQuotesForSQLMode(value, sqlMode)
}

func formatExternalTableOptionsForShowCreate(param *tree.ExternParam, sqlMode string) string {
	if param.ScanType == tree.S3 {
		return formatS3ExternalOptionsForShowCreate(param, sqlMode)
	}
	return formatInfileExternalOptionsForShowCreate(param, sqlMode)
}

func formatIcebergTableOptionsForShowCreate(env sqliceberg.CreateSQLEnvelope, sqlMode string) string {
	options := []struct {
		key   string
		value string
	}{
		{key: "catalog", value: env.Catalog},
		{key: "namespace", value: env.Namespace},
		{key: "table", value: env.Table},
		{key: "ref", value: env.DefaultRef},
		{key: "read_mode", value: env.ReadMode},
		{key: "write_mode", value: env.WriteMode},
	}
	var builder strings.Builder
	builder.WriteString(" ENGINE = ICEBERG WITH (")
	for i, option := range options {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("\"")
		builder.WriteString(option.key)
		builder.WriteString("\" = '")
		builder.WriteString(formatStrInSingleQuotesForSQLMode(option.value, sqlMode))
		builder.WriteString("'")
	}
	builder.WriteString(")")
	return builder.String()
}

func formatMongoDBTableOptionsForShowCreate(env sqlmongodb.CreateSQLEnvelope, sqlMode string) string {
	options := []struct {
		key   string
		value string
	}{
		{key: "connection", value: env.Connection},
		{key: "database", value: env.Database},
		{key: "collection", value: env.Collection},
		{key: "schema_mode", value: env.SchemaMode},
		{key: "conversion_mode", value: env.ConversionMode},
		{key: "max_parallelism", value: fmt.Sprintf("%d", env.MaxParallelism)},
	}
	if env.SplitKey != "" {
		options = append(options, struct {
			key   string
			value string
		}{key: "split_key", value: env.SplitKey})
	}
	var builder strings.Builder
	builder.WriteString(" ENGINE = MONGODB WITH (")
	for i, option := range options {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("\"")
		builder.WriteString(option.key)
		builder.WriteString("\" = '")
		builder.WriteString(formatStrInSingleQuotesForSQLMode(option.value, sqlMode))
		builder.WriteString("'")
	}
	builder.WriteString(")")
	return builder.String()
}

func formatInfileExternalOptionsForShowCreate(param *tree.ExternParam, sqlMode string) string {
	if pattern, writable := GetWriteFilePattern(param); writable {
		// Writable external tables must be recreatable from their own DDL:
		// snapshot/PITR restore replays SHOW CREATE output, so masking the
		// read FILEPATH (or emitting empty optional keys, which the read-side
		// option validator rejects — e.g. 'JSONDATA'='') would silently
		// produce a table that can write but not read its files.
		parts := make([]string, 0, 6)
		appendInfileOptionForShowCreate(&parts, "FILEPATH", param.Filepath, sqlMode)
		appendInfileOptionForShowCreate(&parts, "COMPRESSION", param.CompressType, sqlMode)
		appendInfileOptionForShowCreate(&parts, "FORMAT", param.Format, sqlMode)
		appendInfileOptionForShowCreate(&parts, "JSONDATA", param.JsonData, sqlMode)
		appendInfileOptionForShowCreate(&parts, "WRITE_FILE_PATTERN", pattern, sqlMode)
		// The CSV reader skips lines whose raw prefix matches COMMENT (the writer
		// encloses colliding first fields), so the marker affects readback and
		// must round-trip; omitted when unset.
		appendInfileOptionForShowCreate(&parts, "COMMENT", GetCSVComment(param), sqlMode)
		return " INFILE{" + strings.Join(parts, ",") + "}"
	}
	filepath := ""
	if param.HivePartitioning {
		filepath = param.Filepath
	}
	parts := []string{
		formatStrLitForSQLMode("FILEPATH", sqlMode) + "=" + formatStrLitForSQLMode(filepath, sqlMode),
		formatStrLitForSQLMode("COMPRESSION", sqlMode) + "=" + formatStrLitForSQLMode(param.CompressType, sqlMode),
		formatStrLitForSQLMode("FORMAT", sqlMode) + "=" + formatStrLitForSQLMode(param.Format, sqlMode),
		formatStrLitForSQLMode("JSONDATA", sqlMode) + "=" + formatStrLitForSQLMode(param.JsonData, sqlMode),
	}
	// The CSV reader skips lines whose raw prefix matches COMMENT, so the marker
	// changes which rows are returned; round-trip it (omitted when unset).
	appendInfileOptionForShowCreate(&parts, "COMMENT", GetCSVComment(param), sqlMode)
	appendHivePartitionOptionsForShowCreate(&parts, param, true, sqlMode)
	return " INFILE{" + strings.Join(parts, ",") + "}"
}

// appendInfileOptionForShowCreate appends 'KEY'='value' when the value is
// non-empty (the read-side option validators reject empty values for keys
// like jsondata, so omitted is the recreatable form of "unset").
func appendInfileOptionForShowCreate(parts *[]string, key, value, sqlMode string) {
	if value == "" {
		return
	}
	*parts = append(*parts, formatStrLitForSQLMode(key, sqlMode)+"="+formatStrLitForSQLMode(value, sqlMode))
}

func formatS3ExternalOptionsForShowCreate(param *tree.ExternParam, sqlMode string) string {
	parts := make([]string, 0, len(param.Option)/2+2)
	if param.S3Param != nil {
		appendExternalOptionForShowCreate(&parts, "endpoint", param.S3Param.Endpoint, false, sqlMode)
		appendExternalOptionForShowCreate(&parts, "region", param.S3Param.Region, false, sqlMode)
		if hasExternalOption(param, "access_key_id") {
			appendExternalOptionForShowCreate(&parts, "access_key_id", param.S3Param.APIKey, true, sqlMode)
		}
		if hasExternalOption(param, "secret_access_key") {
			appendExternalOptionForShowCreate(&parts, "secret_access_key", param.S3Param.APISecret, true, sqlMode)
		}
		appendExternalOptionForShowCreate(&parts, "bucket", param.S3Param.Bucket, false, sqlMode)
	}
	appendExternalOptionForShowCreate(&parts, "filepath", param.Filepath, false, sqlMode)
	if param.S3Param != nil {
		appendExternalOptionForShowCreate(&parts, "provider", param.S3Param.Provider, false, sqlMode)
		appendExternalOptionForShowCreate(&parts, "role_arn", param.S3Param.RoleArn, false, sqlMode)
		appendExternalOptionForShowCreate(&parts, "external_id", param.S3Param.ExternalId, false, sqlMode)
	}
	appendExternalOptionForShowCreate(&parts, "compression", param.CompressType, false, sqlMode)
	appendExternalOptionForShowCreate(&parts, "format", param.Format, false, sqlMode)
	appendExternalOptionForShowCreate(&parts, "jsondata", param.JsonData, false, sqlMode)
	if pattern, ok := GetWriteFilePattern(param); ok {
		appendExternalOptionForShowCreate(&parts, ExternalWriteFilePatternKey, pattern, false, sqlMode)
	}
	// The CSV reader skips lines whose raw prefix matches COMMENT, so the marker
	// changes which rows are returned; round-trip it (omitted when unset).
	appendExternalOptionForShowCreate(&parts, CSVCommentKey, GetCSVComment(param), false, sqlMode)
	appendHivePartitionOptionsForShowCreate(&parts, param, false, sqlMode)
	return " URL s3option{" + strings.Join(parts, ",") + "}"
}

func appendHivePartitionOptionsForShowCreate(parts *[]string, param *tree.ExternParam, upperKey bool, sqlMode string) {
	if !param.HivePartitioning {
		return
	}
	hivePartitioningKey := "hive_partitioning"
	hivePartitionColsKey := "hive_partition_columns"
	if upperKey {
		hivePartitioningKey = "HIVE_PARTITIONING"
		hivePartitionColsKey = "HIVE_PARTITION_COLUMNS"
	}
	appendExternalOptionForShowCreate(parts, hivePartitioningKey, "true", false, sqlMode)
	appendExternalOptionForShowCreate(parts, hivePartitionColsKey, strings.Join(param.HivePartitionCols, ","), false, sqlMode)
}

func appendExternalOptionForShowCreate(parts *[]string, key, value string, mask bool, sqlMode string) {
	if value == "" && !mask {
		return
	}
	if mask {
		value = "******"
	}
	*parts = append(*parts, formatStrLitForSQLMode(key, sqlMode)+"="+formatStrLitForSQLMode(value, sqlMode))
}

func hasExternalOption(param *tree.ExternParam, key string) bool {
	for i := 0; i < len(param.Option); i += 2 {
		if strings.EqualFold(param.Option[i], key) {
			return true
		}
	}
	return false
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

// formatStrLit quotes s as a replayable MySQL string literal.
func formatStrLit(s string) string {
	var buf strings.Builder
	buf.Grow(len(s) + 2)
	buf.WriteByte('\'')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			buf.WriteString("\\\\")
		case '\'':
			buf.WriteString("''")
		case '\n':
			buf.WriteString("\\n")
		case '\r':
			buf.WriteString("\\r")
		case '\x00':
			buf.WriteString("\\0")
		default:
			buf.WriteByte(s[i])
		}
	}
	buf.WriteByte('\'')
	return buf.String()
}

// formatStrLitForSQLMode quotes s for a generated SQL statement that will be
// reparsed under sqlMode. The two MySQL string-literal modes have different
// meanings for backslashes, so quote doubling alone is sufficient only when
// NO_BACKSLASH_ESCAPES is active.
func formatStrLitForSQLMode(s, sqlMode string) string {
	if mysql.ParseSQLModeFlags(sqlMode).Has(mysql.SQLModeNoBackslashEscapes) {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'"
	}
	return formatStrLit(s)
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

// formatDefaultExpr escapes literal defaults for the generated CREATE TABLE
// statement. Non-literal defaults already contain SQL syntax in OriginString,
// so escaping their quotes as string contents would corrupt the expression.
func formatDefaultExpr(expr string, defaultExpr *plan.Expr) string {
	trimmed := strings.TrimSpace(expr)
	if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
		return trimmed
	}
	if defaultExpr != nil && defaultExpr.GetLit() == nil {
		return expr
	}
	return formatStr(expr)
}

func getTimeStampByTsHint(ctx CompilerContext, AtTsExpr *tree.AtTimeStamp) (snapshot *plan.Snapshot, err error) {
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	return builder.ResolveTsHint(AtTsExpr)
}
