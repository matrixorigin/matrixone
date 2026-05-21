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

package compile

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// resolveVariableOrDefault wraps proc.GetResolveVariableFunc() with a
// nil-safe fallback to executor.DefaultResolveVariable (populated from
// gSysVarsDefs by pkg/frontend's init). When proc has a session-bound
// resolver (normal frontend path) that resolver is used; when
// proc.ResolveVariableFunc is nil (background paths that came in
// without a frontend session) the fallback returns the variable's
// compile-time default rather than panicking on a nil function call.
//
// Last-resort: when neither the proc resolver nor the executor
// fallback is available (tests that construct a bare Process and
// don't blank-import pkg/frontend) returns an error rather than panic.
func resolveVariableOrDefault(proc *process.Process, name string, isSystemVar, isGlobalVar bool) (any, error) {
	if resolver := proc.GetResolveVariableFunc(); resolver != nil {
		return resolver(name, isSystemVar, isGlobalVar)
	}
	if executor.DefaultResolveVariable != nil {
		return executor.DefaultResolveVariable(name, isSystemVar, isGlobalVar)
	}
	return nil, moerr.NewInternalErrorNoCtxf(
		"resolveVariableOrDefault: no resolver available for %q (proc resolver and executor.DefaultResolveVariable both nil)", name)
}

const (
	INDEX_TYPE_PRIMARY  = "PRIMARY"
	INDEX_TYPE_UNIQUE   = "UNIQUE"
	INDEX_TYPE_MULTIPLE = "MULTIPLE"
	INDEX_TYPE_FULLTEXT = "FULLTEXT"
	INDEX_TYPE_SPATIAL  = "SPATIAL"
)

func indexMetadataType(unique bool, algo string) string {
	switch {
	case unique:
		return INDEX_TYPE_UNIQUE
	case catalog.IsRTreeIndexAlgo(algo):
		return INDEX_TYPE_SPATIAL
	case catalog.IsFullTextIndexAlgo(algo):
		return INDEX_TYPE_FULLTEXT
	default:
		return INDEX_TYPE_MULTIPLE
	}
}

const (
	INDEX_VISIBLE_YES = 1
	INDEX_VISIBLE_NO  = 0
)

const (
	INDEX_HIDDEN_YES = 1
	INDEX_HIDDEN_NO  = 0
)

const (
	NULL_VALUE   = "null"
	EMPTY_STRING = ""
)

const (
	ALLOCID_INDEX_KEY = "index_key"
)

var (
	needSkipDbs = map[string]bool{
		"mysql":              true,
		"system":             true,
		"system_metrics":     true,
		"mo_task":            true,
		"mo_debug":           true,
		"information_schema": true,
		catalog.MO_CATALOG:   true,
	}
)
var (
	// see the comment in fuzzyCheck func genCondition for the reason why has to be two SQLs
	fuzzyNonCompoundCheck = "select %s from `%s`.`%s` where %s in (%s) group by %s having count(*) > 1 limit 1;"
	fuzzyCompoundCheck    = "select serial(%s) from `%s`.`%s` where %s group by serial(%s) having count(*) > 1 limit 1;"
)

var (
	insertIntoSingleIndexTableWithPKeyFormat    = "insert into  `%s`.`%s` select (%s), %s from `%s`.`%s` where (%s) is not null;"
	insertIntoUniqueIndexTableWithPKeyFormat    = "insert into  `%s`.`%s` select serial(%s), %s from `%s`.`%s` where serial(%s) is not null;"
	insertIntoSecondaryIndexTableWithPKeyFormat = "insert into  `%s`.`%s` select serial_full(%s), %s from `%s`.`%s`;"
	insertIntoSingleIndexTableWithoutPKeyFormat = "insert into  `%s`.`%s` select (%s) from `%s`.`%s` where (%s) is not null;"
	insertIntoIndexTableWithoutPKeyFormat       = "insert into  `%s`.`%s` select serial(%s) from `%s`.`%s` where serial(%s) is not null;"
	insertIntoMasterIndexTableFormat            = "insert into  `%s`.`%s` select serial_full('%s', %s, %s), %s from `%s`.`%s`;"
)

var (
	deleteMoIndexesWithDatabaseIdFormat          = `delete from mo_catalog.mo_indexes where database_id = %v;`
	deleteMoIndexesWithTableIdFormat             = `delete from mo_catalog.mo_indexes where table_id = %v;`
	deleteMoIndexesWithTableIdAndIndexNameFormat = `delete from mo_catalog.mo_indexes where table_id = %v and name = '%s';`
	updateMoIndexesVisibleFormat                 = `update mo_catalog.mo_indexes set is_visible = %v where table_id = %v and name = '%s';`
	updateMoIndexesAlgoParams                    = `update mo_catalog.mo_indexes set algo_params = '%s' where table_id = %v and name = '%s';`
	updateMoMergeSettings                        = `update mo_catalog.mo_merge_settings set tid = %v where account_id = %v and tid = %v;`
)

var (
	dropTableBeforeDropDatabase = "drop table if exists `%v`.`%v`;"
)

// genInsertIndexTableSql: Generate an insert statement for inserting data into the index table
func genInsertIndexTableSql(originTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string, isUnique bool) string {
	// insert data into index table
	var insertSQL string
	temp := partsToColsStr(indexDef.Parts)
	spatialIndex := catalog.IsRTreeIndexAlgo(indexDef.IndexAlgo)
	if spatialIndex && len(indexDef.Parts) > 0 {
		temp = partsToColsStr(indexDef.Parts[:1])
	}
	if len(originTableDef.Pkey.PkeyColName) == 0 {
		if len(indexDef.Parts) == 1 || spatialIndex {
			insertSQL = fmt.Sprintf(insertIntoSingleIndexTableWithoutPKeyFormat, DBName, indexDef.IndexTableName, temp, DBName, originTableDef.Name, temp)
		} else {
			insertSQL = fmt.Sprintf(insertIntoIndexTableWithoutPKeyFormat, DBName, indexDef.IndexTableName, temp, DBName, originTableDef.Name, temp)
		}
	} else {
		pkeyName := originTableDef.Pkey.PkeyColName
		var pKeyMsg string
		if pkeyName == catalog.CPrimaryKeyColName {
			pKeyMsg = "serial("
			for i, part := range originTableDef.Pkey.Names {
				if i == 0 {
					pKeyMsg += quoteMySQLIdent(part)
				} else {
					pKeyMsg += "," + quoteMySQLIdent(part)
				}
			}
			pKeyMsg += ")"
		} else {
			pKeyMsg = quoteMySQLQualifiedIdent(pkeyName)
		}
		if len(indexDef.Parts) == 1 || spatialIndex {
			insertSQL = fmt.Sprintf(insertIntoSingleIndexTableWithPKeyFormat, DBName, indexDef.IndexTableName, temp, pKeyMsg, DBName, originTableDef.Name, temp)
		} else {
			if isUnique {
				insertSQL = fmt.Sprintf(insertIntoUniqueIndexTableWithPKeyFormat, DBName, indexDef.IndexTableName, temp, pKeyMsg, DBName, originTableDef.Name, temp)
			} else {
				insertSQL = fmt.Sprintf(insertIntoSecondaryIndexTableWithPKeyFormat, DBName, indexDef.IndexTableName, temp, pKeyMsg, DBName, originTableDef.Name)
			}
		}
	}
	return insertSQL
}

// genInsertIndexTableSqlForMasterIndex: Create inserts for master index table
func genInsertIndexTableSqlForMasterIndex(originTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string) []string {
	// insert data into index table
	var insertSQLs = make([]string, len(indexDef.Parts))

	pkeyName := originTableDef.Pkey.PkeyColName
	var pKeyMsg string
	if pkeyName == catalog.CPrimaryKeyColName {
		pKeyMsg = "serial("
		for i, part := range originTableDef.Pkey.Names {
			if i == 0 {
				pKeyMsg += quoteMySQLIdent(part)
			} else {
				pKeyMsg += "," + quoteMySQLIdent(part)
			}
		}
		pKeyMsg += ")"
	} else {
		pKeyMsg = quoteMySQLQualifiedIdent(pkeyName)
	}

	colSeqNumMap := make(map[string]string)
	for _, col := range originTableDef.Cols {
		// NOTE:
		// ColDef.ColId is not used as "after alter table, different columns may have the same colId"
		// ColDef.SeqNum is used instead as it is always unique.
		colSeqNumMap[col.GetName()] = fmt.Sprintf("%d", col.GetSeqnum())
	}

	for i, part := range indexDef.Parts {
		resolvedPart := catalog.ResolveAlias(part)
		insertSQLs[i] = fmt.Sprintf(insertIntoMasterIndexTableFormat,
			DBName, indexDef.IndexTableName,
			colSeqNumMap[resolvedPart], quoteMySQLQualifiedIdent(resolvedPart), pKeyMsg, pKeyMsg, DBName, originTableDef.Name)
	}

	return insertSQLs
}

// genInsertMOIndexesSql: Generate an insert statement for insert index metadata into `mo_catalog.mo_indexes`
func genInsertMOIndexesSql(eg engine.Engine, proc *process.Process, databaseId string, tableId uint64, ct *engine.ConstraintDef, tableDef *plan.TableDef) (string, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 1024))
	buffer.WriteString("insert into mo_catalog.mo_indexes values")

	getOriginName := func(name string) string {
		if idx, ok := tableDef.Name2ColIndex[name]; ok {
			return tableDef.Cols[idx].OriginName
		}
		return name
	}

	isFirst := true
	for _, constraint := range ct.Cts {
		switch def := constraint.(type) {
		case *engine.IndexDef:
			for _, indexDef := range def.Indexes {
				ctx, cancelFunc := context.WithTimeoutCause(proc.Ctx, time.Second*30, moerr.CauseGenInsertMOIndexesSql)
				indexId, err := eg.AllocateIDByKey(ctx, ALLOCID_INDEX_KEY)
				cancelFunc()
				if err != nil {
					return "", moerr.AttachCause(ctx, err)
				}

				for i, part := range indexDef.Parts {
					// NOTE: Don't resolve the alias here.
					// If we resolve it here, it will insert "OriginalPKColumnName" into the "mo_catalog.mo_indexes" table instead of
					// "AliasPKColumnName". This will result is issues filter "Programmatically added PK" from the output of
					// "show indexes" and "show create table" command.

					//1. index id
					if isFirst {
						fmt.Fprintf(buffer, "(%d, ", indexId)
						isFirst = false
					} else {
						fmt.Fprintf(buffer, ", (%d, ", indexId)
					}

					//2. table_id
					fmt.Fprintf(buffer, "%d, ", tableId)

					// 3. databaseId
					fmt.Fprintf(buffer, "%s, ", databaseId)

					// 4.index.IndexName
					fmt.Fprintf(buffer, "'%s', ", indexDef.IndexName)

					// 5. index_type
					index_type := indexMetadataType(indexDef.Unique, indexDef.IndexAlgo)
					fmt.Fprintf(buffer, "'%s', ", index_type)

					//6. algorithm
					var algorithm = indexDef.IndexAlgo
					fmt.Fprintf(buffer, "'%s', ", algorithm)

					//7. algorithm_table_type
					var algorithm_table_type = indexDef.IndexAlgoTableType
					fmt.Fprintf(buffer, "'%s', ", algorithm_table_type)

					//8. algorithm_params
					var algorithm_params = indexDef.IndexAlgoParams
					fmt.Fprintf(buffer, "'%s', ", algorithm_params)

					// 9. index visible
					fmt.Fprintf(buffer, "%d, ", INDEX_VISIBLE_YES)

					// 10. index vec_hidden
					fmt.Fprintf(buffer, "%d, ", INDEX_HIDDEN_NO)

					// 11. index vec_comment
					fmt.Fprintf(buffer, "'%s', ", indexDef.Comment)

					// 12. index vec_column_name
					fmt.Fprintf(buffer, "'%s', ", getOriginName(part))

					// 13. index vec_ordinal_position
					fmt.Fprintf(buffer, "%d, ", i+1)

					// 14. index vec_options
					if indexDef.Option != nil {
						if indexDef.Option.ParserName != "" {
							fmt.Fprintf(buffer, "'parser=%s,ngram_token_size=%d', ", indexDef.Option.ParserName, indexDef.Option.NgramTokenSize)
						}
					} else {
						fmt.Fprintf(buffer, "%s, ", NULL_VALUE)
					}

					// 15. index vec_index_table
					if indexDef.TableExist {
						fmt.Fprintf(buffer, "'%s')", indexDef.IndexTableName)
					} else {
						fmt.Fprintf(buffer, "%s)", NULL_VALUE)
					}
				}
			}
		case *engine.PrimaryKeyDef:
			ctx, cancelFunc := context.WithTimeoutCause(proc.Ctx, time.Second*30, moerr.CauseGenInsertMOIndexesSql2)
			index_id, err := eg.AllocateIDByKey(ctx, ALLOCID_INDEX_KEY)
			cancelFunc()
			if err != nil {
				return "", moerr.AttachCause(ctx, err)
			}
			if def.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				for i, colName := range def.Pkey.Names {
					//1. index id
					if isFirst {
						fmt.Fprintf(buffer, "(%d, ", index_id)
						isFirst = false
					} else {
						fmt.Fprintf(buffer, ", (%d, ", index_id)
					}

					//2. table_id
					fmt.Fprintf(buffer, "%d, ", tableId)

					// 3. databaseId
					fmt.Fprintf(buffer, "%s, ", databaseId)

					// 4.index.IndexName
					fmt.Fprintf(buffer, "'%s', ", "PRIMARY")

					// 5.index_type
					fmt.Fprintf(buffer, "'%s', ", INDEX_TYPE_PRIMARY)

					//6. algorithm
					fmt.Fprintf(buffer, "'%s', ", EMPTY_STRING)

					//7. algorithm_table_type
					fmt.Fprintf(buffer, "'%s', ", EMPTY_STRING)

					//8. algorithm_params
					fmt.Fprintf(buffer, "'%s', ", EMPTY_STRING)

					//9. index visible
					fmt.Fprintf(buffer, "%d, ", INDEX_VISIBLE_YES)

					// 10. index vec_hidden
					fmt.Fprintf(buffer, "%d, ", INDEX_HIDDEN_NO)

					// 11. index vec_comment
					fmt.Fprintf(buffer, "'%s', ", EMPTY_STRING)

					// 12. index vec_column_name
					fmt.Fprintf(buffer, "'%s', ", getOriginName(colName))

					// 13. index vec_ordinal_position
					fmt.Fprintf(buffer, "%d, ", i+1)

					// 14. index vec_options
					fmt.Fprintf(buffer, "%s, ", NULL_VALUE)

					// 15. index vec_index_table
					fmt.Fprintf(buffer, "%s)", NULL_VALUE)
				}
			}
		}
	}
	buffer.WriteString(";")
	return buffer.String(), nil
}

// makeInsertSingleIndexSQL: make index metadata information sql for a single index object
func makeInsertSingleIndexSQL(eg engine.Engine, proc *process.Process, databaseId string, tableId uint64, idxdef *plan.IndexDef, tableDef *plan.TableDef) (string, error) {
	if idxdef == nil {
		return "", nil
	}
	ct := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.IndexDef{
				Indexes: []*plan.IndexDef{idxdef},
			},
		},
	}
	insertMoIndexesSql, err := genInsertMOIndexesSql(eg, proc, databaseId, tableId, ct, tableDef)
	if err != nil {
		return "", err
	}
	return insertMoIndexesSql, nil
}

// makeInsertMultiIndexSQL :Synchronize the index metadata information of the table to the index metadata table
func makeInsertMultiIndexSQL(eg engine.Engine, ctx context.Context, proc *process.Process, dbSource engine.Database, relation engine.Relation) (string, error) {
	if dbSource == nil || relation == nil {
		return "", nil
	}
	databaseId := dbSource.GetDatabaseId(ctx)
	tableId := relation.GetTableID(ctx)
	tableDef := relation.GetTableDef(ctx)

	ct, err := GetConstraintDef(ctx, relation)
	if err != nil {
		return "", err
	}
	if ct == nil {
		return "", nil
	}

	hasIndex := false
	for _, constraint := range ct.Cts {
		if idxdef, ok := constraint.(*engine.IndexDef); ok && len(idxdef.Indexes) > 0 {
			hasIndex = true
			break
		}
		if pkdef, ok := constraint.(*engine.PrimaryKeyDef); ok {
			if pkdef.Pkey.PkeyColName != catalog.FakePrimaryKeyColName {
				hasIndex = true
				break
			}
		}
	}
	if !hasIndex {
		return "", nil
	}

	insertMoIndexesSql, err := genInsertMOIndexesSql(eg, proc, databaseId, tableId, ct, tableDef)
	if err != nil {
		return "", err
	}
	return insertMoIndexesSql, nil
}

func (s *Scope) checkTableWithValidIndexes(c *Compile, relation engine.Relation) error {
	if relation == nil {
		return nil
	}

	ct, err := GetConstraintDef(c.proc.Ctx, relation)
	if err != nil {
		return err
	}
	if ct == nil {
		return nil
	}

	for _, constraint := range ct.Cts {
		if idxdef, ok := constraint.(*engine.IndexDef); ok && len(idxdef.Indexes) > 0 {
			for _, idx := range idxdef.Indexes {
				if idx.TableExist {
					// Plugin-registered vector indexes contribute their
					// experimental flag (if any) via
					// catalog.Hooks.ExperimentalFlag(). Today only HNSW
					// returns a non-empty flag at this seam; CAGRA and
					// IVF-PQ have flags defined but not enforced here.
					if p, ok := indexplugin.Get(idx.IndexAlgo); ok {
						if flag := p.Catalog().ExperimentalFlag(); flag != "" {
							if ok2, err := s.isExperimentalEnabled(c, flag); err != nil {
								return err
							} else if !ok2 {
								return moerr.NewInternalError(c.proc.Ctx, fmt.Sprintf("%s is not enabled", flag))
							}
						}
					}
				}
			}

			break
		}
	}

	return nil
}

func partsToColsStr(parts []string) string {
	var temp string
	for i, part := range parts {
		part = catalog.ResolveAlias(part)
		part = quoteMySQLQualifiedIdent(part)
		if i == 0 {
			temp += part
		} else {
			temp += "," + part
		}
	}
	return temp
}

func quoteMySQLIdent(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func quoteMySQLQualifiedIdent(ident string) string {
	parts := strings.Split(ident, ".")
	for i := range parts {
		parts[i] = quoteMySQLIdent(parts[i])
	}
	return strings.Join(parts, ".")
}

// haveSinkScanInPlan Start from the `curNodeIdx` node, recursively check its Subtree all nodes,
// determine if they contain `SINK_SCAN` node in the subtree
func haveSinkScanInPlan(nodes []*plan.Node, curNodeIdx int32) bool {
	node := nodes[curNodeIdx]
	if node.NodeType == plan.Node_SINK_SCAN {
		return true
	}
	for _, newIdx := range node.Children {
		flag := haveSinkScanInPlan(nodes, newIdx)
		if flag {
			return flag
		}
	}
	return false
}

var GetConstraintDef = func(ctx context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
	defs, err := rel.TableDefs(ctx)
	if err != nil {
		return nil, err
	}

	return GetConstraintDefFromTableDefs(defs), nil
}

func GetConstraintDefFromTableDefs(defs []engine.TableDef) *engine.ConstraintDef {
	var cstrDef *engine.ConstraintDef
	for _, def := range defs {
		if ct, ok := def.(*engine.ConstraintDef); ok {
			cstrDef = ct
			break
		}
	}
	if cstrDef == nil {
		cstrDef = &engine.ConstraintDef{}
		cstrDef.Cts = make([]engine.Constraint, 0)
	}
	return cstrDef
}

// filterColumnsFromParams reads the comma-joined "included_columns" entry
// stashed in the index algo-params JSON and returns ", src.col1, src.col2, …"
// — a suffix suitable for appending to the positional arg list of
// cagra_create / ivfpq_create. Returns "" when the index has no INCLUDE
// columns or the key is absent.
func filterColumnsFromParams(indexAlgoParams, srcAlias string) string {
	if len(indexAlgoParams) == 0 {
		return ""
	}
	val, err := sonic.Get([]byte(indexAlgoParams), catalog.IncludedColumns)
	if err != nil {
		return ""
	}
	joined, err := val.StrictString()
	if err != nil || len(joined) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, name := range strings.Split(joined, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		sb.WriteString(", ")
		sb.WriteString(srcAlias)
		sb.WriteByte('.')
		sb.WriteString(name)
	}
	return sb.String()
}
