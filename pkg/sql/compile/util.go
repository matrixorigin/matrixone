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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	INDEX_TYPE_PRIMARY  = "PRIMARY"
	INDEX_TYPE_UNIQUE   = "UNIQUE"
	INDEX_TYPE_MULTIPLE = "MULTIPLE"
)

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
	createIndexTableForamt                      = "create table `%s`.`%s` (%s);"
)

var (
	deleteMoIndexesWithDatabaseIdFormat          = `delete from mo_catalog.mo_indexes where database_id = %v;`
	deleteMoIndexesWithTableIdFormat             = `delete from mo_catalog.mo_indexes where table_id = %v;`
	deleteMoIndexesWithTableIdAndIndexNameFormat = `delete from mo_catalog.mo_indexes where table_id = %v and name = '%s';`
	updateMoIndexesVisibleFormat                 = `update mo_catalog.mo_indexes set is_visible = %v where table_id = %v and name = '%s';`
	updateMoIndexesTruncateTableFormat           = `update mo_catalog.mo_indexes set table_id = %v where table_id = %v`
	updateMoIndexesAlgoParams                    = `update mo_catalog.mo_indexes set algo_params = '%s' where table_id = %v and name = '%s';`
)

var (
	deleteMoTablePartitionsWithDatabaseIdFormat = `delete from mo_catalog.mo_table_partitions where database_id = %v;`
	deleteMoTablePartitionsWithTableIdFormat    = `delete from mo_catalog.mo_table_partitions where table_id = %v;`
	//deleteMoTablePartitionsWithTableIdAndIndexNameFormat = `delete from mo_catalog.mo_table_partitions where table_id = %v and name = '%s';`
)

// genCreateIndexTableSql: Generate ddl statements for creating index table
func genCreateIndexTableSql(indexTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string) string {
	var sql string
	planCols := indexTableDef.GetCols()
	for i, planCol := range planCols {
		if i >= 1 {
			sql += ","
		}
		sql += planCol.Name + " "
		typeId := types.T(planCol.Typ.Id)
		switch typeId {
		case types.T_bit:
			sql += fmt.Sprintf("BIT(%d)", planCol.Typ.Width)
		case types.T_char:
			sql += fmt.Sprintf("CHAR(%d)", planCol.Typ.Width)
		case types.T_varchar:
			sql += fmt.Sprintf("VARCHAR(%d)", planCol.Typ.Width)
		case types.T_binary:
			sql += fmt.Sprintf("BINARY(%d)", planCol.Typ.Width)
		case types.T_varbinary:
			sql += fmt.Sprintf("VARBINARY(%d)", planCol.Typ.Width)
		case types.T_decimal64:
			sql += fmt.Sprintf("DECIMAL(%d,%d)", planCol.Typ.Width, planCol.Typ.Scale)
		case types.T_decimal128:
			sql += fmt.Sprintf("DECIMAL(%d,%d)", planCol.Typ.Width, planCol.Typ.Scale)
		default:
			sql += typeId.String()
		}
		if i == 0 {
			sql += " primary key"
		}
	}
	return fmt.Sprintf(createIndexTableForamt, DBName, indexDef.IndexTableName, sql)
}

// genCreateIndexTableSqlForIvfIndex: Generate ddl statements for creating ivf index table
// NOTE: Here the columns are part of meta, centroids, entries table.
// meta      -> key varchar(65535), value varchar(65535)
// centroids -> version int64, centroid_id int64, centroid vecf32(xx)
// entries   -> version int64, entry_id int64, pk xx
// TODO: later on merge with genCreateIndexTableSql
func genCreateIndexTableSqlForIvfIndex(indexTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string) string {
	var sql string
	planCols := indexTableDef.GetCols()
	for i, planCol := range planCols {
		if planCol.Name == catalog.CPrimaryKeyColName {
			continue
		}
		if i >= 1 {
			sql += ","
		}
		sql += "`" + planCol.Name + "`" + " "
		typeId := types.T(planCol.Typ.Id)
		switch typeId {
		case types.T_char:
			sql += fmt.Sprintf("CHAR(%d)", planCol.Typ.Width)
		case types.T_varchar:
			sql += fmt.Sprintf("VARCHAR(%d)", planCol.Typ.Width)
		case types.T_binary:
			sql += fmt.Sprintf("BINARY(%d)", planCol.Typ.Width)
		case types.T_varbinary:
			sql += fmt.Sprintf("VARBINARY(%d)", planCol.Typ.Width)
		case types.T_decimal64:
			sql += fmt.Sprintf("DECIMAL(%d,%d)", planCol.Typ.Width, planCol.Typ.Scale)
		case types.T_decimal128:
			sql += fmt.Sprintf("DECIMAL(%d,%d)", planCol.Typ.Width, planCol.Typ.Scale)
		case types.T_array_float32:
			sql += fmt.Sprintf("VECF32(%d)", planCol.Typ.Width)
		case types.T_array_float64:
			sql += fmt.Sprintf("VECF64(%d)", planCol.Typ.Width)
		default:
			sql += typeId.String()
		}

	}

	if indexTableDef.Pkey != nil && indexTableDef.Pkey.Names != nil {
		pkStr := fmt.Sprintf(", primary key ( %s ) ", partsToColsStr(indexTableDef.Pkey.Names))
		sql += pkStr
	}

	return fmt.Sprintf(createIndexTableForamt, DBName, indexDef.IndexTableName, sql)
}

// genInsertIndexTableSql: Generate an insert statement for inserting data into the index table
func genInsertIndexTableSql(originTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string, isUnique bool) string {
	// insert data into index table
	var insertSQL string
	temp := partsToColsStr(indexDef.Parts)
	if len(originTableDef.Pkey.PkeyColName) == 0 {
		if len(indexDef.Parts) == 1 {
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
					pKeyMsg += part
				} else {
					pKeyMsg += "," + part
				}
			}
			pKeyMsg += ")"
		} else {
			pKeyMsg = pkeyName
		}
		if len(indexDef.Parts) == 1 {
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
				pKeyMsg += part
			} else {
				pKeyMsg += "," + part
			}
		}
		pKeyMsg += ")"
	} else {
		pKeyMsg = pkeyName
	}

	colSeqNumMap := make(map[string]string)
	for _, col := range originTableDef.Cols {
		// NOTE:
		// ColDef.ColId is not used as "after alter table, different columns may have the same colId"
		// ColDef.SeqNum is used instead as it is always unique.
		colSeqNumMap[col.GetName()] = fmt.Sprintf("%d", col.GetSeqnum())
	}

	for i, part := range indexDef.Parts {
		insertSQLs[i] = fmt.Sprintf(insertIntoMasterIndexTableFormat,
			DBName, indexDef.IndexTableName,
			colSeqNumMap[part], part, pKeyMsg, pKeyMsg, DBName, originTableDef.Name)
	}

	return insertSQLs
}

// genInsertMOIndexesSql: Generate an insert statement for insert index metadata into `mo_catalog.mo_indexes`
func genInsertMOIndexesSql(eg engine.Engine, proc *process.Process, databaseId string, tableId uint64, ct *engine.ConstraintDef) (string, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, 1024))
	buffer.WriteString("insert into mo_catalog.mo_indexes values")

	isFirst := true
	for _, constraint := range ct.Cts {
		switch def := constraint.(type) {
		case *engine.IndexDef:
			for _, indexdef := range def.Indexes {
				ctx, cancelFunc := context.WithTimeout(proc.Ctx, time.Second*30)
				index_id, err := eg.AllocateIDByKey(ctx, ALLOCID_INDEX_KEY)
				cancelFunc()
				if err != nil {
					return "", err
				}

				for i, part := range indexdef.Parts {
					// NOTE: Don't resolve the alias here.
					// If we resolve it here, it will insert "OriginalPKColumnName" into the "mo_catalog.mo_indexes" table instead of
					// "AliasPKColumnName". This will result is issues filter "Programmatically added PK" from the output of
					// "show indexes" and "show create table" command.

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
					fmt.Fprintf(buffer, "'%s', ", indexdef.IndexName)

					// 5. index_type
					var index_type string
					if indexdef.Unique {
						index_type = INDEX_TYPE_UNIQUE
					} else {
						index_type = INDEX_TYPE_MULTIPLE
					}
					fmt.Fprintf(buffer, "'%s', ", index_type)

					//6. algorithm
					var algorithm = indexdef.IndexAlgo
					fmt.Fprintf(buffer, "'%s', ", algorithm)

					//7. algorithm_table_type
					var algorithm_table_type = indexdef.IndexAlgoTableType
					fmt.Fprintf(buffer, "'%s', ", algorithm_table_type)

					//8. algorithm_params
					var algorithm_params = indexdef.IndexAlgoParams
					fmt.Fprintf(buffer, "'%s', ", algorithm_params)

					// 9. index visible
					fmt.Fprintf(buffer, "%d, ", INDEX_VISIBLE_YES)

					// 10. index vec_hidden
					fmt.Fprintf(buffer, "%d, ", INDEX_HIDDEN_NO)

					// 11. index vec_comment
					fmt.Fprintf(buffer, "'%s', ", indexdef.Comment)

					// 12. index vec_column_name
					fmt.Fprintf(buffer, "'%s', ", part)

					// 13. index vec_ordinal_position
					fmt.Fprintf(buffer, "%d, ", i+1)

					// 14. index vec_options
					fmt.Fprintf(buffer, "%s, ", NULL_VALUE)

					// 15. index vec_index_table
					if indexdef.TableExist {
						fmt.Fprintf(buffer, "'%s')", indexdef.IndexTableName)
					} else {
						fmt.Fprintf(buffer, "%s)", NULL_VALUE)
					}
				}
			}
		case *engine.PrimaryKeyDef:
			ctx, cancelFunc := context.WithTimeout(proc.Ctx, time.Second*30)
			index_id, err := eg.AllocateIDByKey(ctx, ALLOCID_INDEX_KEY)
			cancelFunc()
			if err != nil {
				return "", err
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
					fmt.Fprintf(buffer, "'%s', ", colName)

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
func makeInsertSingleIndexSQL(eg engine.Engine, proc *process.Process, databaseId string, tableId uint64, idxdef *plan.IndexDef) (string, error) {
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
	insertMoIndexesSql, err := genInsertMOIndexesSql(eg, proc, databaseId, tableId, ct)
	if err != nil {
		return "", err
	}
	return insertMoIndexesSql, nil
}

func makeInsertTablePartitionsSQL(ctx context.Context, dbSource engine.Database, relation engine.Relation) (string, error) {
	if dbSource == nil || relation == nil {
		return "", nil
	}
	databaseId := dbSource.GetDatabaseId(ctx)
	tableId := relation.GetTableID(ctx)
	tableDefs, err := relation.TableDefs(ctx)
	if err != nil {
		return "", err
	}

	for _, def := range tableDefs {
		if partitionDef, ok := def.(*engine.PartitionDef); ok {
			partitionByDef := &plan2.PartitionByDef{}
			if err = partitionByDef.UnMarshalPartitionInfo(([]byte)(partitionDef.Partition)); err != nil {
				return "", nil
			}

			insertMoTablePartitionSql := genInsertMoTablePartitionsSql(databaseId, tableId, partitionByDef, partitionByDef.Partitions)
			return insertMoTablePartitionSql, nil
		}
	}
	return "", nil
}

// makeInsertMultiIndexSQL :Synchronize the index metadata information of the table to the index metadata table
func makeInsertMultiIndexSQL(eg engine.Engine, ctx context.Context, proc *process.Process, dbSource engine.Database, relation engine.Relation) (string, error) {
	if dbSource == nil || relation == nil {
		return "", nil
	}
	databaseId := dbSource.GetDatabaseId(ctx)
	tableId := relation.GetTableID(ctx)

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

	insertMoIndexesSql, err := genInsertMOIndexesSql(eg, proc, databaseId, tableId, ct)
	if err != nil {
		return "", err
	}
	return insertMoIndexesSql, nil
}

func partsToColsStr(parts []string) string {
	var temp string
	for i, part := range parts {
		part = catalog.ResolveAlias(part)
		if i == 0 {
			temp += part
		} else {
			temp += "," + part
		}
	}
	return temp
}

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

// genInsertMoTablePartitionsSql: Generate an insert statement for insert index metadata into `mo_catalog.mo_table_partitions`
func genInsertMoTablePartitionsSql(databaseId string, tableId uint64, partitionByDef *plan2.PartitionByDef, partitions []*plan.PartitionItem) string {
	buffer := bytes.NewBuffer(make([]byte, 0, 2048))
	buffer.WriteString("insert into mo_catalog.mo_table_partitions values")

	isFirst := true
	for _, partition := range partitions {
		// 1. tableId
		if isFirst {
			fmt.Fprintf(buffer, "(%d, ", tableId)
			isFirst = false
		} else {
			fmt.Fprintf(buffer, ", (%d, ", tableId)
		}

		// 2. database_id
		fmt.Fprintf(buffer, "%s, ", databaseId)

		// 3. partition number
		fmt.Fprintf(buffer, "%d, ", partition.OrdinalPosition)

		// 4. partition name
		fmt.Fprintf(buffer, "'%s', ", partition.PartitionName)

		// 5. partition type
		fmt.Fprintf(buffer, "'%s', ", partitionByDef.Type.String())

		// 6. partition expression
		fmt.Fprintf(buffer, "'%s', ", partitionByDef.GenPartitionExprString())

		// 7. description_utf8
		fmt.Fprintf(buffer, "'%s', ", partition.Description)

		// 8. partition item comment
		fmt.Fprintf(buffer, "'%s', ", partition.Comment)

		// 9. partition item options
		fmt.Fprintf(buffer, "%s, ", NULL_VALUE)

		// 10. partition_table_name
		fmt.Fprintf(buffer, "'%s')", partition.PartitionTableName)
	}
	buffer.WriteString(";")
	return buffer.String()
}

func GetConstraintDef(ctx context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
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
