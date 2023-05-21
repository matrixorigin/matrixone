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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// genCreateIndexTableSql: Generate ddl statements for creating index table
func genCreateIndexTableSql(indexTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string) string {
	var sql string
	planCols := indexTableDef.GetCols()
	for i, planCol := range planCols {
		if i == 1 {
			sql += ","
		}
		sql += planCol.Name + " "
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
			sql += fmt.Sprintf("DECIAML(%d,%d)", planCol.Typ.Width, planCol.Typ.Scale)
		default:
			sql += typeId.String()
		}
		if i == 0 {
			sql += " primary key"
		}
	}
	return fmt.Sprintf(createIndexTableForamt, DBName, indexDef.IndexTableName, sql)
}

// genInsertIndexTableSql: Generate an insert statement for inserting data into the index table
func genInsertIndexTableSql(originTableDef *plan.TableDef, indexDef *plan.IndexDef, DBName string) string {
	// insert data into index table
	var insertSQL string
	temp := partsToColsStr(indexDef.Parts)
	if originTableDef.Pkey == nil || len(originTableDef.Pkey.PkeyColName) == 0 {
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
			insertSQL = fmt.Sprintf(insertIntoIndexTableWithPKeyFormat, DBName, indexDef.IndexTableName, temp, pKeyMsg, DBName, originTableDef.Name, temp)
		}
	}
	return insertSQL
}

func genNewUniqueIndexDuplicateCheck(c *Compile, database, table, cols string) error {
	duplicateCheckSql := fmt.Sprintf(selectOriginTableConstraintFormat, cols, database, table, cols, cols)
	fill := func(_ any, b *batch.Batch) error {
		if b == nil || b.Length() == 0 {
			return nil
		}
		t, err := types.Unpack(b.Vecs[0].GetBytesAt(0))
		if err != nil {
			return err
		}
		return moerr.NewDuplicateEntry(c.ctx, t.ErrString(), cols)
	}

	return c.runSql(duplicateCheckSql, fill)
}

func partsToColsStr(parts []string) string {
	var temp string
	for i, part := range parts {
		if i == 0 {
			temp += part
		} else {
			temp += "," + part
		}
	}
	return temp
}
