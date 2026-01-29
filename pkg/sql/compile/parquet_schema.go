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

package compile

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// deleteParquetSchemaForTable deletes all parquet schema entries for a table
func deleteParquetSchemaForTable(c *Compile, dbName, tableName string) error {
	sql := fmt.Sprintf(
		`DELETE FROM %s.%s WHERE database_name = '%s' AND table_name = '%s'`,
		catalog.MO_CATALOG, catalog.MO_PARQUET_SCHEMA,
		strings.ReplaceAll(dbName, "'", "''"),
		strings.ReplaceAll(tableName, "'", "''"),
	)
	return c.runSqlWithOptions(sql, executor.StatementOption{}.WithDisableLog())
}

// updateParquetSchemaTableName updates table name in parquet schema entries
func updateParquetSchemaTableName(c *Compile, dbName, oldTableName, newTableName string) error {
	sql := fmt.Sprintf(
		`UPDATE %s.%s SET table_name = '%s' WHERE database_name = '%s' AND table_name = '%s'`,
		catalog.MO_CATALOG, catalog.MO_PARQUET_SCHEMA,
		strings.ReplaceAll(newTableName, "'", "''"),
		strings.ReplaceAll(dbName, "'", "''"),
		strings.ReplaceAll(oldTableName, "'", "''"),
	)
	return c.runSqlWithOptions(sql, executor.StatementOption{}.WithDisableLog())
}

// updateParquetSchemaColumnName updates column name in parquet schema entries
func updateParquetSchemaColumnName(c *Compile, dbName, tableName, oldColName, newColName string) error {
	sql := fmt.Sprintf(
		`UPDATE %s.%s SET column_name = '%s' WHERE database_name = '%s' AND table_name = '%s' AND column_name = '%s'`,
		catalog.MO_CATALOG, catalog.MO_PARQUET_SCHEMA,
		strings.ReplaceAll(newColName, "'", "''"),
		strings.ReplaceAll(dbName, "'", "''"),
		strings.ReplaceAll(tableName, "'", "''"),
		strings.ReplaceAll(oldColName, "'", "''"),
	)
	return c.runSqlWithOptions(sql, executor.StatementOption{}.WithDisableLog())
}
