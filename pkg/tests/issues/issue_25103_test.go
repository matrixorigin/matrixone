// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue25103InformationSchemaMetadata covers the metadata consumed by
// schema migration tools and Connector/J's getImportedKeys implementation.
func TestIssue25103InformationSchemaMetadata(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		dbName := testutils.GetDatabaseName(t)
		schemaName := strings.ToLower(dbName)
		execSQLRequire(t, ctx, db, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, db, "drop database if exists `"+dbName+"`")

		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.parent (id int primary key)")
		execSQLRequire(t, ctx, db, "create table `"+dbName+"`.child ("+
			"id bigint unsigned primary key, pid int, "+
			"c_dec decimal(20, 6), c_char char(8), c_varchar varchar(32), "+
			"c_float8 float(8), c_float25 float(25), c_float82 float(8, 2), "+
			"c_time time(6), c_datetime datetime(3), c_timestamp timestamp(6), "+
			"constraint fk_parent foreign key (pid) references `"+dbName+"`.parent(id))")

		rows, err := db.QueryContext(ctx, `
select column_name, character_maximum_length, numeric_precision, numeric_scale, datetime_precision
from information_schema.columns
where table_schema = ? and table_name = 'child'
  and column_name in ('id', 'pid', 'c_dec', 'c_char', 'c_varchar', 'c_float8', 'c_float25', 'c_float82', 'c_time', 'c_datetime', 'c_timestamp')
order by ordinal_position`, schemaName)
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		metadata := make(map[string][4]sql.NullInt64)
		for rows.Next() {
			var name string
			var characterLength, numericPrecision, numericScale, datetimePrecision sql.NullInt64
			require.NoError(t, rows.Scan(&name, &characterLength, &numericPrecision, &numericScale, &datetimePrecision))
			metadata[name] = [4]sql.NullInt64{characterLength, numericPrecision, numericScale, datetimePrecision}
		}
		require.NoError(t, rows.Err())
		type columnMetadata struct {
			values [4]int64
			valid  [4]bool
		}
		expect := map[string]columnMetadata{
			"id":          {values: [4]int64{0, 20, 0, 0}, valid: [4]bool{false, true, true, false}},
			"pid":         {values: [4]int64{0, 10, 0, 0}, valid: [4]bool{false, true, true, false}},
			"c_dec":       {values: [4]int64{0, 20, 6, 0}, valid: [4]bool{false, true, true, false}},
			"c_char":      {values: [4]int64{8, 0, 0, 0}, valid: [4]bool{true, false, false, false}},
			"c_varchar":   {values: [4]int64{32, 0, 0, 0}, valid: [4]bool{true, false, false, false}},
			"c_float8":    {values: [4]int64{0, 12, 0, 0}, valid: [4]bool{false, true, false, false}},
			"c_float25":   {values: [4]int64{0, 22, 0, 0}, valid: [4]bool{false, true, false, false}},
			"c_float82":   {values: [4]int64{0, 8, 2, 0}, valid: [4]bool{false, true, true, false}},
			"c_time":      {values: [4]int64{0, 0, 0, 6}, valid: [4]bool{false, false, false, true}},
			"c_datetime":  {values: [4]int64{0, 0, 0, 3}, valid: [4]bool{false, false, false, true}},
			"c_timestamp": {values: [4]int64{0, 0, 0, 6}, valid: [4]bool{false, false, false, true}},
		}
		for name, want := range expect {
			got, ok := metadata[name]
			require.True(t, ok, "missing information_schema.COLUMNS row for %s", name)
			for i := range want.values {
				require.Equal(t, want.valid[i], got[i].Valid, "%s metadata field %d NULL status", name, i)
				if want.valid[i] {
					require.Equal(t, want.values[i], got[i].Int64, "%s metadata field %d", name, i)
				}
			}
		}

		// This is the information_schema query used by Connector/J when
		// DatabaseMetaData.getImportedKeys is backed by INFORMATION_SCHEMA.
		var referencedSchema, referencedTable, referencedColumn string
		var tableSchema, tableName, columnName, constraintName, uniqueConstraintName string
		var ordinalPosition int64
		err = db.QueryRowContext(ctx, `
select distinct A.referenced_table_schema, A.referenced_table_name,
       A.referenced_column_name, A.table_schema, A.table_name,
       A.column_name, A.ordinal_position, A.constraint_name,
       R.unique_constraint_name
from information_schema.key_column_usage A
join information_schema.table_constraints B
  using (constraint_schema, constraint_name, table_name)
join information_schema.referential_constraints R
  on R.constraint_name = B.constraint_name
 and R.table_name = B.table_name
 and R.constraint_schema = B.constraint_schema
where B.constraint_type = 'FOREIGN KEY'
  and A.table_schema = ? and A.table_name = ?
  and A.referenced_table_schema is not null`, schemaName, "child").Scan(
			&referencedSchema, &referencedTable, &referencedColumn,
			&tableSchema, &tableName, &columnName, &ordinalPosition,
			&constraintName, &uniqueConstraintName)
		require.NoError(t, err)
		require.Equal(t, schemaName, referencedSchema)
		require.Equal(t, "parent", referencedTable)
		require.Equal(t, "id", referencedColumn)
		require.Equal(t, schemaName, tableSchema)
		require.Equal(t, "child", tableName)
		require.Equal(t, "pid", columnName)
		require.Equal(t, int64(1), ordinalPosition)
		require.Equal(t, "fk_parent", constraintName)
		require.Equal(t, "PRIMARY", uniqueConstraintName)
	})
}
