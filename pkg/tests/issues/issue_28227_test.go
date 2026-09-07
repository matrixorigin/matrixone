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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28227BitwiseAggregateBinaryOperandWidth(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
			cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		dbName := testutils.GetDatabaseName(t)
		tableName := fmt.Sprintf("`%s`.b512", dbName)
		execSQLRequire(t, ctx, db, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, db, "drop database if exists `"+dbName+"`")

		execSQLRequire(t, ctx, db, fmt.Sprintf(`create table %s(
			id int primary key,
			g int,
			v510 varbinary(510),
			v511 varbinary(511),
			v512 varbinary(512),
			v600 varbinary(600),
			vbytes varbinary(511))`, tableName))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"insert into %s values (1,1,unhex('00FF'),unhex('00FF'),unhex('00FF'),unhex('00FF'),unhex('E4B8AD')),(2,1,unhex('0F0F'),unhex('0F0F'),unhex('0F0F'),unhex('0F0F'),unhex('FF0001')),(3,2,null,null,null,null,null)",
			tableName))

		expectedAggregate := map[string]string{
			"bit_and": "000F",
			"bit_or":  "0FFF",
			"bit_xor": "0FF0",
		}
		expectedWindow := map[string][]string{
			"bit_and": {"00FF", "000F"},
			"bit_or":  {"00FF", "0FFF"},
			"bit_xor": {"00FF", "0FF0"},
		}
		uuidExpression := "uuid_to_bin('6ccd780c-baba-1026-9564-5b8c656024db')"

		for _, derived := range []struct {
			name       string
			expression string
			hexLength  int
		}{
			{name: "uuid_to_bin", expression: uuidExpression, hexLength: 32},
			{name: "inet6_aton", expression: "inet6_aton('2001:db8::1')", hexLength: 32},
			{name: "substring", expression: "substring(v512, 1, 511)", hexLength: 4},
			{name: "binary_and", expression: uuidExpression + " & " + uuidExpression, hexLength: 32},
			{name: "binary_or", expression: uuidExpression + " | " + uuidExpression, hexLength: 32},
			{name: "binary_xor", expression: uuidExpression + " ^ " + uuidExpression, hexLength: 32},
		} {
			t.Run("bounded "+derived.name, func(t *testing.T) {
				for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
					var plain string
					require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
						"select hex(%s(%s)) from %s where g=1",
						functionName, derived.expression, tableName)).Scan(&plain))
					require.Len(t, plain, derived.hexLength)

					var group int
					var grouped string
					require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
						"select g,hex(%s(%s)) from %s where g=1 group by g",
						functionName, derived.expression, tableName)).Scan(&group, &grouped))
					require.Equal(t, 1, group)
					require.Len(t, grouped, derived.hexLength)

					rows, err := db.QueryContext(ctx, fmt.Sprintf(
						"select id,hex(%s(%s) over (order by id)) from %s where id <= 2 order by id",
						functionName, derived.expression, tableName))
					require.NoError(t, err)
					defer rows.Close()
					var windowValues []string
					for rows.Next() {
						var id int
						var value string
						require.NoError(t, rows.Scan(&id, &value))
						require.Len(t, value, derived.hexLength)
						windowValues = append(windowValues, value)
					}
					require.NoError(t, rows.Err())
					require.Len(t, windowValues, 2)
				}
			})
		}

		t.Run("substring derived boundaries and byte semantics", func(t *testing.T) {
			expectedBytes := map[string]string{
				"bit_and": "E40001",
				"bit_or":  "FFB8AD",
				"bit_xor": "1BB8AC",
			}
			for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
				expression := fmt.Sprintf("%s(substring(vbytes, 1, 511))", functionName)
				queries := []string{
					fmt.Sprintf("select hex(%s) from %s where g=1", expression, tableName),
					fmt.Sprintf("select hex(%s(s)) from (select substring(vbytes, 1, 511) as s from %s where g=1) q", functionName, tableName),
					fmt.Sprintf("with q as (select substring(vbytes, 1, 511) as s from %s where g=1) select hex(%s(s)) from q", tableName, functionName),
				}
				for _, query := range queries {
					var got string
					require.NoError(t, db.QueryRowContext(ctx, query).Scan(&got), query)
					require.Equal(t, expectedBytes[functionName], got, query)
				}
			}

			viewName := fmt.Sprintf("`%s`.bitwise_28227_bytes", dbName)
			execSQLRequire(t, ctx, db, fmt.Sprintf(
				"create view %s as select substring(vbytes, 1, 511) as s from %s where g=1",
				viewName, tableName))
			defer execSQLMaybe(t, ctx, db, "drop view if exists "+viewName)
			for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
				var got string
				require.NoError(t, db.QueryRowContext(ctx,
					fmt.Sprintf("select hex(%s(s)) from %s", functionName, viewName)).Scan(&got))
				require.Equal(t, expectedBytes[functionName], got)
			}
		})

		for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
			t.Run(functionName, func(t *testing.T) {
				var got510, got511 string
				require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
					"select hex(%s(v510)),hex(%s(v511)) from %s",
					functionName, functionName, tableName)).Scan(&got510, &got511))
				require.Equal(t, expectedAggregate[functionName], got510)
				require.Equal(t, expectedAggregate[functionName], got511)

				var group int
				var grouped string
				require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
					"select g,hex(%s(v511)) from %s where g=1 group by g",
					functionName, tableName)).Scan(&group, &grouped))
				require.Equal(t, 1, group)
				require.Equal(t, expectedAggregate[functionName], grouped)

				rows, err := db.QueryContext(ctx, fmt.Sprintf(
					"select id,hex(%s(v511) over (order by id)) from %s where id <= 2 order by id",
					functionName, tableName))
				require.NoError(t, err)
				defer rows.Close()
				var windowValues []string
				for rows.Next() {
					var id int
					var value string
					require.NoError(t, rows.Scan(&id, &value))
					windowValues = append(windowValues, value)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, expectedWindow[functionName], windowValues)

				for _, column := range []string{"v512", "v600"} {
					for _, statement := range []string{
						fmt.Sprintf("select %s(%s) from %s", functionName, column, tableName),
						fmt.Sprintf("select g,%s(%s) from %s where g=1 group by g", functionName, column, tableName),
						fmt.Sprintf("select id,%s(%s) over (order by id) from %s where id <= 2 order by id", functionName, column, tableName),
					} {
						_, err := db.ExecContext(ctx, statement)
						require.Error(t, err, "%s must be rejected", statement)
						require.ErrorContains(t, err,
							"Aggregate bitwise functions cannot accept arguments longer than 511 bytes")
						var mysqlErr *mysql.MySQLError
						require.True(t, errors.As(err, &mysqlErr), "%T: %v", err, err)
						require.Equal(t, uint16(3514), mysqlErr.Number)
					}
				}

				for _, statement := range []string{
					fmt.Sprintf("select %s(substring(v512, 1, 512)) from %s where g=1", functionName, tableName),
					fmt.Sprintf("select g,%s(substring(v512, 1, 512)) from %s where g=1 group by g", functionName, tableName),
					fmt.Sprintf("select id,%s(substring(v512, 1, 512)) over (order by id) from %s where id <= 2 order by id", functionName, tableName),
					fmt.Sprintf("select %s(s) from (select substring(v512, 1, 512) as s from %s where g=1) q", functionName, tableName),
					fmt.Sprintf("with q as (select substring(v512, 1, 512) as s from %s where g=1) select %s(s) from q", tableName, functionName),
				} {
					_, err := db.ExecContext(ctx, statement)
					require.Error(t, err, "%s must be rejected", statement)
					require.ErrorContains(t, err,
						"Aggregate bitwise functions cannot accept arguments longer than 511 bytes")
					var mysqlErr *mysql.MySQLError
					require.True(t, errors.As(err, &mysqlErr), "%T: %v", err, err)
					require.Equal(t, uint16(3514), mysqlErr.Number)
				}

				viewName := fmt.Sprintf("`%s`.bitwise_28227_oversized_%s", dbName, functionName)
				execSQLRequire(t, ctx, db, fmt.Sprintf(
					"create view %s as select substring(v512, 1, 512) as s from %s where g=1",
					viewName, tableName))
				_, err := db.ExecContext(ctx, fmt.Sprintf("select %s(s) from %s", functionName, viewName))
				require.Error(t, err)
				require.ErrorContains(t, err,
					"Aggregate bitwise functions cannot accept arguments longer than 511 bytes")
				var mysqlErr *mysql.MySQLError
				require.True(t, errors.As(err, &mysqlErr), "%T: %v", err, err)
				require.Equal(t, uint16(3514), mysqlErr.Number)
				execSQLMaybe(t, ctx, db, "drop view if exists "+viewName)
			})
		}
	})
}
