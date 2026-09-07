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
			start_pos bigint,
			v510 varbinary(510),
			v511 varbinary(511),
			v512 varbinary(512),
			v600 varbinary(600),
			vbytes varbinary(511))`, tableName))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"insert into %s values (1,1,2,unhex('00FF'),unhex('00FF'),unhex('00FF'),unhex('00FF'),unhex('E4B8AD')),(2,1,2,unhex('0F0F'),unhex('0F0F'),unhex('0F0F'),unhex('0F0F'),unhex('FF0001')),(3,2,2,null,null,null,null,null)",
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
			{name: "substring_two_arg_start2", expression: "substring(v512, 2)", hexLength: 2},
			{name: "substring_three_arg_start2", expression: "substring(v512, 2, 512)", hexLength: 2},
			{name: "substring_zero_start", expression: "substring(v512, 0, 512)", hexLength: 0},
			{name: "substring_negative_start", expression: "substring(v512, -2)", hexLength: 4},
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
			for _, substringCase := range []struct {
				name       string
				expression string
				expected   map[string]string
			}{
				{
					name:       "three_arg_start1",
					expression: "substring(vbytes, 1, 511)",
					expected: map[string]string{
						"bit_and": "E40001",
						"bit_or":  "FFB8AD",
						"bit_xor": "1BB8AC",
					},
				},
				{
					name:       "two_arg_start2",
					expression: "substring(vbytes, 2)",
					expected: map[string]string{
						"bit_and": "0001",
						"bit_or":  "B8AD",
						"bit_xor": "B8AC",
					},
				},
				{
					name:       "three_arg_start2",
					expression: "substring(vbytes, 2, 511)",
					expected: map[string]string{
						"bit_and": "0001",
						"bit_or":  "B8AD",
						"bit_xor": "B8AC",
					},
				},
				{
					name:       "three_arg_zero_start",
					expression: "substring(vbytes, 0, 511)",
					expected: map[string]string{
						"bit_and": "",
						"bit_or":  "",
						"bit_xor": "",
					},
				},
				{
					name:       "two_arg_negative_start",
					expression: "substring(vbytes, -2)",
					expected: map[string]string{
						"bit_and": "0001",
						"bit_or":  "B8AD",
						"bit_xor": "B8AC",
					},
				},
			} {
				t.Run(substringCase.name, func(t *testing.T) {
					for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
						queries := []string{
							fmt.Sprintf("select hex(%s(%s)) from %s where g=1", functionName, substringCase.expression, tableName),
							fmt.Sprintf("select hex(%s(s)) from (select %s as s from %s where g=1) q", functionName, substringCase.expression, tableName),
							fmt.Sprintf("with q as (select %s as s from %s where g=1) select hex(%s(s)) from q", substringCase.expression, tableName, functionName),
						}
						for _, query := range queries {
							var got string
							require.NoError(t, db.QueryRowContext(ctx, query).Scan(&got), query)
							require.Equal(t, substringCase.expected[functionName], got, query)
						}
					}

					viewName := fmt.Sprintf("`%s`.bitwise_28227_bytes_%s", dbName, substringCase.name)
					execSQLRequire(t, ctx, db, fmt.Sprintf(
						"create view %s as select %s as s from %s where g=1",
						viewName, substringCase.expression, tableName))
					defer execSQLMaybe(t, ctx, db, "drop view if exists "+viewName)
					for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
						var got string
						require.NoError(t, db.QueryRowContext(ctx,
							fmt.Sprintf("select hex(%s(s)) from %s", functionName, viewName)).Scan(&got))
						require.Equal(t, substringCase.expected[functionName], got)
					}
				})
			}
		})

		t.Run("substring dynamic start with constant length", func(t *testing.T) {
			expression := "substring(v512, start_pos, 511)"
			expected := map[string]string{
				"bit_and": "0F",
				"bit_or":  "FF",
				"bit_xor": "F0",
			}
			expectedWindow := map[string][]string{
				"bit_and": {"FF", "0F"},
				"bit_or":  {"FF", "FF"},
				"bit_xor": {"FF", "F0"},
			}
			for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
				queries := []string{
					fmt.Sprintf("select hex(%s(%s)) from %s where g=1", functionName, expression, tableName),
					fmt.Sprintf("select hex(%s(s)) from (select %s as s from %s where g=1) q", functionName, expression, tableName),
					fmt.Sprintf("with q as (select %s as s from %s where g=1) select hex(%s(s)) from q", expression, tableName, functionName),
				}
				for _, query := range queries {
					var got string
					require.NoError(t, db.QueryRowContext(ctx, query).Scan(&got), query)
					require.Equal(t, expected[functionName], got, query)
				}

				prepared, err := db.PrepareContext(ctx, fmt.Sprintf(
					"select hex(%s(substring(v512, ?, 511))) from %s where g=1",
					functionName, tableName))
				require.NoError(t, err)
				var rebound string
				require.NoError(t, prepared.QueryRowContext(ctx, 2).Scan(&rebound))
				require.Equal(t, expected[functionName], rebound)
				require.NoError(t, prepared.Close())

				_, err = db.ExecContext(ctx, fmt.Sprintf(
					"select %s(substring(v512, start_pos, 512)) from %s where g=1",
					functionName, tableName))
				require.Error(t, err)
				require.ErrorContains(t, err,
					"Aggregate bitwise functions cannot accept arguments longer than 511 bytes")
				var mysqlErr *mysql.MySQLError
				require.True(t, errors.As(err, &mysqlErr), "%T: %v", err, err)
				require.Equal(t, uint16(3514), mysqlErr.Number)

				var grouped string
				require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
					"select hex(%s(%s)) from %s where g=1 group by g",
					functionName, expression, tableName)).Scan(&grouped))
				require.Equal(t, expected[functionName], grouped)

				rows, err := db.QueryContext(ctx, fmt.Sprintf(
					"select id,hex(%s(%s) over (order by id)) from %s where id <= 2 order by id",
					functionName, expression, tableName))
				require.NoError(t, err)
				var windowValues []string
				for rows.Next() {
					var id int
					var value string
					require.NoError(t, rows.Scan(&id, &value))
					windowValues = append(windowValues, value)
				}
				require.NoError(t, rows.Close())
				require.Equal(t, expectedWindow[functionName], windowValues)
			}

			viewName := fmt.Sprintf("`%s`.bitwise_28227_dynamic_start", dbName)
			execSQLRequire(t, ctx, db, fmt.Sprintf(
				"create view %s as select %s as s from %s where g=1",
				viewName, expression, tableName))
			defer execSQLMaybe(t, ctx, db, "drop view if exists "+viewName)
			for _, functionName := range []string{"bit_and", "bit_or", "bit_xor"} {
				var got string
				require.NoError(t, db.QueryRowContext(ctx,
					fmt.Sprintf("select hex(%s(s)) from %s", functionName, viewName)).Scan(&got))
				require.Equal(t, expected[functionName], got)
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
					fmt.Sprintf("select %s(substring(v512, 1)) from %s where g=1", functionName, tableName),
					fmt.Sprintf("select g,%s(substring(v512, 1)) from %s where g=1 group by g", functionName, tableName),
					fmt.Sprintf("select id,%s(substring(v512, 1)) over (order by id) from %s where id <= 2 order by id", functionName, tableName),
					fmt.Sprintf("select %s(s) from (select substring(v512, 1) as s from %s where g=1) q", functionName, tableName),
					fmt.Sprintf("with q as (select substring(v512, 1) as s from %s where g=1) select %s(s) from q", tableName, functionName),
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
				_, err = db.ExecContext(ctx, fmt.Sprintf("select %s(s) from %s", functionName, viewName))
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
