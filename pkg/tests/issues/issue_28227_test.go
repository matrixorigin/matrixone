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
			v600 varbinary(600))`, tableName))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"insert into %s values (1,1,unhex('00FF'),unhex('00FF'),unhex('00FF'),unhex('00FF')),(2,1,unhex('0F0F'),unhex('0F0F'),unhex('0F0F'),unhex('0F0F')),(3,2,null,null,null,null)",
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
			})
		}
	})
}
