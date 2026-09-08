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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28397FieldKeepsExactNumericComparison(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		_, err = conn.ExecContext(ctx, fmt.Sprintf("create database `%s`", dbName))
		require.NoError(t, err)
		defer func() {
			_, cleanupErr := conn.ExecContext(ctx, fmt.Sprintf("drop database if exists `%s`", dbName))
			require.NoError(t, cleanupErr)
		}()
		_, err = conn.ExecContext(ctx, fmt.Sprintf("use `%s`", dbName))
		require.NoError(t, err)

		cases := []struct {
			name string
			sql  string
			want int64
		}{
			{
				name: "integer boundary",
				sql:  "select field(cast(9007199254740993 as decimal(20,0)), cast(9007199254740992 as decimal(20,0)), cast(9007199254740993 as decimal(20,0)))",
				want: 2,
			},
			{
				name: "decimal128 boundary",
				sql:  "select field(cast('99999999999999999999999999999999999999' as decimal(38,0)), cast('99999999999999999999999999999999999998' as decimal(38,0)), cast('99999999999999999999999999999999999999' as decimal(38,0)))",
				want: 2,
			},
			{
				name: "scale boundary",
				sql:  "select field(cast(1.0000000000000001 as decimal(20,16)), cast(1.0000000000000000 as decimal(20,16)), cast(1.0000000000000001 as decimal(20,16)))",
				want: 2,
			},
			{
				name: "mixed integer",
				sql:  "select field(18446744073709551615, -1, 18446744073709551615)",
				want: 1,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var got int64
				require.NoError(t, conn.QueryRowContext(ctx, tc.sql).Scan(&got))
				require.Equal(t, tc.want, got)
			})
		}

		_, err = conn.ExecContext(ctx, "create table field_decimal(search decimal(38,0), candidate1 decimal(38,0), candidate2 decimal(38,0))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into field_decimal values (99999999999999999999999999999999999999, 99999999999999999999999999999999999998, 99999999999999999999999999999999999999), (9007199254740993,9007199254740992,9007199254740993), (123,122,123)")
		require.NoError(t, err)
		rows, err := conn.QueryContext(ctx, "select field(search,candidate1,candidate2) from field_decimal order by search")
		require.NoError(t, err)
		defer rows.Close()
		var got []int64
		for rows.Next() {
			var value int64
			require.NoError(t, rows.Scan(&value))
			got = append(got, value)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int64{2, 2, 2}, got)
	})
}
