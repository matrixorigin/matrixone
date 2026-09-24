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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestGroupedBitwiseCTAS(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "ctas_grouped_bitwise_29323"
		_, err = db.ExecContext(ctx, "drop database if exists "+schema)
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		for _, statement := range []string{
			"create database " + schema,
			"create table " + schema + ".src(g int, i int)",
			"insert into " + schema + ".src values (1,15),(1,51),(2,null)",
		} {
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		for _, tc := range []struct{ table, fn, first, second string }{
			{"grouped_and", "bit_and", "3", "18446744073709551615"},
			{"grouped_or", "bit_or", "63", "0"},
			{"grouped_xor", "bit_xor", "60", "0"},
		} {
			for _, query := range []string{
				"select g, " + tc.fn + "(i) as v from " + schema + ".src group by g order by g",
				"select g, v from (select g, " + tc.fn + "(i) as v from " + schema + ".src group by g) as d order by g",
			} {
				rows, err := db.QueryContext(ctx, query)
				require.NoError(t, err, query)
				count := 0
				for rows.Next() {
					var group int
					var value sql.NullString
					require.NoError(t, rows.Scan(&group, &value))
					require.True(t, value.Valid, "%s returned NULL for group %d", query, group)
					count++
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, 2, count)
			}
			_, err := db.ExecContext(ctx, "create table "+schema+"."+tc.table+" as select g, "+tc.fn+"(i) as v from "+schema+".src group by g")
			require.NoError(t, err, tc.fn)
			rows, err := db.QueryContext(ctx, "select g, v from "+schema+"."+tc.table+" order by g")
			require.NoError(t, err)
			var actual []string
			for rows.Next() {
				var group int
				var value sql.NullString
				require.NoError(t, rows.Scan(&group, &value))
				require.True(t, value.Valid)
				actual = append(actual, fmt.Sprintf("%d:%s", group, value.String))
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, []string{"1:" + tc.first, "2:" + tc.second}, actual)
		}

		for _, statement := range []string{
			"create table " + schema + ".src_binary(g int, b binary(2), v varbinary(2))",
			"insert into " + schema + ".src_binary values (1,x'0f0f',x'3333'),(1,x'3333',x'0f0f'),(2,null,null)",
		} {
			_, err := db.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		for _, tc := range []struct {
			fn     string
			first  []byte
			second []byte
		}{
			{"bit_and", []byte{0x03, 0x03}, []byte{0xff, 0xff}},
			{"bit_or", []byte{0x3f, 0x3f}, []byte{0x00, 0x00}},
			{"bit_xor", []byte{0x3c, 0x3c}, []byte{0x00, 0x00}},
		} {
			for _, column := range []string{"b", "v"} {
				table := schema + ".grouped_" + tc.fn + "_" + column
				_, err := db.ExecContext(ctx, "create table "+table+" as select g, "+tc.fn+"("+column+") as value from "+schema+".src_binary group by g")
				require.NoError(t, err, "%s(%s)", tc.fn, column)
				rows, err := db.QueryContext(ctx, "select g, value from "+table+" order by g")
				require.NoError(t, err)
				var actual [][]byte
				for rows.Next() {
					var group int
					var value []byte
					require.NoError(t, rows.Scan(&group, &value))
					require.Equal(t, len(actual)+1, group)
					actual = append(actual, value)
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, [][]byte{tc.first, tc.second}, actual)
			}
		}
	})
}
