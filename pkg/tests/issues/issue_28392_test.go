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

func TestIssue28392ConvBinNumericPrefixes(t *testing.T) {
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
		exec := func(tb testing.TB, statement string) {
			tb.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(tb, err, "exec failed: %s", statement)
		}
		exec(t, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
			require.NoError(t, err)
		}()
		exec(t, fmt.Sprintf("use `%s`", dbName))

		const negativeTwoBinary = "1111111111111111111111111111111111111111111111111111111111111110"
		cases := []struct {
			name  string
			query string
			want  string
			null  bool
		}{
			{name: "conv valid prefix", query: "select conv('123xyz', 10, 16)", want: "7B"},
			{name: "conv no valid prefix", query: "select conv('xyz123', 10, 16)", want: "0"},
			{name: "conv positive signed prefix", query: "select conv('  +15tail', 10, 16)", want: "F"},
			{name: "conv negative signed prefix", query: "select conv('  -15tail', 10, -16)", want: "-F"},
			{name: "conv binary prefix", query: "select conv(_binary x'3120ff', 10, 16)", want: "1"},
			{name: "conv binary prefix after NUL", query: "select conv(_binary x'0031', 10, 16)", want: "0"},
			{name: "conv negative unsigned overflow", query: "select conv('-18446744073709551617tail', 10, 16)", want: "0"},
			{name: "conv whitespace", query: "select conv('   ', 10, 16)", want: "0"},
			{name: "conv empty", query: "select conv('', 10, 16)", null: true},
			{name: "bin valid prefix", query: "select bin('7x')", want: "111"},
			{name: "bin bit literal", query: "select bin(0b11111111)", want: "11111111"},
			{name: "bin hex literal", query: "select bin(0xFF)", want: "11111111"},
			{name: "bin binary prefix", query: "select bin(_binary x'37ff')", want: "111"},
			{name: "bin binary unicode whitespace", query: "select bin(_binary x'e380803778')", want: "0"},
			{name: "bin negative unsigned overflow", query: "select bin('-18446744073709551617tail')", want: "0"},
			{name: "bin negative prefix", query: "select bin('-2tail')", want: negativeTwoBinary},
			{name: "bin no valid prefix", query: "select bin('abc')", want: "0"},
			{name: "bin whitespace", query: "select bin('   ')", want: "0"},
			{name: "bin empty", query: "select bin('')", null: true},
			{name: "bin null", query: "select bin(null)", null: true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var got sql.NullString
				err := conn.QueryRowContext(ctx, tc.query).Scan(&got)
				require.NoError(t, err)
				if tc.null {
					require.False(t, got.Valid)
					return
				}
				require.True(t, got.Valid)
				require.Equal(t, tc.want, got.String)
			})
		}

		exec(t, "create table issue28392_inputs (id int primary key, s varchar(32))")
		exec(t, "insert into issue28392_inputs values (1, '123xyz'), (2, 'xyz123'), (3, '  +15tail'), (4, '  -15tail'), (5, '7x'), (6, 'abc'), (7, '')")
		rows, err := conn.QueryContext(ctx, "select id, conv(s, 10, 16), bin(s) from issue28392_inputs order by id")
		require.NoError(t, err)
		defer rows.Close()
		wantRows := []struct {
			id       int
			conv     string
			bin      string
			convNull bool
			binNull  bool
		}{
			{id: 1, conv: "7B", bin: "1111011"},
			{id: 2, conv: "0", bin: "0"},
			{id: 3, conv: "F", bin: "1111"},
			{id: 4, conv: "FFFFFFFFFFFFFFF1", bin: "1111111111111111111111111111111111111111111111111111111111110001"},
			{id: 5, conv: "7", bin: "111"},
			{id: 6, conv: "0", bin: "0"},
			{id: 7, convNull: true, binNull: true},
		}
		for _, want := range wantRows {
			require.True(t, rows.Next())
			var id int
			var gotConv, gotBin sql.NullString
			require.NoError(t, rows.Scan(&id, &gotConv, &gotBin))
			require.Equal(t, want.id, id)
			if want.convNull {
				require.False(t, gotConv.Valid)
			} else {
				require.True(t, gotConv.Valid)
				require.Equal(t, want.conv, gotConv.String)
			}
			if want.binNull {
				require.False(t, gotBin.Valid)
			} else {
				require.True(t, gotBin.Valid)
				require.Equal(t, want.bin, gotBin.String)
			}
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	})
}
