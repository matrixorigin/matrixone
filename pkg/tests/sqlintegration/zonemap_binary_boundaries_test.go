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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestBinaryZoneMapAmbiguousMax(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		const schema = "binary_zm_ambiguous"
		exec("create database " + schema)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec("use " + schema)
		exec("create table ordinary (id bigint primary key, b varbinary(256))")
		exec("create table sorted_pk (id bigint, b varbinary(256) primary key)")
		value := func(suffix string) string {
			return "cast(x'" + strings.Repeat("ff", 30) + suffix + "' as varbinary(256))"
		}
		for _, table := range []string{"ordinary", "sorted_pk"} {
			exec("insert into " + table + " values (1," + value("00") + "),(2," + value("01") + "),(3," + value("02") + "),(5,cast(x'' as varbinary(256)))")
			if table == "ordinary" {
				exec("insert into ordinary values (4, null)")
			}
			var flush string
			require.NoError(t, conn.QueryRowContext(ctx, "select mo_ctl('dn','flush','"+schema+"."+table+"')").Scan(&flush))
			require.Contains(t, flush, "OK")
		}
		read := func(tb testing.TB, q string, projection bool) []int64 {
			tb.Helper()
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(tb, err, q)
			defer rows.Close()
			var ids []int64
			for rows.Next() {
				var id int64
				if projection {
					var match sql.NullBool
					require.NoError(tb, rows.Scan(&id, &match), q)
					if !match.Valid || !match.Bool {
						continue
					}
				} else {
					require.NoError(tb, rows.Scan(&id), q)
				}
				ids = append(ids, id)
			}
			require.NoError(tb, rows.Err())
			return ids
		}
		explain := func(tb testing.TB, q string) string {
			tb.Helper()
			rows, err := conn.QueryContext(ctx, "explain analyze "+q)
			require.NoError(tb, err, q)
			defer rows.Close()
			var plan strings.Builder
			for rows.Next() {
				var line string
				require.NoError(tb, rows.Scan(&line))
				plan.WriteString(line)
				plan.WriteByte('\n')
			}
			require.NoError(tb, rows.Err())
			return plan.String()
		}
		cases := []struct {
			name, predicate string
			want            []int64
		}{
			{"lt", "b < " + value("01"), []int64{1, 5}},
			{"reverse lt", value("01") + " > b", []int64{1, 5}},
			{"gt", "b > " + value("01"), []int64{3}},
			{"reverse gt", value("01") + " < b", []int64{3}},
			{"le", "b <= " + value("01"), []int64{1, 2, 5}},
			{"ge", "b >= " + value("01"), []int64{2, 3}},
			{"eq", "b = " + value("01"), []int64{2}},
			{"eq empty", "b = cast(x'' as varbinary(256))", []int64{5}},
			{"eq null", "b = cast(null as varbinary(256))", nil},
			{"eq low", "b = cast(x'61' as varbinary(256))", nil},
			{"reverse eq low", "cast(x'61' as varbinary(256)) = b", nil},
			{"eq high", "b = " + value("02"), []int64{3}},
			{"between", "b between " + value("00") + " and " + value("01"), []int64{1, 2}},
			{"in", "b in (" + value("00") + "," + value("02") + ")", []int64{1, 3}},
			{"no match", "b > " + value("03"), nil},
		}
		for _, table := range []string{"ordinary", "sorted_pk"} {
			for _, tc := range cases {
				t.Run(table+"/"+tc.name, func(t *testing.T) {
					from := " from " + table
					projection := "select id,(" + tc.predicate + ")" + from + " order by id"
					filtered := "select id" + from + " where " + tc.predicate + " order by id"
					require.Equal(t, tc.want, read(t, projection, true))
					require.Equal(t, tc.want, read(t, filtered, false))
					// The ordinary IN path does not currently push a block filter.
					if tc.name != "eq null" && (table != "ordinary" || tc.name != "in") {
						require.Contains(t, explain(t, filtered), "Block Filter Cond:")
					}
				})
			}
		}
	})
}
