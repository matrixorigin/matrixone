// Copyright 2026 Matrix Origin
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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Also covers #29464: FLOAT protocol values must select the numeric comparison
// domain even when the table has no encoded key.
func TestIssue29463PreparedCompositeKeyDomains(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, "create database "+dbName)
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, _ = db.ExecContext(cleanup, "drop database if exists "+dbName)
		}()
		mustExec(t, ctx, conn, "use "+dbName)
		for _, key := range []string{"", "primary key(k1,k2)", "key idx(k1,k2)"} {
			definition := "k1 int, k2 int, v double default 0"
			if key != "" {
				definition += ", " + key
			}
			mustExec(t, ctx, conn, "create table t("+definition+")")
			mustExec(t, ctx, conn, "insert into t(k1,k2) values (1,0),(1,2),(1,3),(2,2)")
			// Include zero: converting an invalid string to zero must match that row,
			// rather than coincidentally pass by producing an empty encoded-key lookup.
			cases := []struct {
				value          any
				sql            string
				equal, greater []int
				warnings       int
			}{
				{int64(2), "2", []int{2}, []int{3}, 0},
				{"invalid", "'invalid'", []int{0}, []int{2, 3}, 1},
				{nil, "null", nil, nil, 0},
				{"2.5", "'2.5'", nil, []int{3}, 0},
				{float64(2.5), "2.5", nil, []int{3}, 0},
				{"2", "'2'", []int{2}, []int{3}, 0},
				{"2suffix", "'2suffix'", []int{2}, []int{3}, 1},
				{int64(3), "3", []int{3}, nil, 0},
			}
			for _, op := range []string{"=", ">", "in"} {
				table := "t"
				if key == "key idx(k1,k2)" {
					table += " force index(idx)"
				}
				predicate := "k2" + op + "?"
				if op == "in" {
					predicate = "k2 in (?)"
				}
				query := "select k2 from " + table + " where k1=? and " + predicate + " order by k2"
				binary, err := conn.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer binary.Close()
				mustExec(t, ctx, conn, "prepare text_stmt from '"+query+"'")
				for _, tc := range cases {
					expected := tc.equal
					if op == ">" {
						expected = tc.greater
					}
					for _, protocol := range []string{"binary", "text"} {
						t.Run(fmt.Sprintf("%s/%s/%s/%T:%v", key, protocol, op, tc.value, tc.value), func(t *testing.T) {
							var rows *sql.Rows
							var err error
							if protocol == "binary" {
								rows, err = binary.QueryContext(ctx, int64(1), tc.value)
							} else {
								mustExec(t, ctx, conn, "set @a=1, @b="+tc.sql)
								rows, err = conn.QueryContext(ctx, "execute text_stmt using @a,@b")
							}
							require.NoError(t, err)
							defer rows.Close()
							var actual []int
							for rows.Next() {
								var v int
								require.NoError(t, rows.Scan(&v))
								actual = append(actual, v)
							}
							require.NoError(t, rows.Err())
							require.NoError(t, rows.Close())
							require.Equal(t, expected, actual)
							var warnings int
							require.NoError(t, conn.QueryRowContext(ctx, "show count(*) warnings").Scan(&warnings))
							require.Equal(t, tc.warnings, warnings)
						})
					}
				}
				require.NoError(t, binary.Close())
				mustExec(t, ctx, conn, "deallocate prepare text_stmt")
			}
			// An unrelated runtime overload must preserve the encoded key's casts.
			mixed, err := conn.PrepareContext(ctx, "select k2 from t where k1=? and k2=? and abs(?)=2")
			require.NoError(t, err)
			defer mixed.Close()
			for _, keyValue := range []int64{2, 3} {
				var found int64
				require.NoError(t, mixed.QueryRowContext(ctx, int64(1), keyValue, int64(-2)).Scan(&found))
				require.Equal(t, keyValue, found)
			}
			require.NoError(t, mixed.Close())
			update, err := conn.PrepareContext(ctx, "update t set v=? where k1=? and k2=?")
			require.NoError(t, err)
			defer update.Close()
			for _, keyValue := range []int64{2, 3} {
				result, err := update.ExecContext(ctx, float64(keyValue)+0.25, int64(1), keyValue)
				require.NoError(t, err)
				changed, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), changed)
			}
			require.NoError(t, update.Close())
			mustExec(t, ctx, conn, "drop table t")
		}
		mustExec(t, ctx, conn, "create table string_key(k1 int, k2 varchar(20), primary key(k1,k2))")
		mustExec(t, ctx, conn, "insert into string_key values(1,'02'),(1,'2'),(1,'3'),(1,'invalid'),(2,'2')")
		strings, err := conn.PrepareContext(ctx, "select count(*) from string_key where k1=? and k2=?")
		require.NoError(t, err)
		defer strings.Close()
		for _, tc := range []struct {
			value any
			count int
		}{
			{"2", 1}, {int64(2), 2}, {float64(2), 2}, {"invalid", 1}, {nil, 0}, {"3", 1},
		} {
			var count int
			require.NoError(t, strings.QueryRowContext(ctx, int64(1), tc.value).Scan(&count))
			require.Equal(t, tc.count, count, "binding %T:%v", tc.value, tc.value)
		}
		require.NoError(t, strings.Close())
		mustExec(t, ctx, conn, "drop table string_key")

		// Ordinary column controls ensure source binding uses common consumer
		// contracts and preserves the existing exact integer-literal optimization.
		mustExec(t, ctx, conn, "create table integer_keys(k bigint primary key)")
		mustExec(t, ctx, conn, "insert into integer_keys values(9007199254740992),(9007199254740993)")
		for _, predicate := range []string{"k='9007199254740993'", "'9007199254740993'=k", "k in ('9007199254740993')"} {
			var key string
			require.NoError(t, conn.QueryRowContext(ctx, "select k from integer_keys where "+predicate).Scan(&key))
			require.Equal(t, "9007199254740993", key)
		}
		mustExec(t, ctx, conn, "create table numeric_domains(t time(0), f time(6), u bigint unsigned, b bit(64), d decimal(5,2))")
		mustExec(t, ctx, conn, "insert into numeric_domains values('00:00:01','00:00:01.500000',18446744073709551615,18446744073709551615,1.25)")
		for _, tc := range []struct{ expr, want string }{
			{"t*u", "18446744073709551615"}, {"u*t", "18446744073709551615"},
			{"f*u", "27670116110564327422.500000"}, {"b*t", "18446744073709551615"},
			{"b*d", "23058430092136939518.75"}, {"d*b", "23058430092136939518.75"},
		} {
			var actual string
			require.NoError(t, conn.QueryRowContext(ctx, "select "+tc.expr+" from numeric_domains").Scan(&actual))
			require.Equal(t, tc.want, actual, tc.expr)
		}
		mustExec(t, ctx, conn, "create table json_values(j json)")
		mustExec(t, ctx, conn, `insert into json_values values('1.6'),('"abc"'),('null'),(NULL)`)
		func() {
			rows, err := conn.QueryContext(ctx, "select concat(j,'x'),concat_ws('-',j,'x') from json_values")
			require.NoError(t, err)
			defer rows.Close()
			var actual []string
			for rows.Next() {
				var first sql.NullString
				var second string
				require.NoError(t, rows.Scan(&first, &second))
				if !first.Valid {
					first.String = "SQLNULL"
				}
				actual = append(actual, first.String+"/"+second)
			}
			require.NoError(t, rows.Err())
			require.ElementsMatch(t, []string{`1.6x/1.6-x`, `"abc"x/"abc"-x`, "nullx/null-x", "SQLNULL/x"}, actual)
		}()

	})
}
