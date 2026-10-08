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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestUnicodeCollationConsumerContract(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		schema := "unicode_collation_29601"
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)

		exec := func(statement string) {
			t.Helper()
			execSQLRequire(t, ctx, db, statement)
		}
		queryStrings := func(statement string) []string {
			t.Helper()
			rows, queryErr := db.QueryContext(ctx, statement)
			require.NoError(t, queryErr, statement)
			defer rows.Close()
			var values []string
			for rows.Next() {
				var value string
				require.NoError(t, rows.Scan(&value))
				values = append(values, value)
			}
			require.NoError(t, rows.Err())
			return values
		}
		queryCount := func(statement string) int {
			t.Helper()
			var value int
			require.NoError(t, db.QueryRowContext(ctx, statement).Scan(&value), statement)
			return value
		}

		exec("drop database if exists " + schema)
		exec("create database " + schema)
		exec("use " + schema)

		// Native Unicode primary-key bytes are not yet canonicalized by the
		// storage dedup/lock paths. Reject both single- and composite-key
		// definitions instead of allowing collation-equivalent values to commit.
		_, err = db.ExecContext(ctx, "create table pk_rejected (s varchar(64) collate utf8mb4_unicode_ci primary key)")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "create table composite_pk_rejected (s varchar(64) collate utf8mb4_unicode_ci, n int, primary key (s, n))")
		require.Error(t, err)

		exec("create table words (id int primary key, s varchar(64) collate utf8mb4_unicode_ci)")
		exec("insert into words values (1,'Z'),(2,'a'),(3,'A'),(4,'b')")
		require.Equal(t, 2, queryCount("select count(*) from words where lower(s)='a'"))
		require.Equal(t, []string{"2", "3"}, queryStrings("select id from words where s='a' order by id"))
		require.Equal(t, []string{"2", "3"}, queryStrings("select id from words where s in ('a','zz') order by id"))
		require.Equal(t, []string{"1", "4"}, queryStrings("select id from words where s not in ('a','zz') order by id"))
		require.Equal(t, []string{"3", "1", "1", "2"}, queryStrings("select dense_rank() over (order by s) from words order by id"))
		require.Equal(t, []string{"1", "2", "2", "1"}, queryStrings("select count(*) over (partition by s) from words order by id"))
		require.Equal(t, 3, queryCount("select count(*) from (select s from words group by s) grouped"))
		require.Equal(t, []string{"a", "A", "b", "Z"}, queryStrings("select s from words order by s,id"))
		require.Equal(t, []string{"a", "A"}, queryStrings("select s from words order by s,id limit 2"))

		var original, serialized string
		require.NoError(t, db.QueryRowContext(ctx,
			"select hex(s), hex(serial_extract(serial(s),0 as varchar(64))) from words where id=4",
		).Scan(&original, &serialized))
		require.Equal(t, "62", original)
		require.Equal(t, original, serialized)

		exec("create table utf8mb3_write (s varchar(64) character set utf8mb3 collate utf8_unicode_ci)")
		_, err = db.ExecContext(ctx, "insert into utf8mb3_write values ('😀')")
		require.Error(t, err)
		_, err = db.ExecContext(ctx, "insert into utf8mb3_write values (unhex('30fbc130ffff20'))")
		require.Error(t, err)
	})
}
