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

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestShowCreateAndCreateLikeSQLModes(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(t *testing.T, stmt string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, stmt)
			require.NoError(t, err, "SQL: %q", stmt)
		}
		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		name := sqlquote.Ident(dbName)
		exec(t, "create database "+name)
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_, err := conn.ExecContext(cleanup, "drop database "+name)
			require.NoError(t, err)
		}()
		exec(t, "use "+name)
		exec(t, "set session sql_mode = ''")
		exec(t, `create table default_src (id int, v varchar(32) default 'a\\t\\\\b\\', q varchar(32) default 'it''s "quoted"')`)
		exec(t, `create table enum_src (id int, e enum('line\nbreak','cr\rend','nul\0byte','literal\\t','plain'))`)

		t.Run("show_external_crlf", func(t *testing.T) {
			exec(t, "create stage "+name+" url="+sqlquote.String("file://"+t.TempDir()))
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				_, tablesErr := conn.ExecContext(cleanup, "drop table if exists line_src, line_copy")
				_, stageErr := conn.ExecContext(cleanup, "drop stage "+name)
				require.NoError(t, tablesErr)
				require.NoError(t, stageErr)
			}()
			path := "stage://" + dbName + "/"
			exec(t, "create external table line_src(id int) infile{'filepath'="+sqlquote.String(path+"input_*.csv")+
				",'format'='csv','write_file_pattern'="+sqlquote.String(path+"input_%U.csv")+
				`} lines terminated by '\r\n'`)
			var table, ddl string
			require.NoError(t, conn.QueryRowContext(ctx, "show create table line_src").Scan(&table, &ddl))
			require.Contains(t, ddl, `LINES TERMINATED BY '\r\n'`)
			const prefix = "CREATE EXTERNAL TABLE `line_src`"
			require.True(t, strings.HasPrefix(ddl, prefix), "%q", ddl)
			exec(t, "CREATE EXTERNAL TABLE `line_copy`"+strings.TrimPrefix(ddl, prefix))
			require.NoError(t, conn.QueryRowContext(ctx, "show create table line_copy").Scan(&table, &ddl))
			require.Contains(t, ddl, `LINES TERMINATED BY '\r\n'`)
		})

		for i, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
			exec(t, "set session sql_mode = "+sqlquote.String(mode))
			t.Run(fmt.Sprintf("show_%s", mode), func(t *testing.T) {
				var table, ddl string
				require.NoError(t, conn.QueryRowContext(ctx, "show create table default_src").Scan(&table, &ddl))
				require.Equal(t, "default_src", table)
				const prefix = "CREATE TABLE `default_src`"
				require.True(t, strings.HasPrefix(ddl, prefix), "%q", ddl)
				target := fmt.Sprintf("default_copy_%d", i)
				exec(t, "CREATE TABLE "+sqlquote.Ident(target)+strings.TrimPrefix(ddl, prefix))
				exec(t, "insert into "+target+"(id) values (1)")
				var v, q string
				require.NoError(t, conn.QueryRowContext(ctx, "select v,q from "+target+" where id=1").Scan(&v, &q))
				require.Equal(t, "a\\t\\\\b\\", v)
				require.Equal(t, `it's "quoted"`, q)
			})
			t.Run(fmt.Sprintf("like_%s", mode), func(t *testing.T) {
				target := fmt.Sprintf("enum_copy_%d", i)
				exec(t, "create table "+target+" like enum_src")
				exec(t, "insert into "+target+" values (1,1),(2,2),(3,3),(4,4),(5,5)")
				rows, err := conn.QueryContext(ctx, "select cast(e as varchar(32)) from "+target+" order by id")
				require.NoError(t, err)
				defer rows.Close()
				var values []string
				for rows.Next() {
					var v string
					require.NoError(t, rows.Scan(&v))
					values = append(values, v)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, []string{"line\nbreak", "cr\rend", "nul\x00byte", "literal\\t", "plain"}, values)
			})
		}
	})
}
