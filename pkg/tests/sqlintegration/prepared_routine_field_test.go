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

func TestPreparedRoutineFieldHistory(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "prepared_routine_field"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		exec("create database " + schema)
		exec("use " + schema)

		for _, protocol := range []string{"binary", "text"} {
			t.Run(protocol, func(t *testing.T) {

				exec := func(q string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, q)
					require.NoError(t, err, q)
				}
				name := "f_" + protocol
				create := func(replace bool, ret, body string) {
					ddl := "create function "
					if replace {
						ddl = "create or replace function "
					}
					exec(ddl + name + "(x varchar(10)) returns " + ret + " language sql as '" + body + "'")
				}
				create(false, "varbinary(10)", "cast($1 as binary)")
				var revision uint64
				require.NoError(t, conn.QueryRowContext(ctx, "select active_revision from mo_catalog.mo_user_defined_function where db=? and name=?", schema, name).Scan(&revision))
				require.Positive(t, revision, "exercise the revision-bearing routine dependency contract")
				var initial uint64
				require.NoError(t, conn.QueryRowContext(ctx, "select field("+name+"('a'),'A','a')").Scan(&initial))
				require.Equal(t, uint64(2), initial, "the body owns a binary comparison domain")
				query := "select field(case when ? then " + name + "('a') else ? end,'A','a')"
				var stmt *sql.Stmt
				if protocol == "binary" {
					stmt, err = conn.PrepareContext(ctx, query)
					require.NoError(t, err)
					defer stmt.Close()
				} else {
					exec("prepare routine_field from " + query)
					defer exec("deallocate prepare routine_field")
					exec("set @field_choose=0")
				}
				check := func(value any, want uint64) {
					t.Helper()
					var got uint64
					if stmt != nil {
						require.NoError(t, stmt.QueryRowContext(ctx, int64(0), value).Scan(&got))
					} else {
						if _, binary := value.([]byte); binary {
							exec("set @field_value=cast('a' as binary)")
						} else {
							exec("set @field_value='a'")
						}
						require.NoError(t, conn.QueryRowContext(ctx, "execute routine_field using @field_choose,@field_value").Scan(&got))
					}
					require.Equal(t, want, got)
				}
				// Even the unselected routine branch contributes its declaration.
				check([]byte("a"), 2)

				// SQL routine DDL is autocommit-only. Changing the body and
				// restoring it both publish new routine declaration generations.
				create(true, "varbinary(10)", "cast($1 as char)")
				check("a", 1)
				create(true, "varbinary(10)", "cast($1 as binary)")

				check([]byte("a"), 2)
				// A return-type change uses the supported DROP/CREATE contract.
				exec("drop function " + name + "(varchar(10))")
				create(false, "varchar(10)", "cast($1 as char)")
				var direct uint64
				require.NoError(t, conn.QueryRowContext(ctx, "select field(case when false then "+name+"('a') else 'a' end,'A','a')").Scan(&direct))
				require.Equal(t, uint64(1), direct)
				check("a", 1)
				check("a", 1)
			})
		}

	})
}
