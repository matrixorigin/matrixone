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

func TestCTASCarriesRoleRuleAcrossSQLModesAndRemap(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		adminDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer adminDB.Close()

		const (
			sourceDB = "pr29707_ctas_source"
			destDB   = "pr29707_ctas_dest"
			role     = "pr29707_ctas_role"
			user     = "pr29707_ctas_user"
			password = "Pr29707CtasPass01"
		)
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists "+user,
			"drop role if exists "+role,
			"drop database if exists "+sourceDB,
			"drop database if exists "+destDB,
		)

		execSQLRequire(t, ctx, adminDB, "set role moadmin")
		execSQLRequire(t, ctx, adminDB, "drop user if exists "+user)
		execSQLRequire(t, ctx, adminDB, "drop role if exists "+role)
		execSQLRequire(t, ctx, adminDB, "drop database if exists "+sourceDB)
		execSQLRequire(t, ctx, adminDB, "drop database if exists "+destDB)
		execSQLRequire(t, ctx, adminDB, "create database `"+sourceDB+"`")
		execSQLRequire(t, ctx, adminDB, "create database `"+destDB+"`")
		execSQLRequire(t, ctx, adminDB,
			"create table `"+sourceDB+"`.t (id int primary key, note varchar(16))")
		execSQLRequire(t, ctx, adminDB,
			"create table `"+destDB+"`.t (id int primary key, note varchar(16))")
		execSQLRequire(t, ctx, adminDB,
			"insert into `"+sourceDB+"`.t values (1, 'ab'), (2, concat('a', char(92), 'b'))")
		execSQLRequire(t, ctx, adminDB,
			"insert into `"+destDB+"`.t values (3, 'ab'), (4, concat('a', char(92), 'b'))")

		adminConn, err := adminDB.Conn(ctx)
		require.NoError(t, err)
		defer adminConn.Close()
		execAdmin := func(statement string) {
			t.Helper()
			_, err := adminConn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		// Keep the backslash in the stored rule literal. The user session below
		// deliberately switches SQL mode to prove that the typed AST survives
		// the CTAS follow-up INSERT without reparsing diagnostic SQL.
		execAdmin("set sql_mode = 'NO_BACKSLASH_ESCAPES'")
		execAdmin("create role " + role)
		execAdmin("alter role " + role +
			" add rule \"select * from `" + sourceDB + "`.t where note <> 'a\\b'\" on table `" + sourceDB + "`.t")
		execAdmin("create user " + user + " identified by '" + password + "' default role " + role)
		execAdmin("grant connect on account * to " + role)
		execAdmin("grant select on table `" + sourceDB + "`.t to " + role)
		execAdmin("grant select on table `" + destDB + "`.t to " + role)
		execAdmin("grant create table,drop table on database `" + sourceDB + "` to " + role)
		execAdmin("grant create table,drop table on database `" + destDB + "` to " + role)

		userDB, err := sql.Open("mysql", fmt.Sprintf(
			"sys#%s#%s:%s@tcp(127.0.0.1:%d)/?interpolateParams=false",
			user, role, password, port))
		require.NoError(t, err)
		defer userDB.Close()
		userConn, err := userDB.Conn(ctx)
		require.NoError(t, err)
		defer userConn.Close()
		execUser := func(statement string) {
			t.Helper()
			_, err := userConn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		queryIDs := func(statement string) []int {
			t.Helper()
			rows, err := userConn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			return ids
		}
		queryMaterialized := func(database, table string) ([]int, []string) {
			t.Helper()
			rows, err := adminDB.QueryContext(ctx,
				"select id, marker from `"+database+"`.`"+table+"` order by id")
			require.NoError(t, err)
			defer rows.Close()
			var ids []int
			var markers []string
			for rows.Next() {
				var id int
				var marker string
				require.NoError(t, rows.Scan(&id, &marker))
				ids = append(ids, id)
				markers = append(markers, marker)
			}
			require.NoError(t, rows.Err())
			return ids, markers
		}

		execUser("set enable_remap_hint = 1")
		execUser("set sql_mode = ''")
		require.Equal(t, []int{1, 2}, queryIDs("select id from `"+sourceDB+"`.t order by id"))
		execUser("create table `" + sourceDB + "`.ctas_default as select id, 'safe_value' as marker from `" + sourceDB + "`.t")
		ids, markers := queryMaterialized(sourceDB, "ctas_default")
		require.Equal(t, []int{1, 2}, ids)
		require.Equal(t, []string{"safe_value", "safe_value"}, markers)
		execAdmin("drop table `" + sourceDB + "`.ctas_default")

		execUser("set sql_mode = 'NO_BACKSLASH_ESCAPES'")
		require.Equal(t, []int{1}, queryIDs("select id from `"+sourceDB+"`.t order by id"))
		execUser("create table `" + sourceDB + "`.ctas_no_backslash as select id, '__mo_query' as marker from `" + sourceDB + "`.t")
		ids, markers = queryMaterialized(sourceDB, "ctas_no_backslash")
		require.Equal(t, []int{1}, ids)
		require.Equal(t, []string{"__mo_query"}, markers)
		execAdmin("drop table `" + sourceDB + "`.ctas_no_backslash")

		execUser(`set remap_rewrites = '{"remapdb":{"` + sourceDB + `":"` + destDB + `"}}'`)
		require.Equal(t, []int{3}, queryIDs("select id from `"+sourceDB+"`.t order by id"))
		execUser("create table `" + destDB + "`.ctas_remap as select id, '__mo_query' as marker from `" + sourceDB + "`.t")
		ids, markers = queryMaterialized(destDB, "ctas_remap")
		require.Equal(t, []int{3}, ids)
		require.Equal(t, []string{"__mo_query"}, markers)
		execAdmin("drop table `" + destDB + "`.ctas_remap")
	})
}
