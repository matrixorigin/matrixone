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

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestRoleRuleDottedLegacyKeyRecovery(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer admin.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists dotted_rule_reader",
			"drop role if exists dotted_rule_owner, dotted_rule_neighbor",
			"drop database if exists dotted_rule_db",
		)

		execSQLRequire(t, ctx, admin, "create database dotted_rule_db")
		execSQLRequire(t, ctx, admin, "create table dotted_rule_db.`t.with.dot` (id int)")
		execSQLRequire(t, ctx, admin, "insert into dotted_rule_db.`t.with.dot` values (1)")
		execSQLRequire(t, ctx, admin, "create role dotted_rule_owner, dotted_rule_neighbor")
		execSQLRequire(t, ctx, admin, "create user dotted_rule_reader identified by 'DottedRulePass01' default role dotted_rule_owner")
		execSQLRequire(t, ctx, admin, "grant connect on account * to dotted_rule_owner")
		execSQLRequire(t, ctx, admin, "grant select on table dotted_rule_db.`t.with.dot` to dotted_rule_owner")

		reader, err := sql.Open("mysql", fmt.Sprintf(
			"sys#dotted_rule_reader#dotted_rule_owner:DottedRulePass01@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer reader.Close()
		conn, err := reader.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const query = "select id from dotted_rule_db.`t.with.dot`"
		var id int
		require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&id))
		require.Equal(t, 1, id)
		prepared, err := conn.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer prepared.Close()
		_, err = conn.ExecContext(ctx, "begin")
		require.NoError(t, err)

		// The v4.2.8 writer stored dbName+"."+tblName even when an
		// identifier contained a dot. Seed those same catalog bytes.
		const key = "dotted_rule_db.t.with.dot"
		testutils.ExecSQL(t, "", cn,
			"insert into mo_catalog.mo_role_rule (role_id, rule_name, `rule`) "+
				"select role_id, '"+key+"', '"+query+"' from mo_catalog.mo_role "+
				"where role_name in ('dotted_rule_owner','dotted_rule_neighbor')")
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		var mysqlErr *mysql.MySQLError
		require.ErrorAs(t, conn.QueryRowContext(ctx, query).Scan(&id), &mysqlErr)
		require.Equal(t, moerr.ER_PARSE_ERROR, mysqlErr.Number)
		require.ErrorAs(t, prepared.QueryRowContext(ctx).Scan(&id), &mysqlErr)
		require.Equal(t, moerr.ER_NEED_REPREPARE, mysqlErr.Number)

		const drop = "alter role dotted_rule_owner drop rule on table dotted_rule_db.`t.with.dot`"
		execSQLRequire(t, ctx, admin, drop)
		var remaining string
		require.NoError(t, admin.QueryRowContext(ctx,
			"select r.role_name from mo_catalog.mo_role_rule p join mo_catalog.mo_role r on p.role_id = r.role_id "+
				"where p.rule_name = ?", key).Scan(&remaining))
		require.Equal(t, "dotted_rule_neighbor", remaining)
		require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&id))
		require.Equal(t, 1, id)
	})
}

func TestRoleRuleWhitespaceLegacyCollisionRecovery(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer admin.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists spaced_rule_reader",
			"drop role if exists spaced_rule_owner",
			"drop database if exists spaced_rule_db",
		)

		execSQLRequire(t, ctx, admin, "create database spaced_rule_db")
		execSQLRequire(t, ctx, admin, "create table spaced_rule_db.t (id int)")
		execSQLRequire(t, ctx, admin, "insert into spaced_rule_db.t values (1), (2)")
		execSQLRequire(t, ctx, admin, "create role spaced_rule_owner")
		execSQLRequire(t, ctx, admin, "create user spaced_rule_reader identified by 'SpacedRulePass01' default role spaced_rule_owner")
		execSQLRequire(t, ctx, admin, "grant connect on account * to spaced_rule_owner")
		execSQLRequire(t, ctx, admin, "grant select on table spaced_rule_db.t to spaced_rule_owner")

		reader, err := sql.Open("mysql", fmt.Sprintf(
			"sys#spaced_rule_reader#spaced_rule_owner:SpacedRulePass01@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer reader.Close()
		conn, err := reader.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const query = "select count(*), min(id) from spaced_rule_db.t"
		var count, first int
		_, err = conn.ExecContext(ctx, "set session sql_mode = ''")
		require.NoError(t, err)
		prepared, err := conn.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer prepared.Close()
		require.NoError(t, prepared.QueryRowContext(ctx).Scan(&count, &first))
		require.Equal(t, 2, count)
		_, err = conn.ExecContext(ctx, "begin")
		require.NoError(t, err)

		// The v4.2.9 writer preserved the space in the key and stored
		// COUNT with a space before the opening parenthesis.
		testutils.ExecSQL(t, "", cn,
			"insert into mo_catalog.mo_role_rule (role_id, rule_name, `rule`) "+
				"select role_id, 'spaced_rule_db. t', 'select count (*) as id from spaced_rule_db.t' "+
				"from mo_catalog.mo_role where role_name = 'spaced_rule_owner' union all "+
				"select role_id, 'spaced_rule_db.t', 'select id from spaced_rule_db.t where id = 2' "+
				"from mo_catalog.mo_role where role_name = 'spaced_rule_owner'")
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		var mysqlErr *mysql.MySQLError
		require.ErrorAs(t, conn.QueryRowContext(ctx, query).Scan(&count, &first), &mysqlErr)
		require.Equal(t, moerr.ER_PARSE_ERROR, mysqlErr.Number)
		require.ErrorAs(t, prepared.QueryRowContext(ctx).Scan(&count, &first), &mysqlErr)
		require.Equal(t, moerr.ER_NEED_REPREPARE, mysqlErr.Number)

		_, err = conn.ExecContext(ctx, "set session sql_mode = 'IGNORE_SPACE'")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&count, &first))
		require.Equal(t, 1, count)
		require.Equal(t, 2, first)
	})
}

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
