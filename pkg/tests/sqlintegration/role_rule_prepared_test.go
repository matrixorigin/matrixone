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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Issue #30347: account-local role DDL must not break another account's
// existing SQL or binary prepared handles on the same CN. Keep the inverse
// security control here: real rule changes must still reject stale handles.
func TestPreparedRoleInvalidationAccountIsolation(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		connect := func(user, password string) *sql.Conn {
			t.Helper()
			db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:%d)/?interpolateParams=false", user, password, port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			return conn
		}
		admin := connect("dump", "111")
		defer admin.Close()
		exec := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		const (
			account = "issue30347_account"
			dbName  = "issue30347_db"
			role    = "issue30347_role"
			user    = "issue30347_user"
		)
		defer cleanupSQLIntegration(t, cn,
			"drop account if exists "+account,
			"drop user if exists "+user,
			"drop role if exists "+role,
			"drop database if exists "+dbName,
		)
		exec(admin, "set role moadmin")
		exec(admin, "create account "+account+" admin_name = 'admin' identified by 'Test30347Pass'")
		exec(admin, "create database "+dbName)
		exec(admin, "create table "+dbName+".t (id int primary key)")
		exec(admin, "insert into "+dbName+".t values (1),(2)")
		exec(admin, "create role "+role)
		exec(admin, "create user "+user+" identified by 'Test30347Pass' default role "+role)
		exec(admin, "grant connect on account * to "+role)
		exec(admin, "grant select on table "+dbName+".t to "+role)

		other := connect(account+"#admin#accountadmin", "Test30347Pass")
		defer other.Close()
		exec(other, "create database "+dbName)
		exec(other, "create table "+dbName+".t (id int primary key)")
		exec(other, "insert into "+dbName+".t values (7)")
		query := "select id from " + dbName + ".t where id >= ? order by id"
		otherBinary, err := other.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer otherBinary.Close()
		exec(other, "prepare other_sql from 'select id from "+dbName+".t order by id'")
		defer exec(other, "deallocate prepare other_sql")
		checkOther := func() {
			t.Helper()
			for range 2 {
				var id int
				require.NoError(t, otherBinary.QueryRowContext(ctx, 0).Scan(&id), "binary handle in another tenant")
				require.Equal(t, 7, id)
				require.NoError(t, other.QueryRowContext(ctx, "execute other_sql").Scan(&id), "SQL handle in another tenant")
				require.Equal(t, 7, id)
			}
		}
		checkOther()

		reader := connect("sys#"+user+"#"+role, "Test30347Pass")
		defer reader.Close()
		prepareReader := func() *sql.Stmt {
			t.Helper()
			stmt, err := reader.PrepareContext(ctx, query)
			require.NoError(t, err)
			return stmt
		}
		readIDs := func(stmt *sql.Stmt) []int {
			t.Helper()
			rows, err := stmt.QueryContext(ctx, 0)
			require.NoError(t, err)
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
		assertReprepare := func(stmt *sql.Stmt) {
			t.Helper()
			var id int
			err := stmt.QueryRowContext(ctx, 0).Scan(&id)
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &mysqlErr)
			require.EqualValues(t, 1615, mysqlErr.Number)
		}
		// A no-op must preserve handles even in the DDL's own account.
		func() {
			stmt := prepareReader()
			defer stmt.Close()
			exec(admin, "drop role if exists issue30347_absent")
			checkOther()
			require.Equal(t, []int{1, 2}, readIDs(stmt))
		}()
		// A previously empty policy must also be invalidated when a rule is
		// installed. Removing a rule must not leave the old filter cached.
		for _, change := range []struct {
			name, ddl string
			want      []int
		}{
			{"add", "alter role " + role + " add rule 'select * from " + dbName + ".t where id = 2' on table " + dbName + ".t", []int{2}},
			{"drop", "alter role " + role + " drop rule on table " + dbName + ".t", []int{1, 2}},
		} {
			t.Logf("verify %s rule isolation and stale-policy rejection", change.name)
			func() {
				stale := prepareReader()
				defer stale.Close()
				exec(admin, change.ddl)
				checkOther()
				assertReprepare(stale)
				fresh := prepareReader()
				defer fresh.Close()
				require.Equal(t, change.want, readIDs(fresh))
			}()
		}
		func() {
			stale := prepareReader()
			defer stale.Close()
			exec(admin, "drop role "+role)
			checkOther()
			assertReprepare(stale)
		}()
	})
}

func TestNativePreparedRoleRuleSurvivesDisabledRemapHint(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		adminDSN := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
		adminDB, err := sql.Open("mysql", adminDSN)
		require.NoError(t, err)
		defer adminDB.Close()
		execSQLRequire(t, ctx, adminDB, "set role moadmin")

		const (
			dbName   = "pr29236_prepared_role_rule"
			roleName = "pr29236_prepared_role"
			userName = "pr29236_prepared_user"
			password = "Pr29236PreparedPass01"
		)
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists "+userName,
			"drop role if exists "+roleName,
			"drop database if exists "+dbName,
		)

		execSQLRequire(t, ctx, adminDB, "drop user if exists "+userName)
		execSQLRequire(t, ctx, adminDB, "drop role if exists "+roleName)
		execSQLRequire(t, ctx, adminDB, "drop database if exists "+dbName)
		execSQLRequire(t, ctx, adminDB, "create database `"+dbName+"`")
		execSQLRequire(t, ctx, adminDB,
			"create table `"+dbName+"`.t (id int primary key, visible int)")
		execSQLRequire(t, ctx, adminDB,
			"insert into `"+dbName+"`.t values (1, 0), (2, 1)")
		execSQLRequire(t, ctx, adminDB, "create role "+roleName)
		execSQLRequire(t, ctx, adminDB,
			"alter role "+roleName+
				" add rule \"select * from `"+dbName+"`.t where visible = 1\" on table `"+dbName+"`.t")
		execSQLRequire(t, ctx, adminDB,
			"create user "+userName+" identified by '"+password+"' default role "+roleName)
		execSQLRequire(t, ctx, adminDB, "grant connect on account * to "+roleName)
		execSQLRequire(t, ctx, adminDB, "grant select on table `"+dbName+"`.t to "+roleName)

		var total int
		require.NoError(t, adminDB.QueryRowContext(ctx,
			"select count(*) from `"+dbName+"`.t").Scan(&total))
		require.Equal(t, 2, total)

		userDSN := fmt.Sprintf("sys#%s#%s:%s@tcp(127.0.0.1:%d)/?interpolateParams=false",
			userName, roleName, password, port)
		userDB, err := sql.Open("mysql", userDSN)
		require.NoError(t, err)
		defer userDB.Close()
		userConn, err := userDB.Conn(ctx)
		require.NoError(t, err)
		defer userConn.Close()

		execSQLOnConn := func(statement string) {
			t.Helper()
			_, err := userConn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		prepareQuery := func() *sql.Stmt {
			t.Helper()
			prepared, err := userConn.PrepareContext(ctx,
				"select id from `"+dbName+"`.t where id >= ? order by id")
			require.NoError(t, err)
			return prepared
		}

		queryIDs := func(prepared *sql.Stmt) []int {
			t.Helper()
			rows, err := prepared.QueryContext(ctx, 0)
			require.NoError(t, err)
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

		runPreparedWithPolicy := func(enabled int) {
			t.Helper()
			execSQLOnConn(fmt.Sprintf("set enable_remap_hint = %d", enabled))
			prepared := prepareQuery()
			defer func() { require.NoError(t, prepared.Close()) }()
			for reuse := 0; reuse < 2; reuse++ {
				require.Equal(t, []int{2}, queryIDs(prepared),
					"prepared statement must keep role filtering with switch=%d on reuse=%d", enabled, reuse)
			}
		}
		runPreparedWithPolicy(0)
		// Changing rewrite state invalidates server-side prepared statements;
		// the helper above closes each protocol handle before the next policy.
		runPreparedWithPolicy(1)
		runPreparedWithPolicy(0)

		stmt := prepareQuery()
		defer func() { require.NoError(t, stmt.Close()) }()

		// Keep the projection stable while changing the schema so the prepared
		// plan must rebuild without losing its saved role-rule AST.
		execSQLRequire(t, ctx, adminDB,
			"alter table `"+dbName+"`.t add column note varchar(20)")
		for reuse := 0; reuse < 2; reuse++ {
			require.Equal(t, []int{2}, queryIDs(stmt),
				"rebuilt prepared statement must keep role filtering on reuse=%d", reuse)
		}
	})
}
