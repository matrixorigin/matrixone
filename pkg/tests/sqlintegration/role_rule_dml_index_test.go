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

func TestRoleRuleDMLAndIndexVisibility(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)

		const (
			db       = "role_rule_dml_index"
			role     = "role_rule_dml_index_role"
			user     = "role_rule_dml_index_user"
			password = "RoleRuleIndexData01"
			target   = db + ".items"
			other    = db + ".other"
		)
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer admin.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop user if exists "+user,
			"drop role if exists "+role,
			"drop database if exists "+db,
		)
		execAdmin := func(statement string) {
			t.Helper()
			execSQLRequire(t, ctx, admin, statement)
		}
		execAdmin("create database " + db)
		execAdmin("create table " + target + " (id int primary key, tenant int, amount int, note varchar(20))")
		execAdmin("create index idx_tenant on " + target + "(tenant)")
		execAdmin("create index idx_amount on " + target + "(amount)")
		execAdmin("create index idx_expression on " + target + " ((tenant + 1))")
		execAdmin("insert into " + target + " values (1, 1, 10, 'first'), (2, 2, 20, 'second'), (3, 1, 30, 'third'), (4, 1, 40, 'fourth')")
		execAdmin("create table " + other + " (id int primary key, amount int)")
		execAdmin("insert into " + other + " values (2, 53), (3, 30)")
		execAdmin("create role " + role)
		execAdmin("grant connect on account * to " + role)
		execAdmin("grant select, update, delete on table " + target + " to " + role)
		execAdmin("grant select, update on table " + other + " to " + role)
		execAdmin("alter role " + role + " add rule '((select amount as Amount, id as ID from " + target + " where tenant = 1))' on table " + target)
		execAdmin("create user " + user + " identified by '" + password + "' default role " + role)

		dsn := fmt.Sprintf("sys#%s#%s:%s@tcp(127.0.0.1:%d)/%s?interpolateParams=false", user, role, password, port, db)
		client, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer client.Close()
		conn, err := client.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		requireCode := func(statement string, code uint16) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			var serverErr *mysql.MySQLError
			require.ErrorAs(t, err, &serverErr, statement)
			require.Equal(t, code, serverErr.Number, statement)
		}
		requireValue := func(row *sql.Row, want int) {
			t.Helper()
			var got int
			require.NoError(t, row.Scan(&got))
			require.Equal(t, want, got)
		}
		requireExplain := func(statement string) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			require.True(t, rows.Next(), statement)
			require.NoError(t, rows.Err(), statement)
		}
		execUser := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		requireAffected := func(statement string, want int) {
			t.Helper()
			execUser(statement)
			requireValue(conn.QueryRowContext(ctx, "select row_count()"), want)
		}

		statistics := "select count(*) from information_schema.statistics where table_schema = '" + db + "' and table_name = 'items' and "
		hiddenIndexes := statistics + "index_name in ('idx_tenant', 'idx_expression')"
		visibleIndex := statistics + "index_name = 'idx_amount' and column_name = 'amount'"
		requireValue(admin.QueryRowContext(ctx, hiddenIndexes), 2)
		for _, enabled := range []int{0, 1} {
			execUser(fmt.Sprintf("set enable_remap_hint = %d", enabled))
			requireValue(conn.QueryRowContext(ctx, hiddenIndexes), 0)
			requireValue(conn.QueryRowContext(ctx, visibleIndex), 1)
			requireCode("show index from items", 20101)
			requireCode("show index ignored from items", 20101)
			requireCode("show index status from items", 20101)
			execUser("show index from other")
			requireCode("update items set amount = 50 where tenant = 1", 1054)
			requireCode("update items set tenant = 1 where id = 1", 1054)
			requireCode("update items set amount = 50, amount = tenant where id = 1", 1054)
			requireCode("delete from items where tenant = 1", 1054)
			requireCode("delete from items order by tenant limit 1", 1054)
			requireCode("delete from items where id = 1 returning note", 1054)
			requireCode("explain update items set amount = 50 where id = 1", 20101)
			requireCode("explain analyze delete from items where id = 1", 20101)
			requireExplain("explain update other set id = id where id = 2")
			requireAffected("update items as i set i.amount = 50 where i.id = 2", 0)
		}
		requireExplain("explain analyze update other set id = id where id = -1")
		const updateUsingRuledSource = "update other set id = id where id in (select id from items)"
		requireCode("explain "+updateUsingRuledSource, 20101)

		requireAffected("/*+ {\"rewrites\":{\""+target+"\":\"select * from "+target+" where amount < 30\"}} */ "+
			"update items set amount = 50 where id = 3", 0)
		requireAffected("update items set amount = amount + 1, amount = amount + 1 where id = 1", 1)
		requireValue(conn.QueryRowContext(ctx, "select amount from items where id = 1"), 12)

		execUser("set enable_remap_hint = 0")
		execUser("set @explain_id = 2")
		execUser("prepare ordinary_update from 'update other set id = id where id = ?'")
		requireExplain("explain force execute ordinary_update using @explain_id")
		execUser("deallocate prepare ordinary_update")
		execUser("prepare policy_update from 'update items set amount = 51 where id = ?'")
		requireCode("explain force execute policy_update using @explain_id", 20101)
		requireCode("explain analyze force execute policy_update using @explain_id", 20101)
		execUser("deallocate prepare policy_update")
		func() {
			prepared, err := conn.PrepareContext(ctx, "update items set amount = ? where id = ?")
			require.NoError(t, err)
			defer prepared.Close()
			for _, tc := range []struct {
				id   int
				want int64
			}{{2, 0}, {1, 1}} {
				result, err := prepared.ExecContext(ctx, 51, tc.id)
				require.NoError(t, err)
				affected, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, tc.want, affected, "primary key %d", tc.id)
			}
		}()
		requireValue(admin.QueryRowContext(ctx, "select id from "+target+" force index (idx_amount) where amount = 51"), 1)
		requireValue(admin.QueryRowContext(ctx, "select amount from "+target+" where id = 2"), 20)
		func() {
			// A selective key predicate may prune the scan. The division still
			// runs only over rows admitted by the role rule.
			execUser("update items set amount = 50 where id = 2 and amount / (id - 2) > 0")
			rows, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer rows.Close()
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}()
		requireProjection := func(statement string, wantAmount, wantID int) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err, statement)
			require.Equal(t, []string{"Amount", "ID"}, columns, statement)
			require.True(t, rows.Next(), statement)
			var amount, id int
			require.NoError(t, rows.Scan(&amount, &id), statement)
			require.Equal(t, wantAmount, amount, statement)
			require.Equal(t, wantID, id, statement)
			require.False(t, rows.Next(), statement)
			require.NoError(t, rows.Err(), statement)
		}
		requireProjection("select * from items where id = 1", 51, 1)
		requireProjection("update items set amount = 52 where id = 1 returning *", 52, 1)
		execUser("set enable_remap_hint = 1")
		requireProjection(`/*+ {"rewrites":{"`+target+`":"select * from `+target+`"}} */ `+
			"delete from items where id = 4 returning items.*", 40, 4)
		execUser("set enable_remap_hint = 0")

		// Both base targets require DELETE privileges. Their index writes are internal.
		const joinedDelete = "delete i, o from items as i join other as o on i.id = o.id"
		requireCode(joinedDelete, 20101)
		execAdmin("grant delete on table " + other + " to " + role)
		execUser(joinedDelete)
		requireValue(admin.QueryRowContext(ctx, "select count(*) from "+other+" where id = 2"), 1)
		requireValue(admin.QueryRowContext(ctx, "select count(*) from "+other+" where id = 3"), 0)

		// LIMIT keeps the joined source readable, while its projected columns
		// determine which unqualified SET names belong to the writable table.
		execAdmin("alter role " + role + " add rule 'select id from " + other + " limit 1' on table " + other)
		const joinedUpdate = "update items i join other o on o.id = i.id + 1 "
		requireAffected(joinedUpdate+"set amount = amount + 1 where i.id = 1", 1)
		requireValue(admin.QueryRowContext(ctx, "select amount from "+target+" where id = 1"), 53)
		execAdmin("alter role " + role + " add rule 'select id, amount from " + other + " limit 1' on table " + other)
		_, err = conn.ExecContext(ctx, joinedUpdate+"set amount = 54 where i.id = 1")
		require.ErrorContains(t, err, "ambiguous column")
		requireCode(joinedUpdate+"set o.amount = 54 where i.id = 1", 20101)
		requireCode("update other o join items i using (amount) set amount = 54", 20101)
		requireValue(admin.QueryRowContext(ctx, "select amount from "+target+" where id = 1"), 53)
		requireValue(admin.QueryRowContext(ctx, "select amount from "+other+" where id = 2"), 53)

		requireAffected("delete from items", 1)
		requireValue(admin.QueryRowContext(ctx, "select count(*) from "+target+" force index (idx_tenant) where tenant = 1"), 0)
		requireValue(admin.QueryRowContext(ctx, "select id from "+target), 2)

		// A replacement without an unambiguous base-row mapping stays readable.
		execAdmin("alter role " + role + " add rule '((select id, amount from " + target + " limit 1))' on table " + target)
		requireValue(conn.QueryRowContext(ctx, "select id from items"), 2)
		requireCode("update items set amount = 60 where id = 2", 20101)
	})
}
