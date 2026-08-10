// Copyright 2021 - 2026 Matrix Origin
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

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// Issue 25526: a prepared UPDATE ... JOIN executed through the binary protocol
// (COM_STMT_PREPARE / COM_STMT_EXECUTE) hung on the second execution: the
// cached compile retained stale operator state (hashbuild ctr, dispatch
// channels), leaving scan receivers blocked in waitForRuntimeFilters.
// interpolateParams=false forces the driver onto protocol-level prepared
// statements, and both executions run on the same prepared handle.
func TestIssue25526PreparedUpdateJoinSecondExecute(t *testing.T) {
	embed.RunBaseClusterTests(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn1.GetServiceConfig().CN.Frontend.Port
			dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer db.Close()

			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			dbName := testutils.GetDatabaseName(t)
			mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
			mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
			defer func() {
				// Best effort: after a hang the session may be wedged, so use a
				// fresh short context and tolerate failure.
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second*10)
				defer cleanupCancel()
				_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
			}()

			mustExec(t, ctx, conn, "create table parent(pid int primary key, code varchar(16) unique)")
			mustExec(t, ctx, conn, `create table acct(
				tenant int not null,
				acct_id int not null,
				status varchar(16),
				amount decimal(12,2),
				parent_id int,
				primary key(tenant, acct_id),
				key idx_status_amount(status, amount),
				key idx_parent_status(parent_id, status),
				constraint fk_acct_parent foreign key(parent_id) references parent(pid))`)
			mustExec(t, ctx, conn, "insert into parent values (1,'p1'),(2,'p2')")
			mustExec(t, ctx, conn, "insert into acct values (1,101,'open',10.50,1),(2,201,'open',30.75,2)")

			stmt, err := conn.PrepareContext(ctx,
				"update acct a join parent p on a.parent_id=p.pid set a.amount=a.amount+?, a.status=? where p.code=? and a.status=?")
			require.NoError(t, err)
			defer stmt.Close()

			for _, args := range [][]any{
				{"3.33", "closed", "p1", "open"},
				{"4.44", "closed", "p2", "open"},
			} {
				execCtx, execCancel := context.WithTimeout(ctx, time.Second*30)
				res, err := stmt.ExecContext(execCtx, args...)
				execCancel()
				require.NoError(t, err, "prepared execute with args %v must not hang", args)
				affected, err := res.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), affected)
			}

			rows, err := conn.QueryContext(ctx, "select status, amount from acct order by tenant")
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var status, amount string
				require.NoError(t, rows.Scan(&status, &amount))
				got = append(got, status+":"+amount)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []string{"closed:13.83", "closed:35.19"}, got)

			// Keep the same real COM_STMT path for the multi-row self-referencing
			// REPLACE cases that exercise recursive FK action remapping.
			mustExec(t, ctx, conn, `create table self_cascade(
				id int primary key,
				pid int,
				key idx_pid(pid),
				foreign key(pid) references self_cascade(id) on delete cascade)`)
			mustExec(t, ctx, conn, "insert into self_cascade values(1,null),(2,1),(3,2)")
			cascadeStmt, err := conn.PrepareContext(ctx,
				"replace into self_cascade values(?,?),(?,?)")
			require.NoError(t, err)
			defer cascadeStmt.Close()
			_, err = cascadeStmt.ExecContext(ctx, 1, nil, 2, 1)
			require.NoError(t, err)
			var cascadeRows int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from self_cascade").Scan(&cascadeRows))
			require.Equal(t, 2, cascadeRows)

			mustExec(t, ctx, conn, `create table self_setnull(
				id int primary key,
				pid int,
				unique key uk_pid(pid),
				foreign key(pid) references self_setnull(id) on delete set null)`)
			mustExec(t, ctx, conn, "insert into self_setnull values(1,null),(2,1),(3,2)")
			setNullStmt, err := conn.PrepareContext(ctx,
				"replace into self_setnull values(?,?),(?,?)")
			require.NoError(t, err)
			defer setNullStmt.Close()
			_, err = setNullStmt.ExecContext(ctx, 2, 1, 1, nil)
			require.NoError(t, err)
			var id2Rows, id2NullRows int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*), count(*) - count(pid) from self_setnull where id=2").Scan(&id2Rows, &id2NullRows))
			require.Equal(t, 1, id2Rows)
			require.Equal(t, 1, id2NullRows)

			// Recursive action graphs must report the FK violation and roll the
			// statement back instead of failing during remapping or execution.
			mustExec(t, ctx, conn, "create table nested_parent(id int primary key)")
			mustExec(t, ctx, conn, `create table nested_child(
				id int primary key,
				pid int,
				foreign key(pid) references nested_parent(id) on delete cascade)`)
			mustExec(t, ctx, conn, `create table nested_guard(
				id int primary key,
				cid int,
				foreign key(cid) references nested_child(id) on delete restrict)`)
			mustExec(t, ctx, conn, "insert into nested_parent values(1)")
			mustExec(t, ctx, conn, "insert into nested_child values(1,1)")
			mustExec(t, ctx, conn, "insert into nested_guard values(1,1)")
			_, err = conn.ExecContext(ctx, "replace into nested_parent values(1)")
			require.Error(t, err)
			for _, table := range []string{"nested_parent", "nested_child", "nested_guard"} {
				var rows int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+table).Scan(&rows))
				require.Equal(t, 1, rows, "failed nested action must preserve %s", table)
			}

			mustExec(t, ctx, conn, "create table cycle_a(id int primary key, bid int)")
			mustExec(t, ctx, conn, `create table cycle_b(
				id int primary key,
				aid int,
				foreign key(aid) references cycle_a(id) on delete cascade)`)
			mustExec(t, ctx, conn, `alter table cycle_a add constraint fk_cycle_a_b
				foreign key(bid) references cycle_b(id) on delete cascade`)
			mustExec(t, ctx, conn, "insert into cycle_a values(1,null)")
			mustExec(t, ctx, conn, "insert into cycle_b values(1,1)")
			mustExec(t, ctx, conn, "update cycle_a set bid=1 where id=1")
			_, err = conn.ExecContext(ctx, "replace into cycle_a values(1,1)")
			require.Error(t, err)
			for _, table := range []string{"cycle_a", "cycle_b"} {
				var rows int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+table).Scan(&rows))
				require.Equal(t, 1, rows, "failed cycle action must preserve %s", table)
			}

			// Existing ON UPDATE CASCADE plans keep their positional table layout.
			mustExec(t, ctx, conn, "create table update_parent(id int primary key)")
			mustExec(t, ctx, conn, `create table update_child(
				id int primary key,
				foreign key(id) references update_parent(id) on update cascade)`)
			mustExec(t, ctx, conn, `create table update_grandchild(
				id int primary key,
				foreign key(id) references update_child(id) on update cascade)`)
			mustExec(t, ctx, conn, "insert into update_parent values(1)")
			mustExec(t, ctx, conn, "insert into update_child values(1)")
			mustExec(t, ctx, conn, "insert into update_grandchild values(1)")
			mustExec(t, ctx, conn, "update update_parent set id=2 where id=1")
			for _, table := range []string{"update_parent", "update_child", "update_grandchild"} {
				var id int
				require.NoError(t, conn.QueryRowContext(ctx, "select id from "+table).Scan(&id))
				require.Equal(t, 2, id, "ON UPDATE CASCADE must update %s", table)
			}

			// A transitive three-table cycle must be detected after the actions,
			// before an orphan replacement image can commit.
			mustExec(t, ctx, conn, "create table transitive_a(id int primary key, cid int)")
			mustExec(t, ctx, conn, "create table transitive_b(id int primary key, aid int)")
			mustExec(t, ctx, conn, "create table transitive_c(id int primary key, bid int)")
			mustExec(t, ctx, conn, `alter table transitive_b add foreign key(aid)
				references transitive_a(id) on delete cascade`)
			mustExec(t, ctx, conn, `alter table transitive_c add foreign key(bid)
				references transitive_b(id) on delete cascade`)
			mustExec(t, ctx, conn, `alter table transitive_a add foreign key(cid)
				references transitive_c(id) on delete cascade`)
			mustExec(t, ctx, conn, "insert into transitive_a values(1,null)")
			mustExec(t, ctx, conn, "insert into transitive_b values(1,1)")
			mustExec(t, ctx, conn, "insert into transitive_c values(1,1)")
			mustExec(t, ctx, conn, "update transitive_a set cid=1 where id=1")
			_, err = conn.ExecContext(ctx, "replace into transitive_a values(1,1)")
			require.Error(t, err)
			for _, table := range []string{"transitive_a", "transitive_b", "transitive_c"} {
				var rows int
				require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+table).Scan(&rows))
				require.Equal(t, 1, rows, "failed transitive cycle must preserve %s", table)
			}

			// Post-action validation is row-scoped: an unrelated historical orphan
			// created while checks were disabled must not reject a legal REPLACE.
			mustExec(t, ctx, conn, "create table scoped_a(id int primary key, bid int)")
			mustExec(t, ctx, conn, "create table scoped_b(id int primary key, aid int)")
			mustExec(t, ctx, conn, `alter table scoped_b add foreign key(aid)
				references scoped_a(id) on delete cascade`)
			mustExec(t, ctx, conn, `alter table scoped_a add foreign key(bid)
				references scoped_b(id) on delete cascade`)
			mustExec(t, ctx, conn, "set foreign_key_checks=0")
			mustExec(t, ctx, conn, "insert into scoped_a values(99,999)")
			mustExec(t, ctx, conn, "set foreign_key_checks=1")
			mustExec(t, ctx, conn, "replace into scoped_a values(2,null)")
			var scopedRows int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*) from scoped_a where id=2 and bid is null").Scan(&scopedRows))
			require.Equal(t, 1, scopedRows)

			// FK actions are completely disabled before any ordered-source rewrite.
			mustExec(t, ctx, conn, `create table checks_off(
				id int primary key, pid int,
				foreign key(pid) references checks_off(id) on delete set null)`)
			mustExec(t, ctx, conn, "insert into checks_off values(1,null),(2,1),(3,2)")
			mustExec(t, ctx, conn, "set foreign_key_checks=0")
			mustExec(t, ctx, conn, "replace into checks_off values(2,1),(1,null)")
			mustExec(t, ctx, conn, "set foreign_key_checks=1")
			var pid2, pid3 int
			require.NoError(t, conn.QueryRowContext(ctx, "select pid from checks_off where id=2").Scan(&pid2))
			require.NoError(t, conn.QueryRowContext(ctx, "select pid from checks_off where id=3").Scan(&pid3))
			require.Equal(t, 1, pid2)
			require.Equal(t, 2, pid3)

			// Ordered transformations consume the materialized row image. Volatile
			// expressions therefore execute once, and SELECT uses the same path.
			mustExec(t, ctx, conn, "create sequence replace_seq start with 100")
			mustExec(t, ctx, conn, `create table volatile_replace(
				id bigint primary key, pid bigint,
				foreign key(pid) references volatile_replace(id) on delete set null)`)
			mustExec(t, ctx, conn, `replace into volatile_replace values
				(nextval('replace_seq'),null),(nextval('replace_seq'),null)`)
			var sequenceValue int64
			require.NoError(t, conn.QueryRowContext(ctx, "select currval('replace_seq')").Scan(&sequenceValue))
			require.Equal(t, int64(101), sequenceValue)

			mustExec(t, ctx, conn, `create table select_setnull(
				id int primary key, pid int,
				foreign key(pid) references select_setnull(id) on delete set null)`)
			mustExec(t, ctx, conn, "insert into select_setnull values(1,null),(2,1),(3,2)")
			mustExec(t, ctx, conn, "create table replace_source(ord int, id int, pid int)")
			mustExec(t, ctx, conn, "insert into replace_source values(1,2,1),(2,1,null)")
			mustExec(t, ctx, conn, `replace into select_setnull
				select id,pid from replace_source order by ord`)
			var selectID2Null, selectID3Null int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*) from select_setnull where id=2 and pid is null").Scan(&selectID2Null))
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*) from select_setnull where id=3 and pid is null").Scan(&selectID3Null))
			require.Equal(t, 1, selectID2Null)
			require.Equal(t, 1, selectID3Null)

			mustExec(t, ctx, conn, `create table select_cascade(
				id int primary key, pid int,
				foreign key(pid) references select_cascade(id) on delete cascade)`)
			mustExec(t, ctx, conn, "insert into select_cascade values(1,null),(2,1),(3,2)")
			mustExec(t, ctx, conn, `replace into select_cascade
				select id,pid from replace_source order by ord`)
			var cascadeID1, cascadeOthers int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*) from select_cascade where id=1").Scan(&cascadeID1))
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*) from select_cascade where id<>1").Scan(&cascadeOthers))
			require.Equal(t, 1, cascadeID1)
			require.Equal(t, 0, cascadeOthers)
		},
	)
}
