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

	"github.com/go-sql-driver/mysql"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue26339ForeignKeyExecutionRegressions(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(3)

		const database = "issue_26339_fk_execution"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+database)
		}()
		execSQLRequire(t, ctx, db, "drop database if exists "+database)
		execSQLRequire(t, ctx, db, "create database "+database)
		// CREATE DATABASE commits before its catalog entry is necessarily visible to
		// a later FK DDL transaction. Wait for the CN's catalog cache so FK setup
		// cannot observe the transient ExpectedEOB state under CI load.
		testutils.WaitDatabaseCreated(t, database, cn)

		t.Run("optimistic parent update is rejected before concurrent child write", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table "+database+".optimistic_parent (id int primary key)")
			execSQLRequire(t, ctx, db, "create table "+database+".optimistic_child ("+
				"id int primary key, parent_id int, foreign key (parent_id) "+
				"references "+database+".optimistic_parent(id) on update cascade)")
			execSQLRequire(t, ctx, db, "insert into "+database+".optimistic_parent values (1)")

			rt := moruntime.ServiceRuntime(cn.ServiceID())
			oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
			oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
			rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Optimistic)
			rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_SI)
			defer func() {
				if hadMode {
					rt.SetGlobalVariables(moruntime.TxnMode, oldMode)
				} else {
					rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
				}
				if hadIsolation {
					rt.SetGlobalVariables(moruntime.TxnIsolation, oldIsolation)
				} else {
					rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
				}
			}()

			parentConn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer parentConn.Close()
			childConn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer childConn.Close()

			_, err = parentConn.ExecContext(ctx, "begin")
			require.NoError(t, err)
			defer func() {
				rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer rollbackCancel()
				_, _ = parentConn.ExecContext(rollbackCtx, "rollback")
			}()
			_, err = childConn.ExecContext(ctx, "begin")
			require.NoError(t, err)
			defer func() {
				rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer rollbackCancel()
				_, _ = childConn.ExecContext(rollbackCtx, "rollback")
			}()

			_, err = parentConn.ExecContext(ctx,
				"update "+database+".optimistic_parent set id = 2 where id = 1")
			require.Error(t, err)
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &mysqlErr)
			require.Equal(t, uint16(20105), mysqlErr.Number)
			require.Contains(t, err.Error(), "optimistic transaction")

			_, err = childConn.ExecContext(ctx,
				"insert into "+database+".optimistic_child values (10, 1)")
			require.NoError(t, err)
			_, err = childConn.ExecContext(ctx, "commit")
			require.NoError(t, err)
			_, err = parentConn.ExecContext(ctx, "rollback")
			require.NoError(t, err)

			// Verify the committed child transaction through the same session.
			// A fresh pooled connection has no last-commit timestamp and may race
			// asynchronous logtail replay under coverage load, which is unrelated
			// to the foreign-key closure this regression exercises.
			var parentID, childParentID, orphanCount int
			require.NoError(t, childConn.QueryRowContext(ctx,
				"select id from "+database+".optimistic_parent").Scan(&parentID))
			require.NoError(t, childConn.QueryRowContext(ctx,
				"select parent_id from "+database+".optimistic_child where id = 10").Scan(&childParentID))
			require.NoError(t, childConn.QueryRowContext(ctx,
				"select count(*) from "+database+".optimistic_child c left join "+
					database+".optimistic_parent p on c.parent_id = p.id where p.id is null").Scan(&orphanCount))
			require.Equal(t, 1, parentID)
			require.Equal(t, 1, childParentID)
			require.Zero(t, orphanCount)
		})

		t.Run("non unique referenced prefix validates actual child mappings", func(t *testing.T) {
			execSQLRequire(t, ctx, db, "create table "+database+".mapping_parent ("+
				"a int, b int, primary key (a, b))")
			execSQLRequire(t, ctx, db, "create table "+database+".mapping_child ("+
				"id int primary key, parent_a int, foreign key (parent_a) "+
				"references "+database+".mapping_parent(a) on update cascade)")

			// No child row means there is no referential-action target to make
			// ambiguous, even when changed parents share the same old prefix.
			execSQLRequire(t, ctx, db,
				"insert into "+database+".mapping_parent values (1, 1), (1, 2)")
			execSQLRequire(t, ctx, db,
				"update "+database+".mapping_parent set a = b + 1 where a = 1")
			var parentCount int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from "+database+".mapping_parent where (a, b) in ((2, 1), (3, 2))").
				Scan(&parentCount))
			require.Equal(t, 2, parentCount)

			execSQLRequire(t, ctx, db, "delete from "+database+".mapping_parent")
			execSQLRequire(t, ctx, db,
				"insert into "+database+".mapping_parent values (1, 1), (1, 2)")
			execSQLRequire(t, ctx, db,
				"insert into "+database+".mapping_child values (10, 1)")

			// Duplicate parent matches are safe when they produce one identical
			// new tuple for the child row.
			execSQLRequire(t, ctx, db,
				"update "+database+".mapping_parent set a = 2 where a = 1")
			var childParent int
			require.NoError(t, db.QueryRowContext(ctx,
				"select parent_a from "+database+".mapping_child where id = 10").Scan(&childParent))
			require.Equal(t, 2, childParent)

			execSQLRequire(t, ctx, db, "delete from "+database+".mapping_child")
			execSQLRequire(t, ctx, db, "delete from "+database+".mapping_parent")
			execSQLRequire(t, ctx, db,
				"insert into "+database+".mapping_parent values (1, 1), (1, 2)")
			execSQLRequire(t, ctx, db,
				"insert into "+database+".mapping_child values (10, 1)")

			// Distinct outputs for the same child remain ambiguous and the whole
			// statement must roll back.
			_, updateErr := db.ExecContext(ctx,
				"update "+database+".mapping_parent set a = b + 1 where a = 1")
			require.Error(t, updateErr)
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, updateErr, &mysqlErr)
			require.Equal(t, uint16(20105), mysqlErr.Number)
			require.Contains(t, updateErr.Error(), "ambiguous non-unique referenced-key mapping")

			var originalParentCount, orphanCount int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from "+database+".mapping_parent where a = 1").Scan(&originalParentCount))
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from "+database+".mapping_child c left join "+
					database+".mapping_parent p on c.parent_a = p.a where p.a is null").Scan(&orphanCount))
			require.Equal(t, 2, originalParentCount)
			require.Zero(t, orphanCount)

			execSQLRequire(t, ctx, db, "create table "+database+".set_null_parent ("+
				"a int, b int, primary key (a, b))")
			execSQLRequire(t, ctx, db, "create table "+database+".set_null_child ("+
				"id int primary key, parent_a int, foreign key (parent_a) "+
				"references "+database+".set_null_parent(a) on update set null)")
			execSQLRequire(t, ctx, db,
				"insert into "+database+".set_null_parent values (1, 1), (1, 2)")
			execSQLRequire(t, ctx, db,
				"insert into "+database+".set_null_child values (10, 1)")
			execSQLRequire(t, ctx, db,
				"update "+database+".set_null_parent set a = b + 1 where a = 1")
			var nullChildCount int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from "+database+".set_null_child where id = 10 and parent_a is null").
				Scan(&nullChildCount))
			require.Equal(t, 1, nullChildCount)
		})

		t.Run("prepared implicit on update observes enabled foreign key checks", func(t *testing.T) {
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			execConn := func(statement string) error {
				_, execErr := conn.ExecContext(ctx, statement)
				return execErr
			}
			require.NoError(t, execConn("create table "+database+".prepared_parent (id datetime primary key)"))
			require.NoError(t, execConn("create table "+database+".prepared_child ("+
				"id int primary key, parent_id datetime on update current_timestamp, note int, "+
				"foreign key (parent_id) references "+database+".prepared_parent(id))"))
			require.NoError(t, execConn(
				"insert into "+database+".prepared_parent values ('2000-01-01 00:00:00')"))
			require.NoError(t, execConn(
				"insert into "+database+".prepared_child values (10, '2000-01-01 00:00:00', 1)"))

			require.NoError(t, execConn("set foreign_key_checks = 0"))
			require.NoError(t, execConn("prepare stmt_26339 from 'update "+database+
				".prepared_child set note = 2 where id = 10'"))
			defer func() {
				deallocateCtx, deallocateCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer deallocateCancel()
				_, _ = conn.ExecContext(deallocateCtx, "deallocate prepare stmt_26339")
			}()
			require.NoError(t, execConn("set foreign_key_checks = 1"))
			executeErr := execConn("execute stmt_26339")
			require.Error(t, executeErr)
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, executeErr, &mysqlErr)
			require.Equal(t, uint16(1452), mysqlErr.Number)
			require.True(t,
				strings.Contains(executeErr.Error(), "Cannot add or update a child row") ||
					strings.Contains(executeErr.Error(), "foreign key constraint fails"),
				"expected a foreign-key violation, got %v", executeErr)

			var parentID, childParentID string
			var note, orphanCount int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(id as char) from "+database+".prepared_parent").Scan(&parentID))
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(parent_id as char), note from "+database+".prepared_child where id = 10").
				Scan(&childParentID, &note))
			require.NoError(t, conn.QueryRowContext(ctx,
				"select count(*) from "+database+".prepared_child c left join "+
					database+".prepared_parent p on c.parent_id = p.id where p.id is null").Scan(&orphanCount))
			require.Equal(t, "2000-01-01 00:00:00", parentID)
			require.Equal(t, parentID, childParentID)
			require.Equal(t, 1, note)
			require.Zero(t, orphanCount)
		})
	})
}
