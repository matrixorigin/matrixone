// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestRenameTableImplicitCommit(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			_ = conn.Close()
			_ = db.Close()
			cleanupSQLIntegration(t, cn, "drop database if exists issue29295")
		}()
		exec := func(s string) { t.Helper(); _, err := conn.ExecContext(ctx, s); require.NoError(t, err, s) }
		// db uses a different connection from the pinned writer. Every read
		// therefore proves committed visibility, not just the writer's workspace.
		count := func(s string, want int) {
			t.Helper()
			var got int
			require.NoError(t, db.QueryRowContext(ctx, s).Scan(&got))
			require.Equal(t, want, got, s)
		}
		marker := func(id, want int) {
			t.Helper()
			count(fmt.Sprintf("select count(*) from issue29295.markers where id=%d", id), want)
		}
		table := func(name string, want int) {
			t.Helper()
			count("select count(*) from information_schema.tables where table_schema='issue29295' and table_name='"+name+"'", want)
		}
		exec("create database issue29295")
		exec("use issue29295")
		exec("create table markers(id int primary key)")
		exec("create table nc_workflow_executions(id int primary key)")
		exec("begin")
		exec("insert into markers values(1)")
		exec("rename table nc_workflow_executions to nc_automation_executions")
		exec("rollback")
		marker(1, 1)
		table("nc_workflow_executions", 0)
		table("nc_automation_executions", 1)

		// A failed DDL still commits preceding work but publishes no rename.
		exec("create table occupied(id int)")
		exec("begin")
		exec("insert into markers values(2)")
		_, err = conn.ExecContext(ctx, "rename table nc_automation_executions to occupied")
		require.Error(t, err)
		exec("rollback")
		marker(2, 1)
		table("nc_automation_executions", 1)
		exec("begin")
		exec("insert into markers values(3)")
		_, err = conn.ExecContext(ctx, "rename table nc_automation_executions to moved, absent to another")
		require.Error(t, err)
		exec("rollback")
		marker(3, 1)
		table("nc_automation_executions", 1)
		table("moved", 0)

		// PREPARE itself cannot commit; EXECUTE must use the saved DDL policy.
		exec("begin")
		exec("insert into markers values(4)")
		exec("prepare rename_stmt from 'rename table nc_automation_executions to prepared_name'")
		marker(4, 0)
		exec("execute rename_stmt")
		exec("rollback")
		marker(4, 1)
		table("prepared_name", 1)
		exec("deallocate prepare rename_stmt")
		exec("begin")
		exec("insert into markers values(5)")
		prepared, err := conn.PrepareContext(ctx, "rename table prepared_name to binary_name")
		require.NoError(t, err)
		marker(5, 0)
		_, err = prepared.ExecContext(ctx)
		require.NoError(t, err)
		require.NoError(t, prepared.Close())
		exec("rollback")
		marker(5, 1)
		table("binary_name", 1)

		exec("begin")
		exec("insert into markers values(6)")
		_, err = conn.ExecContext(ctx, "execute missing_prepared_stmt")
		require.Error(t, err)
		marker(6, 0)
		exec("rollback")
		marker(6, 0)

		// Reset the implicit-commit flag between statements in one COM_QUERY.
		exec("set autocommit=0")
		exec("insert into markers values(7); rename table binary_name to chain_tmp, chain_tmp to final_name; insert into markers values(8); rollback")
		marker(7, 1)
		marker(8, 0)
		table("final_name", 1)
		var auto int
		require.NoError(t, conn.QueryRowContext(ctx, "select @@autocommit").Scan(&auto))
		require.Zero(t, auto)
		exec("insert into markers values(9)")
		_, err = conn.ExecContext(ctx, "rename table final_name to occupied")
		require.Error(t, err)
		exec("rollback")
		marker(9, 1)
		table("final_name", 1)
		exec("set autocommit=1")

		// Internal execution retains the enclosing transaction's ownership.
		sentinel := errors.New("rollback shared rename")
		err = testutils.GetSQLExecutor(cn).ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, s := range []string{"insert into markers values(10)", "rename table final_name to internal_name"} {
				r, err := tx.Exec(s, executor.StatementOption{})
				if err != nil {
					return err
				}
				r.Close()
			}
			return sentinel
		}, executor.Options{}.WithDatabase("issue29295"))
		require.ErrorIs(t, err, sentinel)
		marker(10, 0)
		table("final_name", 1)
		table("internal_name", 0)

		// Migration DDL created in the preceding transaction must be visible
		// to the fresh rename transaction after its implicit precommit.
		exec("begin")
		exec("create table created_in_txn(id int)")
		exec("insert into created_in_txn values(11)")
		exec("rename table created_in_txn to migrated")
		exec("rollback")
		table("created_in_txn", 0)
		count("select count(*) from issue29295.migrated where id=11", 1)
	})
}
