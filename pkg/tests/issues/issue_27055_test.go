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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

type issue27055Item struct {
	id     int
	value  string
	txnCol int
}

func TestIssue27055DatabaseCloneReadsExplicitTransactionSource(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const (
			sourceDB       = "issue_27055_source"
			targetDB       = "issue_27055_target"
			autocommitDB   = "issue_27055_autocommit"
			snapshotDB     = "issue_27055_snapshot"
			createdSource  = "issue_27055_created_source"
			createdTarget  = "issue_27055_created_target"
			rollbackSource = "issue_27055_rollback_source"
			rollbackTarget = "issue_27055_rollback_target"
			snapshotName   = "issue_27055_snapshot_point"
		)
		allDatabases := []string{
			targetDB, autocommitDB, snapshotDB, createdTarget, createdSource,
			rollbackTarget, rollbackSource, sourceDB,
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			_, _ = conn.ExecContext(cleanupCtx, "drop snapshot if exists "+snapshotName)
			for _, database := range allDatabases {
				_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+database)
			}
		}()

		exec := func(statement string) {
			_, execErr := conn.ExecContext(ctx, statement)
			require.NoError(t, execErr, statement)
		}
		items := func(database, table string) []issue27055Item {
			rows, queryErr := conn.QueryContext(ctx,
				"select id, v, txn_col from "+database+"."+table+" order by id")
			require.NoError(t, queryErr)
			defer rows.Close()

			var got []issue27055Item
			for rows.Next() {
				var item issue27055Item
				require.NoError(t, rows.Scan(&item.id, &item.value, &item.txnCol))
				got = append(got, item)
			}
			require.NoError(t, rows.Err())
			return got
		}
		assertItems := func(database, table string, want []issue27055Item) {
			require.Equal(t, want, items(database, table), database+"."+table)
		}

		exec("set role moadmin")
		exec("create database " + sourceDB)
		exec("create table " + sourceDB + ".items (id int primary key, v varchar(20))")
		exec("insert into " + sourceDB + ".items values (1, 'one'), (2, 'two'), (3, 'three')")

		// INSERT, UPDATE, DELETE, and ALTER must all be visible to the database clone.
		exec("begin")
		exec("insert into " + sourceDB + ".items values (4, 'four')")
		exec("update " + sourceDB + ".items set v = 'two-updated' where id = 2")
		exec("delete from " + sourceDB + ".items where id = 3")
		exec("alter table " + sourceDB + ".items add column txn_col int default 7")
		exec("create database " + targetDB + " clone " + sourceDB)
		transactionItems := []issue27055Item{
			{id: 1, value: "one", txnCol: 7},
			{id: 2, value: "two-updated", txnCol: 7},
			{id: 4, value: "four", txnCol: 7},
		}
		assertItems(sourceDB, "items", transactionItems)
		assertItems(targetDB, "items", transactionItems)
		exec("commit")
		assertItems(sourceDB, "items", transactionItems)
		assertItems(targetDB, "items", transactionItems)

		// Table clone remains a shared-transaction reader as established by #26293.
		exec("begin")
		exec("insert into " + sourceDB + ".items values (5, 'table-clone', 7)")
		exec("create table " + sourceDB + ".table_clone clone " + sourceDB + ".items")
		tableCloneItems := append(append([]issue27055Item(nil), transactionItems...),
			issue27055Item{id: 5, value: "table-clone", txnCol: 7})
		assertItems(sourceDB, "table_clone", tableCloneItems)
		exec("commit")

		// A table and its database created in the same transaction are cloneable.
		exec("begin")
		exec("create database " + createdSource)
		exec("create table " + createdSource + ".items (id int primary key, v varchar(20), txn_col int)")
		exec("insert into " + createdSource + ".items values (1, 'created', 9)")
		exec("create database " + createdTarget + " clone " + createdSource)
		assertItems(createdTarget, "items", []issue27055Item{{id: 1, value: "created", txnCol: 9}})
		exec("commit")

		// Autocommit retains the latest source state, while a named snapshot retains its point in time.
		exec("create database " + autocommitDB + " clone " + sourceDB)
		assertItems(autocommitDB, "items", tableCloneItems)
		exec("create snapshot " + snapshotName + " for database " + sourceDB)
		exec("insert into " + sourceDB + ".items values (6, 'after-snapshot', 7)")
		exec("create database " + snapshotDB + " clone " + sourceDB + " {snapshot = '" + snapshotName + "'}")
		assertItems(snapshotDB, "items", tableCloneItems)

		// Rollback removes both transaction-local source and cloned target databases.
		exec("begin")
		exec("create database " + rollbackSource)
		exec("create table " + rollbackSource + ".items (id int primary key, v varchar(20), txn_col int)")
		exec("insert into " + rollbackSource + ".items values (1, 'rollback', 11)")
		exec("create database " + rollbackTarget + " clone " + rollbackSource)
		exec("rollback")
		var count int
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_database where account_id = 0 and datname in (?, ?)",
			rollbackSource, rollbackTarget).Scan(&count))
		require.Zero(t, count)
	})
}
