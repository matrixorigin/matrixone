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
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIssue29133RenameAdmissionSerializesRoleRuleWrite(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		writerCN, err := c.GetCNService(0)
		require.NoError(t, err)
		ddlCN, err := c.GetCNService(1)
		require.NoError(t, err)

		openDB := func(port int64) *sql.DB {
			db, openErr := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
			require.NoError(t, openErr)
			db.SetMaxOpenConns(4)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		writerDB := openDB(writerCN.GetServiceConfig().CN.Frontend.Port)
		ddlDB := openDB(ddlCN.GetServiceConfig().CN.Frontend.Port)
		addDB := openDB(writerCN.GetServiceConfig().CN.Frontend.Port)
		writerExec := testutils.GetSQLExecutor(writerCN)

		database := testutils.GetDatabaseName(t)
		const role = "issue_29133_role"
		execSQLMaybe(t, ctx, writerDB, "drop role if exists `"+role+"`")
		execSQLMaybe(t, ctx, writerDB, "drop database if exists `"+database+"`")
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, writerDB, "drop role if exists `"+role+"`")
			execSQLMaybe(t, cleanupCtx, writerDB, "drop database if exists `"+database+"`")
		})
		execSQLRequire(t, ctx, writerDB, "create role `"+role+"`")
		execSQLRequire(t, ctx, writerDB, "create database `"+database+"`")
		execSQLRequire(t, ctx, writerDB, "create table `"+database+"`.`t` (id int primary key)")

		var roleID int64
		require.NoError(t, writerDB.QueryRowContext(ctx,
			"select role_id from mo_catalog.mo_role where role_name = ?", role).Scan(&roleID))
		var roleRuleTableID uint64
		require.NoError(t, writerDB.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = 'mo_catalog' and relname = 'mo_role_rule'").Scan(&roleRuleTableID))
		require.NotZero(t, roleRuleTableID)

		ddl, err := ddlDB.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, ddl.Close()) })
		require.NoError(t, execIssue29133(ctx, ddl, "set session transaction isolation level read committed"))

		ruleName := database + ".t"
		ruleSQL := "select * from " + database + ".t where id > 0"
		insertRule := func(tx executor.TxnExecutor) error {
			res, err := tx.Exec(fmt.Sprintf(
				"insert into mo_catalog.mo_role_rule (role_id, rule_name, `rule`) values (%d, '%s', '%s')",
				roleID, ruleName, ruleSQL,
			), executor.StatementOption{})
			res.Close()
			return err
		}
		deleteRule := func() {
			t.Helper()
			testutils.ExecSQL(t, "", writerCN,
				"delete from mo_catalog.mo_role_rule where role_id = "+fmt.Sprint(roleID)+
					" and rule_name = '"+ruleName+"'",
			)
			require.Eventually(t, func() bool {
				var count int
				return writerDB.QueryRowContext(ctx,
					"select count(*) from mo_catalog.mo_role_rule where role_id = ? and rule_name = ?",
					roleID, ruleName).Scan(&count) == nil && count == 0
			}, 15*time.Second, 10*time.Millisecond, "role rule cleanup did not commit")
		}
		renameSQL := "alter table `" + database + "`.`t` rename to `" + database + "`.`t_renamed`"

		t.Run("committed writer needs no wait", func(t *testing.T) {
			testutils.ExecSQL(t, "", writerCN,
				"insert into mo_catalog.mo_role_rule (role_id, rule_name, `rule`) values ("+
					fmt.Sprint(roleID)+", '"+ruleName+"', '"+ruleSQL+"')",
			)

			waiterQueued := make(chan struct{})
			var once sync.Once
			restoreHook := lockservice.SetWaiterEnqueuedHookForTest(func(
				tableID uint64, _ []byte, _ [][]byte,
			) {
				if tableID == roleRuleTableID {
					once.Do(func() { close(waiterQueued) })
				}
			})
			defer restoreHook()

			_, err = ddl.ExecContext(ctx, renameSQL)
			require.ErrorContains(t, err, "role rewrite rules exist")
			select {
			case <-waiterQueued:
				t.Fatal("rename waited despite the role-rule writer being committed")
			default:
			}
			deleteRule()
		})

		t.Run("uncommitted writer is excluded before query", func(t *testing.T) {
			writerReady := make(chan struct{})
			writerRelease := make(chan struct{})
			writerDone := make(chan error, 1)
			var releaseOnce sync.Once
			releaseWriter := func() { releaseOnce.Do(func() { close(writerRelease) }) }
			go func() {
				writerDone <- writerExec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
					if err := insertRule(txn); err != nil {
						return err
					}
					close(writerReady)
					select {
					case <-writerRelease:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}, executor.Options{}.WithAccountID(0))
			}()
			select {
			case <-writerReady:
			case <-ctx.Done():
				t.Fatal("role-rule writer did not acquire its transaction")
			}
			writerFinished := false
			defer func() {
				releaseWriter()
				if !writerFinished {
					require.NoError(t, <-writerDone)
				}
			}()

			waiterQueued := make(chan struct{})
			var once sync.Once
			restoreHook := lockservice.SetWaiterEnqueuedHookForTest(func(
				tableID uint64, _ []byte, _ [][]byte,
			) {
				if tableID == roleRuleTableID {
					once.Do(func() { close(waiterQueued) })
				}
			})
			defer restoreHook()

			renameCtx, cancelRename := context.WithCancel(ctx)
			defer cancelRename()
			renameDone := make(chan error, 1)
			go func() {
				_, renameErr := ddl.ExecContext(renameCtx, renameSQL)
				renameDone <- renameErr
			}()

			select {
			case <-waiterQueued:
			case <-ctx.Done():
				t.Fatal("rename did not enter the role-rule lock wait")
			}
			select {
			case renameErr := <-renameDone:
				t.Fatalf("rename returned before the role-rule writer committed: %v", renameErr)
			default:
			}

			releaseWriter()
			writerErr := <-writerDone
			writerFinished = true
			require.NoError(t, writerErr)
			select {
			case renameErr := <-renameDone:
				require.ErrorContains(t, renameErr, "role rewrite rules exist")
			case <-ctx.Done():
				t.Fatal("rename did not finish after the role-rule writer committed")
			}
			deleteRule()
		})

		t.Run("rename-first rejects a stale waiting add", func(t *testing.T) {
			writerReady := make(chan struct{})
			writerRelease := make(chan struct{})
			writerDone := make(chan error, 1)
			var releaseOnce sync.Once
			releaseWriter := func() { releaseOnce.Do(func() { close(writerRelease) }) }
			go func() {
				writerDone <- writerExec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
					res, err := txn.Exec(
						"insert into `"+database+"`.`t` values (29133)",
						executor.StatementOption{},
					)
					res.Close()
					if err != nil {
						return err
					}
					close(writerReady)
					select {
					case <-writerRelease:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}, executor.Options{}.WithAccountID(0))
			}()
			select {
			case <-writerReady:
			case <-ctx.Done():
				t.Fatal("target-table writer did not acquire its transaction")
			}
			writerFinished := false
			defer func() {
				releaseWriter()
				if !writerFinished {
					require.NoError(t, <-writerDone)
				}
			}()

			renameCatalogWaiterQueued := make(chan struct{})
			roleRuleWaiterQueued := make(chan struct{})
			var renameCatalogOnce, roleRuleOnce sync.Once
			restoreHook := lockservice.SetWaiterEnqueuedHookForTest(func(
				tableID uint64, _ []byte, _ [][]byte,
			) {
				switch tableID {
				case catalog.MO_TABLES_ID:
					renameCatalogOnce.Do(func() { close(renameCatalogWaiterQueued) })
				case roleRuleTableID:
					roleRuleOnce.Do(func() { close(roleRuleWaiterQueued) })
				}
			})
			defer restoreHook()

			renameCtx, cancelRename := context.WithCancel(ctx)
			defer cancelRename()
			renameDone := make(chan error, 1)
			go func() {
				_, renameErr := ddl.ExecContext(renameCtx, renameSQL)
				renameDone <- renameErr
			}()
			select {
			case <-renameCatalogWaiterQueued:
			case <-ctx.Done():
				t.Fatal("rename did not wait on the catalog lock after taking the role-rule gate")
			}

			addSQL := fmt.Sprintf(
				"alter role `%s` add rule \"%s\" on table `%s`.`t`",
				role, ruleSQL, database,
			)
			addDone := make(chan error, 1)
			go func() {
				_, addErr := addDB.ExecContext(ctx, addSQL)
				addDone <- addErr
			}()
			select {
			case addErr := <-addDone:
				t.Fatalf("ADD RULE returned before taking the role-rule lifecycle gate: %v", addErr)
			case <-roleRuleWaiterQueued:
			case <-ctx.Done():
				t.Fatal("ADD RULE did not wait on the rename's role-rule lifecycle gate")
			}
			select {
			case addErr := <-addDone:
				t.Fatalf("ADD RULE returned before rename committed: %v", addErr)
			default:
			}

			releaseWriter()
			writerErr := <-writerDone
			writerFinished = true
			require.NoError(t, writerErr)
			select {
			case renameErr := <-renameDone:
				require.NoError(t, renameErr)
			case <-ctx.Done():
				t.Fatal("rename did not finish after the target-table writer committed")
			}
			select {
			case addErr := <-addDone:
				require.ErrorContains(t, addErr, "there is no table")
			case <-ctx.Done():
				t.Fatal("waiting ADD RULE did not finish after rename committed")
			}

			var ruleCount int
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_role_rule where role_id = ? and rule_name = ?",
				roleID, ruleName).Scan(&ruleCount))
			require.Zero(t, ruleCount, "failed ADD RULE must not publish a stale rule")
			execSQLRequire(t, ctx, writerDB, "create table `"+database+"`.`t` (id int primary key)")
			require.NoError(t, writerDB.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_role_rule where role_id = ? and rule_name = ?",
				roleID, ruleName).Scan(&ruleCount))
			require.Zero(t, ruleCount, "recreating the old table must not activate a stale rule")
		})
	})
}

func execIssue29133(ctx context.Context, conn *sql.Conn, statement string) error {
	_, err := conn.ExecContext(ctx, statement)
	return err
}
