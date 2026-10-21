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

package embed

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCheckConstraintNamespace(t *testing.T) {
	defer func() { require.NoError(t, CloseBaseClusterTests()) }()
	RunBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(t *testing.T, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		execFreshSI := func(t *testing.T, statement string) error {
			t.Helper()
			_, err := execCheckNamespaceSI(ctx, func(attempt context.Context, sql string) error {
				_, err := conn.ExecContext(attempt, sql)
				return err
			}, statement)
			return err
		}
		expectError := func(t *testing.T, statement string, code uint16) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			var serverError *mysql.MySQLError
			require.True(t, errors.As(err, &serverError), "%s: %v", statement, err)
			require.Equal(t, code, serverError.Number, "%s: %v", statement, err)
		}
		count := func(t *testing.T, query string, want int) {
			t.Helper()
			var actual int
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&actual), query)
			require.Equal(t, want, actual, query)
		}
		const database = "check_namespace_review"
		exec(t, "create database "+database)
		exec(t, "use "+database)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			_, err := db.ExecContext(cleanupCtx, "drop snapshot if exists check_namespace_legacy")
			require.NoError(t, err)
			_, err = db.ExecContext(cleanupCtx, "drop database if exists "+database)
			require.NoError(t, err)
		}()

		t.Run("legacy SQL-only owners reject new claims", func(t *testing.T) {
			defer func() {
				// The artificial old-format rows must not affect later restore
				// scenarios, including when an admission assertion fails.
				for _, table := range []string{"legacy_duplicate", "structured_duplicate", "renamed_legacy", "sql_only_owner", "legacy_add_target", "rename_legacy_source", "structured_owner"} {
					exec(t, "drop table if exists "+table)
				}
			}()
			svc := cn.RawService().(cnservice.Service)
			res, err := svc.GetSQLExecutor().Exec(defines.AttachAccountId(ctx, 0),
				"create table "+database+".sql_only_owner(x int, constraint legacy_create check(x>0), constraint legacy_add check(x<10), constraint renamed_legacy_chk_1 check(x<>5))",
				executor.Options{}.WithWaitCommittedLogApplied().WithAdjustTableExtraFunc(func(extra *api.SchemaExtra) error {
					extra.Checks = nil
					return nil
				}))
			require.NoError(t, err)
			res.Close()
			var encoded []byte
			var createSQL string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select extra_info, rel_createsql from mo_catalog.mo_tables where reldatabase=? and relname='sql_only_owner'", database).Scan(&encoded, &createSQL))
			var extra api.SchemaExtra
			require.NoError(t, extra.Unmarshal(encoded))
			require.Empty(t, extra.Checks)
			require.Contains(t, createSQL, "legacy_create")
			count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='sql_only_owner' and constraint_type='CHECK'", 3)
			expectError(t, "create table legacy_duplicate(x int, constraint LEGACY_CREATE check(x<0))", 3822)
			exec(t, "create table legacy_add_target(x int)")
			expectError(t, "alter table legacy_add_target add constraint legacy_add check(x>0)", 3822)
			exec(t, "create table rename_legacy_source(x int check(x>0))")
			expectError(t, "rename table rename_legacy_source to renamed_legacy", 3822)
			count(t, "select count(*) from information_schema.tables where table_schema='"+database+"' and table_name in ('legacy_duplicate','renamed_legacy')", 0)
			count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='legacy_add_target' and constraint_type='CHECK'", 0)
			exec(t, "create table structured_owner(x int, constraint structured_ck check(x>0))")
			expectError(t, "create table structured_duplicate(x int, constraint structured_ck check(x<0))", 3822)
			count(t, "select count(*) from information_schema.table_constraints t join information_schema.check_constraints c on t.constraint_schema=c.constraint_schema and t.constraint_name=c.constraint_name where t.table_schema='"+database+"' and t.table_name='sql_only_owner'", 3)
			expectError(t, "insert into rename_legacy_source values(-1)", 3819)
		})

		t.Run("legacy source identities survive combined alter", func(t *testing.T) {
			defer func() {
				for _, table := range []string{"legacy_move", "moved", "move_owner", "legacy_a", "legacy_b", "distinct_owner", "legacy_plain", "moved_plain", "final_plain"} {
					exec(t, "drop table if exists "+table)
				}
			}()
			svc := cn.RawService().(cnservice.Service)
			legacy := func(t *testing.T, name, check string) {
				t.Helper()
				attempts := 0
				// Only fixture installation may replay historical duplicate names;
				// the ALTER statements below use the ordinary public connection.
				res, err := svc.GetSQLExecutor().Exec(defines.AttachAccountId(ctx, 0),
					"create table "+database+"."+name+"(x int, constraint "+check+" check(x>0))",
					executor.Options{}.WithWaitCommittedLogApplied().
						WithStatementOption(executor.StatementOption{}.WithTrustedCheckConstraintNames()).WithAdjustTableExtraFunc(func(extra *api.SchemaExtra) error {
						attempts++
						if attempts == 1 {
							return moerr.NewTxnNeedRetryNoCtx()
						}
						extra.Checks = nil
						return nil
					}))
				require.NoError(t, err)
				res.Close()
				require.GreaterOrEqual(t, attempts, 2, "legacy fixture must survive a physical-attempt retry")
				var encoded []byte
				var createSQL string
				require.NoError(t, conn.QueryRowContext(ctx,
					"select extra_info, rel_createsql from mo_catalog.mo_tables where reldatabase=? and relname=?", database, name).Scan(&encoded, &createSQL))
				var extra api.SchemaExtra
				require.NoError(t, extra.Unmarshal(encoded))
				require.Empty(t, extra.Checks)
				require.Contains(t, createSQL, check)
			}
			t.Run("combined rename", func(t *testing.T) {
				legacy(t, "legacy_move", "legacy_move_chk_1")
				exec(t, "insert into legacy_move values(1)")
				exec(t, "create table move_owner(x int, constraint moved_chk_1 check(x>0))")
				for _, statement := range []string{
					"alter table legacy_move rename to moved, rename column x to y",
					"alter table legacy_move rename column x to y, rename to moved",
				} {
					expectError(t, statement, 3822)
					count(t, "select count(*) from legacy_move where x=1", 1)
					count(t, "select count(*) from information_schema.tables where table_schema='"+database+"' and table_name='moved'", 0)
					count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and constraint_name='moved_chk_1'", 1)
					// This old-format fixture deliberately has no executable
					// structured checks. Rejection must preserve that catalog
					// representation, not manufacture an enforcement definition.
					var encoded []byte
					require.NoError(t, conn.QueryRowContext(ctx,
						"select extra_info from mo_catalog.mo_tables where reldatabase=? and relname='legacy_move'", database).Scan(&encoded))
					var extra api.SchemaExtra
					require.NoError(t, extra.Unmarshal(encoded))
					require.Empty(t, extra.Checks)
				}
				exec(t, "drop table move_owner")
				exec(t, "alter table legacy_move rename to moved, rename column x to y")
				count(t, "select count(*) from moved where y=1", 1)
				expectError(t, "insert into moved values(-1)", 3819)
			})
			t.Run("inherited add", func(t *testing.T) {
				legacy(t, "legacy_a", "__mo_chk_1")
				legacy(t, "legacy_b", "__mo_chk_1")
				exec(t, "insert into legacy_a values(5)")
				exec(t, "alter table legacy_a add constraint new_distinct check(x<10)")
				count(t, "select count(*) from legacy_a where x=5", 1)
				count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and constraint_name='__mo_chk_1'", 2)
				expectError(t, "insert into legacy_a values(-1)", 3819)
				expectError(t, "insert into legacy_a values(10)", 3819)
				expectError(t, "create table distinct_owner(x int, constraint new_distinct check(x>0))", 3822)
			})
			t.Run("plain legacy rename persists identities", func(t *testing.T) {
				legacy(t, "legacy_plain", "legacy_plain_chk_1")
				exec(t, "insert into legacy_plain values(5)")
				exec(t, "rename table legacy_plain to moved_plain")
				count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and constraint_name='legacy_plain_chk_1'", 0)
				count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and constraint_name='moved_plain_chk_1'", 1)
				var encoded []byte
				var createSQL string
				require.NoError(t, conn.QueryRowContext(ctx, "select extra_info,rel_createsql from mo_catalog.mo_tables where reldatabase=? and relname='moved_plain'", database).Scan(&encoded, &createSQL))
				var extra api.SchemaExtra
				require.NoError(t, extra.Unmarshal(encoded))
				require.Empty(t, extra.Checks, "renaming metadata must not invent executable CHECK definitions")
				require.Contains(t, createSQL, "moved_plain_chk_1")
				peer, err := cluster.GetCNService(1)
				require.NoError(t, err)
				peerDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", peer.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer peerDB.Close()
				var n int
				require.NoError(t, peerDB.QueryRowContext(ctx, "select count(*) from information_schema.check_constraints where constraint_schema=? and constraint_name='moved_plain_chk_1'", database).Scan(&n))
				require.Equal(t, 1, n, "a different CN must read the persisted identity")
				exec(t, "create table legacy_plain(x int check(x>0))")
				exec(t, "rename table moved_plain to final_plain")
				exec(t, "create table moved_plain(x int check(x>0))")
				count(t, "select count(*) from final_plain where x=5", 1)
				count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and constraint_name='final_plain_chk_1'", 1)
			})
			t.Run("chained rename validates current identities", func(t *testing.T) {
				for _, oldFormat := range []bool{false, true} {
					t.Run(fmt.Sprintf("legacy=%t", oldFormat), func(t *testing.T) {
						defer func() {
							for _, name := range []string{"chain_source", "chain_middle", "chain_final", "chain_owner"} {
								exec(t, "drop table if exists "+name)
							}
						}()
						if oldFormat {
							legacy(t, "chain_source", "chain_source_chk_1")
						} else {
							exec(t, "create table chain_source(x int check(x>0))")
						}
						exec(t, "insert into chain_source values(5)")
						exec(t, "create table chain_owner(x int, constraint CHAIN_FINAL_CHK_1 check(x<0))")
						var originalSQL string
						require.NoError(t, conn.QueryRowContext(ctx, "select rel_createsql from mo_catalog.mo_tables where reldatabase=? and relname='chain_source'", database).Scan(&originalSQL))
						expectError(t, "rename table chain_source to chain_middle, chain_middle to chain_final", 3822)
						var rolledBackSQL string
						require.NoError(t, conn.QueryRowContext(ctx, "select rel_createsql from mo_catalog.mo_tables where reldatabase=? and relname='chain_source'", database).Scan(&rolledBackSQL))
						require.Equal(t, originalSQL, rolledBackSQL, "failed later rename must roll back the earlier SQL rewrite")
						count(t, "select count(*) from chain_source where x=5", 1)
						count(t, "select count(*) from information_schema.tables where table_schema='"+database+"' and table_name in ('chain_middle','chain_final')", 0)
						count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and constraint_name='chain_source_chk_1'", 1)
						count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and lower(constraint_name)='chain_final_chk_1'", 1)
						exec(t, "drop table chain_owner")
						exec(t, "rename table chain_source to chain_middle, chain_middle to chain_final")
						count(t, "select count(*) from chain_final where x=5", 1)
						count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and constraint_name='chain_final_chk_1'", 1)
						exec(t, "create table chain_source(x int check(x>0))")
						if !oldFormat {
							expectError(t, "insert into chain_final values(-1)", 3819)
						}
					})
				}
			})
		})

		t.Run("repeatable read preserves check authoring", func(t *testing.T) {
			exec(t, "set session transaction_isolation='REPEATABLE-READ'")
			defer exec(t, "set session transaction_isolation='READ-COMMITTED'")
			// Drain known fixture cleanup once. Later unrelated commits remain
			// legal; only a new autocommit transaction can recover their SI abort.
			waitForCheckNamespaceCatalogCleanup(t, ctx, conn)
			execSI := func(statement string) {
				t.Helper()
				require.NoError(t, execFreshSI(t, statement), statement)
			}
			expectSIError := func(statement string, code uint16) {
				t.Helper()
				err := execFreshSI(t, statement)
				var serverError *mysql.MySQLError
				require.True(t, errors.As(err, &serverError), "%s: %v", statement, err)
				require.Equal(t, code, serverError.Number, "%s: %v", statement, err)
			}
			execSI("create table si_checked(x int check(x>0))")
			exec(t, "create table si_add(x int)")
			exec(t, "insert into si_add values(5)")
			execSI("alter table si_add add check(x>0)")
			execSI("create table si_like like si_checked")
			expectError(t, "insert into si_checked values(-1)", 3819)
			expectError(t, "insert into si_add values(-1)", 3819)
			expectError(t, "insert into si_like values(-1)", 3819)
			expectSIError("create table si_duplicate(x int, constraint si_checked_chk_1 check(x<10))", 3822)
			expectSIError("alter table si_add add constraint si_checked_chk_1 check(x<10)", 3822)
			count(t, "select count(*) from si_add where x=5", 1)
			// Failure occurs during COPY validation, after the name was
			// reserved. Autocommit rollback must preserve the old rows/checks
			// and release the claim for an unrelated destination.
			exec(t, "create table si_invalid(x int)")
			exec(t, "insert into si_invalid values(-1)")
			t.Run("fresh autocommit recovers a real namespace abort", func(t *testing.T) {
				peer, err := cluster.GetCNService(1)
				require.NoError(t, err)
				other, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", peer.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer other.Close()
				recoveryCtx, cancelRecovery := context.WithTimeout(ctx, 30*time.Second)
				defer cancelRecovery()
				var connectionID uint32
				require.NoError(t, conn.QueryRowContext(recoveryCtx, "select connection_id()").Scan(&connectionID))
				provider := cn.RawService().(interface {
					SessionMgr() *queryservice.SessionManager
				})
				const statement = "alter table si_invalid add constraint si_reusable check(x>0)"
				matches := func(id []byte) bool {
					for _, session := range provider.SessionMgr().GetAllSessions() {
						if identity, ok := session.(interface {
							GetConnectionID() uint32
							GetTxnId() uuid.UUID
							GetSqlOfStmt() string
						}); ok && identity.GetConnectionID() == connectionID && identity.GetSqlOfStmt() == statement {
							txnID := identity.GetTxnId()
							return bytes.Equal(txnID[:], id)
						}
					}
					return false
				}
				if !moruntime.InTesting(cn.ServiceID()) {
					moruntime.SetupServiceRuntimeTestingContext(cn.ServiceID())
				}
				tc := moruntime.MustGetTestingContext(cn.ServiceID())
				before, result := tc.GetBeforeLockFunc(), tc.GetAdjustLockResultFunc()
				defer tc.SetBeforeLockFunc(before)
				defer tc.SetAdjustLockResultFunc(result)
				var once sync.Once
				var mu sync.Mutex
				var firstTxn, nextTxn []byte
				var firstError, peerError error
				var firstGrant lock.Result
				observed := false
				tc.SetBeforeLockFunc(func(id []byte, tableID uint64) {
					before(id, tableID)
					if tableID == catalog.MO_TABLES_ID && matches(id) {
						once.Do(func() {
							_, err := other.ExecContext(recoveryCtx, "create table "+database+".si_intervening(x int primary key)")
							mu.Lock()
							firstTxn = append([]byte(nil), id...)
							peerError = err
							mu.Unlock()
						})
					}
				})
				tc.SetAdjustLockResultFunc(func(id []byte, tableID uint64, grant *lock.Result) {
					result(id, tableID, grant)
					if tableID == catalog.MO_TABLES_ID && matches(id) {
						mu.Lock()
						if bytes.Equal(id, firstTxn) {
							if !observed {
								firstGrant = *grant
								observed = true
							}
						} else {
							nextTxn = append(nextTxn[:0], id...)
						}
						mu.Unlock()
					}
				})
				attempts, terminal := execCheckNamespaceSI(recoveryCtx, func(attempt context.Context, sql string) error {
					_, err := conn.ExecContext(attempt, sql)
					if sql == statement {
						mu.Lock()
						if firstError == nil {
							firstError = err
						}
						mu.Unlock()
					}
					return err
				}, statement)
				mu.Lock()
				injectedErr, initialErr, original, replacement, grant, seen := peerError, firstError, append([]byte(nil), firstTxn...), append([]byte(nil), nextTxn...), firstGrant, observed
				mu.Unlock()
				require.NoError(t, injectedErr)
				require.NotEmpty(t, original, "inject into the target statement, not a background transaction")
				require.True(t, seen)
				require.False(t, grant.HasConflict)
				require.False(t, grant.HasPrevCommit)
				var serverError *mysql.MySQLError
				require.ErrorAs(t, initialErr, &serverError)
				require.EqualValues(t, 20619, serverError.Number)
				require.Equal(t, "w-w conflict: tableID: 2, catalog namespace snapshot is stale", serverError.Message)
				require.GreaterOrEqual(t, attempts, 2)
				require.NotEmpty(t, replacement)
				require.NotEqual(t, original, replacement, "recovery must create a new autocommit transaction")
				require.ErrorAs(t, terminal, &serverError)
				require.EqualValues(t, 3819, serverError.Number)
			})
			count(t, "select count(*) from si_invalid where x=-1", 1)
			count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='si_invalid' and constraint_type='CHECK'", 0)
			execSI("create table si_reused(x int, constraint si_reusable check(x>0))")
			exec(t, "insert into si_add values(11)")
		})

		t.Run("repeatable read rejects a catalog commit after its snapshot", func(t *testing.T) {
			peer, err := cluster.GetCNService(1)
			require.NoError(t, err)
			other, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", peer.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer other.Close()
			svc := cn.RawService().(cnservice.Service)
			barrier := svc.GetEngine().(interface {
				AcquireLogtailReadBarrier(context.Context) (timestamp.Timestamp, error)
			})
			waitForCheckNamespaceCatalogCleanup(t, ctx, conn)
			frontier, err := barrier.AcquireLogtailReadBarrier(ctx)
			require.NoError(t, err)
			if !moruntime.InTesting(cn.ServiceID()) {
				moruntime.SetupServiceRuntimeTestingContext(cn.ServiceID())
			}
			testingContext := moruntime.MustGetTestingContext(cn.ServiceID())
			var injectedErr error
			var originalSnapshot, finalSnapshot timestamp.Timestamp
			var grants []lock.Result
			var grantsMu sync.Mutex
			const statement = "create table si_after_snapshot(x int, constraint si_after_snapshot_ck check(x>0))"
			err = svc.GetSQLExecutor().ExecTxn(defines.AttachAccountId(ctx, 0), func(tx executor.TxnExecutor) error {
				op := tx.Txn()
				id := append([]byte(nil), op.Txn().ID...)
				originalSnapshot = op.Txn().SnapshotTS
				previousBefore := testingContext.GetBeforeLockFunc()
				previousResult := testingContext.GetAdjustLockResultFunc()
				defer testingContext.SetBeforeLockFunc(previousBefore)
				defer testingContext.SetAdjustLockResultFunc(previousResult)
				var inject sync.Once
				testingContext.SetBeforeLockFunc(func(txnID []byte, tableID uint64) {
					previousBefore(txnID, tableID)
					if tableID == catalog.MO_TABLES_ID && bytes.Equal(txnID, id) {
						inject.Do(func() {
							_, injectedErr = other.ExecContext(ctx, "create table "+database+".si_unrelated(x int)")
						})
					}
				})
				testingContext.SetAdjustLockResultFunc(func(txnID []byte, tableID uint64, result *lock.Result) {
					previousResult(txnID, tableID, result)
					if tableID == catalog.MO_TABLES_ID && bytes.Equal(txnID, id) {
						grantsMu.Lock()
						grants = append(grants, *result)
						grantsMu.Unlock()
					}
				})
				result, execErr := tx.Exec(statement, executor.StatementOption{})
				result.Close()
				finalSnapshot = op.Txn().SnapshotTS
				return execErr
			}, executor.Options{}.WithDatabase(database).WithMinCommittedTS(frontier).
				WithTxnIsolation(txnpb.TxnIsolation_SI).WithTxnMode(txnpb.TxnMode_Pessimistic))
			require.NoError(t, injectedErr)
			require.Equal(t, originalSnapshot, finalSnapshot, "SI admission must not advance a fixed snapshot")
			var conflict *moerr.Error
			require.ErrorAs(t, err, &conflict)
			require.Equal(t, moerr.ErrTxnWWConflict, conflict.ErrorCode())
			require.ErrorContains(t, err, "catalog namespace snapshot is stale")
			grantsMu.Lock()
			observed := append([]lock.Result(nil), grants...)
			grantsMu.Unlock()
			require.Len(t, observed, 1)
			require.False(t, observed[0].HasConflict)
			require.False(t, observed[0].HasPrevCommit)
			count(t, "select count(*) from information_schema.tables where table_schema='"+database+"' and table_name='si_after_snapshot'", 0)
			count(t, "select count(*) from information_schema.check_constraints where constraint_schema='"+database+"' and constraint_name='si_after_snapshot_ck'", 0)
			// The rejected transaction left no namespace claim behind. Once the
			// intervening commit is applied, recover in fresh autocommit
			// transactions; further unrelated commits can still abort admission.
			exec(t, "set session transaction_isolation='REPEATABLE-READ'")
			defer exec(t, "set session transaction_isolation='READ-COMMITTED'")
			require.NoError(t, execFreshSI(t, statement))
			expectError(t, "insert into si_after_snapshot values(-1)", 3819)
		})

		t.Run("copy alter validates only newly introduced names", func(t *testing.T) {
			exec(t, "create table a(x int, constraint ck check(x>0))")
			exec(t, "create table b(x int)")
			exec(t, "insert into b values(5)")
			for _, alter := range []string{
				"alter table b add constraint ck check(x<10)",
				"alter table b add constraint CK check(x<10), algorithm=copy",
			} {
				expectError(t, alter, 3822)
				count(t, "select count(*) from b where x=5", 1)
				count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='b' and constraint_type='CHECK'", 0)
			}
			exec(t, "alter table b add constraint ck_b check(x<10)")
			exec(t, "alter table b add column y int, algorithm=copy")
			count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='b' and constraint_name='ck_b'", 1)
		})

		t.Run("partition backing tables borrow parent identity", func(t *testing.T) {
			exec(t, "create table p(x int, constraint ck_part check(x>0)) partition by hash(x) partitions 2")
			exec(t, "insert into p values(1),(2)")
			exec(t, "rename table p to q")
			count(t, "select count(*) from q", 2)
			expectError(t, "insert into q values(-1)", 3819)
			expectError(t, "create table part_conflict(x int, constraint ck_part check(x>0))", 3822)
			exec(t, "alter table q add column y int, algorithm=copy")
			count(t, "select count(*) from q", 2)
		})

		t.Run("generated identifier boundaries", func(t *testing.T) {
			exec(t, "create table long_rename(x int check(x>0))")
			exec(t, "create table length_source(x int check(x>0))")
			for _, length := range []int{58, 59, 64} {
				name := strings.Repeat("t", length)
				if length > 58 {
					expectError(t, "create table "+name+"(x int check(x>0))", 1059)
					expectError(t, "rename table long_rename to "+name, 1059)
					expectError(t, "create table "+name+" like length_source", 1059)
					expectError(t, "create table "+name+" clone length_source", 1059)
					continue
				}
				exec(t, "create table "+name+"(x int check(x>0))")
				exec(t, "alter table "+name+" add column y int, algorithm=copy")
				exec(t, "drop table "+name)
				exec(t, "create table "+name+" like length_source")
				exec(t, "drop table "+name)
				exec(t, "create table "+name+" clone length_source")
				exec(t, "drop table "+name)
			}
		})

		t.Run("legacy snapshot restore preserves duplicate historical identities", func(t *testing.T) {
			svc := cn.RawService().(cnservice.Service)
			for _, name := range []string{"legacy_a", "legacy_b"} {
				// Emulate names persisted by an old CN; this internal fixture is
				// deliberately unavailable to ordinary user-authored DDL.
				res, err := svc.GetSQLExecutor().Exec(defines.AttachAccountId(ctx, 0),
					"create table "+database+"."+name+"(x int, constraint __mo_chk_1 check(x>0))",
					executor.Options{}.WithWaitCommittedLogApplied().WithStatementOption(executor.StatementOption{}.WithTrustedCheckConstraintNames()))
				require.NoError(t, err)
				res.Close()
				exec(t, "insert into "+name+" values(1)")
			}
			exec(t, "create snapshot check_namespace_legacy for database "+database)
			exec(t, "insert into legacy_a values(2)")
			exec(t, "restore database "+database+" {snapshot='check_namespace_legacy'}")
			exec(t, "use "+database)
			for _, name := range []string{"legacy_a", "legacy_b"} {
				count(t, "select count(*) from "+name, 1)
				count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='"+name+"' and constraint_name='__mo_chk_1'", 1)
				expectError(t, "insert into "+name+" values(-1)", 3819)
			}
			// Ordinary clones remain authors of new identities, even though
			// the clone executor also carries the frontend IsRestore marker.
			exec(t, "create table ordinary_clone clone legacy_a")
			count(t, "select count(*) from information_schema.table_constraints where table_schema='"+database+"' and table_name='ordinary_clone' and constraint_name='ordinary_clone_chk_1'", 1)
			exec(t, "alter table legacy_a add column y int, algorithm=copy")
			exec(t, "rename table legacy_a to legacy_renamed")
			count(t, "select count(*) from legacy_renamed", 1)
		})

		otherCN, err := cluster.GetCNService(1)
		require.NoError(t, err)
		otherDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", otherCN.GetServiceConfig().CN.Frontend.Port, database))
		require.NoError(t, err)
		defer otherDB.Close()
		for _, isolation := range []string{"READ-COMMITTED", "REPEATABLE-READ"} {
			for _, commit := range []bool{true, false} {
				t.Run(fmt.Sprintf("concurrent namespace %s owner commit=%t", isolation, commit), func(t *testing.T) {
					checkConcurrentConstraintClaim(t, ctx, conn, otherDB, cn.ServiceID(), otherCN, isolation, commit)
				})
			}
		}
	})
}

func checkConcurrentConstraintClaim(t *testing.T, parent context.Context, owner *sql.Conn, db *sql.DB, ownerServiceID string, cn ServiceOperator, isolation string, commit bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	writer, err := db.Conn(ctx)
	require.NoError(t, err)
	defer writer.Close()
	exec := func(conn *sql.Conn, query string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	exec(writer, "set session transaction_isolation='"+isolation+"'")
	for _, table := range []string{"concurrent_a", "concurrent_b"} {
		exec(owner, "drop table if exists "+table)
	}
	if isolation == "REPEATABLE-READ" {
		waitForCheckNamespaceCatalogCleanup(t, ctx, owner)
	}
	exec(owner, "begin")
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		_, err := owner.ExecContext(cleanupCtx, "rollback")
		require.NoError(t, err)
	}()
	// These empty tables test namespace admission, so an explicit PK avoids
	// scheduling unrelated hidden auto-increment cleanup between scenarios.
	exec(owner, "create table concurrent_a(x int primary key, constraint shared_ck check(x>0))")
	if isolation == "READ-COMMITTED" {
		exec(writer, "begin")
	}
	// An SI autocommit DDL starts its fixed snapshot when the statement
	// begins. Explicit active-SI lifecycle DDL is a pre-existing frontend
	// restriction, independent of CHECK-name admission.
	var connectionID uint32
	require.NoError(t, writer.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
	provider := cn.RawService().(interface {
		SessionMgr() *queryservice.SessionManager
	})
	writerTxn := func() []byte {
		for _, session := range provider.SessionMgr().GetAllSessions() {
			if identity, ok := session.(interface {
				GetTxnId() uuid.UUID
				GetConnectionID() uint32
			}); ok {
				id := identity.GetTxnId()
				if id != (uuid.UUID{}) && identity.GetConnectionID() == connectionID {
					return append([]byte(nil), id[:]...)
				}
			}
		}
		return nil
	}
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(0)
	for _, name := range []string{"check_namespace_review", "CHECK_CONSTRAINT", "shared_ck"} {
		packer.EncodeStringType([]byte(name))
	}
	// lockop encodes the serialized VARCHAR primary key once more as a
	// string before handing it to the lock service.
	tuple := packer.Bytes()
	packer.Reset()
	packer.EncodeStringType(tuple)
	key := packer.GetBuf()
	services := []lockservice.LockService{
		lockservice.GetLockServiceByServiceID(ownerServiceID),
		lockservice.GetLockServiceByServiceID(cn.ServiceID()),
	}
	for _, service := range services {
		require.NotNil(t, service)
	}
	done := make(chan struct{})
	var writerErr error
	go func() {
		_, writerErr = writer.ExecContext(ctx, "create table concurrent_b(x int primary key, constraint SHARED_CK check(x<10))")
		close(done)
	}()
	defer func() {
		cancel()
		// Join the SQL actor before returning its session.
		<-done
	}()
	// Observe the exact normalized CHECK key in the real lock-service queue,
	// not a timing window or an assumed ordinal of catalog lock callbacks.
	require.Eventually(t, func() bool {
		txnID := writerTxn()
		if len(txnID) == 0 {
			return false
		}
		found := false
		for _, service := range services {
			service.IterLocks(func(tableID uint64, keys [][]byte, held lockservice.Lock) bool {
				if tableID == catalog.MO_TABLES_ID && len(keys) == 1 && bytes.Equal(keys[0], key) {
					held.IterWaiters(func(waiter lock.WaitTxn) bool {
						found = found || bytes.Equal(waiter.TxnID, txnID)
						return !found
					})
				}
				return !found
			})
		}
		return found
	}, 10*time.Second, 5*time.Millisecond, "second CN must wait on the CHECK identity held by the first transaction")
	if commit {
		exec(owner, "commit")
	} else {
		exec(owner, "rollback")
	}
	select {
	case <-done:
		if commit {
			if isolation == "READ-COMMITTED" {
				var serverError *mysql.MySQLError
				require.True(t, errors.As(writerErr, &serverError), "%v", writerErr)
				require.Equal(t, uint16(3822), serverError.Number)
			} else {
				require.ErrorContains(t, writerErr, "w-w conflict", "SI must reject its stale snapshot instead of refreshing it")
			}
			exec(writer, "rollback")
		} else {
			require.NoError(t, writerErr)
			exec(writer, "commit")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// waitForCheckNamespaceCatalogCleanup drains visible orphan allocator rows
// from earlier fixture DROP/COPY statements. Neither this observation nor
// SYNCCOMMIT excludes later/in-flight commits from a fresh SI execution.
func waitForCheckNamespaceCatalogCleanup(t *testing.T, ctx context.Context, conn *sql.Conn) {
	t.Helper()
	syncCommit := func() {
		t.Helper()
		_, err := conn.ExecContext(ctx, "select mo_ctl('cn', 'SYNCCOMMIT', '')")
		require.NoError(t, err)
	}
	syncCommit()
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		var pending int
		// Allocator rows use physical rel_id. COPY may retain an old logical
		// ID, which must not make its dropped physical allocator look live.
		err := conn.QueryRowContext(waitCtx, "select count(*) from mo_catalog.mo_increment_columns i left join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.rel_id is null").Scan(&pending)
		require.NoError(t, err)
		if pending == 0 {
			break
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("auto-increment cleanup still has %d orphan rows: %v", pending, waitCtx.Err())
		case <-tick.C:
		}
	}
	syncCommit()
}

// execCheckNamespaceSI is only for positive RR autocommit DDL assertions: no
// explicit transaction may be active. Each Exec return ends that transaction;
// recovery starts another, never refreshes its fixed snapshot. The namespace
// protocol permits this exact conservative abort even without lock contention.
func execCheckNamespaceSI(parent context.Context, exec func(context.Context, string) error, statement string) (int, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	attempts := 0
	var lastConflict error
	stopped := func(cause error) error {
		return fmt.Errorf("CHECK namespace recovery exhausted after %d attempts: %w", attempts, errors.Join(ctx.Err(), cause, lastConflict))
	}
	for {
		if ctx.Err() != nil {
			return attempts, stopped(nil)
		}
		if err := exec(ctx, "select mo_ctl('cn', 'SYNCCOMMIT', '')"); err != nil {
			return attempts, fmt.Errorf("CHECK namespace synchronization after %d attempts: %w", attempts, errors.Join(err, lastConflict))
		}
		if ctx.Err() != nil {
			return attempts, stopped(nil)
		}
		attempts++
		err := exec(ctx, statement)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return attempts, stopped(err)
		}
		var serverError *mysql.MySQLError
		if !errors.As(err, &serverError) || serverError.Number != 20619 || serverError.Message != "w-w conflict: tableID: 2, catalog namespace snapshot is stale" {
			return attempts, err
		}
		lastConflict = err
	}
}

func TestCheckNamespaceSIRecoveryControl(t *testing.T) {
	const statement = "create table t(x int)"
	const syncSQL = "select mo_ctl('cn', 'SYNCCOMMIT', '')"
	conflict := &mysql.MySQLError{Number: 20619, Message: "w-w conflict: tableID: 2, catalog namespace snapshot is stale"}
	t.Run("only exact namespace abort is recoverable", func(t *testing.T) {
		for _, terminal := range []error{nil, errors.New("connection failure"), &mysql.MySQLError{Number: 3822, Message: "duplicate name"}, &mysql.MySQLError{Number: 3819, Message: "check violation"}, &mysql.MySQLError{Number: 20619, Message: "other write conflict"}, &mysql.MySQLError{Number: 20619, Message: "w-w conflict: tableID: 3, catalog namespace snapshot is stale"}} {
			calls := 0
			attempts, err := execCheckNamespaceSI(t.Context(), func(_ context.Context, q string) error {
				calls++
				if q == syncSQL {
					return nil
				}
				require.Equal(t, statement, q)
				return terminal
			}, statement)
			require.Equal(t, 1, attempts)
			require.Equal(t, 2, calls)
			require.Equal(t, terminal, err)
		}
	})
	t.Run("recover then preserve terminal result", func(t *testing.T) {
		var calls []string
		ddl := 0
		terminal := &mysql.MySQLError{Number: 3822}
		attempts, err := execCheckNamespaceSI(t.Context(), func(_ context.Context, q string) error {
			calls = append(calls, q)
			if q == syncSQL {
				return nil
			}
			ddl++
			if ddl == 1 {
				return fmt.Errorf("wrapped: %w", conflict)
			}
			return terminal
		}, statement)
		require.Equal(t, 2, attempts)
		require.Same(t, terminal, err)
		require.Equal(t, []string{syncSQL, statement, syncSQL, statement}, calls)
	})
	t.Run("synchronization failure stops before DDL", func(t *testing.T) {
		failure := errors.New("sync failed")
		calls := 0
		attempts, err := execCheckNamespaceSI(t.Context(), func(_ context.Context, q string) error { calls++; require.Equal(t, syncSQL, q); return failure }, statement)
		require.Zero(t, attempts)
		require.Equal(t, 1, calls)
		require.ErrorIs(t, err, failure)
	})
	t.Run("cancellation after sync stops before DDL", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		attempts, err := execCheckNamespaceSI(ctx, func(_ context.Context, q string) error { calls++; require.Equal(t, syncSQL, q); cancel(); return nil }, statement)
		require.Zero(t, attempts)
		require.Equal(t, 1, calls)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("cancellation after abort stops recovery", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		attempts, err := execCheckNamespaceSI(ctx, func(_ context.Context, q string) error {
			calls++
			if q == syncSQL {
				return nil
			}
			cancel()
			return conflict
		}, statement)
		require.Equal(t, 1, attempts)
		require.Equal(t, 2, calls)
		require.ErrorIs(t, err, context.Canceled)
		var me *mysql.MySQLError
		require.ErrorAs(t, err, &me)
		require.Same(t, conflict, me)
	})
	for _, cancelDuring := range []string{syncSQL, statement} {
		t.Run("cancellation during recovery "+cancelDuring, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ddl := 0
			calls := 0
			attempts, err := execCheckNamespaceSI(ctx, func(attempt context.Context, q string) error {
				calls++
				if ddl == 1 && q == cancelDuring {
					cancel()
					return attempt.Err()
				}
				if q == statement {
					ddl++
					return conflict
				}
				return nil
			}, statement)
			wantAttempts, wantCalls := 1, 3
			if cancelDuring == statement {
				wantAttempts, wantCalls = 2, 4
			}
			require.Equal(t, wantAttempts, attempts)
			require.Equal(t, wantCalls, calls)
			require.ErrorContains(t, err, fmt.Sprintf("after %d attempts", wantAttempts))
			require.ErrorIs(t, err, context.Canceled)
			var me *mysql.MySQLError
			require.ErrorAs(t, err, &me)
			require.Same(t, conflict, me)
		})
	}
	t.Run("expired budget issues no SQL", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
		defer cancel()
		attempts, err := execCheckNamespaceSI(ctx, func(context.Context, string) error { t.Fatal("SQL after deadline"); return nil }, statement)
		require.Zero(t, attempts)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
