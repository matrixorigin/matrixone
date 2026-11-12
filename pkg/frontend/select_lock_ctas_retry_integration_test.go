//go:build integration

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

package frontend_test

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestSelectLockCTASRetry(t *testing.T) {
	t.Cleanup(frontend.InstallNativeSelectLockFailureExecForTest())
	runNativeViewSchemaCluster(t, func(cn embed.ServiceOperator, svc cnservice.Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		admin := openNativeViewSchemaSQL(t, ctx, cn, "dump:111", "")
		admin.SetMaxOpenConns(1)
		exec := func(query string) {
			t.Helper()
			_, err := admin.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		const database = "select_lock_ctas_retry"
		exec("create database " + database)
		defer exec("drop database " + database)
		exec("create database select_lock_ctas_other")
		defer exec("drop database select_lock_ctas_other")
		exec("create role select_lock_retry_role")
		defer exec("drop role select_lock_retry_role")
		exec("create user select_lock_retry_reader identified by 'test123' default role select_lock_retry_role")
		defer exec("drop user select_lock_retry_reader")
		for _, query := range []string{
			"grant select_lock_retry_role to select_lock_retry_reader",
			"create table " + database + ".a(id int primary key)",
			"create table " + database + ".b(id int primary key)",
			"insert into " + database + ".a values(1)",
			"insert into " + database + ".b values(1)",
			"set view_security_type='INVOKER'",
			"create view " + database + ".v as select id from " + database + ".a",
			"grant select on table " + database + ".* to select_lock_retry_role",
			"grant select, update on view " + database + ".v to select_lock_retry_role",
			"grant update on table " + database + ".a to select_lock_retry_role",
			"grant create table on database " + database + " to select_lock_retry_role",
		} {
			exec(query)
		}
		reader := frontend.GetBackgroundTenant()
		reader.User = "select_lock_retry_reader"
		reader.DefaultRole = "select_lock_retry_role"
		require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='select_lock_retry_reader'").Scan(&reader.UserID))
		require.NoError(t, admin.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name='select_lock_retry_role'").Scan(&reader.DefaultRoleID))
		readerCtx := defines.AttachAccount(ctx, reader.TenantID, reader.UserID, reader.DefaultRoleID)
		noLocks := func(t *testing.T) {
			t.Helper()
			tx, err := admin.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			var id int
			require.NoError(t, tx.QueryRowContext(ctx, "select id from "+database+".b for update nowait").Scan(&id))
			require.Equal(t, 1, id)
		}
		for _, prepared := range []bool{false, true} {
			t.Run(map[bool]string{false: "ordinary", true: "prepared with changed USE"}[prepared], func(t *testing.T) {
				exec("alter view " + database + ".v as select id from " + database + ".a")
				statement, err := mysql.ParseOne(readerCtx, "create table retry_result as select id from v for update", 1)
				require.NoError(t, err)
				defer statement.Free()
				// Establish the original authorized binding before the catalog transition.
				withNativeViewSchemaRead(t, readerCtx, cn, svc, database, reader, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
					p, err := plan.BuildPlan(compiler, statement, prepared, nil)
					require.NoError(t, err)
					require.Nil(t, p.GetQuery())
					require.Equal(t, prepared, p.GetDdl().GetQuery() != nil)
					require.NoError(t, frontend.AuthorizeNativeViewSchemaPlanForTest(readerCtx, compiler, statement, p))
				})
				exec("alter view " + database + ".v as select id from " + database + ".b")
				retry := func(allowed bool) {
					withNativeViewSchemaRead(t, readerCtx, cn, svc, database, reader, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
						source, err := plan.BuildPlan(compiler, statement.(*tree.CreateTable).AsSource, false, nil)
						require.NoError(t, err)
						require.Len(t, source.GetQuery().SelectLockReferences, 1)
						require.Equal(t, "b", source.GetQuery().SelectLockReferences[0].ObjectRef.ObjName)
						if prepared {
							compiler.GetSession().SetDatabaseName("select_lock_ctas_other")
							compiler.SetDatabase("compiler_before_retry")
						}
						p, err := frontend.RebuildNativeSelectLockCTASForTest(readerCtx, compiler, statement, prepared, database)
						if prepared {
							require.Equal(t, "select_lock_ctas_other", compiler.GetSession().GetDatabaseName())
							require.Equal(t, "compiler_before_retry", compiler.GetDatabase())
						}
						if !allowed {
							require.Nil(t, p)
							require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
							require.ErrorContains(t, err, "privilege")
							noLocks(t) // the fixture's reader transaction is still open
						} else {
							require.NoError(t, err)
							require.Equal(t, database, p.GetDdl().GetCreateTable().Database)
						}
					})
				}
				retry(false)
				exec("grant update on table " + database + ".b to select_lock_retry_role")
				retry(true)
				exec("revoke update on table " + database + ".b from select_lock_retry_role")
			})
		}
		// Reuse this cluster and its two one-row tables to exercise the actual
		// batch admission/transaction owner; the pure catalog fixture cannot
		// mint a real snapshot capability.
		t.Run("admitted multi-target authorization", func(t *testing.T) {
			exec("alter view " + database + ".v as select a.id from " + database + ".a join " + database + ".b on a.id=b.id")
			exec("grant update on table " + database + ".b to select_lock_retry_role")
			statement, err := mysql.ParseOne(readerCtx, "select id from v for update", 1)
			require.NoError(t, err)
			defer statement.Free()
			withNativeViewSchemaRead(t, readerCtx, cn, svc, database, reader, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
				p, err := plan.BuildPlan(compiler, statement, false, nil)
				require.NoError(t, err)
				require.Len(t, p.GetQuery().SelectLockReferences, 2)
				checked, ok, err := frontend.AuthorizeNativeSelectLockBatchForTest(readerCtx, compiler, p.GetQuery())
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, 2, checked, "both targets must use the admitted batch, not fallback")
			})
			for _, scenario := range []string{"boundary error", "boundary error and rollback failure", "prefix success and commit failure", "target error and rollback failure", "denial and commit failure", "begin failure"} {
				t.Run(scenario, func(t *testing.T) {
					withNativeViewSchemaRead(t, readerCtx, cn, svc, database, reader, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
						p, err := plan.BuildPlan(compiler, statement, false, nil)
						require.NoError(t, err)
						frontend.AssertNativeSelectLockFinalizationForTest(t, readerCtx, compiler, p.GetQuery(), scenario)
					})
				})
			}
			for _, change := range []string{"generation", "principal", "minimum", "cancel"} {
				t.Run(change, func(t *testing.T) {
					withNativeViewSchemaRead(t, readerCtx, cn, svc, database, reader, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
						p, err := plan.BuildPlan(compiler, statement, false, nil)
						require.NoError(t, err)
						reads, ok, err := frontend.AuthorizeNativeSelectLockBoundaryForTest(readerCtx, compiler, p.GetQuery(), change)
						if change == "generation" {
							require.NoError(t, err)
							require.True(t, ok)
							require.Equal(t, int64(3), reads, "remaining target must resume with an independent catalog read")
						} else {
							require.Error(t, err)
							require.False(t, ok)
							require.Equal(t, int64(2), reads, "invalid context must fail before consuming the second target")
							if change == "cancel" {
								require.ErrorIs(t, err, context.Canceled)
							} else {
								require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
							}
						}
					})
				})
			}
		})

		t.Run("secondary roles and mixed fallback", func(t *testing.T) {
			exec("create role select_lock_retry_secondary")
			defer exec("drop role select_lock_retry_secondary")
			exec("grant select_lock_retry_secondary to select_lock_retry_reader")
			exec("revoke update on table " + database + ".b from select_lock_retry_role")
			defer exec("grant update on table " + database + ".b to select_lock_retry_role")
			exec("grant update on table " + database + ".b to select_lock_retry_secondary")
			public := openNativeViewSchemaSQL(t, ctx, cn, "sys#select_lock_retry_reader#select_lock_retry_role:test123", database)
			public.SetMaxOpenConns(1)
			for _, all := range []bool{false, true, false} {
				mode := "none"
				if all {
					mode = "all"
				}
				_, err := public.ExecContext(ctx, "set secondary role "+mode)
				require.NoError(t, err)
				var id int
				err = public.QueryRowContext(ctx, "select id from v for update").Scan(&id)
				// doSwitchRole intentionally accepts secondary-role syntax as a
				// no-op. ALL must not activate the secondary role's grant.
				require.ErrorContains(t, err, "privilege")
				noLocks(t)
				// Also preserve the existing internal compatibility flag. SQL
				// cannot set it (TestDoSwitchRoleIgnoresSecondaryRole).
				tenant := reader.Copy()
				tenant.SetUseSecondaryRole(all)
				withNativeViewSchemaRead(t, readerCtx, cn, svc, database, tenant, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
					statement, err := mysql.ParseOne(readerCtx, "select id from v for update", 1)
					require.NoError(t, err)
					defer statement.Free()
					p, err := plan.BuildPlan(compiler, statement, false, nil)
					require.NoError(t, err)
					checked, ok, err := frontend.AuthorizeNativeSelectLockBatchForTest(readerCtx, compiler, p.GetQuery())
					require.NoError(t, err)
					require.Equal(t, all, ok)
					if all {
						require.Equal(t, 2, checked)
					} else {
						require.Equal(t, 1, checked)
					}
				})
			}
			// Two view targets plus a bare sibling: secondary roles authorize
			// all targets, but the mixed query must retain independent reads.
			tenant := reader.Copy()
			tenant.SetUseSecondaryRole(true)
			withNativeViewSchemaRead(t, readerCtx, cn, svc, database, tenant, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
				statement, err := mysql.ParseOne(readerCtx, "select v.id from v join a on v.id=a.id for update", 1)
				require.NoError(t, err)
				defer statement.Free()
				p, err := plan.BuildPlan(compiler, statement, false, nil)
				require.NoError(t, err)
				require.Len(t, p.GetQuery().SelectLockReferences, 3)
				require.NotEmpty(t, p.GetQuery().SelectLockReferences[0].ViewPath)
				frontend.AssertNativeSelectLockMixedFallbackForTest(t, readerCtx, compiler, p.GetQuery())
			})
			// Make the unprivileged object specifically the bare sibling.
			exec("alter view " + database + ".v as select id from " + database + ".a")
			_, err := public.ExecContext(ctx, "set secondary role none")
			require.NoError(t, err)
			var id int
			err = public.QueryRowContext(ctx, "select v.id from v join b on v.id=b.id for update").Scan(&id)
			require.ErrorContains(t, err, "privilege")
			noLocks(t)
			_, err = public.ExecContext(ctx, "set secondary role all")
			require.NoError(t, err)
			err = public.QueryRowContext(ctx, "select v.id from v join b on v.id=b.id for update").Scan(&id)
			require.ErrorContains(t, err, "privilege")
			exec("grant update on table " + database + ".b to select_lock_retry_role")
			require.NoError(t, public.QueryRowContext(ctx, "select v.id from v join b on v.id=b.id for update").Scan(&id))
			require.Equal(t, 1, id)
		})

	})
}
