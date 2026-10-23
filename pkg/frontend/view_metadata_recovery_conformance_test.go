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
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func testNativeViewMetadataRecoveryTransitions(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, public *sql.DB, database string) {
	exec := func(t *testing.T, query string) {
		t.Helper()
		_, err := public.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	check := func(t *testing.T, db string, width int32, defaultValue string) {
		t.Helper()
		withNativeViewSchemaRead(t, ctx, cn, svc, db, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			testNativeViewMetadataSameSnapshot(t, ctx, compiler, op, proc, db, width, defaultValue)
		})
	}
	const clone = "view_metadata_clone"
	exec(t, "create database "+clone+" clone "+database+" {snapshot='view_metadata_conformance'}")
	defer exec(t, "drop database "+clone)
	if !t.Run("clone binds target source independently", func(t *testing.T) {
		check(t, clone, 20, "original")
		exec(t, "alter table "+database+".conformance_source modify column label varchar(80) default 'source_after_clone', algorithm=copy")
		check(t, database, 80, "source_after_clone")
		check(t, clone, 20, "original")
		exec(t, "alter table "+clone+".conformance_source modify column label varchar(90) default 'clone_only', algorithm=copy")
		check(t, clone, 90, "clone_only")
		check(t, database, 80, "source_after_clone")
	}) {
		return
	}
	// 后续恢复作用于克隆库，避免全库恢复破坏共享 fixture 的其它场景。
	exec(t, "create pitr view_metadata_recovery_pitr for database "+clone+" range 1 'h'")
	defer exec(t, "drop pitr view_metadata_recovery_pitr")
	exec(t, "create snapshot view_metadata_recovery_snapshot for database "+clone)
	defer exec(t, "drop snapshot view_metadata_recovery_snapshot")
	var before, at string
	clock := "select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s')"
	require.NoError(t, public.QueryRowContext(ctx, clock).Scan(&before))
	// PITR 语法以秒为精度；观察数据库时钟进入提交之后的一秒是公共时间边界，
	// 不是 sleep 猜测 DDL 是否完成。外层 5s 只作为失败保护。
	require.Eventually(t, func() bool {
		err := public.QueryRowContext(ctx, clock).Scan(&at)
		return err == nil && at != before
	}, 5*time.Second, 20*time.Millisecond)
	for _, tc := range []struct{ name, restore string }{
		{"table PITR", "restore database " + clone + " table conformance_source from pitr view_metadata_recovery_pitr '" + at + "'"},
		{"database PITR", "restore database " + clone + " from pitr view_metadata_recovery_pitr '" + at + "'"},
		{"database snapshot", "restore database " + clone + " {snapshot='view_metadata_recovery_snapshot'}"},
	} {
		if !t.Run(tc.name, func(t *testing.T) {
			exec(t, "alter table "+clone+".conformance_source modify column label varchar(100) default 'after_restore_point', algorithm=copy")
			check(t, clone, 100, "after_restore_point")
			exec(t, tc.restore)
			check(t, clone, 90, "clone_only")
			check(t, database, 80, "source_after_clone")
		}) {
			return
		}
	}
}

// This is a persisted historical-template fixture, not an old-binary upgrade.
func testNativeViewMetadataHistoricalTemplate(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, database string) {
	exec := func(query string) {
		t.Helper()
		result, err := svc.GetSQLExecutor().Exec(ctx, query, executor.Options{}.WithDatabase(database))
		require.NoError(t, err, query)
		result.Close()
	}
	exec("create table legacy_template_source (label varchar(12))")
	defer exec("drop table legacy_template_source")
	exec("create view legacy_template_view as select label from legacy_template_source")
	defer exec("drop view legacy_template_view")
	restoreTemplate := func() {
		exec("drop view if exists information_schema.columns")
		exec(sysview.InformationSchemaColumnsDDL)
	}
	restored := false
	defer func() {
		if !restored {
			restoreTemplate()
		}
	}()
	exec("drop view information_schema.columns")
	exec(sysview.InformationSchemaColumnsV58DDL())
	var historical timestamp.Timestamp
	withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(_ *frontend.TxnCompilerContext, op client.TxnOperator, _ *process.Process) {
		historical = op.SnapshotTS()
	})
	restoreTemplate()
	restored = true
	exec("alter table legacy_template_source modify column label varchar(30), algorithm=copy")
	for _, tc := range []struct{ name, hint, typ string }{
		{"V58 historical template", fmt.Sprintf(" {MO_TS = '%s'}", historical.DebugString()), "varchar(12)"},
		{"current template", "", "varchar(30)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testNativeViewMetadataCase(t, ctx, cn, svc, database, nil, nativeViewMetadataCase{
				name: tc.name,
				sql:  "select column_name, lower(column_type) from information_schema.columns" + tc.hint + " where table_schema='" + database + "' and table_name='legacy_template_view'",
				want: [][]string{{"label", tc.typ}}, allowed: []string{"legacy_template_view"},
			})
		})
	}
}

func testNativeViewMetadataAccountRestore(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, system, source *sql.DB, reader *frontend.TenantInfo) {
	exec := func(db *sql.DB, query string) {
		t.Helper()
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	exec(source, "create database schema_creation_env")
	defer exec(source, "drop database schema_creation_env")
	exec(source, "create table schema_creation_env.origin (id bigint)")
	exec(source, "create table schema_security.origin (id int)")
	creation := openNativeViewSchemaSQL(t, ctx, cn, reader.Tenant+"#root#accountadmin:test123", "schema_creation_env")
	defer creation.Close()
	exec(creation, "set view_security_type='DEFINER'")
	exec(creation, "create view schema_security.creation_env_v as select id from origin")
	exec(creation, "create view inner_v as select id from origin")
	exec(creation, "create view schema_security.nested_creation_v as select id from inner_v")
	exec(source, "grant select on view schema_security.creation_env_v to schema_reader")
	exec(source, "grant select on view schema_security.nested_creation_v to schema_reader")
	const account = "schema_restore_target"
	exec(system, "create account "+account+" admin_name 'root' identified by 'test123'")
	defer exec(system, "drop account "+account)
	exec(system, "create snapshot schema_account_restore for account schema_subscriber")
	defer exec(system, "drop snapshot schema_account_restore")
	exec(system, "restore account schema_subscriber {snapshot='schema_account_restore'} to account "+account)
	target := openNativeViewSchemaSQL(t, ctx, cn, account+"#root#accountadmin:test123", "schema_security")
	defer target.Close()
	restored := reader.Copy()
	restored.Tenant = account
	require.NoError(t, system.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name=?", account).Scan(&restored.TenantID))
	require.NotEqual(t, reader.TenantID, restored.TenantID)
	require.NoError(t, target.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='schema_reader_user'").Scan(&restored.UserID))
	require.NoError(t, target.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name='schema_reader'").Scan(&restored.DefaultRoleID))
	targetCtx := defines.AttachAccount(ctx, restored.TenantID, restored.UserID, restored.DefaultRoleID)
	// 同名 source 在源账户变化，不能污染恢复目标的定义绑定与授权。
	exec(source, "alter table schema_security.source modify column id bigint, algorithm=copy")
	withNativeViewSchemaRead(t, targetCtx, cn, svc, "schema_security", restored, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
		_, physical, err := compiler.Resolve("schema_security", "creation_env_v", nil)
		require.NoError(t, err)
		require.NotNil(t, physical)
		require.EqualValues(t, types.T_int64, physical.Cols[0].Typ.Id, "恢复时首次生成的物理列也必须使用创建环境，而不只是事后修补 JSON")
		for _, view := range []string{"definer_v", "invoker_v", "creation_env_v", "nested_creation_v"} {
			func() {
				statements, err := mysql.Parse(targetCtx, "select * from schema_security."+view, 1)
				require.NoError(t, err)
				defer statements[0].Free()
				p, err := compiler.BuildPlanWithViewSchema(targetCtx, statements[0], false, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
				require.NoError(t, err)
				cols := plan.GetResultColumnsFromPlan(p)
				require.Len(t, cols, 1)
				if view == "creation_env_v" || view == "nested_creation_v" {
					require.EqualValues(t, types.T_int64, cols[0].Typ.Id, "恢复不能用 View 所在库的同名表替代创建环境中的来源")
				} else {
					require.EqualValues(t, types.T_int32, cols[0].Typ.Id)
				}
				err = frontend.AuthorizeNativeViewSchemaPlanForTest(targetCtx, compiler, statements[0], p)
				if view != "invoker_v" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "privilege", "恢复不能将原先撤销的基表权限重新授予 INVOKER")
				}
			}()
		}
	})
	testNativeViewMetadataCase(t, targetCtx, cn, svc, "schema_security", restored, nativeViewMetadataCase{
		name: "restored account", sql: "select table_name, lower(column_type) from information_schema.columns where table_schema='schema_security' and table_name in ('creation_env_v','definer_v','invoker_v','nested_creation_v') order by table_name", want: [][]string{{"creation_env_v", "bigint"}, {"definer_v", "int"}, {"invoker_v", "int"}, {"nested_creation_v", "bigint"}}, allowed: []string{"creation_env_v", "definer_v", "invoker_v", "nested_creation_v"},
	})
}
