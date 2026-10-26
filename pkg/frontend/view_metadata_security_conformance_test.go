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

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// 复用订阅集群；每次读使用新 statement，避免把旧合法 snapshot 当作撤权后新读。
func testNativeViewMetadataSecurityTransitions(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, admin, system *sql.DB, tenant *frontend.TenantInfo) {
	exec := func(query string) {
		t.Helper()
		_, err := admin.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	const database = "schema_security"
	exec("create database " + database)
	defer exec("drop database " + database)
	exec("create role schema_reader")
	defer exec("drop role schema_reader")
	exec("create user schema_reader_user identified by 'test123' default role schema_reader")
	defer exec("drop user schema_reader_user")
	for _, query := range []string{
		"grant schema_reader to schema_reader_user",
		"grant connect on account * to schema_reader",
		"create table schema_security.source (id int)",
		"create view schema_security.definer_v as select id from schema_security.source",
		"set view_security_type='INVOKER'",
		"create view schema_security.invoker_v as select id from schema_security.source",
		"set view_security_type='DEFINER'",
		"grant select on view schema_security.definer_v to schema_reader",
		"grant select on view schema_security.invoker_v to schema_reader",
	} {
		exec(query)
	}
	reader := tenant.Copy()
	reader.User, reader.DefaultRole = "schema_reader_user", "schema_reader"
	require.NoError(t, admin.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name='schema_reader_user'").Scan(&reader.UserID))
	require.NoError(t, admin.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name='schema_reader'").Scan(&reader.DefaultRoleID))
	readerCtx := defines.AttachAccount(ctx, reader.TenantID, reader.UserID, reader.DefaultRoleID)
	check := func(t *testing.T, name string, allowed bool) {
		t.Helper()
		withNativeViewSchemaRead(t, readerCtx, cn, svc, database, reader, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
			statements, err := mysql.Parse(readerCtx, "select * from "+database+"."+name, 1)
			require.NoError(t, err)
			defer statements[0].Free()
			p, err := compiler.BuildPlanWithViewSchema(readerCtx, statements[0], false, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
			require.NoError(t, err, "根 View 描述资格与执行基表权限是不同的合同")
			require.Len(t, plan.GetResultColumnsFromPlan(p), 1)
			err = frontend.AuthorizeNativeViewSchemaPlanForTest(readerCtx, compiler, statements[0], p)
			if allowed {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "privilege")
			}
		})
	}
	t.Run("definer does not require invoker base grants", func(t *testing.T) { check(t, "definer_v", true) })
	t.Run("invoker requires source grant", func(t *testing.T) { check(t, "invoker_v", false) })
	exec("grant select on table schema_security.source to schema_reader")
	t.Run("invoker grant recovery", func(t *testing.T) { check(t, "invoker_v", true) })
	exec("revoke select on table schema_security.source from schema_reader")
	t.Run("invoker source revoke", func(t *testing.T) { check(t, "invoker_v", false) })
	exec("revoke select on view schema_security.definer_v from schema_reader")
	t.Run("invisible root never derives", func(t *testing.T) {
		testNativeViewMetadataCase(t, readerCtx, cn, svc, database, reader, nativeViewMetadataCase{
			name: "revoked", sql: "select cast(count(*) as char) from information_schema.columns where table_schema='schema_security' and table_name='definer_v'", want: [][]string{{"0"}},
		})
		withNativeViewSchemaRead(t, readerCtx, cn, svc, database, reader, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
			request := compiler.NewViewSchemaRequest(readerCtx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
			defer request.Close()
			result, err := request.Describe(database, "definer_v", nil)
			result.Release()
			require.Nil(t, result)
			require.ErrorContains(t, err, "access denied")
		})
	})
	exec("grant select on view schema_security.definer_v to schema_reader")
	t.Run("root grant recovery", func(t *testing.T) { check(t, "definer_v", true) })
	t.Run("account restore preserves definition and authorization domain", func(t *testing.T) {
		testNativeViewMetadataAccountRestore(t, ctx, cn, svc, system, admin, reader)
	})
}

func testNativeViewMetadataPublicationTransitions(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, system *sql.DB, tenant *frontend.TenantInfo) {
	exec := func(query string) {
		t.Helper()
		_, err := system.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	readerCtx := defines.AttachAccount(ctx, tenant.TenantID, tenant.UserID, tenant.DefaultRoleID)
	var historical *plan.Snapshot
	check := func(t *testing.T, columns int, allowed bool) uint64 {
		t.Helper()
		var id uint64
		withNativeViewSchemaRead(t, readerCtx, cn, svc, "subscribed", tenant, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			if historical == nil {
				ts := op.SnapshotTS()
				historical = &plan.Snapshot{TS: &ts}
			}
			roleAuthorize := frontend.NativeViewSchemaRoleAuthorizerForTest(compiler)
			request := compiler.NewViewSchemaRequest(readerCtx, func(ctx context.Context, database, name string, snapshot *plan.Snapshot) error {
				// Snapshot/tenant access eligibility belongs to the caller's root
				// authorizer, not historical catalog resolution. The fixture's
				// role-only helper does not establish current publication access.
				if _, err := compiler.GetSubscriptionMeta(database, nil); err != nil {
					return err
				}
				return roleAuthorize(ctx, database, name, snapshot)
			})
			defer request.Close()
			result, err := request.Describe("subscribed", "published", nil)
			if result != nil {
				defer result.Release()
			}
			if !allowed {
				require.Error(t, err)
				require.Nil(t, result, "撤销订阅资格不能回退之前的成功描述")
				old, historyErr := request.Describe("subscribed", "published", historical)
				old.Release()
				require.Nil(t, old, "旧 schema snapshot 不能恢复当前已撤销的订阅资格")
				require.Error(t, historyErr)
				return
			}
			require.NoError(t, err)
			cols, err := result.Columns()
			require.NoError(t, err)
			require.Len(t, cols, columns)
			dependencies, err := result.Dependencies()
			require.NoError(t, err)
			for _, dependency := range dependencies {
				if dependency.RelationName == "published" {
					id = dependency.RelationID
				}
			}
			require.NotZero(t, id)
			result.Release()
			request.Close()
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			require.Zero(t, generation.Used())
		})
		return id
	}
	original := check(t, 2, true)
	for _, tc := range []struct {
		name, ddl string
		allowed   bool
	}{
		{"withdraw", "alter publication schema_publication account drop schema_subscriber", false},
		{"restore membership", "alter publication schema_publication account add schema_subscriber", true},
		{"drop publication", "drop publication schema_publication", false},
		{"recreate publication", "create publication schema_publication database schema_publisher account schema_subscriber", true},
	} {
		if !t.Run(tc.name, func(t *testing.T) {
			exec(tc.ddl)
			id := check(t, 2, tc.allowed)
			if tc.allowed {
				require.Equal(t, original, id)
			}
		}) {
			return
		}
	}
	// 当前发布列表的收缩/恢复必须同时影响单对象与 bulk；被排除的根
	// 不属于坏定义，不得沿历史持久列或之前成功的请求返回元数据。
	t.Run("table list withdrawal and recovery", func(t *testing.T) {
		exec := func(t *testing.T, query string) {
			t.Helper()
			_, err := system.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		defer exec(t, "alter publication schema_publication table `*`")
		for _, tc := range []struct {
			name, tables string
			allowed      bool
		}{
			{"explicit view and source", "source,published", true},
			{"source only", "source", false},
			{"restore view", "source,published", true},
		} {
			if !t.Run(tc.name, func(t *testing.T) {
				exec(t, "alter publication schema_publication table "+tc.tables)
				for _, cacheOff := range []bool{false, true} {
					t.Run(fmt.Sprintf("cache_off=%t", cacheOff), func(t *testing.T) {
						withNativeViewSchemaRead(t, readerCtx, cn, svc, "subscribed", tenant, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
							before := op.GetWorkspace().WriteOffset()
							request := compiler.NewViewMetadataRequest(readerCtx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
							defer request.Close()
							if cacheOff {
								request.DisableMemoForIntegrationTest()
							}
							for range 2 {
								func() {
									result, invalid, err := plan.DescribeViewMetadataCandidate(request.Context(), "subscribed", "published", nil)
									if result != nil {
										defer result.Release()
									}
									require.False(t, invalid, "发布列表过滤不是可跳过的 View 定义错误")
									if !tc.allowed {
										require.Error(t, err)
										require.Nil(t, result)
										return
									}
									require.NoError(t, err)
									columns, err := result.Columns()
									require.NoError(t, err)
									require.Len(t, columns, 2)
								}()
							}
							rows, err := frontend.ReadNativeViewMetadataSQLForTest(request.Context(), compiler, "describe subscribed.published")
							if tc.allowed {
								require.NoError(t, err)
								require.Len(t, rows, 2)
							} else {
								require.Error(t, err)
								require.Empty(t, rows)
							}
							rows, err = frontend.ReadNativeViewMetadataSQLForTest(request.Context(), compiler, "select table_name, column_name from information_schema.columns where table_schema='subscribed' and table_name in ('published','source') order by table_name, ordinal_position")
							require.NoError(t, err)
							want := [][]string{{"source", "id"}}
							if tc.allowed {
								want = [][]string{{"published", "id"}, {"published", "extra"}, {"source", "id"}}
							}
							require.Equal(t, want, rows, "拒绝根对象后，仍可读取有效普通表")
							require.Equal(t, before, op.GetWorkspace().WriteOffset())
							request.Close()
							budget, err := proc.GetExecutionResourceBudget()
							require.NoError(t, err)
							require.Zero(t, budget.Used())
							require.False(t, budget.Closed())
						})
					})
				}
			}) {
				return
			}
		}
	})
	// 同名 publication 指向新的数据库：subscriber alias 不变，定义身份必须改变。
	exec("create database schema_remapped")
	defer exec("drop database schema_remapped")
	exec("create table schema_remapped.source (id varchar(17))")
	exec("create view schema_remapped.published as select id from schema_remapped.source")
	exec("drop publication schema_publication")
	exec("create publication schema_publication database schema_remapped account schema_subscriber")
	defer func() {
		exec("drop publication schema_publication")
		exec("create publication schema_publication database schema_publisher account schema_subscriber")
	}()
	t.Run("same-name publication remaps physical identity", func(t *testing.T) {
		require.NotEqual(t, original, check(t, 1, true))
		testNativeViewMetadataCase(t, readerCtx, cn, svc, "subscribed", tenant, nativeViewMetadataCase{
			name: "remapped", sql: fmt.Sprintf("select lower(column_type) from information_schema.columns where table_schema='%s' and table_name='published'", "subscribed"), want: [][]string{{"varchar(17)"}}, allowed: []string{"published"},
		})
	})
}
