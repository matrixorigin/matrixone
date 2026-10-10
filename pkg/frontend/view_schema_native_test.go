//go:build integration

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func runNativeViewSchemaCluster(t *testing.T, run func(embed.ServiceOperator, cnservice.Service)) {
	t.Helper()
	// 沿用本地集群 fixture 的短相对路径，避免 macOS Unix socket 路径超限。
	cwd, err := os.Getwd()
	require.NoError(t, err)
	tmp, err := filepath.Rel(cwd, os.TempDir())
	require.NoError(t, err)
	t.Setenv("TMPDIR", tmp)
	t.Cleanup(func() { require.NoError(t, embed.CloseSingleCNBaseClusterTests()) })
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		run(cn, cn.RawService().(cnservice.Service))
	})
}

func withNativeViewSchemaRead(t *testing.T, ctx context.Context, cn embed.ServiceOperator,
	svc cnservice.Service, database string, tenant *frontend.TenantInfo,
	run func(*frontend.TxnCompilerContext, client.TxnOperator, *process.Process),
) {
	t.Helper()
	if tenant == nil {
		tenant = frontend.GetBackgroundTenant()
	}
	ctx = defines.AttachAccount(ctx, tenant.TenantID, tenant.UserID, tenant.DefaultRoleID)
	op, err := svc.GetTxnClient().New(ctx, timestamp.Timestamp{})
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, op.Rollback(cleanupCtx))
	}()
	storage := svc.GetEngine().(*disttae.Engine)
	require.NoError(t, storage.New(ctx, op))
	mp := mpool.MustNewZeroNoFixed()
	defer mpool.DeleteMPool(mp)
	proc := process.NewTopProcess(ctx, mp, svc.GetTxnClient(), op, nil,
		lockservice.GetLockServiceByServiceID(cn.ServiceID()), nil, nil, nil, nil, nil)
	defer proc.Free()
	proc.Base.SessionInfo.TimeZone = time.Local
	proc.Base.Lim.Size = 64 << 20
	proc.SetStmtProfile(process.NewStmtProfile(uuid.New(), uuid.New()))
	defer proc.SetStmtProfile(nil)
	require.NoError(t, frontend.WithNativeViewSchemaCompilerForTest(ctx, cn.ServiceID(), storage,
		op, proc, database, tenant, func(compiler *frontend.TxnCompilerContext) error {
			run(compiler, op, proc)
			return nil
		}))
}

type nativeViewSchemaValue struct {
	columns      []*plan.ColDef
	dependencies []plan.ViewDependency
	provenance   []plan.ViewSchemaColumnProvenance
	protocol     int64
}

func describeNativeViewSchema(t *testing.T, request *plan.ViewSchemaRequest, database, name string, snapshot *plan.Snapshot) nativeViewSchemaValue {
	t.Helper()
	result, err := request.Describe(database, name, snapshot)
	if result != nil {
		defer result.Release()
	}
	require.NoError(t, err, "%s.%s", database, name)
	require.NotNil(t, result)
	columns, err := result.Columns()
	require.NoError(t, err)
	dependencies, err := result.Dependencies()
	require.NoError(t, err)
	provenance, err := result.Provenance()
	require.NoError(t, err)
	protocol, err := result.RequiredProtocolVersion()
	require.NoError(t, err)
	return nativeViewSchemaValue{columns, dependencies, provenance, protocol}
}

func nativeViewSchemaStamp(t *testing.T, op client.TxnOperator) (client.CatalogReadStamp, uint64) {
	t.Helper()
	stamp, err := op.(interface {
		CatalogReadStamp() (client.CatalogReadStamp, error)
	}).CatalogReadStamp()
	require.NoError(t, err)
	revision, stable := op.GetWorkspace().(interface{ CatalogVisibility() (uint64, bool) }).CatalogVisibility()
	require.True(t, stable)
	return stamp, revision
}

func TestNativeViewSchemaCatalogOwnDDLHistoryAndSharedReads(t *testing.T) {
	runNativeViewSchemaCluster(t, func(cn embed.ServiceOperator, svc cnservice.Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		ctx = defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
		const database = "view_schema_native"
		exec := func(query string, opts executor.Options) {
			t.Helper()
			result, err := svc.GetSQLExecutor().Exec(ctx, query, opts)
			require.NoError(t, err, query)
			result.Close()
		}
		for _, query := range []string{
			"create database " + database,
			"create table " + database + ".source (id int not null default 7, label varchar(20) default 'value')",
			"create view " + database + ".target as select id from " + database + ".source",
			"create view " + database + ".middle as select id, label from " + database + ".source",
			"create view " + database + ".outer_a as select id, label from " + database + ".middle",
			"create view " + database + ".outer_b (id_alias, label_alias) as select id, label from " + database + ".middle",
		} {
			exec(query, executor.Options{})
		}
		var historical timestamp.Timestamp
		withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(_ *frontend.TxnCompilerContext, op client.TxnOperator, _ *process.Process) {
			historical = op.SnapshotTS()
		})
		exec("alter table "+database+".source modify column label varchar(40) default 'new'", executor.Options{})
		exec("alter view "+database+".target as select id, label from "+database+".source", executor.Options{}.WithDatabase(database))
		// 在源表变更后通过权威 CREATE VIEW 路径建立差分 oracle。
		exec("create view "+database+".oracle as select id, label from "+database+".middle", executor.Options{})
		withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			exec("create view owned as select id, label from source", executor.Options{}.WithDatabase(database).WithTxn(op).WithKeepTxnAlive())
			exec("create view rollback_only as select id from source", executor.Options{}.WithDatabase(database).WithTxn(op).WithKeepTxnAlive())
			_, uncommitted, err := compiler.Resolve(database, "rollback_only", nil)
			require.NoError(t, err)
			require.NotNil(t, uncommitted)
			_, oracle, err := compiler.Resolve(database, "oracle", nil)
			require.NoError(t, err)
			require.NotNil(t, oracle)
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			beforeUsed, beforeStatus, beforeSnapshot := generation.Used(), op.Status(), op.SnapshotTS()
			beforeOffset, beforeWrites := op.GetWorkspace().GetSnapshotWriteOffset(), op.GetWorkspace().WriteOffset()
			beforeStamp, beforeRevision := nativeViewSchemaStamp(t, op)
			authorizations := 0
			request := compiler.NewViewSchemaRequest(ctx, func(context.Context, string, string, *plan.Snapshot) error {
				authorizations++
				return nil
			})
			defer request.Close()
			for _, name := range []string{"target", "owned", "outer_a", "outer_b"} {
				first := describeNativeViewSchema(t, request, database, name, nil)
				require.Len(t, first.columns, 2)
				require.Len(t, first.provenance, 2)
				wantNames := []string{"id", "label"}
				if name == "outer_b" {
					wantNames = []string{"id_alias", "label_alias"}
				}
				for i, column := range first.columns {
					require.Equal(t, wantNames[i], column.Name)
					wantType, gotType := oracle.Cols[i].Typ, column.Typ
					wantType.Table, gotType.Table = "", ""
					require.Equal(t, wantType, gotType)
					require.Equal(t, oracle.Cols[i].Default, column.Default)
					require.Equal(t, oracle.Cols[i].NotNull, column.NotNull)
				}
				require.EqualValues(t, 40, first.columns[1].Typ.Width)
				deps := make(map[string]bool)
				for _, dependency := range first.dependencies {
					require.Equal(t, uint32(catalog.System_Account), dependency.AccountID)
					require.NotZero(t, dependency.RelationID)
					deps[dependency.RelationName] = true
				}
				require.True(t, deps[name])
				require.True(t, deps["source"])
				if name == "outer_a" || name == "outer_b" {
					require.True(t, deps["middle"])
				}
				require.Equal(t, first, describeNativeViewSchema(t, request, database, name, nil))
			}
			require.Equal(t, 8, authorizations, "每次根请求，包括重复根，都必须重新授权")
			// 必须执行真实 shared catalog SQL；空/已排序 workspace 的 Adjust 不得改变代号。
			require.NoError(t, frontend.ReadNativeViewSchemaCatalogForTest(ctx, compiler))
			describeNativeViewSchema(t, request, database, "target", nil)
			history := describeNativeViewSchema(t, request, database, "target", &plan.Snapshot{TS: &historical})
			require.Len(t, history.columns, 1)
			require.Equal(t, "id", history.columns[0].Name)
			for _, dependency := range history.dependencies {
				require.NotNil(t, dependency.Snapshot)
				require.Equal(t, historical, *dependency.Snapshot.TS)
			}
			request.Close()
			afterStamp, afterRevision := nativeViewSchemaStamp(t, op)
			require.Equal(t, beforeStamp, afterStamp)
			require.Equal(t, beforeRevision, afterRevision)
			require.Equal(t, beforeUsed, generation.Used())
			require.False(t, generation.Closed())
			require.True(t, proc.UsesExecutionResourceGeneration(generation))
			require.Equal(t, beforeStatus, op.Status())
			require.Equal(t, beforeSnapshot, op.SnapshotTS())
			require.Equal(t, beforeOffset, op.GetWorkspace().GetSnapshotWriteOffset())
			require.Equal(t, beforeWrites, op.GetWorkspace().WriteOffset())

			// 原生目录读错误与取消都不能发布半成品，也不能关闭父执行代。
			errRequest := compiler.NewViewSchemaRequest(ctx, func(context.Context, string, string, *plan.Snapshot) error { return nil })
			defer errRequest.Close()
			result, err := errRequest.Describe(database, "missing", nil)
			if result != nil {
				result.Release()
			}
			require.Nil(t, result)
			require.Error(t, err)
			errRequest.Close()
			cancelCtx, cancelRead := context.WithCancel(ctx)
			defer cancelRead()
			cancelRequest := compiler.NewViewSchemaRequest(cancelCtx, func(context.Context, string, string, *plan.Snapshot) error { return nil })
			defer cancelRequest.Close()
			describeNativeViewSchema(t, cancelRequest, database, "target", nil)
			cancelRead()
			result, err = cancelRequest.Describe(database, "target", nil)
			if result != nil {
				result.Release()
			}
			require.Nil(t, result)
			require.ErrorIs(t, err, context.Canceled)
			cancelRequest.Close()
			require.Equal(t, beforeUsed, generation.Used())
			require.False(t, generation.Closed())

			stale := compiler.NewViewSchemaRequest(ctx, func(context.Context, string, string, *plan.Snapshot) error { return nil })
			defer stale.Close()
			describeNativeViewSchema(t, stale, database, "owned", nil)
			exec("drop view owned", executor.Options{}.WithDatabase(database).WithTxn(op).WithKeepTxnAlive())
			result, err = stale.Describe(database, "owned", nil)
			if result != nil {
				result.Release()
			}
			require.Nil(t, result)
			require.ErrorIs(t, err, plan.ErrViewSchemaChanged)
			stale.Close()
		})
		// 外层事务由 fixture 回滚；未提交的真实 DDL 不能泄漏至下一事务。
		withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(_ *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			db, err := svc.GetEngine().Database(ctx, database, op)
			require.NoError(t, err)
			for _, name := range []string{"owned", "rollback_only"} {
				exists, err := db.RelationExists(ctx, name, proc)
				require.NoError(t, err)
				require.False(t, exists, name)
			}
		})
	})
}

func openNativeViewSchemaSQL(t *testing.T, ctx context.Context, cn embed.ServiceOperator, credentials, database string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(127.0.0.1:%d)/%s", credentials, cn.GetServiceConfig().CN.Frontend.Port, database))
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestNativeViewSchemaSubscriptionHistoryAndRoleDenial(t *testing.T) {
	runNativeViewSchemaCluster(t, func(cn embed.ServiceOperator, svc cnservice.Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		system := openNativeViewSchemaSQL(t, ctx, cn, "dump:111", "")
		for _, query := range []string{
			"create account schema_subscriber admin_name 'root' identified by 'test123'",
			"create database schema_publisher",
			"create table schema_publisher.source (id int)",
			// Publisher database names can coincide with a subscriber-local alias.
			"create database subscribed",
			"create table subscribed.collision_source (id int)",
			"create table schema_publisher.collision_source (id varchar(30))",
			"create database schema_other",
			"create table schema_other.collision_source (id int)",
		} {
			_, err := system.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		publisher := openNativeViewSchemaSQL(t, ctx, cn, "dump:111", "schema_publisher")
		_, err := publisher.ExecContext(ctx, "create view published as select id from source")
		require.NoError(t, err)
		_, err = publisher.ExecContext(ctx, "create view qualified as select id from schema_publisher.source")
		require.NoError(t, err)
		for _, query := range []string{
			"create view v_collision as select id from subscribed.collision_source",
			"create view v_nested as select id from schema_publisher.v_collision",
			"create view v_other as select id from schema_other.collision_source",
			"create view v_other_nested as select id from schema_publisher.v_other",
		} {
			_, err = publisher.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		collisionCreator := openNativeViewSchemaSQL(t, ctx, cn, "dump:111", "subscribed")
		_, err = collisionCreator.ExecContext(ctx, "create view schema_publisher.v_saved_collision as select id from collision_source")
		require.NoError(t, err)
		_, err = system.ExecContext(ctx, "create publication schema_publication database schema_publisher account schema_subscriber")
		require.NoError(t, err)
		subscriber := openNativeViewSchemaSQL(t, ctx, cn, "schema_subscriber#root#accountadmin:test123", "")
		_, err = subscriber.ExecContext(ctx, "create database subscribed from sys publication schema_publication")
		require.NoError(t, err)
		_, err = subscriber.ExecContext(ctx, "create database schema_other")
		require.NoError(t, err)
		_, err = subscriber.ExecContext(ctx, "create table schema_other.collision_source (id varchar(30))")
		require.NoError(t, err)
		var account uint32
		require.NoError(t, system.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name = 'schema_subscriber'").Scan(&account))
		tenant := &frontend.TenantInfo{Tenant: "schema_subscriber", User: "root", DefaultRole: frontend.GetAccountAdminRole(),
			TenantID: account, UserID: frontend.GetAdminUserId(), DefaultRoleID: frontend.GetAccountAdminRoleId()}
		tenantCtx := defines.AttachAccount(ctx, account, tenant.UserID, tenant.DefaultRoleID)
		var historical timestamp.Timestamp
		var baseline nativeViewSchemaValue
		withNativeViewSchemaRead(t, tenantCtx, cn, svc, "subscribed", tenant, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			historical = op.SnapshotTS()
			// Even an inherited active subscription cannot turn the entry alias
			// into a publisher SQL identifier. Keep the two phases explicit.
			inherited := &pb.SubscriptionMeta{AccountId: int32(catalog.System_Account), DbName: "schema_publisher", SubName: "subscribed", Tables: "*"}
			compiler.SetQueryingSubscription(inherited)
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			beforeUsed := generation.Used()
			beforeStamp, beforeRevision := nativeViewSchemaStamp(t, op)
			request := compiler.NewViewSchemaRequest(tenantCtx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
			defer request.Close()
			baseline = describeNativeViewSchema(t, request, "subscribed", "published", nil)
			require.Len(t, baseline.columns, 1)
			require.Equal(t, baseline, describeNativeViewSchema(t, request, "subscribed", "published", nil))
			for _, test := range []struct{ view, sourceDatabase string }{
				{"v_collision", "subscribed"}, {"v_nested", "subscribed"},
				{"v_saved_collision", "subscribed"}, {"v_other", "schema_other"},
				{"v_other_nested", "schema_other"},
			} {
				t.Run(test.view, func(t *testing.T) {
					for i := 0; i < 2; i++ {
						value := describeNativeViewSchema(t, request, "subscribed", test.view, nil)
						require.Len(t, value.columns, 1)
						require.Equal(t, int32(types.T_int32), value.columns[0].Typ.Id)
						found := false
						for _, dependency := range value.dependencies {
							if dependency.RelationName == "collision_source" {
								require.Equal(t, uint32(catalog.System_Account), dependency.AccountID)
								require.Equal(t, test.sourceDatabase, dependency.DatabaseName)
								found = true
							}
						}
						require.True(t, found)
					}
				})
			}
			qualified := describeNativeViewSchema(t, request, "subscribed", "qualified", nil)
			require.Len(t, qualified.columns, 1)
			require.Equal(t, "id", qualified.columns[0].Name)
			for _, dependency := range baseline.dependencies {
				require.Equal(t, uint32(catalog.System_Account), dependency.AccountID)
				require.Equal(t, "subscribed", dependency.SubscriptionName)
			}
			request.Close()
			require.Same(t, inherited, compiler.GetQueryingSubscription())
			afterStamp, afterRevision := nativeViewSchemaStamp(t, op)
			require.Equal(t, beforeStamp, afterStamp)
			require.Equal(t, beforeRevision, afterRevision)
			require.Equal(t, beforeUsed, generation.Used())
			require.False(t, generation.Closed())
		})
		_, err = publisher.ExecContext(ctx, "alter view published as select id, id as extra from source")
		require.NoError(t, err)
		// 新的外层事务必须具有更新快照，历史查询才真实经过 CloneSnapshotOp。
		withNativeViewSchemaRead(t, tenantCtx, cn, svc, "subscribed", tenant, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			require.True(t, historical.Less(op.SnapshotTS()))
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			beforeUsed := generation.Used()
			beforeStamp, beforeRevision := nativeViewSchemaStamp(t, op)
			request := compiler.NewViewSchemaRequest(tenantCtx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
			defer request.Close()
			history := describeNativeViewSchema(t, request, "subscribed", "published", &plan.Snapshot{
				TS: &historical, Tenant: &pb.SnapshotTenant{TenantID: account},
			})
			require.Equal(t, baseline.columns, history.columns)
			for _, dependency := range history.dependencies {
				require.NotNil(t, dependency.Snapshot)
				require.Equal(t, historical, *dependency.Snapshot.TS)
			}
			current := describeNativeViewSchema(t, request, "subscribed", "published", nil)
			require.Len(t, current.columns, 2)
			require.Equal(t, "extra", current.columns[1].Name)
			qualified := describeNativeViewSchema(t, request, "subscribed", "qualified", &plan.Snapshot{
				TS: &historical, Tenant: &pb.SnapshotTenant{TenantID: account},
			})
			require.Len(t, qualified.columns, 1)
			require.Equal(t, "id", qualified.columns[0].Name)
			request.Close()
			afterStamp, afterRevision := nativeViewSchemaStamp(t, op)
			require.Equal(t, beforeStamp, afterStamp)
			require.Equal(t, beforeRevision, afterRevision)
			require.Equal(t, beforeUsed, generation.Used())
			require.False(t, generation.Closed())
		})

		_, err = subscriber.ExecContext(ctx, "create user denied identified by 'test123'")
		require.NoError(t, err)
		var userID, roleID uint32
		require.NoError(t, subscriber.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name = 'denied'").Scan(&userID))
		require.NoError(t, subscriber.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name = 'public'").Scan(&roleID))
		deniedTenant := &frontend.TenantInfo{Tenant: "schema_subscriber", User: "denied", DefaultRole: "public",
			TenantID: account, UserID: userID, DefaultRoleID: roleID}
		deniedCtx := defines.AttachAccount(ctx, account, userID, roleID)
		withNativeViewSchemaRead(t, deniedCtx, cn, svc, "subscribed", deniedTenant, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			generation, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			beforeUsed := generation.Used()
			beforeStamp, beforeRevision := nativeViewSchemaStamp(t, op)
			request := compiler.NewViewSchemaRequest(deniedCtx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
			defer request.Close()
			for _, name := range []string{"published", "missing"} {
				result, err := request.Describe("subscribed", name, nil)
				if result != nil {
					result.Release()
				}
				require.Nil(t, result)
				require.ErrorContains(t, err, "view schema access denied")
				require.False(t, errors.Is(err, plan.ErrViewSchemaChanged), "真实权限查询自身不得推进请求代号")
			}
			request.Close()
			afterStamp, afterRevision := nativeViewSchemaStamp(t, op)
			require.Equal(t, beforeStamp, afterStamp)
			require.Equal(t, beforeRevision, afterRevision)
			require.Equal(t, beforeUsed, generation.Used())
			require.False(t, generation.Closed())
		})
	})
}
