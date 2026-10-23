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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
			"insert into " + database + ".source values (3, 'first')",
			"create view " + database + ".target as select id from " + database + ".source",
			"create view " + database + ".middle as select id, label from " + database + ".source",
			"create view " + database + ".outer_a as select id, label from " + database + ".middle",
			"create view " + database + ".outer_b (id_alias, label_alias) as select id, label from " + database + ".middle",
		} {
			exec(query, executor.Options{})
		}
		t.Run("placeholder source authorization", func(t *testing.T) {
			testNativeViewSchemaRootScopes(t, ctx, cn, svc, database)
		})
		var historical timestamp.Timestamp
		withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(_ *frontend.TxnCompilerContext, op client.TxnOperator, _ *process.Process) {
			historical = op.SnapshotTS()
		})
		exec("alter table "+database+".source modify column label varchar(40) default 'new'", executor.Options{})
		exec("alter view "+database+".target as select id, label from "+database+".source", executor.Options{}.WithDatabase(database))
		// 在源表变更后通过权威 CREATE VIEW 路径建立差分 oracle。
		exec("create view "+database+".oracle as select id, label from "+database+".middle", executor.Options{})
		t.Run("derived metadata consumers", func(t *testing.T) {
			testNativeViewMetadataConsumers(t, ctx, cn, svc, database, historical)
		})
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
		withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
			db, err := svc.GetEngine().Database(ctx, database, op)
			require.NoError(t, err)
			for _, name := range []string{"owned", "rollback_only"} {
				exists, err := db.RelationExists(ctx, name, proc)
				require.NoError(t, err)
				require.False(t, exists, name)
			}
			ctasStatements, err := mysql.Parse(ctx, "create table plan_only_target as select id_alias, label_alias from outer_b", 1)
			require.NoError(t, err)
			defer ctasStatements[0].Free()
			beforeStamp, beforeRevision := nativeViewSchemaStamp(t, op)
			deniedRoot := errors.New("only the user source View is authorized")
			var authorizedRoots []string
			authorizeSource := func(_ context.Context, dbName, name string, _ *plan.Snapshot) error {
				authorizedRoots = append(authorizedRoots, dbName+"."+name)
				if dbName != database || name != "outer_b" {
					return deniedRoot
				}
				return nil
			}
			ctas, err := compiler.BuildPlanWithViewSchema(ctx, ctasStatements[0], false, authorizeSource)
			require.NoError(t, err)
			require.Equal(t, []string{database + ".outer_b"}, authorizedRoots)
			deniedStatements, err := mysql.Parse(ctx, "select * from mo_catalog.mo_tables", 1)
			require.NoError(t, err)
			defer deniedStatements[0].Free()
			deniedPlan, err := compiler.BuildPlanWithViewSchema(ctx, deniedStatements[0], false, authorizeSource)
			require.Nil(t, deniedPlan)
			require.ErrorIs(t, err, deniedRoot, "user SQL must not gain the internal catalog-query exemption")
			ctasColumns := ctas.GetDdl().GetCreateTable().TableDef.Cols
			require.Equal(t, "id_alias", ctasColumns[0].Name)
			require.Equal(t, "0", ctasColumns[0].Default.OriginString)
			require.EqualValues(t, 40, ctasColumns[1].Typ.Width)
			afterStamp, afterRevision := nativeViewSchemaStamp(t, op)
			require.Equal(t, beforeStamp, afterStamp, "planning CTAS must not advance its borrowed transaction")
			require.Equal(t, beforeRevision, afterRevision)
			exists, err := db.RelationExists(ctx, "plan_only_target", proc)
			require.NoError(t, err)
			require.False(t, exists, "planning cannot execute CTAS")
			// Explicit S3 binding must produce an executable base-table subtree,
			// not a metadata-only View memo scan. The parent owns execution.
			const query = "select id_alias, label_alias from outer_b"
			statements, err := mysql.Parse(ctx, query, 1)
			require.NoError(t, err)
			defer statements[0].Free()
			p, err := compiler.BuildPlanWithViewSchema(ctx, statements[0], false, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
			require.NoError(t, err)
			columns := plan.GetResultColumnsFromPlan(p)
			require.Len(t, columns, 2)
			require.EqualValues(t, 40, columns[1].Typ.Width)
			names := make([]string, 0, len(p.GetQuery().CatalogDependencies))
			for _, ref := range p.GetQuery().CatalogDependencies {
				names = append(names, ref.ObjName)
			}
			require.ElementsMatch(t, []string{"outer_b", "middle", "source"}, names)
			c := compile.NewCompile(cn.GetServiceConfig().CN.ServiceAddress, database, query, "", "",
				svc.GetEngine(), proc, statements[0], false, nil, time.Now())
			defer c.Release()
			var gotIDs []int32
			var gotLabels []string
			require.NoError(t, c.Compile(ctx, p, func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
				if bat != nil && bat.RowCount() > 0 {
					ids := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
					for i := 0; i < bat.RowCount(); i++ {
						gotIDs = append(gotIDs, ids[i])
						gotLabels = append(gotLabels, bat.Vecs[1].GetStringAt(i))
					}
				}
				return nil
			}))
			_, err = c.Run(0)
			require.NoError(t, err)
			require.Equal(t, []int32{3}, gotIDs)
			require.Equal(t, []string{"first"}, gotLabels)
		})
		t.Run("cross-consumer DDL snapshots", func(t *testing.T) {
			testNativeViewMetadataDDLTransitions(t, ctx, cn, svc, database)
		})
	})
}

func testNativeViewSchemaRootScopes(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, database string) {
	t.Helper()
	for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
		for _, tc := range []struct {
			name, query string
			physical    bool
		}{
			{"no_from", "select 1 as one", false},
			{"dual", "select 1 as one from dual", false},
			{"qualified_dual", "select 1 as one from " + database + ".dual", true},
			{"cte_declaration", "with q as (select id from " + database + ".target), r as (select * from q) select sum(id) from (with q as (select id from " + database + ".outer_a) select * from r) d", false},
			{"cte_order", "with r as (select * from target), target as (select id from " + database + ".outer_a) select sum(id) from r", false},
		} {
			cte := tc.name == "cte_declaration" || tc.name == "cte_order"
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				withNativeViewSchemaRead(t, ctx, cn, svc, database, nil, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, proc *process.Process) {
					statements, err := mysql.Parse(ctx, tc.query, 1)
					require.NoError(t, err)
					defer statements[0].Free()
					var input tree.Statement = statements[0]
					if mode == "text" {
						input = &tree.PrepareString{Name: "literal_plan", Sql: tc.query}
					} else if mode == "typed" {
						input = &tree.PrepareStmt{Name: "literal_plan", Stmt: statements[0]}
					}
					denied := errors.New("physical root denied")
					authorizations := 0
					p, err := compiler.BuildPlanWithViewSchema(ctx, input, mode == "direct", func(_ context.Context, db, name string, _ *plan.Snapshot) error {
						authorizations++
						if cte && db == database && name == "target" {
							return nil
						}
						return denied
					})
					if tc.physical {
						require.ErrorIs(t, err, denied)
						require.Nil(t, p)
						require.Equal(t, 1, authorizations)
						return
					}
					require.NoError(t, err)
					if cte {
						require.Positive(t, authorizations)
					} else {
						require.Zero(t, authorizations)
					}
					if mode == "text" || mode == "typed" {
						if !cte {
							require.Empty(t, p.GetDcl().GetPrepare().Schemas)
						}
						p = p.GetDcl().GetPrepare().Plan
					}
					if cte {
						require.NotEmpty(t, p.GetQuery().CatalogDependencies)
						for _, ref := range p.GetQuery().CatalogDependencies {
							require.NotEqual(t, "outer_a", ref.ObjName)
						}
					} else {
						require.Empty(t, p.GetQuery().CatalogDependencies)
					}
					c := compile.NewCompile(cn.GetServiceConfig().CN.ServiceAddress, database, tc.query, "", "",
						svc.GetEngine(), proc, statements[0], false, nil, time.Now())
					defer c.Release()
					var values []int64
					require.NoError(t, c.Compile(ctx, p, func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
						if bat != nil && bat.RowCount() > 0 {
							if bat.Vecs[0].GetType().Oid != types.T_int64 {
								return fmt.Errorf("constant source returned type %s", bat.Vecs[0].GetType())
							}
							values = append(values, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...)
						}
						return nil
					}))
					_, err = c.Run(0)
					require.NoError(t, err)
					want := int64(1)
					if cte {
						want = 3
					}
					require.Equal(t, []int64{want}, values)
				})
			})
		}
	}
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
			// These source databases must predate the publication database.
			"create database subscribed",
			"create table subscribed.collision_source (id int)",
			"create database schema_other",
			"create table schema_other.collision_source (id int)",
			"insert into subscribed.collision_source values (17)",
			"insert into schema_other.collision_source values (23)",
		} {
			_, err := system.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		var sourceHistory timestamp.Timestamp
		publisherCtx := defines.AttachAccount(ctx, catalog.System_Account, catalog.System_User, catalog.System_Role)
		withNativeViewSchemaRead(t, publisherCtx, cn, svc, "subscribed", nil, func(_ *frontend.TxnCompilerContext, op client.TxnOperator, _ *process.Process) {
			sourceHistory = op.SnapshotTS()
		})
		for _, query := range []string{
			"create database schema_publisher",
			"create table schema_publisher.source (id int)",
			"create table schema_publisher.collision_source (id varchar(30))",
		} {
			_, err := system.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		for _, database := range []string{"subscribed", "schema_publisher"} {
			var count uint64
			query := fmt.Sprintf("select count(*) from mo_catalog.mo_database {MO_TS = '%s'} where account_id = 0 and datname = ?", sourceHistory.DebugString())
			require.NoError(t, system.QueryRowContext(ctx, query, database).Scan(&count))
			if database == "subscribed" {
				require.EqualValues(t, 1, count)
			} else {
				require.Zero(t, count, "publication DB cannot stand in for the historical source")
			}
		}
		otherPublisher := openNativeViewSchemaSQL(t, ctx, cn, "dump:111", "schema_other")
		_, err := otherPublisher.ExecContext(ctx, "create view inner_v as select id from collision_source")
		require.NoError(t, err)
		publisher := openNativeViewSchemaSQL(t, ctx, cn, "dump:111", "schema_publisher")
		_, err = publisher.ExecContext(ctx, "create view published as select id from source")
		require.NoError(t, err)
		_, err = publisher.ExecContext(ctx, "create view qualified as select id from schema_publisher.source")
		require.NoError(t, err)
		for _, query := range []string{
			"create view v_collision as select id from subscribed.collision_source",
			"create view v_nested as select id from schema_publisher.v_collision",
			"create view v_other as select id from schema_other.collision_source",
			"create view v_other_nested as select id from schema_publisher.v_other",
			"create view v_cross_middle as select id from schema_other.inner_v",
			fmt.Sprintf("create view v_history_collision as select id from subscribed.collision_source {MO_TS = '%s'}", sourceHistory.DebugString()),
			"create view v_history_nested as select id from schema_publisher.v_history_collision",
			fmt.Sprintf("create view v_history_other as select id from schema_other.collision_source {MO_TS = '%s'}", sourceHistory.DebugString()),
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
		_, err = subscriber.ExecContext(ctx, "insert into schema_other.collision_source values ('subscriber-only')")
		require.NoError(t, err)
		for _, query := range []string{
			"create view schema_other.local_view as select id from schema_other.collision_source",
			"create view schema_other.local_nested as select id from schema_other.local_view",
		} {
			_, err = subscriber.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		localIDs := make(map[string]uint64)
		for _, name := range []string{"collision_source", "local_view", "local_nested"} {
			var id uint64
			require.NoError(t, subscriber.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase = 'schema_other' and relname = ?", name).Scan(&id))
			localIDs[name] = id
		}
		var account uint32
		require.NoError(t, system.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name = 'schema_subscriber'").Scan(&account))
		tenant := &frontend.TenantInfo{Tenant: "schema_subscriber", User: "root", DefaultRole: frontend.GetAccountAdminRole(),
			TenantID: account, UserID: frontend.GetAdminUserId(), DefaultRoleID: frontend.GetAccountAdminRoleId()}
		tenantCtx := defines.AttachAccount(ctx, account, tenant.UserID, tenant.DefaultRoleID)
		cases := []struct{ view, sourceDatabase, sourceView string }{
			{"v_collision", "subscribed", ""}, {"v_nested", "subscribed", ""},
			{"v_saved_collision", "subscribed", ""}, {"v_other", "schema_other", ""},
			{"v_other_nested", "schema_other", ""},
			{"v_cross_middle", "schema_other", "inner_v"},
			{"v_history_collision", "subscribed", ""}, {"v_history_nested", "subscribed", ""},
			{"v_history_other", "schema_other", ""},
		}
		t.Run("derived subscription metadata", func(t *testing.T) { testNativeSubscribedViewMetadata(t, tenantCtx, cn, svc, tenant) })
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
			for _, test := range cases {
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
								if test.view == "v_history_collision" || test.view == "v_history_nested" || test.view == "v_history_other" {
									require.NotNil(t, dependency.Snapshot)
									require.Equal(t, sourceHistory, *dependency.Snapshot.TS)
								}
								found = true
							}
						}
						require.True(t, found)
					}
					for _, prepared := range []bool{false, true} {
						query := "select id from subscribed." + test.view
						if prepared {
							query = "prepare derived_schema from '" + query + "'"
						}
						statements, err := mysql.Parse(tenantCtx, query, 1)
						require.NoError(t, err)
						func() {
							defer statements[0].Free()
							p, err := compiler.BuildPlanWithViewSchema(tenantCtx, statements[0], false, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
							require.NoError(t, err, "prepared=%v", prepared)
							if prepared {
								p = p.GetDcl().GetPrepare().Plan
							}
							require.EqualValues(t, types.T_int32, plan.GetResultColumnsFromPlan(p)[0].Typ.Id)
							foundPublisherSource := false
							for _, ref := range p.GetQuery().CatalogDependencies {
								if ref.ObjName == "collision_source" && ref.PubInfo != nil && ref.PubInfo.TenantId == int32(catalog.System_Account) {
									require.Equal(t, test.sourceDatabase, ref.SchemaName, "prepared=%t dependency=%+v", prepared, ref)
									foundPublisherSource = true
								}
							}
							require.True(t, foundPublisherSource, "reusable plans need the actual publisher catalog account")
							if test.sourceView != "" {
								foundMiddle := false
								for _, ref := range p.GetQuery().CatalogDependencies {
									if ref.ObjName == test.sourceView {
										require.Equal(t, test.sourceDatabase, ref.SchemaName)
										require.NotNil(t, ref.PubInfo)
										require.EqualValues(t, catalog.System_Account, ref.PubInfo.TenantId)
										require.Empty(t, ref.SubscriptionName)
										foundMiddle = true
									}
								}
								require.True(t, foundMiddle)
								foundPath := false
								for _, ref := range p.GetQuery().ViewReferences {
									for _, step := range ref.ViewPath {
										if step.ViewName == test.sourceView {
											require.Equal(t, test.sourceDatabase, step.DatabaseName)
											require.Empty(t, step.SubscriptionName)
											require.EqualValues(t, catalog.System_Account, step.Snapshot.Tenant.TenantID)
											foundPath = true
										}
									}
								}
								require.True(t, foundPath)
							}
						}()
					}
				})
			}
			t.Run("cross_middle_ctas", func(t *testing.T) {
				for _, prepared := range []bool{false, true} {
					query := "create table schema_other.plan_only_middle as select id from subscribed.v_cross_middle"
					if prepared {
						query = "prepare middle_ctas from '" + query + "'"
					}
					func() {
						statements, err := mysql.Parse(tenantCtx, query, 1)
						require.NoError(t, err)
						defer statements[0].Free()
						p, err := compiler.BuildPlanWithViewSchema(tenantCtx, statements[0], false, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
						require.NoError(t, err, "prepared=%t", prepared)
						if prepared {
							p = p.GetDcl().GetPrepare().Plan
						}
						columns := p.GetDdl().GetCreateTable().TableDef.Cols
						require.Equal(t, "id", columns[0].Name)
						require.EqualValues(t, types.T_int32, columns[0].Typ.Id)
					}()
				}
			})
			// Alternate domains within one request, including memo reuse and an
			// error between successful roots. Inherited state must govern neither
			// the local root's identity nor its nested source binding.
			for _, active := range []*pb.SubscriptionMeta{inherited, nil} {
				t.Run(fmt.Sprintf("local_root/inherited=%t", active != nil), func(t *testing.T) {
					compiler.SetQueryingSubscription(active)
					defer compiler.SetQueryingSubscription(inherited)
					mixed := compiler.NewViewSchemaRequest(tenantCtx, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
					defer mixed.Close()
					for _, name := range []string{"local_view", "local_nested", "local_view", "local_nested"} {
						value := describeNativeViewSchema(t, mixed, "schema_other", name, nil)
						require.Len(t, value.columns, 1)
						require.Equal(t, int32(types.T_varchar), value.columns[0].Typ.Id)
						foundRoot, foundSource := false, false
						for _, dependency := range value.dependencies {
							require.Equal(t, account, dependency.AccountID)
							require.Equal(t, "schema_other", dependency.DatabaseName)
							require.Empty(t, dependency.SubscriptionName)
							require.Equal(t, localIDs[dependency.RelationName], dependency.RelationID)
							foundRoot = foundRoot || dependency.RelationName == name
							foundSource = foundSource || dependency.RelationName == "collision_source"
						}
						require.True(t, foundRoot)
						require.True(t, foundSource)
						require.Equal(t, baseline, describeNativeViewSchema(t, mixed, "subscribed", "published", nil))
						result, err := mixed.Describe("schema_other", "missing", nil)
						if result != nil {
							result.Release()
						}
						require.Nil(t, result)
						require.Error(t, err)
						require.Same(t, active, compiler.GetQueryingSubscription())
					}
					mixed.Close()
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
		// Execution owns a fresh statement/transaction: Compile/Release may
		// retire its parent resource generation, unlike read-only description.
		for _, test := range cases {
			for _, prepared := range []bool{false, true} {
				t.Run(fmt.Sprintf("execute/%s/prepared=%t", test.view, prepared), func(t *testing.T) {
					withNativeViewSchemaRead(t, tenantCtx, cn, svc, "subscribed", tenant, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, proc *process.Process) {
						query := "select id from subscribed." + test.view
						if prepared {
							query = "prepare derived_schema from '" + query + "'"
						}
						statements, err := mysql.Parse(tenantCtx, query, 1)
						require.NoError(t, err)
						defer statements[0].Free()
						p, err := compiler.BuildPlanWithViewSchema(tenantCtx, statements[0], false, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
						require.NoError(t, err)
						if prepared {
							p = p.GetDcl().GetPrepare().Plan
						}
						require.EqualValues(t, types.T_int32, plan.GetResultColumnsFromPlan(p)[0].Typ.Id)
						c := compile.NewCompile(cn.GetServiceConfig().CN.ServiceAddress, "subscribed", query, "", "",
							svc.GetEngine(), proc, statements[0], false, nil, time.Now())
						defer c.Release()
						var ids []int32
						require.NoError(t, c.Compile(tenantCtx, p, func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
							if bat != nil && bat.RowCount() > 0 {
								if bat.Vecs[0].GetType().Oid != types.T_int32 {
									return fmt.Errorf("publisher int column executed with type %s", bat.Vecs[0].GetType())
								}
								ids = append(ids, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])...)
							}
							return nil
						}))
						_, err = c.Run(0)
						require.NoError(t, err)
						want := int32(17)
						if test.sourceDatabase == "schema_other" {
							want = 23
						}
						require.Equal(t, []int32{want}, ids, "execution must read the publisher")
					})
				})
			}
		}
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

		t.Run("prepared_caller_snapshot", func(t *testing.T) {
			withNativeViewSchemaRead(t, tenantCtx, cn, svc, "subscribed", tenant, func(compiler *frontend.TxnCompilerContext, _ client.TxnOperator, _ *process.Process) {
				snapshot := &plan.Snapshot{TS: &historical, Tenant: &pb.SnapshotTenant{TenantID: account}}
				previous := compiler.GetSnapshot()
				compiler.SetSnapshot(snapshot)
				defer compiler.SetSnapshot(previous)
				roleAuthorize := frontend.NativeViewSchemaRoleAuthorizerForTest(compiler)
				authorize := func(ctx context.Context, database, name string, actual *plan.Snapshot) error {
					if actual == nil || actual.TS == nil || !actual.TS.Equal(historical) {
						return fmt.Errorf("only the established historical snapshot is authorized: %s.%s actual=%+v expected=%s", database, name, actual, historical.DebugString())
					}
					require.Equal(t, snapshot, actual)
					return roleAuthorize(ctx, database, name, actual)
				}
				for _, mode := range []string{"ordinary", "direct", "text", "typed"} {
					for _, shape := range []struct {
						name, query     string
						aggregate, ctas bool
					}{
						{"", "select * from subscribed.published", false, false},
						{"_cte_sum", "with q as (select id from subscribed.published) select sum(id) from q", true, false},
						{"_ctas", "create table schema_other.historical_target as select * from subscribed.published", false, true},
					} {
						t.Run(mode+shape.name, func(t *testing.T) {
							query := shape.query
							statements, err := mysql.Parse(tenantCtx, query, 1)
							require.NoError(t, err)
							defer statements[0].Free()
							var input tree.Statement = statements[0]
							if mode == "text" {
								input = &tree.PrepareString{Name: "historical_schema", Sql: query}
							} else if mode == "typed" {
								input = &tree.PrepareStmt{Name: "historical_schema", Stmt: statements[0]}
							}
							p, err := compiler.BuildPlanWithViewSchema(tenantCtx, input, mode == "direct", authorize)
							require.NoError(t, err)
							if mode == "text" || mode == "typed" {
								p = p.GetDcl().GetPrepare().Plan
							}
							columns := plan.GetResultColumnsFromPlan(p)
							if shape.ctas {
								columns = nil
								for _, col := range p.GetDdl().GetCreateTable().TableDef.Cols {
									if !col.Hidden {
										columns = append(columns, col)
									}
								}
							}
							require.Len(t, columns, 1)
							if !shape.aggregate {
								require.Equal(t, "id", columns[0].Name)
								require.EqualValues(t, types.T_int32, columns[0].Typ.Id)
							}
						})
					}
				}
			})
		})
		_, err = subscriber.ExecContext(ctx, "create user denied identified by 'test123'")
		require.NoError(t, err)
		var userID, roleID uint32
		require.NoError(t, subscriber.QueryRowContext(ctx, "select user_id from mo_catalog.mo_user where user_name = 'denied'").Scan(&userID))
		require.NoError(t, subscriber.QueryRowContext(ctx, "select role_id from mo_catalog.mo_role where role_name = 'public'").Scan(&roleID))
		deniedTenant := &frontend.TenantInfo{Tenant: "schema_subscriber", User: "denied", DefaultRole: "public",
			TenantID: account, UserID: userID, DefaultRoleID: roleID}
		deniedCtx := defines.AttachAccount(ctx, account, userID, roleID)
		t.Run("excluded metadata never derives", func(t *testing.T) {
			testNativeViewMetadataCase(t, deniedCtx, cn, svc, "subscribed", deniedTenant, nativeViewMetadataCase{
				name: "denied", sql: "select cast(count(*) as char) from information_schema.columns where table_schema='subscribed' and table_name in ('published','missing')", want: [][]string{{"0"}},
			})
		})
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
				statements, parseErr := mysql.Parse(deniedCtx, "select * from subscribed."+name, 1)
				require.NoError(t, parseErr)
				func() {
					defer statements[0].Free()
					p, planErr := compiler.BuildPlanWithViewSchema(deniedCtx, statements[0], false, frontend.NativeViewSchemaRoleAuthorizerForTest(compiler))
					require.Nil(t, p)
					require.ErrorContains(t, planErr, "view schema access denied")
				}()
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
