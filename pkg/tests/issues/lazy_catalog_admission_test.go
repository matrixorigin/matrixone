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

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestLazyCatalogCrossAccountAdmission(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		open := func(t *testing.T, index int, user string) *sql.DB {
			t.Helper()
			cn, err := cluster.GetCNService(index)
			require.NoError(t, err)
			cfg := mysql.NewConfig()
			cfg.User, cfg.Passwd = user, "111"
			cfg.Net = "tcp"
			cfg.Addr = fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
			cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 5*time.Second, 30*time.Second, 30*time.Second
			db, err := sql.Open("mysql", cfg.FormatDSN())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sys0, sys1 := open(t, 0, "dump"), open(t, 1, "dump")
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		eng1 := cn1.RawService().(cnservice.Service).GetEngine().(*disttae.Engine)
		accountID := func(t *testing.T, name string) uint32 {
			t.Helper()
			var id uint32
			require.NoError(t, sys0.QueryRowContext(ctx,
				"select account_id from mo_catalog.mo_account where account_name=?", name).Scan(&id))
			return id
		}
		defer func() {
			for _, q := range []string{
				"drop account if exists lazy_catalog_reader_owner",
				"drop account if exists lazy_counts_owner",
				"drop snapshot if exists lazy_remote_snapshot",
				"drop account if exists lazy_remote_owner",
				"drop snapshot if exists lazy_admission_snapshot",
				"drop account if exists lazy_snapshot_owner",
				"drop account if exists lazy_quota_owner",
				"drop snapshot if exists lazy_quota_source_snapshot",
				"drop database if exists lazy_quota_source",
			} {
				func() {
					cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
					defer stop()
					execSQLRequire(t, cleanup, sys0, q)
				}()
			}
		}()
		for _, q := range []string{
			"create database lazy_quota_source",
			"create table lazy_quota_source.base(id int primary key)",
			"insert into lazy_quota_source.base values(1)",
			"create snapshot lazy_quota_source_snapshot for table lazy_quota_source base",
		} {
			execSQLRequire(t, ctx, sys0, q)
		}

		t.Run("ordinary sys catalog reads include cold accounts", func(t *testing.T) {
			execSQLRequire(t, ctx, sys0, "create account lazy_catalog_reader_owner admin_name 'admin' identified by '111'")
			tenant := open(t, 0, "lazy_catalog_reader_owner#admin#accountadmin")
			execSQLRequire(t, ctx, tenant, "create database lazy_catalog_reader_db")
			execSQLRequire(t, ctx, tenant, "create table lazy_catalog_reader_db.one(id int)")
			id := accountID(t, "lazy_catalog_reader_owner")
			require.False(t, eng1.PushClient().CanServeAccount(id, types.MaxTs().ToTimestamp()))
			check := func(query string, want int) {
				t.Helper()
				for _, db := range []*sql.DB{sys0, sys1} {
					var count int
					require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(query, id)).Scan(&count))
					require.Equal(t, want, count)
				}
			}
			check("select count(*) from mo_catalog.mo_database where account_id=%d and datname='lazy_catalog_reader_db'", 1)
			check("select count(*) from mo_catalog.mo_tables where account_id=%d and reldatabase='lazy_catalog_reader_db' and relname='one'", 1)
			check("select count(*) from mo_catalog.mo_columns where account_id=%d and att_database='lazy_catalog_reader_db' and att_relname='one' and attname='id'", 1)
			check("select count(*) from mo_catalog.mo_tables where (account_id=0 or account_id=%d) and reldatabase='lazy_catalog_reader_db' and relname='one'", 1)
			execSQLRequire(t, ctx, tenant, "create table lazy_catalog_reader_db.two(id int)")
			check("select count(*) from mo_catalog.mo_tables where account_id=%d and reldatabase='lazy_catalog_reader_db' and relname='two'", 1)
			execSQLRequire(t, ctx, tenant, "drop table lazy_catalog_reader_db.two")
			check("select count(*) from mo_catalog.mo_tables where account_id=%d and reldatabase='lazy_catalog_reader_db' and relname='two'", 0)
			require.False(t, eng1.PushClient().CanServeAccount(id, types.MaxTs().ToTimestamp()),
				"a catalog scan must not silently add a cold account's persistent cache/subscription")
		})

		t.Run("show accounts includes cold catalog counts", func(t *testing.T) {
			execSQLRequire(t, ctx, sys0, "create account lazy_counts_owner admin_name 'admin' identified by '111'")
			tenant := open(t, 0, "lazy_counts_owner#admin#accountadmin")
			execSQLRequire(t, ctx, tenant, "create database lazy_counts_db")
			execSQLRequire(t, ctx, tenant, "create table lazy_counts_db.t(id int)")
			id := accountID(t, "lazy_counts_owner")
			require.False(t, eng1.PushClient().CanServeAccount(id, types.MaxTs().ToTimestamp()))
			counts := func(db *sql.DB) [2]string {
				rows, err := db.QueryContext(ctx, "show accounts like 'lazy_counts_owner'")
				require.NoError(t, err)
				defer rows.Close()
				columns, err := rows.Columns()
				require.NoError(t, err)
				values := make([]sql.NullString, len(columns))
				dest := make([]any, len(columns))
				for i := range values {
					dest[i] = &values[i]
				}
				require.True(t, rows.Next())
				require.NoError(t, rows.Scan(dest...))
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
				return [2]string{values[5].String, values[6].String}
			}
			require.Equal(t, counts(sys0), counts(sys1), "cold and warm CN must report identical database/table counts")
		})

		t.Run("snapshot activates a cold owner", func(t *testing.T) {
			execSQLRequire(t, ctx, sys0, "create account lazy_snapshot_owner admin_name 'admin' identified by '111'")
			tenant := open(t, 0, "lazy_snapshot_owner#admin#accountadmin")
			for _, q := range []string{
				"create database lazy_admission_db",
				"create table lazy_admission_db.t(id int primary key)",
				"insert into lazy_admission_db.t values(1)",
			} {
				execSQLRequire(t, ctx, tenant, q)
			}
			id := accountID(t, "lazy_snapshot_owner")
			require.False(t, eng1.PushClient().CanServeAccount(id, types.MaxTs().ToTimestamp()))
			execSQLRequire(t, ctx, sys1, "create snapshot lazy_admission_snapshot for account lazy_snapshot_owner")
			require.True(t, eng1.PushClient().CanServeAccount(id, types.MaxTs().ToTimestamp()))
			execSQLRequire(t, ctx, tenant, "insert into lazy_admission_db.t values(2)")
			require.NoError(t, tenant.Close())
			execSQLRequire(t, ctx, sys1, "restore account lazy_snapshot_owner {snapshot=lazy_admission_snapshot}")
			restored := open(t, 0, "lazy_snapshot_owner#admin#accountadmin")
			var count int
			require.NoError(t, restored.QueryRowContext(ctx, "select count(*) from lazy_admission_db.t").Scan(&count))
			require.Equal(t, 1, count)
			require.NoError(t, restored.Close())
		})

		t.Run("remote execution activates a cold catalog", func(t *testing.T) {
			cn0, err := cluster.GetCNService(0)
			require.NoError(t, err)
			service0 := cn0.RawService().(cnservice.Service)
			eng0 := service0.GetEngine().(*disttae.Engine)
			service1 := cn1.RawService().(cnservice.Service)
			require.Eventually(t, func() bool {
				for _, service := range []cnservice.Service{service0, service1} {
					clusterservice.GetMOCluster(service.ID()).ForceRefresh(true)
					nodes, err := service.GetEngine().Nodes(false, "sys", "dump", nil)
					if err != nil || len(nodes) != 2 {
						return false
					}
				}
				return true
			}, 15*time.Second, 100*time.Millisecond)

			execSQLRequire(t, ctx, sys0, "create account lazy_remote_owner admin_name 'admin' identified by '111'")
			tenant := open(t, 0, "lazy_remote_owner#admin#accountadmin")
			for _, q := range []string{
				"create database lazy_remote_db",
				"create table lazy_remote_db.src(id int primary key)",
				"create table lazy_remote_db.dst(id int auto_increment primary key, v int)",
				"insert into lazy_remote_db.src values(1)",
			} {
				execSQLRequire(t, ctx, tenant, q)
			}
			sourceID := accountID(t, "lazy_remote_owner")
			require.False(t, eng1.PushClient().CanServeAccount(sourceID, types.MaxTs().ToTimestamp()))
			oldForce := plan2.GetForceScanOnMultiCN()
			plan2.SetForceScanOnMultiCN(true)
			defer plan2.SetForceScanOnMultiCN(oldForce)
			execSQLRequire(t, ctx, tenant, "insert into lazy_remote_db.dst(v) select id from lazy_remote_db.src")
			require.True(t, eng1.PushClient().CanServeAccount(sourceID, types.MaxTs().ToTimestamp()),
				"the remote pipeline must activate its own catalog")
			var id, value int
			require.NoError(t, tenant.QueryRowContext(ctx, "select id,v from lazy_remote_db.dst").Scan(&id, &value))
			require.Equal(t, 1, id)
			require.Equal(t, 1, value)

			execSQLRequire(t, ctx, sys1, "create snapshot lazy_remote_snapshot for account lazy_remote_owner")
			require.NoError(t, tenant.Close())
			execSQLRequire(t, ctx, sys1, "drop account lazy_remote_owner")
			execSQLRequire(t, ctx, sys1, "restore account lazy_remote_owner {snapshot=lazy_remote_snapshot}")
			targetID := accountID(t, "lazy_remote_owner")
			require.NotEqual(t, sourceID, targetID)
			require.True(t, eng0.PushClient().CanServeAccount(targetID, types.MaxTs().ToTimestamp()),
				"restore must activate the recreated account on the remote CN before login")
			restored := open(t, 0, "lazy_remote_owner#admin#accountadmin")
			require.NoError(t, restored.QueryRowContext(ctx, "select id,v from lazy_remote_db.dst").Scan(&id, &value))
			require.Equal(t, 1, id)
			require.Equal(t, 1, value)
		})

		t.Run("cold CN cannot overrun branch quota", func(t *testing.T) {
			execSQLRequire(t, ctx, sys0, "create account lazy_quota_owner admin_name 'admin' identified by '111'")
			tenant := open(t, 0, "lazy_quota_owner#admin#accountadmin")
			defer tenant.Close()
			execSQLRequire(t, ctx, tenant, "create database lazy_quota_target")
			id := accountID(t, "lazy_quota_owner")
			execSQLRequire(t, ctx, sys0, fmt.Sprintf("select mo_feature_limit_upsert(%d,'branch','',1)", id))
			execSQLRequire(t, ctx, sys0, "data branch create table lazy_quota_target.first_branch from lazy_quota_source.base{snapshot='lazy_quota_source_snapshot'} to account lazy_quota_owner")
			require.False(t, eng1.PushClient().CanServeAccount(id, types.MaxTs().ToTimestamp()))
			_, err := sys1.ExecContext(ctx,
				"data branch create table lazy_quota_target.second_branch from lazy_quota_source.base{snapshot='lazy_quota_source_snapshot'} to account lazy_quota_owner")
			require.ErrorContains(t, err, "limit of 1")
			var count int
			require.NoError(t, tenant.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_tables where reldatabase='lazy_quota_target' and relname in ('first_branch','second_branch')").Scan(&count))
			require.Equal(t, 1, count)
		})

		t.Run("alter lineage fits the system schema", func(t *testing.T) {
			execSQLRequire(t, ctx, sys0, "data branch create table lazy_quota_source.history from lazy_quota_source.base")
			execSQLRequire(t, ctx, sys0, "truncate table lazy_quota_source.history")
			execSQLRequire(t, ctx, sys0, "alter table lazy_quota_source.history add column extra int")
			var count int
			require.NoError(t, sys0.QueryRowContext(ctx, "select count(*) from lazy_quota_source.history").Scan(&count))
			require.Zero(t, count)
			require.NoError(t, sys0.QueryRowContext(ctx, "select count(*) from lazy_quota_source.base").Scan(&count))
			require.Equal(t, 1, count)
		})
	})
}
