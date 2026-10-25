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

package sqlintegration

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/stretchr/testify/require"
)

func TestTemporaryTablePhysicalNameIsSessionScoped(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "tmp_iso_30584"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)

		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		attacker, err := db.Conn(ctx)
		require.NoError(t, err)
		defer attacker.Close()

		exec := func(conn *sql.Conn, statement string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement, args...)
			require.NoError(t, err, statement)
		}
		exec(owner, "create database "+schema)
		exec(owner, "use "+schema)
		exec(attacker, "use "+schema)
		exec(owner, "create temporary table victim_t(id int primary key, v varchar(32))")
		exec(owner, "insert into victim_t values (1, 'owner')")

		var physical string
		require.NoError(t, owner.QueryRowContext(ctx,
			"select relname from mo_catalog.mo_tables where reldatabase=? and relkind='temporary_table'",
			schema).Scan(&physical))
		qualified := "`" + schema + "`.`" + physical + "`"

		var visible int
		require.NoError(t, attacker.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase=? and relname=?", schema, physical).Scan(&visible))
		require.Zero(t, visible, "foreign temporary roots must not be enumerable")

		assertMySQLError := func(statement string, want ...uint16) {
			t.Helper()
			_, err := attacker.ExecContext(ctx, statement)
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr, statement)
			for _, code := range want {
				if sqlErr.Number == code {
					return
				}
			}
			require.Failf(t, "unexpected MySQL error", "%s returned %d", statement, sqlErr.Number)
		}
		assertMySQLError("select * from "+qualified, 1146)
		assertMySQLError("insert into "+qualified+" values (2, 'attacker')", 1146)
		assertMySQLError("drop table "+qualified, 1051, 1146)

		var count int
		require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from victim_t").Scan(&count))
		require.Equal(t, 1, count)
		exec(owner, "drop temporary table victim_t")
		exec(owner, "create temporary table victim_t(id int primary key, v varchar(32))")
		exec(owner, "insert into victim_t values (2, 'recreated')")
		require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from victim_t").Scan(&count))
		require.Equal(t, 1, count)

		// The durable marker, rather than the textual prefix, classifies temporary
		// roots. A permanent table with the same generated-name shape remains
		// visible to the other session.
		permanent := "__mo_tmp_00112233445566778899aabbccddeeff_" + schema + "_x"
		exec(owner, "create table `"+permanent+"` (id int)")
		defer exec(owner, "drop table `"+permanent+"`")
		require.NoError(t, attacker.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase=? and relname=?", schema, permanent).Scan(&visible))
		require.Equal(t, 1, visible)

		t.Run("create_index_keeps_owner", func(t *testing.T) {
			_, err := owner.ExecContext(ctx, "create index idx_v on victim_t(v)")
			require.NoError(t, err)
			var value string
			require.NoError(t, owner.QueryRowContext(ctx, "select v from victim_t where v='recreated'").Scan(&value))
			require.Equal(t, "recreated", value)
			_, err = owner.ExecContext(ctx, "drop index idx_v on victim_t")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "create unique index uk_v on victim_t(v)")
			require.NoError(t, err)
			var indexPhysical string
			require.NoError(t, owner.QueryRowContext(ctx, "select relname from mo_catalog.mo_tables where reldatabase=? and relname like '%mo_index%'", schema).Scan(&indexPhysical))
			_, err = attacker.ExecContext(ctx, "select * from `"+schema+"`.`"+indexPhysical+"`")
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr)
			require.EqualValues(t, 1146, sqlErr.Number, "internal backfill must not grant another session access")
			_, err = owner.ExecContext(ctx, "insert into victim_t values(3, 'recreated')")
			require.ErrorAs(t, err, &sqlErr)
			require.EqualValues(t, 1062, sqlErr.Number)
		})

		t.Run("inline_index_survives_alter", func(t *testing.T) {
			exec(owner, "create temporary table alter_t(id int primary key, v varchar(32), unique index uk(v))")
			exec(owner, "insert into alter_t values(1, 'hello')")
			exec(owner, "alter table alter_t rename to renamed_t")
			defer exec(owner, "drop table renamed_t")
			exec(owner, "insert into renamed_t values(2, 'world')")
			exec(owner, "alter table renamed_t add index secondary_v(v), algorithm=inplace")
			exec(owner, "insert into renamed_t values(6, 'added')")
			exec(owner, "alter table renamed_t drop index secondary_v, algorithm=inplace")
			exec(owner, "alter table renamed_t add column extra int, algorithm=copy")
			exec(owner, "insert into renamed_t(id,v) values(3, 'copy')")
			exec(owner, "truncate table renamed_t")
			exec(owner, "insert into renamed_t(id,v) values(4, 'truncate')")
			var v string
			require.NoError(t, owner.QueryRowContext(ctx, "select v from renamed_t").Scan(&v))
			require.Equal(t, "truncate", v)
			_, err := owner.ExecContext(ctx, "insert into renamed_t(id,v) values(5, 'truncate')")
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr)
			require.EqualValues(t, 1062, sqlErr.Number)
		})

		t.Run("permanent_prefix_view", func(t *testing.T) {
			exec(owner, "create table __mo_tmp(id int)")
			defer exec(owner, "drop table __mo_tmp")
			exec(owner, "insert into __mo_tmp values(7)")
			exec(owner, "create view permanent_view as select * from __mo_tmp")
			defer exec(owner, "drop view permanent_view")
			var id int
			require.NoError(t, attacker.QueryRowContext(ctx, "select id from permanent_view").Scan(&id))
			require.Equal(t, 7, id)
		})

		t.Run("inline_async_and_background_fulltext", func(t *testing.T) {
			var original int
			require.NoError(t, owner.QueryRowContext(ctx, "select @@experimental_fulltext2_index").Scan(&original))
			exec(owner, "set experimental_fulltext2_index=1")
			defer func() {
				_, _ = owner.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf("set experimental_fulltext2_index=%d", original))
			}()
			exec(owner, "create temporary table async_t(id int primary key, body varchar(100), fulltext ft(body) async)")
			defer exec(owner, "drop table async_t")
			exec(owner, "create temporary table background_t(id int primary key, body varchar(100), fulltext2 ft(body))")
			defer exec(owner, "drop table background_t")
			exec(owner, "insert into background_t values(1, 'apple')")
			var physical string
			require.NoError(t, owner.QueryRowContext(ctx,
				"select relname from mo_catalog.mo_tables where reldatabase=? and relname like '%_background_t'", schema).Scan(&physical))
			service := cn.RawService().(cnservice.Service)
			initSQL := fmt.Sprintf("alter table `%s`.`%s` alter reindex ft fulltext2 force_sync", schema, physical)
			accountCtx := defines.AttachAccountId(ctx, 0)
			require.NoError(t, iscp.ProcessInitSQL(accountCtx, service.ID(), service.GetEngine(), service.GetTxnClient(),
				base64.StdEncoding.EncodeToString([]byte(initSQL)), schema, physical, "ft"))
			var id int
			require.NoError(t, owner.QueryRowContext(ctx, "select id from background_t where match(body) against('apple')").Scan(&id))
			require.Equal(t, 1, id)
			assertMySQLError("select * from `"+schema+"`.`"+physical+"`", 1146)
		})

		t.Run("clone_preserves_allocator", func(t *testing.T) {
			for _, sourceKind := range []string{"permanent", "temporary"} {
				t.Run(sourceKind, func(t *testing.T) {
					exec := func(t *testing.T, statement string) {
						t.Helper()
						_, err := owner.ExecContext(ctx, statement)
						require.NoError(t, err, statement)
					}
					qualifier := ""
					if sourceKind == "temporary" {
						qualifier = "temporary "
					}
					exec(t, "create "+qualifier+"table owned_clone_source(id bigint auto_increment primary key, v int)")
					defer exec(t, "drop table owned_clone_source")
					exec(t, "insert into owned_clone_source values(7, 11)")
					if sourceKind == "temporary" {
						var physical string
						require.NoError(t, owner.QueryRowContext(ctx,
							"select relname from mo_catalog.mo_tables where reldatabase=? and relname like '%_owned_clone_source'", schema).Scan(&physical))
						// Clone's source discovery reports missing tables as a
						// parser error before reaching allocator compilation.
						_, err := attacker.ExecContext(ctx, "create temporary table stolen_clone clone `"+schema+"`.`"+physical+"`")
						var sqlErr *mysql.MySQLError
						require.ErrorAs(t, err, &sqlErr)
						require.EqualValues(t, 1064, sqlErr.Number)
					}
					for _, destinationKind := range []string{"temporary", "permanent"} {
						t.Run(destinationKind+"_destination", func(t *testing.T) {
							qualifier := ""
							if destinationKind == "temporary" {
								qualifier = "temporary "
							}
							_, err := owner.ExecContext(ctx, "create "+qualifier+"table owned_clone_target clone owned_clone_source")
							require.NoError(t, err)
							defer exec(t, "drop table owned_clone_target")
							exec(t, "insert into owned_clone_target(v) values(12)")
							var id, copied int
							require.NoError(t, owner.QueryRowContext(ctx, "select v from owned_clone_target where id=7").Scan(&copied))
							require.Equal(t, 11, copied)
							require.NoError(t, owner.QueryRowContext(ctx, "select id from owned_clone_target where v=12").Scan(&id))
							require.Equal(t, 8, id)
						})
					}
				})
			}
			_, err := owner.ExecContext(ctx, "create table clone_source(id bigint auto_increment primary key, v int) auto_increment=10")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "insert into clone_source(v) values(1), (2)")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "create temporary table clone_target clone clone_source")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "insert into clone_target(v) values(3)")
			require.NoError(t, err)
			var id int
			require.NoError(t, owner.QueryRowContext(ctx, "select id from clone_target where v=3").Scan(&id))
			require.Equal(t, 12, id)
		})

		t.Run("clone_subscription_preserves_source_account", func(t *testing.T) {
			exec := func(conn *sql.Conn, statement string) {
				t.Helper()
				_, err := conn.ExecContext(ctx, statement)
				require.NoError(t, err, statement)
			}
			const (
				account     = "tmp_clone_subscriber"
				publisherDB = "tmp_clone_publisher"
				publication = "tmp_clone_publication"
			)
			defer func() {
				cleanupSQLIntegration(t, cn,
					"drop account if exists "+account,
					"drop publication if exists "+publication,
					"drop database if exists "+publisherDB)
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				var remaining int
				require.NoError(t, owner.QueryRowContext(cleanupCtx,
					"select count(*) from mo_catalog.mo_account where account_name=?", account).Scan(&remaining))
				require.Zero(t, remaining, "subscriber account must not leak into the shared fixture")
			}()
			exec(owner, "create account "+account+" admin_name 'root' identified by '111'")
			exec(owner, "create database "+publisherDB)
			exec(owner, "create table "+publisherDB+".src(id bigint auto_increment primary key, v int)")
			exec(owner, "insert into "+publisherDB+".src values(7,11)")
			exec(owner, "create publication "+publication+" database "+publisherDB+" account "+account)

			tenantDB, err := sql.Open("mysql", fmt.Sprintf(account+"#root#accountadmin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer tenantDB.Close()
			tenantDB.SetMaxOpenConns(1)
			subscriber, err := tenantDB.Conn(ctx)
			require.NoError(t, err)
			defer subscriber.Close()
			execSubscriber := func(statement string) {
				t.Helper()
				_, err := subscriber.ExecContext(ctx, statement)
				require.NoError(t, err, statement)
			}
			execSubscriber("create database " + publisherDB)
			execSubscriber("create database subscribed from sys publication " + publication)
			cloneAndCheck := func(target string, temporary bool) {
				t.Helper()
				qualifier := ""
				if temporary {
					qualifier = "temporary "
				}
				execSubscriber("create " + qualifier + "table " + publisherDB + "." + target + " clone subscribed.src")
				var copied, next int
				require.NoError(t, subscriber.QueryRowContext(ctx, "select v from "+publisherDB+"."+target+" where id=7").Scan(&copied))
				require.Equal(t, 11, copied)
				execSubscriber("insert into " + publisherDB + "." + target + "(v) values(12)")
				require.NoError(t, subscriber.QueryRowContext(ctx, "select id from "+publisherDB+"."+target+" where v=12").Scan(&next))
				require.Equal(t, 8, next)
			}
			cloneAndCheck("control", false)
			execSubscriber("create temporary table " + publisherDB + ".src(id bigint auto_increment primary key, v int)")
			execSubscriber("insert into " + publisherDB + ".src values(99,22)")
			var published, local int
			require.NoError(t, subscriber.QueryRowContext(ctx, "select v from subscribed.src where id=7").Scan(&published))
			require.Equal(t, 11, published)
			require.NoError(t, subscriber.QueryRowContext(ctx, "select v from "+publisherDB+".src where id=99").Scan(&local))
			require.Equal(t, 22, local)
			cloneAndCheck("target", false)
			cloneAndCheck("temporary_target", true)
			// The same non-system account still owns its local temporary source.
			execSubscriber("create temporary table " + publisherDB + ".owner_target clone " + publisherDB + ".src")
			require.NoError(t, subscriber.QueryRowContext(ctx, "select v from "+publisherDB+".owner_target where id=99").Scan(&local))
			require.Equal(t, 22, local)
			execSubscriber("insert into " + publisherDB + ".owner_target(v) values(23)")
			var localNext int
			require.NoError(t, subscriber.QueryRowContext(ctx, "select id from "+publisherDB+".owner_target where v=23").Scan(&localNext))
			require.Equal(t, 100, localNext)
		})

		t.Run("fulltext_search_keeps_owner", func(t *testing.T) {
			_, err := owner.ExecContext(ctx, "create temporary table search_t(id int primary key, title varchar(50), body varchar(100))")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "insert into search_t values(1, 'apple news', 'red apple'), (2, 'banana news', 'yellow banana')")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "create fulltext index ft on search_t(title, body)")
			require.NoError(t, err)
			var id int
			require.NoError(t, owner.QueryRowContext(ctx, "select id from search_t where match(title, body) against('apple')").Scan(&id))
			require.Equal(t, 1, id)
			var indexPhysical string
			require.NoError(t, owner.QueryRowContext(ctx,
				"select distinct i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase=? and t.relname like '%_search_t' and i.name='ft'", schema).Scan(&indexPhysical))
			_, err = attacker.ExecContext(ctx, "select * from `"+schema+"`.`"+indexPhysical+"`")
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr)
			require.EqualValues(t, 1146, sqlErr.Number, "nested search must not grant another session access")
			_, err = owner.ExecContext(ctx, "create temporary table search_clone clone search_t")
			require.NoError(t, err)
			require.NoError(t, owner.QueryRowContext(ctx, "select id from search_clone where match(title, body) against('apple')").Scan(&id))
			require.Equal(t, 1, id)
		})

		t.Run("clone_vector_index_keeps_owner", func(t *testing.T) {
			var originalIVF int
			require.NoError(t, owner.QueryRowContext(ctx, "select @@experimental_ivf_index").Scan(&originalIVF))
			_, err := owner.ExecContext(ctx, "set experimental_ivf_index=1")
			require.NoError(t, err)
			defer func() {
				_, _ = owner.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf("set experimental_ivf_index=%d", originalIVF))
			}()
			_, err = owner.ExecContext(ctx, "create table vector_source(id int primary key, v vecf32(3))")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "insert into vector_source values(1, '[1,2,3]'), (2, '[2,2,3]')")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "create index ivf using ivfflat on vector_source(v) lists=1 op_type 'vector_l2_ops'")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "create temporary table vector_target clone vector_source")
			require.NoError(t, err)
			var id int
			require.NoError(t, owner.QueryRowContext(ctx, "select id from vector_target order by l2_distance(v, '[2,2,3]') limit 1").Scan(&id))
			require.Equal(t, 2, id)
		})

		t.Run("procedure_temporary_lifecycle", func(t *testing.T) {
			_, err := owner.ExecContext(ctx, "create procedure temp_lifecycle() 'begin create temporary table proc_t(id int); insert into proc_t values(1); drop table proc_t; end'")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "call temp_lifecycle()")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "call temp_lifecycle()")
			require.NoError(t, err, "procedure must release its temporary alias")
		})

		t.Run("drop_database_removes_temporary_catalog", func(t *testing.T) {
			const dropped = "tmp_iso_drop"
			_, err := owner.ExecContext(ctx, "create database "+dropped)
			require.NoError(t, err)
			t.Cleanup(func() { cleanupSQLIntegration(t, cn, "drop database if exists "+dropped) })
			_, err = owner.ExecContext(ctx, "create temporary table "+dropped+".t(id int, v int)")
			require.NoError(t, err)
			_, err = attacker.ExecContext(ctx, "create temporary table "+dropped+".other_session(id int)")
			require.NoError(t, err)
			var databaseID uint64
			require.NoError(t, owner.QueryRowContext(ctx, "select dat_id from mo_catalog.mo_database where datname=?", dropped).Scan(&databaseID))
			_, err = owner.ExecContext(ctx, "begin")
			require.NoError(t, err)
			defer func() { _, _ = owner.ExecContext(context.WithoutCancel(ctx), "rollback") }()
			_, err = owner.ExecContext(ctx, "drop database "+dropped)
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "rollback")
			require.NoError(t, err)
			_, err = owner.ExecContext(ctx, "insert into "+dropped+".t values(1, 2)")
			require.NoError(t, err, "rollback must retain the temporary root")
			_, err = attacker.ExecContext(ctx, "insert into "+dropped+".other_session values(1)")
			require.NoError(t, err, "rollback must retain another session's temporary root")
			_, err = owner.ExecContext(ctx, "drop database "+dropped)
			require.NoError(t, err)
			var remaining int
			require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase_id=?", databaseID).Scan(&remaining))
			require.Zero(t, remaining)
			require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_columns where att_database_id=?", databaseID).Scan(&remaining))
			require.Zero(t, remaining)
		})
	})
}
