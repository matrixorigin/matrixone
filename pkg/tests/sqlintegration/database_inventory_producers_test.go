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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Shared lifecycle hooks must cover the real SQL producers, not only CREATE.
// Rows/objects are minimal; this is not a Rebase or performance workload.
func TestDatabaseInventoryRelationProducers(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists nr_inventory_producers_snap", "drop database if exists nr_inventory_producers")
		f := rebaseSQLFixture{t, ctx, db, "nr_inventory_producers_"}
		f.exec("create database nr_inventory_producers character set utf8mb4 collate utf8mb4_general_ci")
		reader, err := db.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()
		_, err = reader.ExecContext(ctx, "use nr_inventory_producers")
		require.NoError(t, err)
		id := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_producers'")
		version := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
		require.EqualValues(t, 1, f.scalar(version))
		for _, step := range []struct {
			name, query, probe string
			advance, read      bool
			want               int64
		}{
			{name: "create table", query: "create table nr_inventory_producers.t(id int primary key,v int)", advance: true},
			{name: "insert is not a directory change", query: "insert into nr_inventory_producers.t values(1,10)"},
			{name: "create table no-op", query: "create table if not exists nr_inventory_producers.t(id int primary key,v int)"},
			{name: "alter database defaults", query: "alter database nr_inventory_producers collate utf8mb4_bin", advance: true},
			{name: "unchanged database defaults", query: "alter database nr_inventory_producers collate utf8mb4_bin"},
			{name: "legacy compatibility configuration", query: "alter database nr_inventory_producers set mysql_compatibility_mode='0.8.0'", advance: true,
				probe: "select count(*) from mo_catalog.mo_mysql_compatibility_mode where dat_name='nr_inventory_producers' and variable_name='version_compatibility' and variable_value='0.8.0'", want: 1},
			{name: "in-place comment", query: "alter table nr_inventory_producers.t comment='inventory'", advance: true},
			{name: "add column", query: "alter table nr_inventory_producers.t add column c int default 7", advance: true, probe: "select c from nr_inventory_producers.t where id=1", want: 7},
			{name: "drop column", query: "alter table nr_inventory_producers.t drop column c", advance: true},
			{name: "rename table", query: "alter table nr_inventory_producers.t rename to renamed", advance: true, probe: "select v from nr_inventory_producers.renamed where id=1", want: 10},
			{name: "create index", query: "create index idx_v on nr_inventory_producers.renamed(v)", advance: true},
			{name: "drop index", query: "drop index idx_v on nr_inventory_producers.renamed", advance: true},
			{name: "create view", query: "create view nr_inventory_producers.v as select id,v from nr_inventory_producers.renamed", advance: true},
			{name: "alter view", query: "alter view nr_inventory_producers.v as select id,v+1 as v from nr_inventory_producers.renamed", advance: true, probe: "select v from nr_inventory_producers.v where id=1", want: 11},
			{name: "drop view", query: "drop view nr_inventory_producers.v", advance: true},
			{name: "create sequence", query: "create sequence nr_inventory_producers.seq as bigint increment by 1 start with 1 no cycle", advance: true},
			{name: "nextval is not a directory change", query: "select nextval('seq')", read: true, want: 1},
			{name: "alter sequence", query: "alter sequence nr_inventory_producers.seq increment by 2", advance: true},
			{name: "drop sequence", query: "drop sequence nr_inventory_producers.seq", advance: true},
			{name: "create snapshot is not a directory change", query: "create snapshot nr_inventory_producers_snap for database nr_inventory_producers"},
			{name: "drop snapshot is not a directory change", query: "drop snapshot nr_inventory_producers_snap"},
		} {
			t.Logf("inventory producer: %s", step.name)
			before := f.scalar(version)
			if step.read {
				var value int64
				require.NoError(t, reader.QueryRowContext(ctx, step.query).Scan(&value), step.name)
				require.Equal(t, step.want, value, step.name)
			} else {
				f.exec(step.query)
			}
			after := f.scalar(version)
			if step.advance {
				require.Greater(t, after, before, step.name)
			} else {
				require.Equal(t, before, after, step.name)
			}
			if step.probe != "" {
				require.Equal(t, step.want, f.scalar(step.probe), step.name)
			}
		}

		before := f.scalar(version)
		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, _ = owner.ExecContext(cleanupCtx, "rollback")
		}()
		defaultsVersion := fmt.Sprintf("select cast(version as signed) from mo_catalog.mo_database_defaults where account_id=0 and database_id=%d", id)
		beforeDefaults := f.scalar(defaultsVersion)
		for _, query := range []string{
			"set session transaction isolation level read committed", "begin",
			"alter database nr_inventory_producers collate utf8mb4_general_ci", "rollback",
		} {
			_, err := owner.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		require.Equal(t, before, f.scalar(version))
		require.Equal(t, beforeDefaults, f.scalar(defaultsVersion))
		for _, query := range []string{
			"set session transaction isolation level read committed", "begin",
			"alter table nr_inventory_producers.renamed add column rolled_back int default 9", "rollback",
		} {
			_, err := owner.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		require.Equal(t, before, f.scalar(version))
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_columns where account_id=0 and att_database='nr_inventory_producers' and att_relname='renamed' and attname='rolled_back'"))
		f.exec("truncate table nr_inventory_producers.renamed")
		require.Greater(t, f.scalar(version), before)
		require.Zero(t, f.scalar("select count(*) from nr_inventory_producers.renamed"))
		before = f.scalar(version)
		f.exec("drop table nr_inventory_producers.renamed")
		require.Greater(t, f.scalar(version), before)
	})
}

func TestDatabaseInventoryRoutineRetirement(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists nr_inventory_retirement")
		f := rebaseSQLFixture{t, ctx, db, "nr_inventory_retirement_"}
		f.exec("create database nr_inventory_retirement")
		f.exec("create table nr_inventory_retirement.t(id int primary key,v int)")
		f.exec("insert into nr_inventory_retirement.t values(1,10)")
		f.exec("create function nr_inventory_retirement.inventory_fn(x int) returns int language sql as '$1+1'")
		f.exec("create procedure nr_inventory_retirement.inventory_proc() 'begin select 1; end'")
		compatibilityCount := "select count(*) from mo_catalog.mo_mysql_compatibility_mode where account_id=0 and dat_name='nr_inventory_retirement'"
		require.EqualValues(t, 1, f.scalar(compatibilityCount))
		for _, table := range []string{"mo_stored_procedure", "mo_user_defined_function"} {
			require.EqualValues(t, 1, f.scalar("select count(*) from mo_catalog."+table+" where db='nr_inventory_retirement'"))
		}
		id := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_retirement'")
		version := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
		before := f.scalar(version)
		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_, _ = owner.ExecContext(cleanupCtx, "rollback")
		}()
		for _, query := range []string{"set session transaction isolation level read committed", "begin", "drop database nr_inventory_retirement"} {
			_, err := owner.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		var stagedCount int64
		require.NoError(t, owner.QueryRowContext(ctx, compatibilityCount).Scan(&stagedCount))
		require.Zero(t, stagedCount, "compatibility settings retire inside the DROP owner")
		_, err = owner.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		require.EqualValues(t, 1, f.scalar(compatibilityCount), "DROP rollback must restore name-keyed settings")
		require.Equal(t, before, f.scalar(version))
		require.Equal(t, []string{"1:10"}, f.rows("nr_inventory_retirement", "t"))
		procedureCount := f.scalar("select count(*) from mo_catalog.mo_stored_procedure where db='nr_inventory_retirement'")
		functionCount := f.scalar("select count(*) from mo_catalog.mo_user_defined_function where db='nr_inventory_retirement'")
		t.Logf("routine retirement rollback counts: procedure=%d function=%d", procedureCount, functionCount)
		for _, table := range []string{"mo_stored_procedure", "mo_user_defined_function"} {
			require.EqualValues(t, 1, f.scalar("select count(*) from mo_catalog."+table+" where db='nr_inventory_retirement'"),
				"database rollback must restore %s in the same owner", table)
		}
		f.exec("drop database nr_inventory_retirement")
		require.Zero(t, f.scalar(compatibilityCount))
		for _, table := range []string{"mo_database", "mo_user_defined_function", "mo_stored_procedure"} {
			column := "db"
			if table == "mo_database" {
				column = "datname"
			}
			require.Zero(t, f.scalar("select count(*) from mo_catalog."+table+" where "+column+"='nr_inventory_retirement'"))
		}
		require.Zero(t, f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)))
		f.exec("create database nr_inventory_retirement")
		replacement := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_retirement'")
		require.NotEqual(t, id, replacement)
		f.exec("alter database nr_inventory_retirement set mysql_compatibility_mode = '0.8.0'")
		require.EqualValues(t, 1, f.scalar(compatibilityCount))
		var compatibility string
		require.NoError(t, db.QueryRowContext(ctx, "select variable_value from mo_catalog.mo_mysql_compatibility_mode where account_id=0 and dat_name='nr_inventory_retirement' and variable_name='version_compatibility'").Scan(&compatibility))
		require.Equal(t, "0.8.0", compatibility)
	})
}

func TestDatabaseCompatibilityCreationTransaction(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists nr_inventory_creation")
		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer stop()
			_, _ = owner.ExecContext(cleanupCtx, "rollback")
		}()
		var sessionVersion string
		require.NoError(t, owner.QueryRowContext(ctx, "select @@version").Scan(&sessionVersion))
		require.NotEmpty(t, sessionVersion)
		for _, query := range []string{"set session transaction isolation level read committed", "begin", "create database nr_inventory_creation"} {
			_, err = owner.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		settings := "select variable_value from mo_catalog.mo_mysql_compatibility_mode where account_id=0 and dat_name='nr_inventory_creation' and variable_name='version_compatibility'"
		var stagedVersion string
		require.NoError(t, owner.QueryRowContext(ctx, settings).Scan(&stagedVersion))
		require.Equal(t, sessionVersion, stagedVersion)
		var firstID uint64
		require.NoError(t, owner.QueryRowContext(ctx, "select dat_id from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_creation'").Scan(&firstID))
		require.Positive(t, firstID)
		_, err = owner.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		f := rebaseSQLFixture{t, ctx, db, "nr_inventory_creation_"}
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_creation'"))
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_mysql_compatibility_mode where account_id=0 and dat_name='nr_inventory_creation'"))
		require.Zero(t, f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", firstID)))
		_, err = owner.ExecContext(ctx, "create database nr_inventory_creation")
		require.NoError(t, err)
		var committedVersion string
		require.NoError(t, db.QueryRowContext(ctx, settings).Scan(&committedVersion))
		require.Equal(t, sessionVersion, committedVersion)
		f.exec("create database if not exists nr_inventory_creation")
		require.EqualValues(t, 1, f.scalar("select count(*) from mo_catalog.mo_mysql_compatibility_mode where account_id=0 and dat_name='nr_inventory_creation'"), "no-op CREATE must not initialize compatibility twice")
	})
}

func TestDatabaseInventoryRoutineCreation(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		service := cn.RawService().(cnservice.Service)
		inventory, ok := service.GetEngine().(interface {
			ReserveDatabaseInventory(context.Context, string, uint64, uint64, client.TxnOperator) error
		})
		require.True(t, ok)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists nr_inventory_routines_clone", "drop database if exists nr_inventory_routines")
		f := rebaseSQLFixture{t, ctx, db, "nr_inventory_routines_"}
		f.exec("create database nr_inventory_routines")
		f.exec("create table nr_inventory_routines.t(id int primary key,v int)")
		f.exec("insert into nr_inventory_routines.t values(1,10)")
		id := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_routines'")
		version := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
		for _, step := range []struct {
			query, catalog, name string
		}{
			{"create function nr_inventory_routines.inventory_fn(x int) returns int language sql as '$1+1'", "mo_user_defined_function", "inventory_fn"},
			{"create procedure nr_inventory_routines.inventory_proc() 'begin select 1; end'", "mo_stored_procedure", "inventory_proc"},
		} {
			before := f.scalar(version)
			f.exec(step.query)
			after := f.scalar(version)
			require.Greater(t, after, before)
			require.EqualValues(t, 1, f.scalar("select count(*) from mo_catalog."+step.catalog+" where db='nr_inventory_routines' and name='"+step.name+"'"))
			reserveCtx := defines.AttachAccountId(ctx, catalog.System_Account)
			err := service.GetSQLExecutor().ExecTxn(reserveCtx, func(tx executor.TxnExecutor) error {
				return inventory.ReserveDatabaseInventory(reserveCtx, "nr_inventory_routines", uint64(id), uint64(before), tx.Txn())
			}, executor.Options{}.WithAccountID(0).WithTxnMode(pbtxn.TxnMode_Pessimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC))
			require.Error(t, err, "a token fixed before routine creation must not publish over it")
			_, err = db.ExecContext(ctx, step.query)
			require.Error(t, err, "duplicate routine declaration must fail")
			require.Equal(t, after, f.scalar(version), "rejected declaration must not retain an epoch write")
		}
		checkFunction := func(database string) {
			t.Helper()
			reader, err := db.Conn(ctx)
			require.NoError(t, err)
			defer reader.Close()
			_, err = reader.ExecContext(ctx, "use "+database)
			require.NoError(t, err)
			var value int64
			require.NoError(t, reader.QueryRowContext(ctx, "select inventory_fn(41)").Scan(&value))
			require.EqualValues(t, 42, value)
		}
		checkFunction("nr_inventory_routines")
		f.exec("data branch create database nr_inventory_routines_clone from nr_inventory_routines")
		checkFunction("nr_inventory_routines_clone")
		require.EqualValues(t, 1, f.scalar("select count(*) from mo_catalog.mo_stored_procedure where db='nr_inventory_routines_clone' and name='inventory_proc'"))
		cloneID := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_routines_clone'")
		require.NotEqual(t, id, cloneID)
		require.EqualValues(t, 1, f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d and epoch>1", cloneID)))
		require.Equal(t, []string{"1:10"}, f.rows("nr_inventory_routines", "t"))
		require.Equal(t, []string{"1:10"}, f.rows("nr_inventory_routines_clone", "t"))
		for _, step := range []struct {
			query, catalog, name string
		}{
			{"drop function nr_inventory_routines.inventory_fn(int)", "mo_user_defined_function", "inventory_fn"},
			{"drop procedure nr_inventory_routines.inventory_proc", "mo_stored_procedure", "inventory_proc"},
		} {
			before := f.scalar(version)
			f.exec(step.query)
			after := f.scalar(version)
			require.Greater(t, after, before)
			require.Zero(t, f.scalar("select count(*) from mo_catalog."+step.catalog+" where db='nr_inventory_routines' and name='"+step.name+"'"))
			reserveCtx := defines.AttachAccountId(ctx, catalog.System_Account)
			err := service.GetSQLExecutor().ExecTxn(reserveCtx, func(tx executor.TxnExecutor) error {
				return inventory.ReserveDatabaseInventory(reserveCtx, "nr_inventory_routines", uint64(id), uint64(before), tx.Txn())
			}, executor.Options{}.WithAccountID(0).WithTxnMode(pbtxn.TxnMode_Pessimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC))
			require.Error(t, err, "a token fixed before routine deletion must not publish over it")
			_, err = db.ExecContext(ctx, step.query)
			require.Error(t, err, "missing routine must still be rejected")
			require.Equal(t, after, f.scalar(version))
		}
		f.exec("drop function if exists nr_inventory_routines.inventory_fn(int)")
		f.exec("drop procedure if exists nr_inventory_routines.inventory_proc")
		require.EqualValues(t, 1, f.scalar("select count(*) from mo_catalog.mo_stored_procedure where db='nr_inventory_routines_clone' and name='inventory_proc'"))
		checkFunction("nr_inventory_routines_clone")
	})
}
