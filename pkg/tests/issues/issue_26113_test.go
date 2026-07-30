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

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue26113CloneCreatedTableInSameTransaction(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(statement string) {
			_, execErr := conn.ExecContext(ctx, statement)
			require.NoError(t, execErr, statement)
		}
		const (
			dbName       = "issue_26113"
			snapshotName = "issue_26113_snapshot"
			roleName     = "issue_26113_copy_grants_role"
		)
		exec("set role moadmin")
		exec("drop snapshot if exists " + snapshotName)
		exec("drop database if exists " + dbName)
		exec("drop role if exists " + roleName)
		exec("create database " + dbName)
		exec("create role " + roleName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			cleanup := func(statement string) {
				_, cleanupErr := conn.ExecContext(cleanupCtx, statement)
				require.NoError(t, cleanupErr, statement)
			}
			cleanup("rollback")
			cleanup("drop snapshot if exists " + snapshotName)
			cleanup("drop database if exists " + dbName)
			cleanup("drop role if exists " + roleName)
		}()
		exec("create table " + dbName + ".src (id int primary key, v varchar(20))")
		exec("insert into " + dbName + ".src values (1, 'a')")
		exec("grant select on table " + dbName + ".src to " + roleName + " with grant option")
		exec("create table " + dbName + ".baseline clone " + dbName + ".src")
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".baseline").Scan(&count))
		require.Equal(t, 1, count)
		exec("drop table " + dbName + ".baseline")
		exec("create snapshot " + snapshotName + " for table " + dbName + " src")
		exec("insert into " + dbName + ".src values (2, 'after snapshot')")

		exec("begin")
		exec("create table " + dbName + ".snapshot_clone clone " + dbName + ".src {snapshot='" + snapshotName + "'}")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".snapshot_clone").Scan(&count))
		require.Equal(t, 1, count)
		exec("create table " + dbName + ".c1 clone " + dbName + ".src")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".c1").Scan(&count))
		require.Equal(t, 2, count)
		exec("create table " + dbName + ".c2 clone " + dbName + ".c1")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".c2").Scan(&count))
		require.Equal(t, 2, count)
		exec("rollback")
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname in ('snapshot_clone', 'c1', 'c2')").Scan(&count))
		require.Zero(t, count)

		exec("begin")
		exec("data branch create table " + dbName + ".r1 from " + dbName + ".src")
		exec("data branch create table " + dbName + ".r2 from " + dbName + ".r1")
		var r1ID, r2ID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'r1'").Scan(&r1ID))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'r2'").Scan(&r2ID))
		exec("rollback")
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname in ('r1', 'r2')").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			"select count(*) from mo_catalog.mo_branch_metadata where table_id in (%d, %d)", r1ID, r2ID)).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			"select count(*) from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_%d', '__mo_branch_%d')", r1ID, r2ID)).Scan(&count))
		require.Zero(t, count)

		exec("begin")
		exec("data branch create table " + dbName + ".b1 from " + dbName + ".src")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".b1").Scan(&count))
		require.Equal(t, 2, count)
		exec("data branch create table " + dbName + ".b2 from " + dbName + ".b1")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".b2").Scan(&count))
		require.Equal(t, 2, count)
		exec("data branch create table " + dbName + ".b3 from " + dbName + ".b2")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".b3").Scan(&count))
		require.Equal(t, 2, count)
		exec("commit")

		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_branch_metadata m "+
				"join mo_catalog.mo_tables child on m.table_id = child.rel_id "+
				"join mo_catalog.mo_tables parent on m.p_table_id = parent.rel_id "+
				"where child.reldatabase = '"+dbName+"' and m.clone_ts > 0 and m.table_deleted = false "+
				"and ((child.relname = 'b1' and parent.relname = 'src') or (child.relname = 'b2' and parent.relname = 'b1') "+
				"or (child.relname = 'b3' and parent.relname = 'b2'))").Scan(&count))
		require.Equal(t, 3, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_branch_metadata m "+
				"join mo_catalog.mo_tables child on m.table_id = child.rel_id "+
				"join mo_catalog.mo_snapshots s on s.sname = concat('__mo_branch_', cast(m.table_id as char)) "+
				"and s.kind = 'branch' and s.ts = m.clone_ts "+
				"where child.reldatabase = '"+dbName+"' and child.relname in ('b1', 'b2', 'b3')").Scan(&count))
		require.Equal(t, 3, count)

		exec("update " + dbName + ".b1 set v = 'changed' where id = 1")
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".b2 against "+dbName+".b1 output count").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".b3 against "+dbName+".src output count").Scan(&count))
		require.Zero(t, count)
		exec("drop table " + dbName + ".b1")
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".b3 against "+dbName+".src output count").Scan(&count))
		require.Zero(t, count)

		exec("begin")
		exec("data branch create table " + dbName + ".d1 from " + dbName + ".src")
		exec("data branch create table " + dbName + ".d2 from " + dbName + ".d1")
		var d1ID, d2ID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'd1'").Scan(&d1ID))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'd2'").Scan(&d2ID))
		exec("drop table " + dbName + ".d1")
		exec("commit")
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where rel_id = ?", d1ID).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_branch_metadata where table_id = ? and table_deleted = true", d1ID).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_branch_metadata where table_id = ? and p_table_id = ? and table_deleted = false", d2ID, d1ID).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_snapshots where kind = 'branch' and sname = concat('__mo_branch_', cast(? as char))", d2ID).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".d2 against "+dbName+".src output count").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".src against "+dbName+".d2 output count").Scan(&count))
		require.Zero(t, count)

		exec("begin")
		exec("data branch create table " + dbName + ".e1 from " + dbName + ".src")
		exec("insert into " + dbName + ".e1 values (3, 'txn change')")
		exec("data branch create table " + dbName + ".e2 from " + dbName + ".e1")
		exec("drop table " + dbName + ".e1")
		exec("commit")
		var value string
		require.NoError(t, conn.QueryRowContext(ctx,
			"select v from "+dbName+".e2 where id = 3").Scan(&value))
		require.Equal(t, "txn change", value)
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".e2 against "+dbName+".src output count").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".src against "+dbName+".e2 output count").Scan(&count))
		require.Equal(t, 1, count)

		exec("begin")
		exec("data branch create table " + dbName + ".f1 from " + dbName + ".src")
		exec("insert into " + dbName + ".f1 values (4, 'shared txn change')")
		exec("data branch create table " + dbName + ".f2 from " + dbName + ".f1")
		exec("data branch create table " + dbName + ".f3 from " + dbName + ".f1")
		var f1ID, f2ID, f3ID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'f1'").Scan(&f1ID))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'f2'").Scan(&f2ID))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'f3'").Scan(&f3ID))
		exec("drop table " + dbName + ".f1")
		exec("commit")
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_branch_metadata where table_id = ? and table_deleted = true", f1ID).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			"select count(*) from mo_catalog.mo_branch_metadata where table_id in (%d, %d) and p_table_id = %d and table_deleted = false",
			f2ID, f3ID, f1ID)).Scan(&count))
		require.Equal(t, 2, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".f2 against "+dbName+".f3 output count").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".f3 against "+dbName+".f2 output count").Scan(&count))
		require.Zero(t, count)
		exec("insert into " + dbName + ".f2 values (5, 'target-only change')")
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".f2 against "+dbName+".f3 output count").Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, conn.QueryRowContext(ctx,
			"data branch diff "+dbName+".f3 against "+dbName+".f2 output count").Scan(&count))
		require.Equal(t, 1, count)

		exec("begin")
		exec("create table " + dbName + ".cgs clone " + dbName + ".src {snapshot='" + snapshotName + "'} copy grants")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".cgs").Scan(&count))
		require.Equal(t, 1, count)
		exec("create table " + dbName + ".cg1 clone " + dbName + ".src copy grants")
		exec("create table " + dbName + ".cg2 clone " + dbName + ".cg1 copy grants")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".cg2").Scan(&count))
		require.Equal(t, 2, count)
		var cgsID, cg1ID, cg2ID uint64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'cgs'").Scan(&cgsID))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'cg1'").Scan(&cg1ID))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname = 'cg2'").Scan(&cg2ID))
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_role_privs where role_name = '"+roleName+"' "+
				fmt.Sprintf("and obj_id in (%d, %d) and privilege_name = 'select' and with_grant_option = true", cgsID, cg2ID)).Scan(&count))
		require.Equal(t, 2, count)
		exec("rollback")
		require.NoError(t, conn.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase = '"+dbName+"' and relname in ('cgs', 'cg1', 'cg2')").Scan(&count))
		require.Zero(t, count)
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			"select count(*) from mo_catalog.mo_role_privs where obj_id in (%d, %d, %d)", cgsID, cg1ID, cg2ID)).Scan(&count))
		require.Zero(t, count)
	})
}
