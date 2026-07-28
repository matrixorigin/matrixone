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
	embed.RunBaseClusterTests(func(c embed.Cluster) {
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
		)
		exec("set role moadmin")
		exec("drop snapshot if exists " + snapshotName)
		exec("drop database if exists " + dbName)
		exec("create database " + dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = conn.ExecContext(cleanupCtx, "rollback")
			_, _ = conn.ExecContext(cleanupCtx, "drop snapshot if exists "+snapshotName)
			_, _ = conn.ExecContext(cleanupCtx, "drop database if exists "+dbName)
		}()
		exec("create table " + dbName + ".src (id int primary key, v varchar(20))")
		exec("insert into " + dbName + ".src values (1, 'a')")
		exec("create table " + dbName + ".baseline clone " + dbName + ".src")
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".baseline").Scan(&count))
		require.Equal(t, 1, count)
		exec("drop table " + dbName + ".baseline")
		exec("create snapshot " + snapshotName + " for table " + dbName + " src")

		exec("begin")
		exec("create table " + dbName + ".snapshot_clone clone " + dbName + ".src {snapshot='" + snapshotName + "'}")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".snapshot_clone").Scan(&count))
		require.Equal(t, 1, count)
		exec("create table " + dbName + ".c1 clone " + dbName + ".src")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".c1").Scan(&count))
		require.Equal(t, 1, count)
		exec("create table " + dbName + ".c2 clone " + dbName + ".c1")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".c2").Scan(&count))
		require.Equal(t, 1, count)
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
		require.Equal(t, 1, count)
		exec("data branch create table " + dbName + ".b2 from " + dbName + ".b1")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".b2").Scan(&count))
		require.Equal(t, 1, count)
		exec("data branch create table " + dbName + ".b3 from " + dbName + ".b2")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+dbName+".b3").Scan(&count))
		require.Equal(t, 1, count)
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
	})
}
