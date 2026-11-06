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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestPartitionMemberBindingSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const schema = "partition_member_binding"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(query string) {
			t.Helper()
			_, err := db.ExecContext(ctx, query)
			require.NoError(t, err)
		}
		exec("create database " + schema)
		exec("create table " + schema + ".t(a int not null,b int not null,primary key(a)) partition by hash(a) partitions 2")
		exec("insert into " + schema + ".t values(1,10),(2,20),(3,30),(4,40)")
		exec("delete from " + schema + ".t where a=2")
		exec("update " + schema + ".t set b=31 where a=3")
		rows, err := db.QueryContext(ctx, "select a,b from "+schema+".t order by a")
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var a, b int
			require.NoError(t, rows.Scan(&a, &b))
			got = append(got, fmt.Sprintf("%d:%d", a, b))
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"1:10", "3:31", "4:40"}, got)
	})
}

func TestPartitionIndexMemberBindingSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		const schema = "partition_index_member_binding"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(query string) {
			t.Helper()
			_, err := db.ExecContext(ctx, query)
			require.NoError(t, err)
		}
		exec("create database " + schema)
		exec("create table " + schema + ".t(a int not null,b int not null,primary key(a),key idx_b(b)) partition by hash(a) partitions 2")
		exec("insert into " + schema + ".t values(1,10),(2,20),(3,30),(4,40)")
		exec("delete from " + schema + ".t where a=2")
		exec("update " + schema + ".t set b=31 where a=3")
		explain, err := db.QueryContext(ctx, "explain select a,b from "+schema+".t force index(idx_b) where b=31")
		require.NoError(t, err)
		defer explain.Close()
		var planLines []string
		for explain.Next() {
			var line string
			require.NoError(t, explain.Scan(&line))
			planLines = append(planLines, line)
		}
		require.NoError(t, explain.Err())
		require.NoError(t, explain.Close())
		require.Contains(t, strings.Join(planLines, "\n"), "Index Table Scan on t.idx_b")
		var matchedID, matchedB int
		require.NoError(t, db.QueryRowContext(ctx, "select a,b from "+schema+".t force index(idx_b) where b=31").Scan(&matchedID, &matchedB))
		require.Equal(t, 3, matchedID)
		require.Equal(t, 31, matchedB)
		rows, err := db.QueryContext(ctx, "select a,b from "+schema+".t force index(idx_b) where b>=10 order by a")
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var a, b int
			require.NoError(t, rows.Scan(&a, &b))
			got = append(got, fmt.Sprintf("%d:%d", a, b))
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"1:10", "3:31", "4:40"}, got)
	})
}

// A relation opened by an RC transaction must not keep the old member set
// when a later statement observes committed partition DDL.
func TestPartitionMembersRefreshAfterRCSnapshotSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(2)
		const schema = "partition_member_rc_refresh"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(query string) {
			t.Helper()
			ddlCtx, ddlCancel := context.WithTimeout(ctx, 20*time.Second)
			defer ddlCancel()
			_, err := db.ExecContext(ddlCtx, query)
			require.NoError(t, err)
		}
		exec("create database " + schema)
		exec("create table " + schema + ".t(a int not null,b int not null,primary key(a)) partition by list(a) (partition p0 values in (1,2),partition p1 values in (3,4))")
		exec("insert into " + schema + ".t values(1,10),(2,20),(3,30),(4,40)")

		reader, err := db.Conn(ctx)
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, _ = reader.ExecContext(cleanupCtx, "rollback")
			_ = reader.Close()
		}()
		_, err = reader.ExecContext(ctx, "set session transaction_isolation='READ-COMMITTED'")
		require.NoError(t, err)
		_, err = reader.ExecContext(ctx, "begin")
		require.NoError(t, err)
		read := func(want []string) {
			t.Helper()
			rows, err := reader.QueryContext(ctx, "select a,b from "+schema+".t order by a")
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var a, b int
				require.NoError(t, rows.Scan(&a, &b))
				got = append(got, fmt.Sprintf("%d:%d", a, b))
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, got)
		}
		read([]string{"1:10", "2:20", "3:30", "4:40"})
		exec("alter table " + schema + ".t add partition (partition p2 values in (5,6))")
		exec("insert into " + schema + ".t values(5,50)")
		read([]string{"1:10", "2:20", "3:30", "4:40", "5:50"})
		exec("alter table " + schema + ".t drop partition p1")
		read([]string{"1:10", "2:20", "5:50"})
	})
}

// Partition metadata changed by the current transaction must be visible to
// subsequent statements without changing its snapshot or leaking on rollback.
func TestPartitionMembersRefreshWithinTxnSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "partition_member_tx_refresh"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		_, err = db.ExecContext(ctx, "create database "+schema)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "create table "+schema+".t(a int not null,b int not null,primary key(a)) partition by list(a) (partition p0 values in (1,2),partition p1 values in (3,4))")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "insert into "+schema+".t values(1,10),(2,20)")
		require.NoError(t, err)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "set session transaction_isolation='REPEATABLE-READ'")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "begin")
		require.NoError(t, err)
		active := true
		defer func() {
			if active {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				_, _ = conn.ExecContext(cleanupCtx, "rollback")
			}
		}()
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+schema+".t").Scan(&count))
		require.Equal(t, 2, count)
		_, err = conn.ExecContext(ctx, "alter table "+schema+".t add partition (partition p2 values in (5,6))")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "insert into "+schema+".t values(5,50)")
		require.NoError(t, err)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from "+schema+".t where a=5 and b=50").Scan(&count))
		require.Equal(t, 1, count)
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		active = false
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".t where a=5").Scan(&count))
		require.Zero(t, count)
		_, err = db.ExecContext(ctx, "insert into "+schema+".t values(5,50)")
		require.Error(t, err, "the newly added partition must also be rolled back")
	})
}
