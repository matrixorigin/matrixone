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

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// This probes the existing transaction substrate, not a Rebase executor. The
// replay is deliberately predetermined: no diff or conflict algorithm is hidden
// in the test. Branch metadata and its retention pin must follow the same owner.
func TestRebaseTransactionPrerequisites(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists rebase_p1_u",
			"drop database if exists rebase_p1_committed",
			"drop database if exists rebase_p1_aborted",
			"drop database if exists rebase_p1_upstream")
		helpers := func(t *testing.T) (func(string), func(string) int) {
			return func(query string) {
					t.Helper()
					_, err := conn.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}, func(query string) int {
					t.Helper()
					var n int
					require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&n), query)
					return n
				}
		}
		exec, _ := helpers(t)
		exec("create database rebase_p1_upstream")
		for _, table := range []string{"a", "b"} {
			exec("create table rebase_p1_upstream." + table + "(id int primary key, v int)")
			exec("insert into rebase_p1_upstream." + table + " values (1,10),(2,20)")
		}
		exec("create snapshot rebase_p1_u for database rebase_p1_upstream")
		exec("set session transaction isolation level read committed")

		t.Run("late_table_failure_rolls_back_data_edge_and_pin", func(t *testing.T) {
			exec, count := helpers(t)
			exec("begin")
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				_, rollbackErr := conn.ExecContext(cleanupCtx, "rollback")
				require.NoError(t, rollbackErr)
			}()
			exec("data branch create database rebase_p1_aborted from rebase_p1_upstream{snapshot='rebase_p1_u'}")
			var childID uint64
			require.NoError(t, conn.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='rebase_p1_aborted' and relname='a'").Scan(&childID))
			require.Equal(t, 1, count(fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where table_id=%d", childID)))
			require.Equal(t, 1, count(fmt.Sprintf("select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname='__mo_branch_%d'", childID)))
			exec("update rebase_p1_aborted.a set v=11 where id=1")
			_, err := conn.ExecContext(ctx, "insert into rebase_p1_aborted.b values(1,99)")
			require.Error(t, err, "second-table constraint failure must not publish the first-table update")
			exec("rollback")
			require.Zero(t, count("select count(*) from mo_catalog.mo_database where account_id=0 and datname='rebase_p1_aborted'"))
			require.Zero(t, count(fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata where table_id=%d", childID)))
			require.Zero(t, count(fmt.Sprintf("select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname='__mo_branch_%d'", childID)))
			require.Equal(t, 10, count("select v from rebase_p1_upstream.a where id=1"))
			require.Equal(t, 10, count("select v from rebase_p1_upstream.b where id=1"))
		})

		if t.Failed() {
			return
		}
		t.Run("commit_retains_upstream_base_before_replay", func(t *testing.T) {
			exec, count := helpers(t)
			exec("begin")
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				_, rollbackErr := conn.ExecContext(cleanupCtx, "rollback")
				require.NoError(t, rollbackErr)
			}()
			exec("data branch create database rebase_p1_committed from rebase_p1_upstream{snapshot='rebase_p1_u'}")
			exec("update rebase_p1_committed.a set v=11 where id=1")
			exec("delete from rebase_p1_committed.b where id=2")
			exec("commit")
			require.Equal(t, 11, count("select v from rebase_p1_committed.a where id=1"))
			require.Equal(t, 1, count("select count(*) from rebase_p1_committed.b"))
			exec("drop snapshot rebase_p1_u")
			var commonBaseTS int64
			for _, table := range []string{"a", "b"} {
				var parentID, childID uint64
				var baseTS int64
				require.NoError(t, conn.QueryRowContext(ctx,
					"select b.table_id,b.p_table_id,b.clone_ts from mo_catalog.mo_branch_metadata b join mo_catalog.mo_tables t on b.table_id=t.rel_id where t.account_id=0 and t.reldatabase='rebase_p1_committed' and t.relname=?", table).Scan(&childID, &parentID, &baseTS))
				require.Positive(t, baseTS)
				if commonBaseTS == 0 {
					commonBaseTS = baseTS
				}
				require.Equal(t, commonBaseTS, baseTS, "all tables must bind the same immutable U")
				require.Equal(t, 1, count(fmt.Sprintf("select count(*) from mo_catalog.mo_snapshots where kind='branch' and sname='__mo_branch_%d' and obj_id=%d and ts=%d", childID, parentID, baseTS)))
				exec("update rebase_p1_upstream." + table + " set v=99 where id=1")
				require.Equal(t, 10, count(fmt.Sprintf("select v from rebase_p1_upstream.%s{MO_TS=%d} where id=1", table, baseTS)), "retained base is U, not the replayed destination or a moving upstream")
			}
		})
	})
}
