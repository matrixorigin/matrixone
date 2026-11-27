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

package embed

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestBoundedPickWithRestoredForkAndIndependentTable(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := recoverySQLClient(t, cn)
		defer db.Close()
		ctx := t.Context()
		run := func(statements ...string) {
			t.Helper()
			for _, statement := range statements {
				_, err := db.ExecContext(ctx, statement)
				require.NoError(t, err, statement)
			}
		}
		checkRows := func(table, want string) {
			t.Helper()
			var rows string
			require.NoError(t, db.QueryRowContext(ctx,
				"select coalesce(group_concat(concat_ws(':',k,v) order by k),'') from "+table).Scan(&rows))
			require.Equal(t, want, rows)
		}

		run("create database pick_restore",
			"create table pick_restore.parent(k int primary key, v int)",
			"insert into pick_restore.parent values(1,10),(2,20)",
			"data branch create table pick_restore.src from pick_restore.parent",
			"delete from pick_restore.parent where k=2",
			"create snapshot fork_restore for table pick_restore src",
			"restore table pick_restore.src {snapshot=fork_restore}",
			"create table pick_restore.dst_fail(k int primary key, v int)",
			"create table pick_restore.dst_skip(k int primary key, v int)",
			"create table pick_restore.src_new(k int primary key, v int)",
			"create snapshot pick_restore_lo for account sys",
			"update pick_restore.src set v=52 where k=2",
			"insert into pick_restore.src values(3,80)",
			"insert into pick_restore.src_new values(4,90)",
			"create snapshot pick_restore_hi for account sys")

		window := " between snapshot pick_restore_lo and pick_restore_hi"
		for _, conflict := range []string{" when conflict fail", " when conflict skip"} {
			_, err := db.ExecContext(ctx, "data branch pick pick_restore.src into pick_restore.parent"+window+conflict)
			require.ErrorContains(t, err, "branch history unavailable after restore")
			checkRows("pick_restore.parent", "1:10")
		}

		run("data branch pick pick_restore.src into pick_restore.dst_fail"+window,
			"data branch pick pick_restore.src into pick_restore.dst_skip"+window+" when conflict skip")
		checkRows("pick_restore.dst_fail", "2:52,3:80")
		checkRows("pick_restore.dst_skip", "2:52,3:80")

		run("data branch pick pick_restore.src_new into pick_restore.src" + window + " when conflict fail")
		checkRows("pick_restore.src", "1:10,2:52,3:80,4:90")

		run("drop snapshot pick_restore_hi", "drop snapshot pick_restore_lo",
			"drop snapshot fork_restore", "drop database pick_restore")
	})
}

func TestBoundedPickWithRestoredBranchCatalog(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sys := recoverySQLClient(t, cn)
		defer sys.Close()
		ctx := t.Context()
		run := func(db *sql.DB, stmts ...string) {
			t.Helper()
			for _, stmt := range stmts {
				_, err := db.ExecContext(ctx, stmt)
				require.NoError(t, err, stmt)
			}
		}
		run(sys, "create account pick_tenant admin_name 'tester' identified by '111'")
		tenant := recoverySQLClientWithUser(t, cn, "pick_tenant#tester")
		defer tenant.Close()
		run(tenant, "create database branch_probe", "create table branch_probe.parent(k int primary key,v int)",
			"insert into branch_probe.parent values(1,10),(2,20)",
			"data branch create table branch_probe.src from branch_probe.parent",
			"create snapshot src_restore for table branch_probe src",
			"restore table branch_probe.src {snapshot=src_restore}")
		run(sys, "create snapshot system_before for account sys")
		run(tenant, "data branch create table branch_probe.dst from branch_probe.parent", "delete from branch_probe.dst where k=2")
		run(sys, "restore account sys {snapshot=system_before}")
		run(tenant, "create snapshot probe_lo for account pick_tenant",
			"update branch_probe.src set v=52 where k=2", "insert into branch_probe.src values(3,80)",
			"create snapshot probe_hi for account pick_tenant")
		_, err = tenant.ExecContext(ctx, "data branch pick branch_probe.src into branch_probe.dst between snapshot probe_lo and probe_hi when conflict skip")
		var result string
		require.NoError(t, tenant.QueryRowContext(ctx, "select group_concat(concat_ws(':',k,v) order by k) from branch_probe.dst").Scan(&result))
		require.Equal(t, "1:10", result)
		require.ErrorContains(t, err, "branch history unavailable after restore")
		run(tenant, "drop snapshot probe_hi", "drop snapshot probe_lo", "drop snapshot src_restore", "drop database branch_probe")
		run(sys, "drop snapshot system_before", "drop account pick_tenant")
	})
}

func TestBoundedPickWithMissingForkEvidence(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := recoverySQLClient(t, cn)
		defer db.Close()
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(t.Context(), "select key_bytes from mo_catalog.mo_branch_fork_keys")
		var sqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &sqlErr)
		require.EqualValues(t, 1146, sqlErr.Number)

		var noAutoIncrement bool
		err = conn.QueryRowContext(t.Context(), "select auto_increment is null from information_schema.tables where table_schema='mo_catalog' and table_name='mo_branch_fork_keys'").Scan(&noAutoIncrement)
		require.NoError(t, err)
		require.True(t, noAutoIncrement)

		for _, tc := range []struct {
			name         string
			setup        []string
			evidenceRows int
			windowWrite  string
			legacyMarker bool
		}{
			{"transaction-local parent", []string{
				"begin", "create table root(k int primary key, v int)",
				"insert into root values(1,10),(2,20)",
				"data branch create table src from root",
				"data branch create table dst from root",
				"delete from dst where k=2", "commit",
			}, 5, "update src set v=52 where k=2", false},
			{"parent key restored in the fork commit", []string{
				"create table root(k int primary key, v int)",
				"insert into root values(1,10),(2,20)", "begin",
				"delete from root where k=2",
				"data branch create table dst from root",
				"insert into root values(2,20)",
				"data branch create table src from root", "commit",
			}, 3, "update src set v=52 where k=2", false},
			{"window insert after an unobservable fork", []string{
				"begin", "create table root(k int primary key, v int)",
				"insert into root values(1,10),(2,20)",
				"data branch create table src from root",
				"data branch create table dst from root",
				"delete from src where k=2", "delete from dst where k=2",
				"drop table root", "commit",
			}, 5, "insert into src values(2,52)", false},
			{"old partition completeness marker", []string{
				"create table root(k int primary key, v int) partition by hash(k) partitions 2",
				"insert into root values(1,10)", "begin", "insert into root values(2,20)",
				"data branch create table src from root",
				"data branch create table dst from root", "commit",
				"delete from dst where k=2",
			}, 5, "update src set v=52 where k=2", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				run := func(statements ...string) {
					t.Helper()
					for _, statement := range statements {
						_, err := conn.ExecContext(t.Context(), statement)
						require.NoError(t, err, statement)
					}
				}
				checkDestination := func(want string) {
					t.Helper()
					var rows string
					require.NoError(t, conn.QueryRowContext(t.Context(),
						"select group_concat(concat_ws(':',k,v) order by k) from dst").Scan(&rows))
					require.Equal(t, want, rows)
				}

				run("create database pick_fork_evidence", "use pick_fork_evidence")
				run(tc.setup...)
				run("create snapshot fork_evidence_lo for account sys",
					tc.windowWrite, "insert into src values(3,80)",
					"create snapshot fork_evidence_hi for account sys")

				// Simulate a branch created without a fork ledger.
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
				defer cancel()
				ctx = context.WithValue(defines.AttachAccountId(ctx, 0), defines.BranchForkKeyAccess{}, true)
				result, err := cn.RawService().(cnservice.Service).GetSQLExecutor().Exec(ctx,
					"delete from mo_catalog.mo_branch_fork_keys where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='pick_fork_evidence' and relname='dst')",
					executor.Options{})
				require.NoError(t, err)
				result.Close()
				require.EqualValues(t, tc.evidenceRows, result.AffectedRows)
				if tc.legacyMarker {
					// An old partition clone could claim completeness
					// even though its pre-commit scan returned no keys.
					result, err = cn.RawService().(cnservice.Service).GetSQLExecutor().Exec(ctx,
						"insert into mo_catalog.mo_branch_fork_keys(table_id,key_bytes) select rel_id,null from mo_catalog.mo_tables where reldatabase='pick_fork_evidence' and relname='dst'",
						executor.Options{})
					require.NoError(t, err)
					result.Close()
					require.EqualValues(t, 1, result.AffectedRows)
				}

				const pick = "data branch pick src into dst between snapshot fork_evidence_lo and fork_evidence_hi keys(2,3) when conflict "
				_, err = conn.ExecContext(t.Context(), pick+"skip")
				require.ErrorContains(t, err, "cannot distinguish an inherited deletion from an absent key without fork history")
				checkDestination("1:10")
				_, err = conn.ExecContext(t.Context(), pick+"fail")
				require.ErrorAs(t, err, &sqlErr)
				require.Contains(t, []uint16{20105, 22501}, sqlErr.Number)
				checkDestination("1:10")
				run(pick + "accept")
				checkDestination("1:10,2:52,3:80")
				run("drop snapshot fork_evidence_hi", "drop snapshot fork_evidence_lo", "drop database pick_fork_evidence")
			})
		}
	})
}

func TestBoundedPickForkEvidenceSurvivesPublisherDrop(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sys := recoverySQLClient(t, cn)
		defer sys.Close()
		control, err := sys.Conn(t.Context())
		require.NoError(t, err)
		defer control.Close()
		run := func(conn *sql.Conn, statements ...string) {
			t.Helper()
			for _, statement := range statements {
				_, err := conn.ExecContext(t.Context(), statement)
				require.NoError(t, err, statement)
			}
		}

		run(control, "create account fork_publisher admin_name 'tester' identified by '111'")
		publisher := recoverySQLClientWithUser(t, cn, "fork_publisher#tester")
		defer publisher.Close()
		owned, err := publisher.Conn(t.Context())
		require.NoError(t, err)
		defer owned.Close()
		run(owned, "create database published", "use published", "begin",
			"create table root(k int primary key,v int)", "insert into root values(1,10),(2,20)",
			"data branch create table dst from root", "delete from dst where k=2",
			"data branch create table src from root", "commit",
			"create publication fork_pub database published account sys")
		run(control, "create database fork_subscription from fork_publisher publication fork_pub",
			"create database fork_survivor", "use fork_survivor",
			"data branch create table src from fork_subscription.src",
			"data branch create table dst from fork_subscription.dst")
		require.NoError(t, owned.Close())
		run(control, "drop account fork_publisher", "create snapshot surviving_fork_lo for account sys",
			"update src set v=52 where k=2", "insert into src values(3,80)",
			"create snapshot surviving_fork_hi for account sys")

		const pick = "data branch pick src into dst between snapshot surviving_fork_lo and surviving_fork_hi when conflict "
		_, err = control.ExecContext(t.Context(), pick+"fail")
		require.ErrorContains(t, err, "conflict:")
		var rows string
		require.NoError(t, control.QueryRowContext(t.Context(),
			"select group_concat(concat_ws(':',k,v) order by k) from dst").Scan(&rows))
		require.Equal(t, "1:10", rows)
		run(control, pick+"skip")
		require.NoError(t, control.QueryRowContext(t.Context(),
			"select group_concat(concat_ws(':',k,v) order by k) from dst").Scan(&rows))
		require.Equal(t, "1:10,3:80", rows)
		run(control, "drop snapshot surviving_fork_hi", "drop snapshot surviving_fork_lo",
			"drop database fork_survivor", "drop database if exists fork_subscription")
	})
}

func TestBoundedPickForkCapture(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := recoverySQLClient(t, cn)
		defer db.Close()
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer conn.Close()
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(t.Context(), statement)
			require.NoError(t, err, statement)
		}
		exec("create database bounded_fork_capture")
		defer exec("drop database bounded_fork_capture")
		exec("use bounded_fork_capture")
		exec("create table root(k int primary key,v int,n bf16,f float16,g float8,h float4,a vecf4(4),b vecf8(4),shape geometry32)")
		exec("begin")
		exec("insert into root values (2,20,1.5,null,1,1,'[1,2,3,4]','[4,3,2,1]',cast(st_geomfromtext('POINT(1 2)') as geometry32))")
		exec("data branch create table src from root")
		for _, dst := range []string{"dst_fail", "dst_skip"} {
			exec("data branch create table " + dst + " from root")
		}
		exec("commit")
		exec("create snapshot capture_lo for table bounded_fork_capture src")
		defer exec("drop snapshot capture_lo")
		exec("update src set v=52 where k=2")
		exec("create snapshot capture_hi for table bounded_fork_capture src")
		defer exec("drop snapshot capture_hi")
		for _, policy := range []string{"fail", "skip"} {
			dst := "dst_" + policy
			exec("data branch pick src into " + dst + " between snapshot capture_lo and capture_hi when conflict " + policy)
			var v int
			require.NoError(t, conn.QueryRowContext(t.Context(), "select v from "+dst+" where k=2").Scan(&v))
			require.Equal(t, 52, v)
		}
		exec("create table part(k int primary key) partition by hash(k) partitions 2")
		exec("insert into part values(1),(2)")
		exec("data branch create table cloned from part")
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		ctx = context.WithValue(defines.AttachAccountId(ctx, 0), defines.BranchForkKeyAccess{}, true)
		result, err := cn.RawService().(cnservice.Service).GetSQLExecutor().Exec(ctx,
			"select count(*) from mo_catalog.mo_branch_fork_keys where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='bounded_fork_capture' and relname='cloned')", executor.Options{})
		require.NoError(t, err)
		defer result.Close()
		require.Len(t, result.Batches, 1)
		require.EqualValues(t, 1, executor.GetFixedRows[int64](result.Batches[0].Vecs[0])[0])
	})
}

func TestBoundedPickPagedForkImage(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := recoverySQLClient(t, cn)
		defer db.Close()
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer conn.Close()
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(t.Context(), statement)
			require.NoError(t, err, statement)
		}
		exec("create database pick_paged")
		defer exec("drop database pick_paged")
		exec("use pick_paged")
		exec("create table root(k int primary key, v int)")
		exec("begin")
		exec("insert into root select result,result from generate_series(1,8000) g")
		exec("data branch create table src from root")
		exec("data branch create table dst from root")
		exec("commit")
		exec("create snapshot paged_lo for table pick_paged src")
		defer exec("drop snapshot paged_lo")
		exec("update src set v=k+1")
		exec("create snapshot paged_hi for table pick_paged src")
		defer exec("drop snapshot paged_hi")
		start := time.Now()
		exec("data branch pick src into dst between snapshot paged_lo and paged_hi when conflict fail")
		t.Logf("8000-row full-image PICK took %s", time.Since(start))
		var total, updated int
		require.NoError(t, conn.QueryRowContext(t.Context(), "select count(*),sum(v=k+1) from dst").Scan(&total, &updated))
		require.Equal(t, 8000, total)
		require.Equal(t, total, updated)
	})
}
