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

func TestBoundedPickWithMissingForkEvidence(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := recoverySQLClient(t, cn)
		defer db.Close()
		conn, err := db.Conn(t.Context())
		require.NoError(t, err)
		defer conn.Close()
		execSQL := func(sql string) {
			t.Helper()
			_, err := conn.ExecContext(t.Context(), sql)
			require.NoError(t, err, sql)
		}
		checkDestination := func(want string) {
			t.Helper()
			var rows string
			require.NoError(t, conn.QueryRowContext(t.Context(),
				"select group_concat(concat_ws(':',k,v) order by k) from dst").Scan(&rows))
			require.Equal(t, want, rows)
		}
		for _, sql := range []string{
			"create database pick_fork_evidence",
			"use pick_fork_evidence",
			"begin",
			"create table root(k int primary key, v int)",
			"insert into root values(1,10),(2,20)",
			"data branch create table src from root",
			"data branch create table dst from root",
			"delete from dst where k=2",
			"commit",
			"create snapshot fork_evidence_lo for account sys",
			"update src set v=52 where k=2",
			"insert into src values(3,80)",
			"create snapshot fork_evidence_hi for account sys",
		} {
			execSQL(sql)
		}
		_, err = conn.ExecContext(t.Context(), "select key_bytes from mo_catalog.mo_branch_fork_keys")
		var sqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &sqlErr)
		require.EqualValues(t, 1146, sqlErr.Number)

		// Simulate a branch created by a binary that did not persist fork keys.
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		ctx = context.WithValue(defines.AttachAccountId(ctx, 0), defines.BranchForkKeyAccess{}, true)
		result, err := cn.RawService().(cnservice.Service).GetSQLExecutor().Exec(ctx,
			"delete from mo_catalog.mo_branch_fork_keys where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='pick_fork_evidence' and relname='dst')",
			executor.Options{})
		require.NoError(t, err)
		result.Close()
		require.EqualValues(t, 3, result.AffectedRows)

		const pick = "data branch pick src into dst between snapshot fork_evidence_lo and fork_evidence_hi keys(2,3) when conflict "
		for _, policy := range []string{"fail", "skip"} {
			_, err := conn.ExecContext(t.Context(), pick+policy)
			require.ErrorContains(t, err, "cannot distinguish an inherited deletion from an absent key without fork history")
			checkDestination("1:10")
		}
		execSQL(pick + "accept")
		checkDestination("1:10,2:52,3:80")
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
