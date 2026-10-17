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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestStatementAuthorizationEpochAcrossCatalogCommit(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		open := func(index int, user string) *sql.Conn {
			cn, err := c.GetCNService(index)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/?multiStatements=true", user, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			return conn
		}
		root := open(0, "dump")
		mustExec(t, ctx, root, "create account epoch_auth admin_name 'admin' identified by '111'")
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			mustExec(t, cleanup, root, "drop account epoch_auth")
		}()
		owner := open(0, "epoch_auth#admin#accountadmin")
		for _, q := range []string{"create database app", "create table app.marker(id int)", "create table app.t(id int primary key, tenant int)", "insert into app.t values (1,1),(2,2)", "create role reader", "create user learner identified by '111' default role reader", "grant reader to learner", "grant connect on account * to reader"} {
			mustExec(t, ctx, owner, q)
		}
		syncCatalog := func() { mustExec(t, ctx, root, "select mo_ctl('cn','synccommit','')") }
		syncCatalog()
		reader := open(1, "epoch_auth#learner#reader")
		if fault.EnableDomain(fault.DomainFrontend) {
			defer fault.DisableDomain(fault.DomainFrontend)
		}
		const barrier = "authorization_before_grant_read"
		const probe = "authorization-before-grant-waiters"
		require.NoError(t, fault.AddFaultPointInDomain(ctx, fault.DomainFrontend, barrier, "1:1::", "wait", 0, "", false))
		defer fault.RemoveFaultPointFromDomain(context.Background(), fault.DomainFrontend, barrier)
		require.NoError(t, fault.AddFaultPointInDomain(ctx, fault.DomainFrontend, probe, ":::", "getwaiters", 0, barrier, false))
		defer fault.RemoveFaultPointFromDomain(context.Background(), fault.DomainFrontend, probe)
		done := make(chan error, 1)
		go func() {
			rows, err := reader.QueryContext(ctx, "select id from app.t order by id")
			if rows != nil {
				defer rows.Close()
				for rows.Next() {
				}
				err2 := rows.Err()
				if err == nil {
					err = err2
				}
			}
			done <- err
		}()
		require.Eventually(t, func() bool { n, _, ok := fault.TriggerFaultInDomain(fault.DomainFrontend, probe); return ok && n == 1 }, 20*time.Second, time.Millisecond, "statement did not reach the grant-read boundary")
		// Both commits occur after A's empty rules and before its grant traversal.
		// Reading grants at the later B would incorrectly allow both unfiltered rows.
		mustExec(t, ctx, owner, "alter role reader add rule 'select id, tenant from app.t where tenant=1' on table app.t")
		mustExec(t, ctx, owner, "grant select on table app.t to reader")
		syncCatalog()
		_, err := fault.RemoveFaultPointFromDomain(ctx, fault.DomainFrontend, barrier)
		require.NoError(t, err)
		select {
		case err := <-done:
			require.ErrorContains(t, err, "privilege")
		case <-ctx.Done():
			t.Fatal("grant read did not finish")
		}
		var id int
		require.NoError(t, reader.QueryRowContext(ctx, "select id from app.t").Scan(&id))
		require.Equal(t, 1, id, "next epoch must include B's rule and grant")
		// One packet changes principal between statements; statement two must not
		// execute under statement one's rewritten policy or membership proof.
		rows, err := reader.QueryContext(ctx, "set role public; select id from app.t")
		if rows != nil {
			defer rows.Close()
			for rows.NextResultSet() {
			}
			if err == nil {
				err = rows.Err()
			}
		}
		require.ErrorContains(t, err, "privilege")
		// Ordinary batch validation remains eager: the valid first write cannot run.
		_, err = owner.ExecContext(ctx, "insert into app.t values (3,3); select '")
		require.Error(t, err)
		var count int
		require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from app.t").Scan(&count))
		require.Equal(t, 2, count)
		// Replacement BEGIN commits the preceding writer before acquiring its new
		// epoch, while the replacement transaction itself remains rollbackable.
		mustExec(t, ctx, owner, "begin; insert into app.marker values (1); begin; insert into app.marker values (2); rollback")
		var marker int
		require.NoError(t, owner.QueryRowContext(ctx, "select count(*), min(id) from app.marker").Scan(&count, &marker))
		require.Equal(t, 1, count, "replacement BEGIN's write must roll back")
		require.Equal(t, 1, marker, "the preceding transaction must commit")
		// An implicit commit advances the real session minimum. Re-establish the
		// whole authorization/rewrite owner before creating the DDL's transaction.
		mustExec(t, ctx, owner, "begin")
		mustExec(t, ctx, owner, "insert into app.t values (3,3)")
		// This first batch statement replaces the epoch before its sibling base
		// is needed. The continuation must still descend from the live request.
		mustExec(t, ctx, owner, "truncate table app.marker; insert into app.marker values (9)")
		syncCatalog()
		observer := open(1, "epoch_auth#admin#accountadmin")
		require.NoError(t, observer.QueryRowContext(ctx, "select count(*) from app.t").Scan(&count))
		require.Equal(t, 3, count)
		require.NoError(t, observer.QueryRowContext(ctx, "select count(*), min(id) from app.marker").Scan(&count, &marker))
		require.Equal(t, 1, count)
		require.Equal(t, 9, marker)
		// CREATE TABLE is transactional in this repository; the implicit-commit
		// TRUNCATE/RENAME forms are excluded from prepareable_stmt. Retained
		// binary DDL must preserve the existing user transaction's data view.
		stmt, err := owner.PrepareContext(ctx, "create table app.binary_commit(id int)")
		require.NoError(t, err)
		defer stmt.Close()
		mustExec(t, ctx, owner, "begin")
		mustExec(t, ctx, owner, "insert into app.t values (4,4)")
		_, err = stmt.ExecContext(ctx)
		require.NoError(t, err)
		require.NoError(t, observer.QueryRowContext(ctx, "select count(*) from app.t").Scan(&count))
		require.Equal(t, 3, count)
		mustExec(t, ctx, owner, "commit")
		syncCatalog()
		require.NoError(t, observer.QueryRowContext(ctx, "select count(*) from app.t").Scan(&count))
		require.Equal(t, 4, count)
	})
}
