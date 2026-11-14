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
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

func TestNativeRebaseCommitFailure(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_commitfail_bu", "drop snapshot if exists nr_commitfail_bf",
			"drop snapshot if exists nr_commitfail_fh", "drop snapshot if exists nr_commitfail_uh",
			"drop database if exists nr_commitfail_result", "drop database if exists nr_commitfail_f",
			"drop database if exists nr_commitfail_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_commitfail_"}
		f.exec("create database nr_commitfail_u")
		for _, table := range []string{"a", "b"} {
			f.exec("create table nr_commitfail_u." + table + "(id int primary key,v int)")
			f.exec("insert into nr_commitfail_u." + table + " values(1,10)")
		}
		bu := f.snapshot("nr_commitfail_u", "bu")
		f.exec("data branch create database nr_commitfail_f from " + bu)
		bf := f.snapshot("nr_commitfail_f", "bf")
		f.exec("update nr_commitfail_f.a set v=11 where id=1")
		f.exec("update nr_commitfail_f.b set v=12 where id=1")
		fh, uh := f.snapshot("nr_commitfail_f", "fh"), f.snapshot("nr_commitfail_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'")
		edges := f.scalar("select count(*) from mo_catalog.mo_branch_metadata")
		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		fired, release := make(chan struct{}), make(chan struct{})
		var releaseOnce, restoreOnce sync.Once
		restore := frontend.SetRebaseTableAppliedHookForTest(func(ctx context.Context, database, table string) {
			if database != "nr_commitfail_result" || table != "b" {
				return
			}
			close(fired)
			select {
			case <-release:
			case <-ctx.Done():
			}
		})
		done := make(chan error, 1)
		joined := false
		defer func() {
			releaseOnce.Do(func() { close(release) })
			if !joined {
				select {
				case <-done:
				case <-time.After(time.Minute):
					t.Error("Rebase owner did not terminate")
				}
			}
			restoreOnce.Do(restore)
		}()
		wasEnabled := fault.Status()
		fault.Enable()
		armed := false
		defer func() {
			if armed {
				_, removeErr := fault.RemoveFaultPoint(context.Background(), objectio.FJ_CNCommitAfterWorkspaceDumpFailed)
				require.NoError(t, removeErr)
			}
			if !wasEnabled {
				fault.Disable()
			}
		}()
		query := fmt.Sprintf("data branch rebase database nr_commitfail_result from %s against %s between %s and %s", fh, uh, bf, bu)
		go func() {
			done <- drainRebaseQuery(ctx, owner, query)
		}()
		select {
		case <-fired:
		case err = <-done:
			joined = true
			t.Fatalf("Rebase exited before the commit barrier: %v", err)
		case <-ctx.Done():
			t.Fatal("Rebase did not reach the commit barrier")
		}
		require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_CNCommitAfterWorkspaceDumpFailed,
			":::", "echo", 0, "native rebase injected commit failure", false))
		armed = true
		releaseOnce.Do(func() { close(release) })
		select {
		case err = <-done:
			joined = true
			require.ErrorContains(t, err, "native rebase injected commit failure")
		case <-ctx.Done():
			t.Fatal("Rebase commit did not terminate")
		}
		_, err = fault.RemoveFaultPoint(ctx, objectio.FJ_CNCommitAfterWorkspaceDumpFailed)
		require.NoError(t, err)
		armed = false
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_commitfail_result'"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'"))
		require.Equal(t, edges, f.scalar("select count(*) from mo_catalog.mo_branch_metadata"))
		require.Equal(t, int64(11), f.scalar("select v from nr_commitfail_f.a where id=1"))
		require.Equal(t, int64(10), f.scalar("select v from nr_commitfail_u.a where id=1"))
		restoreOnce.Do(restore)
		_, _, err = f.rebaseTables("nr_commitfail_result", fh, uh, bf, bu, "", 2)
		require.NoError(t, err, "commit failure must release the owned lifecycle locks")
		require.Equal(t, int64(11), f.scalar("select v from nr_commitfail_result.a where id=1"))
		require.Equal(t, int64(12), f.scalar("select v from nr_commitfail_result.b where id=1"))
	})
}

// A fixed R1 snapshot does not by itself fence writes to the physical source.
// R2 publication must account for a write committed after that snapshot.
func TestNativeRebaseSourceWriteAfterSnapshot(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_write_bu", "drop snapshot if exists nr_write_bf",
			"drop snapshot if exists nr_write_fh", "drop snapshot if exists nr_write_uh",
			"drop database if exists nr_write_result", "drop database if exists nr_write_f",
			"drop database if exists nr_write_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_write_"}
		f.exec("create database nr_write_u")
		f.exec("create table nr_write_u.t(id int primary key,v int)")
		f.exec("insert into nr_write_u.t values(1,10)")
		bu := f.snapshot("nr_write_u", "bu")
		f.exec("data branch create database nr_write_f from " + bu)
		bf := f.snapshot("nr_write_f", "bf")
		f.exec("update nr_write_f.t set v=11 where id=1")
		fh, uh := f.snapshot("nr_write_f", "fh"), f.snapshot("nr_write_u", "uh")
		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		fired, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		restore := frontend.SetRebaseTableAppliedHookForTest(func(ctx context.Context, database, table string) {
			if database != "nr_write_result" || table != "t" {
				return
			}
			close(fired)
			select {
			case <-release:
			case <-ctx.Done():
			}
		})
		done := make(chan error, 1)
		joined := false
		defer func() {
			releaseOnce.Do(func() { close(release) })
			if !joined {
				select {
				case <-done:
				case <-time.After(time.Minute):
					t.Error("Rebase owner did not terminate")
				}
			}
			restore()
		}()
		query := fmt.Sprintf("data branch rebase database nr_write_result from %s against %s between %s and %s", fh, uh, bf, bu)
		go func() {
			done <- drainRebaseQuery(ctx, owner, query)
		}()
		select {
		case <-fired:
		case err = <-done:
			joined = true
			t.Fatalf("Rebase exited before its apply barrier: %v", err)
		case <-ctx.Done():
			t.Fatal("Rebase did not reach its apply barrier")
		}
		writeCtx, writeCancel := context.WithTimeout(ctx, 10*time.Second)
		_, err = db.ExecContext(writeCtx, "update nr_write_f.t set v=12 where id=1")
		writeCancel()
		require.NoError(t, err)
		releaseOnce.Do(func() { close(release) })
		select {
		case err = <-done:
			joined = true
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("Rebase did not terminate after the barrier")
		}
		require.Equal(t, int64(12), f.scalar("select v from nr_write_f.t where id=1"))
		require.Equal(t, int64(11), f.scalar("select v from nr_write_result.t where id=1"))
	})
}

// Public caller-owned R1 shares the transaction but does not itself provide
// an adapter publication protocol or fence writes to the live feature.
func TestNativeRebaseBorrowedOwner(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_borrow_bu", "drop snapshot if exists nr_borrow_bf",
			"drop snapshot if exists nr_borrow_fh", "drop snapshot if exists nr_borrow_uh",
			"drop database if exists nr_borrow_rollback", "drop database if exists nr_borrow_failure",
			"drop database if exists nr_borrow_commit", "drop database if exists nr_borrow_meta",
			"drop database if exists nr_borrow_f", "drop database if exists nr_borrow_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_borrow_"}
		f.exec("create database nr_borrow_u")
		for _, table := range []string{"a", "b"} {
			f.exec("create table nr_borrow_u." + table + "(id int primary key,v int)")
			f.exec("insert into nr_borrow_u." + table + " values(1,10)")
		}
		f.exec("create database nr_borrow_meta")
		f.exec("create table nr_borrow_meta.registry(id int primary key,gen int)")
		f.exec("insert into nr_borrow_meta.registry values(1,0)")
		bu := f.snapshot("nr_borrow_u", "bu")
		f.exec("data branch create database nr_borrow_f from " + bu)
		bf := f.snapshot("nr_borrow_f", "bf")
		f.exec("update nr_borrow_f.a set v=11 where id=1")
		f.exec("update nr_borrow_f.b set v=11 where id=1")
		f.exec("update nr_borrow_u.b set v=12 where id=1")
		fh, uh := f.snapshot("nr_borrow_f", "fh"), f.snapshot("nr_borrow_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'")
		edges := f.scalar("select count(*) from mo_catalog.mo_branch_metadata")
		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		defer func() { _, _ = owner.ExecContext(context.Background(), "rollback") }()
		_, err = owner.ExecContext(ctx, "set session transaction isolation level read committed")
		require.NoError(t, err)
		exec := func(query string) {
			t.Helper()
			_, err := owner.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		rebase := func(target, policy string) error {
			t.Helper()
			query := fmt.Sprintf("data branch rebase database %s from %s against %s between %s and %s %s", target, fh, uh, bf, bu, policy)
			return drainRebaseQuery(ctx, owner, query)
		}

		exec("begin")
		require.NoError(t, rebase("nr_borrow_rollback", "when conflict accept"))
		exec("update nr_borrow_meta.registry set gen=1 where id=1")
		exec("rollback")
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_borrow_rollback'"))
		require.Equal(t, int64(0), f.scalar("select gen from nr_borrow_meta.registry where id=1"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'"))
		require.Equal(t, edges, f.scalar("select count(*) from mo_catalog.mo_branch_metadata"))

		exec("begin")
		exec("update nr_borrow_meta.registry set gen=9 where id=1")
		require.ErrorContains(t, rebase("nr_borrow_failure", "when conflict fail"), "Rebase conflict")
		// A caller may try COMMIT after a statement error; neither a successful
		// commit nor an aborted transaction may publish a partial destination.
		if _, err := owner.ExecContext(ctx, "commit"); err != nil {
			_, _ = owner.ExecContext(ctx, "rollback")
		}
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_borrow_failure'"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'"))
		require.Equal(t, edges, f.scalar("select count(*) from mo_catalog.mo_branch_metadata"))

		exec("begin")
		exec("update nr_borrow_meta.registry set gen=1 where id=1")
		require.NoError(t, rebase("nr_borrow_commit", "when conflict accept"))
		drop, err := db.Conn(ctx)
		require.NoError(t, err)
		defer drop.Close()
		var dropID uint32
		require.NoError(t, drop.QueryRowContext(ctx, "select connection_id()").Scan(&dropID))
		dropDone := make(chan error, 1)
		dropJoined := false
		go func() { _, err := drop.ExecContext(ctx, "drop database nr_borrow_f"); dropDone <- err }()
		defer func() {
			if !dropJoined {
				cleanupCtx, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				_, _ = owner.ExecContext(cleanupCtx, "rollback")
				_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("kill query %d", dropID))
				select {
				case <-dropDone:
				case <-cleanupCtx.Done():
					t.Error("source DROP did not terminate")
				}
			}
		}()
		packer := types.NewPacker()
		packer.EncodeUint32(0)
		packer.EncodeStringType([]byte("nr_borrow_f"))
		catalogKey := append([]byte(nil), packer.Bytes()...)
		packer.Reset()
		packer.EncodeStringType(catalogKey)
		key := hex.EncodeToString(packer.Bytes())
		packer.Close()
		waiter := "select count(*) from mo_catalog.mo_locks where txn_id<>'' and lock_wait<>'' and lock_content='" + key + "' and table_id in (select cast(rel_id as char) from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_database')"
		deadline := time.Now().Add(30 * time.Second)
		for f.scalar(waiter) == 0 {
			select {
			case err := <-dropDone:
				dropJoined = true
				t.Fatalf("source DROP completed before caller commit: %v", err)
			default:
			}
			require.True(t, time.Now().Before(deadline), "source DROP must wait after Rebase returns")
			select {
			case <-ctx.Done():
				t.Fatal("source DROP waiter observation expired")
			case <-time.After(20 * time.Millisecond):
			}
		}
		exec("commit")
		select {
		case err := <-dropDone:
			dropJoined = true
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("source DROP did not finish after caller commit")
		}
		require.Equal(t, int64(1), f.scalar("select gen from nr_borrow_meta.registry where id=1"))
		require.Equal(t, int64(11), f.scalar("select v from nr_borrow_commit.a where id=1"))
		require.Equal(t, int64(11), f.scalar("select v from nr_borrow_commit.b where id=1"))
	})
}

// A caller may opt into MySQL-compatible statement-error handling and then
// COMMIT. A late conflict must never publish the partially replayed database.
func TestNativeRebaseBorrowedOwnerRollbackDisabled(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn,
			"drop snapshot if exists nr_errmode_bu", "drop snapshot if exists nr_errmode_bf",
			"drop snapshot if exists nr_errmode_fh", "drop snapshot if exists nr_errmode_uh",
			"drop database if exists nr_errmode_result", "drop database if exists nr_errmode_meta",
			"drop database if exists nr_errmode_f", "drop database if exists nr_errmode_u")
		f := rebaseSQLFixture{t, ctx, db, "nr_errmode_"}
		f.exec("create database nr_errmode_u")
		for _, table := range []string{"a", "b"} {
			f.exec("create table nr_errmode_u." + table + "(id int primary key,v int)")
			f.exec("insert into nr_errmode_u." + table + " values(1,10)")
		}
		f.exec("create database nr_errmode_meta")
		f.exec("create table nr_errmode_meta.registry(id int primary key,gen int)")
		f.exec("insert into nr_errmode_meta.registry values(1,0)")
		bu := f.snapshot("nr_errmode_u", "bu")
		f.exec("data branch create database nr_errmode_f from " + bu)
		bf := f.snapshot("nr_errmode_f", "bf")
		f.exec("update nr_errmode_f.a set v=11 where id=1")
		f.exec("update nr_errmode_f.b set v=11 where id=1")
		f.exec("update nr_errmode_u.b set v=12 where id=1")
		fh, uh := f.snapshot("nr_errmode_f", "fh"), f.snapshot("nr_errmode_u", "uh")
		pins := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'")
		edges := f.scalar("select count(*) from mo_catalog.mo_branch_metadata")
		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		defer func() { _, _ = owner.ExecContext(context.Background(), "rollback") }()
		for _, query := range []string{
			"set session transaction isolation level read committed",
			"set mo_rollback_txn_on_error=0",
			"begin",
			"update nr_errmode_meta.registry set gen=9 where id=1",
		} {
			_, err = owner.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		applied := make(chan struct{}, 1)
		restoreApplied := frontend.SetRebaseTableAppliedHookForTest(func(_ context.Context, database, table string) {
			if database == "nr_errmode_result" && table == "a" {
				select {
				case applied <- struct{}{}:
				default:
				}
			}
		})
		defer restoreApplied()
		query := fmt.Sprintf("data branch rebase database nr_errmode_result from %s against %s between %s and %s when conflict fail", fh, uh, bf, bu)
		_, err, closeErr := readRebaseQuery(ctx, owner, query)
		require.NoError(t, closeErr)
		require.ErrorContains(t, err, "Rebase conflict")
		select {
		case <-applied:
		default:
			t.Fatal("first table was not replayed before the later conflict")
		}
		_, commitErr := owner.ExecContext(ctx, "commit")
		if commitErr != nil {
			_, _ = owner.ExecContext(ctx, "rollback")
		}
		gen := f.scalar("select gen from nr_errmode_meta.registry where id=1")
		t.Logf("caller commit returned error=%t; prior registry generation=%d", commitErr != nil, gen)
		require.Contains(t, []int64{0, 9}, gen)
		require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_errmode_result'"))
		require.Equal(t, pins, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch'"))
		require.Equal(t, edges, f.scalar("select count(*) from mo_catalog.mo_branch_metadata"))
	})
}

func TestNativeRebasePartialApplySourceDrop(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		for _, scenario := range []struct {
			name         string
			cancel       bool
			callerTxn    bool
			dropSnapshot bool
			addRoleRule  bool
		}{
			{name: "source_drop"},
			{name: "cancel", cancel: true},
			{name: "caller_cancel", cancel: true, callerTxn: true},
			{name: "snapshot_drop", dropSnapshot: true},
			{name: "role_rule", addRoleRule: true},
		} {
			t.Run(scenario.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
				defer cancel()
				db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
				require.NoError(t, err)
				defer db.Close()
				f := rebaseSQLFixture{t, ctx, db, "nr_lifecycle_"}
				const upstream, feature, target = "nr_lifecycle_u", "nr_lifecycle_f", "nr_lifecycle_r"
				defer cleanupSQLIntegration(t, cn,
					"drop role if exists nr_lifecycle_rule_role",
					"drop snapshot if exists nr_lifecycle_bu", "drop snapshot if exists nr_lifecycle_bf",
					"drop snapshot if exists nr_lifecycle_fh", "drop snapshot if exists nr_lifecycle_uh",
					"drop database if exists "+target, "drop database if exists "+feature, "drop database if exists "+upstream)
				f.exec("create database " + upstream)
				if scenario.addRoleRule {
					f.exec("create role nr_lifecycle_rule_role")
				}
				for _, table := range []string{"a", "b"} {
					f.exec("create table " + upstream + "." + table + "(id int primary key,v int)")
					f.exec("insert into " + upstream + "." + table + " values(1,10),(2,20)")
				}
				bu := f.snapshot(upstream, "bu")
				f.exec("data branch create database " + feature + " from " + bu)
				bf := f.snapshot(feature, "bf")
				f.exec("update " + feature + ".a set v=11 where id=1")
				f.exec("update " + feature + ".b set v=12 where id=1")
				fh, uh := f.snapshot(feature, "fh"), f.snapshot(upstream, "uh")
				pinsBefore := f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='" + upstream + "'")
				edgesQuery := "select count(*) from mo_catalog.mo_branch_metadata where p_table_id in (select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='" + upstream + "')"
				edgesBefore := f.scalar(edgesQuery)
				owner, err := db.Conn(ctx)
				require.NoError(t, err)
				defer owner.Close()
				drop, err := db.Conn(ctx)
				require.NoError(t, err)
				defer drop.Close()
				var ownerID, dropID uint32
				require.NoError(t, owner.QueryRowContext(ctx, "select connection_id()").Scan(&ownerID))
				require.NoError(t, drop.QueryRowContext(ctx, "select connection_id()").Scan(&dropID))
				if scenario.callerTxn {
					_, err = owner.ExecContext(ctx, "set session transaction isolation level read committed")
					require.NoError(t, err)
					_, err = owner.ExecContext(ctx, "begin")
					require.NoError(t, err)
					defer func() { _, _ = owner.ExecContext(context.Background(), "rollback") }()
				}
				fired, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				restore := frontend.SetRebaseTableAppliedHookForTest(func(ctx context.Context, database, table string) {
					if database != target || table != "a" {
						return
					}
					close(fired)
					select {
					case <-release:
					case <-ctx.Done():
					}
				})
				ownerDone, dropDone := make(chan error, 1), make(chan error, 1)
				ownerJoined, dropJoined, dropStarted := false, false, false
				defer func() {
					cleanupCtx, stop := context.WithTimeout(context.Background(), time.Minute)
					defer stop()
					for _, id := range []uint32{ownerID, dropID} {
						_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("kill query %d", id))
					}
					releaseOnce.Do(func() { close(release) })
					for _, pending := range []struct {
						done <-chan error
						wait bool
					}{{ownerDone, !ownerJoined}, {dropDone, dropStarted && !dropJoined}} {
						if pending.wait {
							select {
							case <-pending.done:
							case <-cleanupCtx.Done():
								t.Error("lifecycle query did not terminate")
							}
						}
					}
					restore()
				}()
				query := fmt.Sprintf("data branch rebase database %s from %s against %s between %s and %s", target, fh, uh, bf, bu)
				go func() {
					ownerDone <- drainRebaseQuery(ctx, owner, query)
				}()
				select {
				case <-fired:
				case err := <-ownerDone:
					ownerJoined = true
					t.Fatalf("Rebase exited before the first-table barrier: %v", err)
				case <-ctx.Done():
					t.Fatal("first-table replay did not reach the deterministic barrier")
				}
				if scenario.cancel {
					f.exec(fmt.Sprintf("kill query %d", ownerID))
				} else {
					dropStarted = true
					dropSQL := "drop database " + feature
					if scenario.dropSnapshot {
						dropSQL = "drop snapshot nr_lifecycle_fh"
					} else if scenario.addRoleRule {
						dropSQL = "alter role nr_lifecycle_rule_role add rule \"select * from nr_lifecycle_f.a where id=1\" on table nr_lifecycle_f.a"
					}
					go func() { _, err := drop.ExecContext(ctx, dropSQL); dropDone <- err }()
					packer := types.NewPacker()
					packer.EncodeUint32(0)
					packer.EncodeStringType([]byte(feature))
					catalogKey := append([]byte(nil), packer.Bytes()...)
					packer.Reset()
					packer.EncodeStringType(catalogKey)
					key := hex.EncodeToString(packer.Bytes())
					packer.Close()
					waiter := "select count(*) from mo_catalog.mo_locks where txn_id<>'' and lock_wait<>'' and lock_content='" + key + "' and table_id in (select cast(rel_id as char) from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_database')"
					if scenario.dropSnapshot {
						// The snapshot DROP first takes the shared lifecycle gate.
						waiter = "select count(*) from mo_catalog.mo_locks where txn_id<>'' and lock_wait<>'' and table_id in (select cast(rel_id as char) from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_feature_registry')"
					} else if scenario.addRoleRule {
						waiter = "select count(*) from mo_catalog.mo_locks where txn_id<>'' and lock_wait<>'' and table_id in (select cast(rel_id as char) from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_role_rule')"
					}
					deadline := time.Now().Add(30 * time.Second)
					for f.scalar(waiter) == 0 {
						require.True(t, time.Now().Before(deadline), "DROP must queue on the protected lifecycle key")
						select {
						case <-ctx.Done():
							t.Fatal("source DROP waiter observation expired")
						case <-time.After(20 * time.Millisecond):
						}
					}
				}
				releaseOnce.Do(func() { close(release) })
				select {
				case err := <-ownerDone:
					ownerJoined = true
					if scenario.cancel {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				case <-ctx.Done():
					t.Fatal("Rebase did not terminate")
				}
				if dropStarted {
					select {
					case err := <-dropDone:
						dropJoined = true
						require.NoError(t, err)
					case <-ctx.Done():
						t.Fatal("DROP did not terminate")
					}
				}
				if scenario.cancel {
					if scenario.callerTxn {
						if _, err := owner.ExecContext(ctx, "commit"); err != nil {
							_, _ = owner.ExecContext(ctx, "rollback")
						}
					}
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+target+"'"))
					require.Equal(t, pinsBefore, f.scalar("select count(*) from mo_catalog.mo_snapshots where kind='branch' and database_name='"+upstream+"'"))
					require.Equal(t, edgesBefore, f.scalar(edgesQuery))
					f.exec("drop database " + feature) // Confirms the cancelled owner released D.
				} else {
					for _, table := range []string{"a", "b"} {
						require.Equal(t, int64(map[string]int{"a": 11, "b": 12}[table]), f.scalar("select v from "+target+"."+table+" where id=1"))
						pin := fmt.Sprintf("select count(*) from mo_catalog.mo_branch_metadata m join mo_catalog.mo_tables t on m.table_id=t.rel_id join mo_catalog.mo_snapshots s on s.sname=concat('__mo_branch_',cast(m.table_id as char)) where t.reldatabase='%s' and t.relname='%s' and s.kind='branch' and s.obj_id=m.p_table_id and s.ts=m.clone_ts", target, table)
						require.Equal(t, int64(1), f.scalar(pin), "committed data must retain its new base")
					}
					if scenario.dropSnapshot {
						require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_snapshots where sname='nr_lifecycle_fh'"))
					} else if scenario.addRoleRule {
						require.Equal(t, int64(1), f.scalar("select count(*) from mo_catalog.mo_role_rule where rule_name='nr_lifecycle_f.a'"))
					}
				}
			})
		}
	})
}
