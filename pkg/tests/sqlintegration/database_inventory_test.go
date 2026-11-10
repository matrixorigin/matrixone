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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// These test the shared CREATE/reservation boundary. Full native publication,
// other inventory producers and adapter recovery have separate acceptance.
func TestDatabaseInventoryCreateReservation(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		svc := cn.RawService().(cnservice.Service)
		internal := svc.GetSQLExecutor()
		owner, ok := svc.GetEngine().(interface {
			ReserveDatabaseInventory(context.Context, string, uint64, uint64, client.TxnOperator) error
		})
		require.True(t, ok)
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), catalog.System_Account), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists nr_inventory_create")
		execute := func(query string) {
			t.Helper()
			_, err := db.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		read := func(query string) uint64 {
			t.Helper()
			result, err := internal.Exec(ctx, query, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
			require.NoError(t, err, query)
			defer result.Close()
			require.Len(t, result.Batches, 1)
			require.Equal(t, 1, result.Batches[0].RowCount())
			return vector.GetFixedAtNoTypeCheck[uint64](result.Batches[0].Vecs[0], 0)
		}
		options := executor.Options{}.WithAccountID(0).WithTxnMode(pbtxn.TxnMode_Pessimistic).
			WithTxnIsolation(pbtxn.TxnIsolation_RC).WithWaitCommittedLogApplied()
		execute("create database nr_inventory_create")
		id := read("select cast(dat_id as unsigned) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_create'")
		versionQuery := fmt.Sprintf("select epoch from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
		require.EqualValues(t, 1, read(versionQuery))
		execute("create table nr_inventory_create.payload(id int primary key,v int)")
		execute("insert into nr_inventory_create.payload values(1,10)")
		fixedEpoch := read(versionQuery)
		require.Greater(t, fixedEpoch, uint64(1))

		if !t.Run("ddl commits after fixed snapshot", func(t *testing.T) {
			execute("create snapshot nr_inventory_create_fixed for database nr_inventory_create")
			defer cleanupSQLIntegration(t, cn, "drop snapshot if exists nr_inventory_create_fixed")
			execute("create table nr_inventory_create.concurrent(id int primary key,v int)")
			execute("insert into nr_inventory_create.concurrent values(1,42)")
			err := internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				return owner.ReserveDatabaseInventory(ctx, "nr_inventory_create", id, fixedEpoch, tx.Txn())
			}, options)
			require.Error(t, err, "a stale source token must not publish over committed CREATE")
			require.EqualValues(t, 42, read("select cast(v as unsigned) from nr_inventory_create.concurrent where id=1"))
			require.EqualValues(t, 1, read(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)))
		}) {
			return
		}
		if !t.Run("create and epoch rollback together", func(t *testing.T) {
			before := read(versionQuery)
			abort := errors.New("inventory DDL rollback")
			err := internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				result, err := tx.Exec("create table nr_inventory_create.rolled_back(id int primary key)", executor.StatementOption{})
				if err != nil {
					return err
				}
				result.Close()
				return abort
			}, options)
			require.ErrorIs(t, err, abort)
			require.Equal(t, before, read(versionQuery))
			require.Zero(t, read("select count(*) from mo_catalog.mo_tables where account_id=0 and reldatabase='nr_inventory_create' and relname='rolled_back'"))
		}) {
			return
		}
		t.Run("reservation rollback releases queued create", func(t *testing.T) {
			before := read(versionQuery)
			inventoryTable := read("select cast(rel_id as unsigned) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_database_inventory'")
			ready := make(chan []byte, 1)
			release := make(chan struct{})
			done := make(chan error, 1)
			workCtx, stop := context.WithCancel(ctx)
			defer stop()
			abort := errors.New("publication rollback")
			go func() {
				done <- internal.ExecTxn(workCtx, func(tx executor.TxnExecutor) error {
					if err := owner.ReserveDatabaseInventory(workCtx, "nr_inventory_create", id, before, tx.Txn()); err != nil {
						ready <- nil
						return err
					}
					ready <- bytes.Clone(tx.Txn().Txn().ID)
					select {
					case <-release:
						return abort
					case <-workCtx.Done():
						return workCtx.Err()
					}
				}, options)
			}()
			joined := false
			defer func() {
				stop()
				if !joined {
					select {
					case <-done:
					case <-time.After(30 * time.Second):
						t.Error("reservation owner did not exit")
					}
				}
			}()
			var txnID []byte
			select {
			case txnID = <-ready:
				require.NotEmpty(t, txnID)
			case <-ctx.Done():
				t.Fatal("reservation did not reach barrier")
			}
			waiting := make(chan struct{}, 1)
			restore := lockservice.SetWaiterEnqueuedHookForTest(func(table uint64, _ []byte, holders [][]byte) {
				if table != inventoryTable {
					return
				}
				for _, holder := range holders {
					if bytes.Equal(holder, txnID) {
						select {
						case waiting <- struct{}{}:
						default:
						}
					}
				}
			})
			defer restore()
			ddlDone := make(chan error, 1)
			go func() {
				_, err := db.ExecContext(workCtx, "create table nr_inventory_create.queued(id int primary key)")
				ddlDone <- err
			}()
			ddlJoined := false
			defer func() {
				stop()
				if !ddlJoined {
					select {
					case <-ddlDone:
					case <-time.After(30 * time.Second):
						t.Error("CREATE did not exit")
					}
				}
			}()
			select {
			case <-waiting:
			case err := <-ddlDone:
				ddlJoined = true
				t.Fatalf("CREATE bypassed inventory reservation: %v", err)
			case <-ctx.Done():
				t.Fatal("CREATE did not reach the inventory waiter")
			}
			close(release)
			ownerErr := <-done
			joined = true
			require.ErrorIs(t, ownerErr, abort)
			ddlErr := <-ddlDone
			ddlJoined = true
			require.NoError(t, ddlErr)
			require.Equal(t, before+1, read(versionQuery))
			require.EqualValues(t, 1, read("select count(*) from mo_catalog.mo_tables where account_id=0 and reldatabase='nr_inventory_create' and relname='queued'"))
		})
	})
}

// This composes the existing public caller-owned R1 with the epoch protocol.
// It is not the adapter or the disabled public FOR UPDATE EXPECT entry point.
func TestNativeRebaseInventoryPublication(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		service := cn.RawService().(cnservice.Service)
		inventory, ok := service.GetEngine().(interface {
			ReserveDatabaseInventory(context.Context, string, uint64, uint64, client.TxnOperator) error
		})
		require.True(t, ok)
		sessions, ok := cn.RawService().(interface {
			SessionMgr() *queryservice.SessionManager
		})
		require.True(t, ok)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		for _, scenario := range []string{"stale", "commit", "overlap"} {
			if !t.Run(scenario, func(t *testing.T) {
				prefix := "nr_inv_pub_" + scenario + "_"
				upstream, source, target, meta := prefix+"u", prefix+"f", prefix+"next", prefix+"meta"
				defer cleanupSQLIntegration(t, cn,
					"drop snapshot if exists "+prefix+"uh", "drop snapshot if exists "+prefix+"fh",
					"drop snapshot if exists "+prefix+"bf", "drop snapshot if exists "+prefix+"bu",
					"drop database if exists "+target, "drop database if exists "+source,
					"drop database if exists "+upstream, "drop database if exists "+meta)
				f := rebaseSQLFixture{t, ctx, db, prefix}
				f.exec("create database " + upstream)
				f.exec("create table " + upstream + ".t(id int primary key,v int)")
				f.exec("insert into " + upstream + ".t values(1,10)")
				bu := f.snapshot(upstream, "bu")
				f.exec("data branch create database " + source + " from " + bu)
				bf := f.snapshot(source, "bf")
				f.exec("update " + source + ".t set v=11 where id=1")
				f.exec("insert into " + upstream + ".t values(2,20)")
				f.exec("create database " + meta)
				f.exec("create table " + meta + ".registry(name varchar(64) primary key,db varchar(64),gen int)")
				f.exec("insert into " + meta + ".registry values('feature','" + source + "',0)")
				id := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='" + source + "'")
				version := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
				fixed := f.scalar(version)
				fh, uh := f.snapshot(source, "fh"), f.snapshot(upstream, "uh")
				if scenario == "stale" {
					f.exec("create table " + source + ".concurrent(id int primary key,v int)")
					f.exec("insert into " + source + ".concurrent values(1,42)")
				}
				owner, err := db.Conn(ctx)
				require.NoError(t, err)
				defer owner.Close()
				defer func() {
					cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					_, _ = owner.ExecContext(cleanupCtx, "rollback")
				}()
				var connectionID uint32
				require.NoError(t, owner.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
				var session *frontend.Session
				for _, candidate := range sessions.SessionMgr().GetAllSessions() {
					if candidate, ok := candidate.(*frontend.Session); ok && candidate.GetConnectionID() == connectionID {
						session = candidate
						break
					}
				}
				require.NotNil(t, session, "bind the exact authenticated client session, not a different transaction")
				if scenario == "stale" {
					_, err := owner.ExecContext(ctx, "update mo_catalog.mo_database_inventory set epoch=epoch where account_id=0")
					require.ErrorContains(t, err, "do not have privilege", "the test must not grant public catalog writes")
				}
				_, err = owner.ExecContext(ctx, "set session transaction isolation level read committed")
				require.NoError(t, err)
				_, err = owner.ExecContext(ctx, "begin")
				require.NoError(t, err)
				require.Eventually(t, func() bool { return !session.GetQueryInProgress() }, 5*time.Second, 10*time.Millisecond)
				op := session.GetTxnHandler().GetTxn()
				require.NotNil(t, op)
				opID := bytes.Clone(op.Txn().ID)
				// The frontend owns this idle transaction. Invoke the same typed
				// internal boundary used by the future EXPECT entry; public SQL
				// cannot and must not write the protected inventory table.
				reserveCtx := defines.AttachAccountId(ctx, 0)
				err = inventory.ReserveDatabaseInventory(reserveCtx, source, uint64(id), uint64(fixed), op)
				require.Equal(t, opID, session.GetTxnHandler().GetTxn().Txn().ID)
				if scenario == "stale" {
					require.Error(t, err, "committed CREATE must prevent publication before replay")
					_, err = owner.ExecContext(ctx, "rollback")
					require.NoError(t, err)
					require.EqualValues(t, 42, f.scalar("select v from "+source+".concurrent where id=1"))
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+target+"'"))
					require.Zero(t, f.scalar("select gen from "+meta+".registry where name='feature'"))
					return
				}
				require.NoError(t, err)

				var ddlDone chan error
				joined := false
				workCtx, stop := context.WithCancel(ctx)
				defer stop()
				if scenario == "overlap" {
					inventoryTable := uint64(f.scalar("select cast(rel_id as signed) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_database_inventory'"))
					waiting := make(chan struct{}, 1)
					restore := lockservice.SetWaiterEnqueuedHookForTest(func(table uint64, _ []byte, _ [][]byte) {
						if table == inventoryTable {
							select {
							case waiting <- struct{}{}:
							default:
							}
						}
					})
					defer restore()
					ddlDone = make(chan error, 1)
					go func() {
						_, err := db.ExecContext(workCtx, "create table "+source+".concurrent(id int primary key,v int)")
						if err == nil {
							_, err = db.ExecContext(workCtx, "insert into "+source+".concurrent values(1,42)")
						}
						ddlDone <- err
					}()
					defer func() {
						if !joined {
							stop()
							cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 10*time.Second)
							defer cancelCleanup()
							_, _ = owner.ExecContext(cleanupCtx, "rollback")
							select {
							case <-ddlDone:
							case <-time.After(10 * time.Second):
								t.Error("competing CREATE did not terminate")
							}
						}
					}()
					select {
					case <-waiting:
					case err := <-ddlDone:
						joined = true
						t.Fatalf("CREATE bypassed the held publication epoch: %v", err)
					case <-ctx.Done():
						t.Fatal("CREATE did not reach the inventory barrier")
					}
				}

				publishErr := func() error {
					query := fmt.Sprintf("data branch rebase database %s from %s against %s between %s and %s", target, fh, uh, bf, bu)
					rows, err := owner.QueryContext(ctx, query)
					if rows != nil {
						defer rows.Close()
					}
					if err != nil {
						return err
					}
					if !rows.Next() {
						if err := rows.Err(); err != nil {
							return err
						}
						return errors.New("Rebase returned no provisional summary")
					}
					if err := rows.Close(); err != nil {
						return err
					}
					result, err := owner.ExecContext(ctx, "update "+meta+".registry set db='"+target+"',gen=1 where name='feature' and db='"+source+"' and gen=0")
					if err != nil {
						return err
					}
					if n, err := result.RowsAffected(); err != nil {
						return err
					} else if n != 1 {
						return errors.New("registry CAS failed")
					}
					if _, err = owner.ExecContext(ctx, "drop database "+source); err != nil {
						return err
					}
					_, err = owner.ExecContext(ctx, "commit")
					return err
				}()
				if publishErr != nil {
					_, err := owner.ExecContext(ctx, "rollback")
					require.NoError(t, err)
				}
				var ddlErr error
				if ddlDone != nil {
					select {
					case ddlErr = <-ddlDone:
						joined = true
					case <-ctx.Done():
						t.Fatal("competing CREATE did not finish after publication")
					}
				}
				if scenario == "commit" {
					require.NoError(t, publishErr)
				} else {
					require.False(t, publishErr == nil && ddlErr == nil, "publication and stale-source CREATE must not both succeed")
				}
				if publishErr == nil {
					require.Equal(t, []string{"1:11", "2:20"}, f.rows(target, "t"))
					require.EqualValues(t, 1, f.scalar("select gen from "+meta+".registry where name='feature' and db='"+target+"'"))
					require.Zero(t, f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)))
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+source+"'"))
				} else {
					require.NoError(t, ddlErr, "the competing operation must make progress when publication aborts")
					require.Equal(t, []string{"1:42"}, f.rows(source, "concurrent"))
					require.Equal(t, []string{"1:11"}, f.rows(source, "t"))
					require.Zero(t, f.scalar("select gen from "+meta+".registry where name='feature' and db='"+source+"'"))
					require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='"+target+"'"))
				}
				require.Equal(t, []string{"1:10", "2:20"}, f.rows(upstream, "t"))
			}) {
				return
			}
		}
	})
}
