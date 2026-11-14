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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// This tests real storage rows, not catalog producer coverage or R2 safety.
func TestRebaseInventoryPrimitive(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		value, ok := moruntime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		require.True(t, ok)
		internal := value.(executor.SQLExecutor)
		opts := executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()
		exec := func(query string) {
			t.Helper()
			r, e := internal.Exec(ctx, query, opts)
			require.NoError(t, e, query)
			r.Close()
		}
		scalar := func(query string) int64 {
			t.Helper()
			r, e := internal.Exec(ctx, query, opts)
			require.NoError(t, e)
			defer r.Close()
			require.Len(t, r.Batches, 1)
			require.Equal(t, 1, r.Batches[0].RowCount())
			return vector.GetFixedAtNoTypeCheck[int64](r.Batches[0].Vecs[0], 0)
		}
		require.EqualValues(t, 1, scalar("select count(*) from mo_catalog.mo_tables where account_id=0 and reldatabase='mo_catalog' and relname='mo_database_inventory'"))
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			for _, q := range []string{"drop database if exists nr_inventory_source"} {
				r, e := internal.Exec(cleanupCtx, q, opts)
				if e != nil {
					t.Errorf("inventory fixture cleanup: %v", e)
					continue
				}
				r.Close()
			}
		}()
		exec("create database nr_inventory_source")
		exec("create table nr_inventory_source.payload(id int primary key,v int)")
		exec("insert into nr_inventory_source.payload values(1,10)")
		id := scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_source'")
		exec(fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=1 where account_id=0 and database_id=%d", id))
		cardinality := fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
		advance := fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=epoch+1 where account_id=0 and database_id=%d and epoch=1", id)
		read := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)
		for _, first := range []int{0, 1} {
			if !t.Run(fmt.Sprintf("commit_order_%d", first), func(t *testing.T) {
				exec(fmt.Sprintf("delete from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id))
				exec(fmt.Sprintf("insert into mo_catalog.mo_database_inventory values(0,%d,1)", id))
				raceCtx, stop := context.WithCancel(ctx)
				release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
				ready := make(chan error, 2)
				done := [2]chan error{make(chan error, 1), make(chan error, 1)}
				var wg sync.WaitGroup
				defer func() { stop(); wg.Wait() }()
				for i := range 2 {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						done[i] <- internal.ExecTxn(raceCtx, func(tx executor.TxnExecutor) error {
							if tx.Txn().Txn().Mode != pbtxn.TxnMode_Optimistic {
								ready <- errors.New("writer is not optimistic")
								return errors.New("writer is not optimistic")
							}
							meta := tx.Txn().Txn()
							t.Logf("epoch writer=%d id=%x snapshot=%s mode=%s isolation=%s", i, meta.ID, meta.SnapshotTS.DebugString(), meta.Mode, meta.Isolation)
							if meta.Isolation != pbtxn.TxnIsolation_RC {
								ready <- errors.New("writer is not RC")
								return errors.New("writer is not RC")
							}
							r, e := tx.Exec(advance, executor.StatementOption{})
							if e == nil {
								if r.AffectedRows != 1 {
									e = fmt.Errorf("CAS affected %d rows", r.AffectedRows)
								}
								r.Close()
							}
							ready <- e
							if e != nil {
								return e
							}
							select {
							case <-release[i]:
								return nil
							case <-raceCtx.Done():
								return raceCtx.Err()
							}
						}, opts.WithTxnMode(pbtxn.TxnMode_Optimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC))
					}(i)
				}
				for range 2 {
					select {
					case e := <-ready:
						require.NoError(t, e)
					case <-ctx.Done():
						t.Fatal("inventory writers did not reach barrier")
					}
				}
				close(release[first])
				require.NoError(t, <-done[first])
				close(release[1-first])
				e := <-done[1-first]
				// Capture the complete row cardinality even if both commits succeeded.
				t.Logf("epoch first=%d second-error=%v records=%d", first, e, scalar(cardinality))
				require.EqualValues(t, 1, scalar(cardinality))
				require.Error(t, e)
				require.Regexp(t, "(?i)(conflict|retry|def changed)", e.Error())
				require.EqualValues(t, 2, scalar(read))
			}) {
				return // Later cases require a unique, uncorrupted version row.
			}
		}
		for _, statementFailure := range []bool{false, true} {
			if !t.Run(fmt.Sprintf("atomic_rollback_statement_error_%t", statementFailure), func(t *testing.T) {
				exec(fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=1 where account_id=0 and database_id=%d", id))
				abort := errors.New("explicit rollback")
				err := internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
					for _, q := range []string{advance, "update nr_inventory_source.payload set v=20 where id=1"} {
						r, e := tx.Exec(q, executor.StatementOption{})
						if e != nil {
							return e
						}
						r.Close()
					}
					if statementFailure {
						r, e := tx.Exec("insert into nr_inventory_source.payload values(1,99)", executor.StatementOption{})
						if e == nil {
							r.Close()
							return errors.New("duplicate unexpectedly succeeded")
						}
						return e
					}
					return abort
				}, opts.WithTxnMode(pbtxn.TxnMode_Pessimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC))
				if statementFailure {
					require.Error(t, err)
					require.Regexp(t, "(?i)duplicate", err.Error())
				} else {
					require.ErrorIs(t, err, abort)
				}
				require.EqualValues(t, 1, scalar(read))
				require.EqualValues(t, 10, scalar("select cast(v as signed) from nr_inventory_source.payload where id=1"))
			}) {
				return
			}
		}
		exec(fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=1 where account_id=0 and database_id=%d", id))
		abort := errors.New("inventory fixture rollback")
		err = internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			r, e := tx.Exec(advance, executor.StatementOption{})
			if e != nil {
				return e
			}
			r.Close()
			return abort
		}, opts)
		require.ErrorIs(t, err, abort)
		require.EqualValues(t, 1, scalar(read))
		staleCtx, stopStale := context.WithCancel(ctx)
		ready, release, done := make(chan error, 1), make(chan struct{}), make(chan error, 1)
		var stale sync.WaitGroup
		stale.Add(1)
		defer func() { stopStale(); stale.Wait() }()
		go func() {
			defer stale.Done()
			done <- internal.ExecTxn(staleCtx, func(tx executor.TxnExecutor) error {
				r, e := tx.Exec(advance, executor.StatementOption{})
				if e == nil {
					if r.AffectedRows != 1 {
						e = fmt.Errorf("stale CAS affected %d rows", r.AffectedRows)
					}
					r.Close()
				}
				ready <- e
				if e != nil {
					return e
				}
				select {
				case <-release:
					return nil
				case <-staleCtx.Done():
					return staleCtx.Err()
				}
			}, opts.WithTxnMode(pbtxn.TxnMode_Optimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC))
		}()
		select {
		case e := <-ready:
			require.NoError(t, e)
		case <-ctx.Done():
			t.Fatal("stale writer did not reach retirement barrier")
		}
		err = internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, q := range []string{advance, "drop database nr_inventory_source"} {
				r, e := tx.Exec(q, executor.StatementOption{})
				if e != nil {
					return e
				}
				r.Close()
			}
			return nil
		}, opts.WithTxnMode(pbtxn.TxnMode_Pessimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC))
		require.NoError(t, err)
		close(release)
		err = <-done
		require.Error(t, err, "retirement must conflict with a stale version writer")
		require.Regexp(t, "(?i)(conflict|retry|def changed)", err.Error())
		exec("create database nr_inventory_source")
		replacement := scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_source'")
		require.NotEqual(t, id, replacement)
		require.Zero(t, scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", id)))
	})
}
