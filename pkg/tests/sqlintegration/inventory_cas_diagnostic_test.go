// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// These cases are deliberately separate roots: each diagnostic starts with a
// new table, and a failed race never feeds rollback/retirement experiments.
func TestInventoryCASCatalogInternal(t *testing.T) {
	testInventoryCAS(t, "mo_catalog.nr_cas_inventory", false, pbtxn.TxnIsolation_SI)
}

func TestInventoryCASOrdinaryInternal(t *testing.T) {
	testInventoryCAS(t, "nr_cas_probe.inventory", false, pbtxn.TxnIsolation_SI)
}

func TestInventoryCASOrdinaryPublic(t *testing.T) {
	testInventoryCAS(t, "nr_cas_probe.inventory", true, pbtxn.TxnIsolation_SI)
}

func TestInventoryCASCatalogInternalRC(t *testing.T) {
	testInventoryCAS(t, "mo_catalog.nr_cas_inventory", false, pbtxn.TxnIsolation_RC)
}

func TestInventoryCASOrdinaryInternalRC(t *testing.T) {
	testInventoryCAS(t, "nr_cas_probe.inventory", false, pbtxn.TxnIsolation_RC)
}

func TestInventoryCASOrdinaryPublicRC(t *testing.T) {
	testInventoryCAS(t, "nr_cas_probe.inventory", true, pbtxn.TxnIsolation_RC)
}

func TestInventoryCASOrdinaryPublicRCReverse(t *testing.T) {
	testInventoryCASOrder(t, "nr_cas_probe.inventory", true, pbtxn.TxnIsolation_RC, 1, true)
}

func testInventoryCAS(t *testing.T, table string, public bool, isolation pbtxn.TxnIsolation) {
	t.Helper()
	testInventoryCASOrder(t, table, public, isolation, 0, false)
}

func testInventoryCASOrder(t *testing.T, table string, public bool, isolation pbtxn.TxnIsolation, first int, restart bool) {
	t.Helper()
	withInventoryCASCluster(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		require.Zero(t, cn.GetServiceConfig().CN.Engine.CNTransferTxnLifespanThreshold)
		var tnCount int
		c.ForeachServices(func(s embed.ServiceOperator) bool {
			cfg := s.GetServiceConfig()
			if cfg.ServiceType == "TN" {
				tnCount++
				tn := cfg.TN_please_use_getTNServiceConfig
				if tn == nil {
					tn = cfg.TNCompatible
				}
				require.NotNil(t, tn)
				t.Logf("CAS_CONFIG tn-mode=%s incremental-dedup=%s cn-transfer-threshold=%s", tn.Txn.Mode, tn.Txn.IncrementalDedup, cn.GetServiceConfig().CN.Engine.CNTransferTxnLifespanThreshold)
				require.Equal(t, "true", tn.Txn.IncrementalDedup)
			}
			return true
		})
		require.Equal(t, 1, tnCount)
		value, ok := moruntime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		require.True(t, ok)
		internal := value.(executor.SQLExecutor)
		opts := executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()
		exec := func(q string) {
			t.Helper()
			r, e := internal.Exec(ctx, q, opts)
			require.NoError(t, e)
			r.Close()
		}
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxIdleConns(0)
		exec("create database nr_cas_probe")
		defer func() {
			if t.Failed() {
				t.Log("CAS failed fixture retained for diagnosis; shared fixture will be quarantined")
				return
			}
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			for _, q := range []string{"drop table if exists " + table, "drop database nr_cas_probe"} {
				r, e := internal.Exec(cleanup, q, opts)
				if e != nil {
					t.Errorf("CAS cleanup: %v", e)
					continue
				}
				r.Close()
			}
		}()
		exec("create table " + table + " (account_id int unsigned not null, database_id bigint unsigned not null, epoch bigint unsigned not null, primary key(account_id,database_id))")
		exec("insert into " + table + " values (0,42,1)")
		var name, ddl string
		require.NoError(t, db.QueryRowContext(ctx, "show create table "+table).Scan(&name, &ddl))
		t.Logf("CAS_SCHEMA table=%s ddl=%q", table, ddl)
		read := func(label string) [][3]uint64 {
			t.Helper()
			// A fresh session and autocommit read, not either writer's workspace.
			conn, e := db.Conn(ctx)
			require.NoError(t, e)
			defer conn.Close()
			rows, e := conn.QueryContext(ctx, "select account_id,database_id,epoch from "+table+" order by account_id,database_id,epoch")
			require.NoError(t, e)
			defer rows.Close()
			var records [][3]uint64
			for rows.Next() {
				var row [3]uint64
				require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
				records = append(records, row)
			}
			require.NoError(t, rows.Err())
			t.Logf("CAS_ROWS stage=%s records=%v", label, records)
			return records
		}
		require.Equal(t, [][3]uint64{{0, 42, 1}}, read("before"))
		query := "update " + table + " set epoch=epoch+1 where account_id=0 and database_id=42 and epoch=1"
		var commits [2]error
		var ids [2]string
		checkMeta := func(i int, meta pbtxn.TxnMeta) error {
			ids[i] = fmt.Sprintf("%x", meta.ID)
			t.Logf("CAS_TXN writer=%d id=%s snapshot=%s mode=%s isolation=%s", i, ids[i], meta.SnapshotTS.DebugString(), meta.Mode, meta.Isolation)
			if len(meta.ID) == 0 || meta.SnapshotTS.IsEmpty() || meta.Mode != pbtxn.TxnMode_Optimistic || meta.Isolation != isolation {
				return fmt.Errorf("writer %d has unexpected transaction metadata", i)
			}
			return nil
		}
		if public {
			testContext := moruntime.MustGetTestingContext(cn.ServiceID())
			previous := testContext.SetTxnModeTestConfig(moruntime.TxnModeTestConfig{AccountID: 0, Mode: pbtxn.TxnMode_Optimistic, Enabled: true})
			defer testContext.SetTxnModeTestConfig(previous)
			var transactions [2]*sql.Tx
			seen := make(map[string]bool)
			for i := range 2 {
				conn, e := db.Conn(ctx)
				require.NoError(t, e)
				defer conn.Close()
				level := "repeatable read"
				if isolation == pbtxn.TxnIsolation_RC {
					level = "read committed"
				}
				_, e = conn.ExecContext(ctx, "set session transaction isolation level "+level)
				require.NoError(t, e)
				transactions[i], e = conn.BeginTx(ctx, nil)
				require.NoError(t, e)
				defer transactions[i].Rollback()
				r, e := transactions[i].ExecContext(ctx, query)
				require.NoError(t, e)
				n, e := r.RowsAffected()
				require.NoError(t, e)
				require.EqualValues(t, 1, n)
				var current []pbtxn.TxnMeta
				cn.RawService().(cnservice.Service).GetTxnClient().IterTxns(func(v client.TxnOverview) bool {
					if v.UserTxn && !seen[fmt.Sprintf("%x", v.Meta.ID)] {
						current = append(current, v.Meta)
					}
					return true
				})
				require.Len(t, current, 1, "identify the newly started SQL transaction")
				require.NoError(t, checkMeta(i, current[0]))
				seen[ids[i]] = true
			}
			var metas []pbtxn.TxnMeta
			cn.RawService().(cnservice.Service).GetTxnClient().IterTxns(func(v client.TxnOverview) bool {
				if v.UserTxn {
					metas = append(metas, v.Meta)
				}
				return true
			})
			require.Len(t, metas, 2, "identify both user transactions before commit")
			for _, meta := range metas {
				require.True(t, seen[fmt.Sprintf("%x", meta.ID)])
			}
			commits[first] = transactions[first].Commit()
			commits[1-first] = transactions[1-first].Commit()
		} else {
			raceCtx, stop := context.WithCancel(ctx)
			var wg sync.WaitGroup
			defer func() { stop(); wg.Wait() }()
			type prepared struct {
				index int
				meta  pbtxn.TxnMeta
				err   error
			}
			ready := make(chan prepared, 2)
			release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			done := [2]chan error{make(chan error, 1), make(chan error, 1)}
			for i := range 2 {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					var op client.TxnOperator
					e := internal.ExecTxn(raceCtx, func(tx executor.TxnExecutor) error {
						op = tx.Txn()
						r, e := tx.Exec(query, executor.StatementOption{})
						if e == nil {
							if r.AffectedRows != 1 {
								e = fmt.Errorf("writer %d affected %d rows", i, r.AffectedRows)
							}
							r.Close()
						}
						ready <- prepared{i, tx.Txn().Txn(), e}
						if e != nil {
							return e
						}
						select {
						case <-release[i]:
							return nil
						case <-raceCtx.Done():
							return raceCtx.Err()
						}
					}, opts.WithTxnMode(pbtxn.TxnMode_Optimistic).WithTxnIsolation(isolation))
					if op != nil {
						meta := op.Txn()
						t.Logf("CAS_TERMINAL writer=%d id=%x snapshot=%s commit=%s status=%s error=%v", i, meta.ID, meta.SnapshotTS.DebugString(), meta.CommitTS.DebugString(), meta.Status, e)
					}
					done[i] <- e
				}(i)
			}
			for range 2 {
				select {
				case p := <-ready:
					require.NoError(t, p.err)
					require.NoError(t, checkMeta(p.index, p.meta))
				case <-ctx.Done():
					t.Fatal("CAS writers did not reach barrier")
				}
			}
			for i := range 2 {
				close(release[i])
				select {
				case commits[i] = <-done[i]:
				case <-ctx.Done():
					t.Fatal("CAS commit did not terminate")
				}
			}
		}
		t.Logf("CAS_COMMIT first=%v second=%v", commits[0], commits[1])
		service := cn.RawService().(cnservice.Service)
		_, err = service.GetTxnClient().WaitLogTailAppliedAt(ctx, service.GetTxnClient().GetLatestCommitTS())
		require.NoError(t, err)
		final := read("committed")
		require.Equal(t, final, read("fresh-session-reread"))
		// Record the complete result before any conflict assertion can stop us.
		require.NotEqual(t, ids[0], ids[1])
		require.NoError(t, commits[first])
		require.Error(t, commits[1-first], "competing epoch update must not commit")
		require.Regexp(t, "(?i)(conflict|retry|duplicate|def changed)", commits[1-first].Error())
		require.Equal(t, [][3]uint64{{0, 42, 2}}, final)
		if restart {
			require.NoError(t, c.Stop())
			require.NoError(t, c.Start())
			require.Equal(t, [][3]uint64{{0, 42, 2}}, read("restart"))
			// The internal executor belongs to the previous CN generation.
			v, ok := moruntime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(moruntime.InternalSQLExecutor)
			require.True(t, ok)
			internal = v.(executor.SQLExecutor)
		}
	})
}

func withInventoryCASCluster(t *testing.T, fn func(embed.Cluster)) {
	withInventoryCASClusterThreshold(t, 0, fn)
}

func withInventoryCASClusterThreshold(t *testing.T, threshold time.Duration, fn func(embed.Cluster)) {
	t.Helper()
	c, err := embed.StartTestCluster(embed.WithCNCount(1), embed.WithPreStart(func(s embed.ServiceOperator) {
		s.Adjust(func(cfg *embed.ServiceConfig) {
			if cfg.ServiceType == "TN" {
				tn := cfg.TN_please_use_getTNServiceConfig
				if tn == nil {
					tn = cfg.TNCompatible
				}
				if tn != nil {
					tn.Txn.Mode = "Pessimistic"
					tn.Txn.IncrementalDedup = "true"
				}
			}
			if cfg.ServiceType == "CN" {
				cfg.CN.Engine.CNTransferTxnLifespanThreshold = threshold
			}
		})
	}))
	if c != nil {
		defer func() { require.NoError(t, c.Close()) }()
	}
	require.NoError(t, err)
	fn(c)
}

// Flush moves the original physical row while an optimistic RC update is in
// flight. This control must commit: physical maintenance is not a competing
// logical epoch update, and transfer must remain enabled.
func TestInventoryCASRCTransfer(t *testing.T) {
	withInventoryCASCluster(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		exec := func(q string) { t.Helper(); _, e := db.ExecContext(ctx, q); require.NoError(t, e) }
		exec("create database nr_transfer_probe")
		exec("create table nr_transfer_probe.inventory(id int primary key,epoch int)")
		exec("insert into nr_transfer_probe.inventory values(1,1)")
		tc := moruntime.MustGetTestingContext(cn.ServiceID())
		previous := tc.SetTxnModeTestConfig(moruntime.TxnModeTestConfig{AccountID: 0, Mode: pbtxn.TxnMode_Optimistic, Enabled: true})
		defer tc.SetTxnModeTestConfig(previous)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, "set session transaction isolation level read committed")
		require.NoError(t, err)
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		r, err := tx.ExecContext(ctx, "update nr_transfer_probe.inventory set epoch=2 where id=1 and epoch=1")
		require.NoError(t, err)
		n, err := r.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
		transferCount := func() uint64 {
			metric := &dto.Metric{}
			require.NoError(t, v2.BatchTransferTombstonesDurationHistogram.(prometheus.Metric).Write(metric))
			return metric.GetHistogram().GetSampleCount()
		}
		beforeTransfer := transferCount()
		var response string
		require.NoError(t, db.QueryRowContext(ctx, "select mo_ctl('dn','flush','nr_transfer_probe.inventory')").Scan(&response))
		var flush struct {
			Method string `json:"method"`
			Result []struct {
				ReturnStr string `json:"returnStr"`
			} `json:"result"`
		}
		require.NoError(t, json.Unmarshal([]byte(response), &flush))
		require.Equal(t, "Flush", flush.Method)
		require.NotEmpty(t, flush.Result)
		for _, r := range flush.Result {
			require.Equal(t, "OK", r.ReturnStr)
		}
		require.NoError(t, tx.Commit())
		require.Greater(t, transferCount(), beforeTransfer, "the original delete RowID must actually transfer")
		var count, epoch int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*),max(epoch) from nr_transfer_probe.inventory").Scan(&count, &epoch))
		require.Equal(t, 1, count)
		require.Equal(t, 2, epoch)
		tc.SetTxnModeTestConfig(previous)
		exec("drop database nr_transfer_probe")
	})
}

func TestInventoryCASPessimisticRC(t *testing.T) {
	withInventoryCASCluster(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		exec := func(q string) { t.Helper(); _, e := db.ExecContext(ctx, q); require.NoError(t, e) }
		exec("create database nr_pess_probe")
		exec("create table nr_pess_probe.inventory(id int primary key,epoch int)")
		exec("insert into nr_pess_probe.inventory values(1,1)")
		var txs [2]*sql.Tx
		for i := range 2 {
			conn, e := db.Conn(ctx)
			require.NoError(t, e)
			defer conn.Close()
			_, e = conn.ExecContext(ctx, "set session transaction isolation level read committed")
			require.NoError(t, e)
			txs[i], e = conn.BeginTx(ctx, nil)
			require.NoError(t, e)
			defer txs[i].Rollback()
		}
		for i := range 2 {
			r, e := txs[i].ExecContext(ctx, "update nr_pess_probe.inventory set epoch=epoch+1 where id=1 and epoch=1")
			require.NoError(t, e)
			n, e := r.RowsAffected()
			require.NoError(t, e)
			require.EqualValues(t, 1-i, n)
			cn.RawService().(cnservice.Service).GetTxnClient().IterTxns(func(v client.TxnOverview) bool {
				if v.UserTxn {
					require.Equal(t, pbtxn.TxnMode_Pessimistic, v.Meta.Mode)
					require.Equal(t, pbtxn.TxnIsolation_RC, v.Meta.Isolation)
				}
				return true
			})
			require.NoError(t, txs[i].Commit())
		}
		var count, epoch int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*),max(epoch) from nr_pess_probe.inventory").Scan(&count, &epoch))
		require.Equal(t, 1, count)
		require.Equal(t, 2, epoch)
		exec("drop database nr_pess_probe")
	})
}

// Share one cluster: both cases have the same topology/configuration and use
// distinct tables/transactions. A long transfer threshold makes the pre-commit
// revalidation the sole guard after the deliberately failed explicit advance.
func TestInventoryCASFailedValidationCommit(t *testing.T) {
	withInventoryCASClusterThreshold(t, time.Hour, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		value, ok := moruntime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		require.True(t, ok)
		sqlExec := value.(executor.SQLExecutor)
		opts := executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()
		exec := func(q string) { t.Helper(); r, e := sqlExec.Exec(ctx, q, opts); require.NoError(t, e); r.Close() }
		exec("create database cas_failed_validation")
		defer exec("drop database cas_failed_validation")
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		for _, cancelled := range []bool{false, true} {
			t.Run(fmt.Sprintf("cancelled=%v", cancelled), func(t *testing.T) {
				table := fmt.Sprintf("cas_failed_validation.t%v", cancelled)
				exec("create table " + table + "(id int primary key,epoch int)")
				exec("insert into " + table + " values(1,1)")
				err := sqlExec.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
					op := tx.Txn()
					defer op.Rollback(ctx)
					r, e := tx.Exec("update "+table+" set epoch=2 where id=1 and epoch=1", executor.StatementOption{})
					require.NoError(t, e)
					require.EqualValues(t, 1, r.AffectedRows)
					r.Close()
					ws := op.GetWorkspace().(*disttae.Transaction)
					before := op.SnapshotTS()
					exec("update " + table + " set epoch=3 where id=1")
					if cancelled {
						// Model cancellation after RC read visibility has already advanced.
						op.SetSnapshotTS(cn.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS().Next())
						failed, stop := context.WithCancel(ctx)
						stop()
						require.ErrorIs(t, ws.AdvanceSnapshot(failed, op.SnapshotTS()), context.Canceled)
					} else {
						require.Error(t, ws.AdvanceSnapshot(ctx, pbtxn.TxnMeta{}.SnapshotTS))
					}
					require.True(t, before.Less(op.SnapshotTS()), "read snapshot really advanced")
					e = op.Commit(ctx)
					require.Error(t, e, "fresh-context COMMIT must revalidate the retained dependency")
					require.Contains(t, e.Error(), "conflict")
					return e
				}, opts.WithTxnMode(pbtxn.TxnMode_Optimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC))
				require.Error(t, err)
				rows, e := db.QueryContext(ctx, "select id,epoch from "+table)
				require.NoError(t, e)
				defer rows.Close()
				require.True(t, rows.Next())
				var id, epoch int
				require.NoError(t, rows.Scan(&id, &epoch))
				require.Equal(t, 1, id)
				require.Equal(t, 3, epoch)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			})
		}
	})
}
