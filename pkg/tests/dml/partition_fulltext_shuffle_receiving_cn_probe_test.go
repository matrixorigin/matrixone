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

//go:build receiving_cn_acceptance

package dml

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Run with: go test -tags=receiving_cn_acceptance -run '^TestPartitionFulltextShuffleReceivingCNProbe$' ./pkg/tests/dml
// The test uses real embedded CN services and a transaction-scoped probe. It
// controls placement and statistics to exercise the remote pipeline; it does
// not establish the optimizer's natural placement or deployed S3 behavior.
func TestPartitionFulltextShuffleReceivingCNProbe(t *testing.T) {
	t.Setenv("MO_RECEIVING_CN_TARGETED_FORCE", "1")
	t.Setenv("MO_RECEIVING_CN_FORCE_POSTING_SHUFFLE", "1")
	defer func() { require.NoError(t, embed.CloseBaseClusterTests()) }()
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
		defer cancel()
		coordinator, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		for _, pair := range [][2]string{{coordinator.ServiceID(), peer.ServiceID()}, {peer.ServiceID(), coordinator.ServiceID()}} {
			inventory := clusterservice.GetMOCluster(pair[0])
			_, err = waitForCNReadiness(ctx, cnWorkStatePollInterval, inventory.(cnWorkStateInventory), inventory.(clusterservice.AuthoritativeRefresher), pair[0], pair[1])
			require.NoError(t, err)
		}

		db := openRetestSQLDB(t, c)
		db.SetMaxOpenConns(3)
		defer db.Close()
		const schema = "fulltext_shuffle_receiving_cn"
		defer cleanupTestDatabases(t, db, schema)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		oracle, err := db.Conn(ctx)
		require.NoError(t, err)
		defer oracle.Close()
		exec := func(q string) {
			t.Helper()
			_, e := conn.ExecContext(ctx, q)
			require.NoError(t, e, q)
		}
		flush := func(table string) {
			t.Helper()
			var raw string
			require.NoError(t, conn.QueryRowContext(ctx, "select mo_ctl('dn','flush','"+schema+"."+table+"')").Scan(&raw))
			var result struct {
				Method string            `json:"method"`
				Data   []json.RawMessage `json:"result"`
			}
			require.NoError(t, json.Unmarshal([]byte(raw), &result))
			require.Equal(t, "Flush", result.Method)
			require.Len(t, result.Data, 1, "the embedded TN must acknowledge the flush")
			t.Logf("fixture flush table=%s response=%s", table, raw)
		}
		exec("set experimental_fulltext_index=1")
		exec("create database " + schema)
		exec("use " + schema)
		_, err = oracle.ExecContext(ctx, "set experimental_fulltext_index=1")
		require.NoError(t, err)
		_, err = oracle.ExecContext(ctx, "use "+schema)
		require.NoError(t, err)
		exec("create table d(id int primary key, bucket int, body text, fulltext ft(body)) partition by range(bucket)(partition p0 values less than (10), partition p1 values less than maxvalue)")
		exec("create table probe_keys(id int primary key, original_bucket int)")

		type physicalIndex struct {
			parent, index uint64
			name          string
		}
		indexes := make([]physicalIndex, 2)
		engine := coordinator.RawService().(cnservice.Service).GetEngine()
		for i, part := range []string{"p0", "p1"} {
			physical := partitionservice.GetPartitionTableName("d", part)
			require.NoError(t, oracle.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", schema, physical).Scan(&indexes[i].parent))
			err = testutils.GetSQLExecutor(coordinator).ExecTxn(ctx, func(txn executor.TxnExecutor) error {
				accountCtx := defines.AttachAccountId(ctx, 0)
				_, _, parent, e := engine.GetRelationById(accountCtx, txn.Txn(), indexes[i].parent)
				if e != nil {
					return e
				}
				ids := parent.GetExtraInfo().IndexTables
				if len(ids) != 1 {
					return fmt.Errorf("partition %s has %d index relations", part, len(ids))
				}
				indexes[i].index = ids[0]
				_, _, index, e := engine.GetRelationById(accountCtx, txn.Txn(), ids[0])
				if e == nil {
					indexes[i].name = index.GetTableName()
				}
				return e
			}, executor.Options{})
			require.NoError(t, err)
			require.NotEmpty(t, indexes[i].name)
			bucket := 1 + i*19
			exec(fmt.Sprintf("insert into d select result+%d,%d,'alpha' from generate_series(13000) g", i*100000, bucket))
			exec(fmt.Sprintf("insert into probe_keys select id,bucket from d where bucket=%d", bucket))
			flush(physical)
			flush(indexes[i].name)
			var postings int
			indexName := "`" + schema + "`.`" + strings.ReplaceAll(indexes[i].name, "`", "``") + "`"
			require.NoError(t, oracle.QueryRowContext(ctx, "select count(*) from "+indexName+" where word='alpha'").Scan(&postings))
			require.Equal(t, 13000, postings)
		}
		flush("probe_keys")
		var seed int
		require.NoError(t, oracle.QueryRowContext(ctx, "select count(*) from probe_keys").Scan(&seed))
		require.Equal(t, 26000, seed)
		t.Logf("fixture coordinator=%s peer=%s rows=%d physical-indexes=%+v; both parent and hidden index partitions flushed", coordinator.ServiceID(), peer.ServiceID(), seed, indexes)

		// Synthetic estimates exercise the distributed path with a bounded real
		// fixture. The row and posting checks below use the actual 26k rows.
		stats := `{"table_cnt":5000000,"block_number":2048,"accurate_object_number":128,"ndv_map":{"id":3000000,"doc_id":3000000},"min_val_map":{"id":0,"doc_id":0},"max_val_map":{"id":5000000,"doc_id":5000000}}`
		statTables := []string{"d", "probe_keys", partitionservice.GetPartitionTableName("d", "p0"), partitionservice.GetPartitionTableName("d", "p1")}
		func() {
			indexRows, queryErr := oracle.QueryContext(ctx, "select relname from mo_catalog.mo_tables where reldatabase=?", schema)
			require.NoError(t, queryErr)
			defer func() { require.NoError(t, indexRows.Close()) }()
			for indexRows.Next() {
				var name string
				require.NoError(t, indexRows.Scan(&name))
				if strings.HasPrefix(name, "__mo_index_") {
					statTables = append(statTables, name)
				}
			}
			require.NoError(t, indexRows.Err())
		}()
		for _, name := range statTables {
			var patched float64
			require.NoError(t, conn.QueryRowContext(ctx, "select table_cnt from table_stats(?, 'patch', ?) g", schema+"."+name, stats).Scan(&patched))
			require.Equal(t, float64(5000000), patched, name)
		}
		moveSQL := func(word string) string {
			return "update d set bucket=case when bucket=1 then 20 else 1 end, body='" + word + "'"
		}
		logical, err := testutils.QueryTextResult(ctx, conn, "explain verbose "+moveSQL("beta"))
		require.NoError(t, err)
		t.Logf("fixture synthetic_stats=true transaction_scoped_placement=true logical_plan=%s", logical.Text)

		provider := coordinator.RawService().(interface {
			SessionMgr() *queryservice.SessionManager
		})
		checkImage := func(word string, moved bool) {
			t.Helper()
			condition := "d.bucket=k.original_bucket"
			if moved {
				condition = "d.bucket=case when k.original_bucket=1 then 20 else 1 end"
			}
			var total, parents, matches int
			require.NoError(t, oracle.QueryRowContext(ctx, "select count(*) from d").Scan(&total))
			require.NoError(t, oracle.QueryRowContext(ctx, "select count(*) from d join probe_keys k on d.id=k.id where "+condition+" and d.body=?", word).Scan(&parents))
			require.NoError(t, oracle.QueryRowContext(ctx, "select count(*) from d join probe_keys k on d.id=k.id where "+condition+" and match(d.body) against('"+word+"')").Scan(&matches))
			require.Equal(t, seed, total)
			require.Equal(t, seed, parents)
			require.Equal(t, seed, matches)
			for _, index := range indexes {
				name := "`" + schema + "`.`" + strings.ReplaceAll(index.name, "`", "``") + "`"
				for _, candidate := range []string{"alpha", "beta", "gamma", "delta", "control"} {
					var count int
					require.NoError(t, oracle.QueryRowContext(ctx, "select count(*) from "+name+" where word=?", candidate).Scan(&count))
					want := 0
					if candidate == word {
						want = 13000
					}
					require.Equal(t, want, count, "physical index %d word %s", index.index, candidate)
				}
			}
			t.Logf("image word=%s moved=%t parent=%d match=%d per_partition_postings=13000", word, moved, parents, matches)
		}
		checkImage("alpha", false)
		// A single-CN control uses ordinary planning with no probe installed.
		exec(`set session optimizer_hints = "forceOneCN=1"`)
		exec(moveSQL("control"))
		checkImage("control", true)
		exec(moveSQL("alpha"))
		checkImage("alpha", false)
		exec(`set session optimizer_hints = ""`)

		type receipt struct {
			action      uint8
			table, rows uint64
			digest      [32]byte
		}
		runMove := func(mode, word string) bool {
			t.Helper()
			exec("begin")
			txnOpen := true
			defer func() {
				if txnOpen {
					cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
					defer done()
					_, _ = conn.ExecContext(cleanup, "rollback")
				}
			}()
			var connectionID uint32
			require.NoError(t, conn.QueryRowContext(ctx, "select connection_id() from d limit 1").Scan(&connectionID))
			var txnID uuid.UUID
			for _, session := range provider.SessionMgr().GetAllSessions() {
				identity, ok := session.(interface {
					GetConnectionID() uint32
					GetTxnId() uuid.UUID
				})
				if ok && identity.GetConnectionID() == connectionID {
					txnID = identity.GetTxnId()
					break
				}
			}
			require.NotEqual(t, uuid.Nil, txnID)
			var mu sync.Mutex
			var events []multi_update.ReceivingCNEvent
			var overflow, fired, canceled atomic.Bool
			var partitionCalls atomic.Int32
			changed := make(chan struct{}, 1)
			reached, release := make(chan struct{}, 1), make(chan struct{})
			record := func(event multi_update.ReceivingCNEvent) {
				mu.Lock()
				if len(events) < 4096 {
					events = append(events, event)
				} else {
					overflow.Store(true)
				}
				mu.Unlock()
				select {
				case changed <- struct{}{}:
				default:
				}
			}
			uninstall, e := multi_update.InstallReceivingCNProbe("shuffle-"+mode+"-"+word, [16]byte(txnID), func(callCtx context.Context, event multi_update.ReceivingCNEvent) error {
				// Partition Call may run per row; reserve the bounded event budget
				// for the physical sync and terminal-cleanup observations.
				if event.Stage == "partition-call" && partitionCalls.Add(1) > 16 {
					return nil
				}
				record(event)
				if mode == "success" || event.Stage != "sync" || event.ServiceID != peer.ServiceID() || !event.PartitionWriter || event.Objects == 0 || event.ObjectRows == 0 || event.Blocks == 0 || !fired.CompareAndSwap(false, true) {
					return nil
				}
				reached <- struct{}{}
				if mode == "failure" {
					return errors.New("shuffle probe peer post-sync failure")
				}
				select {
				case <-callCtx.Done():
					canceled.Store(true)
					event.Stage = "probe-cancelled"
					record(event)
					return callCtx.Err()
				case <-release:
					return errors.New("shuffle probe barrier released")
				case <-time.After(30 * time.Second):
					return errors.New("shuffle probe peer barrier timeout")
				}
			})
			require.NoError(t, e)
			defer uninstall()
			queryCtx, cancelQuery := context.WithCancel(ctx)
			done := make(chan error, 1)
			joined := false
			go func() {
				_, runErr := conn.ExecContext(queryCtx, moveSQL(word))
				done <- runErr
			}()
			defer func() {
				close(release)
				cancelQuery()
				if !joined {
					select {
					case <-done:
					case <-time.After(30 * time.Second):
						t.Error("query did not terminate after barrier release and cancellation")
					}
				}
			}()
			if mode != "success" {
				select {
				case <-reached:
				case e = <-done:
					joined = true
					// The injected failure may finish the statement before this
					// goroutine selects the already-published progress event.
					select {
					case <-reached:
						if mode != "failure" {
							t.Fatalf("%s ended before origin cancellation: %v", mode, e)
						}
					default:
						t.Fatalf("%s ended before peer progress: %v", mode, e)
					}
				case <-time.After(45 * time.Second):
					t.Fatalf("%s did not reach peer progress", mode)
				}
				if mode == "cancel" {
					_, e = oracle.ExecContext(ctx, fmt.Sprintf("kill query %d", connectionID))
					require.NoError(t, e)
				}
			}
			if !joined {
				select {
				case e = <-done:
					joined = true
				case <-time.After(60 * time.Second):
					t.Fatal("statement did not return")
				}
			}
			if mode == "success" && e == nil {
				exec("commit")
				txnOpen = false
			} else if mode != "success" && e != nil {
				exec("rollback")
				txnOpen = false
			}
			// The origin can return before the peer's cancellation callback and
			// writer cleanup finish. Wait for the actual peer terminal event,
			// and for cancellation require cleanup after the cancellation event.
			timer := time.NewTimer(30 * time.Second)
			defer timer.Stop()
			var snapshot []multi_update.ReceivingCNEvent
			for {
				mu.Lock()
				snapshot = append(snapshot[:0], events...)
				mu.Unlock()
				terminalAfter := -1
				for i, event := range snapshot {
					if event.ServiceID == peer.ServiceID() &&
						((mode == "cancel" && event.Stage == "probe-cancelled") ||
							(mode != "cancel" && event.Stage == "sync" && event.PartitionWriter && event.Objects > 0)) {
						terminalAfter = i
					}
				}
				// The progress event identifies the actual receiver generation.
				// Every observed peer receiver must finish after its cleanup, not
				// merely one writer from one of several remote pipelines.
				opened, closed, progressed := map[uint64]bool{}, map[uint64]bool{}, map[uint64]bool{}
				lastProgress, lastCleanup := map[uint64]int{}, map[uint64]int{}
				for i, event := range snapshot {
					if event.ServiceID != peer.ServiceID() {
						continue
					}
					switch event.Stage {
					case "receiver-open":
						opened[event.ReceiverID] = true
					case "receiver-closed":
						closed[event.ReceiverID] = true
					case "sync":
						if event.PartitionWriter && event.Objects > 0 {
							progressed[event.ReceiverID] = true
							lastProgress[event.ReceiverID] = i
						}
					case "writers-cleaned":
						if event.Remaining == 0 && event.Active+event.Drained > 0 {
							lastCleanup[event.ReceiverID] = i
						}
					}
				}
				receiversDone := len(opened) > 0 && len(progressed) > 0 && !progressed[0]
				for id := range opened {
					receiversDone = receiversDone && id != 0 && closed[id]
				}
				for id := range progressed {
					receiversDone = receiversDone && opened[id] && closed[id] && lastCleanup[id] > lastProgress[id]
				}
				terminal := false
				if terminalAfter >= 0 {
					for _, event := range snapshot[terminalAfter+1:] {
						if event.ServiceID == peer.ServiceID() && event.Stage == "writers-cleaned" &&
							event.Remaining == 0 && event.Active+event.Drained > 0 {
							terminal = true
						}
					}
				}
				if terminal && receiversDone {
					break
				}
				select {
				case <-changed:
				case <-timer.C:
					t.Fatal("peer receiver termination or writer cleanup did not follow physical progress/cancellation")
				}
			}
			require.False(t, overflow.Load())
			produced, applied := map[receipt]int{}, map[receipt]int{}
			calls := map[string]int{}
			queries := map[string]bool{}
			peerTargets := map[uint64]bool{}
			remoteSync := false
			shuffleModes := map[string]int{}
			for _, event := range snapshot {
				if event.Stage == "forced-posting-shuffle" || event.Stage == "planned-posting-shuffle" {
					shuffleModes[event.Stage]++
				}
				if event.QueryID != "" {
					queries[event.QueryID] = true
				}
				if event.Stage == "partition-call" {
					calls[fmt.Sprintf("%s/action=%d", event.ServiceID, event.Action)]++
				}
				if event.Stage == "sync" {
					t.Logf("receipt txn=%s mode=%s stage=sync cn=%s query=%s receiver=%d table=%d action=%d partition=%t objects=%d rows=%d blocks=%d", txnID, mode, event.ServiceID, event.QueryID, event.ReceiverID, event.TableID, event.Action, event.PartitionWriter, event.Objects, event.ObjectRows, event.Blocks)
					if event.ServiceID == peer.ServiceID() && event.PartitionWriter && event.Objects > 0 && event.ObjectRows > 0 && event.Blocks > 0 {
						remoteSync = true
						peerTargets[event.TableID] = true
					}
				}
				if event.Stage == "produced" || event.Stage == "applied" {
					r := receipt{event.Action, event.TableID, event.Rows, event.Digest}
					if event.Stage == "produced" {
						produced[r]++
					} else {
						applied[r]++
					}
					t.Logf("receipt txn=%s mode=%s stage=%s cn=%s query=%s table=%d action=%d rows=%d digest=%x", txnID, mode, event.Stage, event.ServiceID, event.QueryID, event.TableID, event.Action, event.Rows, event.Digest)
				}
				if event.Stage == "compile" || event.Stage == "writers-cleaned" || event.Stage == "receiver-open" || event.Stage == "receiver-closed" {
					t.Logf("receipt txn=%s mode=%s stage=%s cn=%s receiver=%d estimate=%g selected_s3=%t active=%d drained=%d", txnID, mode, event.Stage, event.ServiceID, event.ReceiverID, event.Estimate, event.SelectedS3, event.Active, event.Drained)
				}
			}
			t.Logf("run txn=%s mode=%s calls=%v peer_nonempty_sync=%t produced=%d applied=%d", txnID, mode, calls, remoteSync, len(produced), len(applied))
			require.Len(t, queries, 1, "one executing target statement per transaction")
			require.NotEmpty(t, shuffleModes, "the fixture must exercise a posting shuffle")
			t.Logf("posting shuffle mode=%v (synthetic statistics)", shuffleModes)
			for id := range peerTargets {
				require.True(t, id == indexes[0].index || id == indexes[1].index, "unexpected physical posting table %d", id)
			}
			if mode == "success" {
				require.NoError(t, e)
				require.NotEmpty(t, produced)
				require.Equal(t, produced, applied)
				require.Len(t, peerTargets, 2, "both physical posting partitions must progress on the receiving CN")
			} else {
				require.Error(t, e)
				if mode == "failure" {
					require.Contains(t, e.Error(), "shuffle probe peer post-sync failure")
				} else {
					require.True(t, canceled.Load(), "KILL QUERY must cancel the peer writer context")
				}
			}
			return remoteSync
		}

		remote := runMove("success", "beta")
		checkImage("beta", true)
		require.True(t, remote, "no nonempty peer physical PartitionMultiUpdate Sync")
		require.True(t, runMove("failure", "gamma"))
		checkImage("beta", true)
		require.True(t, runMove("success", "gamma"))
		checkImage("gamma", false)
		require.True(t, runMove("cancel", "delta"))
		checkImage("gamma", false)
		require.True(t, runMove("success", "delta"))
		checkImage("delta", true)
	})
}
