//go:build mo_m0

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
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// Two real CNs are necessary: a local fixture or hand-built scope cannot prove
// production placement, capture, serialization and receiver admission. Four
// rows distinguish coordinator workspace and remote committed contributions.
func TestM0RemoteStatementSQL(t *testing.T) {
	// Like other specialized topologies in this package, retire only the idle
	// shared fixture before acquiring a disposable two-CN generation.
	require.NoError(t, embed.CloseSingleCNBaseClusterTests())
	require.NoError(t, embed.CloseBaseClusterTests())
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(cn.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	updater, ok := inventory.(clusterservice.CNWorkStateUpdaterWithContext)
	require.True(t, ok)
	var peer string
	waitWorkers := func(want int) {
		t.Helper()
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			inventory.GetCNService(clusterservice.NewSelector(), func(s metadata.CNService) bool {
				count++
				if s.ServiceID != cn.ServiceID() {
					peer = s.PipelineServiceAddress
				}
				return true
			})
			return count == want
		}, 30*time.Second, 100*time.Millisecond)
	}
	waitWorkers(2)
	require.NotEmpty(t, peer)
	const schema = "m0_remote_statement"
	db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(cleanupCtx, cn.ServiceID(), int(metadata.WorkState_Working)))
		cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
	})
	m0Exec(t, ctx, db, "insert into "+table+" values(5,5,5,90000,1000,50)")
	m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+table+"')")
	m0Exec(t, ctx, db, "alter table "+table+" add column v int")
	require.Equal(t, uint32(1), m0ReadEvolution(t, ctx, db, schema).LayoutGeneration)
	m0Exec(t, ctx, db, "delete from "+table+" where c_id=5")
	// Plain partitioned tables must retain their ordinary tagged-build behavior.
	m0Exec(t, ctx, db, "create table "+schema+".ordinary(id int primary key,v int) partition by hash(id) partitions 2")
	m0Exec(t, ctx, db, "insert into "+schema+".ordinary values(1,10),(2,20),(3,30)")
	m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+".ordinary')")
	m0Exec(t, ctx, db, "delete from "+schema+".ordinary where id=2")
	oldForce := plan2.GetForceScanOnMultiCN()
	plan2.SetForceScanOnMultiCN(true)
	t.Cleanup(func() { plan2.SetForceScanOnMultiCN(oldForce) })
	query := func(t *testing.T, q interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}, statement string) string {
		t.Helper()
		rows, err := q.QueryContext(ctx, statement)
		require.NoError(t, err, statement)
		defer rows.Close()
		columns, err := rows.Columns()
		require.NoError(t, err)
		var out strings.Builder
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			dest := make([]any, len(columns))
			for i := range values {
				dest[i] = &values[i]
			}
			require.NoError(t, rows.Scan(dest...))
			for i, v := range values {
				if i > 0 {
					out.WriteByte('\t')
				}
				if v.Valid {
					out.WriteString(v.String)
				} else {
					out.WriteString("NULL")
				}
			}
			out.WriteByte('\n')
		}
		require.NoError(t, rows.Err())
		return out.String()
	}
	require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(ctx, cn.ServiceID(), int(metadata.WorkState_Draining)))
	waitWorkers(1)
	t.Run("readonly singleton", func(t *testing.T) {
		statement := "select c_id,v from " + table + " where v is null order by c_id limit 2"
		require.Equal(t, "1\tNULL\n2\tNULL\n", query(t, db, statement))
		require.Contains(t, query(t, db, "explain phyplan analyze "+statement), peer, "scan must actually run on peer CN")
		require.Equal(t, "3\t0\t6\n", query(t, db, "select count(*),count(v),sum(c_id) from "+table))
	})
	if t.Failed() {
		return
	}
	t.Run("ordinary combined relation", func(t *testing.T) {
		require.Equal(t, "2\t40\n", query(t, db, "select count(*),sum(v) from "+schema+".ordinary"))
	})
	if t.Failed() {
		return
	}
	require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(ctx, cn.ServiceID(), int(metadata.WorkState_Working)))
	waitWorkers(2)
	t.Run("ordinary combined writable", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, "delete from "+schema+".ordinary where id=1")
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, "insert into "+schema+".ordinary values(4,40)")
		require.NoError(t, err)
		require.Equal(t, "3\t30\n4\t40\n", query(t, tx, "select id,v from "+schema+".ordinary order by id"))
		require.Equal(t, "2\t70\n", query(t, tx, "select count(*),sum(v) from "+schema+".ordinary"))
		require.NoError(t, tx.Rollback())
	})
	if t.Failed() {
		return
	}
	t.Run("writable memory", func(t *testing.T) {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		for _, statement := range []string{
			"update " + table + " set v=10 where c_id=1",
			"delete from " + table + " where c_id=2",
			"insert into " + table + " values(4,4,4,80000,1000,40,40)",
		} {
			_, err := tx.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		statement := "select c_id,v from " + table + " order by c_id"
		require.Equal(t, "1\t10\n3\tNULL\n4\t40\n", query(t, tx, statement))
		require.Contains(t, query(t, tx, "explain phyplan analyze "+statement), peer, "must include a real remote scan with the coordinator's prefix")
		require.Equal(t, "3\t2\t50\n", query(t, tx, "select count(*),count(v),sum(v) from "+table))
		_, err = tx.ExecContext(ctx, "update "+table+" set v=coalesce(v,0)+1")
		require.NoError(t, err)
		require.Equal(t, "1\t11\n3\t1\n4\t41\n", query(t, tx, statement))
		require.NoError(t, tx.Rollback())
	})

	if t.Failed() {
		return
	}
	t.Run("writable persistent view", func(t *testing.T) { m0RemotePersistentView(t, ctx, cn, db, schema, table) })
}

// The normal CN spill fault makes real SQL produce persistent INSERT objects.
// A real CN tombstone writer supplies the rare pre-commit persistent-delete
// state without thousands of DELETE rows. Only the producer setup is controlled:
// capture, placement, codec, RPC and consumers are the production execution.
func m0RemotePersistentView(t *testing.T, ctx context.Context, cn embed.ServiceOperator, db *sql.DB, schema, table string) {
	t.Helper()
	const flush = "fj/cn/workspace_force_flush"
	defer m0RemoveFaultPointQuiet(context.Background(), db, flush)
	m0AddFaultPoint(t, ctx, db, flush, "echo", "")
	svc := cn.RawService().(cnservice.Service)
	txnCtx := defines.AttachAccountId(ctx, 0)
	var remoteReceipts atomic.Int64
	previousReport := logutil.GetReportZapFunc()
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		if strings.HasPrefix(entry.Message, "inject_send_pipeline: client2server") {
			remoteReceipts.Add(1)
		}
		if previousReport != nil {
			return previousReport(enc, entry, fields)
		}
		return nil, nil
	}})
	defer logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previousReport})
	require.NoError(t, fault.AddFaultPoint(ctx, "inject_send_pipeline", ":::", "return", 0, table, false))
	defer func() {
		_, err := fault.RemoveFaultPoint(context.Background(), "inject_send_pipeline")
		require.NoError(t, err)
	}()
	rollback := errors.New("rollback remote statement view fixture")
	err := svc.GetSQLExecutor().ExecTxn(txnCtx, func(tx executor.TxnExecutor) error {
		returned := false
		defer func() {
			if !returned {
				cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
				defer stop()
				_ = tx.Txn().Rollback(cleanupCtx)
			}
		}()
		exec := func(statement string) executor.Result {
			t.Helper()
			result, err := tx.Exec(statement, executor.StatementOption{})
			require.NoError(t, err, statement)
			return result
		}
		for _, statement := range []string{
			"update " + table + " set v=10 where c_id=1",
			"delete from " + table + " where c_id=2",
			"insert into " + table + " values(4,4,4,80000,1000,40,40)",
			"select 1",
		} {
			r := exec(statement)
			r.Close()
		}
		ws := tx.Txn().GetWorkspace().(*disttae.Transaction)
		database, err := svc.GetEngine().Database(txnCtx, schema, tx.Txn())
		require.NoError(t, err)
		rel, err := database.Relation(txnCtx, "customer", nil)
		require.NoError(t, err)
		dbID, err := strconv.ParseUint(database.GetDatabaseId(txnCtx), 10, 64)
		require.NoError(t, err)
		files, raw := 0, 0
		ws.ForEachTableWrites(dbID, rel.GetTableID(txnCtx), int(ws.WriteOffset()), func(entry disttae.Entry) {
			if entry.Type() != disttae.INSERT || entry.Bat() == nil || entry.Bat().IsEmpty() {
				return
			}
			if entry.FileName() != "" {
				files++
			} else if entry.Bat().Attrs[0] != catalog.BlockMeta_BlockInfo {
				raw += entry.Bat().RowCount()
			}
		})
		require.Positive(t, files, "actual SQL spill must precede remote capture")
		require.Zero(t, raw, "do not mistake a still-in-memory INSERT for persistent data")
		m0RemoveFaultPoint(t, ctx, db, flush)
		r := exec("delete from " + table + " where c_id=4")
		r.Close()
		r = exec("insert into " + table + " values(6,6,6,80000,1000,40,40)")
		r.Close()
		// Obtain the exact persisted row and encoded composite PK through SQL.
		var victim types.Rowid
		var pk []byte
		r = exec("select __mo_rowid,__mo_cpkey_col from " + table + " where c_id=3")
		n := 0
		r.ReadRows(func(rows int, cols []*vector.Vector) bool {
			n += rows
			if rows > 0 {
				victim = vector.MustFixedColNoTypeCheck[types.Rowid](cols[0])[0]
				pk = append([]byte(nil), cols[1].GetBytesAt(0)...)
			}
			return true
		})
		r.Close()
		require.Equal(t, 1, n)
		proc := ws.GetProc()
		fs, err := colexec.GetSharedFSFromProc(proc)
		require.NoError(t, err)
		pkType := types.T_varchar.ToType()
		writer := colexec.NewCNS3TombstoneWriter(proc.Mp(), fs, pkType, -1)
		defer writer.Close()
		rows := readutil.NewCNTombstoneBatch(&pkType, objectio.HiddenColumnSelection_None)
		defer rows.Clean(proc.Mp())
		require.NoError(t, vector.AppendFixed(rows.Vecs[0], victim, false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(rows.Vecs[1], pk, false, proc.Mp()))
		rows.SetRowCount(1)
		require.NoError(t, writer.Write(txnCtx, rows))
		stats, err := writer.Sync(txnCtx)
		require.NoError(t, err)
		require.Len(t, stats, 1)
		written := colexec.AllocCNS3ResultBat(true)
		defer written.Clean(proc.Mp())
		require.NoError(t, vector.AppendBytes(written.Vecs[0], stats[0].Marshal(), false, proc.Mp()))
		written.SetRowCount(1)
		require.NoError(t, ws.WriteFile(disttae.DELETE, 0, dbID, rel.GetTableID(txnCtx), schema, "customer", stats[0].ObjectLocation().String(), written, disttae.DNStore{}))
		ts, err := rel.CollectTombstones(txnCtx, int(ws.WriteOffset()), engine.Policy_CollectUncommittedTombstones)
		require.NoError(t, err)
		require.True(t, ts.HasAnyTombstoneFile())
		read := func(statement string) []string {
			t.Helper()
			r := exec(statement)
			defer r.Close()
			var values []string
			r.ReadRows(func(n int, cols []*vector.Vector) bool {
				for i := 0; i < n; i++ {
					parts := make([]string, len(cols))
					for j, col := range cols {
						if col.IsNull(uint64(i)) {
							parts[j] = "NULL"
							continue
						}
						switch col.GetType().Oid {
						case types.T_int32:
							parts[j] = fmt.Sprint(vector.MustFixedColNoTypeCheck[int32](col)[i])
						case types.T_int64:
							parts[j] = fmt.Sprint(vector.MustFixedColNoTypeCheck[int64](col)[i])
						case types.T_uint64:
							parts[j] = fmt.Sprint(vector.MustFixedColNoTypeCheck[uint64](col)[i])
						default:
							require.True(t, col.GetType().IsVarlen())
							parts[j] = col.GetStringAt(i)
						}
					}
					values = append(values, strings.Join(parts, ":"))
				}
				return true
			})
			return values
		}
		statement := "select c_id,v from " + table + " order by c_id"
		require.Equal(t, []string{"1:10", "6:40"}, read(statement))
		receiptsBefore := remoteReceipts.Load()
		require.Equal(t, []string{"1:10", "6:40"}, read(statement))
		require.Greater(t, remoteReceipts.Load(), receiptsBefore, "the real pipeline server must receive this persistent-view query")
		require.Equal(t, []string{"2:2:50"}, read("select count(*),count(v),sum(v) from "+table))
		require.Equal(t, []string{"6:40"}, read("select c_id,v from "+table+" order by v desc,c_id limit 1"))
		r = exec("update " + table + " set v=coalesce(v,0)+1")
		r.Close()
		require.Equal(t, []string{"1:11", "6:41"}, read(statement))
		returned = true
		return rollback
	}, executor.Options{}.WithWaitCommittedLogApplied())
	require.ErrorIs(t, err, rollback)
	require.Equal(t, 3, m0Count(t, ctx, db, "select count(*) from "+table+" where v is null"))
}
