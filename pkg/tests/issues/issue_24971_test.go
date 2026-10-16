// Copyright 2021 - 2026 MatrixOrigin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"encoding/hex"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestIssue24971LockWaitBackpressureRecovery is a regression guard for
// https://github.com/matrixorigin/matrixone/issues/24971.
//
// Before the explicit pipeline termination protocol, a DML pipeline whose
// consumer side was blocked on a row lock could lose its terminal (End)
// notification during cleanup backpressure. Once the blocking lock was
// released, the query stayed stuck in PipelineSignalReceiver while still
// holding its acquired row locks, so later writers on the same keys queued
// behind it until the session was killed.
//
// This test builds the same shape on a single embedded CN: a REPLACE INTO
// ... SELECT whose plan has a merge/connector edge (Sink Scan) above Lock,
// with session A holding a row lock and session B running the REPLACE. After
// a backpressure window longer than the (shortened) terminal send timeout,
// releasing A's lock must let B finish successfully with all rows written and
// all locks released, and a third session must not be blocked.
//
// It must not assert on the absence of cleanup warnings: falling back to the
// abort path is also a valid outcome; the guarded contract is recovery
// correctness, not a specific cleanup path.
func TestIssue24971LockWaitBackpressureRecovery(t *testing.T) {
	// Shorten the terminal-signal send timeout so the backpressure window is
	// seconds instead of minutes. Other tests (e.g. compile/remoterun_test.go
	// and colexec/connector/types_test.go) adjust the same variable.
	oldTimeout := process.PipelineSignalSendTimeout
	process.PipelineSignalSendTimeout = 2 * time.Second
	defer func() { process.PipelineSignalSendTimeout = oldTimeout }()

	c, err := embed.NewCluster(embed.WithCNCount(1), embed.WithPreStart(func(svc embed.ServiceOperator) {
		svc.Adjust(func(cfg *embed.ServiceConfig) {
			cfg.Log.Level = "error"
			if svc.ServiceType() == metadata.ServiceType_CN {
				cfg.CN.Frontend.SkipCheckUser = true
			}
		})
	}))
	require.NoError(t, err)
	require.NoError(t, c.Start())
	defer func() { require.NoError(t, c.Close()) }()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	t.Logf("single CN=%s, data dir=%s, SQL port=%d", cn.ServiceID(), cn.GetServiceConfig().DataDir, cn.GetServiceConfig().CN.Frontend.Port)

	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?timeout=10s&readTimeout=120s&writeTimeout=10s", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(6)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	admin, err := db.Conn(ctx)
	require.NoError(t, err)
	defer admin.Close()
	exec := func(q string) {
		t.Helper()
		_, e := admin.ExecContext(ctx, q)
		require.NoError(t, e, q)
	}

	exec("create database issue24971")
	exec("create table issue24971.file (id bigint primary key, v bigint)")
	exec("create table issue24971.file_working (id bigint primary key, v bigint)")
	const rows = 16384
	exec(fmt.Sprintf("insert into issue24971.file_working select result, result from generate_series(1, %d) g", rows))

	var tableID uint64
	require.NoError(t, admin.QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where reldatabase='issue24971' and relname='file'").Scan(&tableID))
	ls := lockservice.GetLockServiceByServiceID(cn.ServiceID())
	require.NotNil(t, ls)

	// The two-row recursive CTE keeps the data tiny while steering the plan to
	// a single-CN shape with a merge/connector edge above Lock, which is the
	// shape that was vulnerable to terminal-signal loss under backpressure.
	query := "replace into issue24971.file with recursive seeds(k) as (select 0 union all select k+1 from seeds where k<1) " +
		"select w.id, w.v from issue24971.file_working w join seeds s on w.id % 2 = s.k"
	issue24971RequirePlanShape(t, ctx, admin, query)

	for _, delayed := range []bool{false, true} {
		name := "release_before_terminal_window"
		if delayed {
			name = "release_after_terminal_window"
		}
		if !t.Run(name, func(t *testing.T) {
			exec("truncate table issue24971.file")
			exec("insert into issue24971.file values (1, 0)")
			require.NoError(t, admin.QueryRowContext(ctx,
				"select rel_id from mo_catalog.mo_tables where reldatabase='issue24971' and relname='file'").Scan(&tableID))

			a, e := db.Conn(ctx)
			require.NoError(t, e)
			defer a.Close()
			b, e := db.Conn(ctx)
			require.NoError(t, e)
			defer b.Close()
			probe, e := db.Conn(ctx)
			require.NoError(t, e)
			defer probe.Close()

			_, e = a.ExecContext(ctx, "begin")
			require.NoError(t, e)
			defer a.ExecContext(context.Background(), "rollback")
			var v int64
			require.NoError(t, a.QueryRowContext(ctx, "select v from issue24971.file where id=1 for update").Scan(&v))
			initial := issue24971Locks(ls, tableID)
			require.NotEmpty(t, initial.holders, "session A must hold the row lock")

			var bid, cid uint64
			require.NoError(t, b.QueryRowContext(ctx, "select connection_id()").Scan(&bid))
			require.NoError(t, probe.QueryRowContext(ctx, "select connection_id()").Scan(&cid))
			work := issue24971StartQuery(ctx, b, query)
			defer issue24971StopQuery(t, admin, bid, work)

			require.True(t, issue24971Until(ctx, 15*time.Second, func() bool {
				return len(issue24971Locks(ls, tableID).waiters) > 0 || work.finished()
			}), "query B never entered a real lock wait")
			require.False(t, work.finished(), "query B returned while A still held the lock: %v", work.result())

			if delayed {
				// Hold the backpressure window well past the shortened terminal
				// send timeout so a cleanup attempt inside the window would
				// already have had to deliver (or drop) its terminal signal.
				deadline := time.Now().Add(8 * time.Second)
				for time.Now().Before(deadline) && !work.finished() {
					time.Sleep(100 * time.Millisecond)
				}
				require.False(t, work.finished(), "query B finished during the hold window: %v", work.result())
			}

			_, e = a.ExecContext(ctx, "rollback")
			require.NoError(t, e)

			require.True(t, issue24971Until(ctx, 60*time.Second, work.finished),
				"query B did not finish after the blocking lock was released; possible terminal-signal loss regression")
			require.NoError(t, work.result())
			require.True(t, issue24971Until(ctx, 15*time.Second, func() bool {
				return issue24971Locks(ls, tableID).count == 0
			}), "locks were not released after query B finished: %+v", issue24971Locks(ls, tableID))

			// A third session on the same key must not be blocked by B.
			follower := issue24971StartQuery(ctx, probe, "update issue24971.file set v=v+1 where id=1")
			defer issue24971StopQuery(t, admin, cid, follower)
			require.True(t, issue24971Until(ctx, 20*time.Second, follower.finished),
				"a later statement was still blocked after B finished")
			require.NoError(t, follower.result())

			var count int
			require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from issue24971.file").Scan(&count))
			require.Equal(t, int(rows), count, "query B must write every row when it finishes successfully")
			require.Equal(t, 0, issue24971Locks(ls, tableID).count)
		}) {
			break
		}
	}
}

// issue24971RequirePlanShape fails the test unless the plan contains the
// Lock node and a Sink Scan feeding a merge/connector edge. If the optimizer
// changes and this shape disappears, the test would silently stop covering
// the backpressure path, so treat a shape drift as a failure for maintainers
// to re-check.
func issue24971RequirePlanShape(t *testing.T, ctx context.Context, conn *sql.Conn, query string) {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "explain verbose "+query)
	require.NoError(t, err)
	defer rows.Close()
	cols, err := rows.Columns()
	require.NoError(t, err)
	var plan strings.Builder
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		require.NoError(t, rows.Scan(ptrs...))
		for i := range vals {
			if vals[i] != nil {
				plan.Write(vals[i])
				plan.WriteByte('\n')
			}
		}
	}
	require.NoError(t, rows.Err())
	planText := plan.String()
	require.Contains(t, planText, "Lock", "plan lost the Lock node; re-check the test shape:\n%s", planText)
	require.Contains(t, planText, "Sink Scan", "plan lost the merge/connector edge (Sink Scan); re-check the test shape:\n%s", planText)
	t.Logf("plan shape verified (Lock + Sink Scan present)")
}

type issue24971Query struct {
	done chan struct{}
	err  error
}

func issue24971StartQuery(ctx context.Context, conn *sql.Conn, query string) *issue24971Query {
	q := &issue24971Query{done: make(chan struct{})}
	go func() {
		defer close(q.done)
		_, q.err = conn.ExecContext(ctx, query)
	}()
	return q
}

func (q *issue24971Query) finished() bool {
	select {
	case <-q.done:
		return true
	default:
		return false
	}
}

func (q *issue24971Query) result() error {
	if q.finished() {
		return q.err
	}
	return fmt.Errorf("query is still running")
}

// issue24971StopQuery best-effort cleanup for a query this test owns: kill
// the query, then the connection, with bounded waits. Cleanup waits must
// tolerate a pipeline that legitimately needs a couple of short timeouts to
// tear down, so they are not treated as a failure of the guarded contract.
func issue24971StopQuery(t *testing.T, admin *sql.Conn, id uint64, q *issue24971Query) {
	t.Helper()
	if q.finished() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := admin.ExecContext(ctx, fmt.Sprintf("kill query %d", id))
	t.Logf("cleanup: kill query %d: %v", id, err)
	if issue24971Until(ctx, 20*time.Second, q.finished) {
		return
	}
	_, err = admin.ExecContext(ctx, fmt.Sprintf("kill connection %d", id))
	t.Logf("cleanup: kill connection %d: %v", id, err)
	if !issue24971Until(ctx, 8*time.Second, q.finished) {
		issue24971DumpStuckStacks(t)
		t.Errorf("test-owned query %d did not exit after kill", id)
	}
}

func issue24971Until(ctx context.Context, timeout time.Duration, pred func() bool) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if pred() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return pred()
		case <-ticker.C:
		}
	}
}

type issue24971LockState struct {
	holders, waiters []string
	count            int
}

func issue24971Locks(ls lockservice.LockService, tableID uint64) issue24971LockState {
	s := issue24971LockState{}
	ls.IterLocks(func(table uint64, keys [][]byte, lock lockservice.Lock) bool {
		if table != tableID {
			return true
		}
		s.count++
		lock.IterHolders(func(txn pblock.WaitTxn) bool {
			id := hex.EncodeToString(txn.TxnID)
			if !issue24971Contains(s.holders, id) {
				s.holders = append(s.holders, id)
			}
			return true
		})
		lock.IterWaiters(func(txn pblock.WaitTxn) bool {
			id := hex.EncodeToString(txn.TxnID)
			if !issue24971Contains(s.waiters, id) {
				s.waiters = append(s.waiters, id)
			}
			return true
		})
		return true
	})
	return s
}

func issue24971Contains(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}

// issue24971DumpStuckStacks logs only goroutine blocks that look relevant to
// the guarded path, so a failure log stays readable.
func issue24971DumpStuckStacks(t *testing.T) {
	t.Helper()
	buf := make([]byte, 16<<20)
	n := runtime.Stack(buf, true)
	for _, block := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(block, "PipelineSignalReceiver") ||
			strings.Contains(block, "LockOp") ||
			strings.Contains(block, "lockservice") {
			t.Logf("relevant goroutine stack:\n%s", block)
		}
	}
}
