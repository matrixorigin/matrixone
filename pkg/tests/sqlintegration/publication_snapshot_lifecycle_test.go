// Copyright 2026 Matrix Origin
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

package sqlintegration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Issue 30949 review rounds 3 and 4 replace simulated lifecycle outcomes
// with transaction-backed proof of the approved design §4.4/§9 U5 admission
// protocol for publication table snapshots. The engine rejects snapshot DDL
// inside user transactions, and admission-family locks are statement-scoped,
// so the deterministic interleavings are driven by in-flight statements: the
// process-local admission hook parks a real CREATE SNAPSHOT between physical
// lockservice acquisitions (holding the keys it already granted) while the
// test transacts the conflicting writer. Competing writers prove they reached
// the conflicting lock through a bounded lock_wait_timeout session. Every
// phase checks committed snapshots and quota state through independent reads
// in both accounts, and the harness itself is hardened: catalog teardown runs
// through the still-open fixture connections with postcondition assertions,
// every async statement carries a cancellable context joined during cleanup,
// and every pacer releases its barrier unconditionally on any exit.

const (
	i30949PubAccount = "i30949p"
	i30949SubAccount = "i30949s"
	i30949PubDB      = "i30949pdb"
	i30949PubDB2     = "i30949pdb2"
	i30949SubDB      = "i30949sdb"
	i30949PubName    = "i30949pub"
	i30949PubName2   = "i30949pub2"
	i30949CtlDB      = "i30949ctl"
)

// i30949PublisherSnapshots is every snapshot name any phase can create under
// the publication owner identity; the fixture teardown drops all of them.
// sn_al2 is the single record a phase deliberately creates under the
// SUBSCRIBER (a local snapshot after the alias was replaced by a local
// database); it is dropped through the subscriber connection.
var i30949PublisherSnapshots = []string{
	"sn_green", "sn_ra1", "sn_ra2", "sn_wb1", "sn_ir1", "sn_rw1", "sn_it1",
	"sn_al1", "sn_al2", "sn_al3", "sn_fi", "sn_fh1", "sn_fh2",
	"sn_cc", "sn_cd", "sn_end",
}

func TestIssue30949PublicationSnapshotLifecycleAdmission(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		runIssue30949PublicationSnapshotLifecycle(t, c)
	})
}

type i30949Fixture struct {
	t   *testing.T
	ctx context.Context
	sys *sql.DB
	pub *sql.DB
	sub *sql.DB

	sysDSN string
	pubDSN string
	subDSN string

	pubAcctID int
	subAcctID int
}

func (f *i30949Fixture) exec(db *sql.DB, statement string) {
	f.t.Helper()
	execSQLRequire(f.t, f.ctx, db, statement)
}

func (f *i30949Fixture) count(db *sql.DB, query string) int {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, time.Minute)
	defer cancel()
	var n int
	require.NoError(f.t, db.QueryRowContext(ctx, query).Scan(&n))
	return n
}

// withSession runs fn on a fresh, dedicated handle so session-scoped
// variables (e.g. lock_wait_timeout=1 in the lock probes) are discarded with
// the physical connection: returning the probed connection to the fixture
// pool would leak the one-second budget into later pooled statements
// (observed: a default-budget writer surfacing Error 1205 in r13 run 2).
func (f *i30949Fixture) withSession(dsn string, fn func(*sql.Conn)) {
	f.t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(f.t, err)
	defer func() { require.NoError(f.t, db.Close()) }()
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Minute)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(f.t, err)
	defer func() { require.NoError(f.t, conn.Close()) }()
	fn(conn)
}

// snapshotsByName reads the per-account mo_snapshots relation of the account
// that owns db. Cross-account visibility was the bug class behind the earlier
// simulated tests, so each account reads only its own catalog.
func (f *i30949Fixture) snapshotsByName(db *sql.DB, sname string) int {
	return f.count(db,
		"select count(*) from mo_catalog.mo_snapshots where sname = '"+sname+"'")
}

// ownedRows reports the (sname, database_name, table_name) rows visible to
// the publication owner account; the alias form must record under the
// published-source identity exactly (design §4.4 step 1).
func (f *i30949Fixture) ownedRows(sname string) [][3]string {
	ctx, cancel := context.WithTimeout(f.ctx, time.Minute)
	defer cancel()
	rows, err := f.pub.QueryContext(ctx,
		"select sname, database_name, table_name from mo_catalog.mo_snapshots where sname = '"+sname+"'")
	require.NoError(f.t, err)
	defer rows.Close()
	var out [][3]string
	for rows.Next() {
		var triple [3]string
		require.NoError(f.t, rows.Scan(&triple[0], &triple[1], &triple[2]))
		out = append(out, triple)
	}
	require.NoError(f.t, rows.Err())
	return out
}

// pubSnapshotUsage is the publisher's committed snapshot usage surface (the
// quantity the SNAPSHOT quota limits).
func (f *i30949Fixture) pubSnapshotUsage() int {
	return f.count(f.pub, "select count(*) from mo_catalog.mo_snapshots")
}

// quotaRows reads the sys-owned feature-limit rows through the sys account:
// Q must bind the physical source owner (P_src), never the subscriber.
func (f *i30949Fixture) quotaRows(accountID int) [][2]int64 {
	ctx, cancel := context.WithTimeout(f.ctx, time.Minute)
	defer cancel()
	rows, err := f.sys.QueryContext(ctx, fmt.Sprintf(
		"select account_id, quota from mo_catalog.mo_feature_limit "+
			"where account_id = %d and feature_code = 'SNAPSHOT' order by scope", accountID))
	require.NoError(f.t, err)
	defer rows.Close()
	var out [][2]int64
	for rows.Next() {
		var acc, quota int64
		require.NoError(f.t, rows.Scan(&acc, &quota))
		out = append(out, [2]int64{acc, quota})
	}
	require.NoError(f.t, rows.Err())
	return out
}

type stmtOutcome struct{ err error }

// startAsync runs statement on db in its own goroutine under a cancellable
// child of the fixture context. The cleanup (registered at the start, so it
// runs before any earlier-registered pacer cleanup — LIFO) performs the
// abandon sequence: cancel the client context, kill the server session so a
// request parked inside the admission hook is server-cancelled BEFORE the
// pacer releases the barrier (otherwise the released admission could run to
// commit on a not-yet-cancelled context — observed as usage+1 in r13 run 2),
// then join the goroutine. The join waits on done rather than draining ch a
// second time: a phase that already consumed its outcome must not pay (and
// fail) a 45s timeout here.
func startAsync(t *testing.T, f *i30949Fixture, db *sql.DB, statement string) chan stmtOutcome {
	t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	ch := make(chan stmtOutcome, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := db.ExecContext(ctx, statement)
		ch <- stmtOutcome{err: err}
	}()
	t.Cleanup(func() {
		cancel()
		f.killStatement(t, statement)
		select {
		case <-done:
		case <-time.After(45 * time.Second):
			t.Errorf("async statement did not finish after cancellation: %s", statement)
		}
	})
	return ch
}

// killStatement server-cancels every session still executing statement. It
// exists for the abandon path (client-side cancel alone does not reach a
// request parked inside the admission hook deterministically); on any other
// path the statement has already finished and it is a no-op.
func (f *i30949Fixture) killStatement(t *testing.T, statement string) {
	t.Helper()
	probe := "select conn_id from information_schema.processlist where info = '" +
		strings.ReplaceAll(statement, "'", "''") + "'"
	// One bounded probe -> KILL pass: establish server-side cancellation on
	// every session still executing statement. Waiting for the processlist
	// row to disappear is NOT this helper's job: a request parked inside
	// the admission hook keeps its row until the pacer cleanup releases
	// the barrier (which runs after this cleanup by LIFO order), so the
	// post-release drain belongs to requireStatementDrained. A killed or
	// already-closing session reports an error from KILL (benign: the r13
	// run-2 commit-after-release incident came from releasing WITHOUT the
	// kill/closed session, and the residue assertions below verify the
	// outcome). On any other path the statement already finished and this
	// is a no-op.
	ctx, cancel := context.WithTimeout(f.ctx, time.Minute)
	// Consume and close every row BEFORE cancelling the query context:
	// cancelling first empties the result set (observed: an active
	// statement's row suppressed, its KILL skipped). The closure keeps the
	// deferred rows.Close ahead of the cancel below on every path.
	ids, readErr := func() (ids []int, readErr error) {
		rows, err := f.sys.QueryContext(ctx, probe)
		if err != nil {
			return nil, fmt.Errorf("probe %q: %w", probe, err)
		}
		defer func() {
			if cerr := rows.Close(); cerr != nil && readErr == nil {
				readErr = fmt.Errorf("close processlist rows: %w", cerr)
			}
		}()
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				return nil, fmt.Errorf("scan conn_id: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate processlist rows: %w", err)
		}
		return ids, nil
	}()
	cancel()
	if readErr != nil {
		// A failed probe cannot prove absence: mark the test but keep the
		// cleanup chain alive (this runs inside t.Cleanup, where FailNow
		// would skip the remaining releases).
		t.Errorf("killStatement read for %q failed: %v", statement, readErr)
		return
	}
	for _, id := range ids {
		kctx, kcancel := context.WithTimeout(f.ctx, 30*time.Second)
		_, err := f.sys.ExecContext(kctx, fmt.Sprintf("kill %d", id))
		kcancel()
		if err != nil {
			t.Logf("kill %d for %q reported (benign when the client cancel already closed the session): %v", id, statement, err)
		}
	}
}

// requireStatementDrained waits until no session is still executing (or
// rolling back) statement: the abandoned callback and its transaction must
// have terminated server-side before the residue assertions run, otherwise a
// released request could commit after the absence checks (the r13 reviewer
// scenario) and escape them.
func (f *i30949Fixture) requireStatementDrained(t *testing.T, statement string) {
	t.Helper()
	probe := "select count(*) from information_schema.processlist where info = '" +
		strings.ReplaceAll(statement, "'", "''") + "'"
	deadline := time.Now().Add(30 * time.Second)
	for {
		if f.count(f.sys, probe) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("statement never drained from the processlist: %s", statement)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func requireBlockedFor(t *testing.T, ch chan stmtOutcome, what string) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("%s finished with %v; expected it to stay blocked", what, got.err)
	case <-time.After(2600 * time.Millisecond):
	}
}

func requireCompletes(t *testing.T, ch chan stmtOutcome, what string) error {
	t.Helper()
	select {
	case got := <-ch:
		return got.err
	case <-time.After(90 * time.Second):
		t.Fatalf("%s did not finish", what)
		return nil
	}
}

// i30949GateWaiterSQL identifies the holder/waiter pair on the SNAPSHOT
// lifecycle gate (the mo_feature_registry row that create-snapshot admission
// holds Shared and publication DDL takes FOR UPDATE). mo_locks emits one row
// per (holder txn, waiter txn) pair on a contested key: txn_id holds the
// holder, lock_wait holds the waiter's txn, lock_content the key bytes
// (pkg/sql/colexec/table_function/system_view.go moLocksState.start;
// lock_key is the literal 'point'/'range', so the identity filter is the
// table plus the shape of the pair). An unrelated waiter cannot satisfy
// this: it would have to wait on the very gate-table key against a live
// holder, and this phase runs no other snapshot admission or publication DDL
// (the control statements touch only user databases — the holder/waiter txn
// graph for this phase was inspected live in r13: holder = the parked
// create's admission txn, waiter = the revoke).
const i30949GateWaiterSQL = `
select txn_id, lock_wait, lock_content, lock_mode, lock_status
from mo_catalog.mo_locks
where txn_id <> ''
  and lock_wait <> ''
  and table_id in (
      select cast(rel_id as char) from mo_catalog.mo_tables
      where reldatabase = 'mo_catalog'
        and relname = 'mo_feature_registry')`

// requireGateWaiters polls until the gate holder/waiter pair exists and
// returns the identified waiter transaction ids for the post-release check.
func (f *i30949Fixture) requireGateWaiters(t *testing.T, what string) []string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(f.ctx, time.Minute)
		txns := func() []string {
			rows, err := f.sys.QueryContext(ctx, i30949GateWaiterSQL)
			require.NoError(t, err, "gate waiter probe")
			defer func() { require.NoError(t, rows.Close(), "gate waiter close") }()
			var txns []string
			for rows.Next() {
				var holder, waiter, content, mode, status string
				require.NoError(t, rows.Scan(&holder, &waiter, &content, &mode, &status), "gate waiter scan")
				txns = append(txns, waiter)
				t.Logf("gate pair: holder=%s waiter=%s key=%s mode=%s status=%s (%s)",
					holder, waiter, content, mode, status, what)
			}
			require.NoError(t, rows.Err(), "gate waiter iteration")
			return txns
		}()
		cancel()
		if len(txns) > 0 {
			return txns
		}
		if time.Now().After(deadline) {
			// Dump the raw waiter surface so a structural miss is diagnosable
			// from the test log rather than requiring a live reproduction.
			dctx, dcancel := context.WithTimeout(f.ctx, time.Minute)
			func() {
				drows, derr := f.sys.QueryContext(dctx,
					"select txn_id, lock_wait, table_id, lock_content, lock_mode, lock_status from mo_catalog.mo_locks where lock_wait <> ''")
				if derr != nil {
					t.Logf("unmatched waiter dump failed: %v", derr)
					return
				}
				defer func() {
					if cerr := drows.Close(); cerr != nil {
						t.Logf("unmatched waiter dump close: %v", cerr)
					}
				}()
				for drows.Next() {
					var txn, wait, tbl, content, mode, status string
					if serr := drows.Scan(&txn, &wait, &tbl, &content, &mode, &status); serr == nil {
						t.Logf("unmatched waiter row: txn=%s wait=%s table=%s key=%s mode=%s status=%s",
							txn, wait, tbl, content, mode, status)
					}
				}
				if rerr := drows.Err(); rerr != nil {
					t.Logf("unmatched waiter dump iteration: %v", rerr)
				}
			}()
			dcancel()
			t.Fatalf("%s never appeared as a waiter queued on the held SNAPSHOT gate key", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// requireGateWaitersGone verifies the identified waiter transactions no
// longer hold or wait on any lock: the revoke that was queued on the gate
// key completed after the parked admission released its grants and released
// its own in turn.
func (f *i30949Fixture) requireGateWaitersGone(t *testing.T, txns []string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for _, id := range txns {
		probe := "select count(*) from mo_catalog.mo_locks where txn_id = '" + id + "'"
		for {
			if f.count(f.sys, probe) == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("identified gate waiter txn %s still holds or waits on locks", id)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// admissionPacer installs the process-local admission hook and parks the
// first lock event that matches label exactly ("<relation>#<owner>#<mode>")
// at the requested boundary: acquired=false parks before the physical
// acquisition, acquired=true parks right after the grant while every earlier
// key of the admission is still held. Later events pass through untouched.
// The cleanup releases the barrier unconditionally and idempotently before
// restoring the hook, so no exit path can leave a parked callback (and the
// transaction locks it holds) blocked in the server.
type admissionPacer struct {
	label       string
	acquired    bool
	release     chan struct{}
	fired       chan struct{}
	fireOnce    sync.Once
	releaseOnce sync.Once
	restore     func()
}

func newAdmissionPacer(t *testing.T, label string, acquired bool) *admissionPacer {
	t.Helper()
	p := &admissionPacer{
		label:    label,
		acquired: acquired,
		release:  make(chan struct{}),
		fired:    make(chan struct{}),
	}
	p.restore = frontend.SetLifecycleAdmissionLockEventHookForTest(func(l string, done bool) {
		if l == p.label && done == p.acquired {
			p.fireOnce.Do(func() {
				close(p.fired)
				<-p.release
			})
		}
	})
	t.Cleanup(func() {
		p.releaseOnce.Do(func() { close(p.release) })
		p.restore()
	})
	return p
}

// waitFired requires the admission to reach the paced boundary.
func (p *admissionPacer) waitFired(t *testing.T, what string) {
	t.Helper()
	select {
	case <-p.fired:
	case <-time.After(60 * time.Second):
		t.Fatalf("%s never reached the paced admission boundary %s", what, p.label)
	}
}

// resume releases the parked admission; it must be called only after
// waitFired observed the parked boundary.
func (p *admissionPacer) resume(t *testing.T) {
	t.Helper()
	select {
	case <-p.fired:
	default:
		t.Fatalf("resume before the paced admission boundary %s was reached", p.label)
	}
	p.releaseOnce.Do(func() { close(p.release) })
}

func (f *i30949Fixture) aliasLabel(mode string) string {
	return fmt.Sprintf("mo_database#%d#%s", f.subAcctID, mode)
}

// i30949IdentityLabel is the admission's FIRST physical boundary (the
// feature-registry catalog identity row in beginLifecycleRCAdmission), fired
// BEFORE the SNAPSHOT feature-gate key. Parking there leaves the gate free:
// ALTER/DROP/CREATE PUBLICATION takes that gate FOR UPDATE through
// catalog.SnapshotLifecycleGateSQL (lockViewMetadataLifecycle), so a park at
// any later boundary would make the publication writer block on the held
// shared gate instead of committing (verified: r13 run wedged this way, with
// mo_locks showing the writer queued on the gate row). A statement paused at
// the identity boundary sits exactly in the design v7 4.4 step-5 window
// between the provisional classification read and the first admission lock,
// so after the writer commits the resumed locked-row re-reads must catch the
// drift and fail closed.
func (f *i30949Fixture) i30949IdentityLabel() string { return "mo_tables#0#Shared" }

// i30949PubsGrantedLabel is the admission's mo_pubs key post-grant. Parked
// there, the statement transaction still holds the SNAPSHOT feature-gate
// shared, so a racing ALTER/DROP PUBLICATION queues on the gate until the
// statement ends — the complementary drift window, where the create commits
// first and the revocation lands afterwards.
func (f *i30949Fixture) i30949PubsGrantedLabel() string { return "mo_pubs#0#Shared" }

func (f *i30949Fixture) resubscribe() {
	f.exec(f.sub, "create database "+i30949SubDB+" from "+i30949PubAccount+" publication "+i30949PubName)
	require.Equal(f.t, 2, f.count(f.sub, "select count(*) from "+i30949SubDB+".t"))
}

// requireNoResidue asserts the rejected/cancelled statement wrote nothing in
// either account and consumed no usage; quota rows persist by design.
func (f *i30949Fixture) requireNoResidue(sname string, usageBaseline int) {
	f.t.Helper()
	require.Empty(f.t, f.ownedRows(sname), "no record may be written under the publication owner")
	require.Equal(f.t, 0, f.snapshotsByName(f.sub, sname), "no record may be written under the subscriber")
	require.Equal(f.t, usageBaseline, f.pubSnapshotUsage(), "no quota usage may accrue")
}

// requireWriterFree proves no leaked grant survives: an exclusive writer on
// the alias database with a bounded lock budget must complete immediately.
func (f *i30949Fixture) requireAliasWritable() {
	f.t.Helper()
	f.withSession(f.subDSN, func(conn *sql.Conn) {
		_, err := conn.ExecContext(f.ctx, "set session lock_wait_timeout = 10")
		require.NoError(f.t, err)
		started := time.Now()
		_, err = conn.ExecContext(f.ctx, "drop database "+i30949SubDB)
		require.NoError(f.t, err, "an exclusive writer must not queue behind a leaked alias grant")
		require.Less(f.t, time.Since(started), 30*time.Second)
	})
	require.Zero(f.t, f.count(f.sub,
		"select count(*) from mo_catalog.mo_database where datname = '"+i30949SubDB+"'"),
		"the alias database is dropped")
}

func runIssue30949PublicationSnapshotLifecycle(t *testing.T, c embed.Cluster) {
	t.Helper()
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	port := cn.GetServiceConfig().CN.Frontend.Port

	wrapCtx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	open := func(dsn string) *sql.DB {
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		return db
	}
	f := &i30949Fixture{t: t, ctx: wrapCtx}
	f.sysDSN = fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port)
	f.pubDSN = fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", i30949PubAccount, port)
	f.subDSN = fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", i30949SubAccount, port)
	f.sys = open(f.sysDSN)
	f.pub = open(f.pubDSN)
	f.sub = open(f.subDSN)

	// Teardown runs while the fixture connections are still open (the
	// handles close at the END of this cleanup, never before it), reports
	// every failed statement, and asserts the catalog postconditions. It is
	// registered immediately after resource acquisition so even an early
	// setup failure leaves generation i30949* clean for a repeated run.
	t.Cleanup(func() {
		ctx, cleanupCancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cleanupCancel()
		var teardownErrs []error
		cleanupExec := func(db *sql.DB, statement string) {
			_, err := db.ExecContext(ctx, statement)
			if err != nil {
				t.Logf("cleanup statement failed (reported, continuing): %s: %v", statement, err)
				teardownErrs = append(teardownErrs, fmt.Errorf("%s: %w", statement, err))
			}
		}
		countOrNeg := func(db *sql.DB, query string) int {
			ctx, c2 := context.WithTimeout(ctx, 2*time.Minute)
			defer c2()
			var n int
			if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
				t.Logf("cleanup postcondition query failed: %s: %v", query, err)
				teardownErrs = append(teardownErrs, err)
				return -1
			}
			return n
		}

		cleanupExec(f.sub, "drop database if exists "+i30949SubDB)
		cleanupExec(f.sub, "drop database if exists "+i30949CtlDB)
		cleanupExec(f.sub, "drop snapshot if exists sn_al2")
		for _, sname := range i30949PublisherSnapshots {
			cleanupExec(f.pub, "drop snapshot if exists "+sname)
		}
		cleanupExec(f.pub, "drop publication if exists "+i30949PubName)
		cleanupExec(f.pub, "drop publication if exists "+i30949PubName2)
		cleanupExec(f.pub, "drop database if exists "+i30949PubDB)
		cleanupExec(f.pub, "drop database if exists "+i30949PubDB2)
		// Postconditions are asserted through the still-open connections
		// before the accounts (and their catalogs) go away.
		if got := countOrNeg(f.sys, fmt.Sprintf(
			"select count(*) from mo_catalog.mo_pubs where account_name = '%s' and pub_name in ('%s','%s')",
			i30949PubAccount, i30949PubName, i30949PubName2)); got != 0 {
			t.Errorf("teardown postcondition: %d publication rows left", got)
		}
		if got := countOrNeg(f.pub,
			"select count(*) from mo_catalog.mo_snapshots where sname in ("+
				"'sn_green','sn_ra1','sn_ra2','sn_wb1','sn_ir1','sn_rw1','sn_it1',"+
				"'sn_al1','sn_al3','sn_fi','sn_fh1','sn_fh2','sn_cc','sn_cd','sn_end')"); got != 0 {
			t.Errorf("teardown postcondition: %d snapshot records left under the publisher", got)
		}
		if got := countOrNeg(f.sub,
			"select count(*) from mo_catalog.mo_snapshots where sname = 'sn_al2'"); got != 0 {
			t.Errorf("teardown postcondition: %d snapshot records left under the subscriber", got)
		}
		cleanupExec(f.sys, "drop account if exists "+i30949SubAccount)
		cleanupExec(f.sys, "drop account if exists "+i30949PubAccount)
		if got := countOrNeg(f.sys, fmt.Sprintf(
			"select count(*) from mo_catalog.mo_account where account_name in ('%s','%s')",
			i30949PubAccount, i30949SubAccount)); got != 0 {
			t.Errorf("teardown postcondition: %d account rows left", got)
		}
		require.NoError(t, errors.Join(teardownErrs...), "catalog teardown reported failures")
		if err := f.sys.Close(); err != nil {
			t.Errorf("close sys handle: %v", err)
		}
		if err := f.pub.Close(); err != nil {
			t.Errorf("close pub handle: %v", err)
		}
		if err := f.sub.Close(); err != nil {
			t.Errorf("close sub handle: %v", err)
		}
	})

	f.sys.SetMaxOpenConns(4)
	f.pub.SetMaxOpenConns(3)
	f.sub.SetMaxOpenConns(3)
	execSQLRequire(t, wrapCtx, f.sys, "set role moadmin")
	require.NoError(t, testutils.WaitSystemBootstrap(wrapCtx, f.sys))

	// Defensive drop-if-exists keeps a repeated in-process execution
	// (go test -count=2 over the shared cluster) independent of the previous
	// generation even if its teardown was interrupted.
	f.exec(f.sys, "drop account if exists "+i30949SubAccount)
	f.exec(f.sys, "drop account if exists "+i30949PubAccount)
	f.exec(f.sys, "create account "+i30949PubAccount+" admin_name = 'root' identified by '111'")
	f.exec(f.sys, "create account "+i30949SubAccount+" admin_name = 'root' identified by '111'")

	f.pubAcctID = f.count(f.sys,
		"select account_id from mo_catalog.mo_account where account_name = '"+i30949PubAccount+"'")
	f.subAcctID = f.count(f.sys,
		"select account_id from mo_catalog.mo_account where account_name = '"+i30949SubAccount+"'")

	f.exec(f.pub, "create database "+i30949PubDB)
	f.exec(f.pub, "create table "+i30949PubDB+".t (a int primary key, b varchar(16))")
	f.exec(f.pub, "insert into "+i30949PubDB+".t values (1,'x'),(2,'y')")
	f.exec(f.pub, "create database "+i30949PubDB2)
	f.exec(f.pub, "create table "+i30949PubDB2+".t (a int primary key, b varchar(16))")
	f.exec(f.pub, "create publication "+i30949PubName+" database "+i30949PubDB+" account "+i30949SubAccount)
	f.exec(f.sub, "create database "+i30949SubDB+" from "+i30949PubAccount+" publication "+i30949PubName)
	require.Equal(t, 2, f.count(f.sub, "select count(*) from "+i30949SubDB+".t"))

	// newHeld pins one server session (the engine accepts DROP DATABASE
	// inside an explicit transaction even though snapshot DDL is
	// transaction-gated). The connection context is the cancellable fixture
	// context, and t.Cleanup rolls back an abandoned transaction by closing.
	newHeld := func(t *testing.T, db *sql.DB) *sql.Conn {
		t.Helper()
		conn, err := db.Conn(f.ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(f.ctx, "begin")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = conn.ExecContext(context.Background(), "rollback")
			_ = conn.Close()
		})
		return conn
	}

	t.Run("alias create binds under the publisher identity and routed drop removes it", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		f.exec(f.sub, "create snapshot sn_green for table "+i30949SubDB+" t")
		require.Equal(t, [][3]string{{"sn_green", i30949PubDB, "t"}}, f.ownedRows("sn_green"),
			"the alias form records under the published-source owner identity")
		require.Equal(t, 0, f.snapshotsByName(f.sub, "sn_green"))
		require.Len(t, f.quotaRows(f.pubAcctID), 1,
			"the admission quota step initializes the SNAPSHOT limit under the physical owner")
		require.Empty(t, f.quotaRows(f.subAcctID),
			"Q binds P_src: no SNAPSHOT quota row is created under the subscriber")
		require.Equal(t, usage0+1, f.pubSnapshotUsage())
		f.exec(f.sub, "drop snapshot sn_green from "+i30949PubAccount+" publication "+i30949PubName)
		require.Empty(t, f.ownedRows("sn_green"))
		require.Equal(t, usage0, f.pubSnapshotUsage())
	})

	t.Run("rebind committed before admission acquisition fails closed", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		pacer := newAdmissionPacer(t, f.aliasLabel("Shared"), false)
		create := startAsync(t, f, f.sub, "create snapshot sn_ra1 for table "+i30949SubDB+" t")
		pacer.waitFired(t, "create snapshot sn_ra1")

		// The rebind lands completely before the admission's first key: the
		// binding N referenced by the create no longer exists.
		f.exec(f.sub, "drop database "+i30949SubDB)
		pacer.resume(t)
		err := requireCompletes(t, create, "create snapshot sn_ra1")
		require.Error(t, err, "classification must reject the subscription alias after the rebind")

		f.requireNoResidue("sn_ra1", usage0)
		require.Len(t, f.quotaRows(f.pubAcctID), 1, "the quota row survives a rejected admission")
		f.resubscribe()
	})

	t.Run("rebind rolled back before admission acquisition restores the binding", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		held := newHeld(t, f.sub)
		_, err := held.ExecContext(f.ctx, "drop database "+i30949SubDB)
		require.NoError(t, err, "database DDL is transactional in this engine")

		pacer := newAdmissionPacer(t, f.aliasLabel("Shared"), false)
		create := startAsync(t, f, f.sub, "create snapshot sn_ra2 for table "+i30949SubDB+" t")
		pacer.waitFired(t, "create snapshot sn_ra2")

		_, err = held.ExecContext(f.ctx, "rollback")
		require.NoError(t, err, "rollback restores the binding the parked admission classified against")
		pacer.resume(t)
		require.NoError(t, requireCompletes(t, create, "create snapshot sn_ra2"),
			"the admission proceeds against the restored binding (lock release after rollback)")

		require.Equal(t, [][3]string{{"sn_ra2", i30949PubDB, "t"}}, f.ownedRows("sn_ra2"))
		require.Equal(t, 0, f.snapshotsByName(f.sub, "sn_ra2"))
		require.Equal(t, usage0+1, f.pubSnapshotUsage())
		f.exec(f.sub, "drop snapshot sn_ra2 from "+i30949PubAccount+" publication "+i30949PubName)
		require.Equal(t, usage0, f.pubSnapshotUsage())
	})

	t.Run("rebind writer reaches the conflicting lock and is blocked until the alias grant is released", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		// The admission parks immediately after granting the alias key A
		// shared: every earlier grant stays held inside its transaction.
		pacer := newAdmissionPacer(t, f.aliasLabel("Shared"), true)
		create := startAsync(t, f, f.sub, "create snapshot sn_wb1 for table "+i30949SubDB+" t")
		pacer.waitFired(t, "create snapshot sn_wb1")

		// Probe 1 observes the competing writer's lock attempt: with a
		// one-second lock budget the torn-down database write must fail with
		// a lock wait timeout (it reached the held alias key and queued), it
		// must NOT succeed, and it must NOT fail before its budget expired.
		f.withSession(f.subDSN, func(conn *sql.Conn) {
			_, err := conn.ExecContext(f.ctx, "set session lock_wait_timeout = 1")
			require.NoError(t, err)
			started := time.Now()
			_, err = conn.ExecContext(f.ctx, "drop database "+i30949SubDB)
			require.Error(t, err, "the conflicting writer must not complete while the alias key is held")
			require.Contains(t, err.Error(), "Lock wait timeout exceeded",
				"the writer reached the conflicting lock and exhausted its budget")
			require.GreaterOrEqual(t, time.Since(started), 800*time.Millisecond,
				"the error came after the lock budget, proving a real lock wait")
		})

		// Probe 2: a default-budget writer stays blocked (a saturated host
		// can only lengthen this window), while an unrelated statement
		// completes promptly in the same window, proving the block is the
		// held alias key rather than server slowness.
		writer := startAsync(t, f, f.sub, "drop database "+i30949SubDB)
		requireBlockedFor(t, writer, "drop database "+i30949SubDB+
			" queued on the held shared alias key")
		f.exec(f.sub, "create database "+i30949CtlDB)
		f.exec(f.sub, "drop database "+i30949CtlDB)

		pacer.resume(t)
		require.NoError(t, requireCompletes(t, create, "create snapshot sn_wb1"),
			"the parked create finishes first: it already held A")
		require.Equal(t, [][3]string{{"sn_wb1", i30949PubDB, "t"}}, f.ownedRows("sn_wb1"),
			"the committed record lands under the publisher before the writer proceeds")
		require.Equal(t, usage0+1, f.pubSnapshotUsage())

		require.NoError(t, requireCompletes(t, writer, "drop database "+i30949SubDB),
			"the queued writer resumes once the holding statement ends")
		require.Zero(t, f.count(f.sub,
			"select count(*) from mo_catalog.mo_database where datname = '"+i30949SubDB+"'"),
			"the alias binding is dropped after the admission released A")
		require.Equal(t, [][3]string{{"sn_wb1", i30949PubDB, "t"}}, f.ownedRows("sn_wb1"),
			"the record survives the alias drop: it belongs to the published-source owner")
		f.resubscribe()
		f.exec(f.sub, "drop snapshot sn_wb1 from "+i30949PubAccount+" publication "+i30949PubName)
		require.Equal(t, usage0, f.pubSnapshotUsage())
	})

	// The next two phases simulate assertion failures while a request is
	// parked: the phase returns without resume, so the harness cleanups must
	// cancel the statement, join its goroutine, then release the barrier and
	// restore the hook (startAsync cleanup registered after newAdmissionPacer
	// runs first, and it cancels and joins before the pacer releases).
	var abandonedPreUsage int
	fh1Stmt := "create snapshot sn_fh1 for table " + i30949SubDB + " t"
	t.Run("cleanup releases a request parked before acquisition", func(t *testing.T) {
		abandonedPreUsage = f.pubSnapshotUsage()
		pacer := newAdmissionPacer(t, f.aliasLabel("Shared"), false)
		_ = startAsync(t, f, f.sub, fh1Stmt)
		pacer.waitFired(t, "create snapshot sn_fh1")
		// No resume: the subtest returns here as if an assertion had failed
		// with the request still parked before its first key.
	})
	t.Run("a cancelled pre-acquisition park leaves no record, no usage and no held lock", func(t *testing.T) {
		// The previous phase's cleanups already cancelled, server-killed,
		// joined and released; the abandoned callback and its transaction
		// must have terminated before any absence assertion — otherwise a
		// released request could commit after the checks and escape them.
		f.requireStatementDrained(t, fh1Stmt)
		f.requireNoResidue("sn_fh1", abandonedPreUsage)
		f.requireAliasWritable()
		// Re-assert after the exclusive writer proved lock release.
		f.requireNoResidue("sn_fh1", abandonedPreUsage)
		f.resubscribe()
	})

	var abandonedPostUsage int
	fh2Stmt := "create snapshot sn_fh2 for table " + i30949SubDB + " t"
	t.Run("cleanup releases a request parked after the alias grant", func(t *testing.T) {
		abandonedPostUsage = f.pubSnapshotUsage()
		pacer := newAdmissionPacer(t, f.aliasLabel("Shared"), true)
		_ = startAsync(t, f, f.sub, fh2Stmt)
		pacer.waitFired(t, "create snapshot sn_fh2")
		// No resume: returns with the alias key still granted to the parked
		// admission (its transaction is aborted by the cancellation).
	})
	t.Run("a cancelled post-grant park leaves no record, no usage and no held lock", func(t *testing.T) {
		f.requireStatementDrained(t, fh2Stmt)
		f.requireNoResidue("sn_fh2", abandonedPostUsage)
		f.requireAliasWritable()
		// Re-assert after the exclusive writer proved lock release.
		f.requireNoResidue("sn_fh2", abandonedPostUsage)
		f.resubscribe()
	})

	t.Run("publication revoked before the first admission lock fails closed", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		// The create is parked between classification and its first
		// admission lock, so the revoke (which takes the SNAPSHOT gate FOR
		// UPDATE) commits first; the resumed locked-row revalidation must
		// reject the drift (design §4.4 step 5), not bind a detached source.
		pacer := newAdmissionPacer(t, f.i30949IdentityLabel(), false)
		create := startAsync(t, f, f.sub, "create snapshot sn_ir1 for table "+i30949SubDB+" t")
		pacer.waitFired(t, "create snapshot sn_ir1")
		f.exec(f.pub, "drop publication "+i30949PubName)
		pacer.resume(t)
		require.Error(t, requireCompletes(t, create, "create snapshot sn_ir1"),
			"the parked create must reject the publication revoked during its admission")

		f.requireNoResidue("sn_ir1", usage0)
		require.Len(t, f.quotaRows(f.pubAcctID), 1, "the quota row survives a rejected admission")

		// Healing: recreating the publication restores the existing binding's
		// view, and a fresh create binds the restored source.
		f.exec(f.pub, "create publication "+i30949PubName+" database "+i30949PubDB+" account "+i30949SubAccount)
		require.Equal(t, 2, f.count(f.sub, "select count(*) from "+i30949SubDB+".t"),
			"the existing subscription binding keeps resolving after the publication is recreated")
		f.exec(f.sub, "create snapshot sn_ir1 for table "+i30949SubDB+" t")
		require.Equal(t, [][3]string{{"sn_ir1", i30949PubDB, "t"}}, f.ownedRows("sn_ir1"),
			"a fresh create after the healed publication binds the published source")
		require.Equal(t, usage0+1, f.pubSnapshotUsage())
		f.exec(f.sub, "drop snapshot sn_ir1 from "+i30949PubAccount+" publication "+i30949PubName)
		require.Equal(t, usage0, f.pubSnapshotUsage())
	})

	t.Run("a racing revoke blocks on the held lifecycle gate and lands after the create", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		// Parked post-grant on the mo_pubs key, the create's statement
		// transaction still holds the SNAPSHOT feature-gate shared. A racing
		// DROP PUBLICATION takes that gate FOR UPDATE (catalog
		// .SnapshotLifecycleGateSQL via lockViewMetadataLifecycle), so it can
		// only commit after this statement ends: the complementary drift
		// window, where the create is validated against the still-published
		// locked row and the revocation lands afterwards.
		pacer := newAdmissionPacer(t, f.i30949PubsGrantedLabel(), true)
		create := startAsync(t, f, f.sub, "create snapshot sn_rw1 for table "+i30949SubDB+" t")
		pacer.waitFired(t, "create snapshot sn_rw1")

		// The competing writer demonstrably reaches the conflicting lock: a
		// default-budget revoke stays queued (a saturated host can only
		// lengthen this) and the gate-key holder/waiter graph exists — its
		// waiter transaction is joined to the parked admission's live Shared
		// grant on the same SNAPSHOT gate key, and an unrelated statement
		// completes promptly. (The revoked statement's gate FOR UPDATE runs
		// in its internal session, which does not inherit a client
		// lock_wait_timeout, so the keyed waiter graph — not a budget
		// error — is the observable.)
		dropper := startAsync(t, f, f.pub, "drop publication "+i30949PubName)
		requireBlockedFor(t, dropper, "drop publication "+i30949PubName+" queued on the held gate")
		f.exec(f.sub, "create database "+i30949CtlDB)
		f.exec(f.sub, "drop database "+i30949CtlDB)
		gateWaiters := f.requireGateWaiters(t, "drop publication "+i30949PubName)

		pacer.resume(t)
		require.NoError(t, requireCompletes(t, create, "create snapshot sn_rw1"),
			"the parked create finishes: its locked-row read saw the still-published row")
		require.Equal(t, [][3]string{{"sn_rw1", i30949PubDB, "t"}}, f.ownedRows("sn_rw1"),
			"the committed record binds the publisher source before the revocation lands")
		require.Equal(t, usage0+1, f.pubSnapshotUsage())

		require.NoError(t, requireCompletes(t, dropper, "drop publication "+i30949PubName),
			"the identified queued revoke completes after the parked admission releases its grants")
		f.requireGateWaitersGone(t, gateWaiters)
		require.Equal(t, [][3]string{{"sn_rw1", i30949PubDB, "t"}}, f.ownedRows("sn_rw1"),
			"the record stays under the source owner after the revoke")

		// The source owner's plain drop removes the record orphaned by the
		// revoke; then the fixture surface is restored for later phases.
		f.exec(f.pub, "drop snapshot sn_rw1")
		require.Equal(t, usage0, f.pubSnapshotUsage())
		f.exec(f.pub, "create publication "+i30949PubName+" database "+i30949PubDB+" account "+i30949SubAccount)
		require.Equal(t, 2, f.count(f.sub, "select count(*) from "+i30949SubDB+".t"),
			"recreating the publication heals the surviving alias binding")
	})

	t.Run("publication retargeted before the first admission lock fails closed", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		// Parked between classification and the first admission lock (the
		// retarget's gate FOR UPDATE is free to commit), the resumed
		// locked-row revalidation must reject the persisted-identity drift.
		pacer := newAdmissionPacer(t, f.i30949IdentityLabel(), false)
		create := startAsync(t, f, f.sub, "create snapshot sn_it1 for table "+i30949SubDB+" t")
		pacer.waitFired(t, "create snapshot sn_it1")
		f.exec(f.pub, "alter publication "+i30949PubName+" database "+i30949PubDB2)
		pacer.resume(t)
		require.Error(t, requireCompletes(t, create, "create snapshot sn_it1"),
			"the parked create must reject the publication retargeted during its admission")

		f.requireNoResidue("sn_it1", usage0)

		// A statement that starts after the committed retarget classifies
		// against the new scope (step 5 follow-through): alias view and
		// record bind the same new source.
		require.Equal(t, 0, f.count(f.sub, "select count(*) from "+i30949SubDB+".t"),
			"the subscription view follows the retargeted publication scope (pdb2 is empty)")
		f.exec(f.sub, "create snapshot sn_it1 for table "+i30949SubDB+" t")
		require.Equal(t, [][3]string{{"sn_it1", i30949PubDB2, "t"}}, f.ownedRows("sn_it1"),
			"the record binds the current published source exactly as the alias view does")
		require.Equal(t, usage0+1, f.pubSnapshotUsage())
		f.exec(f.sub, "drop snapshot sn_it1 from "+i30949PubAccount+" publication "+i30949PubName)
		require.Equal(t, usage0, f.pubSnapshotUsage())

		f.exec(f.pub, "alter publication "+i30949PubName+" database "+i30949PubDB)
		require.Equal(t, 1, f.count(f.sys,
			"select count(*) from mo_catalog.mo_pubs where pub_name = '"+i30949PubName+
				"' and database_name = '"+i30949PubDB+"'"),
			"the publication is restored for the remaining phases")
		require.Equal(t, 2, f.count(f.sub, "select count(*) from "+i30949SubDB+".t"),
			"restoring the publication restores the binding's view")
	})

	t.Run("alias replaced by a local database before the first admission lock fails closed", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		pacer := newAdmissionPacer(t, f.i30949IdentityLabel(), false)
		create := startAsync(t, f, f.sub, "create snapshot sn_al1 for table "+i30949SubDB+" t")
		pacer.waitFired(t, "create snapshot sn_al1")
		// The alias N is dropped and re-created as an ordinary local
		// database while the create is parked between classification and its
		// first admission lock (design §4.4 step 5, finding 12).
		f.exec(f.sub, "drop database "+i30949SubDB)
		f.exec(f.sub, "create database "+i30949SubDB)
		f.exec(f.sub, "create table "+i30949SubDB+".t (a int)")
		f.exec(f.sub, "insert into "+i30949SubDB+".t values (7)")
		pacer.resume(t)
		require.Error(t, requireCompletes(t, create, "create snapshot sn_al1"),
			"the parked create must reject the alias that was replaced by a local database")

		f.requireNoResidue("sn_al1", usage0)

		// Classification follows the current binding: a fresh create on the
		// local database is a LOCAL snapshot recorded under the subscriber.
		f.exec(f.sub, "create snapshot sn_al2 for table "+i30949SubDB+" t")
		require.Equal(t, 1, f.snapshotsByName(f.sub, "sn_al2"),
			"the local replacement records under the subscriber account")
		require.Empty(t, f.ownedRows("sn_al2"),
			"no record may appear under the publication owner for the local replacement")
		require.Equal(t, usage0, f.pubSnapshotUsage(), "the publisher accrues no usage for a local snapshot")
		require.Len(t, f.quotaRows(f.subAcctID), 1,
			"the local snapshot initializes the SNAPSHOT quota row under its true owner (the subscriber)")
		f.exec(f.sub, "drop snapshot sn_al2")
		require.Equal(t, 0, f.snapshotsByName(f.sub, "sn_al2"))

		f.exec(f.sub, "drop database "+i30949SubDB)
		f.resubscribe()
	})

	t.Run("alias replaced by another publication before the first admission lock fails closed", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		pacer := newAdmissionPacer(t, f.i30949IdentityLabel(), false)
		create := startAsync(t, f, f.sub, "create snapshot sn_al3 for table "+i30949SubDB+" t")
		pacer.waitFired(t, "create snapshot sn_al3")
		// The alias N is dropped and re-created from a DIFFERENT publication
		// (pub2 over pdb2) while the create is parked between classification
		// and its first admission lock; CREATE PUBLICATION pub2 takes the
		// SNAPSHOT gate FOR UPDATE, which a pre-gate park leaves free.
		f.exec(f.sub, "drop database "+i30949SubDB)
		f.exec(f.pub, "create publication "+i30949PubName2+" database "+i30949PubDB2+" account "+i30949SubAccount)
		f.exec(f.sub, "create database "+i30949SubDB+" from "+i30949PubAccount+" publication "+i30949PubName2)
		pacer.resume(t)
		require.Error(t, requireCompletes(t, create, "create snapshot sn_al3"),
			"the parked create must reject the alias that was rebound to another publication")

		f.requireNoResidue("sn_al3", usage0)

		// A fresh create follows the new binding: pub2/pdb2 consistently.
		f.exec(f.sub, "create snapshot sn_al3 for table "+i30949SubDB+" t")
		require.Equal(t, [][3]string{{"sn_al3", i30949PubDB2, "t"}}, f.ownedRows("sn_al3"),
			"the record binds the new publication's source exactly as the alias view does")
		require.Equal(t, usage0+1, f.pubSnapshotUsage())
		f.exec(f.sub, "drop snapshot sn_al3 from "+i30949PubAccount+" publication "+i30949PubName2)
		require.Equal(t, usage0, f.pubSnapshotUsage())

		f.exec(f.sub, "drop database "+i30949SubDB)
		f.exec(f.pub, "drop publication "+i30949PubName2)
		f.resubscribe()
	})

	t.Run("a failed record insert rolls back admission, quota and locks", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		// A real insertion failure injected at the production record-write
		// point (test-only seam): the statement transaction must roll back
		// its admission locks, its quota bump and its record write together.
		restore := frontend.SetSnapshotRecordWriteFaultHookForTest(func() error {
			return errors.New("injected snapshot record insert failure")
		})
		_, err := f.sub.ExecContext(f.ctx, "create snapshot sn_fi for table "+i30949SubDB+" t")
		restore()
		require.Error(t, err, "the armed fault rejects the create")
		require.Contains(t, err.Error(), "injected snapshot record insert failure")

		f.requireNoResidue("sn_fi", usage0)
		require.Len(t, f.quotaRows(f.pubAcctID), 1, "the quota row survives the rolled-back admission")

		// Failure-atomicity and lock release, proven by the immediate clean
		// retry: any leaked grant or quota half-write would block or double.
		f.exec(f.sub, "create snapshot sn_fi for table "+i30949SubDB+" t")
		require.Equal(t, [][3]string{{"sn_fi", i30949PubDB, "t"}}, f.ownedRows("sn_fi"))
		require.Equal(t, usage0+1, f.pubSnapshotUsage())
		f.exec(f.sub, "drop snapshot sn_fi from "+i30949PubAccount+" publication "+i30949PubName)
		require.Equal(t, usage0, f.pubSnapshotUsage())
	})

	t.Run("concurrent same-name creates commit exactly one record", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		first := startAsync(t, f, f.sub, "create snapshot sn_cc for table "+i30949SubDB+" t")
		second := startAsync(t, f, f.sub, "create snapshot sn_cc for table "+i30949SubDB+" t")
		errs := []error{
			requireCompletes(t, first, "first same-name create"),
			requireCompletes(t, second, "second same-name create"),
		}
		ok, failed := 0, 0
		for _, err := range errs {
			if err == nil {
				ok++
			} else {
				failed++
				require.Contains(t, err.Error(), "already exists",
					"the serialized loser must surface the duplicate-name contract")
			}
		}
		require.Equal(t, 1, ok, "exactly one same-name create commits")
		require.Equal(t, 1, failed, "exactly one same-name create fails")
		require.Equal(t, [][3]string{{"sn_cc", i30949PubDB, "t"}}, f.ownedRows("sn_cc"))
		require.Equal(t, 0, f.snapshotsByName(f.sub, "sn_cc"))
		require.Equal(t, usage0+1, f.pubSnapshotUsage(),
			"the loser committed no record and no quota usage")
	})

	t.Run("concurrent publication-routed drops commit exactly one deletion", func(t *testing.T) {
		usage0 := f.pubSnapshotUsage()
		f.exec(f.sub, "create snapshot sn_cd for table "+i30949SubDB+" t")
		require.Equal(t, 1, f.snapshotsByName(f.pub, "sn_cd"))
		first := startAsync(t, f, f.sub, "drop snapshot sn_cd from "+i30949PubAccount+" publication "+i30949PubName)
		second := startAsync(t, f, f.sub, "drop snapshot sn_cd from "+i30949PubAccount+" publication "+i30949PubName)
		errs := []error{
			requireCompletes(t, first, "first routed drop"),
			requireCompletes(t, second, "second routed drop"),
		}
		ok, failed := 0, 0
		for _, err := range errs {
			if err == nil {
				ok++
			} else {
				failed++
				require.Contains(t, err.Error(), "does not exist",
					"the serialized loser must surface the missing-record contract")
			}
		}
		require.Equal(t, 1, ok, "exactly one routed drop deletes the record")
		require.Equal(t, 1, failed, "exactly one routed drop fails")
		require.Empty(t, f.ownedRows("sn_cd"))
		require.Equal(t, 0, f.snapshotsByName(f.sub, "sn_cd"))
		require.Equal(t, usage0, f.pubSnapshotUsage(),
			"the record was deleted exactly once")
	})

	t.Run("no admission lock survives the rejected and serialized phases", func(t *testing.T) {
		f.exec(f.sub, "create snapshot sn_end for table "+i30949SubDB+" t")
		require.Equal(t, [][3]string{{"sn_end", i30949PubDB, "t"}}, f.ownedRows("sn_end"))
		f.exec(f.sub, "drop snapshot sn_end from "+i30949PubAccount+" publication "+i30949PubName)
		require.Empty(t, f.ownedRows("sn_end"))
		require.Equal(t, 2, f.count(f.sub, "select count(*) from "+i30949SubDB+".t"),
			"the binding still resolves after every phase")
	})
}
