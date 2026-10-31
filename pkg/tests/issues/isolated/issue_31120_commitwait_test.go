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

package isolated

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// TestIssue31120CommitWaitBarrierAcceptance exercises the issue 31120 public
// contract end to end through a real embedded standalone cluster with SQL
// commits traversing the TN 1PC commit-apply path:
//
//  1. a concurrent nonmatching commit neither blocks nor consumes the scoped
//     single-shot barrier (proven by the later matching commit still firing);
//  2. an explicitly submitted matching commit is observed REACHED and parked
//     in WAIT via the barrier's GETWAITERS probe - server-side evidence rather
//     than statement-liveness inference - held far beyond measured baseline
//     commit latency, then released with the barrier left installed;
//  3. after that single firing the same barrier installation is spent: a
//     second fully matching commit registers no waiter and completes within a
//     fraction of the hold window, while the framework's point listing still
//     contains the barrier.
func TestIssue31120CommitWaitBarrierAcceptance(t *testing.T) {
	var faultEnabledHere bool
	const (
		tenantSel  = objectio.FJ_CommitWaitTargetTenant
		txnSel     = objectio.FJ_CommitWaitTargetTxn
		barrier    = objectio.FJ_CommitWait
		waiters    = "issue31120_acceptance_waiters"
		releaseAll = "issue31120_acceptance_release"
	)
	points := []string{tenantSel, txnSel, barrier, waiters, releaseAll}
	var (
		cluster    embed.Cluster
		dbs        []*sql.DB
		conns      []*sql.Conn
		goroutines sync.WaitGroup
	)
	t.Cleanup(func() {
		// Remove WAIT points first: removing a WAIT wakes every registered
		// waiter, so commit goroutines can finish even when an assertion above
		// called FailNow.
		for _, point := range points {
			_, _ = fault.RemoveFaultPoint(context.Background(), point)
		}
		cleanupErr := waitIssue28012And28013Goroutines(&goroutines, 30*time.Second)
		for _, conn := range conns {
			cleanupErr = errors.Join(cleanupErr, conn.Close())
		}
		for _, db := range dbs {
			cleanupErr = errors.Join(cleanupErr, db.Close())
		}
		if cluster != nil {
			cleanupErr = errors.Join(cleanupErr, cluster.Close())
		}
		if faultEnabledHere {
			fault.Disable()
		}
		require.NoError(t, cleanupErr)
	})

	var err error
	cluster, err = embed.StartTestCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	// Cluster admission can queue behind other coverage tests; start the SQL
	// deadline only after this test owns a running cluster.
	faultEnabledHere = fault.Enable()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db := openIssue28012And28013DB(t, ctx, cluster, "dump:111", &dbs)
	dbName := fmt.Sprintf("issue_31120_%d", time.Now().UnixNano())
	mustExecIssue28012And28013(t, ctx, db, "create database "+dbName)
	table := dbName + ".t"
	mustExecIssue28012And28013(t, ctx, db, "create table "+table+" (id int primary key)")

	// Measured unarmed commit latency for this build/mode; hold/spend margins
	// scale from it so WAIT can never be confused with ordinary latency.
	baseStart := time.Now()
	mustExecIssue28012And28013(t, ctx, db, "insert into "+table+" values (1)")
	baseDur := time.Since(baseStart)
	holdFor := baseDur * 10
	if holdFor < 3*time.Second {
		holdFor = 3 * time.Second
	}
	spendWindow := baseDur * 5
	if spendWindow < 2*time.Second {
		spendWindow = 2 * time.Second
	}

	// Pin the victim sessions BEFORE arming: opening a connection performs
	// commits, which a matching armed barrier would block.
	victims := make([]*sql.Conn, 3)
	for i := range victims {
		victim, err := db.Conn(ctx)
		require.NoError(t, err)
		require.NoError(t, victim.PingContext(ctx))
		conns = append(conns, victim)
		victims[i] = victim
	}

	commitAsync := func(conn *sql.Conn, id int) <-chan error {
		result := make(chan error, 1)
		goroutines.Add(1)
		go func() {
			defer goroutines.Done()
			txn, err := conn.BeginTx(ctx, nil)
			if err == nil {
				_, err = txn.ExecContext(ctx, fmt.Sprintf("insert into %s values (%d)", table, id))
			}
			if err == nil {
				err = txn.Commit()
			}
			result <- err
		}()
		return result
	}
	awaitCommit := func(t *testing.T, result <-chan error, what string) {
		t.Helper()
		select {
		case err := <-result:
			require.NoErrorf(t, err, "%s", what)
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not reach a terminal result within 30s", what)
		}
	}
	probeWaiters := func() int64 {
		count, _, ok := fault.TriggerFault(waiters)
		require.True(t, ok)
		return count
	}

	// Leg A: a scope that matches nothing lets a concurrent sys commit finish
	// without registering a barrier waiter; the shot must remain intact.
	require.NoError(t, fault.AddFaultPoint(ctx, tenantSel, ":::", "ECHO", 424242, "", false))
	require.NoError(t, fault.AddFaultPoint(ctx, txnSel, ":::", "ECHO", 0, "no-such-txn", false))
	require.NoError(t, fault.AddFaultPoint(ctx, barrier, "1:1::", "WAIT", 0, "issue31120-acceptance", false))
	require.NoError(t, fault.AddFaultPoint(ctx, waiters, ":::", "GETWAITERS", 0, barrier, false))
	require.NoError(t, fault.AddFaultPoint(ctx, releaseAll, ":::", "NOTIFYALL", 0, barrier, false))
	awaitCommit(t, commitAsync(victims[0], 2), "leg A: nonmatching commit")
	require.Zero(t, probeWaiters(), "leg A: a nonmatching commit must not register a barrier waiter")

	// Leg B: re-scope to this exact (tenant, txn) pair while the barrier from
	// leg A stays installed; the matching commit must reach WAIT and hold.
	pinnedTxn, err := victims[1].BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = pinnedTxn.ExecContext(ctx, "insert into "+table+" values (3)")
	require.NoError(t, err)
	var connectionID uint32
	require.NoError(t, pinnedTxn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
	commitID := issue31120CurrentTxnID(t, cluster, connectionID)
	_, err = fault.RemoveFaultPoint(ctx, txnSel)
	require.NoError(t, err)
	require.NoError(t, fault.AddFaultPoint(ctx, txnSel, ":::", "ECHO", 0, string(commitID[:]), false))
	_, err = fault.RemoveFaultPoint(ctx, tenantSel)
	require.NoError(t, err)
	require.NoError(t, fault.AddFaultPoint(ctx, tenantSel, ":::", "ECHO", 0, "", false))

	blockedResult := make(chan error, 1)
	goroutines.Add(1)
	blockedStart := time.Now()
	go func() {
		defer goroutines.Done()
		blockedResult <- pinnedTxn.Commit()
	}()
	// Server-side evidence the explicitly submitted commit reached the
	// barrier and parked in WAIT before any release.
	waitIssue28012And28013FaultWaiters(t, waiters, 1, 30*time.Second)
	time.Sleep(holdFor)
	require.Equal(t, int64(1), probeWaiters(),
		"leg B: the matching commit must still be parked in WAIT after a hold far above baseline latency %s", baseDur)
	select {
	case commitErr := <-blockedResult:
		t.Fatalf("leg B: the matching commit completed while its barrier was held: %v", commitErr)
	case <-time.After(100 * time.Millisecond):
	}
	// Release by NOTIFYALL: waiters wake, the barrier installation remains.
	_, _, released := fault.TriggerFault(releaseAll)
	require.True(t, released)
	select {
	case commitErr := <-blockedResult:
		require.NoError(t, commitErr)
	case <-time.After(30 * time.Second):
		t.Fatal("leg B: released commit did not finish")
	}
	require.GreaterOrEqual(t, time.Since(blockedStart), holdFor,
		"leg B: elapsed commit time must include the hold, distinguishing WAIT from latency")
	waitIssue28012And28013FaultWaiters(t, waiters, 0, 30*time.Second)
	var visible int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table+" where id = 3").Scan(&visible))
	require.Equal(t, 1, visible, "leg B: the released commit's write must be visible")

	// Leg C: the same barrier installation is spent. A second fully matching
	// commit registers no waiter and completes inside a fraction of the hold.
	spendTxn, err := victims[2].BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = spendTxn.ExecContext(ctx, "insert into "+table+" values (4)")
	require.NoError(t, err)
	require.NoError(t, spendTxn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
	spendID := issue31120CurrentTxnID(t, cluster, connectionID)
	_, err = fault.RemoveFaultPoint(ctx, txnSel)
	require.NoError(t, err)
	require.NoError(t, fault.AddFaultPoint(ctx, txnSel, ":::", "ECHO", 0, string(spendID[:]), false))

	spentResult := make(chan error, 1)
	goroutines.Add(1)
	spentStart := time.Now()
	go func() {
		defer goroutines.Done()
		spentResult <- spendTxn.Commit()
	}()
	require.Never(t, func() bool { return probeWaiters() > 0 }, spendWindow, 10*time.Millisecond,
		"leg C: a matching commit must never park on the spent barrier while the same installation remains")
	select {
	case commitErr := <-spentResult:
		require.NoError(t, commitErr)
	case <-time.After(30 * time.Second):
		t.Fatal("leg C: matching commit blocked on the spent barrier")
	}
	require.Less(t, time.Since(spentStart), holdFor,
		"leg C: the spent barrier must not impose the hold window on matching commits")

	// The barrier is still installed after its single firing, proven through
	// the framework's own installation listing (same installation, not a
	// re-armed one).
	require.Contains(t, fault.ListAllFaultPoints(), barrier,
		"leg C: the spent single-shot barrier remains listed in the same installation")
}

// issue31120CurrentTxnID returns the raw session transaction id of the open
// SQL transaction on connectionID, the same raw 16 bytes the commit-wait
// transaction selector matches (see issue 28012/28013's shutdown test).
func issue31120CurrentTxnID(t *testing.T, cluster embed.Cluster, connectionID uint32) uuid.UUID {
	t.Helper()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	provider, ok := cn.RawService().(interface {
		SessionMgr() *queryservice.SessionManager
	})
	require.True(t, ok)
	for _, session := range provider.SessionMgr().GetAllSessions() {
		identity, ok := session.(interface {
			GetConnectionID() uint32
			GetTxnId() uuid.UUID
		})
		if ok && identity.GetConnectionID() == connectionID {
			txnID := identity.GetTxnId()
			require.NotEqual(t, uuid.Nil, txnID, "capture the actual SQL transaction before arming its commit barrier")
			return txnID
		}
	}
	t.Fatalf("session for connection %d not found", connectionID)
	return uuid.Nil
}
