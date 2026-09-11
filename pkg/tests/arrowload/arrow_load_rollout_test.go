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

package arrowload

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
)

// TestArrowLoadRolloutRollbackDrain exercises the operational transition, not
// just static gate values. It starts from an explicitly enabled local-only policy, stops a
// cluster while an Arrow statement is admitted, restarts with every Arrow gate
// disabled, and finally rolls forward with distributed execution disabled.
// Shutdown may finish the admitted transaction or cancel it; either result must
// be atomic and bounded.
func TestArrowLoadRolloutRollbackDrain(t *testing.T) {
	c := startArrowLoadCluster(t, 1, true, false, false)
	// Both checks use the same policy; run the gate check before shutdown.
	t.Run("S3GateDisabled", func(t *testing.T) { testArrowLoadGateS3Disabled(t, c) })
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_rollout")
	mustExec(t, db, "use arrow_rollout")
	path := fixtureIDName(t, t.TempDir(), "rollout.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "one"}, {id: 2, name: "two"}}})
	const ddl = "id BIGINT NOT NULL, name VARCHAR(50)"
	const expectedRows int64 = 2
	mustExec(t, db, fmt.Sprintf("create table rollout_drain(%s)", ddl))

	ctx, cancel := context.WithCancel(context.Background())
	conn, err := db.Conn(ctx)

	const (
		waitersProbe         = "arrowload_rollout_waiters"
		releasePoint         = "arrowload_rollout_release"
		shutdownWaitersProbe = "arrowload_rollout_shutdown_waiters"
		shutdownReleasePoint = "arrowload_rollout_shutdown_release"
	)
	faultStarted := fault.Enable()
	var loadErrCh chan error
	loadDone := false
	var shutdownErrCh chan error
	shutdownDone := false
	var observerDB *sql.DB
	// Register cleanup before any assertion after the connection is acquired and
	// before arming either barrier. It releases both barriers and cancels/joins
	// both asynchronous tasks even when a later assertion calls FailNow;
	// teardown must never wait behind a barrier it owns.
	t.Cleanup(func() {
		objectio.NotifyInjected(releasePoint)
		objectio.NotifyInjected(shutdownReleasePoint)
		cancel()
		if conn != nil {
			_ = conn.Close()
		}
		if db != nil {
			_ = db.Close()
		}
		if observerDB != nil {
			_ = observerDB.Close()
		}
		// RemoveFaultPoint also wakes a waiter that raced with the explicit
		// notification above. Do this before joining either task so cleanup
		// cannot inherit a missed-notification deadlock.
		for _, key := range []string{
			objectio.FJ_ArrowLoadRolloutWait, waitersProbe, releasePoint,
			embed.ArrowLoadRolloutShutdown, shutdownWaitersProbe, shutdownReleasePoint,
		} {
			_, _ = fault.RemoveFaultPoint(context.Background(), key)
		}
		if loadErrCh != nil && !loadDone {
			select {
			case <-loadErrCh:
			case <-time.After(30 * time.Second):
			}
		}
		if shutdownErrCh != nil && !shutdownDone {
			select {
			case <-shutdownErrCh:
			case <-time.After(30 * time.Second):
			}
		}
		if faultStarted {
			fault.Disable()
		}
	})
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "use arrow_rollout")
	require.NoError(t, err)
	var connID int64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))
	observerDB = openArrowLoadDB(t, c, 0)
	require.NoError(t, fault.AddFaultPoint(
		ctx, objectio.FJ_ArrowLoadRolloutWait, "1:1::", "WAIT", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(
		ctx, waitersProbe, ":::", "GETWAITERS", 0,
		objectio.FJ_ArrowLoadRolloutWait, false))
	require.NoError(t, fault.AddFaultPoint(
		ctx, releasePoint, ":::", "NOTIFYALL", 0,
		objectio.FJ_ArrowLoadRolloutWait, false))
	require.NoError(t, fault.AddFaultPoint(
		ctx, embed.ArrowLoadRolloutShutdown, "1:1::", "WAIT", 0, "", false))
	require.NoError(t, fault.AddFaultPoint(
		ctx, shutdownWaitersProbe, ":::", "GETWAITERS", 0,
		embed.ArrowLoadRolloutShutdown, false))
	require.NoError(t, fault.AddFaultPoint(
		ctx, shutdownReleasePoint, ":::", "NOTIFYALL", 0,
		embed.ArrowLoadRolloutShutdown, false))

	loadErrCh = make(chan error, 1)
	go func() {
		_, execErr := conn.ExecContext(ctx, fmt.Sprintf(
			"load data infile {'filepath'='%s','format'='arrow'} into table rollout_drain parallel 'true'", path))
		loadErrCh <- execErr
	}()
	// Wait for the post-admission boundary first. The processlist observer is
	// deliberately delayed until the statement is held, so it is a secondary
	// observation and cannot race the lifecycle transition.
	waitUntilArrowLoadRolloutHook(t, waitersProbe, 30*time.Second)
	waitUntilStatementRunning(t, observerDB, connID, "load data", 30*time.Second)

	shutdownErrCh = make(chan error, 1)
	go func() {
		shutdownErrCh <- c.Close()
	}()
	// The shutdown waiter proves that Close has entered its real boundary. Only
	// then release shutdown and the admitted LOAD; either the drain or the
	// cancellation path must remain atomic.
	waitUntilArrowLoadRolloutHook(t, shutdownWaitersProbe, 30*time.Second)
	objectio.NotifyInjected(shutdownReleasePoint)
	objectio.NotifyInjected(releasePoint)
	var shutdownErr error
	select {
	case shutdownErr = <-shutdownErrCh:
		shutdownDone = true
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for cluster shutdown")
	}
	require.NoError(t, shutdownErr)

	var loadErr error
	select {
	case loadErr = <-loadErrCh:
		loadDone = true
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for admitted Arrow LOAD during cluster shutdown")
	}
	_ = conn.Close()
	_ = db.Close()

	adjustArrowLoadCluster(c, arrowLoadClusterOptions{cnCount: 1})
	require.NoError(t, c.Start())
	waitArrowLoadClusterReady(t, c)
	rollbackDB := openArrowLoadDB(t, c, 0)
	rows := queryCount(t, rollbackDB, "select count(*) from arrow_rollout.rollout_drain")
	if loadErr != nil {
		t.Logf("LOAD returned %v; using post-restart readback as the commit oracle", loadErr)
	}
	require.True(t, rows == 0 || rows == expectedRows,
		"shutdown must leave either no rows or the complete fixture, got %d", rows)

	missing := filepath.Join(t.TempDir(), "must-not-be-read.arrow")
	_, err = rollbackDB.Exec(fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table arrow_rollout.rollout_drain", missing))
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "disabled by configuration")

	require.NoError(t, rollbackDB.Close())
	require.NoError(t, c.Close())
	adjustArrowLoadCluster(c, arrowLoadClusterOptions{
		cnCount: 1, enabled: true, s3Enabled: false, distributedEnabled: false,
	})
	require.NoError(t, c.Start())
	waitArrowLoadClusterReady(t, c)
	rolledForwardDB := openArrowLoadDB(t, c, 0)
	mustExec(t, rolledForwardDB, "truncate table arrow_rollout.rollout_drain")
	mustExec(t, rolledForwardDB, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table arrow_rollout.rollout_drain parallel 'true'", path))
	require.Equal(t, expectedRows, queryCount(t, rolledForwardDB,
		"select count(*) from arrow_rollout.rollout_drain"))
}

func waitUntilArrowLoadRolloutHook(t testing.TB, waitersProbe string, deadline time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		waiters, _, ok := fault.TriggerFault(waitersProbe)
		if ok && waiters >= 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for Arrow rollout hook waiter (waiters=%d, registered=%v)", waiters, ok)
		case <-ticker.C:
		}
	}
}
