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

package arrowload

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestArrowLoadFailedStatementKeepsEarlierTransactionWrite proves the documented
// default transaction-error contract for Arrow LOAD: a failed statement rolls
// back only its own writes, while preceding successful statements remain
// committable and the same connection can execute a valid retry afterwards.
func TestArrowLoadFailedStatementKeepsEarlierTransactionWrite(t *testing.T) {
	c := startArrowLoadCluster(t, 1, true, false, false)
	db := openArrowLoadDB(t, c, 0)
	mustExec(t, db, "create database if not exists arrow_lifecycle")
	mustExec(t, db, "use arrow_lifecycle")
	mustExec(t, db, "create table txn_failed_load(id bigint not null, name varchar(50))")

	dir := t.TempDir()
	valid := fixtureIDName(t, dir, "valid.arrow", containerFile,
		[][]idNameRow{{{id: 2, name: "loaded"}}})
	corrupt := filepath.Join(dir, "corrupt.arrow")
	require.NoError(t, os.WriteFile(corrupt, []byte("not an Arrow IPC file"), 0o600))

	mustExec(t, db, "begin")
	mustExec(t, db, "insert into txn_failed_load values (1, 'before-load')")
	_, err := db.Exec(fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table txn_failed_load", corrupt))
	require.Error(t, err)
	mustExec(t, db, "commit")

	require.Equal(t, int64(1), queryCount(t, db, "select count(*) from txn_failed_load"))
	require.Equal(t, int64(1), queryCount(t, db,
		"select count(*) from txn_failed_load where id=1 and name='before-load'"))
	mustExec(t, db, fmt.Sprintf(
		"load data infile {'filepath'='%s','format'='arrow'} into table txn_failed_load", valid))
	require.Equal(t, int64(2), queryCount(t, db, "select count(*) from txn_failed_load"))
}

// TestArrowLoadKillQueryRollsBackAndKeepsConnectionUsable holds a real,
// conditional S3 range GET, kills that exact MySQL query from a second
// connection, and proves the server propagates cancellation to the object
// read. This avoids treating a test-only in-process wait hook as a client
// cancellation boundary.
func TestArrowLoadKillQueryRollsBackAndKeepsConnectionUsable(t *testing.T) {
	c := startArrowLoadCluster(t, 1, true, true, false)
	db := openArrowLoadDB(t, c, 0)
	observer := openArrowLoadDB(t, c, 0)
	server := startArrowLoadMinIO(t)

	path := fixtureIDName(t, t.TempDir(), "kill.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "must-not-commit"}}})
	const key = "fault/kill-query.arrow"
	server.put(t, key, mustReadFile(t, path))
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	var blocked atomic.Bool
	proxyEndpoint := startArrowMinIOProxy(t, server.endpointURL,
		func(w http.ResponseWriter, r *http.Request) bool {
			if !isConditionalRangeGET(r) || !blocked.CompareAndSwap(false, true) {
				return false
			}
			close(requestStarted)
			select {
			case <-r.Context().Done():
				close(requestCanceled)
			case <-time.After(30 * time.Second):
				http.Error(w, "timed out waiting for KILL QUERY cancellation", http.StatusGatewayTimeout)
			}
			return true
		})

	mustExec(t, db, "create database if not exists arrow_lifecycle")
	mustExec(t, db, "use arrow_lifecycle")
	mustExec(t, db, "create table kill_query_load(id bigint not null, name varchar(50))")
	mustExec(t, db, "insert into kill_query_load values (0, 'seed')")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx, "use arrow_lifecycle")
	require.NoError(t, err)
	var connID int64
	require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connID))

	errCh := make(chan error, 1)
	go func() {
		_, loadErr := conn.ExecContext(ctx,
			minioLoadSQL(proxyEndpoint, server, key, "kill_query_load", "file", false))
		errCh <- loadErr
	}()
	select {
	case <-requestStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the conditional MinIO request")
	}
	waitUntilStatementRunning(t, observer, connID, "load data", 30*time.Second)
	mustExec(t, observer, fmt.Sprintf("kill query %d", connID))

	select {
	case loadErr := <-errCh:
		require.Error(t, loadErr)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for killed Arrow LOAD")
	}
	select {
	case <-requestCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight MinIO range request did not observe KILL QUERY cancellation")
	}
	require.Equal(t, int64(1), queryCount(t, observer, "select count(*) from arrow_lifecycle.kill_query_load"))
	var one int
	require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&one))
	require.Equal(t, 1, one)
	_, err = conn.ExecContext(ctx, minioLoadSQL(server.endpointURL, server, key, "kill_query_load", "file", false))
	require.NoError(t, err)
	require.Equal(t, int64(2), queryCount(t, observer, "select count(*) from arrow_lifecycle.kill_query_load"))
}

// TestArrowLoadClientDisconnectRollsBackAndReleasesSession closes the physical
// client connection during a real conditional object read. It verifies that a
// client-disconnect path is distinct from KILL QUERY: the session disappears,
// the reader request is canceled, and no LOAD rows are made durable.
func TestArrowLoadClientDisconnectRollsBackAndReleasesSession(t *testing.T) {
	c := startArrowLoadCluster(t, 1, true, true, false)
	admin := openArrowLoadDB(t, c, 0)
	observer := openArrowLoadDB(t, c, 0)
	server := startArrowLoadMinIO(t)
	path := fixtureIDName(t, t.TempDir(), "disconnect.arrow", containerFile,
		[][]idNameRow{{{id: 1, name: "must-not-commit"}}})
	const key = "fault/client-disconnect.arrow"
	server.put(t, key, mustReadFile(t, path))
	requestStarted := make(chan struct{})
	requestCanceled := make(chan struct{})
	var blocked atomic.Bool
	proxyEndpoint := startArrowMinIOProxy(t, server.endpointURL,
		func(w http.ResponseWriter, r *http.Request) bool {
			if !isConditionalRangeGET(r) || !blocked.CompareAndSwap(false, true) {
				return false
			}
			close(requestStarted)
			select {
			case <-r.Context().Done():
				close(requestCanceled)
			case <-time.After(30 * time.Second):
				http.Error(w, "timed out waiting for client disconnect", http.StatusGatewayTimeout)
			}
			return true
		})

	mustExec(t, admin, "create database if not exists arrow_lifecycle")
	mustExec(t, admin, "create table arrow_lifecycle.client_disconnect_load(id bigint not null, name varchar(50))")
	mustExec(t, admin, "insert into arrow_lifecycle.client_disconnect_load values (0, 'seed')")

	rawConn, connID := openArrowLoadRawConn(t, c, 0)
	t.Cleanup(func() { _ = rawConn.Close() })
	execer, ok := rawConn.(driver.ExecerContext)
	require.True(t, ok, "MySQL driver connection does not support ExecContext")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := execer.ExecContext(ctx, "use arrow_lifecycle", nil)
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() {
		_, loadErr := execer.ExecContext(ctx,
			minioLoadSQL(proxyEndpoint, server, key, "client_disconnect_load", "file", false), nil)
		errCh <- loadErr
	}()
	select {
	case <-requestStarted:
	case <-time.After(30 * time.Second):
		_ = rawConn.Close()
		t.Fatal("timed out waiting for the conditional MinIO request")
	}
	waitUntilStatementRunning(t, observer, connID, "load data", 30*time.Second)
	require.NoError(t, rawConn.Close())
	select {
	case loadErr := <-errCh:
		require.Error(t, loadErr)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the disconnected Arrow LOAD")
	}
	select {
	case <-requestCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight MinIO range request did not observe client disconnect")
	}
	waitUntilConnectionGone(t, observer, connID, 30*time.Second)
	require.Equal(t, int64(1), queryCount(t, observer,
		"select count(*) from arrow_lifecycle.client_disconnect_load"))
	mustExec(t, admin, minioLoadSQL(server.endpointURL, server, key, "arrow_lifecycle.client_disconnect_load", "file", false))
	require.Equal(t, int64(2), queryCount(t, observer,
		"select count(*) from arrow_lifecycle.client_disconnect_load"))
}
