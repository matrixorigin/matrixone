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

package issues

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/status"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue29715ODKUConcurrentDropIndex(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		writerCN, err := c.GetCNService(0)
		require.NoError(t, err)
		ddlCN, err := c.GetCNService(1)
		require.NoError(t, err)
		writerDB := openIssue277xxDB(t, writerCN.GetServiceConfig().CN.Frontend.Port)
		defer writerDB.Close()
		ddlDB := openIssue277xxDB(t, ddlCN.GetServiceConfig().CN.Frontend.Port)
		defer ddlDB.Close()
		const database = "issue_29715"
		ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
		defer cancel()
		resetIssue277xxDatabase(t, ctx, writerDB, database)
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			execSQLMaybe(t, cleanupCtx, writerDB, "drop database if exists "+database)
		}()
		services := issue27487LockServices(c)
		for _, terminal := range []string{"commit", "rollback", "late admission"} {
			t.Run(terminal, func(t *testing.T) {
				execSQLRequire(t, ctx, writerDB, "create table "+database+".t (id int primary key, u int, v int, unique uk(u))")
				defer execSQLMaybe(t, ctx, writerDB, "drop table if exists "+database+".t")
				execSQLRequire(t, ctx, writerDB, "insert into "+database+".t values (1,1,0)")
				require.Eventually(t, func() bool {
					var n int
					return ddlDB.QueryRowContext(ctx, "select count(*) from "+database+".t").Scan(&n) == nil && n == 1
				}, 20*time.Second, 10*time.Millisecond, "DDL CN must see the table before scheduling the race")
				writer, err := writerDB.Conn(ctx)
				require.NoError(t, err)
				defer writer.Close()
				const odku = "insert into issue_29715.t values (1,1,1) on duplicate key update v=v+1"
				const drop = "alter table issue_29715.t drop index uk"
				if terminal == "late admission" {
					testIssue29715LateAdmission(t, ctx, writer, writerDB, ddlDB, writerCN, odku, drop)
				} else {
					var tableID uint64
					require.NoError(t, writerDB.QueryRowContext(ctx,
						"select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", database).Scan(&tableID))
					require.NoError(t, execIssue27487(ctx, writer, "begin"))
					defer func() {
						cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						_, _ = writer.ExecContext(cleanupCtx, "rollback")
					}()
					require.NoError(t, execIssue27487(ctx, writer, odku))
					txnID := findIssue27487WriterTxn(services, tableID)
					require.NotEmpty(t, txnID, "ODKU must hold its base row lock")
					packer := types.NewPacker()
					defer packer.Close()
					packer.EncodeUint32(0)
					packer.EncodeStringType([]byte(database))
					packer.EncodeStringType([]byte("t"))
					serial := bytes.Clone(packer.GetBuf())
					packer.Reset()
					packer.EncodeStringType(serial)
					key := bytes.Clone(packer.GetBuf())
					require.True(t, issue29715BaseMetadataHeld(services, txnID, key))
					ddlCtx, stopDDL := context.WithCancel(ctx)
					defer stopDDL()
					done := make(chan error, 1)
					go func() { _, err := ddlDB.ExecContext(ddlCtx, drop); done <- err }()
					joined := false
					defer func() {
						if !joined {
							stopDDL()
							cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
							defer stop()
							_, _ = writer.ExecContext(cleanupCtx, "rollback")
							select {
							case <-done:
							case <-cleanupCtx.Done():
								t.Error("DROP did not exit during cleanup")
							}
						}
					}()
					require.Eventually(t, func() bool { return hasIssue27487Waiter(services, [][]byte{key}) },
						20*time.Second, 10*time.Millisecond, "DROP must wait for ODKU's base metadata lock")
					require.NoError(t, execIssue27487(ctx, writer, terminal))
					select {
					case err := <-done:
						joined = true
						require.NoError(t, err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				// DDL responds on CN1; make its committed catalog visible on CN0
				// only after exercising the race, before checking the final image.
				commitTS := ddlCN.RawService().(cnservice.Service).GetTxnClient().GetLatestCommitTS()
				require.False(t, commitTS.IsEmpty())
				testutils.WaitLogtailApplied(t, commitTS, writerCN)
				want := 1
				if terminal == "rollback" {
					want = 0
				}
				var count, sum int
				require.NoError(t, writer.QueryRowContext(ctx, "select count(*),sum(v) from issue_29715.t").Scan(&count, &sum))
				require.Equal(t, 1, count)
				require.Equal(t, want, sum, "ODKU must apply exactly once, or not at all after rollback")
				require.Zero(t, queryIssue277xxInt(t, ctx, writerDB,
					"select count(*) from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t') and name='uk'", database))
				// With uk removed, another primary key may reuse u. The public
				// result checks that the recompiled writer did not retain that index.
				require.NoError(t, execIssue27487(ctx, writer, "insert into issue_29715.t values (2,1,0) on duplicate key update v=v+1"))
				require.NoError(t, writer.QueryRowContext(ctx, "select count(*),sum(v) from issue_29715.t").Scan(&count, &sum))
				require.Equal(t, 2, count)
				require.Equal(t, want, sum)
			})
		}
	})
}

// ODKU holds separate base and unique-index metadata keys. Observe the base
// key explicitly instead of the older single-metadata-key fixture assumption.
func issue29715BaseMetadataHeld(services []lockservice.LockService, txnID, key []byte) bool {
	found := false
	for _, service := range services {
		service.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			if tableID == catalog.MO_TABLES_ID && lock.GetLockMode() == pblock.LockMode_Shared && equalIssue27487Keys(keys, [][]byte{key}) {
				lock.IterHolders(func(holder pblock.WaitTxn) bool { found = found || bytes.Equal(holder.TxnID, txnID); return !found })
			}
			return !found
		})
	}
	return found
}

func testIssue29715LateAdmission(t *testing.T, ctx context.Context, writer *sql.Conn, writerDB, ddlDB *sql.DB, writerCN embed.ServiceOperator, odku, drop string) {
	t.Helper()
	sid := writerCN.ServiceID()
	provider, ok := writerCN.RawService().(interface {
		SessionMgr() *queryservice.SessionManager
	})
	require.True(t, ok)
	sessionManager := provider.SessionMgr()
	require.NotNil(t, sessionManager)
	var writerConnectionID uint32
	require.NoError(t, writer.QueryRowContext(ctx, "select connection_id()").Scan(&writerConnectionID))

	execSQLRequire(t, ctx, writerDB, "create table issue_29715.probe(id int primary key, v int)")
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		execSQLMaybe(t, cleanupCtx, writerDB, "drop table if exists issue_29715.probe")
	}()
	probe, err := writerDB.Conn(ctx)
	require.NoError(t, err)
	defer probe.Close()
	var probeConnectionID uint32
	require.NoError(t, probe.QueryRowContext(ctx, "select connection_id()").Scan(&probeConnectionID))
	require.NotEqual(t, writerConnectionID, probeConnectionID)
	require.NoError(t, execIssue27487(ctx, probe, "begin"))
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, _ = probe.ExecContext(cleanupCtx, "rollback")
	}()
	execProbe := func(statement string) {
		probeCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		require.NoError(t, execIssue27487(probeCtx, probe, statement))
	}

	paused, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var releaseOnce sync.Once
	var admissions atomic.Int32
	var probeAdmissions atomic.Int32
	var owner atomic.Value
	tc := moruntime.MustGetTestingContext(sid)
	tc.SetBeforeLockFunc(func(txnID []byte, tableID uint64) {
		if tableID != catalog.MO_TABLES_ID {
			return
		}
		sessions := sessionManager.GetAllSessions()
		if issue29715TxnBelongsToConnection(sessions, sid, probeConnectionID, txnID) {
			probeAdmissions.Add(1)
		}
		if issue29715TxnBelongsToConnection(sessions, sid, writerConnectionID, txnID) && first.CompareAndSwap(false, true) {
			owner.Store(bytes.Clone(txnID))
			close(paused)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	// GetBeforeLockFunc requires the result hook. Observe real committed DDL
	// evidence without changing the result or injecting a retry.
	tc.SetAdjustLockResultFunc(func(txnID []byte, tableID uint64, result *pblock.Result) {
		id, ok := owner.Load().([]byte)
		if ok && bytes.Equal(id, txnID) && tableID == catalog.MO_TABLES_ID && !result.Timestamp.IsEmpty() {
			admissions.Add(1)
		}
	})
	defer tc.SetBeforeLockFunc(nil)
	defer tc.SetAdjustLockResultFunc(nil)
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	var done chan error
	joined := false
	defer func() {
		stop()
		releaseOnce.Do(func() { close(release) })
		if done != nil && !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("ODKU did not exit during cleanup")
			}
		}
	}()
	// A real CN0 transaction reaches the same hooks first, but must neither
	// capture the writer barrier nor contribute to its retry count.
	execProbe("insert into issue_29715.probe values (1,0)")
	execProbe("update issue_29715.probe set v=v+1 where id=1")
	require.GreaterOrEqual(t, probeAdmissions.Load(), int32(2))
	require.Nil(t, owner.Load())
	require.Zero(t, admissions.Load())
	select {
	case <-paused:
		t.Fatal("unrelated transaction captured the writer barrier")
	default:
	}
	done = make(chan error, 1)
	go func() { done <- execIssue27487(workCtx, writer, odku) }()
	select {
	case <-paused:
	case err := <-done:
		joined = true
		t.Fatalf("ODKU did not reach metadata admission: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	probeBefore := probeAdmissions.Load()
	execProbe("update issue_29715.probe set v=v+1 where id=1")
	require.Greater(t, probeAdmissions.Load(), probeBefore)
	require.Zero(t, admissions.Load(), "only the paused writer may contribute admissions")
	execProbe("commit")
	var probeValue int
	require.NoError(t, probe.QueryRowContext(ctx, "select v from issue_29715.probe where id=1").Scan(&probeValue))
	require.Equal(t, 2, probeValue)
	execSQLRequire(t, ctx, ddlDB, drop)
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		joined = true
		require.NoError(t, err, "stale compiled ODKU must rebuild rather than expose the hidden table")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.GreaterOrEqual(t, admissions.Load(), int32(2),
		"stale metadata admission must refresh and rebuild before the second admission")
}

// Project only the active callback actor's identity. Full StatusSession reads
// unrelated mutable statement fields; the transaction UUID getter is locked.
func issue29715TxnBelongsToConnection(sessions []queryservice.Session, sid string, connectionID uint32, txnID []byte) bool {
	var identities []*status.Session
	for _, session := range sessions {
		identity, ok := session.(interface {
			GetTxnId() uuid.UUID
			GetConnectionID() uint32
			GetService() string
		})
		if !ok {
			continue
		}
		id := identity.GetTxnId()
		if id == (uuid.UUID{}) || !bytes.Equal(id[:], txnID) {
			continue
		}
		identities = append(identities, &status.Session{
			NodeID: identity.GetService(),
			ConnID: identity.GetConnectionID(),
			TxnID:  hex.EncodeToString(id[:]),
		})
	}
	id, err := issue26068SessionTxnID(identities, sid, connectionID)
	return err == nil && bytes.Equal(id, txnID)
}

func TestIssue29715TxnBelongsToConnection(t *testing.T) {
	actor := &issue29715IdentitySession{txnID: uuid.UUID{1}, sid: "cn0", connectionID: 7}
	sessions := []queryservice.Session{
		&issue29715IdentitySession{},                    // idle; connection getters must not be called
		&issue29715IdentitySession{txnID: uuid.UUID{2}}, // another active transaction
		actor,
	}
	require.True(t, issue29715TxnBelongsToConnection(sessions, "cn0", 7, actor.txnID[:]))
	require.False(t, issue29715TxnBelongsToConnection(sessions, "cn1", 7, actor.txnID[:]))
	require.False(t, issue29715TxnBelongsToConnection(sessions, "cn0", 8, actor.txnID[:]))
	oldID := actor.txnID
	actor.txnID = uuid.UUID{3}
	require.False(t, issue29715TxnBelongsToConnection(sessions, "cn0", 7, oldID[:]))
	require.True(t, issue29715TxnBelongsToConnection(sessions, "cn0", 7, actor.txnID[:]))
}

type issue29715IdentitySession struct {
	queryservice.Session
	txnID        uuid.UUID
	sid          string
	connectionID uint32
}

func (s *issue29715IdentitySession) GetTxnId() uuid.UUID { return s.txnID }

func (s *issue29715IdentitySession) GetConnectionID() uint32 {
	if s.connectionID == 0 {
		panic("connection identity read for unrelated session")
	}
	return s.connectionID
}

func (s *issue29715IdentitySession) GetService() string {
	if s.sid == "" {
		panic("service identity read for unrelated session")
	}
	return s.sid
}
