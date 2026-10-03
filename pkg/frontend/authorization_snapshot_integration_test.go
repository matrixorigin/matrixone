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

package frontend_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

type publishingAuthorizationEngine struct {
	engine.Engine
	reader       engine.TableContentReader
	afterCapture func()
	captures     int
}

func (e *publishingAuthorizationEngine) ReadTableContentVersions(ctx context.Context, s timestamp.Timestamp, deps []engine.TableContentDependency, out []engine.TableContentVersion) bool {
	e.captures++
	ok := e.reader.ReadTableContentVersions(ctx, s, deps, out)
	if ok && e.afterCapture != nil {
		f := e.afterCapture
		e.afterCapture = nil
		f()
	}
	return ok
}

type authorizationTxnObserver struct {
	client.TxnClient
	opens int
}

func (c *authorizationTxnObserver) New(ctx context.Context, s timestamp.Timestamp, opts ...client.TxnOption) (client.TxnOperator, error) {
	c.opens++
	return c.TxnClient.New(ctx, s, opts...)
}
func (c *authorizationTxnObserver) ReadSnapshot(ctx context.Context, s timestamp.Timestamp) (timestamp.Timestamp, error) {
	return c.TxnClient.(client.ReadSnapshotClient).ReadSnapshot(ctx, s)
}

func TestAuthorizationSnapshotMatchesPublishedSQL(t *testing.T) {
	catalog.SetupDefines("")
	baseCtx := defines.AttachAccountId(context.Background(), 0)
	fixedPhysical := time.Now().Add(time.Hour).UnixNano()
	tnClock := clock.NewHLCClock(func() int64 { return fixedPhysical }, 0)
	tnClock.SetNodeID(17)
	de, tae, rpc, _ := testutil.CreateEngines(baseCtx, testutil.TestOptions{TaeEngineOptions: &options.Options{Clock: tnClock}}, t)
	t.Cleanup(func() { de.Close(baseCtx); tae.Close(true); rpc.Close() })
	tae.GetDB().Runtime.SyncProtectionValidator = nil
	ctx, cancel := context.WithTimeout(baseCtx, 90*time.Second)
	defer cancel()
	pu := frontend.AuthorizationParameterUnitForTest()
	pu.SV.SetDefaultValues()
	pu.SV.KeyEncryptionKey = "0123456789abcdef"
	pu.FileService = tae.GetDB().Runtime.Fs
	ctx = context.WithValue(ctx, config.ParameterUnitKey, pu)
	raw, exists := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	require.True(t, exists)
	sqlExec := raw.(executor.SQLExecutor)
	exec := func(query string) {
		result, err := sqlExec.Exec(ctx, query, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err, query)
		result.Close()
	}
	exec(frontend.MoCatalogMoIndexesDDL)
	require.NoError(t, sqlExec.ExecTxn(ctx, func(tx executor.TxnExecutor) error { return frontend.InitSysTenant(ctx, tx, "test") }, executor.Options{}.WithWaitCommittedLogApplied()))
	exec(frontend.MoCatalogMoForeignKeysDDL)
	for _, query := range []string{
		"create database app", "create table app.t(id int)",
		"insert into mo_catalog.mo_user(user_id,user_host,user_name,authentication_string,status,created_time,login_type,creator,owner,default_role) values(42,'localhost','auth_u','unused','unlock',now(),'PASSWORD',0,0,43)",
		"insert into mo_catalog.mo_role(role_id,role_name,creator,owner,created_time) values(43,'auth_r',0,0,now())",
		"insert into mo_catalog.mo_user_grant(role_id,user_id,granted_time,with_grant_option) values(43,42,now(),false)",
		fmt.Sprintf("insert into mo_catalog.mo_role_privs(role_id,role_name,obj_type,obj_id,privilege_id,privilege_name,privilege_level,operation_user_id,granted_time,with_grant_option) select 43,'auth_r','table',rel_id,%d,'select','d.t',0,now(),false from mo_catalog.mo_tables where reldatabase='app' and relname='t'", frontend.PrivilegeTypeSelect),
	} {
		exec(query)
	}
	observed := &publishingAuthorizationEngine{Engine: de.Engine, reader: de.Engine}
	pu.StorageEngine = observed
	transactions := &authorizationTxnObserver{TxnClient: de.GetTxnClient()}
	pu.TxnClient = transactions
	ses := frontend.NewAuthorizationSessionForTest(t, ctx)
	snapshots := de.GetTxnClient().(client.ReadSnapshotClient)
	initial, err := snapshots.ReadSnapshot(ctx, timestamp.Timestamp{})
	require.NoError(t, err)
	allowed, _, err := frontend.AuthorizationAtSnapshotForTest(ctx, ses, initial)
	require.NoError(t, err)
	require.True(t, allowed)
	// Initial cold evaluation resolves/subscribes all dependencies. Subscription
	// snapshots may be newer than that first S; choose the next real view before
	// requiring a reusable certificate.
	initial, err = snapshots.ReadSnapshot(ctx, timestamp.Timestamp{})
	require.NoError(t, err)
	allowed, sealed, err := frontend.AuthorizationAtSnapshotForTest(ctx, ses, initial)
	require.NoError(t, err)
	require.True(t, allowed)
	require.True(t, sealed, "initial=%v captures=%d", initial, observed.captures)

	// Hold a real catalog mutation after production pre-prepare and timestamp
	// binding, before WAL/application/publication. No timestamp is fabricated.
	op, err := de.GetTxnClient().New(ctx, timestamp.Timestamp{})
	require.NoError(t, err)
	defer op.Rollback(baseCtx)
	require.NoError(t, de.Engine.New(ctx, op))
	result, err := sqlExec.Exec(ctx, "delete from mo_catalog.mo_role_privs where role_id=43", executor.Options{}.WithTxn(op))
	require.NoError(t, err)
	result.Close()
	txn, err := tae.GetDB().GetOrCreateTxnWithMeta(nil, op.Txn().ID, types.TimestampToTS(op.SnapshotTS()))
	require.NoError(t, err)
	prepared := make(chan timestamp.Timestamp, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	txn.SetPrepareCommitFn(func(tx txnif.AsyncTxn) error {
		if err := tx.GetStore().PrepareCommit(); err != nil {
			return err
		}
		prepared <- tx.GetPrepareTS().ToTimestamp()
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	committed := make(chan error, 1)
	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		committed <- op.Commit(ctx)
	}()
	defer func() {
		unblock()
		cancel()
		<-commitDone
	}()
	var commitTS timestamp.Timestamp
	select {
	case commitTS = <-prepared:
	case err := <-committed:
		t.Fatalf("commit before prepare barrier: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	s, err := snapshots.ReadSnapshot(ctx, timestamp.Timestamp{})
	require.NoError(t, err)
	require.True(t, commitTS.Greater(s), "catalog mutation C=%v must be beyond inclusive snapshot S=%v even with a frozen physical clock", commitTS, s)
	observed.captures = 0
	transactions.opens = 0
	observed.afterCapture = func() {
		unblock()
		require.NoError(t, <-committed)
		// Use the fixed-view admission's applied-timestamp waiter directly;
		// the deliberately future TN clock need not match the fixture CN clock.
		fence, err := de.GetTxnClient().New(ctx, commitTS.Next(), client.WithReadOnlySnapshot(commitTS.Next()))
		require.NoError(t, err)
		require.NoError(t, fence.Rollback(ctx))
	}

	allowed, _, err = frontend.AuthorizationAtSnapshotForTest(ctx, ses, s)
	require.NoError(t, err)
	require.True(t, allowed)
	require.Equal(t, 1, observed.captures, "a later publication cannot invalidate the fixed logical view")
	require.Zero(t, transactions.opens, "warm admission must not create an authorization transaction")
	t.Logf("warm capture S=%v C=%v", s, commitTS)

	oracle := func(snapshot timestamp.Timestamp) bool {
		read, err := de.GetTxnClient().New(ctx, snapshot, client.WithReadOnlySnapshot(snapshot))
		require.NoError(t, err)
		defer read.Rollback(baseCtx)
		require.NoError(t, de.Engine.New(ctx, read))
		query := "select count(*) from mo_catalog.mo_user u join mo_catalog.mo_user_grant g on u.user_id=g.user_id join mo_catalog.mo_role_privs p on p.role_id=g.role_id where u.user_id=42 and u.user_name='auth_u' and g.role_id=43"
		result, err := sqlExec.Exec(ctx, query, executor.Options{}.WithTxn(read))
		require.NoError(t, err)
		defer result.Close()
		var count int64
		result.ReadRows(func(rows int, cols []*vector.Vector) bool {
			count = vector.GetFixedAtNoTypeCheck[int64](cols[0], 0)
			return true
		})
		require.Equal(t, snapshot, read.SnapshotTS())
		return count > 0
	}
	require.Equal(t, allowed, oracle(s), "same-S SQL must retain the grant revoked later")
	require.False(t, oracle(commitTS), "SQL includes the exact committed timestamp")
	next, err := snapshots.ReadSnapshot(ctx, commitTS)
	require.NoError(t, err)
	allowed, _, err = frontend.AuthorizationAtSnapshotForTest(ctx, ses, next)
	require.NoError(t, err)
	require.False(t, allowed)
	require.Equal(t, allowed, oracle(next))
	allowed, sealed, err = frontend.AuthorizationAtSnapshotForTest(ctx, ses, s)
	require.NoError(t, err)
	require.True(t, allowed)
	require.False(t, sealed, "old SQL cannot certify a newer physical revision")
}
