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

package compile

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mockfrontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mocklock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type retrySnapshotTxn struct {
	client.TxnOperator
	snapshot txn.CNTxnSnapshot
}

func (t retrySnapshotTxn) TryEnterRunSqlWithTokenAndSQL(cancel context.CancelFunc, sql string) (uint64, error) {
	return t.TxnOperator.(interface {
		TryEnterRunSqlWithTokenAndSQL(context.CancelFunc, string) (uint64, error)
	}).TryEnterRunSqlWithTokenAndSQL(cancel, sql)
}
func (t retrySnapshotTxn) Snapshot() (txn.CNTxnSnapshot, error) { return t.snapshot, nil }

// A real remote catalog failure reaches Run via RemoteRun. The retry plan is a
// simple local query, so this proves the retry transition, not a distributed
// replan of the original role query.
func TestCurrentRolesRemoteErrorRunsAutomaticRetry(t *testing.T) {
	moruntime.RunTest(t.Name(), func(rt moruntime.Runtime) {
		colexec.NewServer(t.Name())
		sender, err := rpc.NewSender(rpc.Config{}, rt)
		require.NoError(t, err)
		tc := client.NewTxnClient(t.Name(), sender)
		tc.Resume()
		defer func() { require.NoError(t, tc.Close()) }()
		ctx := defines.AttachAccountId(context.Background(), 7)
		original, err := tc.New(ctx, timestamp.Timestamp{})
		require.NoError(t, err)
		defer func() { require.NoError(t, original.Rollback(context.Background())) }()
		snap, err := original.Snapshot()
		require.NoError(t, err)
		ctrl := gomock.NewController(t)
		eng := mockfrontend.NewMockEngine(ctrl)
		center := &message.MessageCenter{StmtIDToBoard: make(map[uuid.UUID]*message.MessageBoard), RwMutex: &sync.Mutex{}}
		eng.EXPECT().GetMessageCenter().Return(center).AnyTimes()
		sql := executortest.NewMockSQLExecutor(ctrl)
		rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sql)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
		var calls atomic.Int32
		sql.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, _ string, _ executor.Options) (executor.Result, error) {
			calls.Add(1)
			return executor.Result{}, moerr.NewTxnNeedRetry(ctx)
		}).AnyTimes()
		lock := mocklock.NewMockLockService(ctrl)
		lock.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: t.Name()}).AnyTimes()
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := ln.Addr().String()
		require.NoError(t, ln.Close())
		server, err := morpc.NewRPCServer(t.Name(), addr, morpc.NewMessageCodec(t.Name(), func() morpc.Message { return cnclient.AcquireMessage() }), morpc.WithServerDisableAutoCancelContext())
		require.NoError(t, err)
		completed := make(chan struct{}, 1)
		remoteSession := make(chan morpc.ClientSession, 1)
		server.RegisterRequestHandler(func(ctx context.Context, msg morpc.RPCMessage, _ uint64, cs morpc.ClientSession) error {
			go func() {
				if msg.Cancel != nil {
					defer msg.Cancel()
				}
				req := msg.Message.(*pipeline.Message)
				if req.GetCmd() == pipeline.Method_PipelineMessage {
					remoteSession <- cs
					defer func() { completed <- struct{}{} }()
				}
				if err := CnServerMessageHandler(ctx, addr, req, cs, eng, nil, lock, nil, nil, nil, roleRPCClient{tc}, nil, func() morpc.Message { return cnclient.AcquireMessage() }); err != nil {
					t.Error(err)
				}
			}()
			return nil
		})
		require.NoError(t, server.Start())
		defer func() { require.NoError(t, server.Close()) }()
		pc, err := cnclient.NewPipelineClient(t.Name(), "", &cnclient.PipelineConfig{})
		require.NoError(t, err)
		defer pc.Close()
		rt.SetGlobalVariables(moruntime.PipelineClient, pc)
		proc := testutil.NewProcessWithMPool(t, t.Name(), mpool.MustNewZero())
		proc.Base.LockService = lock
		txnCli, mocked := newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = retrySnapshotTxn{TxnOperator: mocked, snapshot: snap}
		proc.Base.SessionInfo.TimeZone = time.UTC
		proc.Base.SessionInfo.CurrentRoleContext = process.CurrentRoleContext{Mode: 1, AccountID: 7, UserID: 8, PrimaryRoleID: 9, AccountName: "tenant", UserName: "alice"}
		proc.ReplaceTopCtx(ctx)
		compilerCtx := plan2.NewEmptyCompilerContext()
		compilerCtx.SetContext(ctx)
		stmts, err := mysql.Parse(ctx, "select 1", 1)
		require.NoError(t, err)
		query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
		require.NoError(t, err)
		pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
		var rows atomic.Int32
		var c *Compile
		fill := func(b *batch.Batch, _ *perfcounter.CounterSet) error {
			if b != nil {
				require.NotNil(t, c.currentRolesCache, "original attempt retains its cache until Run exits")
				require.False(t, table_function.HasCurrentRolesExecutionCache(proc.Ctx), "retry without role scan masks the inherited cache")
				rows.Add(int32(b.RowCount()))
			}
			return nil
		}
		c = NewCompile("127.0.0.1:1", "test", "select 1", "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
		require.NoError(t, c.Compile(ctx, pn, fill))
		oldScopes := c.scopes
		tf := &table_function.TableFunction{FuncName: "mo_current_roles", Attrs: []string{"role_id"}, Rets: []*plan.ColDef{{Name: "role_id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
		c.scopes = []*Scope{{Magic: Remote, NodeInfo: engine.Node{Addr: addr, Mcpu: 1}, Proc: proc.NewNoContextChildProc(0), RootOp: tf}}
		defer func() { c.scopes = oldScopes; c.Release(); proc.Free() }()
		_, err = c.Run(0)
		require.NoError(t, err)
		require.Equal(t, 1, c.retryTimes)
		require.EqualValues(t, 1, rows.Load())
		require.EqualValues(t, 1, calls.Load())
		require.NoError(t, pc.Close())
		// The server detects disconnected sockets on a ten-second scan. Close
		// the test-owned session explicitly so this test does not race that scan.
		select {
		case session := <-remoteSession:
			require.NoError(t, session.Close())
		case <-time.After(10 * time.Second):
			t.Fatal("remote session was not registered")
		}
		select {
		case <-completed:
		case <-time.After(10 * time.Second):
			t.Fatal("remote handler did not finish")
		}
		require.Nil(t, c.currentRolesCache)
		require.Empty(t, center.StmtIDToBoard)
	})
}
