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
	"strings"
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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	executortest "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The mock engine does not create a remote workspace; provide the same
// transaction boundary required by real remote execution.
type roleRPCClient struct{ client.TxnClient }

func (c roleRPCClient) NewWithSnapshot(ctx context.Context, snap txn.CNTxnSnapshot) (client.TxnOperator, error) {
	op, err := c.TxnClient.NewWithSnapshot(ctx, snap)
	if err == nil {
		op.AddWorkspace(&Ws{})
	}
	return op, err
}

// Exercise identity serialization, rejection and role closure over a real pipeline RPC.
// The transaction client reconstructs a mirror of a live transaction; the
// mock SQL executor supplies catalog rows and the engine supplies a message center.
func TestCurrentRoleSnapshotPipelineRPC(t *testing.T) {
	currentRoleSnapshotPipelineRPC(t, false, false)
}

func TestCurrentRoleSnapshotPipelineStopSending(t *testing.T) {
	currentRoleSnapshotPipelineRPC(t, true, false)
}

func TestCurrentRoleSnapshotPipelineRetryableError(t *testing.T) {
	currentRoleSnapshotPipelineRPC(t, false, true)
}

func currentRoleSnapshotPipelineRPC(t *testing.T, stopSending, retryError bool) {
	moruntime.RunTest(t.Name(), func(rt moruntime.Runtime) {
		colexec.NewServer(t.Name())
		sender, err := rpc.NewSender(rpc.Config{}, rt)
		require.NoError(t, err)
		txnClient := client.NewTxnClient(t.Name(), sender)
		txnClient.Resume()
		defer func() { require.NoError(t, txnClient.Close()) }()
		ctrl := gomock.NewController(t)
		engine := mock_frontend.NewMockEngine(ctrl)
		center := &message.MessageCenter{StmtIDToBoard: make(map[uuid.UUID]*message.MessageBoard), RwMutex: &sync.Mutex{}}
		engine.EXPECT().GetMessageCenter().Return(center).AnyTimes()
		resultMP := mpool.MustNewZero()
		sqlExec := executortest.NewMockSQLExecutor(ctrl)
		rt.SetGlobalVariables(moruntime.InternalSQLExecutor, sqlExec)
		var queries []string
		var expectedTxnID []byte
		var blockQuery atomic.Bool
		var failQuery atomic.Bool
		queryStarted := make(chan struct{})
		queryCanceled := make(chan struct{})
		releaseQuery := make(chan struct{})
		sqlExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
			require.NotNil(t, opts.Txn())
			require.Equal(t, expectedTxnID, opts.Txn().Txn().ID)
			require.True(t, opts.KeepTxnAlive())
			if failQuery.Load() {
				return executor.Result{}, moerr.NewTxnNeedRetry(ctx)
			}
			if blockQuery.Load() {
				close(queryStarted)
				select {
				case <-ctx.Done():
					close(queryCanceled)
					return executor.Result{}, ctx.Err()
				case <-releaseQuery:
					return executor.Result{}, context.Canceled
				}
			}
			queries = append(queries, sql)
			var rows []int64
			switch {
			case strings.Contains(sql, "mo_account"):
				require.Equal(t, uint32(0), opts.AccountID())
				rows = []int64{7}
			case strings.Contains(sql, "mo_user WHERE"):
				require.Equal(t, uint32(7), opts.AccountID())
				rows = []int64{8}
			case strings.Contains(sql, "mo_user_grant"):
				rows = []int64{9}
			case strings.Contains(sql, "grantee_id IN (9)"):
				rows = []int64{10}
			case strings.Contains(sql, "grantee_id IN (10)"):
				rows = []int64{9}
			default:
				t.Errorf("unexpected SQL: %s", sql)
			}
			result := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, resultMP)
			result.NewBatchWithRowCount(len(rows))
			require.NoError(t, executor.AppendFixedRows(result, 0, rows))
			return result.GetResult(), nil
		}).AnyTimes()
		lock := mock_lock.NewMockLockService(ctrl)
		lock.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: t.Name()}).AnyTimes()
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addr := listener.Addr().String()
		require.NoError(t, listener.Close())
		server, err := morpc.NewRPCServer(t.Name(), addr, morpc.NewMessageCodec(t.Name(), func() morpc.Message { return cnclient.AcquireMessage() }), morpc.WithServerDisableAutoCancelContext())
		require.NoError(t, err)
		identities := make(chan process.CurrentRoleContext, 2)
		completed := make(chan pipelineStreamLifecycleKey, 16)
		finishCompleted := make(chan struct{}, 1)
		stopCompleted := make(chan struct{}, 1)
		keys := make(chan pipelineStreamLifecycleKey, 2)
		server.RegisterRequestHandler(func(ctx context.Context, msg morpc.RPCMessage, _ uint64, cs morpc.ClientSession) error {
			// Match the CN server: handlers run concurrently so FIN can arrive
			// while the original request waits for it.
			go func() {
				if msg.Cancel != nil {
					defer msg.Cancel()
				}
				request := msg.Message.(*pipeline.Message)
				if request.GetCmd() == pipeline.Method_StopSending {
					defer func() { stopCompleted <- struct{}{} }()
				}
				if request.GetCmd() == pipeline.Method_PipelineStreamFinish {
					defer func() { finishCompleted <- struct{}{} }()
				}
				if request.GetCmd() == pipeline.Method_PipelineMessage {
					key := pipelineStreamLifecycleKey{session: cs, id: request.GetID()}
					defer func() { completed <- key }()
					keys <- key
					info := new(pipeline.ProcessInfo)
					if err := info.Unmarshal(request.ProcInfoData); err != nil {
						t.Error(err)
						return
					}
					session, err := process.ConvertToProcessSessionInfo(info.SessionInfo)
					if err != nil {
						t.Error(err)
						return
					}
					identities <- session.CurrentRoleContext
				}
				if err := CnServerMessageHandler(ctx, addr, request, cs, engine, nil, lock, nil, nil, nil, roleRPCClient{txnClient}, nil, func() morpc.Message { return cnclient.AcquireMessage() }); err != nil && !blockQuery.Load() {
					t.Error(err)
				}
			}()
			return nil
		})
		require.NoError(t, server.Start())
		defer func() { require.NoError(t, server.Close()) }()
		defer close(releaseQuery)
		pc, err := cnclient.NewPipelineClient(t.Name(), "", &cnclient.PipelineConfig{})
		require.NoError(t, err)
		defer func() { _ = pc.Close() }()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		defer cancel()
		for i, id := range []process.CurrentRoleContext{{Mode: 1, AccountID: 7, UserID: 8, PrimaryRoleID: 9, AccountName: "tenant", UserName: "alice"}, {Mode: 2, AccountID: 7, UserID: 8, PrimaryRoleID: 10, AccountName: "tenant", UserName: "alice"}, {Mode: 1, AccountID: 7, UserID: 8, PrimaryRoleID: 9, AccountName: "tenant", UserName: "alice"}, {Mode: 1, AccountID: 8, UserID: 8, PrimaryRoleID: 9, AccountName: "tenant", UserName: "alice"}, {Mode: 1, AccountID: 7, UserID: 8, PrimaryRoleID: 9, AccountName: "tenant", UserName: "alice"}, {Mode: 1, AccountID: 7, UserID: 8, PrimaryRoleID: 9, AccountName: "tenant", UserName: "alice"}, {Mode: 1, AccountID: 7, UserID: 8, PrimaryRoleID: 9, AccountName: "tenant", UserName: "alice"}} {
			if i == 4 && retryError {
				failQuery.Store(true)
			}
			if i == 6 {
				pc, err = cnclient.NewPipelineClient(t.Name(), "", &cnclient.PipelineConfig{})
				require.NoError(t, err)
				blockQuery.Store(true)
			}
			stream, err := pc.NewStream(ctx, addr)
			require.NoError(t, err)
			responses, err := stream.Receive()
			require.NoError(t, err)
			proc := process.NewTopProcess(defines.AttachAccountId(context.Background(), 7), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			proc.Base.SessionInfo.TimeZone = time.UTC
			proc.Base.SessionInfo.CurrentRoleContext = id
			// BuildProcessInfo obtains the snapshot from an active client transaction.
			original, err := txnClient.New(ctx, timestamp.Timestamp{})
			require.NoError(t, err)
			expectedTxnID = append(expectedTxnID[:0], original.Txn().ID...)
			proc.Base.TxnOperator = original
			data, err := encodeProcessInfo(proc, "", nil, [16]byte{})
			require.NoError(t, err)
			scopeData := []byte{0xff}
			wantError := "unexpected EOF"
			if i >= 2 {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion12))
				tf := &table_function.TableFunction{FuncName: "mo_current_roles", Attrs: []string{"role_id"}, Rets: []*plan.ColDef{{Name: "role_id", Typ: plan.Type{Id: int32(types.T_int64)}}}}
				scopeData, err = encodeScope(&Scope{Proc: proc, RootOp: tf})
				require.NoError(t, err)
				if i == 2 {
					// Downgrade after encoding but before sending the request.
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, int64(defines.MORPCVersion11))
				}
				wantError = "mo_current_roles requires protocol version 12"
				if i == 3 {
					wantError = "current role context account does not match execution account"
				}
			}
			request := &pipeline.Message{Id: stream.ID(), Sid: pipeline.Status_Last, Cmd: pipeline.Method_PipelineMessage, ProcInfoData: data, Data: scopeData}
			if i >= 4 {
				request.RequestedTeardownMode = pipeline.StreamTeardownMode_FinishAck
			}
			require.NoError(t, stream.Send(ctx, request))
			key := <-keys
			select {
			case got := <-identities:
				require.Equal(t, id, got)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if i == 6 {
				select {
				case <-queryStarted:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if stopSending {
					require.NoError(t, stream.Send(ctx, generateStopSendingMessage(stream.ID())))
					select {
					case <-stopCompleted:
					case <-ctx.Done():
						t.Fatal("StopSending handler did not finish")
					}
				} else {
					require.NoError(t, pc.Close())
				}
				select {
				case <-queryCanceled:
				case <-ctx.Done():
					t.Fatal("running catalog query was not canceled")
				}
				if stopSending {
					select {
					case msg := <-responses:
						require.Equal(t, pipeline.Status_MessageEnd, msg.(*pipeline.Message).Sid)
					case <-ctx.Done():
						t.Fatal("remote pipeline did not send End")
					}
					require.NoError(t, stream.Send(ctx, generatePipelineStreamFinishMessage(stream.ID())))
					select {
					case msg := <-responses:
						require.Equal(t, pipeline.Method_PipelineStreamFinishAck, msg.(*pipeline.Message).Cmd)
					case <-ctx.Done():
						t.Fatal("remote pipeline did not acknowledge FIN")
					}
					<-finishCompleted
				}
				select {
				case finished := <-completed:
					require.Equal(t, key, finished)
					_, exists := pipelineStreamLifecycles.Load(key)
					require.False(t, exists)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				require.Empty(t, center.StmtIDToBoard)
				require.Zero(t, resultMP.CurrNB())
				require.NoError(t, original.Rollback(context.Background()))
				break
			}
			if i >= 4 && !failQuery.Load() {
				select {
				case msg := <-responses:
					response := msg.(*pipeline.Message)
					require.Equal(t, pipeline.Method_BatchMessage, response.Cmd)
					bat := new(batch.Batch)
					require.NoError(t, bat.UnmarshalBinary(response.Data))
					require.Equal(t, []int64{9, 10}, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0]))
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case msg := <-responses:
				require.NotNil(t, msg)
				response := msg.(*pipeline.Message)
				require.Equal(t, pipeline.Status_MessageEnd, response.Sid)
				remoteErr, ok := response.TryToGetMoErr()
				if failQuery.Load() {
					require.True(t, ok)
					require.True(t, moerr.IsMoErrCode(remoteErr, moerr.ErrTxnNeedRetry))
					tx := mock_frontend.NewMockTxnOperator(ctrl)
					tx.EXPECT().Txn().Return(txn.TxnMeta{Isolation: txn.TxnIsolation_RC}).AnyTimes()
					checkProc := process.NewTopProcess(context.Background(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
					checkProc.Base.TxnOperator = tx
					retryCompile := &Compile{proc: checkProc}
					require.True(t, retryCompile.canRetry(remoteErr))
					retryCompile.disableRetry = true
					require.False(t, retryCompile.canRetry(remoteErr))
				} else if i >= 4 {
					require.False(t, ok)
					require.Equal(t, pipeline.StreamTeardownMode_FinishAck, response.AcceptedTeardownMode)
					require.Len(t, queries, (i-3)*5)
					require.Zero(t, resultMP.CurrNB())
				} else {
					require.True(t, ok)
					require.ErrorContains(t, remoteErr, wantError)
					require.Empty(t, queries, "rejected requests must not read the catalog")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if i == 4 {
				require.NoError(t, stream.Send(ctx, generatePipelineStreamFinishMessage(stream.ID())))
				select {
				case msg := <-responses:
					ack := msg.(*pipeline.Message)
					require.Equal(t, pipeline.Method_PipelineStreamFinishAck, ack.Cmd)
					require.Equal(t, pipeline.Status_MessageEnd, ack.Sid)
					require.Equal(t, pipeline.StreamTeardownMode_FinishAck, ack.AcceptedTeardownMode)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case <-finishCompleted:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			} else if i == 5 {
				// Drop the transport after End without sending FIN. The server must
				// release the negotiated lifecycle when its connection closes.
				require.NoError(t, pc.Close())
			}
			if i >= 4 {
				for {
					select {
					case finished := <-completed:
						if finished != key {
							continue
						}
						_, exists := pipelineStreamLifecycles.Load(key)
						require.False(t, exists)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					break
				}
			}
			if i != 5 {
				require.NoError(t, stream.Close(true))
			}
			require.Empty(t, center.StmtIDToBoard)
			require.NoError(t, original.Rollback(ctx))
			if failQuery.Load() {
				break
			}
		}
	})
}
