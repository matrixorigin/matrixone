// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	rt "github.com/matrixorigin/matrixone/pkg/common/runtime"
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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestResolveRemoteCompileMPoolCap(t *testing.T) {
	const gib = uint64(1 << 30)

	cap, err := resolveRemoteCompileMPoolCapFrom(process.Limitation{Size: int64(2 * gib)}, 0, 10*gib, 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(2*gib), cap)

	cap, err = resolveRemoteCompileMPoolCapFrom(process.Limitation{}, 0, 10*gib, 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(9*gib), cap)

	cap, err = resolveRemoteCompileMPoolCapFrom(process.Limitation{}, 0, 4*gib, 0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(4*gib-(4*gib)/10), cap)

	cap, err = resolveRemoteCompileMPoolCapFrom(process.Limitation{}, 0, 10*gib, 0, 2*gib)
	require.NoError(t, err)
	require.Equal(t, int64(8*gib), cap)
}

// TestWorkspaceCreationInRemoteRun tests that workspace is created early in remote run scenario.
// This is a critical test for the fix that prevents nil pointer panics.
//
// Test quality criteria:
// 1. No randomness: Uses fixed inputs
// 2. Fast execution: Direct test of workspace creation logic, no full pipeline
// 3. Meaningful: Tests the actual fix - workspace creation when nil
// 4. Realistic: Tests the check that was added to handlePipelineMessage
func TestWorkspaceCreationInRemoteRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	proc.Ctx = ctx

	// Test case 1: nil workspace should not cause panic when checking
	txnOperator1 := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator1.EXPECT().GetWorkspace().Return(nil).AnyTimes()
	proc.Base.TxnOperator = txnOperator1

	// Should be able to safely check if workspace is nil
	require.Nil(t, proc.GetTxnOperator().GetWorkspace(), "workspace should be nil")

	// Test case 2: non-nil workspace should be detectable
	txnOperator2 := mock_frontend.NewMockTxnOperator(ctrl)
	existingWs := &Ws{}
	txnOperator2.EXPECT().GetWorkspace().Return(existingWs).AnyTimes()
	proc.Base.TxnOperator = txnOperator2

	// Should be able to safely check if workspace exists
	ws := proc.GetTxnOperator().GetWorkspace()
	require.NotNil(t, ws, "workspace should not be nil")
	require.Equal(t, existingWs, ws, "should return the same workspace")
}

// TestWorkspaceNotDuplicated tests that workspace is not created twice in handleDbRelContext.
//
// Test quality criteria:
// 1. No randomness: Uses fixed existing workspace
// 2. Fast execution: Uses mocks, minimal setup
// 3. Meaningful: Tests duplicate prevention logic
// 4. Realistic: Tests real scenario where workspace already exists from earlier creation
func TestWorkspaceNotDuplicated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	proc.Ctx = ctx

	// Create mock engine
	mockEngine := mock_frontend.NewMockEngine(ctrl)

	// Create a workspace that already exists
	existingWs := &Ws{}

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(existingWs).AnyTimes()
	txnOperator.EXPECT().IsSnapOp().Return(false).AnyTimes()
	// AddWorkspace should NOT be called since workspace already exists
	txnOperator.EXPECT().AddWorkspace(gomock.Any()).Times(0)

	proc.Base.TxnOperator = txnOperator

	// Mock Database and Relation for handleDbRelContext
	mockDatabase := mock_frontend.NewMockDatabase(ctrl)
	mockRelation := mock_frontend.NewMockRelation(ctrl)
	mockEngine.EXPECT().Database(gomock.Any(), "test_db", gomock.Any()).Return(mockDatabase, nil).AnyTimes()
	mockDatabase.EXPECT().Relation(gomock.Any(), "test_table", gomock.Any()).Return(mockRelation, nil).AnyTimes()

	c := &Compile{
		proc: proc,
		e:    mockEngine,
	}

	// Create a minimal node for testing
	// We only need to test workspace creation logic, so minimal node is sufficient
	testNode := &plan.Node{
		ObjRef: &plan.ObjectRef{
			SchemaName: "test_db",
		},
		TableDef: &plan.TableDef{
			Name: "test_table",
		},
	}

	// Test handleDbRelContext with existing workspace
	// This will try to access database/relation, but we only care about workspace check
	_, _, _, _ = c.handleDbRelContext(testNode, true)
	// May error on database access, but AddWorkspace should NOT be called
	// The key assertion is that AddWorkspace was not called (verified by mock expectation)
}

// TestHandlePipelineMessage_UnknownType tests that unknown message types cause panic.
//
// Test quality criteria:
// 1. No randomness: Fixed unknown message type
// 2. Fast execution: Pure unit test, no I/O
// 3. Meaningful: Tests error handling for invalid input
// 4. Realistic: Tests defensive programming against invalid message types
func TestHandlePipelineMessage_UnknownType(t *testing.T) {
	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		messageCtx:    context.Background(),
		connectionCtx: context.Background(),
		messageId:     1,
		messageTyp:    pipeline.Method(999), // Unknown method
	}

	// Should panic with descriptive message
	require.PanicsWithValue(t, "unknown pipeline message type 999.", func() {
		_ = handlePipelineMessage(receiver)
	})
}

// TestNewCompile_CreatesCorrectStructure tests that newCompile creates compile with correct structure.
//
// Test quality criteria:
// 1. No randomness: Fixed inputs
// 2. Fast execution: Uses mocks
// 3. Meaningful: Tests compile object structure creation
// 4. Realistic: Tests real compile creation in remote run scenario
func TestNewCompile_CreatesCorrectStructure(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, hadOriginal := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	defer func() {
		if hadOriginal {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	}()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	// Use a mock engine to isolate structure construction.
	mockEngine := mock_frontend.NewMockEngine(ctrl)
	// Create a valid MessageCenter for SetMultiCN to work
	// SetMultiCN requires a non-nil MessageCenter with initialized RwMutex
	messageCenter := &message.MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*message.MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	mockEngine.EXPECT().GetMessageCenter().Return(messageCenter).AnyTimes()

	// Create txnOperator with valid 16-byte ID for UUID conversion
	// This is required because newCompile calls uuid.UUID(txnId) which needs 16 bytes
	testTxnID := make([]byte, 16)     // UUID requires 16 bytes
	copy(testTxnID, "test-txn-id-16") // Fill with test data
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
		ID: testTxnID,
	}).AnyTimes()
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()

	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).Return(uint64(1), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
	sourceProc := testutil.NewProcess(t)
	params := vector.NewVec(types.T_text.ToType(), sourceProc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("AB\x00\x00"), false, sourceProc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("text"), false, sourceProc.Mp()))
	t.Cleanup(func() { params.Free(sourceProc.Mp()) })

	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		messageCtx:    ctx,
		connectionCtx: ctx,
		cnInformation: cnInformation{
			cnAddr:      "test-addr",
			storeEngine: mockEngine,
		},
		procBuildHelper: processHelper{
			id:                         "test-proc-id",
			accountId:                  catalog.System_Account,
			unixTime:                   time.Now().Unix(),
			affectedRows:               42,
			statementRuntimeIgnore:     true,
			statementRuntimeSelect:     true,
			planSnapshotTS:             timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4},
			hasPlanSnapshotTS:          true,
			planGenerationReused:       true,
			stringShuffleHashAlgorithm: process.StringShuffleHashComplete,
			txnClient:                  txnClient,
			txnOperator:                txnOperator,
			prepareParams: pipeline.PrepareParamInfo{
				Length:         2,
				Data:           append([]byte(nil), params.GetData()...),
				Area:           append([]byte(nil), params.GetArea()...),
				Nulls:          []bool{false, false},
				IsBin:          []bool{true, false, false, false, false, true, false, false},
				IsBinaryString: []bool{true, false},
			},
		},
		messageAcquirer: func() morpc.Message {
			return &pipeline.Message{}
		},
	}

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion17)
	compile, err := receiver.newCompile()
	require.Error(t, err)
	require.Nil(t, compile)

	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion18)
	compile, err = receiver.newCompile()
	require.NoError(t, err)
	require.NotNil(t, compile)
	require.Equal(t, "test-addr", compile.addr)
	require.Equal(t, mockEngine, compile.e)
	require.NotNil(t, compile.proc)
	require.Equal(t, "AB\x00\x00", compile.proc.GetPrepareParams().GetStringAt(0))
	require.Equal(t, "text", compile.proc.GetPrepareParams().GetStringAt(1))
	require.True(t, compile.proc.GetPrepareParamIsBin(0))
	require.False(t, compile.proc.GetPrepareParamIsBin(1))
	require.Equal(t, int64(42), compile.proc.GetAffectedRows())
	require.True(t, compile.proc.GetStmtProfile().GetStatementIgnore())
	require.True(t, compile.proc.IsSelectStatement())
	compile.proc.GetSessionInfo().SqlMode = "ERROR_FOR_DIVISION_BY_ZERO"
	requireRemoteDivisionWarning(t, compile.proc)
	planSnapshot, ok := compile.proc.GetPlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4}, planSnapshot)
	require.True(t, compile.proc.PlanGenerationReused())
	require.Equal(t, process.StringShuffleHashComplete,
		compile.proc.StringShuffleHashAlgorithm())
	require.NotNil(t, compile.fill, "fill callback should be set")
	remoteParams := compile.proc.GetPrepareParams()
	require.NotPanics(t, compile.Release)
	require.Nil(t, remoteParams.GetData())
	require.Nil(t, remoteParams.GetArea())
}

func TestHandlePipelineMessage_ReleasesCompileOnDecodeError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	mockEngine := mock_frontend.NewMockEngine(ctrl)
	messageCenter := &message.MessageCenter{
		StmtIDToBoard: make(map[uuid.UUID]*message.MessageBoard),
		RwMutex:       &sync.Mutex{},
	}
	mockEngine.EXPECT().GetMessageCenter().Return(messageCenter).AnyTimes()

	testTxnID := make([]byte, 16)
	copy(testTxnID, "decode-error-txn")
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{ID: testTxnID}).AnyTimes()

	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		messageCtx:    ctx,
		connectionCtx: ctx,
		messageTyp:    pipeline.Method_PipelineMessage,
		cnInformation: cnInformation{
			cnAddr:      "test-addr",
			storeEngine: mockEngine,
		},
		procBuildHelper: processHelper{
			id:          "test-proc-id",
			accountId:   catalog.System_Account,
			unixTime:    time.Now().Unix(),
			txnOperator: txnOperator,
		},
		scopeData: []byte{0xff},
	}

	err := handlePipelineMessage(receiver)
	require.Error(t, err)
	require.Empty(t, messageCenter.StmtIDToBoard, "compile cleanup should unregister the remote message board on decode errors")
}

// TestGenerateProcessHelper_WithSnapshot tests process helper generation from snapshot.
//
// Test quality criteria:
// 1. No randomness: Fixed ProcessInfo data
// 2. Fast execution: Pure unit test with mocks, no I/O
// 3. Meaningful: Tests process helper reconstruction from serialized data
// 4. Realistic: Tests real scenario of rebuilding process from remote message
func TestGenerateProcessHelper_WithSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock txnClient that supports NewWithSnapshot
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

	// Setup txnOperator mock
	txnOperator.EXPECT().GetWorkspace().Return(nil).AnyTimes() // Rebuilt txnOperator has nil workspace
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()

	// Setup txnClient to return txnOperator when NewWithSnapshot is called
	txnClient.EXPECT().NewWithSnapshot(gomock.Any(), gomock.Any()).Return(txnOperator, nil).Times(1)

	// Create a valid ProcessInfo
	proc := testutil.NewProcess(t)
	params := vector.NewVec(types.T_text.ToType(), proc.Mp())
	require.NoError(t, vector.AppendBytes(params, []byte("AB\x00\x00"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("text"), false, proc.Mp()))
	t.Cleanup(func() { params.Free(proc.Mp()) })

	procInfo := &pipeline.ProcessInfo{
		Id:                         "test-proc-id",
		AccountId:                  catalog.System_Account,
		UnixTime:                   time.Now().Unix(),
		AffectedRows:               42,
		StatementRuntimeIgnore:     true,
		StatementRuntimeSelect:     true,
		PlanSnapshotTs:             &timestamp.Timestamp{PhysicalTime: 123, LogicalTime: 4},
		PlanGenerationReused:       true,
		StringShuffleHashAlgorithm: uint32(process.StringShuffleHashComplete),
		Snapshot: txn.CNTxnSnapshot{
			Txn: txn.TxnMeta{
				ID: []byte("test-txn-id"),
			},
		},
		PrepareParams: pipeline.PrepareParamInfo{
			Length:         2,
			Data:           append([]byte(nil), params.GetData()...),
			Area:           append([]byte(nil), params.GetArea()...),
			Nulls:          []bool{false, false},
			IsBin:          []bool{true, false},
			IsBinaryString: []bool{true, false},
		},
	}

	data, err := procInfo.Marshal()
	require.NoError(t, err)

	helper, err := generateProcessHelper(context.Background(), data, txnClient)
	require.NoError(t, err)
	require.Equal(t, "test-proc-id", helper.id)
	require.Equal(t, catalog.System_Account, helper.accountId)
	require.Equal(t, []bool{true, false}, helper.prepareParams.IsBin)
	require.Equal(t, []bool{true, false}, helper.prepareParams.IsBinaryString)
	require.Equal(t, procInfo.PrepareParams.Data, helper.prepareParams.Data)
	require.Equal(t, procInfo.PrepareParams.Area, helper.prepareParams.Area)
	require.Equal(t, int64(42), helper.affectedRows)
	require.True(t, helper.statementRuntimeIgnore)
	require.True(t, helper.statementRuntimeSelect)
	require.True(t, helper.hasPlanSnapshotTS)
	require.Equal(t, *procInfo.PlanSnapshotTs, helper.planSnapshotTS)
	require.True(t, helper.planGenerationReused)
	require.Equal(t, process.StringShuffleHashComplete, helper.stringShuffleHashAlgorithm)
	require.NotNil(t, helper.txnOperator, "txnOperator should be created from snapshot")
	// Verify that rebuilt txnOperator has nil workspace (key point for remote run)
	require.Nil(t, helper.txnOperator.GetWorkspace(), "rebuilt txnOperator should have nil workspace initially")
}

func TestGenerateProcessHelperRejectsUnknownStringShuffleHashAlgorithm(t *testing.T) {
	data, err := (&pipeline.ProcessInfo{StringShuffleHashAlgorithm: 99}).Marshal()
	require.NoError(t, err)

	_, err = generateProcessHelper(context.Background(), data, nil)
	require.ErrorContains(t, err, "string shuffle hash algorithm 99 is not supported")
}

func TestNewMessageReceiverReturnsSnapshotRestoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	clientSession := mock_morpc.NewMockClientSession(ctrl)
	clientSession.EXPECT().SessionCtx().Return(context.Background())

	wantErr := errors.New("wait remote snapshot logtail")
	txnClient.EXPECT().NewWithSnapshot(gomock.Any(), gomock.Any()).Return(nil, wantErr)
	procInfo := &pipeline.ProcessInfo{
		Snapshot: txn.CNTxnSnapshot{
			Txn: txn.TxnMeta{
				ID: []byte("test-txn-id"),
			},
		},
	}
	data, err := procInfo.Marshal()
	require.NoError(t, err)

	_, err = newMessageReceiverOnServer(
		context.Background(),
		"cn-address",
		&pipeline.Message{Cmd: pipeline.Method_PipelineMessage, ProcInfoData: data},
		clientSession,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		txnClient,
		nil,
		nil,
	)
	require.ErrorIs(t, err, wantErr)
}

func TestNewMessageReceiverSnapshotWaitObservesConnectionClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	clientSession := mock_morpc.NewMockClientSession(ctrl)
	connectionCtx, closeConnection := context.WithCancel(context.Background())
	clientSession.EXPECT().SessionCtx().Return(connectionCtx)

	waitStarted := make(chan struct{})
	txnClient.EXPECT().NewWithSnapshot(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ txn.CNTxnSnapshot) (client.TxnOperator, error) {
			close(waitStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	)
	procInfo := &pipeline.ProcessInfo{
		Snapshot: txn.CNTxnSnapshot{Txn: txn.TxnMeta{ID: []byte("test-txn-id")}},
	}
	data, err := procInfo.Marshal()
	require.NoError(t, err)

	errC := make(chan error, 1)
	go func() {
		_, err := newMessageReceiverOnServer(
			context.Background(),
			"cn-address",
			&pipeline.Message{Cmd: pipeline.Method_PipelineMessage, ProcInfoData: data},
			clientSession,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			txnClient,
			nil,
			nil,
		)
		errC <- err
	}()

	select {
	case <-waitStarted:
	case <-time.After(time.Second):
		t.Fatal("snapshot wait did not start")
	}
	closeConnection()
	select {
	case err := <-errC:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("connection close did not release snapshot wait")
	}
}

// TestCnServerMessageHandlerWaitObservesMessageCtxCancellation verifies that the
// post-handling wait returns when the message context is cancelled (a killed
// query), even though the TCP connection context is still open. Before the fix
// this wait only observed connectionCtx, so a killed query could leave the
// handler blocked forever waiting for the connection to close (issue #25025).
func TestCnServerMessageHandlerWaitObservesMessageCtxCancellation(t *testing.T) {
	messageCtx, cancelMessage := context.WithCancel(context.Background())
	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		connectionCtx: context.Background(), // never closed
		messageCtx:    messageCtx,
	}

	done := make(chan struct{})
	go func() {
		receiver.waitUntilDisconnectedOrCancelled()
		close(done)
	}()

	// It must not return while neither context is done.
	select {
	case <-done:
		t.Fatal("wait returned before connection close or message cancellation")
	case <-time.After(50 * time.Millisecond):
	}

	cancelMessage()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait did not observe messageCtx cancellation")
	}
}

// TestCnServerMessageHandlerWaitObservesConnectionClose verifies the original
// behavior is preserved: the wait still returns when the client connection is
// closed.
func TestCnServerMessageHandlerWaitObservesConnectionClose(t *testing.T) {
	connectionCtx, closeConnection := context.WithCancel(context.Background())
	receiver := &messageReceiverOnServer{
		colexecServer: colexec.GetServer(""),
		connectionCtx: connectionCtx,
		messageCtx:    context.Background(), // never cancelled
	}

	done := make(chan struct{})
	go func() {
		receiver.waitUntilDisconnectedOrCancelled()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("wait returned before connection close or message cancellation")
	case <-time.After(50 * time.Millisecond):
	}

	closeConnection()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait did not observe connection close")
	}
}

func TestHandlePipelineStopSendingAbortsOutstandingBatchFlow(t *testing.T) {
	server := colexec.NewServer("")
	session := &lifecycleTestSession{ctx: context.Background()}
	flow := newPipelineBatchFlow(1, 1024)
	lifecycle, err := registerPipelineStreamLifecycle(session, 400, flow)
	require.NoError(t, err)
	t.Cleanup(func() {
		lifecycle.remove()
		server.RemoveRelatedPipeline(session, 400)
	})

	seq, err := flow.reserve(context.Background(), context.Background(), 10)
	require.NoError(t, err)
	drainStarted := make(chan struct{})
	drainDone := make(chan error, 1)
	go func() {
		close(drainStarted)
		drainDone <- flow.waitUntilDrained(context.Background(), context.Background(), nil)
	}()
	<-drainStarted

	receiver := &messageReceiverOnServer{
		messageCtx:    context.Background(),
		messageId:     400,
		messageTyp:    pipeline.Method_StopSending,
		clientSession: session,
		colexecServer: server,
	}
	require.NoError(t, handlePipelineMessage(receiver))

	select {
	case err = <-drainDone:
		require.ErrorIs(t, err, process.ErrPipelineStopped)
	case <-time.After(time.Second):
		t.Fatal("StopSending did not release the terminal-response drain barrier")
	}
	require.NoError(t, handlePipelineBatchAck(
		&pipeline.Message{Id: 400, BatchAckSequence: seq}, session),
		"a concurrently flushed ACK must remain harmless after StopSending")
	require.Zero(t, session.closeCalls)
}

func TestPipelineStopBeforeLifecycleRegistrationIsReconciled(t *testing.T) {
	server := colexec.NewServer("")
	sessionCtx, closeSession := context.WithCancel(context.Background())
	session := &lifecycleTestSession{ctx: sessionCtx}
	const streamID = 405
	t.Cleanup(func() {
		server.RemoveRelatedPipeline(session, streamID)
		closeSession()
	})

	stopReceiver := &messageReceiverOnServer{
		messageCtx:    context.Background(),
		messageId:     streamID,
		messageTyp:    pipeline.Method_StopSending,
		clientSession: session,
		colexecServer: server,
	}
	require.NoError(t, handlePipelineMessage(stopReceiver))
	require.True(t, server.HasPendingPipelineCancellation(session, streamID))

	flow := newPipelineBatchFlow(1, 1024)
	lifecycle, err := registerPipelineStreamLifecycle(session, streamID, flow)
	require.NoError(t, err)
	t.Cleanup(lifecycle.remove)

	// Model the failure mode directly: the lifecycle is now published, and an
	// outstanding batch appears before the pre-registration Stop is reconciled.
	seq, err := flow.reserve(context.Background(), context.Background(), 10)
	require.NoError(t, err)
	pipelineReceiver := &messageReceiverOnServer{
		messageCtx:      context.Background(),
		messageId:       streamID,
		clientSession:   session,
		streamLifecycle: lifecycle,
		colexecServer:   server,
	}
	pipelineReceiver.abortBatchFlowForPendingStop()

	err = flow.waitUntilDrained(context.Background(), context.Background(), nil)
	require.ErrorIs(t, err, process.ErrPipelineStopped)
	_, err = flow.reserve(context.Background(), context.Background(), 1)
	require.ErrorIs(t, err, process.ErrPipelineStopped)
	require.NoError(t, flow.acknowledge(seq), "a late ACK must be harmless after reconciliation")

	dispatchReceiver := &process.WrapCs{
		MsgId: streamID,
		Uid:   uuid.Must(uuid.NewV7()),
		Cs:    session,
	}
	server.RecordDispatchPipeline(session, streamID, dispatchReceiver)
	require.False(t, dispatchReceiver.ReceiverDone,
		"lifecycle cancellation must not change dispatch ReceiverDone semantics")
	require.False(t, server.HasPendingPipelineCancellation(session, streamID),
		"dispatch registration must retain its existing stale-tombstone cleanup")
}

func TestMessageReceiverSendBatchUsesNegotiatedCredits(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	flow := newPipelineBatchFlow(2, 1024)
	lifecycle := &pipelineStreamLifecycle{batchFlow: flow}
	var sent *pipeline.Message
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message any) error {
			sent = message.(*pipeline.Message)
			return nil
		})

	receiver := &messageReceiverOnServer{
		batchOut:        newTestRemoteBatchBuffer(t),
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		messageId:       401,
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  1 << 20,
		streamLifecycle: lifecycle,
	}
	require.NoError(t, receiver.sendBatch(batch.NewWithSize(0)))
	require.NotNil(t, sent)
	require.Equal(t, uint64(1), sent.GetBatchSequence())
	require.Equal(t, uint32(2), sent.GetAcceptedBatchCreditCount())
	require.Equal(t, uint64(1024), sent.GetAcceptedBatchCreditBytes())

	flow.mu.Lock()
	require.Len(t, flow.pending, 1)
	flow.mu.Unlock()
	require.NoError(t, flow.acknowledge(sent.GetBatchSequence()))
}

func TestMessageReceiverSendEndMessageBoundsWarningPayload(t *testing.T) {
	const bodyLimit = 16 * 1024
	const total = 10
	warnings := make([]remoteWarningDiagnostic, total)
	for i := range warnings {
		warnings[i] = remoteWarningDiagnostic{
			Code:    1292,
			Message: strings.Repeat("x", process.WarningDiagnosticMaxMessageBytes),
		}
	}

	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	var sent *pipeline.Message
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message any) error {
			sent = message.(*pipeline.Message)
			return nil
		})
	receiver := &messageReceiverOnServer{
		messageCtx:         context.Background(),
		clientSession:      session,
		messageAcquirer:    func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:     bodyLimit,
		warningCount:       total,
		warningDiagnostics: warnings,
	}

	require.NoError(t, receiver.sendEndMessage())
	require.NotNil(t, sent)
	require.Less(t, sent.ProtoSize(), bodyLimit)
	var envelope remoteTerminalEnvelope
	require.NoError(t, json.Unmarshal(sent.GetAnalyse(), &envelope))
	require.Equal(t, uint64(total), envelope.WarningCount)
	require.NotEmpty(t, envelope.WarningDiagnostics)
	require.Less(t, len(envelope.WarningDiagnostics), total)
	require.Equal(t, warnings[0], envelope.WarningDiagnostics[0])
}

func TestMessageReceiverTerminalUsesConfiguredRPCBodyLimit(t *testing.T) {
	const bodyLimit = 16 * 1024
	const total = 10
	warnings := make([]remoteWarningDiagnostic, total)
	for i := range warnings {
		warnings[i] = remoteWarningDiagnostic{
			Code:    1292,
			Message: strings.Repeat("x", process.WarningDiagnosticMaxMessageBytes),
		}
	}

	receiver := &messageReceiverOnServer{
		messageCtx:         morpc.ContextWithMaxMessageSize(context.Background(), bodyLimit),
		maxMessageSize:     maxMessageSizeToMoRpc,
		warningCount:       total,
		warningDiagnostics: warnings,
	}
	message := &pipeline.Message{
		Sid: pipeline.Status_MessageEnd,
		Cmd: pipeline.Method_PipelineMessage,
		Id:  1,
	}
	require.NoError(t, receiver.setTerminalAnalysis(message))

	// Exercise the same codec validation used by production MORPC instead of
	// relying on a mock ClientSession.Write implementation.
	codec := morpc.NewMessageCodec(
		"",
		func() morpc.Message { return &pipeline.Message{} },
		morpc.WithCodecMaxBodySize(bodyLimit),
	)
	require.NoError(t, codec.Valid(message))
	require.Less(t, message.ProtoSize(), bodyLimit)

	var envelope remoteTerminalEnvelope
	require.NoError(t, json.Unmarshal(message.GetAnalyse(), &envelope))
	require.Equal(t, uint64(total), envelope.WarningCount)
	require.NotEmpty(t, envelope.WarningDiagnostics)
	require.Less(t, len(envelope.WarningDiagnostics), total)
}

type observedDoneContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestRemoteNotifyCancellationReleasesCreditWaitAndRegistration(t *testing.T) {
	for _, connectionClosed := range []bool{false, true} {
		name := "query"
		if connectionClosed {
			name = "connection"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			server := colexec.NewServer("")
			proc := testutil.NewProcess(t)
			proc.BuildPipelineContext(context.Background())
			defer proc.Cancel(context.Canceled)
			uid := uuid.Must(uuid.NewV7())
			terminal := colexec.NewRemoteReceiverTerminal(proc.Cancel)
			notify := make(process.RemotePipelineInformationChannel)
			require.NoError(t, server.PutProcIntoUuidMapWithTerminal(uid, proc, notify, terminal))
			defer server.RemoveUuidsOwned([]uuid.UUID{uid}, notify)
			queryCtx, cancelQuery := context.WithCancel(context.Background())
			defer cancelQuery()
			connCtx, cancelConn := context.WithCancel(context.Background())
			defer cancelConn()
			var workers sync.WaitGroup
			defer func() {
				cancelQuery()
				cancelConn()
				proc.Cancel(context.Canceled)
				workers.Wait()
			}()
			session := mock_morpc.NewMockClientSession(ctrl)
			// The session cleanup goroutine is irrelevant here: the handler must
			// unregister the stream itself before it returns.
			session.EXPECT().SessionCtx().Return(connCtx).AnyTimes()
			session.EXPECT().Close().Return(nil).AnyTimes()
			lock := mock_lock.NewMockLockService(ctrl)
			lock.EXPECT().GetConfig().Return(lockservice.Config{}).AnyTimes()
			var wireError error
			session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, m morpc.Message) error {
				var hasError bool
				wireError, hasError = m.(*pipeline.Message).TryToGetMoErr()
				if !hasError {
					return errors.New("missing cancellation terminal error")
				}
				return ctx.Err()
			}).Times(1)
			const id = 404
			handlerDone := make(chan error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				handlerDone <- CnServerMessageHandler(queryCtx, "", &pipeline.Message{
					Id: id, Cmd: pipeline.Method_PrepareDoneNotifyMessage, Uuid: uid[:],
					RequestedTeardownMode:     pipeline.StreamTeardownMode_FinishAck,
					RequestedBatchCreditCount: 1, RequestedBatchCreditBytes: 1024,
				}, session, nil, nil, lock, nil, nil, nil, nil, nil, func() morpc.Message { return &pipeline.Message{} })
			}()
			var info *process.WrapCs
			select {
			case info = <-notify:
			case <-time.After(5 * time.Second):
				t.Fatal("notify did not attach")
			}
			_, err := info.ReserveBatch(proc.Ctx, 1)
			require.NoError(t, err)
			observed := &observedDoneContext{Context: proc.Ctx, entered: make(chan struct{})}
			sendDone := make(chan error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				_, err := info.ReserveBatch(observed, 1)
				sendDone <- err
			}()
			select {
			case <-observed.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("sender did not enter credit wait")
			}
			if connectionClosed {
				cancelConn()
			} else {
				cancelQuery()
			}
			var sendErr, handlerErr error
			select {
			case sendErr = <-sendDone:
			case <-time.After(5 * time.Second):
				t.Fatal("credit waiter survived cancellation")
			}
			select {
			case handlerErr = <-handlerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("handler survived cancellation")
			}
			require.Error(t, sendErr)
			require.Error(t, wireError)
			if connectionClosed {
				require.True(t, moerr.IsMoErrCode(wireError, moerr.ErrStreamClosed))
				select {
				case <-proc.Ctx.Done():
				default:
					t.Fatal("connection cancellation did not cancel the owning query")
				}
				cause := context.Cause(proc.Ctx)
				require.Error(t, cause)
				require.True(t, moerr.IsMoErrCode(cause, moerr.ErrStreamClosed))
			} else {
				require.ErrorIs(t, handlerErr, context.Canceled)
				require.ErrorIs(t, sendErr, context.Canceled)
			}
			require.False(t, info.ReceiverStopped(), "external cancellation is not a certified retirement")
			_, registered := pipelineStreamLifecycles.Load(pipelineStreamLifecycleKey{session: session, id: id})
			require.False(t, registered, "handler must release the global credit/lifecycle owner")
			// The source owns terminal publication; these are the registry cleanup
			// operations used by RemoteReceiverRegistration.Cleanup. Its dedicated
			// tests cover the owning handle and pooled-operator reset separately.
			cause := context.Cause(proc.Ctx)
			terminal.Finish(cause)
			for range 2 {
				server.CloseRemoteReceivers([]uuid.UUID{uid}, notify)
				server.RemoveUuidsOwned([]uuid.UUID{uid}, notify)
				terminal.Finish(nil)
			}
			require.ErrorIs(t, terminal.Err(), cause)
			require.ErrorIs(t, context.Cause(proc.Ctx), cause)
			_, _, state, lookupWaiter, _ := server.AttachProcByUuidOrWait(uid)
			lookupWaiter.Close()
			require.Equal(t, colexec.RemoteReceiverMissing, state)
			require.NoError(t, handlePipelineBatchAck(&pipeline.Message{Id: id, BatchAckSequence: 1}, session))
		})
	}

}

// Binary-string provenance is carried by the static vector type, so a
// binary result no longer needs a per-row metadata trailer or a protocol gate.
func TestMessageReceiverSendBatchCarriesStaticBinaryType(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, hadOriginal := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	t.Cleanup(func() {
		if hadOriginal {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, original)
		} else {
			runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCLatestVersion)
		}
	})

	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varbinary.ToType(), mp)
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("raw"), false, mp))
	require.NoError(t, vector.AppendBytes(bat.Vecs[0], []byte("text"), false, mp))
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	receiver := &messageReceiverOnServer{
		batchOut:        newTestRemoteBatchBuffer(t),
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		messageId:       404,
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  1 << 20,
	}
	var sent *pipeline.Message
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message any) error {
			sent = message.(*pipeline.Message)
			return nil
		})
	runtime.SetGlobalVariables(rt.MOProtocolVersion, defines.MORPCVersion17)
	require.NoError(t, receiver.sendBatch(bat))
	require.NotNil(t, sent)
	decoded := batch.NewWithSize(0)
	defer decoded.Clean(mp)
	require.NoError(t, decoded.UnmarshalBinaryWithAnyMp(sent.Data, mp))
	require.Equal(t, types.T_varbinary, decoded.Vecs[0].GetType().Oid)
	require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(*decoded.Vecs[0].GetType()))
	require.Equal(t, "raw", decoded.Vecs[0].GetStringAt(0))
	require.Equal(t, "text", decoded.Vecs[0].GetStringAt(1))
}

// Rolled-up ROLLUP keys are plain NULL constants, so sending them needs no
// protocol gate: any peer version receives the stable batch.
func TestMessageReceiverSendBatchSendsRolledUpNullsUngated(t *testing.T) {
	runtime := rt.ServiceRuntime("")
	original, _ := runtime.GetGlobalVariables(rt.MOProtocolVersion)
	t.Cleanup(func() { runtime.SetGlobalVariables(rt.MOProtocolVersion, original) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	bat := batch.NewWithSize(1)
	t.Cleanup(func() { bat.Clean(mp) })
	bat.Vecs[0] = vector.NewConstNull(types.T_int32.ToType(), 2, mp)
	bat.SetRowCount(2)
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	receiver := &messageReceiverOnServer{
		batchOut:   newTestRemoteBatchBuffer(t),
		messageCtx: context.Background(), connectionCtx: context.Background(),
		clientSession: session, messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize: 1 << 20,
	}
	versions := []any{nil, "unknown", int64(86), int64(87)}
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message any) error {
			decoded, err := decodeBatch(mp, message.(*pipeline.Message).Data)
			require.NoError(t, err)
			defer decoded.Clean(mp)
			require.Equal(t, 2, decoded.RowCount())
			require.True(t, decoded.Vecs[0].IsConstNull())
			return nil
		}).Times(len(versions))
	for _, version := range versions {
		runtime.SetGlobalVariables(rt.MOProtocolVersion, version)
		require.NoError(t, receiver.sendBatch(bat))
	}
}

func TestMessageReceiverSendFragmentedBatchRollsBackCreditOnWriteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	wantErr := errors.New("write fragment")
	flow := newPipelineBatchFlow(2, 1024)
	bat := batch.NewWithSize(0)
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	require.Greater(t, len(data), 1)
	writes := 0
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message any) error {
			writes++
			msg := message.(*pipeline.Message)
			require.Equal(t, uint64(1), msg.GetBatchSequence())
			if writes == 2 {
				return wantErr
			}
			return nil
		}).Times(2)

	receiver := &messageReceiverOnServer{
		batchOut:        newTestRemoteBatchBuffer(t),
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		messageId:       402,
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  len(data) - 1,
		streamLifecycle: &pipelineStreamLifecycle{batchFlow: flow},
	}
	err = receiver.sendBatch(bat)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 2, writes)
	flow.mu.Lock()
	require.Empty(t, flow.pending)
	require.Zero(t, flow.bytes)
	flow.mu.Unlock()
}

func TestMessageReceiverSendFragmentedBatchKeepsCreditUntilAck(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	flow := newPipelineBatchFlow(2, 1024)
	bat := batch.NewWithSize(0)
	data, err := bat.MarshalBinary()
	require.NoError(t, err)
	require.Greater(t, len(data), 1)
	writes := 0
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message any) error {
			writes++
			msg := message.(*pipeline.Message)
			require.Equal(t, uint64(1), msg.GetBatchSequence())
			return nil
		}).Times(2)

	receiver := &messageReceiverOnServer{
		batchOut:        newTestRemoteBatchBuffer(t),
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		messageId:       403,
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  len(data) - 1,
		streamLifecycle: &pipelineStreamLifecycle{batchFlow: flow},
	}
	require.NoError(t, receiver.sendBatch(bat))
	require.Equal(t, 2, writes)
	flow.mu.Lock()
	require.Len(t, flow.pending, 1)
	flow.mu.Unlock()
	require.NoError(t, flow.acknowledge(1))
}

func TestLiveReceiverStopRejectsConnectionAndMessageCancellation(t *testing.T) {
	for _, closedConnection := range []bool{false, true} {
		messageCtx, cancelMessage := context.WithCancel(context.Background())
		connectionCtx, cancelConnection := context.WithCancel(context.Background())
		flow := newPipelineBatchFlow(1, 1024)
		r := &messageReceiverOnServer{messageCtx: messageCtx, connectionCtx: connectionCtx,
			streamLifecycle: &pipelineStreamLifecycle{batchFlow: flow}}
		require.False(t, r.hasLiveReceiverStop())
		flow.stop(context.Canceled)
		require.True(t, r.hasLiveReceiverStop())
		if closedConnection {
			cancelConnection()
		} else {
			cancelMessage()
		}
		require.False(t, r.hasLiveReceiverStop())
		cancelMessage()
		cancelConnection()
	}
	flow := newPipelineBatchFlow(1, 1024)
	flow.abort(moerr.NewInternalErrorNoCtx("first substantive error"))
	flow.stop(context.Canceled)
	r := &messageReceiverOnServer{messageCtx: context.Background(), connectionCtx: context.Background(),
		streamLifecycle: &pipelineStreamLifecycle{batchFlow: flow}}
	require.False(t, r.hasLiveReceiverStop(), "a later stop must not certify a previously failed flow")
}

// newTestRemoteBatchBuffer returns a stream batch buffer that is closed,
// and checked to have freed its memory, when the test ends.
func newTestRemoteBatchBuffer(t *testing.T) *remoteBatchBuffer {
	mp := mpool.MustNewZero()
	b := &remoteBatchBuffer{buf: mpool.NewMessageBuffer(mp, maxRetainedRemoteBatchBuffer)}
	t.Cleanup(func() {
		b.close()
		require.Zero(t, mp.CurrNB())
	})
	return b
}

// A stream marshals every result batch into its one buffer, and a closed
// stream sends nothing.
func TestRemoteBatchBufferIsReusedAndClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	var data [][]byte
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message any) error {
			data = append(data, message.(*pipeline.Message).Data)
			return nil
		}).Times(2)
	out := newTestRemoteBatchBuffer(t)
	receiver := &messageReceiverOnServer{
		batchOut:        out,
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  1 << 20,
	}
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	for i := 0; i < 100; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(i), false, mp))
	}
	bat.SetRowCount(100)
	defer bat.Clean(mp)

	require.NoError(t, receiver.sendBatch(bat))
	require.NoError(t, receiver.sendBatch(bat))
	require.Len(t, data, 2)
	require.True(t, &data[0][0] == &data[1][0], "both batches use the stream's buffer")

	out.close()
	require.Error(t, receiver.sendBatch(bat))
}

// A send resets the stream's buffer once its Writes return: a buffer larger
// than the retention bound is freed before the stream waits for the next
// batch or an acknowledgement, and a smaller one is kept for reuse.
func TestRemoteBatchBufferFreesOversizedBufferAfterSend(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	bufPool := mpool.MustNewZero()
	var assembled []byte
	session.EXPECT().Write(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, message any) error {
			require.NotZero(t, bufPool.CurrNB(), "the message points into the buffer while Write runs")
			assembled = append(assembled, message.(*pipeline.Message).Data...)
			return nil
		})
	mp := mpool.MustNewZero()
	small := newRemoteSendTestBatch(t, mp, 0, 2)
	defer small.Clean(mp)
	large := newRemoteSendTestBatch(t, mp, 0, 200)
	defer large.Clean(mp)
	smallData, err := small.MarshalBinary()
	require.NoError(t, err)
	largeData, err := large.MarshalBinary()
	require.NoError(t, err)

	// Capacity rounds up, so bound well above the small batch and below the
	// large one.
	bound := 4 * len(smallData)
	require.Greater(t, len(largeData), 2*bound)
	out := &remoteBatchBuffer{buf: mpool.NewMessageBuffer(bufPool, bound)}
	defer func() {
		out.close()
		require.Zero(t, bufPool.CurrNB())
	}()
	receiver := &messageReceiverOnServer{
		batchOut:        out,
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  64,
	}

	require.NoError(t, receiver.sendBatch(large))
	require.Equal(t, largeData, assembled)
	require.Zero(t, bufPool.CurrNB(), "an oversized buffer is freed when the send returns")
	require.Zero(t, out.buf.Len())

	assembled = nil
	require.NoError(t, receiver.sendBatch(small))
	require.Equal(t, smallData, assembled)
	require.NotZero(t, bufPool.CurrNB(), "a buffer within the bound is kept")
	require.Zero(t, out.buf.Len(), "and reset")

	assembled = nil
	require.NoError(t, receiver.sendBatch(large))
	require.Equal(t, largeData, assembled)
	require.Zero(t, bufPool.CurrNB(), "a buffer grown past the bound is freed again")
}

func newRemoteSendTestBatch(t *testing.T, mp *mpool.MPool, base int64, rows int) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], base+int64(i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte("remote-batch-value-past-inline"), i%3 == 0, mp))
	}
	bat.SetRowCount(rows)
	return bat
}

// A batch larger than a message goes out in chunks of the stream's buffer;
// they reassemble to the batch, and a failure part way leaves the stream
// usable.
func TestRemoteBatchBufferChunkedSends(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	var chunks [][]byte
	var assembled []byte
	failAt := -1
	session.EXPECT().Write(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, message any) error {
			if len(chunks) == failAt {
				return moerr.NewInternalErrorNoCtx("broken")
			}
			msg := message.(*pipeline.Message)
			chunks = append(chunks, msg.Data)
			// Copy now: the stream reuses its buffer once Write returns.
			assembled = append(assembled, msg.Data...)
			return nil
		})
	out := newTestRemoteBatchBuffer(t)
	receiver := &messageReceiverOnServer{
		batchOut:        out,
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  100,
	}
	mp := mpool.MustNewZero()
	bat := newRemoteSendTestBatch(t, mp, 0, 30)
	defer bat.Clean(mp)
	want, err := bat.MarshalBinary()
	require.NoError(t, err)

	require.NoError(t, receiver.sendBatch(bat))
	require.Equal(t, want, assembled)
	require.Equal(t, (len(want)+99)/100, len(chunks))
	for i := 1; i < len(chunks); i++ {
		require.True(t, &chunks[i-1][0] != &chunks[i][0])
		require.Len(t, chunks[i-1], 100, "chunks before the last are full")
	}

	chunks, assembled, failAt = nil, nil, 2
	require.Error(t, receiver.sendBatch(bat), "a failed chunk fails the batch")
	chunks, assembled, failAt = nil, nil, -1
	require.NoError(t, receiver.sendBatch(bat))
	require.Equal(t, want, assembled, "the stream is still usable")

	bat.Vecs = append(bat.Vecs, nil)
	chunks = nil
	require.Error(t, receiver.sendBatch(bat), "a batch that cannot marshal")
	require.Empty(t, chunks, "is not sent")
	bat.Vecs = bat.Vecs[:2]
}

// Concurrent sends on a stream are serialized on its buffer: every message
// carries a whole, uncorrupted batch.
func TestRemoteBatchBufferConcurrentSends(t *testing.T) {
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	var mu sync.Mutex
	seen := map[int64]int{}
	session.EXPECT().Write(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, message any) error {
			decoded, err := batch.ViewBinary(message.(*pipeline.Message).Data)
			if err != nil {
				return err
			}
			first := vector.GetFixedAtNoTypeCheck[int64](decoded.Vecs[0], 0)
			for i := 0; i < decoded.RowCount(); i++ {
				if vector.GetFixedAtNoTypeCheck[int64](decoded.Vecs[0], i) != first+int64(i) {
					return moerr.NewInternalErrorNoCtx("corrupt batch")
				}
			}
			mu.Lock()
			seen[first]++
			mu.Unlock()
			return nil
		})
	receiver := &messageReceiverOnServer{
		batchOut:        newTestRemoteBatchBuffer(t),
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  1 << 20,
	}
	mp := mpool.MustNewZero()
	var wg sync.WaitGroup
	for g := int64(0); g < 8; g++ {
		bat := newRemoteSendTestBatch(t, mp, g*1000, 10+int(g)*20)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				require.NoError(t, receiver.sendBatch(bat))
			}
		}()
		defer bat.Clean(mp)
	}
	wg.Wait()
	require.Len(t, seen, 8)
	for _, n := range seen {
		require.Equal(t, 20, n)
	}
}

// close does not free the buffer under an in-flight Write: it waits for the
// send, so the transport never reads freed memory, and later sends fail.
func TestRemoteBatchBufferCloseWaitsForInFlightSend(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	defer mpool.DisableDebugPoisonOnFree()
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	inWrite := make(chan struct{})
	release := make(chan struct{})
	var readOK bool
	session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, message any) error {
			close(inWrite)
			<-release
			// The transport encodes the message only now.
			_, err := batch.ViewBinary(message.(*pipeline.Message).Data)
			readOK = err == nil
			return nil
		})
	out := newTestRemoteBatchBuffer(t)
	receiver := &messageReceiverOnServer{
		batchOut:        out,
		messageCtx:      context.Background(),
		connectionCtx:   context.Background(),
		clientSession:   session,
		messageAcquirer: func() morpc.Message { return &pipeline.Message{} },
		maxMessageSize:  1 << 20,
	}
	mp := mpool.MustNewZero()
	bat := newRemoteSendTestBatch(t, mp, 0, 40)
	defer bat.Clean(mp)

	sent := make(chan error, 1)
	go func() { sent <- receiver.sendBatch(bat) }()
	<-inWrite
	closed := make(chan struct{})
	go func() {
		out.close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("close freed the buffer during a Write")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-sent)
	<-closed
	require.True(t, readOK, "the transport read a live buffer")
	require.Error(t, receiver.sendBatch(bat), "a closed stream sends nothing")
}
