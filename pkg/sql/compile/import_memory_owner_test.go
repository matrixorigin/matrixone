// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type memoryOwnerMessage struct {
	message.Message
	destroyed atomic.Int32
}

func (m *memoryOwnerMessage) GetReceiverAddr() message.MessageAddress {
	return message.MessageAddress{CnAddr: message.CURRENTCN}
}
func (m *memoryOwnerMessage) NeedBlock() bool { return false }
func (m *memoryOwnerMessage) Destroy()        { m.destroyed.Add(1) }

func memoryOwnerFixture(t *testing.T) (string, *process.ImportMemory, rscthrottler.RSCThrottler, *message.MessageBoard) {
	t.Helper()
	service := t.Name()
	throttler := rscthrottler.NewMemThrottler(service, .9, rscthrottler.WithConstLimit(1<<30))
	working, err := rscthrottler.ReserveWorkingSet(context.Background(), throttler, rscthrottler.WorkingSetSpec{UnitBytes: 64, Inputs: []rscthrottler.InputSlotSpec{{Name: "test", Count: 2, Bytes: 64}}, IOBytes: 128})
	require.NoError(t, err)
	memory := &process.ImportMemory{WorkingSet: working, UnitBytes: 64}
	t.Cleanup(working.Close)
	center := &message.MessageCenter{RwMutex: &sync.Mutex{}, StmtIDToBoard: make(map[uuid.UUID]*message.MessageBoard)}
	board := message.NewMessageBoard().SetMultiCN(center, uuid.New())
	return service, memory, throttler, board
}

func TestStatementMemoryLateScopeAndTerminalOwnership(t *testing.T) {
	service, memory, throttler, board := memoryOwnerFixture(t)
	token, closeOwner, err := RegisterStatementMemory(service, memory)
	require.NoError(t, err)
	t.Cleanup(func() { closeOwner(nil) })
	ctx1, cancel1 := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel1(nil) })
	_, _, err = AttachStatementMemory(service, token, board, cancel1)
	require.Error(t, err, "uncommitted metadata must never become visible")
	require.NoError(t, CommitStatementMemory(service, token, 32))
	witness := &memoryOwnerMessage{}
	message.SendMessage(witness, board)
	shared, detach1, err := AttachStatementMemory(service, token, board, cancel1)
	require.NoError(t, err)
	t.Cleanup(detach1)
	require.Same(t, memory, shared)
	require.Equal(t, int64(32), shared.UnitBytes)
	require.Error(t, CommitStatementMemory(service, token, 16), "no metadata mutation after commit")
	ctx2, cancel2 := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel2(nil) })
	// The first RPC ends before the probe RPC arrives. Reset must preserve the
	// existing board and the original promise for that late scope.
	cancel1(errors.New("normal first RPC terminal"))
	detach1()
	board.Reset()
	require.Zero(t, witness.destroyed.Load())
	_, detach2, err := AttachStatementMemory(service, token, board, cancel2)
	require.NoError(t, err)
	t.Cleanup(detach2)
	require.NoError(t, ctx2.Err(), "an individual RPC cancel cannot close the group")
	_, _, err = AttachStatementMemory(service, token, message.NewMessageBoard(), cancel2)
	require.Error(t, err, "cross-statement token use must be rejected")
	before := rscthrottler.WorkingSetHeadroom(throttler)
	closeOwner(errors.New("control connection lost"))
	require.Error(t, ctx1.Err())
	require.Error(t, ctx2.Err())
	require.Zero(t, witness.destroyed.Load(), "cancel does not mean the last consumer finished reading")
	during := rscthrottler.WorkingSetHeadroom(throttler)
	require.Equal(t, before.Input, during.Input)
	require.Equal(t, before.Workspace, during.Workspace)
	_, _, err = AttachStatementMemory(service, token, board, cancel2)
	require.Error(t, err, "late RPC cannot resurrect an ended epoch")
	detach2()
	detach2()
	after := rscthrottler.WorkingSetHeadroom(throttler)
	require.Equal(t, before.Input-128, after.Input)
	require.Equal(t, before.Workspace-128, after.Workspace)
	require.Equal(t, int32(1), witness.destroyed.Load())
	closeOwner(nil)
	require.Equal(t, int32(1), witness.destroyed.Load())
}

func TestStatementMemoryConcurrentCloseAndDetach(t *testing.T) {
	service, memory, _, board := memoryOwnerFixture(t)
	token, closeOwner, err := RegisterStatementMemory(service, memory)
	require.NoError(t, err)
	t.Cleanup(func() { closeOwner(nil) })
	require.NoError(t, CommitStatementMemory(service, token, 64))
	witness := &memoryOwnerMessage{}
	message.SendMessage(witness, board)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		_, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(nil) })
		_, detach, err := AttachStatementMemory(service, token, board, cancel)
		require.NoError(t, err)
		t.Cleanup(detach)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			detach()
			detach()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		closeOwner(context.Canceled)
	}()
	close(start)
	wg.Wait()
	require.Equal(t, int32(1), witness.destroyed.Load())
	_, err = findStatementMemory(service, token)
	require.Error(t, err)
}

func TestStatementMemoryRepeatedCommitDoesNotMutatePublishedMetadata(t *testing.T) {
	service, memory, _, board := memoryOwnerFixture(t)
	token, closeOwner, err := RegisterStatementMemory(service, memory)
	require.NoError(t, err)
	t.Cleanup(func() { closeOwner(nil) })
	require.NoError(t, CommitStatementMemory(service, token, 64))
	_, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	shared, detach, err := AttachStatementMemory(service, token, board, cancel)
	require.NoError(t, err)
	t.Cleanup(detach)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for range 1000 {
			if shared.UnitBytes != 64 {
				panic("published memory unit changed")
			}
		}
	}()
	close(start)
	for range 1000 {
		require.NoError(t, CommitStatementMemory(service, token, 64))
	}
	wg.Wait()
}

type memoryControlSession struct {
	morpc.ClientSession
	ctx     context.Context
	replies chan *pipeline.Message
	err     error
}

func (s *memoryControlSession) SessionCtx() context.Context { return s.ctx }
func (s *memoryControlSession) Write(_ context.Context, raw morpc.Message) error {
	if s.err != nil {
		return s.err
	}
	copy := *raw.(*pipeline.Message)
	copy.Data = append([]byte(nil), copy.Data...)
	s.replies <- &copy
	return nil
}

func memoryControlCall(t *testing.T, ctx context.Context, service string, session *memoryControlSession, request MemoryControlRequest) error {
	t.Helper()
	if request.Action == MemoryControlOpen && request.ExpectedProcessID == "" {
		request.ExpectedProcessID = runtime.ProcessInstanceID()
	}
	data, err := EncodeMemoryControlRequest(request)
	require.NoError(t, err)
	handled, err := handleMemoryControl(ctx, service, &pipeline.Message{Id: 19, Cmd: pipeline.Method_StopSending, Data: data}, session, func() morpc.Message { return &pipeline.Message{} })
	require.True(t, handled)
	return err
}

func receiveMemoryControl(t *testing.T, session *memoryControlSession) MemoryControlResponse {
	t.Helper()
	select {
	case raw := <-session.replies:
		response, err := DecodeMemoryControlResponse(raw.Data)
		require.NoError(t, err)
		return response
	case <-time.After(10 * time.Second):
		t.Fatal("control ACK did not arrive")
		return MemoryControlResponse{}
	}
}

func TestStatementMemoryControlConnectionLifetime(t *testing.T) {
	for _, terminal := range []string{"end", "disconnect", "lost-open-ack"} {
		t.Run(terminal, func(t *testing.T) {
			service := t.Name()
			rt := runtime.NewRuntime(metadata.ServiceType_CN, service, zap.NewNop())
			runtime.SetupServiceBasedRuntime(service, rt)
			throttler := rscthrottler.NewMemThrottler(service, .9, rscthrottler.WithConstLimit(1<<30))
			rt.SetGlobalVariables(runtime.CNMemoryThrottler, throttler)
			before := rscthrottler.WorkingSetHeadroom(throttler)
			connectionCtx, disconnect := context.WithCancel(context.Background())
			t.Cleanup(disconnect)
			session := &memoryControlSession{ctx: connectionCtx, replies: make(chan *pipeline.Message, 4)}
			if terminal == "lost-open-ack" {
				session.err = errors.New("write ACK failed")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			t.Cleanup(cancel)
			done := make(chan error, 1)
			manifest := &MemoryManifest{TargetUnit: 1 << 20, Parallel: 1, Stages: []MemoryStageDescriptor{{Role: MemoryStageOutput, Count: 1, Schema: []types.Type{types.T_int64.ToType()}, TableCount: 1}}}
			go func() {
				done <- memoryControlCall(t, ctx, service, session, MemoryControlRequest{Action: MemoryControlOpen, Manifest: manifest})
			}()
			if terminal != "lost-open-ack" {
				response := receiveMemoryControl(t, session)
				require.Empty(t, response.Error)
				require.NotEmpty(t, response.Token)
				require.NoError(t, memoryControlCall(t, ctx, service, session, MemoryControlRequest{Action: MemoryControlCommit, Token: response.Token, UnitBytes: response.UnitBytes}))
				require.Empty(t, receiveMemoryControl(t, session).Error)
				if terminal == "end" {
					require.NoError(t, memoryControlCall(t, ctx, service, session, MemoryControlRequest{Action: MemoryControlEnd, Token: response.Token}))
					require.Empty(t, receiveMemoryControl(t, session).Error)
				} else {
					disconnect()
				}
			}
			select {
			case err := <-done:
				if session.err != nil {
					require.ErrorIs(t, err, session.err)
				} else {
					require.NoError(t, err)
				}
			case <-ctx.Done():
				t.Fatal("control owner survived its terminal connection")
			}
			after := rscthrottler.WorkingSetHeadroom(throttler)
			require.Equal(t, before.Input, after.Input)
			require.Equal(t, before.Workspace, after.Workspace)
			statementMemoryOwners.Lock()
			for key := range statementMemoryOwners.owners {
				require.NotEqual(t, service, key.service, "terminal owner must leave the registry")
			}
			statementMemoryOwners.Unlock()
		})
	}
}

func TestStatementMemoryRejectsChangedProcessBeforeAdmission(t *testing.T) {
	service, _, throttler, _ := memoryOwnerFixture(t)
	before := rscthrottler.WorkingSetHeadroom(throttler)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	session := &memoryControlSession{ctx: ctx, replies: make(chan *pipeline.Message, 1)}
	request := MemoryControlRequest{Action: MemoryControlOpen, ExpectedProcessID: "earlier-process", Manifest: &MemoryManifest{TargetUnit: 1 << 20, Parallel: 1, Stages: []MemoryStageDescriptor{{Role: MemoryStageOutput, Count: 1, Schema: []types.Type{types.T_int64.ToType()}, TableCount: 1}}}}
	// No CN runtime is registered: rejection must precede both the runtime
	// lookup and ReserveWorkingSet, not only the Open ACK.
	require.NoError(t, memoryControlCall(t, ctx, service, session, request))
	require.Contains(t, receiveMemoryControl(t, session).Error, "process changed")
	after := rscthrottler.WorkingSetHeadroom(throttler)
	require.Equal(t, before.Input, after.Input)
	require.Equal(t, before.Workspace, after.Workspace)
}
