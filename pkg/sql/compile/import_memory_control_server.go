// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package compile

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var errStatementMemoryClosed = errors.New("statement memory owner closed")

type statementMemoryKey struct{ service, token string }

var statementMemoryOwners = struct {
	sync.Mutex
	owners map[statementMemoryKey]*statementMemoryOwner
}{owners: make(map[statementMemoryKey]*statementMemoryOwner)}

// One owner covers the complete CN DAG, including scopes that have not yet
// arrived. Its lifetime is independent of any one execution RPC.
type statementMemoryOwner struct {
	mu        sync.Mutex
	key       statementMemoryKey
	memory    *process.ImportMemory
	committed bool
	closed    bool
	done      chan struct{}
	board     *message.MessageBoard
	next      uint64
	attached  map[uint64]context.CancelCauseFunc
	session   <-chan struct{}
	stream    uint64
}

func RegisterStatementMemory(serviceID string, memory *process.ImportMemory) (string, func(error), error) {
	if serviceID == "" || memory == nil || memory.WorkingSet == nil || memory.UnitBytes <= 0 {
		return "", nil, errors.New("invalid statement memory owner")
	}
	owner := &statementMemoryOwner{
		key: statementMemoryKey{serviceID, uuid.NewString()}, memory: memory,
		done: make(chan struct{}), attached: make(map[uint64]context.CancelCauseFunc),
	}
	statementMemoryOwners.Lock()
	statementMemoryOwners.owners[owner.key] = owner
	statementMemoryOwners.Unlock()
	return owner.key.token, owner.close, nil
}

func findStatementMemory(serviceID, token string) (*statementMemoryOwner, error) {
	statementMemoryOwners.Lock()
	owner := statementMemoryOwners.owners[statementMemoryKey{serviceID, token}]
	statementMemoryOwners.Unlock()
	if owner == nil {
		return nil, errStatementMemoryClosed
	}
	return owner, nil
}

func CommitStatementMemory(serviceID, token string, unit int64) error {
	owner, err := findStatementMemory(serviceID, token)
	if err != nil {
		return err
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closed {
		return errStatementMemoryClosed
	}
	if unit <= 0 || unit > owner.memory.UnitBytes {
		return errors.New("memory commit cannot increase the admitted unit")
	}
	if owner.committed {
		if unit != owner.memory.UnitBytes {
			return errors.New("statement memory unit already committed")
		}
		return nil
	}
	// No execution RPC can observe this metadata before the commit ACK.
	owner.memory.UnitBytes = unit
	owner.committed = true
	return nil
}

func CloseStatementMemory(serviceID, token string, cause error) error {
	owner, err := findStatementMemory(serviceID, token)
	if err != nil {
		return err
	}
	owner.close(cause)
	return nil
}

func AttachStatementMemory(serviceID, token string, board *message.MessageBoard, cancel context.CancelCauseFunc) (*process.ImportMemory, func(), error) {
	owner, err := findStatementMemory(serviceID, token)
	if err != nil {
		return nil, nil, err
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closed {
		return nil, nil, errStatementMemoryClosed
	}
	if !owner.committed || board == nil || cancel == nil {
		return nil, nil, errors.New("statement memory attach before commit or without scope owner")
	}
	if owner.board != nil && owner.board != board {
		return nil, nil, errors.New("statement memory token belongs to a different message board")
	}
	release, err := owner.memory.WorkingSet.Retain()
	if err != nil {
		return nil, nil, err
	}
	if owner.board == nil {
		if err = board.RetainStatementMemory(); err != nil {
			release()
			return nil, nil, err
		}
		owner.board = board
	}
	owner.next++
	id := owner.next
	owner.attached[id] = cancel
	var once sync.Once
	detach := func() {
		once.Do(func() {
			owner.mu.Lock()
			delete(owner.attached, id)
			var board *message.MessageBoard
			if owner.closed && len(owner.attached) == 0 {
				board, owner.board = owner.board, nil
			}
			owner.mu.Unlock()
			release()
			if board != nil {
				board.ReleaseStatementMemory()
			}
		})
	}
	return owner.memory, detach, nil
}

func (owner *statementMemoryOwner) close(cause error) {
	owner.mu.Lock()
	if owner.closed {
		owner.mu.Unlock()
		return
	}
	owner.closed = true
	cancels := make([]context.CancelCauseFunc, 0, len(owner.attached))
	for _, cancel := range owner.attached {
		cancels = append(cancels, cancel)
	}
	var board *message.MessageBoard
	if len(owner.attached) == 0 {
		board, owner.board = owner.board, nil
	}
	owner.mu.Unlock()
	statementMemoryOwners.Lock()
	delete(statementMemoryOwners.owners, owner.key)
	statementMemoryOwners.Unlock()
	if cause == nil {
		cause = errStatementMemoryClosed
	}
	for _, cancel := range cancels {
		cancel(cause)
	}
	// Active RPC Retain refs and operator leases preserve their capacity until
	// their own cleanup; closing the root never announces those bytes as free.
	owner.memory.WorkingSet.Close()
	if board != nil {
		board.ReleaseStatementMemory()
	}
	close(owner.done)
}

func writeMemoryControl(ctx context.Context, cs morpc.ClientSession, streamID uint64, response MemoryControlResponse, terminal bool, acquire func() morpc.Message) error {
	data, err := EncodeMemoryControlResponse(response)
	if err != nil {
		return err
	}
	msg, ok := acquire().(*pipeline.Message)
	if !ok {
		return errors.New("invalid memory control response message")
	}
	msg.SetID(streamID)
	msg.Cmd = pipeline.Method_StopSending
	msg.Data = data
	msg.Sid = pipeline.Status_WaitingNext
	if terminal {
		msg.Sid = pipeline.Status_MessageEnd
	}
	return cs.Write(ctx, msg)
}

// The dedicated locked pipeline connection is the terminal owner even when an
// END packet or its ACK is lost. It is not shared with normal scope streams.
func handleMemoryControl(ctx context.Context, serviceID string, msg *pipeline.Message, cs morpc.ClientSession, acquire func() morpc.Message) (bool, error) {
	if msg.Cmd != pipeline.Method_StopSending {
		return false, nil
	}
	request, matched, decodeErr := DecodeMemoryControlRequest(msg.Data)
	if !matched {
		return false, nil
	}
	fail := func(err error) (bool, error) {
		response := MemoryControlResponse{Error: err.Error(), RetryParallel: MemoryRetryParallel(err)}
		return true, writeMemoryControl(ctx, cs, msg.Id, response, true, acquire)
	}
	if decodeErr != nil {
		return fail(decodeErr)
	}
	if request.Action == MemoryControlOpen {
		if request.Manifest == nil || request.Token != "" || cs.SessionCtx().Done() == nil {
			return fail(errors.New("invalid memory control open"))
		}
		if request.ExpectedProcessID != runtime.ProcessInstanceID() {
			return fail(errors.New("memory admission process changed after capability discovery"))
		}
		value, ok := runtime.ServiceRuntime(serviceID).GetGlobalVariables(runtime.CNMemoryThrottler)
		throttler, capable := value.(rscthrottler.RSCThrottler)
		if !ok || !capable {
			return fail(errors.New("CN memory admission unavailable"))
		}
		admitCtx, cancel := context.WithCancelCause(ctx)
		stop := context.AfterFunc(cs.SessionCtx(), func() { cancel(context.Cause(cs.SessionCtx())) })
		memory, err := AdmitMemoryManifest(admitCtx, throttler, *request.Manifest)
		stop()
		cancel(nil)
		if err != nil {
			return fail(err)
		}
		if memory == nil {
			return fail(errors.New("empty memory manifest"))
		}
		token, closeOwner, err := RegisterStatementMemory(serviceID, memory)
		if err != nil {
			memory.WorkingSet.Close()
			return fail(err)
		}
		defer closeOwner(errStatementMemoryClosed)
		owner, _ := findStatementMemory(serviceID, token)
		owner.mu.Lock()
		owner.session, owner.stream = cs.SessionCtx().Done(), msg.Id
		owner.mu.Unlock()
		if err = writeMemoryControl(ctx, cs, msg.Id, MemoryControlResponse{Token: token, UnitBytes: memory.UnitBytes}, false, acquire); err != nil {
			return true, err
		}
		select {
		case <-cs.SessionCtx().Done():
		case <-owner.done:
		}
		return true, nil
	}
	owner, err := findStatementMemory(serviceID, request.Token)
	if err != nil {
		return fail(err)
	}
	// A token cannot be driven from an unrelated stream or a re-used backend.
	owner.mu.Lock()
	connectionMatches := owner.session == cs.SessionCtx().Done() && owner.stream == msg.Id
	owner.mu.Unlock()
	if !connectionMatches {
		return fail(errors.New("memory control connection mismatch"))
	}
	switch request.Action {
	case MemoryControlCommit:
		err = CommitStatementMemory(serviceID, request.Token, request.UnitBytes)
	case MemoryControlEnd, MemoryControlAbort:
		// The root can retire before ACK: active consumers still own their refs.
		var cause error
		if request.Action == MemoryControlAbort {
			cause = context.Canceled
		}
		owner.close(cause)
	default:
		err = errors.New("unknown memory control action")
	}
	if err != nil {
		return fail(err)
	}
	return true, writeMemoryControl(ctx, cs, msg.Id, MemoryControlResponse{}, request.Action != MemoryControlCommit, acquire)
}
