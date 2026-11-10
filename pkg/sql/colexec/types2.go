// Copyright 2021 Matrix Origin
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

package colexec

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func (srv *Server) RecordDispatchPipeline(
	session morpc.ClientSession, streamID uint64, dispatchReceiver *process.WrapCs) {

	key := generateRecordKey(session, streamID)

	logutil.Debug("RecordDispatchPipeline called",
		zap.Uint64("streamID", streamID),
		zap.String("receiverUid", dispatchReceiver.Uid.String()))

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	// Carry the stream-local wait owner through attachment. Ordinary dispatch
	// registrations have no such callback and retain their shared ownership.
	previous, registered := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	// check if sender has sent a stop running message.
	if v := previous; registered && v.alreadyDone {
		// Fix: Check if this is a stale record created by CancelPipelineSending
		// before RecordDispatchPipeline was called (race condition).
		// If receiver is nil, it means CancelPipelineSending created this record
		// when the pipeline wasn't registered yet. We should clean it up and
		// allow the normal registration to proceed.
		if v.receiver == nil || v.receiver.Uid != dispatchReceiver.Uid {
			// This is a stale record created by CancelPipelineSending before
			// RecordDispatchPipeline was called. Clean it up and proceed with
			// normal registration.
			logutil.Debug("RecordDispatchPipeline cleaning stale record",
				zap.Uint64("streamID", streamID))
			delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
		} else {
			// This is a legitimate cancellation - the same receiver was already registered
			// and then cancelled. Set ReceiverDone to true.
			logutil.Debug("RecordDispatchPipeline setting ReceiverDone=true (legitimate cancellation)",
				zap.Uint64("streamID", streamID),
				zap.String("existingReceiverUid", v.receiver.Uid.String()))
			dispatchReceiver.Lock()
			dispatchReceiver.ReceiverDone = true
			dispatchReceiver.Unlock()
			return
		}
	}

	value := runningPipelineInfo{
		alreadyDone:    false,
		isDispatch:     true,
		pipelineCancel: previous.pipelineCancel,
		receiver:       dispatchReceiver,
	}

	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = value
	srv.ensureSessionCleanupLocked(session)
	logutil.Debug("RecordDispatchPipeline registered successfully",
		zap.Uint64("streamID", streamID),
		zap.String("receiverUid", dispatchReceiver.Uid.String()))
}

func (srv *Server) RecordBuiltPipeline(
	session morpc.ClientSession, streamID uint64, proc *process.Process) {

	// StopSending owns the remote pipeline tree, not its query context.
	srv.RecordPipelineCancellation(session, streamID, proc.Cancel)
}

// RecordPipelineCancellation also owns a notify's wait before its Dispatch
// exists. Reuse the stream tombstone and session cleanup for both owners.
func (srv *Server) RecordPipelineCancellation(
	session morpc.ClientSession, streamID uint64, pipelineCancel context.CancelCauseFunc) {
	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	// check if sender has sent a stop running message.
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
		if pipelineCancel != nil {
			pipelineCancel(process.ErrPipelineStopped)
		}
		return
	}

	value := runningPipelineInfo{
		alreadyDone:    false,
		isDispatch:     false,
		pipelineCancel: pipelineCancel,
		receiver:       nil,
	}
	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = value
	srv.ensureSessionCleanupLocked(session)
}

func (srv *Server) CancelPipelineSending(
	session morpc.ClientSession, streamID uint64) {

	key := generateRecordKey(session, streamID)

	logutil.Debug("CancelPipelineSending called",
		zap.Uint64("streamID", streamID))

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		logutil.Debug("CancelPipelineSending found existing record",
			zap.Uint64("streamID", streamID),
			zap.Bool("alreadyDone", v.alreadyDone),
			zap.Bool("hasReceiver", v.receiver != nil),
			zap.Bool("isDispatch", v.isDispatch))

		if v.pipelineCancel != nil {
			// A notify's callback retires only its subscription after the
			// handoff; it never cancels the shared Dispatch producer.
			logutil.Debug("CancelPipelineSending canceling stream owner",
				zap.Uint64("streamID", streamID))
			v.pipelineCancel(process.ErrPipelineStopped)
		}
		return
	}

	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = runningPipelineInfo{
		alreadyDone: true,
	}
	srv.ensureSessionCleanupLocked(session)
}

// HasPendingPipelineCancellation reports whether StopSending arrived before
// the pipeline record was published. The tombstone remains owned by the
// colexec registry so RecordBuiltPipeline and RecordDispatchPipeline preserve
// their existing, distinct cancellation semantics.
func (srv *Server) HasPendingPipelineCancellation(
	session morpc.ClientSession, streamID uint64,
) bool {
	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	info, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	return ok && info.alreadyDone && info.receiver == nil && info.pipelineCancel == nil
}

func (srv *Server) RemoveRelatedPipeline(session morpc.ClientSession, streamID uint64) {
	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()
	delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
}

func generateRecordKey(session morpc.ClientSession, streamID uint64) rpcClientItem {
	return rpcClientItem{tcp: session, id: streamID}
}

func (srv *Server) ensureSessionCleanupLocked(session morpc.ClientSession) {
	if session == nil || session.SessionCtx() == nil || session.SessionCtx().Done() == nil {
		return
	}
	if _, ok := srv.receivedRunningPipeline.sessionCleanupWaiters[session]; ok {
		return
	}
	srv.receivedRunningPipeline.sessionCleanupWaiters[session] = struct{}{}

	done := session.SessionCtx().Done()
	go func() {
		<-done
		srv.cleanupPipelinesForSession(session)
	}()
}

func (srv *Server) cleanupPipelinesForSession(session morpc.ClientSession) {
	srv.receivedRunningPipeline.Lock()
	var infos []runningPipelineInfo
	delete(srv.receivedRunningPipeline.sessionCleanupWaiters, session)
	for key := range srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline {
		if key.tcp == session {
			infos = append(infos, srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key])
			delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
		}
	}
	srv.receivedRunningPipeline.Unlock()

	for i := range infos {
		infos[i].cancelPipeline(moerr.NewStreamClosedNoCtx())
	}
}
