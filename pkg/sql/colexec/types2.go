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

	// check if sender has sent a stop running message.
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
		logutil.Debug("RecordDispatchPipeline found alreadyDone record",
			zap.Uint64("streamID", streamID),
			zap.Bool("hasReceiver", v.receiver != nil),
			zap.Bool("isDispatch", v.isDispatch),
			zap.Bool("hasQueryCancel", v.queryCancel != nil))

		// Fix: Check if this is a stale record created by CancelPipelineSending
		// before RecordDispatchPipeline was called (race condition).
		// If receiver is nil, it means CancelPipelineSending created this record
		// when the pipeline wasn't registered yet. We should clean it up and
		// allow the normal registration to proceed.
		if v.receiver == nil {
			// This is a stale record created by CancelPipelineSending before
			// RecordDispatchPipeline was called. Clean it up and proceed with
			// normal registration.
			logutil.Debug("RecordDispatchPipeline cleaning stale record",
				zap.Uint64("streamID", streamID))
			delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
		} else if v.receiver.Uid == dispatchReceiver.Uid {
			// This is a legitimate cancellation - the same receiver was already registered
			// and then cancelled. Set ReceiverDone to true.
			logutil.Debug("RecordDispatchPipeline setting ReceiverDone=true (legitimate cancellation)",
				zap.Uint64("streamID", streamID),
				zap.String("existingReceiverUid", v.receiver.Uid.String()))
			dispatchReceiver.Lock()
			dispatchReceiver.ReceiverDone = true
			dispatchReceiver.Unlock()
			return
		} else {
			// Different receiver with same streamID - this can happen when multiple
			// receivers share the same streamID. Clean up the old record and allow
			// the new receiver to register.
			logutil.Debug("RecordDispatchPipeline: cleaning old receiver record with different Uid",
				zap.Uint64("streamID", streamID),
				zap.String("oldReceiverUid", v.receiver.Uid.String()),
				zap.String("newReceiverUid", dispatchReceiver.Uid.String()))
			delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
		}
	}

	// Check if there's an existing record with a different receiver
	// This can happen when multiple receivers share the same streamID
	if existing, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; exists {
		if existing.receiver != nil && existing.receiver.Uid != dispatchReceiver.Uid {
			logutil.Debug("RecordDispatchPipeline: overwriting existing receiver with different Uid",
				zap.Uint64("streamID", streamID),
				zap.String("existingReceiverUid", existing.receiver.Uid.String()),
				zap.String("newReceiverUid", dispatchReceiver.Uid.String()))
		}
	}

	value := runningPipelineInfo{
		alreadyDone: false,
		isDispatch:  true,
		queryCancel: nil,
		receiver:    dispatchReceiver,
	}

	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = value
	logutil.Debug("RecordDispatchPipeline registered successfully",
		zap.Uint64("streamID", streamID),
		zap.String("receiverUid", dispatchReceiver.Uid.String()))
}

func (srv *Server) RecordBuiltPipeline(
	session morpc.ClientSession, streamID uint64, proc *process.Process) {

	key := generateRecordKey(session, streamID)

	_, cancel := process.GetQueryCtxFromProc(proc)

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	// check if sender has sent a stop running message.
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
		return
	}

	value := runningPipelineInfo{
		alreadyDone: false,
		isDispatch:  false,
		queryCancel: cancel,
		receiver:    nil,
	}
	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = value
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

		// Fix: StopSending message is used to stop sending data, not to cancel
		// dispatch receivers. Dispatch receivers are used to receive data and
		// should continue receiving until data sending is complete.
		// Only cancel non-dispatch pipelines (those that execute queries).
		if v.isDispatch {
			logutil.Debug("CancelPipelineSending: ignoring dispatch receiver (StopSending should not cancel receivers)",
				zap.Uint64("streamID", streamID),
				zap.String("receiverUid", func() string {
					if v.receiver != nil {
						return v.receiver.Uid.String()
					}
					return "nil"
				}()))
			// Don't cancel dispatch receivers - they should continue receiving data
		} else {
			// Only cancel non-dispatch pipelines (query execution pipelines)
			logutil.Debug("CancelPipelineSending canceling non-dispatch pipeline",
				zap.Uint64("streamID", streamID))
			v.cancelPipeline()
		}
	} else {
		// Fix: Don't create a canceled record if no record exists.
		// This can happen when StopSending arrives before PrepareDoneNotifyMessage.
		// The RecordDispatchPipeline will handle the registration properly.
		// Creating a canceled record here causes issues when multiple receivers
		// share the same streamID (different sessions).
		logutil.Debug("CancelPipelineSending: no existing record, ignoring (will be handled by RecordDispatchPipeline)",
			zap.Uint64("streamID", streamID))
		// Don't create canceled record - let RecordDispatchPipeline handle it
	}
}

func (srv *Server) RemoveRelatedPipeline(session morpc.ClientSession, streamID uint64) {
	key := generateRecordKey(session, streamID)

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		logutil.Debug("RemoveRelatedPipeline removing record",
			zap.Uint64("streamID", streamID),
			zap.Bool("alreadyDone", v.alreadyDone),
			zap.Bool("hasReceiver", v.receiver != nil),
			zap.Bool("isDispatch", v.isDispatch))
	} else {
		logutil.Debug("RemoveRelatedPipeline called but no record found",
			zap.Uint64("streamID", streamID))
	}

	delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
}

func generateRecordKey(session morpc.ClientSession, streamID uint64) rpcClientItem {
	return rpcClientItem{tcp: session, id: streamID}
}
