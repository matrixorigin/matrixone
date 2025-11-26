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

	logutil.Info("[DEBUG] RecordDispatchPipeline called",
		zap.Uint64("streamID", streamID),
		zap.String("receiverUid", dispatchReceiver.Uid.String()))

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	// check if sender has sent a stop running message.
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
		logutil.Warn("[DEBUG] RecordDispatchPipeline found alreadyDone record",
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
			logutil.Info("[DEBUG] RecordDispatchPipeline cleaning stale record",
				zap.Uint64("streamID", streamID))
			delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
		} else {
			// This is a legitimate cancellation - the receiver was already registered
			// and then cancelled. Set ReceiverDone to true.
			logutil.Warn("[DEBUG] RecordDispatchPipeline setting ReceiverDone=true (legitimate cancellation)",
				zap.Uint64("streamID", streamID),
				zap.String("existingReceiverUid", v.receiver.Uid.String()))
			dispatchReceiver.Lock()
			dispatchReceiver.ReceiverDone = true
			dispatchReceiver.Unlock()
			return
		}
	}

	value := runningPipelineInfo{
		alreadyDone: false,
		isDispatch:  true,
		queryCancel: nil,
		receiver:    dispatchReceiver,
	}

	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = value
	logutil.Info("[DEBUG] RecordDispatchPipeline registered successfully",
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

	logutil.Info("[DEBUG] CancelPipelineSending called",
		zap.Uint64("streamID", streamID))

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		logutil.Info("[DEBUG] CancelPipelineSending found existing record, calling cancelPipeline",
			zap.Uint64("streamID", streamID),
			zap.Bool("alreadyDone", v.alreadyDone),
			zap.Bool("hasReceiver", v.receiver != nil),
			zap.Bool("isDispatch", v.isDispatch))
		v.cancelPipeline()
	} else {
		logutil.Warn("[DEBUG] CancelPipelineSending creating canceled record (no existing record)",
			zap.Uint64("streamID", streamID))
		srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = generateCanceledRecord()
	}
}

func (srv *Server) RemoveRelatedPipeline(session morpc.ClientSession, streamID uint64) {
	key := generateRecordKey(session, streamID)

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		logutil.Info("[DEBUG] RemoveRelatedPipeline removing record",
			zap.Uint64("streamID", streamID),
			zap.Bool("alreadyDone", v.alreadyDone),
			zap.Bool("hasReceiver", v.receiver != nil),
			zap.Bool("isDispatch", v.isDispatch))
	} else {
		logutil.Info("[DEBUG] RemoveRelatedPipeline called but no record found",
			zap.Uint64("streamID", streamID))
	}

	delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
}

func generateCanceledRecord() runningPipelineInfo {
	return runningPipelineInfo{alreadyDone: true}
}

func generateRecordKey(session morpc.ClientSession, streamID uint64) rpcClientItem {
	return rpcClientItem{tcp: session, id: streamID}
}
