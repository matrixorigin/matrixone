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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (srv *Server) RecordDispatchPipeline(
	session morpc.ClientSession, streamID uint64, dispatchReceiver *process.WrapCs) {

	key := generateRecordKey(session, streamID)

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	// check if sender has sent a stop running message.
	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok && v.alreadyDone {
		dispatchReceiver.Lock()
		dispatchReceiver.ReceiverDone = true
		dispatchReceiver.Unlock()
		return
	}

	value := runningPipelineInfo{
		alreadyDone: false,
		isDispatch:  true,
		queryCancel: nil,
		receiver:    dispatchReceiver,
	}

	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = value
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

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	if v, ok := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]; ok {
		v.cancelPipeline()
	} else {
		srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key] = generateCanceledRecord()
	}
}

func (srv *Server) RemoveRelatedPipeline(session morpc.ClientSession, streamID uint64) {
	key := generateRecordKey(session, streamID)

	srv.receivedRunningPipeline.Lock()
	defer srv.receivedRunningPipeline.Unlock()

	delete(srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline, key)
}

func generateCanceledRecord() runningPipelineInfo {
	return runningPipelineInfo{alreadyDone: true}
}

func generateRecordKey(session morpc.ClientSession, streamID uint64) rpcClientItem {
	return rpcClientItem{tcp: session, id: streamID}
}
