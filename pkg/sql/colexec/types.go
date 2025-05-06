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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ResultPos struct {
	Rel int32
	Pos int32
}

func NewResultPos(rel int32, pos int32) ResultPos {
	return ResultPos{Rel: rel, Pos: pos}
}

// ReceiveInfo used to spec which node,
// and which registers you need
type ReceiveInfo struct {
	// it's useless
	NodeAddr string
	Uuid     uuid.UUID
}

type Server struct {
	hakeeper      logservice.CNHAKeeperClient
	uuidCsChanMap UuidProcMap
	//txn's local segments.
	cnSegmentMap CnSegmentMap

	receivedRunningPipeline RunningPipelineMapForRemoteNode
}

// RunningPipelineMapForRemoteNode
// is a map to record which pipeline was built for a remote node.
// these pipelines will send data to a remote node,
// we record them for a better control for their lives.
type RunningPipelineMapForRemoteNode struct {
	sync.Mutex

	fromRpcClientToRelatedPipeline map[rpcClientItem]runningPipelineInfo
}

type rpcClientItem struct {
	// connection.
	tcp morpc.ClientSession

	// stream id.
	id uint64
}

type runningPipelineInfo struct {
	alreadyDone bool
	queryCancel context.CancelFunc

	isDispatch bool
	receiver   *process.WrapCs
}

func (info *runningPipelineInfo) cancelPipeline() {
	// If this was a pipeline responsible for distributing data, we cannot end this
	// because we are just one of the receivers.
	if info.isDispatch {
		info.receiver.Lock()
		info.receiver.ReceiverDone = true
		info.receiver.Unlock()

	} else {
		if info.queryCancel != nil {
			info.queryCancel()
		}
	}
}

type uuidProcMapItem struct {
	proc *process.Process
	ch   process.RemotePipelineInformationChannel
}

type UuidProcMap struct {
	sync.Mutex
	mp map[uuid.UUID]uuidProcMapItem
}

const (
	DefaultBatchSize = objectio.BlockMaxRows
)

func GetSharedFSFromProc(proc *process.Process) (fs fileservice.FileService, err error) {
	fs, err = fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	if err != nil {
		logutil.Error("get shared fs from proc failed", zap.Error(err))
		return nil, err
	}

	return fs, nil
}
