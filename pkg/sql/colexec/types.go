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
	"sync"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ResultPos struct {
	Rel int32
	Pos int32
}

func NewResultPos(rel int32, pos int32) ResultPos {
	return ResultPos{Rel: rel, Pos: pos}
}

// WrapperNode used to spec which node,
// and which register you need
type WrapperNode struct {
	Node engine.Node
	Uuid uuid.UUID
}

// Server used to support cn2s3 directly, for more info, refer to docs about it
type Server struct {
	sync.Mutex
	id uint64
	mp map[uint64]*process.WaitRegister // k = id, v = reg
	// chanMp will be used in two ways
	// 1. uuid --> WaitRegister, we need to know the batch which is recieved from
	// remote CN should be filled into which chan
	// 2. messgage.Id --> dataBuf (when a batch is too large, it will be split into small ones in the source
	// CN, and the target CN need to recieve them all and then merge them into one batch)
	ChanBufMp     sync.Map
	hakeeper      logservice.CNHAKeeperClient
	CNSegmentId   [12]byte
	InitSegmentId bool
}
