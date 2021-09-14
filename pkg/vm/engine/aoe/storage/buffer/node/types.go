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

package node

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"sync"
)

type NodeBuffer struct {
	buf.IBuffer
	ID uint64
	// Type nif.BufferType
}

type NodeHandleCtx struct {
	ID          uint64
	Buff        buf.IBuffer
	Spillable   bool
	Manager     mgrif.IBufferManager
	File        common.IVFile
	Constructor buf.MemoryNodeConstructor
	Dir         []byte
	UseCompress bool
}

type NodeHandle struct {
	sync.Mutex
	State       nif.NodeState
	ID          uint64
	Buff        buf.IBuffer
	Spillable   bool
	File        common.IVFile
	UseCompress bool
	RTState     nif.NodeRTState
	Refs        uint64
	Manager     mgrif.IBufferManager
	Iter        uint64
	IO          ioif.IO
	Constructor buf.MemoryNodeConstructor
}

// BufferHandle is created from IBufferManager::Pin, which will set the INodeHandle reference to 1
// The following IBufferManager::Pin will call INodeHandle::Ref to increment the reference count
// BufferHandle should alway be closed manually when it is not needed, which will call IBufferManager::Unpin
type BufferHandle struct {
	Handle  nif.INodeHandle
	Manager mgrif.IBufferManager
}
