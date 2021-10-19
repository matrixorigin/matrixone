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

package base

import (
	"io"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v2"
	"sync"
)

type INodeHandle interface {
	sync.Locker
	io.Closer
	common.IRef
	RLock()
	RUnlock()
	GetID() common.ID
	Unload()
	Unloadable() bool
	IsLoaded() bool
	Load()
	Destroy()
	Size() uint64
	Iteration() uint64
	IncIteration() uint64
	IsClosed() bool
	GetState() iface.NodeState
}

type IMemTable interface {
	common.IRef
	Append(bat *batch.Batch, offset uint64, index *metadata.LogIndex) (n uint64, err error)
	IsFull() bool
	Flush() error
	Unpin()
	GetMeta() *metadata.Block
	GetID() common.ID
	String() string
}

type ICollection interface {
	common.IRef
	Append(bat *batch.Batch, index *metadata.LogIndex) (err error)
	Flush() error
	FetchImmuTable() IMemTable
	String() string
}

type IManager interface {
	WeakRefCollection(id uint64) ICollection
	StrongRefCollection(id uint64) ICollection
	RegisterCollection(interface{}) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
	CollectionIDs() map[uint64]uint64
	String() string
}
