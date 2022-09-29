// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package base

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	ErrNoSpace       = moerr.NewInternalError("buffer: no space left")
	ErrNotFound      = moerr.NewInternalError("buffer: node not found")
	ErrDuplicataNode = moerr.NewInternalError("buffer: duplicate node")
)

type MemoryFreeFunc func(IMemoryNode)

type IMemoryNode interface {
	io.ReaderFrom
	io.WriterTo
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	FreeMemory()
	Reset()
	GetMemorySize() uint64
	GetMemoryCapacity() uint64
}

type INodeHandle interface {
	io.Closer
	Key() any
	GetNode() INode
}

type INode interface {
	sync.Locker
	io.Closer
	common.IRef
	RLock()
	RUnlock()
	Key() any

	// unload the data return the size quota back
	Unload()
	// whether the node unloadable
	Unloadable() bool
	// whether the node is loaded
	IsLoaded() bool
	// load data into the node
	Load()

	// increase the node reference count and get a handle of the node
	MakeHandle() INodeHandle

	// true if the node can be destoryed and hard evicted from the node manager
	// false if the node can only be unloaded on evicted
	HardEvictable() bool

	// destory the node resources
	// node manager destoryes a node when Close a node
	Destroy()

	// the size of the node
	Size() uint64

	// the iteration of the node.
	// it is increased by 1 when the reference count is 0 during UnPin
	Iteration() uint64
	IncIteration() uint64

	// whether a node is closed
	IsClosed() bool
	// try to close a node. It cannot be closed when the reference count is not 0
	// true if closed and false otherwise
	TryClose() bool

	// the node state
	GetState() NodeState

	// expand a node size and execute the callback
	Expand(uint64, func() error) error
}

type INodeManager interface {
	ISizeLimiter
	sync.Locker
	RLock()
	RUnlock()
	String() string
	Count() int
	Add(INode) error
	RegisterNode(INode) error
	UnregisterNode(INode)
	Pin(INode) INodeHandle
	PinByKey(any) (INodeHandle, error)
	TryPin(INode, time.Duration) (INodeHandle, error)
	TryPinByKey(any, time.Duration) (INodeHandle, error)
	Unpin(INode)
	MakeRoom(uint64) bool
}

type ISizeLimiter interface {
	Total() uint64
	ApplyQuota(uint64) bool
	RetuernQuota(uint64) uint64
}

type IEvictHandle interface {
	sync.Locker
	IsClosed() bool
	TryClose() bool
	Unload()
	Unloadable() bool
	HardEvictable() bool
	Iteration() uint64
}

type NodeState = uint32

const (
	NodeUnload NodeState = iota
	NodeLoading
	NodeRollback
	NodeCommit
	NodeUnloadING
	NodeLoaded
)

func NodeStateString(state NodeState) string {
	switch state {
	case NodeUnload:
		return "UNLOAD"
	case NodeLoading:
		return "LOADING"
	case NodeRollback:
		return "ROLLBACK"
	case NodeCommit:
		return "COMMIT"
	case NodeUnloadING:
		return "UNLOADING"
	case NodeLoaded:
		return "LOADED"
	}
	panic(fmt.Sprintf("unsupported: %d", state))
}
