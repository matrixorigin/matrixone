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

	"errors"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	ErrNoSpace = errors.New("buffer: no space left")
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
	GetID() common.ID
	GetNode() INode
}

type INode interface {
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
	MakeHandle() INodeHandle
	Destroy()
	Size() uint64
	Iteration() uint64
	IncIteration() uint64
	IsClosed() bool
	GetState() NodeState
	Expand(uint64, func() error) error
}

type INodeManager interface {
	ISizeLimiter
	sync.Locker
	RLock()
	RUnlock()
	String() string
	Count() int
	RegisterNode(INode)
	UnregisterNode(INode)
	Pin(INode) INodeHandle
	TryPin(INode, time.Duration) (INodeHandle, error)
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
	Unload()
	Unloadable() bool
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
