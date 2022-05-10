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

package base

import (
	"fmt"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	NODE_UNLOAD NodeState = iota
	NODE_LOADING
	NODE_ROOLBACK
	NODE_COMMIT
	NODE_UNLOADING
	NODE_LOADED
)

func NodeStateString(state NodeState) string {
	switch state {
	case NODE_UNLOAD:
		return "UNLOAD"
	case NODE_LOADING:
		return "LOADING"
	case NODE_ROOLBACK:
		return "ROLLBACK"
	case NODE_COMMIT:
		return "COMMIT"
	case NODE_UNLOADING:
		return "UNLOADING"
	case NODE_LOADED:
		return "LOADED"
	}
	panic(fmt.Sprintf("unsupported: %d", state))
}
