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
	"io"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
)

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
	GetState() iface.NodeState
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
