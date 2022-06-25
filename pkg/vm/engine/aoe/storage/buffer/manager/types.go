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

package manager

import (
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"sync"
)

var (
	MockVFile = mockVFile{}
)

type IEvictHandle interface {
	sync.Locker
	IsClosed() bool
	Unload()
	Unloadable() bool
	Iteration() uint64
}

type EvictNode struct {
	Handle IEvictHandle
	Iter   uint64
}

type IEvictHolder interface {
	sync.Locker
	Enqueue(n *EvictNode)
	Dequeue() *EvictNode
}

type BufferManager struct {
	buf.IMemoryPool
	sync.RWMutex
	Nodes           map[uint64]iface.INodeHandle // Manager is not responsible to Close handle
	EvictHolder     IEvictHolder
	NextID          uint64
	NextTransientID uint64
	EvictTimes      int64
	LoadTimes       int64
	UnregisterTimes int64
	Dir             []byte
}

type mockVFile struct{}

func (vf *mockVFile) Ref()   {}
func (vf *mockVFile) Unref() {}
func (vf *mockVFile) Read(p []byte) (n int, err error) {
	return n, err
}
