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
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"testing"

	"github.com/stretchr/testify/assert"
	// "os"
	// "context"
)

var WORK_DIR = "/tmp/buff/node_test"

func TestNode(t *testing.T) {
	node_capacity := uint64(4096)
	capacity := node_capacity * 4
	pool := buf.NewSimpleMemoryPool(capacity)
	assert.NotNil(t, pool)
	assert.Equal(t, capacity, pool.GetCapacity())
	assert.Equal(t, uint64(0), pool.GetUsage())
	node1 := pool.Alloc(common.NewMemFile(int64(node_capacity)), false, buf.RawMemoryNodeConstructor)
	assert.NotNil(t, node1)
	assert.Equal(t, node_capacity, node1.GetMemoryCapacity())
	assert.Equal(t, capacity, pool.GetCapacity())
	assert.Equal(t, node_capacity, pool.GetUsage())

	id := uint64(0)
	id1 := id
	id++
	node_buff1 := NewNodeBuffer(id1, node1)
	assert.Equal(t, node_buff1.GetCapacity(), node_capacity)
	assert.Equal(t, id1, node_buff1.GetID())

	node2 := pool.Alloc(common.NewMemFile(int64(node_capacity)), false, buf.RawMemoryNodeConstructor)
	assert.NotNil(t, node2)
	id2 := id
	id++
	node_buff2 := NewNodeBuffer(id2, node2)
	assert.Equal(t, node_buff2.GetCapacity(), node_capacity)
	assert.Equal(t, id2, node_buff2.GetID())
	assert.Equal(t, 2*node_capacity, pool.GetUsage())

	node_buff1.Close()
	assert.Equal(t, node_capacity, pool.GetUsage())

	node_buff2.Close()
	assert.Equal(t, uint64(0), pool.GetUsage())
}

func TestHandle(t *testing.T) {
	id := uint64(0)
	ctx := NodeHandleCtx{
		ID: id,
	}

	handle := NewNodeHandle(&ctx)
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(0))
	assert.False(t, handle.HasRef())

	handle.Ref()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(1))
	assert.True(t, handle.HasRef())

	handle.Ref()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(2))
	assert.True(t, handle.HasRef())

	handle.UnRef()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(1))
	assert.True(t, handle.HasRef())

	handle.UnRef()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(0))
	assert.False(t, handle.HasRef())

	handle.UnRef()
	assert.Equal(t, handle.(*NodeHandle).Refs, uint64(0))
	assert.False(t, handle.HasRef())
}
