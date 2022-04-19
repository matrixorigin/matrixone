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

package flusher

import (
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/shard"
	"github.com/stretchr/testify/assert"
)

type mockHub struct {
	sync.RWMutex
	shards map[uint64]*DummyNodeDriver
}

func newMockHub() *mockHub {
	return &mockHub{
		shards: make(map[uint64]*DummyNodeDriver),
	}
}

func (hub *mockHub) add(id uint64, driver *DummyNodeDriver) bool {
	hub.Lock()
	defer hub.Unlock()
	e := hub.shards[id]
	if e != nil {
		return false
	}

	hub.shards[id] = driver
	return true
}

func (hub *mockHub) get(id uint64) *DummyNodeDriver {
	hub.RLock()
	defer hub.RUnlock()
	return hub.shards[id]
}

type DummyNodeDriver struct {
	sync.RWMutex
	id  uint64
	set *roaring64.Bitmap
}

func (d *DummyNodeDriver) GetId() uint64 {
	return d.id
}

func (d *DummyNodeDriver) FlushNode(id uint64) error {
	d.Lock()
	defer d.Unlock()
	d.set.Add(id)
	return nil
}

func (d *DummyNodeDriver) GetFlushed() uint64 {
	d.RLock()
	defer d.RUnlock()
	return d.set.GetCardinality()
}

func dummy_factory(id uint64, hub *mockHub) NodeDriver {
	driver := &DummyNodeDriver{
		id:  id,
		set: roaring64.New(),
	}
	hub.add(id, driver)
	return driver
}

func TestShardFlusher(t *testing.T) {
	hub := newMockHub()
	shard_id := uint64(42)
	node_driver := dummy_factory(shard_id, hub)
	flusher := newShardFlusher(shard_id, node_driver)

	assert.Nil(t, flusher.addNode(uint64(11)))
	assert.Equal(t, ErrDuplicateNodeFlusher, flusher.addNode(uint64(11)))
	_, err := flusher.deleteNode(uint64(13))
	assert.Equal(t, ErrNotFoundNodeFlusher, err)
	err = flusher.addNode(uint64(3))
	assert.Nil(t, err)
	left, _ := flusher.deleteNode(uint64(11))
	assert.Equal(t, uint64(1), left)

	err = flusher.addNode(uint64(12))
	assert.Nil(t, err)
	err = flusher.addNode(uint64(7))
	assert.Nil(t, err)

	t.Logf("%s", flusher)

	flusher.doFlush()
	assert.Equal(t, uint64(3), hub.get(shard_id).GetFlushed())
}

func TestFlusher(t *testing.T) {
	driver := NewDriver()
	hub := newMockHub()
	factory := func(id uint64) NodeDriver {
		return dummy_factory(id, hub)
	}
	driver.InitFactory(factory)
	driver.Start()
	defer driver.Stop()

	shard_id := uint64(42)
	shard_id2 := uint64(84)

	driver.ShardDeleted(shard_id) // nothing happened
	driver.ShardCreated(shard_id)

	driver.ShardNodeDeleted(uint64(1), uint64(1)) // no shard, warning
	driver.ShardNodeDeleted(shard_id, uint64(1))  // no node, warning

	driver.ShardNodeCreated(shard_id, uint64(1))
	driver.ShardNodeCreated(shard_id, uint64(2))
	driver.ShardNodeCreated(shard_id2, uint64(11))
	driver.OnStats([]*shard.ItemsToCheckpointStat{
		{ShardId: shard_id, Count: 10},
		{ShardId: shard_id, Count: 2},
	})
	testutils.WaitExpect(100, func() bool {
		o := hub.get(shard_id)
		if o == nil {
			return false
		}
		return o.GetFlushed() == uint64(2)
	})
	t.Logf("%s", driver)
	assert.Equal(t, uint64(2), hub.get(shard_id).GetFlushed())

	driver.OnStats([]*shard.ItemsToCheckpointStat{
		{ShardId: shard_id2, Count: 10},
	})
	testutils.WaitExpect(100, func() bool {
		o := hub.get(shard_id2)
		if o == nil {
			return false
		}
		return o.GetFlushed() == uint64(1)
	})
	assert.Equal(t, uint64(1), hub.get(shard_id2).GetFlushed())

	driver.ShardNodeDeleted(shard_id2, uint64(11)) // shard-84 will be removed
	testutils.WaitExpect(100, func() bool {
		return driver.ShardCnt() == 1
	})
	assert.Equal(t, 1, driver.ShardCnt())
}
