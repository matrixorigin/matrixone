package flusher

import (
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"
	"github.com/stretchr/testify/assert"
)

var global_dummies map[uint64]*DummyNodeDriver

func init() {
	global_dummies = make(map[uint64]*DummyNodeDriver)
}

type DummyNodeDriver struct {
	id  uint64
	set *roaring64.Bitmap
}

func (d *DummyNodeDriver) GetId() uint64 {
	return d.id
}

func (d *DummyNodeDriver) FlushNode(id uint64) error {
	d.set.Add(id)
	return nil
}

func dummy_factory(id uint64) NodeDriver {
	driver := &DummyNodeDriver{
		id:  id,
		set: roaring64.New(),
	}
	global_dummies[id] = driver
	return driver
}

func TestShardFlusher(t *testing.T) {
	shard_id := uint64(42)
	node_driver := dummy_factory(shard_id)
	flusher := newShardFlusher(shard_id, node_driver)

	assert.Nil(t, flusher.addNode(uint64(11)))
	assert.Equal(t, ErrDuplicateNodeFlusher, flusher.addNode(uint64(11)))
	_, err := flusher.deleteNode(uint64(13))
	assert.Equal(t, ErrNotFoundNodeFlusher, err)
	flusher.addNode(uint64(3))
	left, _ := flusher.deleteNode(uint64(11))
	assert.Equal(t, uint64(1), left)

	flusher.addNode(uint64(12))
	flusher.addNode(uint64(7))

	t.Logf("%s", flusher)

	flusher.doFlush()
	assert.Equal(t, uint64(3), global_dummies[shard_id].set.GetCardinality())
}

func TestFlusher(t *testing.T) {
	wait_time := 5 * time.Millisecond
	driver := NewDriver()
	driver.InitFactory(dummy_factory)
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
	time.Sleep(wait_time)
	t.Logf("%s", driver)
	assert.Equal(t, uint64(2), global_dummies[shard_id].set.GetCardinality())

	driver.OnStats([]*shard.ItemsToCheckpointStat{
		{ShardId: shard_id2, Count: 10},
	})
	time.Sleep(wait_time)
	assert.Equal(t, uint64(1), global_dummies[shard_id2].set.GetCardinality())

	driver.ShardNodeDeleted(shard_id2, uint64(11)) // shard-84 will be removed
	time.Sleep(wait_time)
	assert.Equal(t, 1, len(driver.shards))
}
