package flusher

import (
	"errors"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	ErrDuplicateNodeFlusher = errors.New("duplicate flusher node")
	ErrNotFoundNodeFlusher  = errors.New("not found flusher node")
)

type shardFlusher struct {
	mu           *sync.RWMutex
	id           uint64
	mask         *roaring64.Bitmap
	driver       NodeDriver
	pendingItems int
}

func newShardFlusher(id uint64, driver NodeDriver) *shardFlusher {
	return &shardFlusher{
		id:     id,
		mu:     new(sync.RWMutex),
		mask:   roaring64.New(),
		driver: driver,
	}
}

func (sf *shardFlusher) updatePengingItems(items int) bool {
	sf.pendingItems = items
	if items >= 1000 {
		return true
	}
	return false
}

func (sf *shardFlusher) addNode(id uint64) error {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if sf.mask.Contains(id) {
		return ErrDuplicateNodeFlusher
	}
	sf.mask.Add(id)
	return nil
}

func (sf *shardFlusher) deleteNode(nodeId uint64) (left uint64, err error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()
	if !sf.mask.Contains(nodeId) {
		err = ErrNotFoundNodeFlusher
		return
	}
	sf.mask.Remove(nodeId)
	left = sf.mask.GetCardinality()
	return
}

func (sf *shardFlusher) doFlush() {
	nodes := sf.getNodes()
	it := nodes.Iterator()
	for it.HasNext() {
		id := it.Next()
		if err := sf.driver.FlushNode(id); err != nil {
			logutil.Warnf("cannot flush shard-%d node-%d", sf.id, id)
			break
		}
	}
}

func (sf *shardFlusher) getNodes() *roaring64.Bitmap {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	return sf.mask.Clone()
}

func (sf *shardFlusher) String() string {
	sf.mu.RLock()
	defer sf.mu.RUnlock()
	str := fmt.Sprintf("ShardFlusher<%d>:{", sf.id)
	for k, _ := range sf.mask.ToArray() {
		str = fmt.Sprintf("%s %d", str, k)
	}
	str = fmt.Sprintf("%s}", str)
	return str
}
