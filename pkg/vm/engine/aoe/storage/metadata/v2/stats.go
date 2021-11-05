package metadata

import (
	"fmt"
	"sync/atomic"
)

type shardStats struct {
	id          uint64
	coarseSize  int64
	coarseCount int64
}

func newShardStats(id uint64) *shardStats {
	return &shardStats{
		id: id,
	}
}

func (stats *shardStats) AddCount(count int64) {
	atomic.AddInt64(&stats.coarseCount, count)
}

func (stats *shardStats) GetCount() int64 {
	return atomic.LoadInt64(&stats.coarseCount)
}

func (stats *shardStats) AddSize(size int64) {
	atomic.AddInt64(&stats.coarseSize, size)
}

func (stats *shardStats) GetSize() int64 {
	return atomic.LoadInt64(&stats.coarseSize)
}

func (stats *shardStats) String() string {
	return fmt.Sprintf("Stats<%d>(Size=%d, Count=%d)", stats.id, stats.GetSize(), stats.GetCount())
}
