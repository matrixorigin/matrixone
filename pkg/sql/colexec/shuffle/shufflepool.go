// Copyright 2024 Matrix Origin
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

package shuffle

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ShufflePool struct {
	bucketNum  int32
	maxHolders int32
	drainAll   bool

	holders    int32
	finished   int32
	stoppers   int32
	holderLock sync.Mutex
	aborted    bool
	cleaned    bool

	batchSets  []*batch.BatchSet
	batchLocks []sync.Mutex
	batchPool  []*batch.Batch
	batchLock  sync.Mutex

	batchWaiters   []chan bool
	endingWaiters  []chan bool
	anyBatchWaiter chan struct{}
	endingWaiter   chan struct{}
	endingOnce     sync.Once

	readyLimit         int
	readyCount         int
	readyLock          sync.Mutex
	spaceWaiter        chan struct{}
	bucketReadyCounts  []int
	bucketSpaceWaiters []chan struct{}
	readyBuckets       chan int32
	finalCursor        atomic.Uint32

	memoryLock sync.Mutex
	tracked    map[*batch.Batch]int64
	current    int64
	peak       int64
}

func NewShufflePool(bucketNum int32, maxHolders int32, drainAll bool) *ShufflePool {
	allBuckets := drainAll
	readyLimit := max(2, int(maxHolders)*2)
	sp := &ShufflePool{
		bucketNum:          bucketNum,
		maxHolders:         maxHolders,
		drainAll:           allBuckets,
		batchSets:          make([]*batch.BatchSet, bucketNum),
		batchLocks:         make([]sync.Mutex, bucketNum),
		batchWaiters:       make([]chan bool, bucketNum),
		endingWaiters:      make([]chan bool, bucketNum),
		batchPool:          make([]*batch.Batch, 0, readyLimit),
		anyBatchWaiter:     make(chan struct{}, 1),
		endingWaiter:       make(chan struct{}),
		readyLimit:         readyLimit,
		spaceWaiter:        make(chan struct{}),
		bucketReadyCounts:  make([]int, bucketNum),
		bucketSpaceWaiters: make([]chan struct{}, bucketNum),
		tracked:            make(map[*batch.Batch]int64),
	}
	if allBuckets {
		sp.readyBuckets = make(chan int32, readyLimit)
	}
	for i := range sp.batchSets {
		sp.batchSets[i] = batch.NewBatchSet(objectio.BlockMaxRows)
		sp.batchWaiters[i] = make(chan bool, 1)
		sp.endingWaiters[i] = make(chan bool, 1)
		sp.bucketSpaceWaiters[i] = make(chan struct{})
	}
	return sp
}

func (sp *ShufflePool) hold() bool {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	if sp.aborted || sp.cleaned {
		return false
	}
	sp.holders++
	if sp.holders > sp.maxHolders {
		panic("shuffle pool too many holders!")
	}
	return true
}

func (sp *ShufflePool) stopWriting() {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	sp.stoppers++
	if sp.stoppers > sp.holders || sp.stoppers > sp.maxHolders {
		panic("shuffle pool too many stoppers!")
	}
	if sp.stoppers == sp.maxHolders {
		for i := range sp.endingWaiters {
			select {
			case sp.endingWaiters[i] <- true:
			default:
			}
		}
		sp.signalEndLocked()
	}
}

func (sp *ShufflePool) allStop() bool {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	return sp.aborted || sp.stoppers == sp.maxHolders
}

// release returns the pool peak only to the holder that owns final cleanup.
func (sp *ShufflePool) release(m *mpool.MPool, failed bool) (int64, bool) {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	if failed {
		sp.abortLocked()
	}
	sp.finished++
	if sp.finished > sp.holders {
		panic("shuffle pool too many finished holders!")
	}
	if sp.finished != sp.holders || (!sp.aborted && sp.holders != sp.maxHolders) {
		return 0, false
	}
	peak := sp.memoryPeak()
	sp.cleanupLocked(m)
	return peak, true
}

func (sp *ShufflePool) abort(m *mpool.MPool) {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	sp.abortLocked()
	if sp.finished == sp.holders {
		sp.cleanupLocked(m)
	}
}

func (sp *ShufflePool) abortLocked() {
	if sp.aborted {
		return
	}
	sp.aborted = true
	for i := range sp.endingWaiters {
		select {
		case sp.endingWaiters[i] <- true:
		default:
		}
	}
	sp.signalEndLocked()
}

func (sp *ShufflePool) signalEndLocked() {
	sp.endingOnce.Do(func() { close(sp.endingWaiter) })
}

func (sp *ShufflePool) cleanupLocked(m *mpool.MPool) {
	if sp.cleaned {
		return
	}
	sp.cleaned = true
	for i := range sp.batchSets {
		if !sp.aborted && sp.batchSets[i].RowCount() > 0 {
			logutil.Warnf("shuffle pool reset, batch %v rowcnt %v, maybe something wrong!", i, sp.batchSets[i].RowCount())
		}
		for j := 0; j < sp.batchSets[i].Length(); j++ {
			sp.forgetBatch(sp.batchSets[i].Get(j))
		}
		sp.batchSets[i].Clean(m)
	}
	sp.cleanBatchPool(m)
}

func (sp *ShufflePool) cleanBatchPool(m *mpool.MPool) {
	sp.batchLock.Lock()
	defer sp.batchLock.Unlock()
	for _, bat := range sp.batchPool {
		sp.forgetBatch(bat)
		bat.Clean(m)
	}
	sp.batchPool = sp.batchPool[:0]
}

func (sp *ShufflePool) putBatchToPool(buf *batch.Batch, m *mpool.MPool) {
	sp.syncBatch(buf)
	sp.batchLock.Lock()
	defer sp.batchLock.Unlock()
	if len(sp.batchPool) < sp.readyLimit {
		sp.batchPool = append(sp.batchPool, buf)
		return
	}
	sp.forgetBatch(buf)
	buf.Clean(m)
}

func (sp *ShufflePool) discardBatch(buf *batch.Batch, m *mpool.MPool) {
	if buf == nil {
		return
	}
	sp.forgetBatch(buf)
	buf.Clean(m)
}

func (sp *ShufflePool) getBatchFromPool() *batch.Batch {
	sp.batchLock.Lock()
	defer sp.batchLock.Unlock()
	if len(sp.batchPool) == 0 {
		return nil
	}
	last := len(sp.batchPool) - 1
	buf := sp.batchPool[last]
	sp.batchPool = sp.batchPool[:last]
	return buf
}

func (sp *ShufflePool) syncBatch(buf *batch.Batch) {
	sp.memoryLock.Lock()
	defer sp.memoryLock.Unlock()
	allocated := int64(buf.Allocated())
	previous := sp.tracked[buf]
	sp.tracked[buf] = allocated
	sp.current += allocated - previous
	sp.peak = max(sp.peak, sp.current)
}

func (sp *ShufflePool) syncBatchSet(bs *batch.BatchSet) {
	for i := 0; i < bs.Length(); i++ {
		sp.syncBatch(bs.Get(i))
	}
}

func (sp *ShufflePool) forgetBatch(buf *batch.Batch) {
	sp.memoryLock.Lock()
	defer sp.memoryLock.Unlock()
	if allocated, ok := sp.tracked[buf]; ok {
		sp.current -= allocated
		delete(sp.tracked, buf)
	}
}

func (sp *ShufflePool) memoryPeak() int64 {
	sp.memoryLock.Lock()
	defer sp.memoryLock.Unlock()
	return sp.peak
}

func (sp *ShufflePool) reserveReady(bucket int32, count int) (<-chan struct{}, bool) {
	if count == 0 {
		return nil, true
	}
	sp.readyLock.Lock()
	defer sp.readyLock.Unlock()
	if !sp.drainAll {
		// A fixed-bucket holder can only release batches from its own bucket.
		// Bound each bucket independently so a hot bucket cannot consume the
		// credits needed to publish work for every other holder.
		const fixedBucketReadyLimit = 2
		if sp.bucketReadyCounts[bucket]+count > fixedBucketReadyLimit {
			return sp.bucketSpaceWaiters[bucket], false
		}
		sp.bucketReadyCounts[bucket] += count
		sp.readyCount += count
		return nil, true
	}
	if sp.readyCount+count > sp.readyLimit {
		return sp.spaceWaiter, false
	}
	sp.readyCount += count
	return nil, true
}

func (sp *ShufflePool) releaseReady(bucket int32, count int) {
	if count == 0 {
		return
	}
	sp.readyLock.Lock()
	sp.readyCount -= count
	if sp.readyCount < 0 {
		sp.readyLock.Unlock()
		panic("shuffle pool negative ready batch count")
	}
	if sp.drainAll {
		close(sp.spaceWaiter)
		sp.spaceWaiter = make(chan struct{})
	} else {
		sp.bucketReadyCounts[bucket] -= count
		if sp.bucketReadyCounts[bucket] < 0 {
			sp.readyLock.Unlock()
			panic("shuffle pool negative bucket ready batch count")
		}
		close(sp.bucketSpaceWaiters[bucket])
		sp.bucketSpaceWaiters[bucket] = make(chan struct{})
	}
	sp.readyLock.Unlock()
}

func (sp *ShufflePool) publishReady(bucket int32, count int) {
	if count == 0 {
		return
	}
	if sp.drainAll {
		for range count {
			sp.readyBuckets <- bucket
			sp.notifyAnyBatch()
		}
		return
	}
	select {
	case sp.batchWaiters[bucket] <- true:
	default:
	}
}

func (sp *ShufflePool) getFullBatch(shuffleIDX int32) *batch.Batch {
	sp.batchLocks[shuffleIDX].Lock()
	var bat *batch.Batch
	if sp.batchSets[shuffleIDX].ReadyCount() > 0 {
		bat = sp.batchSets[shuffleIDX].PopFront()
		bat.ShuffleIDX = shuffleIDX
	}
	sp.batchLocks[shuffleIDX].Unlock()
	if bat != nil {
		sp.releaseReady(shuffleIDX, 1)
	}
	return bat
}

func (sp *ShufflePool) getAnyFullBatch() *batch.Batch {
	if !sp.drainAll {
		return nil
	}
	select {
	case bucket := <-sp.readyBuckets:
		select {
		case <-sp.anyBatchWaiter:
		default:
		}
		bat := sp.popReadyBatch(bucket)
		if len(sp.readyBuckets) > 0 {
			sp.notifyAnyBatch()
		}
		return bat
	default:
		return nil
	}
}

func (sp *ShufflePool) popReadyBatch(bucket int32) *batch.Batch {
	sp.batchLocks[bucket].Lock()
	bat := sp.batchSets[bucket].PopFront()
	if bat != nil {
		bat.ShuffleIDX = bucket
	}
	sp.batchLocks[bucket].Unlock()
	if bat == nil {
		panic("shuffle pool ready queue is inconsistent")
	}
	sp.releaseReady(bucket, 1)
	return bat
}

func (sp *ShufflePool) getLastBatch(shuffleIDX int32) *batch.Batch {
	sp.batchLocks[shuffleIDX].Lock()
	defer sp.batchLocks[shuffleIDX].Unlock()
	bat := sp.batchSets[shuffleIDX].Pop()
	if bat != nil {
		bat.ShuffleIDX = shuffleIDX
	}
	return bat
}

// getLastPartialBatch claims only a non-ready tail. Full batches remain owned
// by their ready queue tokens, including tokens already claimed by a consumer.
func (sp *ShufflePool) getLastPartialBatch(shuffleIDX int32) *batch.Batch {
	sp.batchLocks[shuffleIDX].Lock()
	defer sp.batchLocks[shuffleIDX].Unlock()
	bs := sp.batchSets[shuffleIDX]
	if bs.Length() == bs.ReadyCount() {
		return nil
	}
	bat := bs.Pop()
	bat.ShuffleIDX = shuffleIDX
	return bat
}

func (sp *ShufflePool) getAnyLastBatch() *batch.Batch {
	for {
		idx := sp.finalCursor.Add(1) - 1
		if idx >= uint32(sp.bucketNum) {
			return nil
		}
		if bat := sp.getLastPartialBatch(int32(idx)); bat != nil {
			return bat
		}
	}
}

func (sp *ShufflePool) waitBatchOrEnd(shuffleIDX int32, proc *process.Process) {
	select {
	case <-sp.batchWaiters[shuffleIDX]:
	case <-sp.endingWaiters[shuffleIDX]:
	case <-proc.Ctx.Done():
	}
}

func (sp *ShufflePool) waitAnyBatchOrEnd(proc *process.Process) {
	select {
	case <-sp.endingWaiter:
	case <-proc.Ctx.Done():
	case <-sp.anyBatchWaiter:
	}
}

func (sp *ShufflePool) notifyAnyBatch() {
	select {
	case sp.anyBatchWaiter <- struct{}{}:
	default:
	}
}

// tryWrite writes selections starting at bucket/offset. It never mutates the
// blocked chunk, so the operator can safely retain and resume the child batch.
func (sp *ShufflePool) tryWrite(
	srcBatch *batch.Batch,
	sels [][]int32,
	startBucket int,
	startOffset int,
	proc *process.Process,
) (nextBucket int, nextOffset int, wait <-chan struct{}, done bool, err error) {
	for bucket := startBucket; bucket < len(sp.batchSets); bucket++ {
		offset := 0
		if bucket == startBucket {
			offset = startOffset
		}
		current := sels[bucket]
		for offset < len(current) {
			end := min(offset+objectio.BlockMaxRows, len(current))
			chunk := current[offset:end]
			sp.batchLocks[bucket].Lock()
			readyDelta := sp.batchSets[bucket].ReadyDelta(len(chunk))
			wait, ok := sp.reserveReady(int32(bucket), readyDelta)
			if !ok {
				sp.batchLocks[bucket].Unlock()
				return bucket, offset, wait, false, nil
			}

			oldReady := sp.batchSets[bucket].ReadyCount()
			buf := sp.getBatchFromPool()
			consumed, writeErr := sp.batchSets[bucket].Union(proc.Mp(), srcBatch, chunk, buf)
			if !consumed && buf != nil {
				sp.putBatchToPool(buf, proc.Mp())
			}
			sp.syncBatchSet(sp.batchSets[bucket])
			actualDelta := sp.batchSets[bucket].ReadyCount() - oldReady
			if actualDelta < readyDelta {
				sp.releaseReady(int32(bucket), readyDelta-actualDelta)
			}
			sp.batchLocks[bucket].Unlock()
			sp.publishReady(int32(bucket), actualDelta)
			if writeErr != nil {
				return bucket, offset, nil, false, writeErr
			}
			offset = end
		}
	}
	return len(sp.batchSets), 0, nil, true, nil
}
