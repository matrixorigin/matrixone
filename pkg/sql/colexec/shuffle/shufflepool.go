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
	"context"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/sys/cpu"
)

const (
	shuffleMemoryShardCount            = 64
	shuffleBatchPoolShardSize          = 2
	shuffleFixedBucketReadyLimit       = 2
	shuffleProducerTailBlocksPerBucket = shuffleFixedBucketReadyLimit * shuffleBatchPoolShardSize
)

// MaxOwnedBuckets bounds the metadata and buffers of a statement-owned shuffle.
const MaxOwnedBuckets = 8

type shuffleMemoryShard struct {
	sync.Mutex
	tracked map[*batch.Batch]shuffleTrackedBatch
	_       cpu.CacheLinePad
}

type shuffleTrackedBatch struct {
	allocated int64
	producer  int32
}

type shuffleBatchPoolShard struct {
	sync.Mutex
	batches [shuffleBatchPoolShardSize]*batch.Batch
	count   int
	_       cpu.CacheLinePad
}

type ShufflePool struct {
	bucketNum            int32
	maxHolders           int32
	drainAll             bool
	ownedOutput          bool
	ownedColumns         int
	ownedInit            sync.Once
	ownedInitErr         error
	ownedReservation     *process.ExecutionTransientMemoryReservation
	outputAllocation     *vector.AllocationAccountSelection
	producerBatchMaxRows int
	sharedLaneOnce       sync.Once
	sharedBatchAccount   *vector.AllocationAccountSelection
	sharedVecAccounts    []*vector.AllocationAccountSelection

	holders    int32
	finished   int32
	stoppers   int32
	consumers  int32
	holderLock sync.Mutex
	aborted    bool
	abortErr   error
	cleaned    bool
	producers  map[int32]context.CancelCauseFunc

	// Each destination bucket keeps one writable lane per producer. Accounted
	// vectors cannot change allocation provenance after their first allocation,
	// so sharing one tail across producers would seal and publish a partial batch
	// on every producer switch.
	batchSets      [][]*batch.BatchSet
	readyProducers [][]int32
	batchLocks     []sync.Mutex
	closed         []bool

	// Recycle storage is sharded by the local holder bound, not by the global
	// bucket count. This keeps lookup O(1), gives each concurrent holder two
	// cache slots, and prevents one previously hot bucket from consuming the
	// capacity needed to warm every other holder shard.
	batchPools []shuffleBatchPoolShard

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

	memoryShards [shuffleMemoryShardCount]shuffleMemoryShard
	current      atomic.Int64
	peak         atomic.Int64
}

func NewShufflePool(bucketNum int32, maxHolders int32, drainAll bool) *ShufflePool {
	sp := &ShufflePool{bucketNum: bucketNum, maxHolders: maxHolders, drainAll: drainAll}
	sp.initBuckets(nil)
	return sp
}

// NewOwnedShufflePool delays its bucket allocations until the execution's
// account has been attached to a runtime clone.
func NewOwnedShufflePool(bucketNum, maxHolders int32, columns int) *ShufflePool {
	return &ShufflePool{bucketNum: bucketNum, maxHolders: maxHolders, drainAll: true, ownedOutput: true, ownedColumns: columns}
}

func (sp *ShufflePool) initBuckets(selection *vector.AllocationAccountSelection) {
	bucketNum, maxHolders, allBuckets := sp.bucketNum, sp.maxHolders, sp.drainAll
	readyLimit := max(2, int(maxHolders)*2)
	batchPoolShards := max(1, int(maxHolders))
	producerLanes := max(1, int(maxHolders))
	producerBatchMaxRows := objectio.BlockMaxRows
	if producerLanes > shuffleProducerTailBlocksPerBucket {
		// Bound all producer-local writable tails for one destination to the
		// same number of full batches as its ready and recycle capacity combined.
		// This still coalesces several evenly shuffled inputs at high DOP without
		// letting the DOP-squared lane count retain a full block for every pair.
		producerBatchMaxRows = max(
			1,
			objectio.BlockMaxRows*shuffleProducerTailBlocksPerBucket/producerLanes,
		)
	}
	sp.producerBatchMaxRows = producerBatchMaxRows
	sp.outputAllocation = selection
	sp.batchSets = make([][]*batch.BatchSet, bucketNum)
	sp.readyProducers = make([][]int32, bucketNum)
	sp.batchLocks = make([]sync.Mutex, bucketNum)
	sp.closed = make([]bool, bucketNum)
	sp.batchWaiters = make([]chan bool, bucketNum)
	sp.endingWaiters = make([]chan bool, bucketNum)
	sp.batchPools = make([]shuffleBatchPoolShard, batchPoolShards)
	sp.anyBatchWaiter = make(chan struct{}, 1)
	sp.endingWaiter = make(chan struct{})
	sp.readyLimit = readyLimit
	sp.spaceWaiter = make(chan struct{})
	sp.bucketReadyCounts = make([]int, bucketNum)
	sp.bucketSpaceWaiters = make([]chan struct{}, bucketNum)
	sp.producers = make(map[int32]context.CancelCauseFunc)
	if allBuckets {
		sp.readyBuckets = make(chan int32, readyLimit)
	}
	for i := range sp.batchSets {
		sp.batchSets[i] = make([]*batch.BatchSet, producerLanes)
		for producer := range sp.batchSets[i] {
			sp.batchSets[i][producer] = batch.NewBatchSetWithAllocation(producerBatchMaxRows, selection)
		}
		if producerBatchMaxRows < objectio.BlockMaxRows {
			// Inputs with identical allocation provenance can share a full-sized
			// tail. Other provenance continues to use bounded producer-local lanes.
			// This adds at most one block per destination, not one per producer.
			sp.batchSets[i] = append(sp.batchSets[i], batch.NewBatchSetWithAllocation(objectio.BlockMaxRows, selection))
		}
		sp.batchWaiters[i] = make(chan bool, 1)
		sp.endingWaiters[i] = make(chan bool, 1)
		sp.bucketSpaceWaiters[i] = make(chan struct{})
	}
}

func (sp *ShufflePool) initOwned(proc *process.Process, selection *vector.AllocationAccountSelection) error {
	sp.ownedInit.Do(func() {
		if err := sp.ownedFailure(proc, nil); err != nil {
			sp.ownedInitErr = err
			return
		}
		if sp.bucketNum < 1 || sp.bucketNum > MaxOwnedBuckets || sp.maxHolders < 1 || sp.maxHolders > 8 || sp.ownedColumns < 1 || selection == nil {
			sp.ownedInitErr = sp.ownedFailure(proc, mpool.ErrAllocationAccountInvalid)
			return
		}
		if err := process.PipelineContextError(proc.Ctx); err != nil {
			sp.ownedInitErr = sp.ownedFailure(proc, err)
			return
		}
		// Cover bounded pool and dispatch-spool descriptors as well as vector
		// headers for every lane, ready slot, recycle slot and spool slot. The
		// reservation outlives the pool until transport borrowers are drained.
		p, s := uint64(sp.bucketNum), uint64(sp.maxHolders)
		slots := 2*p*s + 6*s + 3*p + 16
		perColumn := uint64(unsafe.Sizeof(vector.Vector{})) + 256
		fixed := uint64(64<<10) + p*s*(4<<10)
		if uint64(sp.ownedColumns) > (math.MaxUint64-fixed)/slots/perColumn {
			sp.ownedInitErr = sp.ownedFailure(proc, mpool.ErrAllocationAccountInvalid)
			return
		}
		metadata := fixed + slots*uint64(sp.ownedColumns)*perColumn
		reservation, err := sp.reserveOwnedMemory(proc, metadata)
		if err != nil {
			sp.ownedInitErr = err
			return
		}
		sp.holderLock.Lock()
		defer sp.holderLock.Unlock()
		if sp.aborted || sp.cleaned || process.PipelineContextError(proc.Ctx) != nil {
			reservation.Release()
			sp.ownedInitErr = sp.abortErr
			if sp.ownedInitErr == nil {
				sp.ownedInitErr = process.PipelineContextError(proc.Ctx)
			}
			if sp.ownedInitErr == nil {
				sp.ownedInitErr = context.Canceled
			}
			return
		}
		sp.ownedReservation = reservation
		// Leave no reservation or partial bucket state behind if initialization
		// panics. A normal allocation failure is returned above.
		initialized := false
		defer func() {
			if !initialized {
				sp.abortLocked(nil)
				sp.cleanupLocked(proc.Mp())
				sp.ownedReservation.Release()
				sp.ownedReservation = nil
			}
		}()
		sp.initBuckets(selection)
		initialized = true
	})
	if sp.ownedInitErr != nil {
		return sp.ownedInitErr
	}
	if !vector.AllocationAccountSelectionsEqual(sp.outputAllocation, selection) {
		return mpool.ErrAllocationAccountMismatch
	}
	return nil
}

type ownedMemoryAdmission struct {
	pool    *ShufflePool
	stopped bool
}

func (a *ownedMemoryAdmission) Lock()   { a.pool.holderLock.Lock() }
func (a *ownedMemoryAdmission) Unlock() { a.pool.holderLock.Unlock() }

func (a *ownedMemoryAdmission) ResolveAdmissionFailure(err error, stopped bool) error {
	if a.pool.abortErr != nil {
		return a.pool.abortErr
	}
	if stopped {
		a.stopped = true
		return context.Canceled
	}
	if a.pool.aborted || a.pool.cleaned {
		return context.Canceled
	}
	a.pool.abortLocked(err)
	return err
}

func (sp *ShufflePool) reserveOwnedMemory(proc *process.Process, size uint64) (*process.ExecutionTransientMemoryReservation, error) {
	budget, err := proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, sp.ownedFailure(proc, err)
	}
	admission := &ownedMemoryAdmission{pool: sp}
	reservation, err := budget.ReserveTransientMemoryGuarded(size, proc.Ctx.Done(), admission)
	if admission.stopped {
		if cause := process.PipelineContextError(proc.Ctx); cause != nil {
			return nil, cause
		}
	}
	return reservation, err
}

// Resolve an admission failure against a terminal event published while
// admission was in progress. An earlier producer failure keeps its identity.
func (sp *ShufflePool) ownedFailure(proc *process.Process, fallback error) error {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	if sp.abortErr != nil {
		return sp.abortErr
	}
	if cause := process.PipelineContextError(proc.Ctx); cause != nil {
		return cause
	}
	if sp.aborted || sp.cleaned {
		return context.Canceled
	}
	return fallback
}

// Runtime account attachment may roll back before this lazy descriptor has
// been initialized. In that case it owns no allocation or admitted holder.
func (sp *ShufflePool) hasOwnedResources() bool {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	return sp.ownedReservation != nil || sp.holders != sp.finished
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

func (sp *ShufflePool) terminalError() error {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	return sp.abortErr
}

func (sp *ShufflePool) registerProducer(bucket int32, cancel context.CancelCauseFunc) {
	sp.holderLock.Lock()
	sp.producers[bucket] = cancel
	allConsumersClosed := sp.consumers == sp.maxHolders
	sp.holderLock.Unlock()
	if allConsumersClosed {
		cancel(nil)
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
		sp.abortLocked(nil)
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
	sp.abortWithError(m, nil)
}

func (sp *ShufflePool) abortWithError(m *mpool.MPool, err error) {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	sp.abortLocked(err)
	if sp.finished == sp.holders {
		sp.cleanupLocked(m)
	}
}

func (sp *ShufflePool) abortLocked(err error) {
	if sp.abortErr == nil && err != nil {
		sp.abortErr = err
	}
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
	if sp.endingWaiter != nil {
		sp.endingOnce.Do(func() { close(sp.endingWaiter) })
	}
}

func (sp *ShufflePool) cleanupLocked(m *mpool.MPool) {
	if sp.cleaned {
		return
	}
	sp.cleaned = true
	for bucket := range sp.batchSets {
		var rows int
		for _, set := range sp.batchSets[bucket] {
			if set == nil {
				continue
			}
			rows += set.RowCount()
			for i := 0; i < set.Length(); i++ {
				sp.forgetBatch(set.Get(i))
			}
			set.Clean(m)
		}
		if !sp.aborted && rows > 0 {
			logutil.Warnf("shuffle pool reset, batch %v rowcnt %v, maybe something wrong!", bucket, rows)
		}
	}
	sp.cleanBatchPool(m)
}

// Called at statement-account retirement, after every transport borrower has
// drained. All clones share this reservation and may call it independently.
func (sp *ShufflePool) releaseOwnedReservation() error {
	sp.holderLock.Lock()
	defer sp.holderLock.Unlock()
	if !sp.cleaned {
		return mpool.ErrAllocationAccountLive
	}
	if sp.ownedReservation != nil {
		sp.ownedReservation.Release()
		sp.ownedReservation = nil
	}
	return nil
}

// closeConsumer permanently closes one fixed-bucket destination for this
// generation. Producers skip subsequent rows for the bucket because its
// downstream pipeline has declared that it needs no more input.
func (sp *ShufflePool) closeConsumer(bucket int32, m *mpool.MPool) {
	if sp.drainAll || bucket < 0 || bucket >= sp.bucketNum {
		return
	}

	sp.batchLocks[bucket].Lock()
	if sp.closed[bucket] {
		sp.batchLocks[bucket].Unlock()
		return
	}
	sp.closed[bucket] = true
	ready := len(sp.readyProducers[bucket])
	for _, set := range sp.batchSets[bucket] {
		for i := 0; i < set.Length(); i++ {
			sp.forgetBatch(set.Get(i))
		}
		set.Clean(m)
	}
	for producer := range sp.batchSets[bucket] {
		rows := sp.producerBatchMaxRows
		if producer == max(1, int(sp.maxHolders)) {
			rows = objectio.BlockMaxRows
		}
		sp.batchSets[bucket][producer] = batch.NewBatchSet(rows)
	}
	sp.readyProducers[bucket] = nil
	sp.batchLocks[bucket].Unlock()

	if ready > 0 {
		sp.releaseReady(bucket, ready)
	}
	select {
	case sp.batchWaiters[bucket] <- true:
	default:
	}
	select {
	case sp.endingWaiters[bucket] <- true:
	default:
	}

	sp.holderLock.Lock()
	sp.consumers++
	if sp.consumers > sp.maxHolders {
		sp.holderLock.Unlock()
		panic("shuffle pool too many closed consumers!")
	}
	var cancels []context.CancelCauseFunc
	if sp.consumers == sp.maxHolders {
		cancels = make([]context.CancelCauseFunc, 0, len(sp.producers))
		for _, cancel := range sp.producers {
			cancels = append(cancels, cancel)
		}
	}
	sp.holderLock.Unlock()
	for _, cancel := range cancels {
		cancel(nil)
	}
}

func (sp *ShufflePool) cleanBatchPool(m *mpool.MPool) {
	for i := range sp.batchPools {
		shard := &sp.batchPools[i]
		shard.Lock()
		pool := shard.batches
		count := shard.count
		clear(shard.batches[:count])
		shard.count = 0
		shard.Unlock()
		for _, bat := range pool[:count] {
			sp.forgetBatch(bat)
			bat.Clean(m)
		}
	}
}

func (sp *ShufflePool) putBatchToPool(buf *batch.Batch, m *mpool.MPool) {
	producer := sp.batchProducer(buf)
	sp.syncBatch(buf, producer)
	shard := sp.batchPoolShard(producer)
	shard.Lock()
	if shard.count < len(shard.batches) {
		shard.batches[shard.count] = buf
		shard.count++
		shard.Unlock()
		return
	}
	shard.Unlock()
	sp.forgetBatch(buf)
	buf.Clean(m)
}

func (sp *ShufflePool) getBatchFromPool(producer int32) *batch.Batch {
	shard := sp.batchPoolShard(producer)
	shard.Lock()
	if shard.count == 0 {
		shard.Unlock()
		return nil
	}
	shard.count--
	buf := shard.batches[shard.count]
	shard.batches[shard.count] = nil
	shard.Unlock()
	return buf
}

func (sp *ShufflePool) batchPoolLength() int {
	length := 0
	for i := range sp.batchPools {
		shard := &sp.batchPools[i]
		shard.Lock()
		length += shard.count
		shard.Unlock()
	}
	return length
}

func (sp *ShufflePool) batchPoolShard(bucket int32) *shuffleBatchPoolShard {
	if bucket < 0 {
		bucket = 0
	}
	return &sp.batchPools[int(bucket)%len(sp.batchPools)]
}

func (sp *ShufflePool) discardBatch(buf *batch.Batch, m *mpool.MPool) {
	if buf == nil {
		return
	}
	sp.forgetBatch(buf)
	buf.Clean(m)
}

func (sp *ShufflePool) batchProducer(buf *batch.Batch) int32 {
	shard := sp.memoryShard(buf)
	shard.Lock()
	tracked, ok := shard.tracked[buf]
	shard.Unlock()
	if ok {
		return tracked.producer
	}
	// Test and defensive callers may insert a fresh batch directly. Preserve
	// the historical shard choice in that case.
	return buf.ShuffleIDX
}

func (sp *ShufflePool) syncBatch(buf *batch.Batch, producer int32) {
	shard := sp.memoryShard(buf)
	shard.Lock()
	allocated := int64(buf.Allocated())
	if shard.tracked == nil {
		shard.tracked = make(map[*batch.Batch]shuffleTrackedBatch)
	}
	previous := shard.tracked[buf]
	shard.tracked[buf] = shuffleTrackedBatch{
		allocated: allocated,
		producer:  producer,
	}
	shard.Unlock()
	sp.addMemory(allocated - previous.allocated)
}

func (sp *ShufflePool) syncBatchSetFrom(bs *batch.BatchSet, start int, producer int32) {
	for i := start; i < bs.Length(); i++ {
		sp.syncBatch(bs.Get(i), producer)
	}
}

func (sp *ShufflePool) forgetBatch(buf *batch.Batch) {
	shard := sp.memoryShard(buf)
	shard.Lock()
	tracked, ok := shard.tracked[buf]
	if ok {
		delete(shard.tracked, buf)
	}
	shard.Unlock()
	if ok {
		sp.current.Add(-tracked.allocated)
	}
}

func (sp *ShufflePool) memoryShard(buf *batch.Batch) *shuffleMemoryShard {
	idx := (uintptr(unsafe.Pointer(buf)) >> 6) & (shuffleMemoryShardCount - 1)
	return &sp.memoryShards[idx]
}

func (sp *ShufflePool) addMemory(delta int64) {
	if delta == 0 {
		return
	}
	current := sp.current.Add(delta)
	for {
		peak := sp.peak.Load()
		if current <= peak || sp.peak.CompareAndSwap(peak, current) {
			return
		}
	}
}

func (sp *ShufflePool) memoryPeak() int64 {
	return sp.peak.Load()
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
		if sp.bucketReadyCounts[bucket]+count > shuffleFixedBucketReadyLimit {
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
	if !sp.closed[shuffleIDX] {
		bat = sp.popReadyBatchLocked(shuffleIDX)
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
	bat := sp.popReadyBatchLocked(bucket)
	sp.batchLocks[bucket].Unlock()
	if bat == nil {
		panic("shuffle pool ready queue is inconsistent")
	}
	sp.releaseReady(bucket, 1)
	return bat
}

func (sp *ShufflePool) popReadyBatchLocked(bucket int32) *batch.Batch {
	ready := sp.readyProducers[bucket]
	if len(ready) == 0 {
		return nil
	}
	producer := ready[0]
	sp.readyProducers[bucket] = ready[1:]
	set := sp.batchSets[bucket][producer]
	if set.ReadyCount() == 0 {
		panic("shuffle pool producer ready queue is inconsistent")
	}
	bat := set.PopFront()
	bat.ShuffleIDX = bucket
	return bat
}

func (sp *ShufflePool) getLastBatch(shuffleIDX int32) *batch.Batch {
	sp.batchLocks[shuffleIDX].Lock()
	defer sp.batchLocks[shuffleIDX].Unlock()
	if sp.closed[shuffleIDX] {
		return nil
	}
	for _, set := range sp.batchSets[shuffleIDX] {
		if set.Length() == set.ReadyCount() {
			continue
		}
		bat := set.Pop()
		bat.ShuffleIDX = shuffleIDX
		return bat
	}
	return nil
}

func (sp *ShufflePool) getStoppedBatch(shuffleIDX int32) *batch.Batch {
	// The caller's earlier ready check can race with the final publication.
	// Once all producers stop, recheck ready batches before draining tails.
	if bat := sp.getFullBatch(shuffleIDX); bat != nil {
		return bat
	}
	return sp.getLastBatch(shuffleIDX)
}

func (sp *ShufflePool) getAnyStoppedBatch() *batch.Batch {
	// Claim the ready token normally; getAnyLastBatch must not steal a full
	// batch whose token another consumer has already claimed.
	if bat := sp.getAnyFullBatch(); bat != nil {
		return bat
	}
	return sp.getAnyLastBatch()
}

// getLastPartialBatch claims only a non-ready tail. Full batches remain owned
// by their ready queue tokens, including tokens already claimed by a consumer.
func (sp *ShufflePool) getLastPartialBatch(shuffleIDX, producer int32) *batch.Batch {
	sp.batchLocks[shuffleIDX].Lock()
	defer sp.batchLocks[shuffleIDX].Unlock()
	bs := sp.batchSets[shuffleIDX][producer]
	if bs.Length() == bs.ReadyCount() {
		return nil
	}
	bat := bs.Pop()
	bat.ShuffleIDX = shuffleIDX
	return bat
}

func (sp *ShufflePool) getAnyLastBatch() *batch.Batch {
	producerLanes := uint32(max(1, int(sp.maxHolders)))
	if sp.producerBatchMaxRows < objectio.BlockMaxRows {
		producerLanes++ // include the shared same-provenance tail
	}
	totalLanes := uint32(sp.bucketNum) * producerLanes
	for {
		idx := sp.finalCursor.Add(1) - 1
		if idx >= totalLanes {
			return nil
		}
		bucket := int32(idx / producerLanes)
		producer := int32(idx % producerLanes)
		if bat := sp.getLastPartialBatch(bucket, producer); bat != nil {
			return bat
		}
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
	producer int32,
	startBucket int,
	startOffset int,
	proc *process.Process,
) (nextBucket int, nextOffset int, wait <-chan struct{}, done bool, err error) {
	if producer < 0 || len(sp.batchSets) == 0 {
		return startBucket, startOffset, nil, false, moerr.NewInternalErrorf(
			proc.Ctx,
			"invalid shuffle producer %d for %d configured holders",
			producer,
			sp.maxHolders,
		)
	}
	producer %= int32(max(1, int(sp.maxHolders)))
	lane := producer
	maxRows := sp.producerBatchMaxRows
	if maxRows < objectio.BlockMaxRows && sp.matchesSharedLane(srcBatch) {
		lane = int32(max(1, int(sp.maxHolders)))
		maxRows = objectio.BlockMaxRows
	}
	for bucket := startBucket; bucket < len(sp.batchSets); bucket++ {
		offset := 0
		if bucket == startBucket {
			offset = startOffset
		}
		current := sels[bucket]
		for offset < len(current) {
			// BatchSet historically assumed its maximum was at least the input
			// batch size. Producer-local lanes deliberately use a smaller maximum
			// at high DOP, so split selections at that boundary before Union. This
			// keeps ReadyDeltaFor and the batches actually published in lockstep.
			end := min(offset+maxRows, len(current))
			chunk := current[offset:end]
			sp.batchLocks[bucket].Lock()
			if sp.closed[bucket] {
				sp.batchLocks[bucket].Unlock()
				break
			}
			batchSet := sp.batchSets[bucket][lane]
			readyDelta := batchSet.ReadyDeltaFor(srcBatch, len(chunk))
			wait, ok := sp.reserveReady(int32(bucket), readyDelta)
			if !ok {
				sp.batchLocks[bucket].Unlock()
				return bucket, offset, wait, false, nil
			}

			oldReady := batchSet.ReadyCount()
			oldLength := batchSet.Length()
			buf := sp.getBatchFromPool(producer)
			consumed, writeErr := batchSet.Union(proc.Mp(), srcBatch, chunk, buf)
			if !consumed && buf != nil {
				sp.putBatchToPool(buf, proc.Mp())
			}
			// Union can only grow the previous writable tail and append new
			// batches. Full batches before that tail are immutable, so avoid
			// rescanning the entire bucket after every chunk.
			sp.syncBatchSetFrom(batchSet, max(0, oldLength-1), producer)
			actualDelta := batchSet.ReadyCount() - oldReady
			if actualDelta < readyDelta {
				sp.releaseReady(int32(bucket), readyDelta-actualDelta)
			}
			for range actualDelta {
				sp.readyProducers[bucket] = append(
					sp.readyProducers[bucket], lane,
				)
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

// The first input selects one shared lane for this pool generation. Keep only
// immutable allocation selections, never source vectors or their payloads.
// Equality includes capacity classes: a producer's private spill recovery
// capacity must not be spent by another producer's writes.
func (sp *ShufflePool) matchesSharedLane(bat *batch.Batch) bool {
	if sp.outputAllocation != nil {
		return true
	}
	sp.sharedLaneOnce.Do(func() {
		sp.sharedBatchAccount = bat.AllocationAccountSelection()
		sp.sharedVecAccounts = make([]*vector.AllocationAccountSelection, len(bat.Vecs))
		for i, vec := range bat.Vecs {
			if vec != nil {
				sp.sharedVecAccounts[i] = vec.AllocationAccountSelection()
			}
		}
	})
	if len(sp.sharedVecAccounts) != len(bat.Vecs) ||
		!vector.AllocationAccountSelectionsEqual(sp.sharedBatchAccount, bat.AllocationAccountSelection()) {
		return false
	}
	for i, vec := range bat.Vecs {
		var selection *vector.AllocationAccountSelection
		if vec != nil {
			selection = vec.AllocationAccountSelection()
		}
		if !vector.AllocationAccountSelectionsEqual(sp.sharedVecAccounts[i], selection) {
			return false
		}
	}
	return true
}
