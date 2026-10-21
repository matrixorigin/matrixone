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

package pSpool

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

const (
	// SendToAllLocal and SendToAnyLocal
	// are Special receiver IDs for SendBatch method.
	SendToAllLocal = -1
	SendToAnyLocal = -2
)

var ErrPipelineSpoolAborted = moerr.NewInternalErrorNoCtx("pipeline spool aborted")

type PipelineSpool struct {
	mu sync.RWMutex

	shardPool  []pipelineSpoolMessage
	shardRefs  []atomic.Int32
	doRefCheck []bool

	// rs record all receivers' input data queue.
	// it supports push-index and pop-index methods.
	rs []receiver

	// cache manage all the reuse memories.
	cache *cachedBatch
	// free element index on shardPool.
	freeShardPool chan uint32

	abortOnce sync.Once
	abortDone chan struct{}
	aborted   bool
	abortErr  error

	cleanupOnce sync.Once
	// cleanupDone means the cache-wide cleanup pass has already run. A typed
	// terminal receiver can still release its current batch afterward; that late
	// release must free directly instead of putting memory back into a cache that
	// will not be cleaned again.
	cleanupDone bool
}

// pipelineSpoolMessage is the element of PipelineSpool.
type pipelineSpoolMessage struct {
	// data or error information.
	dataContent *batch.Batch
	errContent  error

	// use cache or not,
	// and which cache pool does dataContent's memory allocated from.
	// we should put this memory back after the use of dataContent.
	useCache bool
	cacheID  uint32
}

// SendBatch do copy for data, and send it to any or all data receiver.
// after sent, data can be got by method ReceiveBatch.
func (ps *PipelineSpool) SendBatch(
	ctx context.Context, receiverID int, data *batch.Batch, info error) (bool, error) {

	if receiverID == SendToAnyLocal {
		panic("do not support SendToAnyLocal for pipeline spool now.")
	}

	done, messageIdx := ps.getFreeIdFromSharedPool(ctx)
	if done {
		return true, nil
	}

	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if ps.aborted {
		return true, ps.abortErr
	}

	dst, useCache, cacheID, err := ps.cache.GetCopiedBatch(data)
	if err != nil {
		ps.freeShardPool <- messageIdx
		return false, err
	}
	ps.updateSpoolMessage(messageIdx, dst, info, useCache, cacheID)

	if receiverID == SendToAllLocal {
		ps.sendToAll(messageIdx)
	} else {
		ps.sendToIdx(messageIdx, receiverID)
	}
	return false, nil
}

// ReleaseCurrent force to release the last received one.
func (ps *PipelineSpool) ReleaseCurrent(idx int) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	ps.releaseCurrentLocked(idx)
}

// ReceiveBatch get data from the idx-th receiver.
func (ps *PipelineSpool) ReceiveBatch(idx int) (data *batch.Batch, info error) {
	ps.mu.RLock()
	ps.releaseCurrentLocked(idx)
	if ps.aborted {
		info = ps.abortErr
		ps.mu.RUnlock()
		return nil, info
	}

	next := ps.rs[idx].popNextIndex()
	data = ps.shardPool[next].dataContent
	info = ps.shardPool[next].errContent
	ps.mu.RUnlock()

	return data, info
}

// ForceCleanupAfterTerminalSignal reclaims reusable cache memory after the
// producer has stopped sending and published its typed terminal outside the
// spool. It never waits for a receiver acknowledgement.
//
// Downstream consumers may still own current or pending batches: neither is in
// the reusable cache, and both must remain readable. cleanupDone makes their
// eventual release free directly. Quiescent edge drain at reuse/scope release
// discards notifications left behind by a receiver that timed out during cleanup.
func (ps *PipelineSpool) ForceCleanupAfterTerminalSignal() {
	if ps == nil {
		return
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.aborted {
		return
	}

	ps.cleanupOnce.Do(ps.forceCleanup)
}

// Abort terminates the spool without waiting for receiver acknowledgement.
// Pending, not-yet-consumed slots are released immediately. Slots already handed
// to receivers stay valid until their receiver calls ReleaseCurrent. The first
// abort cause is returned to receivers whose queued spool signals race with the
// terminal signal. A nil cause falls back to ErrPipelineSpoolAborted.
func (ps *PipelineSpool) Abort(cause error) {
	if ps == nil {
		return
	}
	if cause == nil {
		cause = ErrPipelineSpoolAborted
	}

	ps.abortOnce.Do(func() {
		ps.mu.Lock()
		defer ps.mu.Unlock()

		ps.aborted = true
		ps.abortErr = cause
		close(ps.abortDone)
		ps.abortLocked()
	})
}

func (ps *PipelineSpool) forceCleanup() {
	// Typed-terminal sender
	// cleanup can precede slower consumers: free only cache-owned memory, and
	// make current/pending batches free directly on their eventual release.
	ps.cache.free()
	ps.cleanupDone = true
}

func (ps *PipelineSpool) getFreeIdFromSharedPool(
	ctx context.Context) (queryDone bool, id uint32) {
	if ctx.Err() != nil {
		return true, 0
	}
	// A free slot is the normal case. Avoid a multi-way select here; SendBatch
	// still checks Abort under the spool lock before touching slot ownership.
	select {
	case id = <-ps.freeShardPool:
		return false, id
	default:
	}
	select {
	case <-ctx.Done():
		return true, 0
	case <-ps.abortDone:
		return true, 0
	case id = <-ps.freeShardPool:
		return false, id
	}
}

func (ps *PipelineSpool) updateSpoolMessage(idx uint32, data *batch.Batch, err error, useCache bool, cacheID uint32) {
	ps.shardPool[idx].dataContent = data
	ps.shardPool[idx].errContent = err

	ps.shardPool[idx].useCache = useCache
	ps.shardPool[idx].cacheID = cacheID
}

func (ps *PipelineSpool) sendToAll(sharedPoolIndex uint32) {
	if ps.shardRefs == nil {
		ps.doRefCheck[sharedPoolIndex] = false
	} else {
		ps.shardRefs[sharedPoolIndex].Store(int32(len(ps.rs)))
		ps.doRefCheck[sharedPoolIndex] = true
	}

	for i := 0; i < len(ps.rs); i++ {
		ps.rs[i].pushNextIndex(sharedPoolIndex)
	}
}

func (ps *PipelineSpool) sendToIdx(sharedPoolIndex uint32, idx int) {
	// if send to only one, there is no need to do ref check.
	ps.doRefCheck[sharedPoolIndex] = false

	ps.rs[idx].pushNextIndex(sharedPoolIndex)
}

// DiscardUnsent returns the reference reserved by the immediately preceding
// SendBatch when its receiver notification was rejected. The single producer
// must call it exactly once per unnotified receiver after SendBatch returned
// (false, nil), before another SendBatch or cleanup. idx must be a real receiver
// index addressed by that send, never a broadcast sentinel.
// Already notified entries and receiver-held batches retain their ownership.
func (ps *PipelineSpool) DiscardUnsent(idx int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.aborted {
		return // Abort already reclaimed every pending reference.
	}
	ps.releaseSlotLocked(ps.rs[idx].discardLastIndex())
}

func (ps *PipelineSpool) releaseCurrentLocked(idx int) {
	if last, hasLast := ps.rs[idx].getLastPop(); hasLast {
		ps.releaseSlotLocked(last)
		ps.rs[idx].flagLastPopRelease()
	}
}

func (ps *PipelineSpool) releaseSlotLocked(idx uint32) {
	if ps.doRefCheck[idx] && ps.shardRefs[idx].Add(-1) != 0 {
		return
	}
	if ps.aborted || ps.cleanupDone {
		ps.cleanSlotLocked(idx)
		return
	}
	// Detach before either pool can reuse the batch or slot independently.
	msg := &ps.shardPool[idx]
	data, useCache, cacheID := msg.dataContent, msg.useCache, msg.cacheID
	ps.clearSlotLocked(idx)
	ps.cache.CacheBatch(useCache, cacheID, data)
	ps.freeShardPool <- idx
}

func (ps *PipelineSpool) abortLocked() {
	if ps.shardRefs == nil {
		ps.shardRefs = make([]atomic.Int32, len(ps.shardPool))
	}

	// Abort has two cleanup classes:
	//   - slots already handed to receivers are kept until ReleaseCurrent drops
	//     their ref count;
	//   - all other slots can be cleaned immediately.
	//
	// cleanSlotLocked releases per-batch vector memory for each non-current slot
	// and clears the slot so the batch cannot be released twice. cache.free only
	// cleans byte slices that have already been returned to the spool cache; it
	// does not own current batches still held by receivers. Those are released
	// later by releaseCurrentLocked when their last receiver drops the ref.
	currentRefs := make([]int32, len(ps.shardPool))
	for i := range ps.rs {
		if last, hasLast := ps.rs[i].getLastPop(); hasLast {
			currentRefs[last]++
		}
	}

	for i := range ps.shardPool {
		if currentRefs[i] > 0 {
			ps.doRefCheck[i] = true
			ps.shardRefs[i].Store(currentRefs[i])
			continue
		}
		ps.cleanSlotLocked(uint32(i))
	}

	ps.cleanupOnce.Do(ps.forceCleanup)
}

func (ps *PipelineSpool) cleanSlotLocked(idx uint32) {
	msg := &ps.shardPool[idx]
	if msg.useCache && msg.dataContent != nil {
		msg.dataContent.Clean(ps.cache.mp)
	}
	ps.clearSlotLocked(idx)
}

func (ps *PipelineSpool) clearSlotLocked(idx uint32) {
	ps.shardPool[idx] = pipelineSpoolMessage{}
	ps.doRefCheck[idx] = false
}
