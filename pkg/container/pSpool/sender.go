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
	"time"

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

	// cache owns every batch copied into the spool and reuses released ones.
	cache *batchCache
	// free element index on shardPool.
	freeShardPool chan uint32

	// once each cs done its work (after the readers get an End-Message from it, reader will put a value into this channel).
	//
	// the data producer should wait all consumers done before its close.
	csDoneSignal chan struct{}

	// drainedReceivers counts how many receivers have already reached their
	// End-message (i.e. how many csDoneSignal values have been consumed by the
	// close path). It is only touched from the single owner goroutine that
	// runs Close / CloseWithTimeout, never concurrently.
	drainedReceivers int

	abortOnce sync.Once
	abortDone chan struct{}
	aborted   bool
	abortErr  error
}

// pipelineSpoolMessage is the element of PipelineSpool.
type pipelineSpoolMessage struct {
	// data or error information.
	dataContent *batch.Batch
	errContent  error

	// owned reports whether dataContent is a spool-owned copy that goes back
	// to the cache once its last receiver releases it; aliased reports whether
	// several of its columns share one vector.
	owned   bool
	aliased bool
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

	dst, owned, aliased, err := ps.cache.copyBatch(data)
	if err != nil {
		ps.freeShardPool <- messageIdx
		return false, err
	}
	ps.updateSpoolMessage(messageIdx, dst, info, owned, aliased)

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

	if data == nil {
		defer func() {
			ps.csDoneSignal <- struct{}{}
		}()
	}
	return data, info
}

// Close waits until every receiver has reached its End-message, then frees the
// cached batches.
func (ps *PipelineSpool) Close() {
	ps.CloseWithTimeout(0)
}

func (ps *PipelineSpool) CloseWithTimeout(timeout time.Duration) bool {
	timer := (*time.Timer)(nil)
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
	}

	// wait for all receivers done its work first.
	for ps.drainedReceivers < len(ps.rs) {
		if timer == nil {
			select {
			case <-ps.csDoneSignal:
				ps.drainedReceivers++
			case <-ps.abortDone:
				return true
			}
			continue
		}

		select {
		case <-ps.csDoneSignal:
			ps.drainedReceivers++
		case <-ps.abortDone:
			return true
		case <-timer.C:
			return false
		}
	}

	ps.cache.free()
	return true
}

// ReleaseCache frees the batches receivers have already released. It is safe
// at any time: batches released afterwards are freed directly. Call it once the
// producer is done, e.g. after a typed EventEnd has been delivered.
func (ps *PipelineSpool) ReleaseCache() {
	if ps == nil {
		return
	}
	ps.cache.free()
}

// FinalizeAfterConsumersQuiesced releases every batch still retained by the
// transport. The caller must have joined all producer and consumer scopes, so
// no goroutine can subsequently read a queued slot or use a current batch.
//
// A typed terminal signal bypasses the spool queue. Consequently a receiver
// may stop with older GetFromSpool signals still queued even though its scope
// has completed. Those slots are no longer observable and must be reclaimed
// here before the statement allocation account can reach terminal zero.
func (ps *PipelineSpool) FinalizeAfterConsumersQuiesced() {
	if ps == nil {
		return
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	for i := range ps.shardPool {
		ps.cleanSlotLocked(uint32(i))
	}
	for i := range ps.rs {
		ps.rs[i].flagLastPopRelease()
	}
	ps.cache.free()
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

func (ps *PipelineSpool) getFreeIdFromSharedPool(
	ctx context.Context) (queryDone bool, id uint32) {
	select {
	case <-ctx.Done():
		return true, 0
	case <-ps.abortDone:
		return true, 0
	case id = <-ps.freeShardPool:
		return false, id
	}
}

func (ps *PipelineSpool) updateSpoolMessage(idx uint32, data *batch.Batch, err error, owned, aliased bool) {
	ps.shardPool[idx].dataContent = data
	ps.shardPool[idx].errContent = err
	ps.shardPool[idx].owned = owned
	ps.shardPool[idx].aliased = aliased
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

func (ps *PipelineSpool) releaseCurrentLocked(idx int) {
	if last, hasLast := ps.rs[idx].getLastPop(); hasLast {
		if ps.aborted {
			if !ps.doRefCheck[last] || ps.shardRefs[last].Add(-1) == 0 {
				ps.cleanSlotLocked(last)
			}
		} else if !ps.doRefCheck[last] || ps.shardRefs[last].Add(-1) == 0 {
			msg := ps.shardPool[last]
			ps.clearSlotLocked(last)
			if msg.owned && msg.dataContent != nil {
				ps.cache.put(msg.dataContent, msg.aliased)
			}
			ps.freeShardPool <- last
		}
		ps.rs[idx].flagLastPopRelease()
	}
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
	// and clears the slot so the batch cannot be released twice. Current
	// batches still held by receivers are released later by
	// releaseCurrentLocked when their last receiver drops the ref.
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
	ps.cache.free()
}

func (ps *PipelineSpool) cleanSlotLocked(idx uint32) {
	msg := &ps.shardPool[idx]
	if msg.owned && msg.dataContent != nil {
		msg.dataContent.Clean(ps.cache.mp)
	}
	ps.clearSlotLocked(idx)
}

func (ps *PipelineSpool) clearSlotLocked(idx uint32) {
	ps.shardPool[idx] = pipelineSpoolMessage{}
	ps.doRefCheck[idx] = false
}
