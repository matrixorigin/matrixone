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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"sync/atomic"
)

const (
	// SendToAllLocal and SendToAnyLocal
	// are Special receiver IDs for SendBatch method.
	SendToAllLocal = -1
	SendToAnyLocal = -2
)

type PipelineSpool struct {
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

	// once each cs done its work (after the readers get an End-Message from it, reader will put a value into this channel).
	//
	// the data producer should wait all consumers done before its close.
	csDoneSignal chan struct{}
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

	dst, useCache, cacheID, err := ps.cache.GetCopiedBatch(data)
	if err != nil {
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
	if last, hasLast := ps.rs[idx].getLastPop(); hasLast {
		if !ps.doRefCheck[last] || ps.shardRefs[last].Add(-1) == 0 {
			ps.cache.CacheBatch(
				ps.shardPool[last].useCache, ps.shardPool[last].cacheID, ps.shardPool[last].dataContent)
			ps.freeShardPool <- last
		}
		ps.rs[idx].flagLastPopRelease()
	}
}

// ReceiveBatch get data from the idx-th receiver.
func (ps *PipelineSpool) ReceiveBatch(idx int) (data *batch.Batch, info error) {
	ps.ReleaseCurrent(idx)

	next := ps.rs[idx].popNextIndex()
	if ps.shardPool[next].dataContent == nil {
		ps.csDoneSignal <- struct{}{}
	}
	return ps.shardPool[next].dataContent, ps.shardPool[next].errContent
}

// Close the sender and receivers, and do memory clean.
func (ps *PipelineSpool) Close() {
	// wait for all receivers done its work first.
	requireEndingReceiver := len(ps.rs)
	for requireEndingReceiver > 0 {
		requireEndingReceiver--

		<-ps.csDoneSignal
	}

	ps.cache.free()
}

func (ps *PipelineSpool) getFreeIdFromSharedPool(
	ctx context.Context) (queryDone bool, id uint32) {
	select {
	case <-ctx.Done():
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
