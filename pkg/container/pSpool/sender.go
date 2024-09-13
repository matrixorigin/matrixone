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
	shardPool []pipelineSpoolMessage
	shardRefs []atomic.Int32

	rs []receiver

	cache *cachedBatch
	// free element index on shardPool.
	freeShardPool chan int8

	// each cs done its work (after the readers get an End-Message from it, reader will put a value into this channel).
	// and the data producer should wait all consumers done before its close or reset.
	csDoneSignal chan struct{}
}

// pipelineSpoolMessage is the element of PipelineSpool.
type pipelineSpoolMessage struct {
	content *batch.Batch
	err     error
}

// SendBatch do copy for data, and send it to any or all data receiver.
// after sent, data can be got by method ReceiveBatch.
func (ps *PipelineSpool) SendBatch(
	ctx context.Context, receiverID int, data *batch.Batch, info error) (queryDone bool, err error) {

	if receiverID == SendToAnyLocal {
		panic("do not support SendToAnyLocal for pipeline spool now.")
	}

	var dst *batch.Batch
	dst, queryDone, err = ps.cache.GetCopiedBatch(ctx, data)
	if err != nil || queryDone {
		return queryDone, err
	}

	msg := pipelineSpoolMessage{
		content: dst,
		err:     info,
	}

	if receiverID == SendToAllLocal {
		queryDone = ps.sendToAll(ctx, msg)
	} else {
		queryDone = ps.sendToIdx(ctx, receiverID, msg)
	}

	if queryDone {
		ps.cache.CacheBatch(dst)
	}
	return queryDone, nil
}

// ReleaseCurrent force to release the last received one.
func (ps *PipelineSpool) ReleaseCurrent(idx int) {
	if last := ps.rs[idx].getLastPop(); last != noneLastPop {
		if ps.shardRefs[last].Add(-1) == 0 {
			ps.cache.CacheBatch(ps.shardPool[last].content)
			ps.freeShardPool <- last
		}
		ps.rs[idx].lastPop = noneLastPop
	}
}

// ReceiveBatch get data from the idx-th receiver.
func (ps *PipelineSpool) ReceiveBatch(idx int) (data *batch.Batch, info error) {
	ps.ReleaseCurrent(idx)

	next := ps.rs[idx].popNextIndex()
	if ps.shardPool[next].content == nil {
		ps.csDoneSignal <- struct{}{}
	}
	return ps.shardPool[next].content, ps.shardPool[next].err
}

// Close the sender and receivers, and do memory clean.
func (ps *PipelineSpool) Close() {
	// wait for all receivers done its work first.
	requireEndingReceiver := len(ps.rs)
	for requireEndingReceiver > 0 {
		requireEndingReceiver--

		<-ps.csDoneSignal
	}

	ps.cache.Free()
}

func (ps *PipelineSpool) sendToAll(ctx context.Context, msg pipelineSpoolMessage) (queryDone bool) {
	select {
	case <-ctx.Done():
		return true
	case index := <-ps.freeShardPool:
		ps.shardPool[index] = msg
		ps.shardRefs[index].Store(int32(len(ps.rs)))

		for i := 0; i < len(ps.rs); i++ {
			ps.rs[i].pushNextIndex(index)
		}
	}
	return false
}

func (ps *PipelineSpool) sendToIdx(ctx context.Context, idx int, msg pipelineSpoolMessage) (queryDone bool) {
	select {
	case <-ctx.Done():
		return true
	case index := <-ps.freeShardPool:
		ps.shardPool[index] = msg
		ps.shardRefs[index].Store(1)

		ps.rs[idx].pushNextIndex(index)
	}
	return false
}
