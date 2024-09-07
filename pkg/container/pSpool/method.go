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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/spool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"sync/atomic"
)

type PipelineCommunication interface {
	// SendBatch do copy for data, and send it to any or all data receiver.
	// after sent, data can be got by method ReceiveBatch.
	SendBatch(
		ctx context.Context,
		receiverID int,
		data *batch.Batch,
		info error) (queryDone bool, err error)

	// ReceiveBatch get data from the idx-th receiver.
	ReceiveBatch(idx int) (data *batch.Batch, info error)

	// ReleaseCurrent force to release the last received one.
	ReleaseCurrent(idx int)

	// Skip ignore next coming data from idx-th receiver.
	Skip(idx int)

	// Close the sender and receivers.
	Close()
}

// GeneratePipelineSpool make a pipeline spool for using.
// you only need to support how many receivers it should hold.
func GeneratePipelineSpool(mp *mpool.MPool, receiverCnt int) PipelineCommunication {
	bl := getBufferLength(receiverCnt)

	send, cursor := spool.New[pipelineSpoolMessage](int64(bl), receiverCnt)
	memoryCache := initCachedBatch(mp, bl)

	sp := &pipelineSpool{
		sp:           send,
		cs:           cursor,
		cache:        memoryCache,
		csDoneSignal: make(chan struct{}, receiverCnt),
	}
	return sp
}

// InitMyPipelineSpool return a simple pipeline spool for temporary plan.
//
// todo: use GeneratePipelineSpool after pipeline construct process is simple.
func InitMyPipelineSpool(mp *mpool.MPool, receiverCnt int) PipelineCommunication {
	bl := getBufferLength(receiverCnt)

	ps2 := &pipelineSpool2{
		shardPool:    make([]pipelineSpoolMessage, bl),
		shardRefs:    make([]atomic.Int32, bl),
		rs:           newReceivers(receiverCnt),
		cache:        initCachedBatch(mp, bl),
		csDoneSignal: make(chan struct{}, receiverCnt),
	}

	ps2.freeShardPool = make(chan int8, len(ps2.shardPool))
	for i := 0; i < len(ps2.shardPool); i++ {
		ps2.freeShardPool <- int8(i)
	}

	return ps2
}

func getBufferLength(cnt int) int {
	if cnt <= 4 {
		return 2
	}
	return (cnt + 1) / 2
}
