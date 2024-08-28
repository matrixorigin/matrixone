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

package pipelineSpool

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/spool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"sync"
)

// This document defines the interfaces and methods exposed by the pipelineSpool package.
// In principle,
// the external users (send operators (connector, dispatch), receive operators (merge)),
// have no need to know how the batch spool is specifically implemented.

type SenderToLocalPipeline interface {
	SendBatch(ctx context.Context, receiverID int, bat *batch.Batch, errInfo error) (queryDone bool, err error)
	ReceiverCount() int
	Reset()
	Close()
}

type ReceiverFromLocalPipeline interface {
	ReceiveBatch() (bat *batch.Batch, err error)
	GetSender() *spool.Spool[BatchMessage]
	Reset()
	Close()
}

// NewSenderToLocalPipeline creates a new sender and its multiple receivers.
func NewSenderToLocalPipeline(
	mp *mpool.MPool, senderUsr int32, receiverCount int32, sendBufferCap int) (SenderToLocalPipeline, []ReceiverFromLocalPipeline) {
	s, cs := spool.New[BatchMessage](int64(sendBufferCap), int(receiverCount))

	memoryCache := initCachedBatch(mp, sendBufferCap)
	sender := &PipelineBatchSenderToLocal{
		sp:           s,
		cs:           cs,
		cache:        memoryCache,
		csDoneSignal: make(chan struct{}, len(cs)),

		usrCount: senderUsr,
		aliveUsr: senderUsr,
	}

	if sender.usrCount > 1 {
		sender.usrLock = &sync.Mutex{}
	}

	receiver := make([]ReceiverFromLocalPipeline, receiverCount)
	for i := int32(0); i < receiverCount; i++ {
		receiver[i] = &PipelineBatchReceiverFromLocal{
			sd: sender,
			cs: cs[i],
		}
	}

	return sender, receiver
}
