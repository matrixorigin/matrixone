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
)

type pipelineSpool2 struct {
	cache *cachedBatch

	// each cs done its work (after the readers get an End-Message from it, reader will put a value into this channel).
	// and the data producer should wait all consumers done before its close or reset.
	csDoneSignal chan struct{}
}

func (ps *pipelineSpool2) SendBatch(
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
		src:     ps.cache,
	}

	if receiverID == SendToAllLocal {
		ps.sendToAll(msg)
	} else {
		ps.sendToIdx(receiverID, msg)
	}
	return false, nil
}

func (ps *pipelineSpool2) ReceiveBatch(idx int) (data *batch.Batch, info error) {
	// get batch from global part.
	if idx == SendToAllLocal {
		return nil, nil
	}

	// get batch from specific part.
	return nil, nil
}

func (ps *pipelineSpool2) Skip(idx int) {
	// this function shouldn't do anything.
}

func (ps *pipelineSpool2) Close() {
	return
}

func (ps *pipelineSpool2) sendToAll(msg pipelineSpoolMessage) {
	return
}

func (ps *pipelineSpool2) sendToIdx(idx int, msg pipelineSpoolMessage) {
	return
}
