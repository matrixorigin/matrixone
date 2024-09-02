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
	"github.com/matrixorigin/matrixone/pkg/common/spool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

const (
	// SendToAllLocal and SendToAnyLocal
	// are Special receiver IDs for SendBatch method.
	SendToAllLocal = -1
	SendToAnyLocal = -2
)

// pipelineSpool is the shared memories between producers and consumers.
// this structure supports
type pipelineSpool struct {
	sp *spool.Spool[pipelineSpoolMessage]
	cs []*spool.Cursor[pipelineSpoolMessage]

	// memory buffer for reusing.
	cache *cachedBatch

	// each cs done its work (after the readers get an End-Message from it, reader will put a value into this channel).
	// and the data producer should wait all consumers done before its close or reset.
	csDoneSignal chan struct{}
}

func (ps *pipelineSpool) SendBatch(
	ctx context.Context,
	receiverID int,
	data *batch.Batch, info error) (queryDone bool, err error) {

	if receiverID == SendToAnyLocal {
		panic("do not support SendToAnyLocal for pipeline spool now.")
	}

	var dst *batch.Batch
	dst, queryDone, err = ps.cache.GetCopiedBatch(ctx, data)
	if err != nil || queryDone {
		return queryDone, err
	}

	msg := pipelineSpoolMessage{
		msgCtx:  ctx,
		content: dst,
		err:     info,
		src:     ps.cache,
	}

	if receiverID == SendToAllLocal {
		ps.sp.Send(msg)
	} else {
		ps.sp.SendTo(ps.cs[receiverID], msg)
	}
	return false, nil
}

func (ps *pipelineSpool) ReceiveBatch(idx int) (*batch.Batch, error) {
	b, done := ps.cs[idx].Next()
	if !done || b.content == nil {
		ps.csDoneSignal <- struct{}{}
		return nil, b.err
	}
	return b.content, b.err
}

func (ps *pipelineSpool) waitReceiversOver() {
	require := len(ps.cs)
	for i := 0; i < require; i++ {
		<-ps.csDoneSignal
	}
}

func (ps *pipelineSpool) Close() {
	ps.waitReceiversOver()
	ps.cache.Free()
}

var _ spool.Element = pipelineSpoolMessage{}

// pipelineSpoolMessage is the element of pipelineSpool.
type pipelineSpoolMessage struct {
	msgCtx context.Context

	content *batch.Batch
	err     error

	src *cachedBatch
}

func (p pipelineSpoolMessage) SizeInSpool() int64 {
	if p.content == nil || p.content == batch.EmptyBatch {
		return 0
	}
	return 1
}
func (p pipelineSpoolMessage) SpoolFree() {
	if p.content == nil || p.content == batch.EmptyBatch {
		return
	}
	p.src.CacheBatch(p.content)
}
