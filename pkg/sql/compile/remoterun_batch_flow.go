// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
)

const (
	pipelineBatchCreditCount = uint32(8)
	pipelineBatchCreditBytes = uint64(64 << 20)
)

type pipelineBatchFlow struct {
	sendMu sync.Mutex
	mu     sync.Mutex

	maxCount uint32
	maxBytes uint64
	nextSeq  uint64
	ackedSeq uint64
	bytes    uint64
	pending  map[uint64]uint64
	changed  chan struct{}
}

func newPipelineBatchFlow(requestedCount uint32, requestedBytes uint64) *pipelineBatchFlow {
	if requestedCount == 0 || requestedBytes == 0 {
		return nil
	}
	return &pipelineBatchFlow{
		maxCount: min(requestedCount, pipelineBatchCreditCount),
		maxBytes: min(requestedBytes, pipelineBatchCreditBytes),
		pending:  make(map[uint64]uint64),
		changed:  make(chan struct{}),
	}
}

func (f *pipelineBatchFlow) accepted() (uint32, uint64) {
	if f == nil {
		return 0, 0
	}
	return f.maxCount, f.maxBytes
}

func (f *pipelineBatchFlow) notifyLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *pipelineBatchFlow) reserve(
	messageCtx context.Context,
	connectionCtx context.Context,
	size uint64,
) (uint64, error) {
	if f == nil {
		return 0, nil
	}
	for {
		f.mu.Lock()
		countAvailable := uint32(len(f.pending)) < f.maxCount
		bytesAvailable := f.bytes+size <= f.maxBytes || len(f.pending) == 0
		if countAvailable && bytesAvailable {
			f.nextSeq++
			seq := f.nextSeq
			f.pending[seq] = size
			f.bytes += size
			f.mu.Unlock()
			return seq, nil
		}
		changed := f.changed
		f.mu.Unlock()

		select {
		case <-changed:
		case <-messageCtx.Done():
			return 0, moerr.AttachCause(messageCtx, messageCtx.Err())
		case <-connectionCtx.Done():
			return 0, moerr.NewStreamClosed(messageCtx)
		}
	}
}

func (f *pipelineBatchFlow) rollback(seq uint64) {
	if f == nil || seq == 0 {
		return
	}
	f.mu.Lock()
	if size, ok := f.pending[seq]; ok {
		delete(f.pending, seq)
		f.bytes -= size
		f.notifyLocked()
	}
	f.mu.Unlock()
}

func (f *pipelineBatchFlow) acknowledge(seq uint64) error {
	if f == nil || seq == 0 {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if seq <= f.ackedSeq {
		return nil
	}
	if seq > f.nextSeq {
		return moerr.NewInvalidStateNoCtxf(
			"pipeline batch ACK %d is ahead of sent sequence %d", seq, f.nextSeq)
	}
	for current := f.ackedSeq + 1; current <= seq; current++ {
		if size, ok := f.pending[current]; ok {
			delete(f.pending, current)
			f.bytes -= size
		}
	}
	f.ackedSeq = seq
	f.notifyLocked()
	return nil
}

func (f *pipelineBatchFlow) waitUntilDrained(
	messageCtx context.Context,
	connectionCtx context.Context,
	onDelayed func(count int, bytes uint64),
) error {
	if f == nil {
		return nil
	}
	timer := time.NewTimer(pipelineStreamFinishTimeout)
	defer timer.Stop()
	delayC := timer.C
	for {
		f.mu.Lock()
		if len(f.pending) == 0 {
			f.mu.Unlock()
			return nil
		}
		count, bytes := len(f.pending), f.bytes
		changed := f.changed
		f.mu.Unlock()

		select {
		case <-changed:
		case <-messageCtx.Done():
			return moerr.AttachCause(messageCtx, messageCtx.Err())
		case <-connectionCtx.Done():
			return moerr.NewStreamClosed(messageCtx)
		case <-delayC:
			delayC = nil
			if onDelayed != nil {
				onDelayed(count, bytes)
			}
		}
	}
}

func handlePipelineBatchAck(message *pipeline.Message, cs morpc.ClientSession) error {
	key := pipelineStreamLifecycleKey{session: cs, id: message.GetID()}
	value, ok := pipelineStreamLifecycles.Load(key)
	if !ok {
		// Cancellation can remove the lifecycle while an already flushed ACK is
		// still being decoded. The connection/query teardown owns that race.
		return nil
	}
	lifecycle := value.(*pipelineStreamLifecycle)
	if lifecycle.batchFlow == nil {
		_ = cs.Close()
		return moerr.NewInvalidStateNoCtx("pipeline batch ACK was not negotiated")
	}
	if err := lifecycle.batchFlow.acknowledge(message.GetBatchAckSequence()); err != nil {
		_ = cs.Close()
		return err
	}
	return nil
}
