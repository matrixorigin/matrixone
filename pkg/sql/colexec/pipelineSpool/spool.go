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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/spool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"sync"
)

const (
	// SendToAllLocal and SendToAnyLocal
	// are Special receiver IDs for SendBatch method.
	SendToAllLocal = -1
	SendToAnyLocal = -2
)

// MessageSpool is a single-producer and multiple-consumer queue.
type MessageSpool spool.Spool[BatchMessage]

// PipelineBatchSenderToLocal sends batch messages to the local receiver.
type PipelineBatchSenderToLocal struct {
	sp *spool.Spool[BatchMessage]
	cs []*spool.Cursor[BatchMessage]

	// memory buffer to cache the free-batch.
	cache *cachedBatch

	// if only one user to use this sender, usrLock will be nil.
	usrLock *sync.Mutex
	// usrCount is the routine-number which used this sender.
	// if aliveUsr is not zero, we cannot reset or close this sender.
	usrCount int32
	aliveUsr int32

	// receiver done check.
	// csDoneSignal used to check if all cs has done.
	// each cs should send a done signal to this channel once done.
	csDoneSignal chan struct{}
}

type PipelineBatchReceiverFromLocal struct {
	sd  *PipelineBatchSenderToLocal
	cs  *spool.Cursor[BatchMessage]
	end bool
}

// SendBatch copy batch and send it to the local receiver.
// this function will block until get enough space to cache the batch.
// or return true if the caller pipeline is done.
func (s *PipelineBatchSenderToLocal) SendBatch(ctx context.Context, receiverID int, bat *batch.Batch, errInfo error) (queryDone bool, err error) {
	var dst *batch.Batch
	dst, queryDone, err = s.cache.GetCopiedBatch(ctx, bat)
	if err != nil || queryDone {
		return queryDone, err
	}
	if dst == nil {
		return false, moerr.NewInternalError(ctx, "cannot use SendBatch to send nil batch.")
	}

	msg := BatchMessage{
		msgCtx:         ctx,
		content:        dst,
		err:            errInfo,
		releaseToCache: s.cache.CacheBatch,
	}

	s.doSend(receiverID, msg)
	return false, nil
}

func (s *PipelineBatchSenderToLocal) doSend(receiverID int, msg BatchMessage) {
	if receiverID == SendToAllLocal {
		s.sp.Send(msg)
		return
	}
	if receiverID == SendToAnyLocal {
		s.sp.SendAny(msg)
		return
	}
	s.sp.SendTo(s.cs[receiverID], msg)
}

func (s *PipelineBatchSenderToLocal) tryToStopSender() bool {
	if s.usrLock != nil {
		s.usrLock.Lock()
		defer s.usrLock.Unlock()
	}
	s.aliveUsr--
	if s.aliveUsr > 0 {
		return false
	}
	s.doSend(SendToAllLocal, BatchMessage{
		msgCtx:         context.TODO(),
		content:        nil,
		err:            nil,
		releaseToCache: s.cache.CacheBatch,
	})
	s.aliveUsr = s.usrCount
	return true
}

func (s *PipelineBatchSenderToLocal) ReceiverCount() int {
	return len(s.cs)
}

// waitingReceiverDone blocking until all cursors were done.
func (s *PipelineBatchSenderToLocal) waitingReceiverDone() {
	n := len(s.cs)
	for n > 0 {
		n--
		<-s.csDoneSignal
	}
}

func (s *PipelineBatchSenderToLocal) notifyReceiverDone() {
	s.csDoneSignal <- struct{}{}
}

func (s *PipelineBatchSenderToLocal) Reset() {
	if done := s.tryToStopSender(); !done {
		return
	}
	s.waitingReceiverDone()
	s.cache.Reset()
}

func (s *PipelineBatchSenderToLocal) Close() {
	if done := s.tryToStopSender(); !done {
		return
	}
	s.waitingReceiverDone()
	s.sp.Close()
	s.cache.Free()
}

func (s *PipelineBatchReceiverFromLocal) ReceiveBatch() (bat *batch.Batch, err error) {
	for {
		msg, isOpen := s.cs.Next()
		if !isOpen {
			s.end = true
			return nil, moerr.NewInternalErrorNoCtx("pipeline batch sender is closed.")
		}
		if msg.isOutDateMessage() {
			continue
		}
		if msg.isEndMessage() {
			s.end = true
		}
		return msg.content, msg.err
	}
}

func (s *PipelineBatchReceiverFromLocal) GetSender() *spool.Spool[BatchMessage] {
	return s.sd.sp
}

// doClean will keep receiving data until it was an END message.
func (s *PipelineBatchReceiverFromLocal) doClean() {
	for !s.end {
		_, err := s.ReceiveBatch()
		if err != nil {
			return
		}
	}
}

func (s *PipelineBatchReceiverFromLocal) Reset() {
	s.doClean()
	s.sd.notifyReceiverDone()
}

func (s *PipelineBatchReceiverFromLocal) Close() {
	s.doClean()
	s.sd.notifyReceiverDone()
	s.cs.Close()
}

// BatchMessage to implement the spool.Element interface.
type BatchMessage struct {
	msgCtx  context.Context
	content *batch.Batch
	err     error

	// method to free content.
	releaseToCache func(*batch.Batch)
}

func (m BatchMessage) SizeInSpool() int64 {
	if m.content == nil || m.content.IsEmpty() {
		return 0
	}
	return 1
}
func (m BatchMessage) SpoolFree() {
	if m.releaseToCache != nil {
		m.releaseToCache(m.content)
	}
	m.content = nil
	m.err = nil
}
func (m BatchMessage) isEndMessage() bool {
	return m.content == nil
}
func (m BatchMessage) isOutDateMessage() bool {
	return m.msgCtx.Err() != nil
}
