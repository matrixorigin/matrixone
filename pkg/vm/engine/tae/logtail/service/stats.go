// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"fmt"
	"sync/atomic"
)

// serverStatistics maintains statistics for logtail server.
type serverStatistics struct {
	counter struct {
		subscriptionRequests   uint64
		unsubscriptionRequests uint64
	}
}

func newServerStatistics() *serverStatistics {
	return &serverStatistics{}
}

func (s *serverStatistics) Stats() string {
	return fmt.Sprintf(
		"SubscriptionRequest=%d, UnsubscriptionRequest=%d",
		s.subscriptionRequests(), s.unsubscriptionRequests(),
	)
}

func (s *serverStatistics) subscriptionRequestInc() {
	atomic.AddUint64(&s.counter.subscriptionRequests, 1)
}

func (s *serverStatistics) subscriptionRequests() uint64 {
	return atomic.LoadUint64(&s.counter.subscriptionRequests)
}

func (s *serverStatistics) unsubscriptionRequestInc() {
	atomic.AddUint64(&s.counter.unsubscriptionRequests, 1)
}

func (s *serverStatistics) unsubscriptionRequests() uint64 {
	return atomic.LoadUint64(&s.counter.unsubscriptionRequests)
}

// sessionStatistics maintains statistics for session.
type sessionStatistics struct {
	counter struct {
		subscriptionResponse   uint64
		unsubscriptionResponse uint64
		updateResponse         uint64
		errorResponse          uint64
	}
	gauge struct {
		buffer int64
		table  int32
	}
}

func newSessionStatistics() *sessionStatistics {
	return &sessionStatistics{}
}

func (s *sessionStatistics) Stats() string {
	return fmt.Sprintf(
		"SubscriptionResponse=%d, UnsubscriptionResponse=%d, ErrorResponse=%d, UpdateResponse=%d, ResponseToBeSent=%d, Table=%d",
		s.subscriptionResponses(), s.unsubscriptionResponses(), s.errorResponses(), s.updateResponses(), s.buffers(), s.tables(),
	)
}

func (s *sessionStatistics) bufferGaugeInc() {
	atomic.AddInt64(&s.gauge.buffer, 1)
}

func (s *sessionStatistics) bufferGaugeDec() {
	atomic.AddInt64(&s.gauge.buffer, -1)
}

func (s *sessionStatistics) buffers() int64 {
	return atomic.LoadInt64(&s.gauge.buffer)
}

func (s *sessionStatistics) tableGaugeInc() {
	atomic.AddInt32(&s.gauge.table, 1)
}

func (s *sessionStatistics) tableGaugeDec() {
	atomic.AddInt32(&s.gauge.table, -1)
}

func (s *sessionStatistics) tables() int32 {
	return atomic.LoadInt32(&s.gauge.table)
}

func (s *sessionStatistics) subscriptionResponseInc() {
	atomic.AddUint64(&s.counter.subscriptionResponse, 1)
}

func (s *sessionStatistics) subscriptionResponses() uint64 {
	return atomic.LoadUint64(&s.counter.subscriptionResponse)
}

func (s *sessionStatistics) unsubscriptionResponseInc() {
	atomic.AddUint64(&s.counter.unsubscriptionResponse, 1)
}

func (s *sessionStatistics) unsubscriptionResponses() uint64 {
	return atomic.LoadUint64(&s.counter.unsubscriptionResponse)
}

func (s *sessionStatistics) updateResponseInc() {
	atomic.AddUint64(&s.counter.updateResponse, 1)
}

func (s *sessionStatistics) updateResponses() uint64 {
	return atomic.LoadUint64(&s.counter.updateResponse)
}

func (s *sessionStatistics) errorResponseInc() {
	atomic.AddUint64(&s.counter.errorResponse, 1)
}

func (s *sessionStatistics) errorResponses() uint64 {
	return atomic.LoadUint64(&s.counter.errorResponse)
}
