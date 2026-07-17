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
	"math"
	"math/bits"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
)

const maxRPCMessageSize = 100 * 1024 * 1024

// ValidateRPCMaxMessageSize verifies that response segments have room for a
// payload and cannot request an unsafe allocation.
func ValidateRPCMaxMessageSize(maxMessageSize int64) error {
	if maxMessageSize <= 0 {
		return moerr.NewBadConfigNoCtxf(
			"logtail rpc max message size must be positive, got %d", maxMessageSize)
	}
	if maxMessageSize > maxRPCMessageSize {
		return moerr.NewBadConfigNoCtxf(
			"logtail rpc max message size %d exceeds the supported limit %d",
			maxMessageSize, maxRPCMessageSize)
	}
	if leastEffectiveCapacity(int(maxMessageSize)) <= 0 {
		return moerr.NewBadConfigNoCtxf(
			"logtail rpc max message size %d is too small", maxMessageSize)
	}
	return nil
}

func leastEffectiveCapacity(maxMessageSize int) int {
	segment := LogtailResponseSegment{}
	segment.StreamID = math.MaxUint64
	segment.Sequence = math.MaxInt32
	segment.MaxSequence = math.MaxInt32
	segment.MessageSize = math.MaxInt32
	// Payload contributes one field tag and the encoded length. Compute the
	// worst-case header without allocating a maxMessageSize payload.
	headerSize := segment.ProtoSize() + 1 + (bits.Len64(uint64(maxMessageSize)|1)+6)/7
	return maxMessageSize - headerSize
}

// LogtailPhase is the logtail information of one phase of
// subscription request. Phase 1 is executed asynchronously
// to collect most of logtails of the subscription request.
// And phase 2 is executed sync.
type LogtailPhase struct {
	tail    logtail.TableLogtail
	closeCB func()
	sub     subscription
}

// LogtailResponse wraps logtail.LogtailResponse.
type LogtailResponse struct {
	logtail.LogtailResponse
	closeCB func()
}

var _ morpc.Message = (*LogtailResponse)(nil)

func (r *LogtailResponse) SetID(id uint64) {
	r.ResponseId = id
}

func (r *LogtailResponse) GetID() uint64 {
	return r.ResponseId
}

func (r *LogtailResponse) DebugString() string {
	return ""
}

// LogtailResponsePool acquires or releases LogtailResponse.
type LogtailResponsePool interface {
	// Acquire fetches item from pool.
	Acquire() *LogtailResponse

	// Release puts item back to pool.
	Release(*LogtailResponse)
}

type responsePool struct {
	pool *sync.Pool
}

func NewLogtailResponsePool() LogtailResponsePool {
	return &responsePool{
		pool: &sync.Pool{
			New: func() any {
				return &LogtailResponse{}
			},
		},
	}
}

func (p *responsePool) Acquire() *LogtailResponse {
	return p.pool.Get().(*LogtailResponse)
}

func (p *responsePool) Release(resp *LogtailResponse) {
	if resp.closeCB != nil {
		resp.closeCB()
		resp.closeCB = nil
	}
	resp.Reset()
	p.pool.Put(resp)
}

// LogtailResponseSegment wrps logtail.MessageSegment.
type LogtailResponseSegment struct {
	logtail.MessageSegment
}

var _ morpc.Message = (*LogtailResponseSegment)(nil)

func (s *LogtailResponseSegment) SetID(id uint64) {
	s.StreamID = id
}

func (s *LogtailResponseSegment) GetID() uint64 {
	return s.StreamID
}

func (s *LogtailResponseSegment) DebugString() string {
	return fmt.Sprintf(
		"LogtailResponseSegment: StreamID=%d, MessageSize=%d, Sequence=%d, MaxSequence=%d",
		s.GetStreamID(),
		s.GetMessageSize(),
		s.GetSequence(),
		s.GetMaxSequence(),
	)
}

func (s *LogtailResponseSegment) Size() int {
	return s.ProtoSize()
}

// LogtailResponseSegmentPool acquires or releases LogtailResponseSegment.
type LogtailResponseSegmentPool interface {
	// Acquire fetches item from pool.
	Acquire() *LogtailResponseSegment

	// Release puts item back to pool.
	Release(*LogtailResponseSegment)
}

// LogtailServerSegmentPool describes segment pool for logtail server.
type LogtailServerSegmentPool interface {
	LogtailResponseSegmentPool

	// LeastEffectiveCapacity evaluates least effective payload size.
	LeastEffectiveCapacity() int
}

type serverSegmentPool struct {
	maxMessageSize int
	pool           *sync.Pool
}

func NewLogtailServerSegmentPool(maxMessageSize int) LogtailServerSegmentPool {
	return &serverSegmentPool{
		maxMessageSize: maxMessageSize,
		pool: &sync.Pool{
			New: func() any {
				seg := &LogtailResponseSegment{}
				seg.Payload = make([]byte, maxMessageSize)
				return seg
			},
		},
	}
}

func (p *serverSegmentPool) Acquire() *LogtailResponseSegment {
	return p.pool.Get().(*LogtailResponseSegment)
}

func (p *serverSegmentPool) Release(seg *LogtailResponseSegment) {
	buf := seg.Payload
	seg.Reset()
	seg.Payload = buf[:cap(buf)]
	p.pool.Put(seg)
}

func (p *serverSegmentPool) LeastEffectiveCapacity() int {
	return leastEffectiveCapacity(p.maxMessageSize)
}
