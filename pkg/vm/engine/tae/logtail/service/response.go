// Copyright 2021 Matrix Origin
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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
)

// LogtailResponse wraps logtail.LogtailResponse.
type LogtailResponse struct {
	logtail.LogtailResponse
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

func (r *LogtailResponse) Size() int {
	return r.ProtoSize()
}

// ResponsePool acquires or releases LogtailResponse.
type ResponsePool interface {
	// Acquire fetches item from pool.
	Acquire() *LogtailResponse

	// Release puts item back to pool.
	Release(*LogtailResponse)
}

type responsePool struct {
	pool *sync.Pool
}

func NewResponsePool() ResponsePool {
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

// SegmentPool acquires or releases LogtailResponseSegment.
type SegmentPool interface {
	// Acquire fetches item from pool.
	Acquire() *LogtailResponseSegment

	// Release puts item back to pool.
	Release(*LogtailResponseSegment)

	// LeastEffectiveCapacity evaluates least payload limit.
	LeastEffectiveCapacity() int
}

type segmentPool struct {
	maxMessageSize int
	pool           *sync.Pool
}

func NewSegmentPool(maxMessageSize int) SegmentPool {
	s := &segmentPool{
		maxMessageSize: maxMessageSize,
		pool: &sync.Pool{
			New: func() any {
				seg := &LogtailResponseSegment{}
				seg.Payload = make([]byte, maxMessageSize)
				return seg
			},
		},
	}
	return s
}

func (s *segmentPool) Acquire() *LogtailResponseSegment {
	return s.pool.Get().(*LogtailResponseSegment)
}

func (s *segmentPool) Release(seg *LogtailResponseSegment) {
	buf := seg.Payload
	seg.Reset()
	seg.Payload = buf[:cap(buf)]
	s.pool.Put(seg)
}

func (s *segmentPool) LeastEffectiveCapacity() int {
	segment := s.Acquire()
	defer s.Release(segment)

	segment.StreamID = math.MaxUint64
	segment.Sequence = math.MaxInt32
	segment.MaxSequence = math.MaxInt32
	segment.MessageSize = math.MaxInt32
	// Now, maxHeaderSize is 32 bytes.
	maxHeaderSize := segment.ProtoSize() - s.maxMessageSize

	// Take out reserved size, then effective capacity left.
	return s.maxMessageSize - maxHeaderSize
}
