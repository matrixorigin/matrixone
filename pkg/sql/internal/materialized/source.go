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

package materialized

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// Source stores one producer's immutable batches so multiple dependent
// SINK_SCAN consumers can advance independently. Retained memory is charged to
// the query MPool and bounded before later batches spill to a query-scoped file.
// The last producer/reader release reclaims both forms of storage.
type Source struct {
	mu sync.Mutex

	notify chan struct{}
	mp     *mpool.MPool

	batches            []*batch.Batch
	bytes              int64
	memoryLimit        int64
	memoryBatchLimit   int
	spillFile          *os.File
	spillFactory       SpillFileFactory
	spillStartPosition int
	spillBatchCount    int
	spillBytes         int64
	spillReadOffsets   []int64
	spillReadPositions []int
	spillReadersActive []bool
	generation         uint64
	done               bool
	err                error
	active             bool

	readerReleased   []bool
	producerReleased bool
}

const sharedMaterializedSourceMaxBytes = int64(64 * mpool.MB)
const sharedMaterializedSourceMaxInMemoryBatches = 4096

const spillBatchHeaderSize = int64(8)

// SpillFileFactory creates an anonymous query-scoped file for overflow data.
type SpillFileFactory func(string) (*os.File, error)

// CTESinkOption marks a planner-approved bounded multi-consumer CTE source.
const CTESinkOption = "cte_reuse_materialized_sink"

func NewSource(readerCount int) *Source {
	return newSource(readerCount, sharedMaterializedSourceMaxBytes)
}

func newSource(readerCount int, memoryLimit int64) *Source {
	return &Source{
		notify:             make(chan struct{}),
		memoryLimit:        memoryLimit,
		memoryBatchLimit:   sharedMaterializedSourceMaxInMemoryBatches,
		readerReleased:     make([]bool, readerCount),
		spillReadOffsets:   make([]int64, readerCount),
		spillReadPositions: make([]int, readerCount),
		spillReadersActive: make([]bool, readerCount),
	}
}

// Begin starts one execution generation. It must run before scope goroutines
// start, including on prepared-statement reuse.
func (s *Source) Begin(mp *mpool.MPool, spillFactory ...SpillFileFactory) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active && !s.allReleasedLocked() {
		return moerr.NewInternalErrorNoCtx("materialized sink source reused before all owners released")
	}
	s.cleanLocked()
	s.generation++
	s.notify = make(chan struct{})
	s.mp = mp
	s.spillFactory = nil
	if len(spillFactory) > 0 {
		s.spillFactory = spillFactory[0]
	}
	s.done = false
	s.err = nil
	s.active = true
	s.producerReleased = false
	clear(s.readerReleased)
	clear(s.spillReadOffsets)
	clear(s.spillReadPositions)
	clear(s.spillReadersActive)
	return nil
}

func (s *Source) Append(bat *batch.Batch) error {
	if s == nil || bat == nil {
		return nil
	}
	reserved := int64(max(bat.Size(), bat.Allocated()))
	s.mu.Lock()
	if !s.active || s.done {
		s.mu.Unlock()
		return moerr.NewInternalErrorNoCtx("materialized sink source is not accepting data")
	}
	if s.spillFile != nil || reserved < 0 || reserved > s.memoryLimit || s.bytes > s.memoryLimit-reserved ||
		len(s.batches) >= s.memoryBatchLimit {
		err := s.appendSpilledLocked(bat)
		if err != nil {
			s.failLocked(err)
		}
		s.mu.Unlock()
		return err
	}
	s.bytes += reserved
	mp := s.mp
	generation := s.generation
	s.mu.Unlock()

	cloned, err := bat.Dup(mp)
	if err != nil {
		s.mu.Lock()
		if s.generation == generation && s.active {
			s.bytes -= reserved
			if !s.done {
				s.failLocked(err)
			}
			if s.err != nil {
				err = s.err
			}
		}
		s.mu.Unlock()
		return err
	}
	actual := int64(max(cloned.Size(), cloned.Allocated()))

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation != generation || !s.active || s.done {
		if s.generation == generation && s.active {
			s.bytes -= reserved
		}
		cloned.Clean(mp)
		if s.generation == generation && s.err != nil {
			return s.err
		}
		return moerr.NewInternalErrorNoCtx("materialized sink source stopped while copying data")
	}
	if actual > reserved && (actual-reserved > s.memoryLimit || s.bytes > s.memoryLimit-(actual-reserved)) {
		s.bytes -= reserved
		cloned.Clean(mp)
		err = s.appendSpilledLocked(bat)
		if err != nil {
			s.failLocked(err)
		}
		return err
	}
	s.bytes += actual - reserved
	s.batches = append(s.batches, cloned)
	s.wakeLocked()
	return nil
}

// Next returns a batch owned by the caller, or end=true after the producer
// finishes. Every successful caller must clean the returned batch exactly once.
func (s *Source) Next(ctx context.Context, readerID, position int) (bat *batch.Batch, end bool, err error) {
	if s == nil {
		return nil, true, moerr.NewInternalErrorNoCtx("nil materialized sink source")
	}
	for {
		s.mu.Lock()
		if readerID < 0 || readerID >= len(s.readerReleased) || s.readerReleased[readerID] {
			s.mu.Unlock()
			return nil, true, moerr.NewInternalErrorNoCtx("invalid materialized sink reader")
		}
		if position >= 0 && position < len(s.batches) {
			bat, err = s.batches[position].Dup(s.mp)
			s.mu.Unlock()
			return bat, false, err
		}
		if position >= s.spillStartPosition && position < s.spillStartPosition+s.spillBatchCount {
			if s.spillReadersActive[readerID] || position != s.spillStartPosition+s.spillReadPositions[readerID] {
				s.mu.Unlock()
				return nil, true, moerr.NewInternalErrorNoCtx("materialized sink reader position is not sequential")
			}
			s.spillReadersActive[readerID] = true
			file := s.spillFile
			offset := s.spillReadOffsets[readerID]
			availableBytes := s.spillBytes
			generation := s.generation
			mp := s.mp
			s.mu.Unlock()

			decoded, nextOffset, readErr := readSpilledBatch(file, offset, availableBytes, mp)

			s.mu.Lock()
			if s.generation == generation && readerID < len(s.spillReadersActive) {
				s.spillReadersActive[readerID] = false
			}
			if readErr != nil {
				s.mu.Unlock()
				return nil, true, readErr
			}
			if s.generation != generation || !s.active || s.readerReleased[readerID] {
				s.mu.Unlock()
				decoded.Clean(mp)
				return nil, true, moerr.NewInternalErrorNoCtx("materialized sink source stopped while reading spill data")
			}
			s.spillReadOffsets[readerID] = nextOffset
			s.spillReadPositions[readerID]++
			s.mu.Unlock()
			return decoded, false, nil
		}
		if s.done {
			err = s.err
			s.mu.Unlock()
			return nil, true, err
		}
		notify := s.notify
		s.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			return nil, true, context.Cause(ctx)
		}
	}
}

func (s *Source) appendSpilledLocked(bat *batch.Batch) error {
	if s.spillFactory == nil {
		return moerr.NewInternalErrorNoCtx("materialized sink source spill is unavailable")
	}
	if s.spillFile == nil {
		file, err := s.spillFactory(fmt.Sprintf("cte_materialized_%s", uuid.NewString()))
		if err != nil {
			if file != nil {
				_ = file.Close()
			}
			return err
		}
		if file == nil {
			return moerr.NewInternalErrorNoCtx("materialized sink spill file is nil")
		}
		s.spillFile = file
		s.spillStartPosition = len(s.batches)
	}
	data, err := bat.MarshalBinary()
	if err != nil {
		return err
	}
	var header [spillBatchHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(data)))
	if n, writeErr := s.spillFile.Write(header[:]); writeErr != nil {
		return writeErr
	} else if n != len(header) {
		return io.ErrShortWrite
	}
	if n, writeErr := s.spillFile.Write(data); writeErr != nil {
		return writeErr
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	s.spillBatchCount++
	s.spillBytes += spillBatchHeaderSize + int64(len(data))
	s.wakeLocked()
	return nil
}

func readSpilledBatch(file *os.File, offset, availableBytes int64, mp *mpool.MPool) (*batch.Batch, int64, error) {
	if file == nil || offset < 0 || offset > availableBytes-spillBatchHeaderSize {
		return nil, offset, moerr.NewInternalErrorNoCtx("invalid materialized sink spill offset")
	}
	var header [spillBatchHeaderSize]byte
	if _, err := file.ReadAt(header[:], offset); err != nil {
		return nil, offset, err
	}
	size := int64(binary.LittleEndian.Uint64(header[:]))
	if size < 0 || size > availableBytes-offset-spillBatchHeaderSize || size > int64(int(^uint(0)>>1)) {
		return nil, offset, moerr.NewInternalErrorNoCtx("invalid materialized sink spill batch size")
	}
	data := make([]byte, int(size))
	if _, err := file.ReadAt(data, offset+spillBatchHeaderSize); err != nil {
		return nil, offset, err
	}
	decoded := batch.NewWithSize(0)
	if err := decoded.UnmarshalBinaryWithAnyMp(data, mp); err != nil {
		decoded.Clean(mp)
		return nil, offset, err
	}
	return decoded, offset + spillBatchHeaderSize + size, nil
}

func (s *Source) CurrentBytes() int64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

// Finish publishes the producer's terminal state and releases its ownership.
func (s *Source) Finish(err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active {
		return
	}
	if !s.done {
		s.done = true
		s.err = err
		s.wakeLocked()
	}
	s.producerReleased = true
	s.tryCleanLocked()
}

func (s *Source) ReleaseReader(readerID int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if readerID < 0 || readerID >= len(s.readerReleased) || s.readerReleased[readerID] {
		return
	}
	s.readerReleased[readerID] = true
	s.tryCleanLocked()
}

// Close releases a source after all pipeline goroutines have stopped. It is a
// compile-owner safety net for failures that happen before operator Reset can
// release every producer and reader normally.
func (s *Source) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanLocked()
	s.generation++
	s.active = false
	s.done = true
	s.producerReleased = true
	for i := range s.readerReleased {
		s.readerReleased[i] = true
	}
	s.wakeLocked()
}

func (s *Source) wakeLocked() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *Source) failLocked(err error) {
	if s.done {
		return
	}
	s.done = true
	s.err = err
	s.wakeLocked()
}

func (s *Source) allReleasedLocked() bool {
	if !s.producerReleased {
		return false
	}
	for _, released := range s.readerReleased {
		if !released {
			return false
		}
	}
	return true
}

func (s *Source) tryCleanLocked() {
	if s.allReleasedLocked() {
		s.cleanLocked()
		s.active = false
	}
}

func (s *Source) cleanLocked() {
	if s.mp != nil {
		for _, bat := range s.batches {
			bat.Clean(s.mp)
		}
	}
	if s.spillFile != nil {
		_ = s.spillFile.Close()
		s.spillFile = nil
	}
	s.batches = nil
	s.bytes = 0
	s.spillStartPosition = 0
	s.spillBatchCount = 0
	s.spillBytes = 0
	clear(s.spillReadOffsets)
	clear(s.spillReadPositions)
	clear(s.spillReadersActive)
	s.mp = nil
	s.spillFactory = nil
}
