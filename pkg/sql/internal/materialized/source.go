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
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// Source stores one producer's immutable batches so multiple
// dependent SINK_SCAN consumers can advance independently. Storage is charged
// to the query MPool; the last producer/reader release reclaims every batch.
type Source struct {
	mu sync.Mutex

	notify chan struct{}
	mp     *mpool.MPool

	batches    []storedBatch
	bytes      int64
	generation uint64
	done       bool
	err        error
	active     bool

	readerReleased   []bool
	readerBatches    []*batch.Batch
	producerReleased bool
	spillFile        *os.File
	spillFactory     SpillFileFactory
}

type storedBatch struct {
	batch  *batch.Batch
	offset int64
	size   int64
}

// SpillFileFactory creates an anonymous query-scoped file for overflow data.
type SpillFileFactory func(context.Context, string) (*os.File, error)

const sharedMaterializedSourceMaxBytes = int64(64 * mpool.MB)

// CTESinkOption marks a planner-approved bounded multi-consumer CTE source.
const CTESinkOption = "cte_reuse_materialized_sink"

func NewSource(readerCount int) *Source {
	return &Source{
		notify:         make(chan struct{}),
		readerReleased: make([]bool, readerCount),
		readerBatches:  make([]*batch.Batch, readerCount),
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
	if reserved < 0 || s.bytes > sharedMaterializedSourceMaxBytes-reserved {
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
	if actual > reserved && s.bytes > sharedMaterializedSourceMaxBytes-(actual-reserved) {
		s.bytes -= reserved
		cloned.Clean(mp)
		err = s.appendSpilledLocked(bat)
		if err != nil {
			s.failLocked(err)
		}
		return err
	}
	s.bytes += actual - reserved
	s.batches = append(s.batches, storedBatch{batch: cloned})
	s.wakeLocked()
	return nil
}

// Next returns one immutable batch, or end=true after the producer finishes.
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
		s.cleanReaderBatchLocked(readerID)
		if position < len(s.batches) {
			stored := s.batches[position]
			if stored.batch != nil {
				bat = stored.batch
				s.mu.Unlock()
				return bat, false, nil
			}
			file := s.spillFile
			generation := s.generation
			mp := s.mp
			s.mu.Unlock()

			if cause := context.Cause(ctx); cause != nil {
				return nil, true, cause
			}
			data := make([]byte, stored.size)
			if _, err = file.ReadAt(data, stored.offset); err != nil {
				return nil, true, err
			}
			if cause := context.Cause(ctx); cause != nil {
				return nil, true, cause
			}
			decoded := batch.NewWithSize(0)
			if err = decoded.UnmarshalBinaryWithAnyMp(data, mp); err != nil {
				decoded.Clean(mp)
				return nil, true, err
			}

			s.mu.Lock()
			if s.generation != generation || !s.active || s.readerReleased[readerID] {
				s.mu.Unlock()
				decoded.Clean(mp)
				return nil, true, moerr.NewInternalErrorNoCtx("materialized sink source stopped while reading spill data")
			}
			s.readerBatches[readerID] = decoded
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
		file, err := s.spillFactory(context.Background(), fmt.Sprintf("cte_materialized_%s", uuid.NewString()))
		if err != nil {
			return err
		}
		s.spillFile = file
	}
	data, err := bat.MarshalBinary()
	if err != nil {
		return err
	}
	offset, err := s.spillFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	written, err := s.spillFile.Write(data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	s.batches = append(s.batches, storedBatch{offset: offset, size: int64(len(data))})
	s.wakeLocked()
	return nil
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
	s.cleanReaderBatchLocked(readerID)
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
		for _, stored := range s.batches {
			if stored.batch != nil {
				stored.batch.Clean(s.mp)
			}
		}
		for i := range s.readerBatches {
			s.cleanReaderBatchLocked(i)
		}
	}
	if s.spillFile != nil {
		_ = s.spillFile.Close()
		s.spillFile = nil
	}
	s.batches = nil
	s.bytes = 0
	s.mp = nil
	s.spillFactory = nil
}

func (s *Source) cleanReaderBatchLocked(readerID int) {
	if s.mp != nil && s.readerBatches[readerID] != nil {
		s.readerBatches[readerID].Clean(s.mp)
		s.readerBatches[readerID] = nil
	}
}
