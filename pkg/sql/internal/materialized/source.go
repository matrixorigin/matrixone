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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// Source stores one producer's immutable batches so multiple dependent
// SINK_SCAN consumers can advance independently. Retained memory is charged to
// the query MPool and bounded before later batches spill to a query-scoped file.
// The last producer/reader release reclaims both forms of storage.
type Source struct {
	mu sync.Mutex

	notify chan struct{}
	mp     *mpool.MPool

	batches            []storedBatch
	bytes              int64
	memoryLimit        int64
	memoryBatchLimit   int
	spillFile          *os.File
	spillConfig        SpillConfig
	spillDisk          GrowingReservation
	spillFD            Reservation
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

	// Immutable execution dependencies, configured before Begin. Completed
	// inputs can share the existing source facility with streaming CTE readers.
	completionSources []*Source
}

const sharedMaterializedSourceMaxBytes = int64(64 * mpool.MB)
const sharedMaterializedSourceMaxInMemoryBatches = 4096

const spillBatchHeaderSize = int64(8)

// SpillFileFactory creates an anonymous query-scoped file for overflow data.
type SpillFileFactory func(context.Context, string) (*os.File, error)

// Reservation owns one query-scoped resource charge.
type Reservation interface {
	Release() bool
}

// GrowingReservation owns a charge that can grow without allocating
// per-batch bookkeeping objects.
type GrowingReservation interface {
	Reservation
	Grow(uint64) error
}

// SpillBudget admits transient Go-heap buffers, spill bytes and spill file
// descriptors against one statement/CN-scoped budget generation.
type SpillBudget struct {
	ReserveMemory func(uint64) (Reservation, error)
	ReserveDisk   func(uint64) (GrowingReservation, error)
	ReserveFD     func(uint64) (Reservation, error)
}

// SpillConfig supplies the query-scoped spill file and admission controls.
// A source fails closed if spilling is required without a complete config.
type SpillConfig struct {
	FileFactory SpillFileFactory
	Budget      SpillBudget
	// Writer fences copy into the current attempt, independently of the
	// producer's allocation selection. Streaming CTE sources leave this nil.
	Allocation *vector.AllocationAccountSelection
}

type storedBatch struct {
	batch    *batch.Batch
	metadata Reservation
}

func (b storedBatch) clean(mp *mpool.MPool) {
	b.batch.Clean(mp)
	if b.metadata != nil {
		b.metadata.Release()
	}
}

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
func (s *Source) Begin(mp *mpool.MPool, spillConfig ...SpillConfig) error {
	if s == nil {
		return nil
	}
	if len(spillConfig) > 0 && spillConfig[0].Allocation != nil &&
		spillConfig[0].Budget.ReserveMemory == nil {
		return mpool.ErrAllocationAccountInvalid
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
	s.spillConfig = SpillConfig{}
	if len(spillConfig) > 0 {
		s.spillConfig = spillConfig[0]
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

// RequireCompletionOf is configured by the compile owner before producer scopes
// start. Each dependency publishes success/error through its existing Finish.
func (s *Source) RequireCompletionOf(sources []*Source) { s.completionSources = sources }

func (s *Source) waitForFinish(ctx context.Context) error {
	s.mu.Lock()
	generation := s.generation
	s.mu.Unlock()
	for {
		s.mu.Lock()
		if s.generation != generation {
			s.mu.Unlock()
			return moerr.NewInternalErrorNoCtx("materialized producer generation ended before its consumer")
		}
		if s.done {
			err := s.err
			s.mu.Unlock()
			return err
		}
		if !s.active {
			s.mu.Unlock()
			return moerr.NewInternalErrorNoCtx("materialized producer is inactive")
		}
		notify := s.notify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

// Append checks the producer pipeline's context around owned copying and
// publication. That context can end before the compile owner's context.
func (s *Source) Append(ctx context.Context, bat *batch.Batch) error {
	if s == nil || bat == nil {
		return nil
	}
	reserved := int64(max(bat.Size(), bat.Allocated()))
	s.mu.Lock()
	if !s.active || s.done {
		err := s.err
		if err == nil {
			err = moerr.NewInternalErrorNoCtx("materialized sink source is not accepting data")
		}
		s.mu.Unlock()
		return err
	}
	if err := context.Cause(ctx); err != nil {
		s.failLocked(err)
		s.mu.Unlock()
		return err
	}
	if s.spillFile != nil || reserved < 0 || reserved > s.memoryLimit || s.bytes > s.memoryLimit-reserved ||
		len(s.batches) >= s.memoryBatchLimit {
		err := s.appendSpilledLocked(ctx, bat)
		if err != nil {
			s.failLocked(err)
		}
		s.mu.Unlock()
		return err
	}
	s.bytes += reserved
	mp := s.mp
	generation := s.generation
	config := s.spillConfig
	s.mu.Unlock()

	cloned, metadata, err := copyBatch(ctx, bat, mp, config)
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
		storedBatch{cloned, metadata}.clean(mp)
		if s.generation == generation && s.err != nil {
			return s.err
		}
		return moerr.NewInternalErrorNoCtx("materialized sink source stopped while copying data")
	}
	if err := context.Cause(ctx); err != nil {
		s.bytes -= reserved
		storedBatch{cloned, metadata}.clean(mp)
		s.failLocked(err)
		return err
	}
	if actual > reserved && (actual-reserved > s.memoryLimit || s.bytes > s.memoryLimit-(actual-reserved)) {
		s.bytes -= reserved
		storedBatch{cloned, metadata}.clean(mp)
		err = s.appendSpilledLocked(ctx, bat)
		if err != nil {
			s.failLocked(err)
		}
		return err
	}
	s.bytes += actual - reserved
	s.batches = append(s.batches, storedBatch{cloned, metadata})
	s.wakeLocked()
	return nil
}

// Next returns a batch owned by the caller, or end=true after the producer
// finishes. The caller owns the batch and any returned reservation: clean
// the batch before releasing its reservation, including on reset or reuse.
func (s *Source) Next(ctx context.Context, readerID, position int) (bat *batch.Batch, reservation Reservation, end bool, err error) {
	if s == nil {
		return nil, nil, true, moerr.NewInternalErrorNoCtx("nil materialized sink source")
	}
	for _, source := range s.completionSources {
		if err := source.waitForFinish(ctx); err != nil {
			return nil, nil, true, err
		}
	}
	for {
		s.mu.Lock()
		if readerID < 0 || readerID >= len(s.readerReleased) || s.readerReleased[readerID] {
			s.mu.Unlock()
			return nil, nil, true, moerr.NewInternalErrorNoCtx("invalid materialized sink reader")
		}
		if position >= 0 && position < len(s.batches) {
			bat, reservation, err = copyBatch(ctx, s.batches[position].batch, s.mp, s.spillConfig)
			s.mu.Unlock()
			return bat, reservation, false, err
		}
		if position >= s.spillStartPosition && position < s.spillStartPosition+s.spillBatchCount {
			if s.spillReadersActive[readerID] || position != s.spillStartPosition+s.spillReadPositions[readerID] {
				s.mu.Unlock()
				return nil, nil, true, moerr.NewInternalErrorNoCtx("materialized sink reader position is not sequential")
			}
			s.spillReadersActive[readerID] = true
			file := s.spillFile
			offset := s.spillReadOffsets[readerID]
			availableBytes := s.spillBytes
			generation := s.generation
			mp := s.mp
			budget := s.spillConfig.Budget
			s.mu.Unlock()

			decoded, reservation, nextOffset, readErr := readSpilledBatch(file, offset, availableBytes, mp, budget)

			s.mu.Lock()
			if s.generation == generation && readerID < len(s.spillReadersActive) {
				s.spillReadersActive[readerID] = false
			}
			if readErr != nil {
				s.mu.Unlock()
				return nil, nil, true, readErr
			}
			if s.generation != generation || !s.active || s.readerReleased[readerID] {
				s.mu.Unlock()
				decoded.Clean(mp)
				reservation.Release()
				return nil, nil, true, moerr.NewInternalErrorNoCtx("materialized sink source stopped while reading spill data")
			}
			s.spillReadOffsets[readerID] = nextOffset
			s.spillReadPositions[readerID]++
			s.mu.Unlock()
			return decoded, reservation, false, nil
		}
		if s.done {
			err = s.err
			s.mu.Unlock()
			return nil, nil, true, err
		}
		notify := s.notify
		s.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			return nil, nil, true, context.Cause(ctx)
		}
	}
}

func (s *Source) appendSpilledLocked(ctx context.Context, bat *batch.Batch) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if s.spillConfig.FileFactory == nil || s.spillConfig.Budget.ReserveMemory == nil ||
		s.spillConfig.Budget.ReserveDisk == nil || s.spillConfig.Budget.ReserveFD == nil {
		return moerr.NewInternalErrorNoCtx("materialized sink source spill is unavailable")
	}
	encodedSize, err := bat.MarshalBinarySize()
	if err != nil {
		return err
	}
	serializedBytes := uint64(encodedSize)
	recordBytes := uint64(spillBatchHeaderSize) + serializedBytes
	if recordBytes > math.MaxInt64 || s.spillBytes > math.MaxInt64-int64(recordBytes) || s.spillBatchCount == math.MaxInt {
		return moerr.NewInternalErrorNoCtx("materialized sink spill position overflow")
	}
	scratch, err := bat.MarshalBinaryScratchSize()
	if err != nil {
		return err
	}
	if uint64(scratch) > math.MaxUint64-serializedBytes {
		return moerr.NewInternalErrorNoCtx("materialized sink spill memory size overflow")
	}
	memoryReservation, err := s.spillConfig.Budget.ReserveMemory(serializedBytes + uint64(scratch))
	if err != nil {
		return err
	}
	if memoryReservation == nil {
		return moerr.NewInternalErrorNoCtx("materialized sink spill memory reservation is nil")
	}
	defer memoryReservation.Release()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	buf := bytes.NewBuffer(make([]byte, 0, encodedSize))
	if err := bat.MarshalBinaryTo(buf); err != nil {
		return err
	}
	data := buf.Bytes()
	if uint64(len(data)) != serializedBytes {
		return moerr.NewInternalErrorNoCtxf("materialized sink spill batch size changed while serializing: expected=%d actual=%d", serializedBytes, len(data))
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	newFile := s.spillFile == nil
	if newFile {
		s.spillDisk, err = s.spillConfig.Budget.ReserveDisk(recordBytes)
	} else {
		err = s.spillDisk.Grow(recordBytes)
	}
	if err != nil {
		return err
	}
	if s.spillDisk == nil {
		return moerr.NewInternalErrorNoCtx("materialized sink spill disk reservation is nil")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if newFile {
		s.spillFD, err = s.spillConfig.Budget.ReserveFD(1)
		if err != nil {
			s.spillDisk.Release()
			s.spillDisk = nil
			return err
		}
		if s.spillFD == nil {
			s.spillDisk.Release()
			s.spillDisk = nil
			return moerr.NewInternalErrorNoCtx("materialized sink spill file reservation is nil")
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		file, fileErr := s.spillConfig.FileFactory(ctx, fmt.Sprintf("cte_materialized_%s", uuid.NewString()))
		if fileErr != nil || file == nil {
			if file != nil {
				_ = file.Close()
			}
			s.spillFD.Release()
			s.spillFD = nil
			s.spillDisk.Release()
			s.spillDisk = nil
			if fileErr != nil {
				return fileErr
			}
			return moerr.NewInternalErrorNoCtx("materialized sink spill file is nil")
		}
		s.spillFile = file
		s.spillStartPosition = len(s.batches)
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	var header [spillBatchHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(data)))
	if n, writeErr := s.spillFile.Write(header[:]); writeErr != nil {
		return writeErr
	} else if n != len(header) {
		return io.ErrShortWrite
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if n, writeErr := s.spillFile.Write(data); writeErr != nil {
		return writeErr
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	s.spillBatchCount++
	s.spillBytes += spillBatchHeaderSize + int64(len(data))
	s.wakeLocked()
	return nil
}

// A spill reader owns both its aliased frame and separately decoded metadata.
// The consumer releases them together, after cleaning the returned batch.
type spillReadReservation struct{ frame, metadata Reservation }

func (r *spillReadReservation) Release() bool {
	released := false
	if r.metadata != nil {
		released = r.metadata.Release() || released
		r.metadata = nil
	}
	if r.frame != nil {
		released = r.frame.Release() || released
		r.frame = nil
	}
	return released
}

func readSpilledBatch(file *os.File, offset, availableBytes int64, mp *mpool.MPool, budget SpillBudget) (*batch.Batch, Reservation, int64, error) {
	if file == nil || availableBytes < spillBatchHeaderSize || offset < 0 || offset > availableBytes-spillBatchHeaderSize {
		return nil, nil, offset, moerr.NewInternalErrorNoCtx("invalid materialized sink spill offset")
	}
	var header [spillBatchHeaderSize]byte
	if _, err := file.ReadAt(header[:], offset); err != nil {
		return nil, nil, offset, err
	}
	size := int64(binary.LittleEndian.Uint64(header[:]))
	if size < 0 || size > availableBytes-offset-spillBatchHeaderSize || size > int64(int(^uint(0)>>1)) {
		return nil, nil, offset, moerr.NewInternalErrorNoCtx("invalid materialized sink spill batch size")
	}
	info, err := file.Stat()
	if err != nil {
		return nil, nil, offset, err
	}
	if info.Size() < offset+spillBatchHeaderSize || size > info.Size()-offset-spillBatchHeaderSize {
		return nil, nil, offset, io.ErrUnexpectedEOF
	}
	if budget.ReserveMemory == nil {
		return nil, nil, offset, moerr.NewInternalErrorNoCtx("materialized sink spill read budget is unavailable")
	}
	memoryReservation, err := budget.ReserveMemory(uint64(size))
	if err != nil {
		return nil, nil, offset, err
	}
	if memoryReservation == nil {
		return nil, nil, offset, moerr.NewInternalErrorNoCtx("materialized sink spill read reservation is nil")
	}
	reservation := &spillReadReservation{frame: memoryReservation}
	transferred := false
	defer func() {
		if !transferred {
			reservation.Release()
		}
	}()
	data := make([]byte, int(size))
	if _, err := file.ReadAt(data, offset+spillBatchHeaderSize); err != nil {
		return nil, nil, offset, err
	}
	metadataBytes, err := batch.UnmarshalBinaryMetadataSize(data)
	if err != nil {
		return nil, nil, offset, err
	}
	reservation.metadata, err = budget.ReserveMemory(metadataBytes)
	if err != nil {
		return nil, nil, offset, err
	}
	if reservation.metadata == nil {
		return nil, nil, offset, moerr.NewInternalErrorNoCtx("materialized sink spill metadata reservation is nil")
	}
	decoded := batch.NewWithSize(0)
	if err := decoded.UnmarshalBinaryWithAnyMp(data, mp); err != nil {
		decoded.Clean(mp)
		return nil, nil, offset, err
	}
	transferred = true
	return decoded, reservation, offset + spillBatchHeaderSize + size, nil
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
			bat.clean(s.mp)
		}
	}
	if s.spillFile != nil {
		_ = s.spillFile.Close()
		s.spillFile = nil
	}
	if s.spillFD != nil {
		s.spillFD.Release()
		s.spillFD = nil
	}
	if s.spillDisk != nil {
		s.spillDisk.Release()
		s.spillDisk = nil
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
	s.spillConfig = SpillConfig{}
}
