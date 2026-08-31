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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
}

const sharedMaterializedSourceMaxBytes = int64(64 * mpool.MB)
const sharedMaterializedSourceMaxInMemoryBatches = 4096

const spillBatchHeaderSize = int64(8)
const maxSpillBatchBytes = uint64(64 * mpool.MB)

// SpillFileFactory creates an anonymous query-scoped file for overflow data.
type SpillFileFactory func(string) (*os.File, error)

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
}

// CTESinkOption marks a planner-approved bounded multi-consumer CTE source.
const CTESinkOption = "cte_reuse_materialized_sink"

// CTEHashBuildScanOption marks a CTE reader whose full-drain proof depends on
// remaining the build input of an equality hash join. The planner uses this
// marker to keep a later build/probe-side decision from invalidating the proof.
const CTEHashBuildScanOption = "cte_reuse_hash_build_scan"

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

// AppendStats describes the storage effect of one successful append. RetainedBytes
// is the source's current in-memory footprint; SpilledBytes and SpilledRows are
// deltas for the batch written by this call.
type AppendStats struct {
	RetainedBytes int64
	SpilledBytes  int64
	SpilledRows   int64
}

func (s *Source) Append(bat *batch.Batch) error {
	_, err := s.AppendWithStats(bat)
	return err
}

// AppendWithStats stores one producer batch and returns the exact storage delta
// owned by that producer. Spill deltas become visible only after the complete
// framed record has been written successfully.
func (s *Source) AppendWithStats(bat *batch.Batch) (stats AppendStats, err error) {
	if s == nil || bat == nil {
		return stats, nil
	}
	reserved := int64(max(bat.Size(), bat.Allocated()))
	s.mu.Lock()
	if !s.active || s.done {
		stats.RetainedBytes = s.bytes
		s.mu.Unlock()
		return stats, moerr.NewInternalErrorNoCtx("materialized sink source is not accepting data")
	}
	if s.spillFile != nil || reserved < 0 || reserved > s.memoryLimit || s.bytes > s.memoryLimit-reserved ||
		len(s.batches) >= s.memoryBatchLimit {
		stats.SpilledBytes, err = s.appendSpilledLocked(bat)
		if err != nil {
			s.failLocked(err)
		} else {
			stats.SpilledRows = int64(bat.RowCount())
		}
		stats.RetainedBytes = s.bytes
		s.mu.Unlock()
		return stats, err
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
		stats.RetainedBytes = s.bytes
		s.mu.Unlock()
		return stats, err
	}
	actual := int64(max(cloned.Size(), cloned.Allocated()))

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation != generation || !s.active || s.done {
		if s.generation == generation && s.active {
			s.bytes -= reserved
		}
		stats.RetainedBytes = s.bytes
		cloned.Clean(mp)
		if s.generation == generation && s.err != nil {
			return stats, s.err
		}
		return stats, moerr.NewInternalErrorNoCtx("materialized sink source stopped while copying data")
	}
	if actual > reserved && (actual-reserved > s.memoryLimit || s.bytes > s.memoryLimit-(actual-reserved)) {
		s.bytes -= reserved
		cloned.Clean(mp)
		stats.SpilledBytes, err = s.appendSpilledLocked(bat)
		if err != nil {
			s.failLocked(err)
		} else {
			stats.SpilledRows = int64(bat.RowCount())
		}
		stats.RetainedBytes = s.bytes
		return stats, err
	}
	s.bytes += actual - reserved
	s.batches = append(s.batches, cloned)
	s.wakeLocked()
	stats.RetainedBytes = s.bytes
	return stats, nil
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
			budget := s.spillConfig.Budget
			s.mu.Unlock()

			decoded, nextOffset, readErr := readSpilledBatch(file, offset, availableBytes, mp, budget)

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

func (s *Source) appendSpilledLocked(bat *batch.Batch) (int64, error) {
	if s.spillConfig.FileFactory == nil || s.spillConfig.Budget.ReserveMemory == nil ||
		s.spillConfig.Budget.ReserveDisk == nil || s.spillConfig.Budget.ReserveFD == nil {
		return 0, moerr.NewInternalErrorNoCtx("materialized sink source spill is unavailable")
	}
	serializedBytes, scratchBytes, err := spillBatchSize(bat)
	if err != nil {
		return 0, err
	}
	memoryReservation, err := s.spillConfig.Budget.ReserveMemory(scratchBytes)
	if err != nil {
		return 0, err
	}
	if memoryReservation == nil {
		return 0, moerr.NewInternalErrorNoCtx("materialized sink spill memory reservation is nil")
	}
	defer memoryReservation.Release()
	buf := bytes.NewBuffer(make([]byte, 0, int(serializedBytes)))
	data, err := bat.MarshalBinaryWithPrepareParamKinds(buf, false)
	if err != nil {
		return 0, err
	}
	if uint64(len(data)) != serializedBytes {
		return 0, moerr.NewInternalErrorNoCtxf("materialized sink spill batch size changed while serializing: expected=%d actual=%d", serializedBytes, len(data))
	}
	recordBytes := uint64(spillBatchHeaderSize) + serializedBytes
	if recordBytes > math.MaxInt64 || s.spillBytes > math.MaxInt64-int64(recordBytes) || s.spillBatchCount == math.MaxInt {
		return 0, moerr.NewInternalErrorNoCtx("materialized sink spill position overflow")
	}
	newFile := s.spillFile == nil
	if newFile {
		s.spillDisk, err = s.spillConfig.Budget.ReserveDisk(recordBytes)
	} else {
		err = s.spillDisk.Grow(recordBytes)
	}
	if err != nil {
		return 0, err
	}
	if s.spillDisk == nil {
		return 0, moerr.NewInternalErrorNoCtx("materialized sink spill disk reservation is nil")
	}
	if newFile {
		s.spillFD, err = s.spillConfig.Budget.ReserveFD(1)
		if err != nil {
			s.spillDisk.Release()
			s.spillDisk = nil
			return 0, err
		}
		if s.spillFD == nil {
			s.spillDisk.Release()
			s.spillDisk = nil
			return 0, moerr.NewInternalErrorNoCtx("materialized sink spill file reservation is nil")
		}
		file, fileErr := s.spillConfig.FileFactory(fmt.Sprintf("cte_materialized_%s", uuid.NewString()))
		if fileErr != nil || file == nil {
			if file != nil {
				_ = file.Close()
			}
			s.spillFD.Release()
			s.spillFD = nil
			s.spillDisk.Release()
			s.spillDisk = nil
			if fileErr != nil {
				return 0, fileErr
			}
			return 0, moerr.NewInternalErrorNoCtx("materialized sink spill file is nil")
		}
		s.spillFile = file
		s.spillStartPosition = len(s.batches)
	}
	var header [spillBatchHeaderSize]byte
	binary.LittleEndian.PutUint64(header[:], uint64(len(data)))
	if n, writeErr := s.spillFile.Write(header[:]); writeErr != nil {
		return 0, writeErr
	} else if n != len(header) {
		return 0, io.ErrShortWrite
	}
	if n, writeErr := s.spillFile.Write(data); writeErr != nil {
		return 0, writeErr
	} else if n != len(data) {
		return 0, io.ErrShortWrite
	}
	s.spillBatchCount++
	s.spillBytes += spillBatchHeaderSize + int64(len(data))
	s.wakeLocked()
	return int64(recordBytes), nil
}

func readSpilledBatch(file *os.File, offset, availableBytes int64, mp *mpool.MPool, budget SpillBudget) (*batch.Batch, int64, error) {
	if file == nil || offset < 0 || offset > availableBytes-spillBatchHeaderSize {
		return nil, offset, moerr.NewInternalErrorNoCtx("invalid materialized sink spill offset")
	}
	var header [spillBatchHeaderSize]byte
	if _, err := file.ReadAt(header[:], offset); err != nil {
		return nil, offset, err
	}
	size := int64(binary.LittleEndian.Uint64(header[:]))
	if size < 0 || size > availableBytes-offset-spillBatchHeaderSize || uint64(size) > maxSpillBatchBytes || size > int64(int(^uint(0)>>1)) {
		return nil, offset, moerr.NewInternalErrorNoCtx("invalid materialized sink spill batch size")
	}
	if budget.ReserveMemory == nil {
		return nil, offset, moerr.NewInternalErrorNoCtx("materialized sink spill read budget is unavailable")
	}
	memoryReservation, err := budget.ReserveMemory(uint64(size))
	if err != nil {
		return nil, offset, err
	}
	if memoryReservation == nil {
		return nil, offset, moerr.NewInternalErrorNoCtx("materialized sink spill read reservation is nil")
	}
	defer memoryReservation.Release()
	data := make([]byte, int(size))
	if _, err := file.ReadAt(data, offset+spillBatchHeaderSize); err != nil {
		return nil, offset, err
	}
	decoded := batch.NewWithSize(0)
	if err := decoded.UnmarshalBinaryWithPrepareParamKinds(data, mp); err != nil {
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

func spillBatchSize(bat *batch.Batch) (serialized, scratch uint64, err error) {
	if bat == nil {
		return 0, 0, moerr.NewInternalErrorNoCtx("nil materialized sink spill batch")
	}
	serialized = 8 + 4 + 4 + 4 + 4 + 4
	if len(bat.Vecs) > math.MaxInt32 || len(bat.Attrs) > math.MaxInt32 || len(bat.ExtraBuf) > math.MaxInt32 {
		return 0, 0, moerr.NewInternalErrorNoCtx("materialized sink spill batch exceeds encoding limit")
	}
	for _, vec := range bat.Vecs {
		if vec == nil || vec.Length() < 0 {
			return 0, 0, moerr.NewInternalErrorNoCtx("invalid materialized sink spill vector")
		}
		typeSize := vec.GetType().TypeSize()
		if typeSize <= 0 || vec.Length() > math.MaxUint32 || len(vec.GetArea()) > math.MaxUint32 {
			return 0, 0, moerr.NewInternalErrorNoCtx("materialized sink spill vector exceeds encoding limit")
		}
		dataBytes := uint64(typeSize)
		if !vec.IsConst() {
			if uint64(vec.Length()) > math.MaxUint32/dataBytes {
				return 0, 0, moerr.NewInternalErrorNoCtx("materialized sink spill batch size overflow")
			}
			dataBytes *= uint64(vec.Length())
		} else if vec.IsConstNull() {
			dataBytes = 0
		}
		if dataBytes > uint64(len(vec.GetData())) {
			return 0, 0, moerr.NewInternalErrorNoCtx("invalid materialized sink spill vector data")
		}
		nullBytes := uint64(0)
		if !vec.GetNulls().EmptyByFlag() {
			nullBytes = uint64(nulls.Size(vec.GetNulls())) + 24
		}
		if nullBytes > math.MaxUint32 {
			return 0, 0, moerr.NewInternalErrorNoCtx("materialized sink spill null bitmap exceeds encoding limit")
		}
		vectorBytes := uint64(4 + 1 + types.TSize + 4 + 4 + 4 + 4 + 1)
		vectorBytes, err = checkedAdd(vectorBytes, dataBytes, uint64(len(vec.GetArea())), nullBytes)
		if err != nil {
			return 0, 0, err
		}
		serialized, err = checkedAdd(serialized, vectorBytes)
		if err != nil {
			return 0, 0, err
		}
		if nullBytes > 0 {
			nullScratch, addErr := checkedAdd(nullBytes, nullBytes)
			if addErr != nil {
				return 0, 0, addErr
			}
			nullScratch = max(uint64(64), nullScratch)
			scratch, err = checkedAdd(scratch, nullScratch)
			if err != nil {
				return 0, 0, err
			}
		}
	}
	for _, attr := range bat.Attrs {
		if len(attr) > math.MaxInt32 {
			return 0, 0, moerr.NewInternalErrorNoCtx("materialized sink spill attribute exceeds encoding limit")
		}
		serialized, err = checkedAdd(serialized, 4, uint64(len(attr)))
		if err != nil {
			return 0, 0, err
		}
	}
	metadataBytes, metadataErr := bat.PrepareParamKindMetadataSize()
	if metadataErr != nil {
		return 0, 0, metadataErr
	}
	serialized, err = addSpillBatchTail(
		serialized,
		uint64(len(bat.ExtraBuf)),
		uint64(metadataBytes),
	)
	if err != nil || serialized > maxSpillBatchBytes || serialized > uint64(math.MaxInt) {
		return 0, 0, moerr.NewInternalErrorNoCtx("materialized sink spill batch exceeds runtime limit")
	}
	scratch, err = checkedAdd(scratch, serialized)
	return serialized, scratch, err
}

func addSpillBatchTail(serialized, extraBytes, metadataBytes uint64) (uint64, error) {
	withExtra, err := checkedAdd(serialized, extraBytes)
	if err != nil {
		return 0, err
	}
	return checkedAdd(withExtra, metadataBytes)
}

func checkedAdd(values ...uint64) (uint64, error) {
	var total uint64
	for _, value := range values {
		if value > math.MaxUint64-total {
			return 0, moerr.NewInternalErrorNoCtx("materialized sink spill batch size overflow")
		}
		total += value
	}
	return total, nil
}
