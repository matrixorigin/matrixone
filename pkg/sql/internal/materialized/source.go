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
	"errors"
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

	batches            []*batch.Batch
	bytes              int64
	memoryLimit        int64
	memoryBatchLimit   int
	spillFile          *os.File
	spillConfig        SpillConfig
	allocation         *vector.AllocationAccountSelection
	spillDisk          GrowingReservation
	spillFD            Reservation
	spillStartPosition int
	spillBatchCount    int
	spillBytes         int64
	spillReadOffsets   []int64
	spillReadPositions []int
	spillReadersActive []bool
	spillBatchLimit    uint64
	generation         uint64
	done               bool
	err                error
	active             bool

	readerReleased   []bool
	producerReleased bool
}

// MaxSourceRetainedBytes is the per-source in-memory retention bound before
// later batches use the query spill ledger.
const MaxSourceRetainedBytes = int64(64 * mpool.MB)

const sharedMaterializedSourceMaxBytes = MaxSourceRetainedBytes
const sharedMaterializedSourceMaxInMemoryBatches = 4096

const spillBatchHeaderSize = int64(8)

const (
	spillPayloadGrouping byte = iota
	spillPayloadSelectedRange
)

// MaxSpillBatchBytes is the largest decoded record admitted by the shared
// materialized-source reader. The writer splits wider multi-row batches to the
// same bound, while the planner rejects a schema whose single declared row can
// approach it.
const MaxSpillBatchBytes = uint64(64 * mpool.MB)

const maxSpillBatchBytes = MaxSpillBatchBytes

var errSpillBatchTooLarge = moerr.NewInternalErrorNoCtx("materialized sink spill batch exceeds runtime limit")

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
	FileFactory       SpillFileFactory
	Budget            SpillBudget
	AllocationAccount *mpool.AllocationAccount
}

const (
	cteAllocationSiteData mpool.AllocationSite = iota + 1
	cteAllocationSiteArea
	cteAllocationSiteNulls
	cteAllocationSiteGrouping
)

// CTESinkOption marks a planner-approved bounded multi-consumer CTE source.
const CTESinkOption = "cte_reuse_materialized_sink"

// CTEHashBuildScanOption marks a CTE reader whose complete-evaluation witness
// depends on remaining the build input of a planner-approved join. The planner
// uses this marker to keep later join ordering or build/probe selection from
// invalidating the proof.
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
		spillBatchLimit:    maxSpillBatchBytes,
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
		return moerr.NewInternalErrorNoCtxf(
			"materialized sink source reused before all owners released: generation=%d producer_released=%t unreleased_readers=%v",
			s.generation, s.producerReleased, s.unreleasedReaderIDsLocked())
	}
	s.cleanLocked()
	s.generation++
	s.notify = make(chan struct{})
	s.mp = mp
	s.spillConfig = SpillConfig{}
	s.allocation = nil
	if len(spillConfig) > 0 {
		s.spillConfig = spillConfig[0]
		if s.spillConfig.AllocationAccount == nil {
			return moerr.NewInternalErrorNoCtx(
				"materialized sink spill allocation account is unavailable",
			)
		}
		allocation, err := vector.NewAllocationAccountSelection(
			s.spillConfig.AllocationAccount,
			mpool.AllocationOwnerCTE,
			cteAllocationSiteData,
			cteAllocationSiteArea,
			cteAllocationSiteNulls,
			cteAllocationSiteGrouping,
		)
		if err != nil {
			return err
		}
		s.allocation = allocation
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
	if len(bat.ExtraBuf) != 0 {
		return stats, moerr.NewInternalErrorNoCtx(
			"materialized sink source requires finalized positional batches",
		)
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
	allocation := s.allocation
	generation := s.generation
	s.mu.Unlock()

	cloned, err := cloneMaterializedBatch(bat, mp, allocation)
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
	// SINK_SCAN binds vectors by position. Attribute names are neither observed
	// nor part of the planner's storage estimate, so do not retain a second,
	// batch-cardinality-dependent copy of them.
	cloned.Attrs = nil

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
			allocation := s.allocation
			s.mu.Unlock()

			decoded, nextOffset, readErr := readSpilledBatch(
				file, offset, availableBytes, mp, budget, allocation,
			)

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
	positional := *bat
	positional.Attrs = nil
	serializedBytes, scratchBytes, err := spillBatchSizeWithLimit(&positional, s.spillBatchLimit)
	if errors.Is(err, errSpillBatchTooLarge) && positional.RowCount() > 1 {
		return s.appendSpillRangeLocked(&positional, 0, positional.RowCount())
	}
	if err != nil {
		return 0, err
	}
	return s.appendSpillRecordLocked(&positional, serializedBytes, scratchBytes)
}

func (s *Source) appendSpillRangeLocked(
	bat *batch.Batch,
	start, end int,
) (int64, error) {
	if bat == nil || start < 0 || end <= start || end > bat.RowCount() {
		return 0, moerr.NewInternalErrorNoCtx("invalid materialized sink spill window")
	}
	// The range codec reads values directly from the producer batch. Computing
	// its exact size is allocation-free, so an oversized parent is divided
	// before any data-scaled compact copy or serialization buffer exists.
	if end-start > math.MaxInt32 {
		mid := start + (end-start)/2
		leftBytes, appendErr := s.appendSpillRangeLocked(bat, start, mid)
		if appendErr != nil {
			return 0, appendErr
		}
		rightBytes, appendErr := s.appendSpillRangeLocked(bat, mid, end)
		if appendErr != nil {
			return 0, appendErr
		}
		if leftBytes > math.MaxInt64-rightBytes {
			return 0, moerr.NewInternalErrorNoCtx("materialized sink spill byte count overflow")
		}
		return leftBytes + rightBytes, nil
	}
	serializedBytes, sizeErr := selectedSpillBatchSize(bat, start, end)
	if sizeErr != nil {
		return 0, sizeErr
	}
	if serializedBytes > s.spillBatchLimit {
		if end-start == 1 {
			return 0, errSpillBatchTooLarge
		}
		mid := start + (end-start)/2
		leftBytes, appendErr := s.appendSpillRangeLocked(bat, start, mid)
		if appendErr != nil {
			return 0, appendErr
		}
		rightBytes, appendErr := s.appendSpillRangeLocked(bat, mid, end)
		if appendErr != nil {
			return 0, appendErr
		}
		if leftBytes > math.MaxInt64-rightBytes {
			return 0, moerr.NewInternalErrorNoCtx("materialized sink spill byte count overflow")
		}
		return leftBytes + rightBytes, nil
	}
	return s.appendEncodedSpillRecordLocked(
		serializedBytes,
		serializedBytes,
		func(w io.Writer) error {
			return marshalSelectedSpillBatchTo(w, bat, start, end)
		},
	)
}

func (s *Source) appendSpillRecordLocked(
	bat *batch.Batch,
	serializedBytes, scratchBytes uint64,
) (int64, error) {
	return s.appendEncodedSpillRecordLocked(
		serializedBytes,
		scratchBytes,
		func(w io.Writer) error {
			if n, err := w.Write([]byte{spillPayloadGrouping}); err != nil {
				return err
			} else if n != 1 {
				return io.ErrShortWrite
			}
			return bat.MarshalBinaryWithGroupingTo(w)
		},
	)
}

func (s *Source) appendEncodedSpillRecordLocked(
	serializedBytes, scratchBytes uint64,
	encode func(io.Writer) error,
) (int64, error) {
	if s.spillConfig.FileFactory == nil || s.spillConfig.Budget.ReserveMemory == nil ||
		s.spillConfig.Budget.ReserveDisk == nil || s.spillConfig.Budget.ReserveFD == nil ||
		encode == nil {
		return 0, moerr.NewInternalErrorNoCtx("materialized sink source spill is unavailable")
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
	if err = encode(buf); err != nil {
		return 0, err
	}
	data := buf.Bytes()
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

func readSpilledBatch(
	file *os.File,
	offset, availableBytes int64,
	mp *mpool.MPool,
	budget SpillBudget,
	allocation *vector.AllocationAccountSelection,
) (*batch.Batch, int64, error) {
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
	decoded, err := unmarshalSpillBatch(data, mp, allocation)
	if err != nil {
		if decoded != nil {
			decoded.Clean(mp)
		}
		return nil, offset, err
	}
	return decoded, offset + spillBatchHeaderSize + size, nil
}

func unmarshalSpillBatch(
	data []byte,
	mp *mpool.MPool,
	allocation *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	if len(data) == 0 || mp == nil || allocation == nil {
		return nil, moerr.NewInternalErrorNoCtx("invalid materialized sink spill payload")
	}
	r := bytes.NewReader(data[1:])
	var decoded *batch.Batch
	var err error
	switch data[0] {
	case spillPayloadGrouping:
		decoded = batch.NewOffHeapWithSize(0)
		if err = decoded.SetAllocationAccount(allocation); err == nil {
			err = decoded.UnmarshalFromReaderWithGrouping(r, mp)
		}
	case spillPayloadSelectedRange:
		decoded, err = unmarshalSelectedSpillBatchFrom(r, mp, allocation)
	default:
		return nil, moerr.NewInvalidInputNoCtx("unknown materialized sink spill payload")
	}
	if err != nil {
		if decoded != nil {
			decoded.Clean(mp)
		}
		return nil, err
	}
	if r.Len() != 0 {
		decoded.Clean(mp)
		return nil, moerr.NewInvalidInputNoCtx("trailing materialized sink spill payload")
	}
	return decoded, nil
}

func cloneMaterializedBatch(
	source *batch.Batch,
	mp *mpool.MPool,
	allocation *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	if source == nil || mp == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"materialized sink allocation account is unavailable",
		)
	}
	// Unit-only in-memory sources may omit a spill configuration. Production
	// compilation always supplies the execution account before Begin.
	if allocation == nil {
		return source.Dup(mp)
	}
	attrs, attrTypes := source.GetSchema()
	cloned := batch.NewWithSchema(true, attrs, attrTypes)
	if err := cloned.SetAllocationAccount(allocation); err != nil {
		cloned.Clean(mp)
		return nil, err
	}
	cloned.Recursive = source.Recursive
	if err := source.CloneTo(cloned, mp); err != nil {
		return nil, err
	}
	return cloned, nil
}

type spillSizeCounter struct {
	size uint64
}

func (w *spillSizeCounter) Write(value []byte) (int, error) {
	if w == nil || uint64(len(value)) > math.MaxUint64-w.size {
		return 0, moerr.NewInternalErrorNoCtx(
			"materialized sink spill batch size overflow",
		)
	}
	w.size += uint64(len(value))
	return len(value), nil
}

func selectedSpillBatchSize(
	bat *batch.Batch,
	start, end int,
) (uint64, error) {
	counter := &spillSizeCounter{}
	if err := marshalSelectedSpillBatchTo(counter, bat, start, end); err != nil {
		return 0, err
	}
	if counter.size > uint64(math.MaxInt) {
		return 0, errSpillBatchTooLarge
	}
	return counter.size, nil
}

func marshalSelectedSpillBatchTo(
	w io.Writer,
	bat *batch.Batch,
	start, end int,
) error {
	if w == nil || bat == nil || start < 0 || end <= start || end > bat.RowCount() ||
		end-start > math.MaxInt32 || len(bat.Vecs) > math.MaxInt32 {
		return moerr.NewInvalidInputNoCtx("invalid materialized sink spill row range")
	}
	if n, err := w.Write([]byte{spillPayloadSelectedRange}); err != nil {
		return err
	} else if n != 1 {
		return io.ErrShortWrite
	}
	if err := types.WriteInt64(w, int64(end-start)); err != nil {
		return err
	}
	if err := types.WriteInt32(w, int32(len(bat.Vecs))); err != nil {
		return err
	}
	for _, vec := range bat.Vecs {
		if vec == nil || vec.GetType() == nil {
			return moerr.NewInvalidInputNoCtx("invalid materialized sink spill vector")
		}
		typ := *vec.GetType()
		typeBytes := types.EncodeType(&typ)
		if n, err := w.Write(typeBytes); err != nil {
			return err
		} else if n != len(typeBytes) {
			return io.ErrShortWrite
		}
		sorted := byte(0)
		if vec.GetSorted() {
			sorted = 1
		}
		if n, err := w.Write([]byte{sorted}); err != nil {
			return err
		} else if n != 1 {
			return io.ErrShortWrite
		}
		if err := vec.MarshalRowRangeTo(w, start, end); err != nil {
			return err
		}
	}
	if err := types.WriteInt32(w, bat.Recursive); err != nil {
		return err
	}
	return types.WriteInt32(w, bat.ShuffleIDX)
}

func unmarshalSelectedSpillBatchFrom(
	r *bytes.Reader,
	mp *mpool.MPool,
	allocation *vector.AllocationAccountSelection,
) (*batch.Batch, error) {
	if r == nil || mp == nil || allocation == nil {
		return nil, moerr.NewInvalidInputNoCtx("invalid materialized sink spill decoder")
	}
	rows64, err := types.ReadInt64(r)
	if err != nil || rows64 <= 0 || rows64 > math.MaxInt32 || int64(int(rows64)) != rows64 {
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInvalidInputNoCtx("invalid materialized sink spill row count")
	}
	columns, err := types.ReadInt32AsInt(r)
	const minimumSelectedVectorBytes = types.TSize + 1 + 4 + 1
	if err != nil || columns < 0 || columns > r.Len()/minimumSelectedVectorBytes {
		if err != nil {
			return nil, err
		}
		return nil, moerr.NewInvalidInputNoCtx("invalid materialized sink spill column count")
	}
	decoded := batch.NewOffHeapWithSize(columns)
	if err = decoded.SetAllocationAccount(allocation); err != nil {
		decoded.Clean(mp)
		return nil, err
	}
	for i := range columns {
		typ, readErr := types.ReadType(r)
		if readErr != nil {
			decoded.Clean(mp)
			return nil, readErr
		}
		sorted, readErr := types.ReadByte(r)
		if readErr != nil || sorted > 1 {
			decoded.Clean(mp)
			if readErr != nil {
				return nil, readErr
			}
			return nil, moerr.NewInvalidInputNoCtx(
				"invalid materialized sink spill sorted flag",
			)
		}
		decoded.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(typ, allocation)
		if err != nil {
			decoded.Clean(mp)
			return nil, err
		}
		if err = decoded.Vecs[i].UnmarshalSelectedRowsFrom(r, int(rows64), mp); err != nil {
			decoded.Clean(mp)
			return nil, err
		}
		decoded.Vecs[i].SetSorted(sorted != 0)
	}
	if decoded.Recursive, err = types.ReadInt32(r); err != nil {
		decoded.Clean(mp)
		return nil, err
	}
	if decoded.ShuffleIDX, err = types.ReadInt32(r); err != nil {
		decoded.Clean(mp)
		return nil, err
	}
	decoded.SetRowCount(int(rows64))
	return decoded, nil
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
	if !s.active {
		return
	}
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

func (s *Source) unreleasedReaderIDsLocked() []int {
	unreleased := make([]int, 0, len(s.readerReleased))
	for readerID, released := range s.readerReleased {
		if !released {
			unreleased = append(unreleased, readerID)
		}
	}
	return unreleased
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
	s.allocation = nil
}

func spillBatchSize(bat *batch.Batch) (serialized, scratch uint64, err error) {
	return spillBatchSizeWithLimit(bat, maxSpillBatchBytes)
}

func spillBatchSizeWithLimit(
	bat *batch.Batch,
	limit uint64,
) (serialized, scratch uint64, err error) {
	if bat == nil {
		return 0, 0, moerr.NewInternalErrorNoCtx("nil materialized sink spill batch")
	}
	serialized = 1 + 8 + 4 + 4 + 4 + 4 + 4
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
	if err != nil {
		return 0, 0, err
	}
	// The spill-only wrapper frames the stable batch/parameter payload and then
	// appends one grouping bitmap per vector. Grouping provenance distinguishes
	// rollup sentinels from SQL NULL and therefore must survive a source crossing
	// the in-memory threshold.
	serialized, err = checkedAdd(serialized, 8, 4)
	if err != nil {
		return 0, 0, err
	}
	for _, vec := range bat.Vecs {
		groupingBytes := vec.GroupingMarshalBinarySize()
		if groupingBytes < 0 || groupingBytes > math.MaxInt32 {
			return 0, 0, moerr.NewInternalErrorNoCtx(
				"materialized sink spill grouping bitmap exceeds encoding limit",
			)
		}
		serialized, err = checkedAdd(serialized, 4, uint64(groupingBytes))
		if err != nil {
			return 0, 0, err
		}
	}
	if serialized > limit || serialized > uint64(math.MaxInt) {
		return 0, 0, errSpillBatchTooLarge
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
