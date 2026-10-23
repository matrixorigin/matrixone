// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package insert

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// controlledS3 keeps staged data, IO scratch, and transaction output in their
// admitted domains. Metadata is streamed to an anonymous file during Build;
// Eval returns the original MergeBlock protocol one frame per Call.
type controlledS3 struct {
	owner      *Insert
	fs         fileservice.FileService
	memory     *process.ImportMemory
	throttler  rscthrottler.RSCThrottler
	dataMP     *mpool.MPool
	outputMP   *mpool.MPool
	outputSlot *rscthrottler.InputLease
	writer     *colexec.CNS3Writer
	unit       int64
	staged     int64
	mu         sync.Mutex
	granted    int64
	mutations  int
	spool      *os.File
	written    int64
	read       int64
	output     *batch.Batch
}

func (insert *Insert) insertControlledS3(proc *process.Process, analyzer process.Analyzer) (result vm.CallResult, err error) {
	s := insert.ctr.controlled
	defer func() {
		if err != nil {
			insert.ctr.state = vm.End
			err = errors.Join(err, s.close())
		}
	}()
	if insert.ctr.state == vm.Build {
		for {
			input, readErr := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
			if readErr != nil {
				return vm.CancelResult, readErr
			}
			if input.Batch == nil {
				break
			}
			if input.Batch.IsEmpty() {
				continue
			}
			if insert.InsertCtx.AddAffectedRows {
				atomic.AddUint64(&insert.ctr.affectedRows, uint64(input.Batch.RowCount()))
			}
			if err = s.append(proc, analyzer, input.Batch); err != nil {
				return vm.CancelResult, err
			}
		}
		if err = s.flush(proc, analyzer); err != nil {
			return vm.CancelResult, err
		}
		insert.ctr.state = vm.Eval
	}
	if insert.ctr.state == vm.Eval {
		var output *batch.Batch
		if output, err = s.next(); err != nil {
			return vm.CancelResult, err
		}
		if output != nil {
			result = vm.NewCallResult()
			result.Batch = output
			return result, nil
		}
		insert.ctr.state = vm.End
	}
	return vm.CancelResult, nil
}

func newControlledS3(proc *process.Process, owner *Insert, fs fileservice.FileService, throttler rscthrottler.RSCThrottler) (*controlledS3, error) {
	memory := proc.Base.ImportMemory
	unit := min(memory.UnitBytes, rscthrottler.BatchBytes(throttler))
	if unit <= 0 {
		return nil, fmt.Errorf("invalid admitted insert unit %d", unit)
	}
	lease, err := memory.AcquireOutput(proc.Ctx)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, fmt.Errorf("admitted insert has no output slot")
	}
	return &controlledS3{owner: owner, fs: fs, memory: memory, throttler: throttler,
		unit: unit, outputSlot: lease, dataMP: mpool.MustNew("insert-data"), outputMP: mpool.MustNew("insert-output")}, nil
}

func (s *controlledS3) reconcileLocked() {
	if s.throttler != nil && s.dataMP != nil {
		retained := s.dataMP.AllocatedBytes()
		if s.writer != nil {
			retained += s.writer.RetainedBitmapBytes()
		}
		if excess := s.granted - retained; excess > 0 {
			s.throttler.Release(excess)
			s.granted -= excess
		}
	}
}

func (s *controlledS3) sourceReleased() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mutations == 0 {
		s.reconcileLocked()
	}
}

func (s *controlledS3) begin() {
	s.mu.Lock()
	s.reconcileLocked()
	s.mutations++
	s.mu.Unlock()
}

func (s *controlledS3) end() {
	s.mu.Lock()
	s.mutations--
	if s.mutations == 0 {
		s.reconcileLocked()
	}
	s.mu.Unlock()
}

func (s *controlledS3) acquire(n int64) bool {
	if n <= 0 {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.throttler != nil {
		if _, ok := s.throttler.Acquire(n); ok {
			s.granted += n
			return true
		}
	}
	return false
}

func (s *controlledS3) ensureWriter(proc *process.Process) {
	if s.writer != nil {
		return
	}
	opts := []ioutil.SinkerOption{ioutil.WithMemoryWorkspace(s.memory.WorkingSet),
		ioutil.WithBufferBytes(s.unit), ioutil.WithRetainedRelease(s.sourceReleased)}
	if pipeline, _ := proc.Ctx.Value(ioutil.PipelineFlushKey).(bool); pipeline {
		opts = append(opts, ioutil.WithPipelineFlush())
	}
	s.writer = colexec.NewCNS3DataWriter(s.dataMP, s.fs, s.owner.InsertCtx.TableDef, int(s.unit), false, opts...)
}

func insertWindowBytes(bat *batch.Batch, start, end int) int64 {
	var n int64
	for _, v := range bat.Vecs {
		n += v.WindowSize(start, end)
	}
	return n
}

func insertWindowEnd(bat *batch.Batch, start int, unit int64) int {
	last := bat.RowCount()
	if insertWindowBytes(bat, start, last) <= unit {
		return last
	}
	lo, hi := start+1, last
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if insertWindowBytes(bat, start, mid) <= unit {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

func (s *controlledS3) append(proc *process.Process, analyzer process.Analyzer, input *batch.Batch) error {
	count := len(s.owner.InsertCtx.Attrs)
	if count == 0 || count > len(input.Vecs) {
		return fmt.Errorf("invalid admitted insert projection width %d", count)
	}
	cols := make([]int, count)
	for i := range cols {
		cols[i] = i
	}
	projected := input.SelectColumns(cols, s.owner.InsertCtx.Attrs)
	for start := 0; start < projected.RowCount(); {
		if err := context.Cause(proc.Ctx); err != nil {
			return err
		}
		end := insertWindowEnd(projected, start, s.unit)
		window, err := projected.Window(start, end)
		if err != nil {
			return err
		}
		err = s.appendWindow(proc, analyzer, window)
		window.Clean(s.dataMP)
		if err != nil {
			return err
		}
		start = end
	}
	return nil
}

func (s *controlledS3) appendWindow(proc *process.Process, analyzer process.Analyzer, window *batch.Batch) error {
	n := insertWindowBytes(window, 0, window.RowCount())
	if s.staged > 0 && (n > s.unit || s.staged > s.unit-n) {
		if err := s.flush(proc, analyzer); err != nil {
			return err
		}
	}
	for {
		s.ensureWriter(proc)
		s.begin()
		ok := s.acquire(s.writer.AppendAllocation(window))
		if ok {
			err := s.writer.Write(proc.Ctx, window)
			s.end()
			if err != nil {
				return err
			}
			s.staged += n
			if s.staged >= s.unit {
				return s.flush(proc, analyzer)
			}
			return nil
		}
		s.end()
		if s.staged > 0 || s.dataMP.AllocatedBytes() > 0 {
			// Retire all actual D owners before retrying with a cold writer.
			if err := s.flush(proc, analyzer); err != nil {
				return err
			}
			continue
		}
		// A cold writer cannot wait for D while its upstream input owns I.
		// The pre-admitted W operation clones and writes the borrowed input.
		lease, err := s.memory.AcquireIO(proc.Ctx)
		if err != nil {
			return err
		}
		err = s.writer.WriteBorrowedToFile(proc.Ctx, window, lease)
		lease.Release()
		if err != nil {
			return err
		}
		// Sync can drain jobs/acquire W; the borrowed W is already released.
		return s.flush(proc, analyzer)
	}
}

func (s *controlledS3) flush(proc *process.Process, analyzer process.Analyzer) error {
	if s.writer == nil {
		return nil
	}
	// Sync/Close transfer or retire local bitmap owners as well as vector
	// backing. A worker callback must not inspect those lists mid-mutation.
	s.begin()
	defer s.end()
	counters := analyzer.GetOpCounterSet()
	ctx := perfcounter.AttachS3RequestKey(proc.Ctx, counters)
	stats, err := s.writer.Sync(ctx)
	if err == nil {
		err = s.spoolStats(stats)
	}
	analyzer.AddS3RequestCount(counters)
	analyzer.AddFileServiceCacheInfo(counters)
	analyzer.AddDiskIO(counters)
	// Close drains async jobs and destroys the retained free list. Its source
	// callbacks may return only actual capacity, never the prepaid next N.
	err = errors.Join(err, s.writer.Close())
	s.writer = nil
	s.staged = 0
	return err
}

func (s *controlledS3) spoolStats(stats []objectio.ObjectStats) (err error) {
	if len(stats) == 0 {
		return nil
	}
	var blocks int64
	for i := range stats {
		if blocks > math.MaxInt64-int64(stats[i].BlkCnt()) {
			return fmt.Errorf("insert block metadata count overflow")
		}
		blocks += int64(stats[i].BlkCnt())
	}
	frameLimit, err := s.metadataFrameLimit()
	if err != nil {
		return err
	}
	// Fixed encoding/footer and vector headers have ample room in the output
	// envelope's fixed overhead. Check the variable payload before allocation.
	if blocks > math.MaxInt64/int64(objectio.BlockInfoSize+24) || int64(len(stats)) > math.MaxInt64/int64(objectio.ObjectStatsLen+24) {
		return fmt.Errorf("insert block metadata size overflow")
	}
	bound := blocks*int64(objectio.BlockInfoSize+24) + int64(len(stats))*int64(objectio.ObjectStatsLen+24)
	if bound < 0 || bound > frameLimit-4096 {
		return fmt.Errorf("insert metadata exceeds admitted frame capacity: %d", bound)
	}
	if s.output != nil {
		s.output.Clean(s.outputMP)
	}
	s.output = colexec.AllocCNS3ResultBat(false, false)
	defer func() {
		s.output.Clean(s.outputMP)
		s.output = nil
	}()
	counts := []int64{blocks, int64(len(stats))}
	widths := []int64{int64(objectio.BlockInfoSize), int64(objectio.ObjectStatsLen)}
	var allocation int64
	for i, count := range counts {
		if count > int64(math.MaxInt)/widths[i] {
			return fmt.Errorf("insert metadata allocation overflow")
		}
		allocation += s.output.Vecs[i].AppendAllocation(int(count), int(count*widths[i]))
	}
	if allocation > s.outputSlot.Capacity()-s.outputMP.AllocatedBytes() {
		return fmt.Errorf("insert metadata allocation exceeds admitted output capacity")
	}
	for i, count := range counts {
		if err = s.output.Vecs[i].PreExtendWithArea(int(count), int(count*widths[i]), s.outputMP); err != nil {
			return err
		}
	}
	if err = colexec.ExpandObjectStatsToBatch(s.outputMP, false, s.output, true, stats...); err != nil {
		return err
	}
	n := s.output.MarshalBinarySize()
	if n < 0 || n > frameLimit {
		return fmt.Errorf("insert metadata frame exceeds admitted output capacity: %d", n)
	}
	if s.spool == nil {
		s.spool, err = os.CreateTemp("", "mo-insert-*.spool")
		if err != nil {
			return err
		}
		if err = os.Remove(s.spool.Name()); err != nil {
			return err
		}
	}
	if _, err = s.spool.Seek(s.written, io.SeekStart); err != nil {
		return err
	}
	var header [8]byte
	binary.LittleEndian.PutUint64(header[:], uint64(n))
	if _, err = s.spool.Write(header[:]); err != nil {
		return err
	}
	if err = s.output.MarshalBinaryTo(s.spool); err != nil {
		_ = s.spool.Truncate(s.written)
		return err
	}
	s.written += 8 + n
	return nil
}

// The shared output slot may also serve raw joins or deletes. Its capacity
// cannot redefine this writer's fixed metadata protocol.
func (s *controlledS3) metadataFrameLimit() (int64, error) {
	limit, err := colexec.InsertMetadataFrameLimit(s.unit, 1)
	if err != nil {
		return 0, err
	}
	return min(limit, s.outputSlot.Capacity()/4), nil
}

func (s *controlledS3) next() (*batch.Batch, error) {
	if s.output != nil {
		s.output.Clean(s.outputMP)
		s.output = nil
	}
	if s.read == s.written {
		return nil, nil
	}
	if s.written-s.read < 8 {
		return nil, io.ErrUnexpectedEOF
	}
	var header [8]byte
	if _, err := s.spool.ReadAt(header[:], s.read); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint64(header[:])
	frameLimit, err := s.metadataFrameLimit()
	if err != nil {
		return nil, err
	}
	if n > uint64(s.written-s.read-8) || n > uint64(frameLimit) {
		return nil, fmt.Errorf("invalid insert metadata frame size %d", n)
	}
	r := io.NewSectionReader(s.spool, s.read+8, int64(n))
	s.output = batch.NewOffHeapEmpty()
	if err := s.output.UnmarshalFromReader(r, s.outputMP); err != nil {
		return nil, err
	}
	if remaining, _ := r.Seek(0, io.SeekCurrent); remaining != int64(n) {
		return nil, fmt.Errorf("unconsumed insert metadata frame bytes")
	}
	if len(s.output.Vecs) != 2 || len(s.output.Attrs) != 2 ||
		s.output.Attrs[0] != catalog.BlockMeta_BlockInfo || s.output.Attrs[1] != catalog.ObjectMeta_ObjectStats ||
		s.output.RowCount() != s.output.Vecs[0].Length() {
		return nil, fmt.Errorf("invalid insert metadata frame schema")
	}
	s.read += 8 + int64(n)
	return s.output, nil
}

func (s *controlledS3) close() (err error) {
	s.begin()
	if s.writer != nil {
		err = s.writer.Close()
		s.writer = nil
	}
	// Reconcile after the writer's callbacks have joined and before deleting
	// its pool; otherwise the final grant would lose its measurable owner.
	s.end()
	if s.output != nil {
		s.output.Clean(s.outputMP)
		s.output = nil
	}
	if s.spool != nil {
		err = errors.Join(err, s.spool.Close())
		s.spool = nil
	}
	if s.outputSlot != nil {
		s.outputSlot.Release()
		s.outputSlot = nil
	}
	if s.outputMP != nil {
		mpool.DeleteMPool(s.outputMP)
		s.outputMP = nil
	}
	if s.dataMP != nil {
		mpool.DeleteMPool(s.dataMP)
		s.dataMP = nil
	}
	return err
}
