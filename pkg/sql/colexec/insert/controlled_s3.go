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

package insert

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// controlledS3 keeps staged data, IO scratch, and transaction output in their
// admitted domains. Build retains compact object stats in the output slot;
// Eval expands the original MergeBlock protocol one object per Call.
type controlledS3 struct {
	proc                *process.Process
	owner               *Insert
	fs                  fileservice.FileService
	memory              *process.ImportMemory
	throttler           rscthrottler.RSCThrottler
	dataMP              *mpool.MPool
	outputMP            *mpool.MPool
	outputSlot          *rscthrottler.InputLease
	writer              *colexec.CNS3Writer
	unrecordedSinkNames []string
	unit                int64
	bufferBytes         int64
	staged              int64
	mu                  sync.Mutex
	granted             int64
	mutations           int
	statsSpool          *colexec.MetadataSpool
	statsCount          int64
	stats               *vector.Vector
	read                int
	output              *batch.Batch
	counters            *perfcounter.CounterSet
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
		if output, err = s.next(proc.Ctx); err != nil {
			return vm.CancelResult, err
		}
		if output != nil {
			var names []string
			if colexec.WorkspaceHandoffEnabled(proc) {
				names, err = colexec.WorkspaceHandoffObjectNames(output, 1)
				if err != nil {
					return vm.CancelResult, err
				}
			}
			if err = colexec.AttachWorkspaceHandoffScope(proc, output, names...); err != nil {
				return vm.CancelResult, err
			}
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
		return nil, moerr.NewInternalErrorNoCtxf("invalid admitted insert unit %d", unit)
	}
	lease, err := memory.AcquireOutput(proc.Ctx)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, moerr.NewInternalErrorNoCtx("admitted insert has no output slot")
	}
	_, schema, _, _, _ := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(owner.InsertCtx.TableDef)
	bufferBytes := ioutil.WriterBufferBytes(memory.WorkingSet, rscthrottler.BatchBytes(throttler), [][]types.Type{schema})
	return &controlledS3{proc: proc, owner: owner, fs: fs, memory: memory, throttler: throttler,
		unit: unit, bufferBytes: bufferBytes, outputSlot: lease, dataMP: mpool.MustNew("insert-data"), outputMP: mpool.MustNew("insert-output")}, nil
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

func (s *controlledS3) ensureWriter() {
	if s.writer != nil {
		return
	}
	opts := []ioutil.SinkerOption{ioutil.WithMemoryWorkspace(s.memory.WorkingSet),
		ioutil.WithBufferBytes(s.bufferBytes), ioutil.WithRetainedRelease(s.sourceReleased), ioutil.WithPipelineFlush()}
	if opt := colexec.WorkspaceSinkerCapacityOption(s.proc); opt != nil {
		opts = append(opts, opt)
	}
	s.writer = colexec.NewCNS3DataWriter(s.dataMP, s.fs, s.owner.InsertCtx.TableDef, int(s.bufferBytes), false, opts...)
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
		return moerr.NewInternalErrorNoCtxf("invalid admitted insert projection width %d", count)
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
		end := insertWindowEnd(projected, start, s.bufferBytes)
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
	if s.staged > 0 && (n > s.bufferBytes || s.staged > s.bufferBytes-n) {
		if err := s.flushPending(proc); err != nil {
			return err
		}
	}
	for {
		s.ensureWriter()
		s.begin()
		ok := s.acquire(s.writer.AppendAllocation(window))
		if ok {
			err := s.writer.Write(s.writeContext(proc.Ctx), window)
			s.end()
			if err != nil {
				return err
			}
			s.staged = s.writer.StagedSize()
			stats, err := s.writer.TakeCompletedStats()
			if err != nil {
				return err
			}
			return s.collectStats(proc.Ctx, stats)
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
		need, _, err := ioutil.BatchIOEnvelope(window, s.bufferBytes)
		if err != nil {
			return err
		}
		lease, err := s.memory.WorkingSet.AcquireIO(proc.Ctx, need)
		if err != nil {
			return err
		}
		err = s.writer.WriteBorrowedToFile(s.writeContext(proc.Ctx), window, lease)
		lease.Release()
		if err != nil {
			return err
		}
		// Detached uploads already own their complete W and need no source
		// progress. Keep them running after returning the borrowed parent W.
		stats, err := s.writer.TakeCompletedStats()
		if err != nil {
			return err
		}
		return s.collectStats(proc.Ctx, stats)
	}
}

func (s *controlledS3) flushPending(proc *process.Process) error {
	if s.writer == nil {
		return nil
	}
	s.begin()
	defer s.end()
	ctx := s.writeContext(proc.Ctx)
	if err := s.writer.Flush(ctx); err != nil {
		return err
	}
	stats, err := s.writer.TakeCompletedStats()
	if err != nil {
		return err
	}
	s.staged = s.writer.StagedSize()
	return s.collectStats(proc.Ctx, stats)
}

func (s *controlledS3) flush(proc *process.Process, analyzer process.Analyzer) error {
	if s.writer == nil {
		return nil
	}
	// Sync/Close transfer or retire local bitmap owners as well as vector
	// backing. A worker callback must not inspect those lists mid-mutation.
	s.begin()
	defer s.end()
	ctx := s.writeContext(proc.Ctx)
	stats, err := s.writer.Sync(ctx)
	if err == nil {
		err = s.collectStats(proc.Ctx, stats)
	}
	if s.writer.HasUncommittedCapacityHook() {
		s.unrecordedSinkNames = append(s.unrecordedSinkNames, s.writer.AbandonAndRetainableNames()...)
	}
	// Close drains async jobs and destroys the retained free list. Its source
	// callbacks may return only actual capacity, never the prepaid next N.
	err = errors.Join(err, s.writer.Close())
	s.writer = nil
	s.staged = 0
	s.reportCounters(analyzer)
	return err
}

func (s *controlledS3) writeContext(ctx context.Context) context.Context {
	if s.counters == nil {
		s.counters = new(perfcounter.CounterSet)
	}
	return perfcounter.AttachS3RequestKey(ctx, s.counters)
}

func (s *controlledS3) reportCounters(analyzer process.Analyzer) {
	if s.counters != nil {
		analyzer.AddS3RequestCount(s.counters)
		analyzer.AddFileServiceCacheInfo(s.counters)
		analyzer.AddDiskIO(s.counters)
		s.counters.Reset()
	}
}

func (s *controlledS3) collectStats(ctx context.Context, stats []objectio.ObjectStats) error {
	for i := range stats {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		limit, err := s.metadataFrameLimit()
		if err != nil {
			return err
		}
		blocks := int64(stats[i].BlkCnt())
		if blocks > (math.MaxInt64-int64(objectio.ObjectStatsLen+24))/int64(objectio.BlockInfoSize+24) {
			return moerr.NewInternalErrorNoCtx("insert block metadata size overflow")
		}
		bound := blocks*int64(objectio.BlockInfoSize+24) + int64(objectio.ObjectStatsLen+24)
		if bound > limit-4096 {
			return moerr.NewInternalErrorNoCtxf("insert metadata exceeds admitted frame capacity: %d", bound)
		}
		if s.statsSpool != nil {
			if err := s.writeStat(&stats[i]); err != nil {
				return err
			}
			continue
		}
		if s.stats == nil {
			s.stats = vector.NewOffHeapVecWithType(types.T_binary.ToType())
		}
		growth := s.stats.AppendAllocation(1, objectio.ObjectStatsLen)
		if err = colexec.GrowMetadataOutput(ctx, s.outputSlot, s.outputMP.AllocatedBytes(), growth); err != nil {
			var capacity *rscthrottler.WorkingSetCapacityError
			if !errors.As(err, &capacity) {
				return err
			}
			if err := s.spoolStats(ctx); err != nil {
				return err
			}
			if err := s.writeStat(&stats[i]); err != nil {
				return err
			}
			continue
		}
		if err = vector.AppendBytes(s.stats, stats[i][:], false, s.outputMP); err != nil {
			return err
		}
	}
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

func (s *controlledS3) next(ctx context.Context) (*batch.Batch, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if s.output != nil {
		s.output.Clean(s.outputMP)
		s.output = nil
	}
	var stats objectio.ObjectStats
	if s.statsSpool != nil {
		if int64(s.read) == s.statsCount {
			return nil, nil
		}
		if _, err := s.statsSpool.ReadAt(stats[:], int64(s.read)*int64(objectio.ObjectStatsLen)); err != nil {
			return nil, err
		}
	} else {
		if s.stats == nil {
			return nil, nil
		}
		if s.read == s.stats.Length() {
			s.stats.Free(s.outputMP)
			s.stats = nil
			s.read = 0
			return nil, nil
		}
		stats = objectio.ObjectStats(s.stats.GetBytesAt(s.read))
	}
	s.output = colexec.AllocCNS3ResultBat(false)
	counts := []int64{int64(stats.BlkCnt()), 1}
	widths := []int64{int64(objectio.BlockInfoSize), int64(objectio.ObjectStatsLen)}
	var allocation int64
	for i, count := range counts {
		allocation += s.output.Vecs[i].AppendAllocation(int(count), int(count*widths[i]))
	}
	if err := colexec.GrowMetadataOutput(ctx, s.outputSlot, s.outputMP.AllocatedBytes(), allocation); err != nil {
		var capacity *rscthrottler.WorkingSetCapacityError
		if s.statsSpool != nil || !errors.As(err, &capacity) {
			return nil, err
		}
		s.output.Clean(s.outputMP)
		s.output = nil
		if err := s.spoolStats(ctx); err != nil {
			return nil, err
		}
		return s.next(ctx)
	}
	for i, count := range counts {
		if err := s.output.Vecs[i].PreExtendWithArea(int(count), int(count*widths[i]), s.outputMP); err != nil {
			return nil, err
		}
	}
	if err := colexec.ExpandObjectStatsToBatch(s.outputMP, false, s.output, true, stats); err != nil {
		return nil, err
	}
	s.read++
	return s.output, nil
}

func (s *controlledS3) close() (err error) {
	s.begin()
	if s.owner != nil {
		s.owner.retainUnrecordedSinkObjects(s.proc)
	}
	if s.writer != nil {
		err = s.writer.Close()
		s.writer = nil
	}
	if s.owner != nil && s.owner.OpAnalyzer != nil {
		s.reportCounters(s.owner.OpAnalyzer)
	}
	// Reconcile after the writer's callbacks have joined and before deleting
	// its pool; otherwise the final grant would lose its measurable owner.
	s.end()
	if s.output != nil {
		s.output.Clean(s.outputMP)
		s.output = nil
	}
	if s.stats != nil {
		s.stats.Free(s.outputMP)
		s.stats = nil
	}
	if s.statsSpool != nil {
		err = errors.Join(err, s.statsSpool.Close())
		s.statsSpool = nil
	}
	s.statsCount = 0
	s.read = 0
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

func (s *controlledS3) writeStat(stats *objectio.ObjectStats) error {
	if err := s.statsSpool.Reserve(uint64(len(stats))); err != nil {
		return err
	}
	if _, err := s.statsSpool.Write(stats[:]); err != nil {
		return err
	}
	s.statsCount++
	return nil
}

func (s *controlledS3) spoolStats(ctx context.Context) error {
	f, err := colexec.NewMetadataSpool(s.proc)
	if err != nil {
		return err
	}
	s.statsSpool = f
	for i := s.read; s.stats != nil && i < s.stats.Length(); i++ {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		stats := objectio.ObjectStats(s.stats.GetBytesAt(i))
		if err := s.writeStat(&stats); err != nil {
			return err
		}
	}
	if s.stats != nil {
		s.stats.Free(s.outputMP)
		s.stats = nil
	}
	s.read = 0
	return nil
}
