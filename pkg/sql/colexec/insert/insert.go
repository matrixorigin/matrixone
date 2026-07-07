// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package insert

import (
	"bytes"
	"context"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/externalwrite"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// flushSemaphore limits concurrent flushS3WriterOnMemoryPressure calls to
// prevent thundering-herd mpool explosion when many workers are denied memory
// simultaneously and all try to flush + read objectio metadata at once.
const (
	minFlushConcurrencyLimit = 4
	maxFlushConcurrencyLimit = 16
)

type flushLimiter struct {
	mu     sync.Mutex
	inUse  int
	notify chan struct{}
}

var flushLimiterState = newFlushLimiter()
var flushConcurrencyForAcquire = flushConcurrency
var flushSemaphoreAcquireTimeout = 200 * time.Millisecond
var flushConcurrencyRefreshInterval = 20 * time.Millisecond

func newFlushLimiter() *flushLimiter {
	return &flushLimiter{notify: make(chan struct{})}
}

func (l *flushLimiter) tryAcquire() (func(), <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.inUse < flushConcurrencyForAcquire() {
		l.inUse++
		return l.release, nil
	}
	return nil, l.notify
}

func (l *flushLimiter) release() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.inUse <= 0 {
		panic("flush limiter released without acquire")
	}
	l.inUse--
	close(l.notify)
	l.notify = make(chan struct{})
}

func (l *flushLimiter) inUseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inUse
}

func flushConcurrency() int {
	return flushConcurrencyForGOMAXPROCS(goruntime.GOMAXPROCS(0))
}

func flushConcurrencyForGOMAXPROCS(gomaxprocs int) int {
	n := gomaxprocs / 2
	if n < minFlushConcurrencyLimit {
		n = minFlushConcurrencyLimit
	}
	if n > maxFlushConcurrencyLimit {
		n = maxFlushConcurrencyLimit
	}
	return n
}

const opName = "insert"

func (insert *Insert) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": insert")
}

func (insert *Insert) OpType() vm.OpType {
	return vm.Insert
}

func (insert *Insert) Prepare(proc *process.Process) error {
	if insert.OpAnalyzer == nil {
		insert.OpAnalyzer = process.NewAnalyzer(insert.GetIdx(), insert.IsFirst, insert.IsLast, "insert")
	} else {
		insert.OpAnalyzer.Reset()
	}

	insert.ctr.state = vm.Build
	if insert.ToExternal {
		cfg := insert.InsertCtx.ExternalConfig
		cfg.Attrs = insert.InsertCtx.Attrs
		// Prefer the per-execution statement start over the compile-time value:
		// prepared statements reuse the cached Compile across EXECUTEs, so the
		// config's Stmt would otherwise stay frozen at the first execution and
		// time-directive patterns would keep writing into the first day's path.
		// (Remote CNs lack StartTS on the context and keep the proto-carried
		// value, which the sender resolves the same way at encode time.)
		if v := proc.Ctx.Value(defines.StartTS{}); v != nil {
			if t, ok := v.(time.Time); ok {
				cfg.Stmt = t
			}
		}
		if cfg.TimeZone == nil {
			// Resolved here rather than at compile time so that an operator rebuilt
			// on a remote CN (whose process carries the session info) renders
			// TIMESTAMP values in the session's time zone too.
			cfg.TimeZone = proc.GetSessionInfo().TimeZone
		}
		insert.ctr.extWriter = externalwrite.NewExternalWriter(proc, cfg)
		// ColDefs aligned with Attrs, for the NOT NULL check (the minimal
		// external-insert plan runs no PreInsert, which normally enforces it).
		byName := make(map[string]*plan.ColDef, len(insert.InsertCtx.TableDef.Cols))
		for _, col := range insert.InsertCtx.TableDef.Cols {
			byName[col.GetOriginCaseName()] = col
		}
		insert.ctr.extCols = make([]*plan.ColDef, len(insert.InsertCtx.Attrs))
		for j, attr := range insert.InsertCtx.Attrs {
			insert.ctr.extCols[j] = byName[attr]
		}
		insert.ctr.affectedRows = 0
		return nil
	}
	if insert.ToWriteS3 {
		fs, err := colexec.GetSharedFSFromProc(proc)
		if err != nil {
			return err
		}

		// If the target is not partition table, you only need to operate the main table
		var sinkerOpts []ioutil.SinkerOption
		pipelineFlush := false
		if v, ok := proc.Ctx.Value(ioutil.PipelineFlushKey).(bool); ok && v {
			pipelineFlush = true
			sinkerOpts = append(sinkerOpts, ioutil.WithPipelineFlush())
		}
		s3Writer := colexec.NewCNS3DataWriter(
			proc.Mp(), fs, insert.InsertCtx.TableDef, -1, insert.isMemoryTable(),
			sinkerOpts...)

		insert.ctr.s3Writer = s3Writer
		insert.ctr.s3MemGranted = 0
		insert.ctr.s3MemNoThresholdCap = pipelineFlush
		insert.ctr.s3MemThrottler = nil
		if throttler, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.CNMemoryThrottler); ok {
			insert.ctr.s3MemThrottler = throttler.(rscthrottler.RSCThrottler)
		}

		if insert.ctr.buf == nil {
			insert.initBufForS3()
		}
	} else {
		ref := insert.InsertCtx.Ref
		eng := insert.InsertCtx.Engine

		if insert.ctr.source == nil {
			rel, err := colexec.GetRelAndPartitionRelsByObjRef(proc.Ctx, proc, eng, ref)
			if err != nil {
				return err
			}
			insert.ctr.source = rel
		} else {
			err := insert.ctr.source.Reset(proc.GetTxnOperator())
			if err != nil {
				return err
			}
		}

		if insert.ctr.buf == nil {
			insert.ctr.buf = batch.NewWithSize(len(insert.InsertCtx.Attrs))
			insert.ctr.buf.SetAttributes(insert.InsertCtx.Attrs)
		}
	}
	insert.ctr.affectedRows = 0
	return nil
}

// first parameter: true represents whether the current pipeline has ended
// first parameter: false
func (insert *Insert) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := insert.OpAnalyzer

	t := time.Now()
	defer func() {
		analyzer.AddInsertTime(t)
	}()

	if insert.ToExternal {
		return insert.insert_external(proc, analyzer)
	}
	if insert.ToWriteS3 {
		return insert.insert_s3(proc, analyzer)
	}
	return insert.insert_table(proc, analyzer)
}

// checkExternalNotNull rejects NULLs in NOT NULL columns. ctr.extCols is
// aligned with InsertCtx.Attrs (and therefore with the leading batch vectors).
func (insert *Insert) checkExternalNotNull(proc *process.Process, bat *batch.Batch) error {
	for j, col := range insert.ctr.extCols {
		if col == nil || j >= len(bat.Vecs) {
			continue
		}
		// Either signal marks the column NOT NULL: the explicit flag, or the
		// default-value descriptor PreInsert's check relies on.
		notNull := col.NotNull || (col.Default != nil && !col.Default.NullAbility)
		if !notNull {
			continue
		}
		vec := bat.Vecs[j]
		if vec.IsConstNull() || nulls.Any(vec.GetNulls()) {
			return moerr.NewConstraintViolationf(proc.Ctx, "Column '%s' cannot be null", insert.InsertCtx.Attrs[j])
		}
	}
	return nil
}

// insert_external writes a batch into a writable external table's backing file.
// One operator instance owns one ExternalWriter and therefore one output file.
// The file is finalized when the input stream ends (nil batch).
func (insert *Insert) insert_external(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	input, err := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
	if err != nil {
		return input, err
	}

	if input.Batch == nil {
		// End of input: flush and finalize the file.
		if insert.ctr.extWriter != nil {
			if _, cerr := insert.ctr.extWriter.Close(proc.Ctx); cerr != nil {
				return input, cerr
			}
			insert.ctr.extWriter = nil
		}
		return input, nil
	}
	if input.Batch.IsEmpty() {
		return input, nil
	}

	// NOT NULL enforcement normally happens in the PreInsert operator, which the
	// minimal external-insert plan does not run.
	if err = insert.checkExternalNotNull(proc, input.Batch); err != nil {
		return input, err
	}
	if err = insert.ctr.extWriter.WriteBatch(proc.Ctx, input.Batch); err != nil {
		return input, err
	}

	rows := uint64(input.Batch.RowCount())
	analyzer.AddWrittenRows(int64(rows))
	if insert.InsertCtx.AddAffectedRows {
		atomic.AddUint64(&insert.ctr.affectedRows, rows)
	}
	return input, nil
}

func (insert *Insert) insert_s3(proc *process.Process, analyzer process.Analyzer) (result vm.CallResult, err error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementInsertS3DurationHistogram.Observe(time.Since(start).Seconds())
	}()
	defer func() {
		if err != nil {
			insert.refreshAndReleaseS3MemGrant()
		}
	}()

	result = vm.NewCallResult()
	result.Batch = insert.ctr.buf

	if insert.ctr.state == vm.Build {
		for {
			input, err := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
			if err != nil {
				return input, err
			}

			if input.Batch == nil {
				insert.ctr.state = vm.Eval
				break
			}
			if input.Batch.IsEmpty() {
				continue
			}

			if insert.InsertCtx.AddAffectedRows {
				affectedRows := uint64(input.Batch.RowCount())
				atomic.AddUint64(&insert.ctr.affectedRows, affectedRows)
			}

			// write to s3.
			input.Batch.Attrs = append(input.Batch.Attrs[:0], insert.InsertCtx.Attrs...)
			if err = insert.acquireS3WriteMemory(proc, analyzer, input.Batch.Size()); err != nil {
				insert.ctr.state = vm.End
				return vm.CancelResult, err
			}
			err = insert.ctr.s3Writer.Write(proc.Ctx, input.Batch)
			if err != nil {
				insert.ctr.state = vm.End
				return vm.CancelResult, err
			}
		}
	}

	if insert.ctr.state == vm.Eval {
		writer := insert.ctr.s3Writer
		// handle the last Batch that batchSize less than DefaultBlockMaxRows
		// for more info, refer to the comments about reSizeBatch.
		//
		// data returned to the result would be raw data if it is a memory table.
		err := flushTailBatch(proc, writer, &result, analyzer, insert.isMemoryTable())
		if err != nil {
			insert.ctr.state = vm.End
			return result, err
		}
		insert.refreshAndReleaseS3MemGrant()
		insert.ctr.state = vm.End
		return result, nil
	}

	if insert.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")
}

type forceRefreshThrottler interface {
	ForceRefresh()
}

func (insert *Insert) acquireS3WriteMemory(proc *process.Process, analyzer process.Analyzer, size int) error {
	if insert.isMemoryTable() || insert.ctr.s3MemThrottler == nil || size <= 0 {
		return nil
	}

	ask := insert.s3WriteMemoryReservation(size) - insert.ctr.s3MemGranted
	if ask <= 0 {
		return nil
	}

	if insert.tryAcquireS3WriteMemory(ask) {
		return nil
	}

	forcedRefresh(insert.ctr.s3MemThrottler)
	if insert.tryAcquireS3WriteMemory(ask) {
		return nil
	}

	if err := insert.flushS3WriterOnMemoryPressure(proc, analyzer); err != nil {
		return err
	}

	ask = insert.s3WriteMemoryReservation(size) - insert.ctr.s3MemGranted
	if ask <= 0 || insert.tryAcquireS3WriteMemory(ask) {
		return nil
	}

	forcedRefresh(insert.ctr.s3MemThrottler)
	if insert.tryAcquireS3WriteMemory(ask) {
		return nil
	}
	return moerr.NewInternalErrorf(proc.Ctx,
		"CN S3 write memory is exhausted, available %d bytes, ask %d bytes",
		insert.ctr.s3MemThrottler.Available(), ask)
}

func (insert *Insert) s3WriteMemoryReservation(size int) int64 {
	reservation := insert.ctr.s3MemGranted + int64(size)
	if insert.ctr.s3MemNoThresholdCap {
		return reservation
	}
	if insert.ctr.s3Writer != nil {
		if threshold := int64(insert.ctr.s3Writer.MemorySizeThreshold()); threshold > 0 && reservation > threshold {
			reservation = threshold
		}
	} else if threshold := int64(colexec.WriteS3Threshold); reservation > threshold {
		reservation = int64(colexec.WriteS3Threshold)
	}
	return reservation
}

func (insert *Insert) tryAcquireS3WriteMemory(ask int64) bool {
	if _, ok := insert.ctr.s3MemThrottler.Acquire(ask); ok {
		insert.ctr.s3MemGranted += ask
		return true
	}
	return false
}

func forcedRefresh(throttler interface{ Refresh() }) {
	if refresher, ok := throttler.(forceRefreshThrottler); ok {
		refresher.ForceRefresh()
		return
	}
	throttler.Refresh()
}

func (insert *Insert) flushS3WriterOnMemoryPressure(proc *process.Process, analyzer process.Analyzer) (err error) {
	if insert.isMemoryTable() || insert.ctr.s3Writer == nil {
		insert.releaseS3MemGrant()
		return nil
	}
	defer func() {
		if err != nil {
			insert.refreshAndReleaseS3MemGrant()
		}
	}()

	releaseFlushSlot, err := acquireFlushSlot(proc.Ctx)
	if err != nil {
		return err
	}
	defer releaseFlushSlot()

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

	blockInfoBat, err := insert.ctr.s3Writer.SyncAndFillBlockInfoBat(newCtx)
	if err != nil {
		return err
	}

	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)

	if blockInfoBat != nil && blockInfoBat.RowCount() > 0 {
		insert.ctr.buf, err = insert.ctr.buf.Append(proc.Ctx, proc.GetMPool(), blockInfoBat)
		if err != nil {
			return err
		}
		insert.ctr.s3Writer.ResetBlockInfoBat()
	}

	insert.refreshAndReleaseS3MemGrant()
	return nil
}

func acquireFlushSlot(ctx context.Context) (func(), error) {
	timer := time.NewTimer(flushSemaphoreAcquireTimeout)
	defer timer.Stop()

	for {
		release, waitCh := flushLimiterState.tryAcquire()
		if release != nil {
			return release, nil
		}
		select {
		case <-waitCh:
		case <-timer.C:
			goto retry
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

retry:
	ticker := time.NewTicker(flushConcurrencyRefreshInterval)
	defer ticker.Stop()
	for {
		release, waitCh := flushLimiterState.tryAcquire()
		if release != nil {
			return release, nil
		}
		select {
		case <-waitCh:
		case <-ticker.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (insert *Insert) insert_table(proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	if !insert.delegated {
		input, err := vm.ChildrenCall(insert.GetChildren(0), proc, analyzer)
		if err != nil {
			return input, err
		}

		if input.Batch == nil || input.Batch.IsEmpty() {
			return input, nil
		}

		insert.input = input
	}

	input := insert.input
	affectedRows := uint64(input.Batch.RowCount())
	insert.ctr.buf.CleanOnlyData()
	for i := range insert.ctr.buf.Attrs {
		if insert.ctr.buf.Vecs[i] == nil {
			insert.ctr.buf.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
		}
		if err := insert.ctr.buf.Vecs[i].UnionBatch(input.Batch.Vecs[i], 0, input.Batch.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
			return input, err
		}
	}
	insert.ctr.buf.SetRowCount(input.Batch.RowCount())

	crs := analyzer.GetOpCounterSet()
	newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

	// insert into table, insertBat will be deeply copied into txn's workspace.
	err := insert.ctr.source.Write(newCtx, insert.ctr.buf)
	if err != nil {
		return input, err
	}
	analyzer.AddWrittenRows(int64(insert.ctr.buf.RowCount()))
	analyzer.AddS3RequestCount(crs)
	analyzer.AddFileServiceCacheInfo(crs)
	analyzer.AddDiskIO(crs)

	if insert.InsertCtx.AddAffectedRows {
		atomic.AddUint64(&insert.ctr.affectedRows, affectedRows)
	}
	// `insertBat` does not include partition expression columns
	return input, nil
}

func flushTailBatch(
	proc *process.Process,
	writer *colexec.CNS3Writer,
	result *vm.CallResult,
	analyzer process.Analyzer,
	isMemoryTable bool,
) error {

	var (
		err error
		bat *batch.Batch
	)

	if !isMemoryTable {
		crs := analyzer.GetOpCounterSet()

		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)

		if _, err = writer.Sync(newCtx); err != nil {
			return err
		}

		analyzer.AddS3RequestCount(crs)
		analyzer.AddFileServiceCacheInfo(crs)
		analyzer.AddDiskIO(crs)

		if bat, err = writer.FillBlockInfoBat(); err != nil {
			return err
		}

		result.Batch, err = result.Batch.Append(proc.Ctx, proc.GetMPool(), bat)
		if err != nil {
			return err
		}

		writer.ResetBlockInfoBat()

		return nil
	}

	return writer.OutputInMemoryData(result.Batch)
}
