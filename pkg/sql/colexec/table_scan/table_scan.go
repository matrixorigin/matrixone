// Copyright 2021-2023 Matrix Origin
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

package table_scan

import (
	"bytes"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "table_scan"

func (tableScan *TableScan) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": table_scan ")
}

func (tableScan *TableScan) OpType() vm.OpType {
	return vm.TableScan
}

func (tableScan *TableScan) Prepare(proc *process.Process) (err error) {
	if tableScan.OpAnalyzer == nil {
		tableScan.OpAnalyzer = process.NewAnalyzer(tableScan.GetIdx(), tableScan.IsFirst, tableScan.IsLast, "table_scan")
	} else {
		tableScan.OpAnalyzer.Reset()
	}
	if tableScan.ctr.padCharToFullLength, err = process.ResolvePadCharToFullLength(proc); err != nil {
		return err
	}
	if err = tableScan.bindOutputAllocation(proc); err != nil {
		return err
	}

	if tableScan.TopValueMsgTag > 0 {
		tableScan.ctr.msgReceiver = message.NewMessageReceiver([]int32{tableScan.TopValueMsgTag}, tableScan.GetAddress(), proc.GetMessageBoard())
	}

	if len(tableScan.ctr.filterExecutors) == 0 && len(tableScan.FilterExprs) > 0 {
		tableScan.ctr.filterExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(proc, tableScan.FilterExprs, tableScan.OutputAllocation)
		if err != nil {
			return
		}
		tableScan.ctr.subjectReuseAdmissionDone = false
	}

	// runtime filters are set fresh by handleRuntimeFilters before each Prepare call
	if len(tableScan.RuntimeFilterExprs) > 0 {
		tableScan.ctr.runtimeFilterExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(proc, tableScan.RuntimeFilterExprs, tableScan.OutputAllocation)
		if err != nil {
			return
		}
	}

	// build allFilterExecutors: runtime first (usually more selective), then static
	if tableScan.ctr.allFilterExecutors == nil {
		tableScan.ctr.allFilterExecutors = make([]colexec.ExpressionExecutor, 0,
			len(tableScan.ctr.runtimeFilterExecutors)+len(tableScan.ctr.filterExecutors))
	} else {
		tableScan.ctr.allFilterExecutors = tableScan.ctr.allFilterExecutors[:0]
	}
	tableScan.ctr.allFilterExecutors = append(tableScan.ctr.allFilterExecutors, tableScan.ctr.runtimeFilterExecutors...)
	tableScan.ctr.allFilterExecutors = append(tableScan.ctr.allFilterExecutors, tableScan.ctr.filterExecutors...)
	tableScan.configureLateMaterialization()
	tableScan.prepareReaderFilter(proc)

	err = tableScan.PrepareProjectionWithAllocation(proc, tableScan.OutputAllocation)
	if err != nil {
		return err
	}
	if tableScan.ctr.buf == nil {
		if err = tableScan.prepareScanMP(proc); err != nil {
			return err
		}
		tableScan.ctr.buf = batch.NewWithSize(len(tableScan.Types))
		tableScan.ctr.buf.Attrs = append(tableScan.ctr.buf.Attrs, tableScan.Attrs...)
		for i := range tableScan.Types {
			tableScan.ctr.buf.Vecs[i] = vector.NewVec(plan.MakeTypeByPlan2Type(tableScan.Types[i]), tableScan.mp(proc))
		}
	}
	err = tableScan.prepareLocalDomains(proc)
	return
}

func (tableScan *TableScan) Call(proc *process.Process) (vm.CallResult, error) {
	start := time.Now()

	analyzer := tableScan.OpAnalyzer
	defer func() {
		cost := time.Since(start)

		v2.TxnStatementScanDurationHistogram.Observe(cost.Seconds())
	}()

	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}
	if len(tableScan.ctr.domainProbes) > 0 {
		if err := tableScan.waitLocalDomains(proc); err != nil {
			return vm.CancelResult, err
		}
		if tableScan.ctr.domainEmpty {
			return vm.CancelResult, nil
		}
		if !tableScan.ctr.membershipPrepared {
			tableScan.prepareReaderMembership(proc)
			tableScan.ctr.membershipPrepared = true
		}
	}

	for {
		tableScan.ctr.membershipApplied = false
		// receive topvalue message
		if tableScan.ctr.msgReceiver != nil {
			msgs, _, _ := tableScan.ctr.msgReceiver.ReceiveMessage(false, proc.Ctx)
			for i := range msgs {
				msg, ok := msgs[i].(message.TopValueMessage)
				if !ok {
					panic("only support top value message in table scan!")
				}
				tableScan.Reader.SetFilterZM(msg.TopValueZM)
			}
		}

		// read data from storage engine
		tableScan.ctr.buf.CleanOnlyData()
		tableScan.ctr.filterReadMetrics = false
		tableScan.ctr.filterLateMaterialized = false
		tableScan.ctr.filterActiveDuration = 0

		crs := analyzer.GetOpCounterSet()
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
		if tableScan.ReadPolicy != 0 {
			newCtx = fileservice.WithFileServicePolicy(newCtx, fileservice.GetFileServicePolicy(newCtx)|tableScan.ReadPolicy)
			newCtx = fileservice.WithPreloadGroup(newCtx, tableScan.TableID)
		}
		isEnd, err := tableScan.readBatch(newCtx, proc)
		if err != nil {
			return vm.CancelResult, err
		}
		colexec.CollectReaderExplainDiagnostics(tableScan.Reader, analyzer)
		// Account this read's late columns before retirement restores the
		// ordinary schedule, then retire before any empty-batch shortcut.
		if !isEnd && tableScan.ctr.filterReadMetrics && tableScan.ctr.filterLateMaterialized && !tableScan.ctr.buf.IsEmpty() {
			tableScan.recordLateInput(tableScan.ctr.buf)
		}
		tableScan.retireReaderMembership(proc)

		// Record ReadSize metrics when scan completes all blocks (isEnd == true)
		// This matches explain analyze output which shows total read size for the entire scan
		if isEnd {
			for _, source := range tableScan.ctr.domainSources {
				source.producer.Complete(false)
			}
			if analyzer != nil {
				opStats := analyzer.GetOpStats()
				// GetOpStats() will panic if opStats is nil, so we can safely access it here
				v2.TxnReadSizeHistogram.Observe(float64(opStats.ReadSize))
				v2.TxnS3ReadSizeHistogram.Observe(float64(opStats.S3ReadSize))
				v2.TxnDiskReadSizeHistogram.Observe(float64(opStats.DiskReadSize))
			}

			return vm.CancelResult, err
		}

		if tableScan.ctr.filterReadMetrics {
			if tableScan.ctr.buf.IsEmpty() {
				continue
			}
		} else {
			if tableScan.ctr.buf.IsEmpty() {
				continue
			}

			// record storage I/O metrics before filtering
			analyzer.InputBlock()
			analyzer.ScanBytes(tableScan.ctr.buf)
			analyzer.Input(tableScan.ctr.buf) // record pre-filter rows for EXPLAIN ANALYZE inputRows
			batSize := tableScan.ctr.buf.Size()
			tableScan.ctr.maxAllocSize = max(tableScan.ctr.maxAllocSize, batSize)

			// inline filter evaluation: filter rows before returning to caller
			if len(tableScan.ctr.allFilterExecutors) > 0 {
				if _, err = tableScan.evalFilter(proc, tableScan.ctr.buf, nil); err != nil {
					return vm.CancelResult, err
				}
				if tableScan.ctr.buf.IsEmpty() {
					continue // all rows filtered out, read next block
				}
			}

		}

		if tableScan.ctr.padCharToFullLength {
			if err = padCharColumnsToFullLength(tableScan.ctr.buf, tableScan.mp(proc)); err != nil {
				return vm.CancelResult, err
			}
		}

		if len(tableScan.ctr.domainSources) > 0 || len(tableScan.ctr.domainProbes) > 0 {
			if err = tableScan.applyLocalDomains(proc); err != nil {
				return vm.CancelResult, err
			}
			if tableScan.ctr.buf.IsEmpty() {
				continue
			}
		}
		break
	}

	retBatch := tableScan.ctr.buf

	return vm.CallResult{Batch: retBatch, Status: vm.ExecNext}, nil
}

func padCharColumnsToFullLength(bat *batch.Batch, mp *mpool.MPool) error {
	for _, vec := range bat.Vecs {
		if vec == nil || vec.GetType().Oid != types.T_char || vec.IsConstNull() {
			continue
		}
		// A late-materialized filter sees the full schema but only its early
		// columns have values. Leave unloaded CHAR vectors for the post-read
		// padding pass, after the reader has materialized them.
		if bat.RowCount() != 0 && vec.Length() == 0 {
			continue
		}

		width := int(vec.GetType().Width)
		if width <= 0 {
			continue
		}
		rowCount := bat.RowCount()
		if vec.IsConst() {
			rowCount = 1
		}
		for row := 0; row < rowCount; row++ {
			if vec.GetNulls().Contains(uint64(row)) {
				continue
			}
			value := vec.GetBytesAt(row)
			missing := width - utf8.RuneCount(value)
			if missing <= 0 {
				continue
			}
			padded := make([]byte, len(value)+missing)
			copy(padded, value)
			for i := len(value); i < len(padded); i++ {
				padded[i] = ' '
			}
			if err := vector.SetBytesAt(vec, row, padded, mp); err != nil {
				return err
			}
		}
	}
	return nil
}

// shrinkPopulatedVecs applies the surviving-row selection to every vector the reader
// actually materialized, and skips any it left empty.
//
// batch.ShrinkByMask walks all of bat.Vecs and assumes each holds bat.RowCount() rows. The
// vector-TopN pushdown breaks that: blockio.materializeVectorTopNRows loads only the rows
// the in-block Top-K selected and then CleanOnlyData()s the ORDER BY vector column itself
// (outputBat.Vecs[topColPos]) — its values were already consumed to compute the distances,
// so it is deliberately left with 0 rows while its siblings hold len(selectRows). The
// reader still hands that batch to the filter callback as fully loaded (readutil/reader.go
// passes loadedColumns=nil whenever r.orderByLimit != nil, which also disables late
// materialization), so shrinking by row index reached the empty column and panicked with
// "index out of range [N] with length 0", killing the query. Reproduced on an IVF-FLAT
// include-mode entries scan: ORDER BY vec_dist LIMIT 100 plus a predicate on an INCLUDE
// column.
//
// Skipping is the only safe local action: a vector with no rows has nothing to realign,
// and shrinking it is precisely the crash. It cannot mask a real misalignment either --
// any vector the reader DID fill is shrunk, so every populated column stays consistent
// with the row count set below.
// shrinkPopulatedVecsByMask is shrinkPopulatedVecs for a selection bitmap.
func shrinkPopulatedVecsByMask(bat *batch.Batch, sels *bitmap.Bitmap, mp *mpool.MPool) error {
	kept := sels.Count()
	if kept == bat.RowCount() {
		return nil
	}
	rowCount := bat.RowCount()
	for _, vec := range bat.Vecs {
		if vec == nil || (vec.Length() == 0 && rowCount != 0) {
			continue
		}
		if err := vec.ShrinkByMask(sels, false, 0, mp); err != nil {
			return err
		}
	}
	bat.SetRowCount(kept)
	return nil
}

func shrinkPopulatedVecs(bat *batch.Batch, sels []int64, mp *mpool.MPool) error {
	if len(sels) == bat.RowCount() {
		return nil
	}
	rowCount := bat.RowCount()
	for _, vec := range bat.Vecs {
		if vec == nil || (vec.Length() == 0 && rowCount != 0) {
			continue
		}
		if err := vec.ShrinkByRows(sels, mp); err != nil {
			return err
		}
	}
	bat.SetRowCount(len(sels))
	return nil
}

// evalFilter evaluates inline filter expressions in-place. loadedColumns is
// nil for an eager batch; otherwise it identifies the vectors currently
// present in a late-materialized full-schema batch.
func (tableScan *TableScan) evalFilter(
	proc *process.Process,
	bat *batch.Batch,
	loadedColumns []int,
) (engine.ReaderFilterResult, error) {
	firstInvocation := false
	if !bat.IsEmpty() {
		firstInvocation = !tableScan.ctr.subjectReuseAdmissionDone
		tableScan.ctr.subjectReuseAdmissionDone = true
	}
	if tableScan.ctr.padCharToFullLength {
		if err := padCharColumnsToFullLength(bat, tableScan.mp(proc)); err != nil {
			return engine.ReaderFilterResult{}, err
		}
	}

	analyzer := tableScan.OpAnalyzer
	s := &tableScan.ctr.selection
	// The selection bitmaps are the operator's scratch, not scan output:
	// they come from the process pool. Shrinking the output uses its pool.
	mp := proc.Mp()
	outMP := tableScan.mp(proc)
	for _, bm := range []*bitmap.Bitmap{&s.keep, &s.pred, &s.dep} {
		if bm.MPool() != mp {
			bm.Free()
			bm.Init(mp)
		}
	}
	// keep tracks the surviving rows of the input as loaded, which the
	// reader needs to align its late columns; pred holds one predicate's
	// TRUE rows over the rows still present in bat.
	keep, pred := &s.keep, &s.pred
	inputRows := bat.RowCount()
	selected := false
	trackOriginalRows := loadedColumns != nil
	none := func() (engine.ReaderFilterResult, error) {
		tableScan.clearLoadedFilterColumns(bat, loadedColumns)
		if !trackOriginalRows {
			return engine.ReaderFilterResult{}, nil
		}
		if err := keep.InitWithSize(int64(inputRows), mp); err != nil {
			return engine.ReaderFilterResult{}, err
		}
		return engine.ReaderFilterResult{Mask: keep}, nil
	}

	for i := range tableScan.ctr.allFilterExecutors {
		if bat.IsEmpty() {
			break
		}
		if firstInvocation && i >= len(tableScan.ctr.runtimeFilterExecutors) {
			staticIndex := i - len(tableScan.ctr.runtimeFilterExecutors)
			executor := colexec.ReuseFilterExpressionExecutor(tableScan.FilterExprs[staticIndex], tableScan.ctr.filterExecutors[staticIndex], bat.RowCount())
			tableScan.ctr.filterExecutors[staticIndex] = executor
			tableScan.ctr.allFilterExecutors[i] = executor
		}

		vec, err := tableScan.ctr.allFilterExecutors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return engine.ReaderFilterResult{}, err
		}

		if proc.OperatorOutofMemory(int64(vec.Size())) {
			return engine.ReaderFilterResult{}, moerr.NewOOM(proc.Ctx)
		}
		analyzer.Alloc(int64(vec.Size()))

		if !vec.GetType().IsBoolean() {
			return engine.ReaderFilterResult{}, moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
		}

		if vec.IsConst() {
			if vec.IsConstNull() || !vector.GetFixedAtNoTypeCheck[bool](vec, 0) {
				return none()
			}
			continue
		}
		count := bat.RowCount()
		if vec.Length() < count {
			return engine.ReaderFilterResult{}, moerr.NewInternalErrorf(proc.Ctx,
				"filter condition has %d rows for %d input rows", vec.Length(), count)
		}
		// The TRUE, non-NULL rows, built a word at a time.
		values := vector.MustFixedColNoTypeCheck[bool](vec)[:count]
		var nulls *bitmap.Bitmap
		if nsp := vec.GetNulls(); !nsp.IsEmpty() {
			nulls = nsp.GetBitmap()
		}
		if err = pred.InitWithBools(values, nulls, mp); err != nil {
			return engine.ReaderFilterResult{}, err
		}
		if pred.Count() == count {
			continue
		}
		if pred.IsEmpty() {
			return none()
		}
		if trackOriginalRows {
			if !selected {
				if err = keep.InitWith(pred, mp); err != nil {
					return engine.ReaderFilterResult{}, err
				}
			} else {
				// pred covers the kept rows only: keep the kept row of rank
				// k when the predicate holds for row k.
				if err = s.dep.InitDeposit(keep, pred, mp); err != nil {
					return engine.ReaderFilterResult{}, err
				}
				keep.Swap(&s.dep)
			}
		}
		selected = true

		if loadedColumns == nil {
			if err := shrinkPopulatedVecsByMask(bat, pred, outMP); err != nil {
				return engine.ReaderFilterResult{}, err
			}
		} else {
			for _, pos := range loadedColumns {
				if err := bat.Vecs[pos].ShrinkByMask(pred, false, 0, outMP); err != nil {
					return engine.ReaderFilterResult{}, err
				}
			}
			bat.SetRowCount(pred.Count())
		}
	}

	if trackOriginalRows && selected {
		return engine.ReaderFilterResult{Mask: keep}, nil
	}
	if !trackOriginalRows {
		return engine.ReaderFilterResult{}, nil
	}
	return engine.ReaderFilterResult{All: true}, nil
}

func (tableScan *TableScan) clearLoadedFilterColumns(
	bat *batch.Batch,
	loadedColumns []int,
) {
	for _, pos := range loadedColumns {
		bat.Vecs[pos].CleanOnlyData()
	}
	bat.SetRowCount(0)
}
