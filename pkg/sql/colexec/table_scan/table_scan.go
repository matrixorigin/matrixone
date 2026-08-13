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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
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

	if tableScan.TopValueMsgTag > 0 {
		tableScan.ctr.msgReceiver = message.NewMessageReceiver([]int32{tableScan.TopValueMsgTag}, tableScan.GetAddress(), proc.GetMessageBoard())
	}

	if len(tableScan.ctr.filterExecutors) == 0 && len(tableScan.FilterExprs) > 0 {
		tableScan.ctr.filterExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableScan.FilterExprs)
		if err != nil {
			return
		}
	}

	// runtime filters are set fresh by handleRuntimeFilters before each Prepare call
	if len(tableScan.RuntimeFilterExprs) > 0 {
		tableScan.ctr.runtimeFilterExecutors, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableScan.RuntimeFilterExprs)
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
	if len(tableScan.ctr.earlyColumns) > 0 {
		tableScan.ctr.readerFilter = func(
			bat *batch.Batch,
			loadedColumns []int,
		) (engine.ReaderFilterResult, error) {
			return tableScan.applyReaderFilter(proc, bat, loadedColumns)
		}
	} else {
		tableScan.ctr.readerFilter = nil
	}

	err = tableScan.PrepareProjection(proc)
	if tableScan.ctr.buf == nil {
		tableScan.ctr.buf = batch.NewOffHeapWithSize(len(tableScan.Types))
		tableScan.ctr.buf.Attrs = append(tableScan.ctr.buf.Attrs, tableScan.Attrs...)
		for i := range tableScan.Types {
			tableScan.ctr.buf.Vecs[i] = vector.NewOffHeapVecWithType(plan.MakeTypeByPlan2Type(tableScan.Types[i]))
		}
	}
	return
}

func (tableScan *TableScan) Call(proc *process.Process) (vm.CallResult, error) {
	var e error
	start := time.Now()
	txnOp := proc.GetTxnOperator()
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
	}

	trace.GetService(proc.GetService()).AddTxnDurationAction(
		txnOp,
		client.TableScanEvent,
		seq,
		tableScan.TableID,
		0,
		nil)

	analyzer := tableScan.OpAnalyzer
	defer func() {
		cost := time.Since(start)

		trace.GetService(proc.GetService()).AddTxnDurationAction(
			txnOp,
			client.TableScanEvent,
			seq,
			tableScan.TableID,
			cost,
			e)
		v2.TxnStatementScanDurationHistogram.Observe(cost.Seconds())
	}()

	if err, isCancel := vm.CancelCheck(proc); isCancel {
		e = err
		return vm.CancelResult, err
	}

	for {
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
		isEnd, err := tableScan.readBatch(newCtx, proc)
		if err != nil {
			e = err
			return vm.CancelResult, err
		}

		// Record ReadSize metrics when scan completes all blocks (isEnd == true)
		// This matches explain analyze output which shows total read size for the entire scan
		if isEnd {
			if analyzer != nil {
				opStats := analyzer.GetOpStats()
				// GetOpStats() will panic if opStats is nil, so we can safely access it here
				v2.TxnReadSizeHistogram.Observe(float64(opStats.ReadSize))
				v2.TxnS3ReadSizeHistogram.Observe(float64(opStats.S3ReadSize))
				v2.TxnDiskReadSizeHistogram.Observe(float64(opStats.DiskReadSize))
			}
			e = err
			return vm.CancelResult, err
		}

		if tableScan.ctr.filterReadMetrics {
			if tableScan.ctr.filterLateMaterialized && !tableScan.ctr.buf.IsEmpty() {
				tableScan.recordLateInput(tableScan.ctr.buf)
			}
			if tableScan.ctr.buf.IsEmpty() {
				continue
			}
			if tableScan.ctr.filterLateMaterialized {
				tableScan.traceRead(proc, tableScan.ctr.buf)
			}
		} else {
			if tableScan.ctr.buf.IsEmpty() {
				continue
			}
			tableScan.traceRead(proc, tableScan.ctr.buf)

			// record storage I/O metrics before filtering
			analyzer.InputBlock()
			analyzer.ScanBytes(tableScan.ctr.buf)
			analyzer.Input(tableScan.ctr.buf) // record pre-filter rows for EXPLAIN ANALYZE inputRows
			batSize := tableScan.ctr.buf.Size()
			tableScan.ctr.maxAllocSize = max(tableScan.ctr.maxAllocSize, batSize)

			// inline filter evaluation: filter rows before returning to caller
			if len(tableScan.ctr.allFilterExecutors) > 0 {
				if _, err = tableScan.evalFilter(proc, tableScan.ctr.buf, nil); err != nil {
					e = err
					return vm.CancelResult, err
				}
				if tableScan.ctr.buf.IsEmpty() {
					continue // all rows filtered out, read next block
				}
			}
		}

		if tableScan.ctr.padCharToFullLength {
			if err = padCharColumnsToFullLength(tableScan.ctr.buf, proc); err != nil {
				e = err
				return vm.CancelResult, err
			}
		}

		break
	}

	retBatch := tableScan.ctr.buf

	return vm.CallResult{Batch: retBatch, Status: vm.ExecNext}, nil
}

func padCharColumnsToFullLength(bat *batch.Batch, proc *process.Process) error {
	for _, vec := range bat.Vecs {
		if vec == nil || vec.GetType().Oid != types.T_char || vec.IsConstNull() {
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
			if err := vector.SetBytesAt(vec, row, padded, proc.Mp()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tableScan *TableScan) traceRead(proc *process.Process, bat *batch.Batch) {
	trace.GetService(proc.GetService()).TxnRead(
		proc.GetTxnOperator(),
		proc.GetTxnOperator().Txn().SnapshotTS,
		tableScan.TableID,
		tableScan.Attrs,
		bat,
	)
}

// shrinkPopulatedVecs applies the surviving-row selection to every vector the reader
// actually materialized, and skips any it left empty.
//
// batch.Shrink walks all of bat.Vecs and assumes each holds bat.RowCount() rows. The
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
func shrinkPopulatedVecs(bat *batch.Batch, sels []int64) {
	if len(sels) == bat.RowCount() {
		return
	}
	rowCount := bat.RowCount()
	for _, vec := range bat.Vecs {
		if vec == nil || (vec.Length() == 0 && rowCount != 0) {
			continue
		}
		vec.Shrink(sels, false)
	}
	bat.SetRowCount(len(sels))
}

// evalFilter evaluates inline filter expressions in-place. loadedColumns is
// nil for an eager batch; otherwise it identifies the vectors currently
// present in a late-materialized full-schema batch.
func (tableScan *TableScan) evalFilter(
	proc *process.Process,
	bat *batch.Batch,
	loadedColumns []int,
) (engine.ReaderFilterResult, error) {
	if tableScan.ctr.padCharToFullLength {
		if err := padCharColumnsToFullLength(bat, proc); err != nil {
			return engine.ReaderFilterResult{}, err
		}
	}

	analyzer := tableScan.OpAnalyzer
	var sels []int64
	defer func() {
		if sels != nil {
			vector.PutSels(sels)
		}
	}()

	tableScan.ctr.filterRows = tableScan.ctr.filterRows[:0]
	hasSelection := false
	trackOriginalRows := loadedColumns != nil

	for i := range tableScan.ctr.allFilterExecutors {
		if bat.IsEmpty() {
			break
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

		if tableScan.ctr.filterBs == nil {
			tableScan.ctr.filterBs = vector.GenerateFunctionFixedTypeParameter[bool](vec)
		} else {
			ok := vector.ReuseFunctionFixedTypeParameter[bool](vec, tableScan.ctr.filterBs)
			if !ok {
				tableScan.ctr.filterBs = vector.GenerateFunctionFixedTypeParameter[bool](vec)
			}
		}
		bs := tableScan.ctr.filterBs

		if vec.IsConst() {
			v, null := bs.GetValue(0)
			if null || !v {
				tableScan.clearLoadedFilterColumns(bat, loadedColumns)
				tableScan.ctr.filterRows = tableScan.ctr.filterRows[:0]
				hasSelection = true
				break
			}
		} else {
			if sels == nil {
				sels = vector.GetSels()
			}
			sels = sels[:0]

			l := uint64(vec.Length())
			if bs.WithAnyNullValue() {
				for j := uint64(0); j < l; j++ {
					v, null := bs.GetValue(j)
					if !null && v {
						sels = append(sels, int64(j))
					}
				}
			} else {
				for j := uint64(0); j < l; j++ {
					v, _ := bs.GetValue(j)
					if v {
						sels = append(sels, int64(j))
					}
				}
			}

			if len(sels) == 0 {
				tableScan.clearLoadedFilterColumns(bat, loadedColumns)
				tableScan.ctr.filterRows = tableScan.ctr.filterRows[:0]
				hasSelection = true
				break
			}

			if len(sels) != bat.RowCount() {
				if trackOriginalRows {
					if !hasSelection {
						tableScan.ctr.filterRows = append(tableScan.ctr.filterRows, sels...)
						hasSelection = true
					} else {
						for j, row := range sels {
							tableScan.ctr.filterRows[j] = tableScan.ctr.filterRows[row]
						}
						tableScan.ctr.filterRows = tableScan.ctr.filterRows[:len(sels)]
					}
				}

				if loadedColumns == nil {
					shrinkPopulatedVecs(bat, sels)
				} else {
					for _, pos := range loadedColumns {
						bat.Vecs[pos].Shrink(sels, false)
					}
					bat.SetRowCount(len(sels))
				}
			}
		}
	}

	if trackOriginalRows && hasSelection {
		return engine.ReaderFilterResult{Sels: tableScan.ctr.filterRows}, nil
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
