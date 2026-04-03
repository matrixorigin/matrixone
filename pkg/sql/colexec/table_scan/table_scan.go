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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
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

		crs := analyzer.GetOpCounterSet()
		newCtx := perfcounter.AttachS3RequestKey(proc.Ctx, crs)
		isEnd, err := tableScan.Reader.Read(newCtx, tableScan.Attrs, nil, proc.Mp(), tableScan.ctr.buf)
		if err != nil {
			e = err
			return vm.CancelResult, err
		}
		analyzer.AddS3RequestCount(crs)
		analyzer.AddFileServiceCacheInfo(crs)
		analyzer.AddDiskIO(crs)
		analyzer.AddReadSizeInfo(crs)

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

		if tableScan.ctr.buf.IsEmpty() {
			continue
		}

		trace.GetService(proc.GetService()).TxnRead(
			proc.GetTxnOperator(),
			proc.GetTxnOperator().Txn().SnapshotTS,
			tableScan.TableID,
			tableScan.Attrs,
			tableScan.ctr.buf)

		// record storage I/O metrics before filtering
		analyzer.InputBlock()
		analyzer.ScanBytes(tableScan.ctr.buf)
		analyzer.Input(tableScan.ctr.buf) // record pre-filter rows for EXPLAIN ANALYZE inputRows
		batSize := tableScan.ctr.buf.Size()
		tableScan.ctr.maxAllocSize = max(tableScan.ctr.maxAllocSize, batSize)

		// inline filter evaluation: filter rows before returning to caller
		if len(tableScan.ctr.allFilterExecutors) > 0 {
			if err = tableScan.evalFilter(proc); err != nil {
				e = err
				return vm.CancelResult, err
			}
			if tableScan.ctr.buf.IsEmpty() {
				continue // all rows filtered out, read next block
			}
		}

		break
	}

	retBatch := tableScan.ctr.buf

	return vm.CallResult{Batch: retBatch, Status: vm.ExecNext}, nil
}

// evalFilter evaluates inline filter expressions on ctr.buf in-place.
// After evaluation, ctr.buf contains only the rows that pass all filters.
// If all rows are filtered out, ctr.buf is set to EmptyBatch.
func (tableScan *TableScan) evalFilter(proc *process.Process) error {
	bat := tableScan.ctr.buf
	analyzer := tableScan.OpAnalyzer
	var sels []int64

	for i := range tableScan.ctr.allFilterExecutors {
		if bat.IsEmpty() {
			break
		}

		vec, err := tableScan.ctr.allFilterExecutors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}

		if proc.OperatorOutofMemory(int64(vec.Size())) {
			return moerr.NewOOM(proc.Ctx)
		}
		analyzer.Alloc(int64(vec.Size()))

		if !vec.GetType().IsBoolean() {
			return moerr.NewInvalidInput(proc.Ctx, "filter condition is not boolean")
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
				bat.SetRowCount(0)
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
				bat.SetRowCount(0)
				break
			}

			if len(sels) != bat.RowCount() {
				bat.Shrink(sels, false)
			}
		}
	}

	if sels != nil {
		vector.PutSels(sels)
	}

	return nil
}
