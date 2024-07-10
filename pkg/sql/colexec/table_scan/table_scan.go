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

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "table_scan"

func (tableScan *TableScan) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": table_scan ")
}

func (tableScan *TableScan) Prepare(proc *process.Process) (err error) {
	tableScan.ctr = new(container)
	tableScan.ctr.orderBy = tableScan.Reader.GetOrderBy()
	if tableScan.TopValueMsgTag > 0 {
		tableScan.ctr.msgReceiver = proc.NewMessageReceiver([]int32{tableScan.TopValueMsgTag}, tableScan.GetAddress())
	}
	return nil
}

func (tableScan *TableScan) Call(proc *process.Process) (vm.CallResult, error) {
	var e error
	start := time.Now()
	txnOp := proc.GetTxnOperator()
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
	}

	trace.GetService().AddTxnDurationAction(
		txnOp,
		client.TableScanEvent,
		seq,
		tableScan.TableID,
		0,
		nil)

	anal := proc.GetAnalyze(tableScan.GetIdx(), tableScan.GetParallelIdx(), tableScan.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()

		cost := time.Since(start)

		trace.GetService().AddTxnDurationAction(
			txnOp,
			client.TableScanEvent,
			seq,
			tableScan.TableID,
			cost,
			e)
		v2.TxnStatementScanDurationHistogram.Observe(cost.Seconds())
	}()

	result := vm.NewCallResult()
	//select {
	//case <-proc.Ctx.Done():
	//	result.Batch = nil
	//	result.Status = vm.ExecStop
	//	return result, proc.Ctx.Err()
	//default:
	//}
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		e = err
		return vm.CancelResult, err
	}

	if tableScan.ctr.buf != nil {
		proc.PutBatch(tableScan.ctr.buf)
		tableScan.ctr.buf = nil
	}

	for {
		// receive topvalue message
		if tableScan.ctr.msgReceiver != nil {
			msgs, _ := tableScan.ctr.msgReceiver.ReceiveMessage(false, proc.Ctx)
			for i := range msgs {
				msg, ok := msgs[i].(process.TopValueMessage)
				if !ok {
					panic("only support top value message in table scan!")
				}
				tableScan.Reader.SetFilterZM(msg.TopValueZM)
			}
		}
		// read data from storage engine
		bat, err := tableScan.Reader.Read(proc.Ctx, tableScan.Attrs, nil, proc.Mp(), proc)
		if err != nil {
			result.Status = vm.ExecStop
			e = err
			return result, err
		}

		if bat == nil {
			result.Status = vm.ExecStop
			e = err
			return result, err
		}

		if bat.IsEmpty() {
			continue
		}

		trace.GetService().TxnRead(
			proc.GetTxnOperator(),
			proc.GetTxnOperator().Txn().SnapshotTS,
			tableScan.TableID,
			tableScan.Attrs,
			bat)

		bat.Cnt = 1
		anal.InputBlock()
		anal.S3IOByte(bat)
		batSize := bat.Size()
		tableScan.ctr.maxAllocSize = max(tableScan.ctr.maxAllocSize, batSize)

		tableScan.ctr.buf = bat
		break
	}

	result.Batch = tableScan.ctr.buf
	return result, nil
}
