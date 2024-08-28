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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/message"

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

func (tableScan *TableScan) OpType() vm.OpType {
	return vm.TableScan
}

func (tableScan *TableScan) Prepare(proc *process.Process) (err error) {
	if tableScan.TopValueMsgTag > 0 {
		tableScan.ctr.msgReceiver = message.NewMessageReceiver([]int32{tableScan.TopValueMsgTag}, tableScan.GetAddress(), proc.GetMessageBoard())
	}
	err = tableScan.PrepareProjection(proc)
	if tableScan.ctr.buf == nil {
		tableScan.ctr.buf = batch.NewWithSize(len(tableScan.Types))
		tableScan.ctr.buf.Attrs = append(tableScan.ctr.buf.Attrs, tableScan.Attrs...)
		for i := range tableScan.Types {
			tableScan.ctr.buf.Vecs[i] = vector.NewVec(plan.MakeTypeByPlan2Type(tableScan.Types[i]))
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

	anal := proc.GetAnalyze(tableScan.GetIdx(), tableScan.GetParallelIdx(), tableScan.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()

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
			msgs, _ := tableScan.ctr.msgReceiver.ReceiveMessage(false, proc.Ctx)
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
		isEnd, err := tableScan.Reader.Read(proc.Ctx, tableScan.Attrs, nil, proc.Mp(), proc, tableScan.ctr.buf)
		if err != nil {
			e = err
			return vm.CancelResult, err
		}

		if isEnd {
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

		anal.InputBlock()
		anal.S3IOByte(tableScan.ctr.buf)
		batSize := tableScan.ctr.buf.Size()
		tableScan.ctr.maxAllocSize = max(tableScan.ctr.maxAllocSize, batSize)
		break
	}

	anal.Input(tableScan.ctr.buf, tableScan.IsFirst)
	retBatch, err := tableScan.EvalProjection(tableScan.ctr.buf, proc)
	if err != nil {
		e = err
		return vm.CancelResult, err
	}
	anal.Output(retBatch, tableScan.IsLast)
	return vm.CallResult{Batch: retBatch, Status: vm.ExecNext}, nil

}
