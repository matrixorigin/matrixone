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

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "table_scan"
const maxBatchMemSize = colexec.DefaultBatchSize * 1024

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": table_scan ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.OrderBy = arg.Reader.GetOrderBy()
	if arg.TopValueMsgTag > 0 {
		arg.msgReceiver = proc.NewMessageReceiver([]int32{arg.TopValueMsgTag}, arg.GetAddress())
	}
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	var e error
	start := time.Now()
	txnOp := proc.TxnOperator
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
	}

	trace.GetService().AddTxnDurationAction(
		txnOp,
		client.TableScanEvent,
		seq,
		arg.TableID,
		0,
		nil)

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()

		cost := time.Since(start)

		trace.GetService().AddTxnDurationAction(
			txnOp,
			client.TableScanEvent,
			seq,
			arg.TableID,
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

	if arg.buf != nil {
		proc.PutBatch(arg.buf)
	}

	for {
		// receive topvalue message
		if arg.msgReceiver != nil {
			msgs, _ := arg.msgReceiver.ReceiveMessage(false, proc.Ctx)
			for i := range msgs {
				msg, ok := msgs[i].(process.TopValueMessage)
				if !ok {
					panic("only support top value message in table scan!")
				}
				arg.Reader.SetFilterZM(msg.TopValueZM)
			}
		}
		// read data from storage engine
		bat, err := arg.Reader.Read(proc.Ctx, arg.Attrs, nil, proc.Mp(), proc)
		if err != nil {
			result.Status = vm.ExecStop
			e = err
			return result, err
		}

		if bat == nil {
			if arg.tmpBuf != nil {
				arg.buf = arg.tmpBuf
				arg.tmpBuf = nil
				break
			} else {
				result.Status = vm.ExecStop
				e = err
				return result, err
			}
		}

		if bat.IsEmpty() {
			continue
		}

		trace.GetService().TxnRead(
			proc.TxnOperator,
			proc.TxnOperator.Txn().SnapshotTS,
			arg.TableID,
			arg.Attrs,
			bat)

		bat.Cnt = 1
		anal.S3IOByte(bat)
		batSize := bat.Size()
		arg.maxAllocSize = max(arg.maxAllocSize, batSize)

		if arg.tmpBuf == nil {
			arg.tmpBuf = bat
			continue
		}

		tmpSize := arg.tmpBuf.Size()
		if arg.tmpBuf.RowCount()+bat.RowCount() < colexec.DefaultBatchSize && tmpSize+batSize < maxBatchMemSize {
			_, err := arg.tmpBuf.Append(proc.Ctx, proc.GetMPool(), bat)
			proc.PutBatch(bat)
			if err != nil {
				e = err
				return result, err
			}
			continue
		}

		arg.buf = arg.tmpBuf
		arg.tmpBuf = bat
		break
	}

	result.Batch = arg.buf
	return result, nil
}
