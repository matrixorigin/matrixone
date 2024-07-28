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

package loopjoin

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_join"

func (loopJoin *LoopJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": loop join ")
}

func (loopJoin *LoopJoin) OpType() vm.OpType {
	return vm.LoopJoin
}

func (loopJoin *LoopJoin) Prepare(proc *process.Process) error {
	var err error

	loopJoin.ctr = new(container)
	loopJoin.ctr.InitReceiver(proc, true)

	if loopJoin.Cond != nil {
		loopJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopJoin.Cond)
	}
	return err
}

func (loopJoin *LoopJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(loopJoin.GetIdx(), loopJoin.GetParallelIdx(), loopJoin.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := loopJoin.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err := loopJoin.build(proc, anal); err != nil {
				return result, err
			}
			if ctr.bat == nil {
				// for inner ,right and semi join, if hashmap is empty, we can finish this pipeline
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			if ctr.inBat != nil {
				err = ctr.probe(loopJoin, proc, anal, loopJoin.GetIsLast(), &result)
				return result, err
			}
			msg := ctr.ReceiveFromAllRegs(anal)
			if msg.Err != nil {
				return result, msg.Err
			}
			ctr.inBat = msg.Batch
			if ctr.inBat == nil {
				ctr.state = End
				continue
			}
			if ctr.inBat.IsEmpty() {
				proc.PutBatch(ctr.inBat)
				ctr.inBat = nil
				continue
			}
			if ctr.bat == nil || ctr.bat.RowCount() == 0 {
				proc.PutBatch(ctr.inBat)
				ctr.inBat = nil
				continue
			}
			anal.Input(ctr.inBat, loopJoin.GetIsFirst())
			err = ctr.probe(loopJoin, proc, anal, loopJoin.GetIsLast(), &result)
			return result, err
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopJoin *LoopJoin) build(proc *process.Process, anal process.Analyze) error {
	ctr := loopJoin.ctr
	mp := proc.ReceiveJoinMap(anal, loopJoin.JoinMapTag, false, 0)
	if mp == nil {
		return nil
	}
	batches := mp.GetBatches()
	var err error
	//maybe optimize this in the future
	for i := range batches {
		ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), batches[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) probe(ap *LoopJoin, proc *process.Process, anal process.Analyze, isLast bool, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.inBat.Vecs[rp.Pos].GetType())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.bat.Vecs[rp.Pos].GetType())
		}
	}
	count := ctr.inBat.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(ctr.inBat, proc.Mp())
	}

	rowCountIncrease := 0
	for i := ctr.probeIdx; i < count; i++ {
		if err := colexec.SetJoinBatchValues(ctr.joinBat, ctr.inBat, int64(i),
			ctr.bat.RowCount(), ctr.cfs); err != nil {
			return err
		}
		vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, ctr.bat}, nil)
		if err != nil {
			return err
		}

		rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
		if vec.IsConst() {
			b, null := rs.GetValue(0)
			if !null && b {
				for j := 0; j < ctr.bat.RowCount(); j++ {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err = ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err = ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease++
				}
			}
		} else {
			l := uint64(ctr.bat.RowCount())
			for j := uint64(0); j < l; j++ {
				b, null := rs.GetValue(j)
				if !null && b {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err = ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err = ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease++
				}
			}
		}
		if rowCountIncrease >= colexec.DefaultBatchSize {
			anal.Output(ctr.rbat, isLast)
			result.Batch = ctr.rbat
			ctr.rbat.SetRowCount(rowCountIncrease)
			ctr.probeIdx = i + 1
			return nil
		}
	}

	ctr.probeIdx = 0
	ctr.rbat.SetRowCount(rowCountIncrease)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	proc.PutBatch(ctr.inBat)
	ctr.inBat = nil
	return nil
}
