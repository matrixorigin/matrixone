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

package loopleft

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_left"

func (loopLeft *LoopLeft) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": loop left join ")
}

func (loopLeft *LoopLeft) OpType() vm.OpType {
	return vm.LoopLeft
}

func (loopLeft *LoopLeft) Prepare(proc *process.Process) error {
	var err error

	loopLeft.ctr = new(container)
	loopLeft.ctr.InitReceiver(proc, true)
	loopLeft.ctr.bat = batch.NewWithSize(len(loopLeft.Typs))
	for i, typ := range loopLeft.Typs {
		loopLeft.ctr.bat.Vecs[i] = proc.GetVector(typ)
	}

	if loopLeft.Cond != nil {
		loopLeft.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopLeft.Cond)
	}
	return err
}

func (loopLeft *LoopLeft) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(loopLeft.GetIdx(), loopLeft.GetParallelIdx(), loopLeft.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := loopLeft.ctr
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err = loopLeft.build(proc, anal); err != nil {
				return result, err
			}
			ctr.state = Probe

		case Probe:
			if ctr.inBat != nil {
				err = ctr.probe(loopLeft, proc, anal, loopLeft.GetIsLast(), &result)
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
			anal.Input(ctr.inBat, loopLeft.GetIsFirst())
			if ctr.bat.RowCount() == 0 {
				err = ctr.emptyProbe(loopLeft, proc, anal, loopLeft.GetIsLast(), &result)
			} else {
				err = ctr.probe(loopLeft, proc, anal, loopLeft.GetIsLast(), &result)
			}
			return result, err

		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopLeft *LoopLeft) build(proc *process.Process, anal process.Analyze) error {
	ctr := loopLeft.ctr
	mp := proc.ReceiveJoinMap(anal, loopLeft.JoinMapTag, false, 0)
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

func (ctr *container) emptyProbe(ap *LoopLeft, proc *process.Process, anal process.Analyze, isLast bool, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			// rbat.Vecs[i] = bat.Vecs[rp.Pos]
			// bat.Vecs[rp.Pos] = nil
			typ := *ctr.inBat.Vecs[rp.Pos].GetType()
			ctr.rbat.Vecs[i] = proc.GetVector(typ)
			if err := vector.GetUnionAllFunction(typ, proc.Mp())(ctr.rbat.Vecs[i], ctr.inBat.Vecs[rp.Pos]); err != nil {
				return err
			}
		} else {
			ctr.rbat.Vecs[i] = vector.NewConstNull(ap.Typs[rp.Pos], ctr.inBat.RowCount(), proc.Mp())
		}
	}
	ctr.probeIdx = 0
	ctr.rbat.AddRowCount(ctr.inBat.RowCount())
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	proc.PutBatch(ctr.inBat)
	ctr.inBat = nil
	return nil
}

func (ctr *container) probe(ap *LoopLeft, proc *process.Process, anal process.Analyze, isLast bool, result *vm.CallResult) error {
	if ctr.rbat != nil {
		proc.PutBatch(ctr.rbat)
		ctr.rbat = nil
	}
	ctr.rbat = batch.NewWithSize(len(ap.Result))
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			ctr.rbat.Vecs[i] = proc.GetVector(*ctr.inBat.Vecs[rp.Pos].GetType())
		} else {
			ctr.rbat.Vecs[i] = proc.GetVector(ap.Typs[rp.Pos])
		}
	}

	count := ctr.inBat.RowCount()
	rowCountIncrease := 0
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(ctr.inBat, proc.Mp())
	}
	for i := ctr.probeIdx; i < count; i++ {
		if ctr.expr == nil {
			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := ctr.rbat.Vecs[k].UnionMulti(ctr.inBat.Vecs[rp.Pos], int64(i), ctr.bat.RowCount(), proc.Mp()); err != nil {
						return err
					}
				} else {
					if err := ctr.rbat.Vecs[k].UnionBatch(ctr.bat.Vecs[rp.Pos], 0, ctr.bat.RowCount(), nil, proc.Mp()); err != nil {
						return err
					}
				}
			}
			rowCountIncrease += ctr.bat.RowCount()
		} else {
			matched := false
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
					matched = true
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[k].UnionMulti(ctr.inBat.Vecs[rp.Pos], int64(i), ctr.bat.RowCount(), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.rbat.Vecs[k].UnionBatch(ctr.bat.Vecs[rp.Pos], 0, ctr.bat.RowCount(), nil, proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease += ctr.bat.RowCount()
				}
			} else {
				l := vec.Length()
				for j := uint64(0); j < uint64(l); j++ {
					b, null := rs.GetValue(j)
					if !null && b {
						matched = true
						for k, rp := range ap.Result {
							if rp.Rel == 0 {
								if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
									return err
								}
							} else {
								if err := ctr.rbat.Vecs[k].UnionOne(ctr.bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
									return err
								}
							}
						}
						rowCountIncrease++
					}
				}
			}
			if !matched {
				for k, rp := range ap.Result {
					if rp.Rel == 0 {
						if err := ctr.rbat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
							return err
						}
					} else {
						if err := ctr.rbat.Vecs[k].UnionNull(proc.Mp()); err != nil {
							return err
						}
					}
				}
				rowCountIncrease++
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
	ctr.rbat.SetRowCount(ctr.rbat.RowCount() + rowCountIncrease)
	anal.Output(ctr.rbat, isLast)
	result.Batch = ctr.rbat
	proc.PutBatch(ctr.inBat)
	ctr.inBat = nil
	return nil
}
