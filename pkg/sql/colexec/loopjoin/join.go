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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_join"

func (loopJoin *LoopJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	switch loopJoin.JoinType {
	case LoopInner:
		buf.WriteString(": loop inner join ")
	case LoopAnti:
		buf.WriteString(": loop anti join ")
	case LoopLeft:
		buf.WriteString(": loop left join ")
	case LoopMark:
		buf.WriteString(": loop mark join ")
	case LoopSemi:
		buf.WriteString(": loop semi join ")
	case LoopSingle:
		buf.WriteString(": loop single join ")
	}
}

func (loopJoin *LoopJoin) OpType() vm.OpType {
	return vm.LoopJoin
}

func (loopJoin *LoopJoin) Prepare(proc *process.Process) error {
	var err error
	if loopJoin.OpAnalyzer == nil {
		loopJoin.OpAnalyzer = process.NewAnalyzer(loopJoin.GetIdx(), loopJoin.IsFirst, loopJoin.IsLast, "loop_join")
	} else {
		loopJoin.OpAnalyzer.Reset()
	}

	if loopJoin.Cond != nil && loopJoin.ctr.expr == nil {
		loopJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopJoin.Cond)
		if err != nil {
			return err
		}
	}

	if loopJoin.ProjectList != nil && loopJoin.ProjectExecutors == nil {
		err = loopJoin.PrepareProjection(proc)
	}
	return err
}

func (loopJoin *LoopJoin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := loopJoin.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &loopJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	probeResult := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err = loopJoin.build(proc, analyzer); err != nil {
				return result, err
			}
			if ctr.mp == nil && (loopJoin.JoinType == LoopInner || loopJoin.JoinType == LoopSemi) {
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			if ctr.inbat == nil {
				//input, err = loopJoin.Children[0].Call(proc)
				input, err = vm.ChildrenCall(loopJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				ctr.inbat = input.Batch
				if ctr.inbat == nil {
					ctr.state = End
					continue
				}
				if ctr.inbat.IsEmpty() {
					continue
				}
				//anal.Input(ctr.inbat, loopJoin.GetIsFirst())
				ctr.probeIdx = 0
				ctr.batIdx = 0
			}

			if ctr.rbat == nil {
				ctr.rbat = batch.NewWithSize(len(loopJoin.Result))
				for i, rp := range loopJoin.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i] = vector.NewVec(*ctr.inbat.Vecs[rp.Pos].GetType())
						ctr.rbat.Vecs[i].SetSorted(ctr.inbat.Vecs[rp.Pos].GetSorted())
					} else {
						if loopJoin.JoinType != LoopMark {
							ctr.rbat.Vecs[i] = vector.NewVec(loopJoin.Typs[rp.Pos])
						} else {
							ctr.rbat.Vecs[i] = vector.NewVec(types.T_bool.ToType())
						}
					}
				}
			} else {
				ctr.rbat.CleanOnlyData()
				for i, rp := range loopJoin.Result {
					if rp.Rel == 0 {
						ctr.rbat.Vecs[i].SetSorted(ctr.inbat.Vecs[rp.Pos].GetSorted())
					}
				}
			}

			if ctr.mp == nil {
				err = ctr.emptyProbe(loopJoin, proc, &probeResult)
			} else {
				err = ctr.probe(loopJoin, proc, &probeResult)
			}

			if err != nil {
				return result, err
			}

			result.Batch, err = loopJoin.EvalProjection(probeResult.Batch, proc)
			if err != nil {
				return result, err
			}

			//anal.Output(result.Batch, loopJoin.GetIsLast())
			analyzer.Output(result.Batch)
			return result, err
		default:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (loopJoin *LoopJoin) build(proc *process.Process, analyzer process.Analyzer) (err error) {
	start := time.Now()
	defer analyzer.WaitStop(start)
	loopJoin.ctr.mp, err = message.ReceiveJoinMap(loopJoin.JoinMapTag, false, 0, proc.GetMessageBoard(), proc.Ctx)
	return err
}

func (ctr *container) emptyProbe(ap *LoopJoin, proc *process.Process, result *vm.CallResult) error {
	for i, rp := range ap.Result {
		if rp.Rel == 0 {
			if err := vector.GetUnionAllFunction(*ctr.rbat.Vecs[i].GetType(), proc.Mp())(ctr.rbat.Vecs[i], ctr.inbat.Vecs[rp.Pos]); err != nil {
				return err
			}
		} else {
			if ap.JoinType == LoopLeft || ap.JoinType == LoopSingle {
				ctr.rbat.Vecs[i].SetClass(vector.CONSTANT)
				ctr.rbat.Vecs[i].SetLength(ctr.inbat.RowCount())
			} else if ap.JoinType == LoopMark {
				err := vector.SetConstFixed(ctr.rbat.Vecs[i], false, ctr.inbat.RowCount(), proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}
	ctr.rbat.AddRowCount(ctr.inbat.RowCount())
	result.Batch = ctr.rbat
	ctr.inbat = nil
	return nil
}

func (ctr *container) probe(ap *LoopJoin, proc *process.Process, result *vm.CallResult) error {
	inbat := ctr.inbat
	mpbat := ctr.mp.GetBatches()
	count := inbat.RowCount()
	if ctr.joinBat == nil {
		ctr.joinBat, ctr.cfs = colexec.NewJoinBatch(inbat, proc.Mp())
	}

	rowCountIncrease := 0
	for i := ctr.probeIdx; i < count; i++ {
		matched := false
		if ctr.batIdx != 0 {
			matched = true
		}
		for idx := ctr.batIdx; idx < len(mpbat); idx++ {
			if rowCountIncrease >= colexec.DefaultBatchSize {
				result.Batch = ctr.rbat
				ctr.rbat.SetRowCount(rowCountIncrease)
				ctr.probeIdx = i
				ctr.batIdx = idx
				return nil
			}
			bat := mpbat[idx]
			if ctr.expr != nil {
				if err := colexec.SetJoinBatchValues(ctr.joinBat, inbat, int64(i),
					bat.RowCount(), ctr.cfs); err != nil {
					return err
				}
				vec, err := ctr.expr.Eval(proc, []*batch.Batch{ctr.joinBat, bat}, nil)
				if err != nil {
					return err
				}

				rs := vector.GenerateFunctionFixedTypeParameter[bool](vec)
				if ap.JoinType != LoopMark {
					l := uint64(bat.RowCount())
					for j := uint64(0); j < l; j++ {
						b, null := rs.GetValue(j)
						if !null && b {
							if ap.JoinType == LoopSingle && matched {
								return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
							}
							matched = true
							if ap.JoinType == LoopAnti {
								continue
							}
							for k, rp := range ap.Result {
								if rp.Rel == 0 {
									if err = ctr.rbat.Vecs[k].UnionOne(inbat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
										return err
									}
								} else {
									if err = ctr.rbat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
										return err
									}
								}
							}
							rowCountIncrease++
							if ap.JoinType == LoopSemi {
								break
							}
						}
					}
				} else {
					hasTrue := false
					hasNull := false
					if vec.IsConst() {
						v, null := rs.GetValue(0)
						if null {
							hasNull = true
						} else {
							hasTrue = v
						}
					} else {
						for j := uint64(0); j < uint64(vec.Length()); j++ {
							val, null := rs.GetValue(j)
							if null {
								hasNull = true
							} else if val {
								hasTrue = true
							}
						}
					}
					for j := range ap.Result {
						if ap.Result[j].Rel == 0 {
							if err = ctr.rbat.Vecs[j].UnionOne(inbat.Vecs[ap.Result[j].Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						} else {
							if hasTrue {
								err = vector.AppendFixed(ctr.rbat.Vecs[j], true, false, proc.Mp())
							} else if hasNull {
								err = vector.AppendFixed(ctr.rbat.Vecs[j], false, true, proc.Mp())
							} else {
								err = vector.AppendFixed(ctr.rbat.Vecs[j], false, false, proc.Mp())
							}
							if err != nil {
								return err
							}
							rowCountIncrease++
						}
					}
				}
			} else {
				matched = true
				if ap.JoinType == LoopLeft {
					for k, rp := range ap.Result {
						if rp.Rel == 0 {
							if err := ctr.rbat.Vecs[k].UnionMulti(ctr.inbat.Vecs[rp.Pos], int64(i), bat.RowCount(), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.rbat.Vecs[k].UnionBatch(bat.Vecs[rp.Pos], 0, bat.RowCount(), nil, proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease += bat.RowCount()
				} else if ap.JoinType == LoopSingle {
					if bat.RowCount() == 1 {
						for k, rp := range ap.Result {
							if rp.Rel == 0 {
								err := ctr.rbat.Vecs[k].UnionOne(ctr.inbat.Vecs[rp.Pos], int64(i), proc.Mp())
								if err != nil {
									return err
								}
							} else {
								err := ctr.rbat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], 0, proc.Mp())
								if err != nil {
									return err
								}
							}
						}
						rowCountIncrease++
					} else {
						return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
					}
				}
			}
			if ap.JoinType == LoopSemi && matched {
				break
			}
		}
		if (ap.JoinType == LoopAnti || ap.JoinType == LoopLeft || ap.JoinType == LoopSingle) && !matched {
			for k, rp := range ap.Result {
				if rp.Rel == 0 {
					if err := ctr.rbat.Vecs[k].UnionOne(inbat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
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
		ctr.batIdx = 0
	}

	ctr.inbat = nil
	ctr.rbat.SetRowCount(rowCountIncrease)
	result.Batch = ctr.rbat
	return nil
}
