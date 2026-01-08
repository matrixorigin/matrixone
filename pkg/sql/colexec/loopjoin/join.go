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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "loop_join"

func (loopJoin *LoopJoin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	switch loopJoin.JoinType {
	case plan.Node_INNER:
		buf.WriteString(": loop inner join ")
	case plan.Node_ANTI:
		buf.WriteString(": loop anti join ")
	case plan.Node_LEFT:
		buf.WriteString(": loop left join ")
	case plan.Node_MARK:
		buf.WriteString(": loop mark join ")
	case plan.Node_SEMI:
		buf.WriteString(": loop semi join ")
	case plan.Node_SINGLE:
		buf.WriteString(": loop single join ")
	}
}

func (loopJoin *LoopJoin) OpType() vm.OpType {
	return vm.LoopJoin
}

func (loopJoin *LoopJoin) Prepare(proc *process.Process) error {
	var err error
	if loopJoin.OpAnalyzer == nil {
		loopJoin.OpAnalyzer = process.NewAnalyzer(loopJoin.GetIdx(), loopJoin.IsFirst, loopJoin.IsLast, opName)
	} else {
		loopJoin.OpAnalyzer.Reset()
	}

	if loopJoin.NonEqCond != nil && loopJoin.ctr.expr == nil {
		loopJoin.ctr.expr, err = colexec.NewExpressionExecutor(proc, loopJoin.NonEqCond)
		if err != nil {
			return err
		}
	}
	return err
}

func (loopJoin *LoopJoin) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := loopJoin.OpAnalyzer

	ctr := &loopJoin.ctr
	input := vm.NewCallResult()
	result := vm.NewCallResult()
	var err error
	for {
		switch ctr.state {
		case Build:
			if err = loopJoin.build(proc, analyzer); err != nil {
				return result, err
			}
			if ctr.mp == nil && (loopJoin.JoinType == plan.Node_INNER || loopJoin.JoinType == plan.Node_SEMI) {
				ctr.state = End
			} else {
				ctr.state = Probe
			}

		case Probe:
			if ctr.inBat == nil {
				input, err = vm.ChildrenCall(loopJoin.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}
				if input.Batch == nil {
					ctr.state = End
					continue
				}
				if input.Batch.IsEmpty() {
					continue
				}
				ctr.inBat = input.Batch
				ctr.probeIdx = 0
				ctr.batIdx = 0
			}

			loopJoin.resetResultBat()
			for i, rp := range loopJoin.ResultCols {
				if rp.Rel == 0 {
					ctr.resBat.Vecs[i].SetSorted(ctr.inBat.Vecs[rp.Pos].GetSorted())
				}
			}

			if ctr.mp == nil {
				err = ctr.emptyProbe(loopJoin, proc, &result)
			} else {
				err = ctr.probe(loopJoin, proc, &result)
			}

			if err != nil {
				return result, err
			}

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
	for i, rp := range ap.ResultCols {
		if rp.Rel == 0 {
			if err := vector.GetUnionAllFunction(*ctr.resBat.Vecs[i].GetType(), proc.Mp())(ctr.resBat.Vecs[i], ctr.inBat.Vecs[rp.Pos]); err != nil {
				return err
			}
		} else {
			switch ap.JoinType {
			case plan.Node_LEFT, plan.Node_SINGLE:
				ctr.resBat.Vecs[i].SetClass(vector.CONSTANT)
				ctr.resBat.Vecs[i].SetLength(ctr.inBat.RowCount())

			case plan.Node_MARK:
				err := vector.SetConstFixed(ctr.resBat.Vecs[i], false, ctr.inBat.RowCount(), proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}
	ctr.resBat.AddRowCount(ctr.inBat.RowCount())
	result.Batch = ctr.resBat
	ctr.inBat = nil
	return nil
}

func (ctr *container) probe(ap *LoopJoin, proc *process.Process, result *vm.CallResult) error {
	inbat := ctr.inBat
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
				result.Batch = ctr.resBat
				ctr.resBat.SetRowCount(rowCountIncrease)
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
				if ap.JoinType != plan.Node_MARK {
					l := uint64(bat.RowCount())
					for j := uint64(0); j < l; j++ {
						b, null := rs.GetValue(j)
						if !null && b {
							if ap.JoinType == plan.Node_SINGLE && matched {
								return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
							}

							matched = true

							if ap.JoinType == plan.Node_ANTI {
								continue
							}

							for k, rp := range ap.ResultCols {
								if rp.Rel == 0 {
									if err = ctr.resBat.Vecs[k].UnionOne(inbat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
										return err
									}
								} else {
									if err = ctr.resBat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], int64(j), proc.Mp()); err != nil {
										return err
									}
								}
							}
							rowCountIncrease++

							if ap.JoinType == plan.Node_SEMI {
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

					for j := range ap.ResultCols {
						if ap.ResultCols[j].Rel == 0 {
							if err = ctr.resBat.Vecs[j].UnionOne(inbat.Vecs[ap.ResultCols[j].Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						} else {
							if hasTrue {
								err = vector.AppendFixed(ctr.resBat.Vecs[j], true, false, proc.Mp())
							} else if hasNull {
								err = vector.AppendFixed(ctr.resBat.Vecs[j], false, true, proc.Mp())
							} else {
								err = vector.AppendFixed(ctr.resBat.Vecs[j], false, false, proc.Mp())
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
				switch ap.JoinType {
				case plan.Node_LEFT:
					for k, rp := range ap.ResultCols {
						if rp.Rel == 0 {
							if err := ctr.resBat.Vecs[k].UnionMulti(ctr.inBat.Vecs[rp.Pos], int64(i), bat.RowCount(), proc.Mp()); err != nil {
								return err
							}
						} else {
							if err := ctr.resBat.Vecs[k].UnionBatch(bat.Vecs[rp.Pos], 0, bat.RowCount(), nil, proc.Mp()); err != nil {
								return err
							}
						}
					}
					rowCountIncrease += bat.RowCount()

				case plan.Node_SINGLE:
					if bat.RowCount() == 1 {
						for k, rp := range ap.ResultCols {
							if rp.Rel == 0 {
								err := ctr.resBat.Vecs[k].UnionOne(ctr.inBat.Vecs[rp.Pos], int64(i), proc.Mp())
								if err != nil {
									return err
								}
							} else {
								err := ctr.resBat.Vecs[k].UnionOne(bat.Vecs[rp.Pos], 0, proc.Mp())
								if err != nil {
									return err
								}
							}
						}
						rowCountIncrease++
					} else {
						return moerr.NewInternalError(proc.Ctx, "scalar subquery returns more than 1 row")
					}

				case plan.Node_SEMI:
					if bat.RowCount() > 0 {
						for k, rp := range ap.ResultCols {
							if err := ctr.resBat.Vecs[k].UnionOne(inbat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						}
						rowCountIncrease++
					}

				case plan.Node_ANTI:
					if bat.RowCount() == 0 {
						for k, rp := range ap.ResultCols {
							if err := ctr.resBat.Vecs[k].UnionOne(inbat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
								return err
							}
						}
						rowCountIncrease++
					}
				}
			}

			if ap.JoinType == plan.Node_SEMI && matched {
				break
			}
		}

		if !matched && (ap.JoinType == plan.Node_ANTI || ap.JoinType == plan.Node_LEFT || ap.JoinType == plan.Node_SINGLE) {
			for k, rp := range ap.ResultCols {
				if rp.Rel == 0 {
					if err := ctr.resBat.Vecs[k].UnionOne(inbat.Vecs[rp.Pos], int64(i), proc.Mp()); err != nil {
						return err
					}
				} else {
					if err := ctr.resBat.Vecs[k].UnionNull(proc.Mp()); err != nil {
						return err
					}
				}
			}
			rowCountIncrease++
		}
		ctr.batIdx = 0
	}

	ctr.inBat = nil
	ctr.resBat.SetRowCount(rowCountIncrease)
	result.Batch = ctr.resBat
	return nil
}

func (loopJoin *LoopJoin) resetResultBat() {
	ctr := &loopJoin.ctr
	if ctr.resBat != nil {
		ctr.resBat.CleanOnlyData()
	} else {
		ctr.resBat = batch.NewWithSize(len(loopJoin.ResultCols))

		for i, rp := range loopJoin.ResultCols {
			switch rp.Rel {
			case 0:
				ctr.resBat.Vecs[i] = vector.NewVec(*ctr.inBat.Vecs[rp.Pos].GetType())

			case 1:
				ctr.resBat.Vecs[i] = vector.NewVec(loopJoin.RightTypes[rp.Pos])

			case -1:
				ctr.resBat.Vecs[i] = vector.NewVec(types.T_bool.ToType())
			}
		}
	}
}
