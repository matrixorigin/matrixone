// Copyright 2022 Matrix Origin
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

package timewin

import (
	"bytes"
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "time_window"

func (timeWin *TimeWin) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": time window")
}

func (timeWin *TimeWin) OpType() vm.OpType {
	return vm.TimeWin
}

func (timeWin *TimeWin) Prepare(proc *process.Process) (err error) {
	ctr := &timeWin.ctr
	if timeWin.OpAnalyzer == nil {
		timeWin.OpAnalyzer = process.NewAnalyzer(timeWin.GetIdx(), timeWin.IsFirst, timeWin.IsLast, "time_window")
	} else {
		timeWin.OpAnalyzer.Reset()
	}

	if len(ctr.aggExe) == 0 {
		ctr.aggExe = make([]colexec.ExpressionExecutor, len(timeWin.Aggs))
		for i, ag := range timeWin.Aggs {
			if expressions := ag.GetArgExpressions(); len(expressions) > 0 {
				ctr.aggExe[i], err = colexec.NewExpressionExecutor(proc, expressions[0])
				if err != nil {
					return err
				}
			}
		}
		ctr.aggs = make([]aggexec.AggFuncExec, len(timeWin.Aggs))
		for i, ag := range timeWin.Aggs {
			ctr.aggs[i] = aggexec.MakeAgg(proc, ag.GetAggID(), ag.IsDistinct(), timeWin.Types[i])
			if config := ag.GetExtraConfig(); config != nil {
				if err = ctr.aggs[i].SetExtraInformation(config, 0); err != nil {
					return err
				}
			}
		}
	}

	if ctr.tsExe == nil {
		ctr.tsExe, err = colexec.NewExpressionExecutor(proc, timeWin.Ts)
		if err != nil {
			return err
		}

		if timeWin.WStart {
			s, err := newTsExpr(timeWin.TsType, proc.Ctx)
			if err != nil {
				return err
			}
			ctr.startExe, err = colexec.NewExpressionExecutor(proc, s)
			if err != nil {
				return err
			}
		}

		if timeWin.WEnd {
			e := timeWin.EndExpr
			if e == nil {
				e, err = newTsExpr(timeWin.TsType, proc.Ctx)
				if err != nil {
					return err
				}
			}
			ctr.endExe, err = colexec.NewExpressionExecutor(proc, e)
			if err != nil {
				return err
			}
		}
	}

	ctr.tsOid = types.T(timeWin.TsType.Id)
	ctr.resetParam(timeWin)

	ctr.colCnt = len(timeWin.Aggs)
	if timeWin.WStart {
		ctr.colCnt++
	}
	if timeWin.WEnd {
		ctr.colCnt++
	}
	return nil
}

func (timeWin *TimeWin) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := timeWin.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &timeWin.ctr
	var err error

	result := vm.NewCallResult()
	for {
		switch ctr.status {
		case interval:
			result, err := timeWin.GetChildren(0).Call(proc)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				result.Status = vm.ExecStop
				return result, nil
			}

			if err = ctr.evalVector(result.Batch, proc); err != nil {
				return result, err
			}

			if err = ctr.calResForInterval(timeWin, proc); err != nil {
				return result, err
			}

			result.Batch = ctr.bat
			analyzer.Output(result.Batch)
			return result, nil
		case receive:
			result, err := timeWin.GetChildren(0).Call(proc)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				result.Status = vm.ExecStop
				return result, nil
			}

			if err = ctr.evalVector(result.Batch, proc); err != nil {
				return result, err
			}

			if ctr.curVecIdx == 0 && ctr.curRowIdx == 0 {
				ctr.status = firstWindow
			} else {
				ctr.status = nextWindow
			}

		case firstWindow:

			if err = ctr.firstWindow(timeWin); err != nil {
				return result, err
			}
			ctr.status = fill

		case nextWindow:

			if err = ctr.nextWindow(timeWin); err != nil {
				return result, err
			}
			ctr.status = fill

		case nextBatch:
			if ctr.curVecIdx < ctr.i-1 {
				ctr.curVecIdx++
				ctr.curRowIdx = 0
				ctr.status = fill
				break
			}

			if ctr.last {
				ctr.wStart = append(ctr.wStart, ctr.left)
				ctr.wEnd = append(ctr.wEnd, ctr.right)
				ctr.status = flush
				break
			}

			if ctr.end {
				if ctr.preVecIdx == ctr.i-1 &&
					ctr.preRowIdx == ctr.tsVec[ctr.preVecIdx].Length()-1 {
					ctr.last = true
					if ctr.lastVal < ctr.nextLeft {
						ctr.wStart = append(ctr.wStart, ctr.left)
						ctr.wEnd = append(ctr.wEnd, ctr.right)
						ctr.status = flush
						break
					}
				}
				ctr.status = nextWindow
				break
			}

			result, err := timeWin.GetChildren(0).Call(proc)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				ctr.end = true
				break
			}

			if err = ctr.evalVector(result.Batch, proc); err != nil {
				return result, err
			}

			ctr.curVecIdx++
			ctr.curRowIdx = 0

			ctr.status = fill

		case fill:

			if err = ctr.fillRows(); err != nil {
				return result, err
			}

		case flush:

			if err = ctr.calRes(timeWin, proc); err != nil {
				return result, err
			}

			if ctr.last {
				ctr.status = end
			} else {
				ctr.status = nextWindow
				for i, ag := range timeWin.Aggs {
					ctr.aggs[i] = aggexec.MakeAgg(proc, ag.GetAggID(), ag.IsDistinct(), timeWin.Types[i])
					if config := ag.GetExtraConfig(); config != nil {
						if err = ctr.aggs[i].SetExtraInformation(config, 0); err != nil {
							return result, err
						}
					}
				}
				ctr.group = 0
				for _, ag := range ctr.aggs {
					if err := ag.GroupGrow(1); err != nil {
						return result, err
					}
				}
				ctr.withoutFill = true
			}

			result.Batch = ctr.bat
			analyzer.Output(result.Batch)
			return result, nil

		case end:
			result.Batch = nil
			result.Status = vm.ExecStop
			analyzer.Output(result.Batch)
			return result, nil

		}

	}
}

func newTsExpr(typ plan.Type, ctx context.Context) (*plan.Expr, error) {
	col := &plan.Expr{
		Typ: plan.Type{},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: 0,
			},
		},
	}

	typ.NotNullable = col.Typ.NotNullable
	argsType := []types.Type{
		types.New(types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale),
		types.New(types.T(typ.Id), typ.Width, typ.Scale),
	}
	fGet, err := function.GetFunctionByName(ctx, "cast", argsType)
	if err != nil {
		return nil, err
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{Obj: fGet.GetEncodedOverloadID(), ObjName: "cast"},
				Args: []*plan.Expr{
					col,
					{
						Typ: typ,
						Expr: &plan.Expr_T{
							T: &plan.TargetType{},
						},
					},
				},
			},
		},
		Typ: typ,
	}, nil
}

func (ctr *container) nextWindow(t *TimeWin) error {
	if !ctr.withoutFill {
		ctr.wStart = append(ctr.wStart, ctr.left)
		ctr.wEnd = append(ctr.wEnd, ctr.right)
	}

	ctr.left = ctr.nextLeft
	ctr.right = ctr.nextRight

	ctr.nextLeft = ctr.left + t.Sliding
	ctr.nextRight = ctr.nextLeft + t.Interval

	ctr.curVecIdx = ctr.preVecIdx
	ctr.curRowIdx = ctr.preRowIdx

	if !ctr.withoutFill {
		for _, ag := range ctr.aggs {
			if err := ag.GroupGrow(1); err != nil {
				return err
			}
		}
		ctr.group++
	}
	return nil
}

func (ctr *container) firstWindow(t *TimeWin) error {
	val := vector.MustFixedColWithTypeCheck[types.Datetime](ctr.tsVec[ctr.curVecIdx])[ctr.curRowIdx]
	ctr.left = val - val%t.Interval
	ctr.right = ctr.left + t.Interval

	ctr.nextLeft = ctr.left + t.Sliding
	ctr.nextRight = ctr.nextLeft + t.Interval

	for _, ag := range ctr.aggs {
		if err := ag.GroupGrow(1); err != nil {
			return err
		}
	}
	ctr.group++
	return nil
}

func (ctr *container) fillRows() error {
	cnt := ctr.tsVec[ctr.curVecIdx].Length()
	vals := vector.MustFixedColNoTypeCheck[types.Datetime](ctr.tsVec[ctr.curVecIdx])

	outRange := false
	ctr.withoutFill = true
	for ; ctr.curRowIdx < cnt; ctr.curRowIdx++ {
		if vals[ctr.curRowIdx] <= ctr.nextLeft {
			ctr.preVecIdx = ctr.curVecIdx
			ctr.preRowIdx = ctr.curRowIdx
		}

		if vals[ctr.curRowIdx] >= ctr.left && vals[ctr.curRowIdx] < ctr.right {
			for j, agg := range ctr.aggs {
				if err := agg.Fill(ctr.group, ctr.curRowIdx, []*vector.Vector{ctr.aggVec[ctr.curVecIdx][j]}); err != nil {
					return err
				}
			}
			ctr.withoutFill = false
		} else if vals[ctr.curRowIdx] >= ctr.right {
			outRange = true
			break
		}
	}

	if outRange {
		if ctr.group > maxTimeWindowRows {
			ctr.wStart = append(ctr.wStart, ctr.left)
			ctr.wEnd = append(ctr.wEnd, ctr.right)
			ctr.status = flush
		} else {
			ctr.status = nextWindow
		}
	} else {
		ctr.lastVal = vals[cnt-1]
		ctr.status = nextBatch
	}
	return nil
}

const maxTimeWindowRows = 8192

func (ctr *container) calRes(ap *TimeWin, proc *process.Process) (err error) {
	ctr.bat = batch.NewWithSize(ctr.colCnt)
	i := 0
	for _, agg := range ctr.aggs {
		vec, err := agg.Flush()
		if err != nil {
			return err
		}
		ctr.bat.SetVector(int32(i), vec)
		i++
	}

	if !ap.WStart && !ap.WEnd {
		batch.SetLength(ctr.bat, ctr.bat.Vecs[0].Length())
		return nil
	}
	bat := batch.NewWithSize(1)
	if ap.WStart {
		if ctr.startVec != nil {
			ctr.startVec.CleanOnlyData()
		} else {
			ctr.startVec = vector.NewVec(types.T_datetime.ToType())
		}
		err = vector.AppendFixedList(ctr.startVec, ctr.wStart, nil, proc.Mp())
		if err != nil {
			return err
		}

		bat.SetVector(0, ctr.startVec)
		batch.SetLength(bat, ctr.startVec.Length())
		ctr.bat.Vecs[int32(i)], err = ctr.startExe.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		i++
	}

	if ap.WEnd {
		if ctr.endVec != nil {
			ctr.endVec.CleanOnlyData()
		} else {
			ctr.endVec = vector.NewVec(types.T_datetime.ToType())
		}
		err = vector.AppendFixedList(ctr.endVec, ctr.wEnd, nil, proc.Mp())
		if err != nil {
			return err
		}

		bat.SetVector(0, ctr.endVec)
		batch.SetLength(bat, ctr.endVec.Length())
		ctr.bat.Vecs[int32(i)], err = ctr.endExe.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
	}

	batch.SetLength(ctr.bat, ctr.bat.Vecs[0].Length())
	ctr.wStart = nil
	ctr.wEnd = nil
	return nil
}

func (ctr *container) calResForInterval(ap *TimeWin, proc *process.Process) (err error) {
	ctr.bat = batch.NewWithSize(ctr.colCnt)
	i := 0
	for _, vec := range ctr.aggVec[ctr.i-1] {
		ctr.bat.SetVector(int32(i), vec)
		i++
	}

	if !ap.WStart && !ap.WEnd {
		batch.SetLength(ctr.bat, ctr.bat.Vecs[0].Length())
		return nil
	}
	bat := batch.NewWithSize(1)
	if ap.WStart {
		bat.SetVector(0, ctr.tsVec[ctr.i-1])
		batch.SetLength(bat, ctr.tsVec[ctr.i-1].Length())
		ctr.bat.Vecs[int32(i)], err = ctr.startExe.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		i++
	}

	if ap.WEnd {
		bat.SetVector(0, ctr.tsVec[ctr.i-1])
		batch.SetLength(bat, ctr.tsVec[ctr.i-1].Length())
		ctr.bat.Vecs[int32(i)], err = ctr.endExe.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
	}

	batch.SetLength(ctr.bat, ctr.bat.Vecs[0].Length())
	ctr.wStart = nil
	ctr.wEnd = nil
	return nil
}

func (ctr *container) evalVector(bat *batch.Batch, proc *process.Process) error {
	if err := ctr.evalTsVector(bat, proc); err != nil {
		return err
	}
	if err := ctr.evalAggVector(bat, proc); err != nil {
		return err
	}
	ctr.i++
	return nil
}

func (ctr *container) evalTsVector(bat *batch.Batch, proc *process.Process) error {
	vec, err := ctr.tsExe.Eval(proc, []*batch.Batch{bat}, nil)
	if err != nil {
		return err
	}

	if len(ctr.tsVec) > ctr.i {
		ctr.tsVec[ctr.i].CleanOnlyData()
		if err = ctr.tsVec[ctr.i].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
			return err
		}
	} else {
		tv, err := vec.Dup(proc.Mp())
		if err != nil {
			return err
		}
		ctr.tsVec = append(ctr.tsVec, tv)
	}

	return nil
}

func (ctr *container) evalAggVector(bat *batch.Batch, proc *process.Process) error {
	f := len(ctr.aggVec) > ctr.i
	if !f {
		ctr.aggVec = append(ctr.aggVec, make([]*vector.Vector, len(ctr.aggExe)))
	}
	for i := range ctr.aggExe {
		if ctr.aggExe[i] != nil {
			vec, err := ctr.aggExe[i].Eval(proc, []*batch.Batch{bat}, nil)
			if err != nil {
				return err
			}
			if f {
				ctr.aggVec[ctr.i][i].CleanOnlyData()
				if err = ctr.aggVec[ctr.i][i].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
					return err
				}
			} else {
				ctr.aggVec[ctr.i][i], err = vec.Dup(proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
