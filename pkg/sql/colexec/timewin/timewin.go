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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
	}

	// Gated separately from the expression executors: Reset discards the
	// aggregate state (it cannot survive a Flush) while keeping the executors,
	// so a reused operator arrives here with executors but no aggregates.
	if len(ctr.aggs) == 0 {
		ctr.aggs, err = makeAggExecutors(timeWin, proc, false)
		if err != nil {
			return err
		}
	}

	if len(ctr.partExe) == 0 && len(timeWin.PartitionBy) > 0 {
		ctr.partExe = make([]colexec.ExpressionExecutor, len(timeWin.PartitionBy))
		ctr.partSet = make([]func(v, w *vector.Vector, sel int64, length int) error, len(timeWin.PartitionBy))
		for i, expr := range timeWin.PartitionBy {
			ctr.partExe[i], err = colexec.NewExpressionExecutor(proc, expr)
			if err != nil {
				return err
			}
			ctr.partSet[i] = getPartitionSetFunction(
				types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale), proc.Mp())
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

	// Must match plan.BuildTimeWindowLayout: aggregates, then the boundaries,
	// then the partition keys.
	ctr.colCnt = len(timeWin.Aggs)
	if timeWin.WStart {
		ctr.colCnt++
	}
	if timeWin.WEnd {
		ctr.colCnt++
	}
	ctr.colCnt += len(timeWin.PartitionBy)
	return nil
}

func getPartitionSetFunction(
	typ types.Type,
	mp *mpool.MPool,
) func(v, w *vector.Vector, sel int64, length int) error {
	if typ.Oid != types.T_any {
		return vector.GetConstSetFunction(typ, mp)
	}

	// T_any is the physical type of an untyped NULL literal. It has no value
	// representation for GetConstSetFunction to copy, but GROUP BY NULL is a
	// valid single partition and its key still occupies an output-layout slot.
	return func(v, w *vector.Vector, sel int64, length int) error {
		if !w.IsConstNull() && !w.IsNull(uint64(sel)) {
			return moerr.NewInternalErrorNoCtx("time window received a non-NULL T_any partition key")
		}
		return vector.SetConstNull(v, length, mp)
	}
}

func (timeWin *TimeWin) Call(proc *process.Process) (vm.CallResult, error) {
	ctr := &timeWin.ctr
	var err error

	result := vm.NewCallResult()
	for {
		switch ctr.status {
		case interval:
			result, err := vm.Exec(timeWin.GetChildren(0), proc)
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
			return result, nil
		case receive:
			result, err := vm.Exec(timeWin.GetChildren(0), proc)
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

			result, err := vm.Exec(timeWin.GetChildren(0), proc)
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

			switch {
			case ctr.last:
				ctr.status = end
			case ctr.partitionBreak:
				// The previous partition is fully emitted. Resume at its first
				// foreign row and rebuild the window state from scratch, so
				// the new partition's windows are anchored on its own data
				// rather than sliding the old partition's state forward.
				replacements, err := makeAggExecutors(timeWin, proc, false)
				if err != nil {
					return result, err
				}
				ctr.freeAgg()
				ctr.aggs = replacements
				ctr.curVecIdx = ctr.breakVecIdx
				ctr.curRowIdx = ctr.breakRowIdx
				ctr.preVecIdx = ctr.breakVecIdx
				ctr.preRowIdx = ctr.breakRowIdx
				ctr.group = -1
				ctr.withoutFill = true
				ctr.partEnd = false
				ctr.partitionBreak = false
				ctr.status = firstWindow
			default:
				replacements, err := makeAggExecutors(timeWin, proc, true)
				if err != nil {
					return result, err
				}

				// Flush transfers only result vectors; executor-owned state (for
				// example DISTINCT hash tables) remains live until Free. Construct
				// every replacement first so an allocation/configuration failure
				// leaves the current executors owned by ctr and available for the
				// normal terminal cleanup path.
				ctr.freeAgg()
				ctr.aggs = replacements
				ctr.status = nextWindow
				ctr.group = 0
				ctr.withoutFill = true
			}

			result.Batch = ctr.bat
			return result, nil

		case end:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil

		}

	}
}

func makeAggExecutors(timeWin *TimeWin, proc *process.Process, growFirstGroup bool) (_ []aggexec.AggFuncExec, err error) {
	aggs := make([]aggexec.AggFuncExec, len(timeWin.Aggs))
	defer func() {
		if err != nil {
			for _, agg := range aggs {
				if agg != nil {
					agg.Free()
				}
			}
		}
	}()

	for i, expression := range timeWin.Aggs {
		aggs[i], err = aggexec.MakeAgg(
			proc.Mp(), expression.GetAggID(), expression.IsDistinct(), timeWin.Types[i])
		if err != nil {
			return nil, err
		}
		if config := expression.GetExtraConfig(); config != nil {
			if err = aggs[i].SetExtraInformation(config, 0); err != nil {
				return nil, err
			}
		}
		if growFirstGroup {
			if err = aggs[i].GroupGrow(1); err != nil {
				return nil, err
			}
		}
	}
	return aggs, nil
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
	// See firstWindow: the new window is empty until a row lands in it.
	ctr.withoutFill = true
	return nil
}

func (ctr *container) firstWindow(t *TimeWin) error {
	val := vector.MustFixedColWithTypeCheck[types.Datetime](ctr.tsVec[ctr.curVecIdx])[ctr.curRowIdx]
	ctr.left = val - val%t.Interval
	ctr.right = ctr.left + t.Interval

	ctr.nextLeft = ctr.left + t.Sliding
	ctr.nextRight = ctr.nextLeft + t.Interval

	// This row opens the partition every window until the next boundary
	// belongs to; fillRows compares against it to spot the change.
	ctr.partIdx = ctr.curVecIdx
	ctr.partRow = ctr.curRowIdx

	for _, ag := range ctr.aggs {
		if err := ag.GroupGrow(1); err != nil {
			return err
		}
	}
	ctr.group++
	// The window is empty until fillRows lands a row in it. This must live at
	// window switches, not at the top of fillRows: a window's rows can arrive
	// in one batch and its closing row in the next, and resetting the flag
	// per batch made such a window look empty and drop from the output.
	ctr.withoutFill = true
	return nil
}

func (ctr *container) fillRows() error {
	cnt := ctr.tsVec[ctr.curVecIdx].Length()
	vals := vector.MustFixedColNoTypeCheck[types.Datetime](ctr.tsVec[ctr.curVecIdx])

	outRange := false
	partBreak := false
	for ; ctr.curRowIdx < cnt; ctr.curRowIdx++ {
		// Input arrives ordered by partition key, so the first row that
		// disagrees with the key this window opened on ends the partition's
		// data. Its remaining windows still have to be produced, so record
		// where to resume and fall through to the same slide-to-exhaustion
		// logic that end-of-stream uses.
		if len(ctr.partExe) > 0 && !ctr.samePartition(ctr.curVecIdx, ctr.curRowIdx, ctr.partIdx, ctr.partRow) {
			ctr.partEnd = true
			ctr.breakVecIdx = ctr.curVecIdx
			ctr.breakRowIdx = ctr.curRowIdx
			partBreak = true
			break
		}
		ctr.partLastVal = vals[ctr.curRowIdx]
		ctr.partLastVecIdx = ctr.curVecIdx
		ctr.partLastRowIdx = ctr.curRowIdx

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

	switch {
	case partBreak:
		// Same shape as the end-of-stream check in nextBatch, scoped to the
		// partition: once the rewind point has reached the partition's last
		// row and the next window would start past it, the partition is done.
		if ctr.preVecIdx == ctr.partLastVecIdx && ctr.preRowIdx == ctr.partLastRowIdx &&
			ctr.partLastVal < ctr.nextLeft {
			ctr.partitionBreak = true
			ctr.wStart = append(ctr.wStart, ctr.left)
			ctr.wEnd = append(ctr.wEnd, ctr.right)
			ctr.status = flush
		} else {
			ctr.status = nextWindow
		}
	case outRange:
		if ctr.group > maxTimeWindowRows {
			ctr.wStart = append(ctr.wStart, ctr.left)
			ctr.wEnd = append(ctr.wEnd, ctr.right)
			ctr.status = flush
		} else {
			ctr.status = nextWindow
		}
	default:
		ctr.lastVal = vals[cnt-1]
		ctr.status = nextBatch
	}
	return nil
}

const maxTimeWindowRows = 8192

func (ctr *container) calRes(ap *TimeWin, proc *process.Process) (err error) {
	ctr.freeFlushedAggVecs(proc.Mp())
	ctr.bat = batch.NewWithSize(ctr.colCnt)
	i := 0
	for _, agg := range ctr.aggs {
		vecs, err := agg.Flush()
		if err != nil {
			return err
		}
		result, err := aggexec.MergeSplitResult(vecs, proc.Mp())
		if err != nil {
			return err
		}

		ctr.bat.SetVector(int32(i), result)
		i++
	}

	if !ap.WStart && !ap.WEnd {
		if err = ctr.setPartVecsForWindows(i, ctr.bat.Vecs[0].Length(), proc); err != nil {
			return err
		}
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
		i++
	}

	if err = ctr.setPartVecsForWindows(i, ctr.bat.Vecs[0].Length(), proc); err != nil {
		return err
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
		// The partition keys still have to land in their slots, and they sit
		// after the (absent) boundaries.
		ctr.setPartVecsForInterval(i)
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
		i++
	}

	ctr.setPartVecsForInterval(i)

	batch.SetLength(ctr.bat, ctr.bat.Vecs[0].Length())
	ctr.wStart = nil
	ctr.wEnd = nil
	return nil
}

// setPartVecsForInterval forwards the partition keys row-for-row. Without
// sliding the child aggregate has already reduced each (partition, window) to
// one row, so this path never merges rows and the key passes straight through.
func (ctr *container) setPartVecsForInterval(slot int) {
	if len(ctr.partExe) == 0 {
		// Without partition keys evalPartVector buffers nothing, so partVec
		// has no entry for this batch to read.
		return
	}
	for _, vec := range ctr.partVec[ctr.i-1] {
		ctr.bat.SetVector(int32(slot), vec)
		slot++
	}
}

// freeFlushedAggVecs releases the aggregate vectors of the previously flushed
// result. Each flush replaces ctr.bat, and the Flush() results are the only
// vectors that batch owns outright -- the boundaries belong to their expression
// executors and the partition keys to ctr.partOut. Without this, every flush
// after the first orphans a set of aggregate vectors, which partitioning makes
// routine rather than rare.
func (ctr *container) freeFlushedAggVecs(mp *mpool.MPool) {
	if ctr.bat == nil {
		return
	}
	for i := 0; i < len(ctr.aggs) && i < len(ctr.bat.Vecs); i++ {
		if vec := ctr.bat.Vecs[i]; vec != nil {
			vec.Free(mp)
			ctr.bat.SetVector(int32(i), nil)
		}
	}
}

// setPartVecsForWindows broadcasts the current partition's key across the rows
// being flushed. Sliding windows collapse many input rows into one row per
// window, but a flush never spans partitions, so one key covers the batch.
func (ctr *container) setPartVecsForWindows(slot, length int, proc *process.Process) error {
	if len(ctr.partExe) == 0 {
		return nil
	}
	if ctr.partIdx < 0 {
		return moerr.NewInternalErrorNoCtx("time window flushed a result before any partition key was seen")
	}
	if len(ctr.partOut) == 0 {
		ctr.partOut = make([]*vector.Vector, len(ctr.partExe))
	}
	for p, src := range ctr.partVec[ctr.partIdx] {
		if ctr.partOut[p] == nil {
			ctr.partOut[p] = vector.NewVec(*src.GetType())
		} else {
			ctr.partOut[p].CleanOnlyData()
		}
		if err := ctr.partSet[p](ctr.partOut[p], src, int64(ctr.partRow), length); err != nil {
			return err
		}
		ctr.bat.SetVector(int32(slot), ctr.partOut[p])
		slot++
	}
	return nil
}

func (ctr *container) evalVector(bat *batch.Batch, proc *process.Process) error {
	if err := ctr.evalTsVector(bat, proc); err != nil {
		return err
	}
	if err := ctr.evalAggVector(bat, proc); err != nil {
		return err
	}
	if err := ctr.evalPartVector(bat, proc); err != nil {
		return err
	}
	ctr.i++
	return nil
}

func (ctr *container) evalPartVector(bat *batch.Batch, proc *process.Process) error {
	if len(ctr.partExe) == 0 {
		return nil
	}
	f := len(ctr.partVec) > ctr.i
	if !f {
		ctr.partVec = append(ctr.partVec, make([]*vector.Vector, len(ctr.partExe)))
	}
	for i := range ctr.partExe {
		vec, err := ctr.partExe[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		if f {
			ctr.partVec[ctr.i][i].CleanOnlyData()
			if err = ctr.partVec[ctr.i][i].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
				return err
			}
		} else {
			ctr.partVec[ctr.i][i], err = vec.Dup(proc.Mp())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// samePartition reports whether two buffered rows carry the same partition key.
func (ctr *container) samePartition(vecIdx1, rowIdx1, vecIdx2, rowIdx2 int) bool {
	for i := range ctr.partExe {
		v1, v2 := ctr.partVec[vecIdx1][i], ctr.partVec[vecIdx2][i]
		null1, null2 := v1.IsNull(uint64(rowIdx1)), v2.IsNull(uint64(rowIdx2))
		if null1 || null2 {
			// GROUP BY folds NULLs together, so the window must too.
			if null1 != null2 {
				return false
			}
			continue
		}
		if !bytes.Equal(v1.GetRawBytesAt(rowIdx1), v2.GetRawBytesAt(rowIdx2)) {
			return false
		}
	}
	return true
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
