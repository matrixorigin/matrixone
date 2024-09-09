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

package window

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "window"

func (window *Window) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": window")
}

func (window *Window) OpType() vm.OpType {
	return vm.Window
}

func (window *Window) Prepare(proc *process.Process) (err error) {
	ctr := &window.ctr

	if len(ctr.aggVecs) == 0 {
		ctr.aggVecs = make([]group.ExprEvalVector, len(window.Aggs))
		for i, ag := range window.Aggs {
			expressions := ag.GetArgExpressions()
			if ctr.aggVecs[i], err = group.MakeEvalVector(proc, expressions); err != nil {
				return err
			}
		}
	}

	w := window.WinSpecList[0].Expr.(*plan.Expr_W).W
	if len(w.PartitionBy) == 0 {
		ctr.status = receiveAll
	}

	return nil
}

func (window *Window) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	var err error
	ctr := &window.ctr
	anal := proc.GetAnalyze(window.GetIdx(), window.GetParallelIdx(), window.GetParallelMajor())
	anal.Start()
	defer anal.Stop()

	for {
		switch ctr.status {
		case receiveAll:
			for {
				result, err := window.GetChildren(0).Call(proc)
				if err != nil {
					return result, err
				}
				if result.Batch == nil {
					ctr.status = eval
					break
				}
				anal.Input(result.Batch, window.GetIsFirst())
				ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
				if err != nil {
					return result, err
				}
			}
		case receive:
			result, err := window.GetChildren(0).Call(proc)
			if err != nil {
				return result, err
			}
			if result.Batch == nil {
				ctr.status = done
			} else {
				ctr.status = eval
				anal.Input(result.Batch, window.GetIsFirst())
				if ctr.bat != nil {
					ctr.bat.CleanOnlyData()
				}
				ctr.bat, err = ctr.bat.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
				if err != nil {
					return result, err
				}
			}
		case eval:
			result := vm.NewCallResult()
			if err = ctr.evalAggVector(ctr.bat, proc); err != nil {
				return result, err
			}

			ctr.bat.Aggs = make([]aggexec.AggFuncExec, len(window.Aggs))
			for i, ag := range window.Aggs {
				ctr.bat.Aggs[i] = aggexec.MakeAgg(proc, ag.GetAggID(), ag.IsDistinct(), window.Types[i])
				if config := ag.GetExtraConfig(); config != nil {
					if err = ctr.bat.Aggs[i].SetExtraInformation(config, 0); err != nil {
						return result, err
					}
				}
				if err = ctr.bat.Aggs[i].GroupGrow(ctr.bat.RowCount()); err != nil {
					return result, err
				}
			}
			// calculate
			for i, w := range window.WinSpecList {
				// sort and partitions
				if window.Fs = makeOrderBy(w); window.Fs != nil {
					if len(ctr.orderVecs) == 0 {
						ctr.orderVecs = make([]group.ExprEvalVector, len(window.Fs))
						for j := range ctr.orderVecs {
							ctr.orderVecs[j], err = group.MakeEvalVector(proc, []*plan.Expr{window.Fs[j].Expr})
							if err != nil {
								return result, err
							}
						}
					}

					_, err = ctr.processOrder(i, window, ctr.bat, proc)
					if err != nil {
						return result, err
					}
				}
				// evaluate func
				if err = ctr.processFunc(i, window, proc, anal); err != nil {
					return result, err
				}
			}
			// we can not reuse agg func
			ctr.freeAggFun()

			anal.Output(ctr.bat, window.GetIsLast())
			if len(window.WinSpecList[0].Expr.(*plan.Expr_W).W.PartitionBy) == 0 {
				ctr.status = done
			} else {
				ctr.status = receive
			}

			if ctr.rBat != nil {
				result.Batch = ctr.resetResultBatch(ctr.bat, ctr.vec)
			} else {
				result.Batch = ctr.makeResultBatch(ctr.bat, ctr.vec)
			}
			result.Status = vm.ExecNext
			return result, nil
		case done:
			result := vm.NewCallResult()
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) makeResultBatch(bat *batch.Batch, vec *vector.Vector) *batch.Batch {
	ctr.rBat = batch.NewWithSize(len(bat.Vecs) + 1)
	i := 0
	for i < len(bat.Vecs) {
		ctr.rBat.Vecs[i] = bat.Vecs[i]
		i++
	}
	ctr.rBat.Vecs[i] = vec
	ctr.rBat.SetRowCount(vec.Length())
	return ctr.rBat
}

func (ctr *container) resetResultBatch(bat *batch.Batch, vec *vector.Vector) *batch.Batch {
	i := 0
	for i < len(bat.Vecs) {
		ctr.rBat.Vecs[i] = bat.Vecs[i]
		i++
	}
	ctr.rBat.Vecs[i] = vec
	ctr.rBat.SetRowCount(vec.Length())
	return ctr.rBat
}

func (ctr *container) processFunc(idx int, ap *Window, proc *process.Process, anal process.Analyze) error {
	var err error
	n := ctr.bat.Vecs[0].Length()
	isWinOrder := function.GetFunctionIsWinOrderFunByName(ap.WinSpecList[idx].Expr.(*plan.Expr_W).W.Name)
	if isWinOrder {
		if ctr.ps == nil {
			ctr.ps = append(ctr.ps, 0)
		}
		if ctr.os == nil {
			ctr.os = append(ctr.os, 0)
		}
		ctr.ps = append(ctr.ps, int64(n))
		ctr.os = append(ctr.os, int64(n))
		if len(ctr.os) < len(ctr.ps) {
			ctr.os = ctr.ps
		}

		vec := vector.NewVec(types.T_int64.ToType())
		defer vec.Free(proc.Mp())
		if err = vector.AppendFixedList(vec, ctr.os, nil, proc.Mp()); err != nil {
			return err
		}

		o := 0
		for p := 1; p < len(ctr.ps); p++ {
			for ; o < len(ctr.os); o++ {

				if ctr.os[o] <= ctr.ps[p] {

					if err = ctr.bat.Aggs[idx].Fill(p-1, o, []*vector.Vector{vec}); err != nil {
						return err
					}

				} else {
					o--
					break
				}

			}
		}
	} else {
		//nullVec := vector.NewConstNull(*ctr.aggVecs[idx].Vec[0].GetType(), 1, proc.Mp())
		//defer nullVec.Free(proc.Mp())

		// plan.Function_AGG, plan.Function_WIN_VALUE
		for j := 0; j < n; j++ {

			start, end := 0, n

			if ctr.ps != nil {
				start, end = buildPartitionInterval(ctr.ps, j, n)
			}

			left, right, err := ctr.buildInterval(j, start, end, ap.WinSpecList[idx].Expr.(*plan.Expr_W).W.Frame)
			if err != nil {
				return err
			}

			if right < start || left > end || left >= right {
				// todo: I commented this out because it was a waste of time to fill a null value.
				//if err = ctr.bat.Aggs[idx].Fill(j, 0, []*vector.Vector{nullVec}); err != nil {
				//	return err
				//}
				continue
			}

			if left < start {
				left = start
			}
			if right > end {
				right = end
			}

			for k := left; k < right; k++ {
				if err = ctr.bat.Aggs[idx].Fill(j, k, ctr.aggVecs[idx].Vec); err != nil {
					return err
				}
			}

		}
	}

	// result of agg eval is not reuse the vector
	if ctr.vec != nil {
		ctr.vec.Free(proc.Mp())
	}
	ctr.vec, err = ctr.bat.Aggs[idx].Flush()
	if err != nil {
		return err
	}
	if isWinOrder {
		ctr.vec.SetNulls(nil)
	}
	if ctr.vec != nil {
		anal.Alloc(int64(ctr.vec.Size()))
	}
	ctr.os = nil
	ctr.ps = nil
	return nil
}

func (ctr *container) buildInterval(rowIdx, start, end int, frame *plan.FrameClause) (int, int, error) {
	// FrameClause_ROWS
	if frame.Type == plan.FrameClause_ROWS {
		start, end = ctr.buildRowsInterval(rowIdx, start, end, frame)
		return start, end, nil
	}

	if len(ctr.orderVecs) == 0 {
		return start, end, nil
	}

	// FrameClause_Range
	return ctr.buildRangeInterval(rowIdx, start, end, frame)
}

func (ctr *container) buildRowsInterval(rowIdx int, start, end int, frame *plan.FrameClause) (int, int) {
	switch frame.Start.Type {
	case plan.FrameBound_CURRENT_ROW:
		start = rowIdx
	case plan.FrameBound_PRECEDING:
		if !frame.Start.UnBounded {
			pre := frame.Start.Val.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			start = rowIdx - int(pre)
		}
	case plan.FrameBound_FOLLOWING:
		fol := frame.Start.Val.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
		start = rowIdx + int(fol)
	}

	switch frame.End.Type {
	case plan.FrameBound_CURRENT_ROW:
		end = rowIdx + 1
	case plan.FrameBound_PRECEDING:
		pre := frame.End.Val.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
		end = rowIdx - int(pre) + 1
	case plan.FrameBound_FOLLOWING:
		if !frame.End.UnBounded {
			fol := frame.End.Val.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			end = rowIdx + int(fol) + 1
		}
	}
	return start, end
}

func (ctr *container) buildRangeInterval(rowIdx int, start, end int, frame *plan.FrameClause) (int, int, error) {
	var err error
	switch frame.Start.Type {
	case plan.FrameBound_CURRENT_ROW:
		start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], nil, false)
		if err != nil {
			return start, end, err
		}
	case plan.FrameBound_PRECEDING:
		if !frame.Start.UnBounded {
			start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.Start.Val, false)
			if err != nil {
				return start, end, err
			}
		}
	case plan.FrameBound_FOLLOWING:
		start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.Start.Val, true)
		if err != nil {
			return start, end, err
		}
	}

	switch frame.End.Type {
	case plan.FrameBound_CURRENT_ROW:
		end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], nil, false)
		if err != nil {
			return start, end, err
		}
	case plan.FrameBound_PRECEDING:
		end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.End.Val, true)
		if err != nil {
			return start, end, err
		}
	case plan.FrameBound_FOLLOWING:
		if !frame.End.UnBounded {
			end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].Vec[0], frame.End.Val, false)
			if err != nil {
				return start, end, err
			}
		}
	}
	return start, end, nil
}

func buildPartitionInterval(ps []int64, j int, l int) (int, int) {
	left, right := 0, 0
	for i, p := range ps {
		if p > int64(j) {
			right = int(p)
			if i == 0 {
				left = 0
			} else {
				left = int(ps[i-1])
			}
			break
		}
	}
	if right == 0 {
		return int(ps[len(ps)-1]), l
	}
	return left, right
}

func (ctr *container) evalAggVector(bat *batch.Batch, proc *process.Process) (err error) {
	input := []*batch.Batch{bat}

	for i := range ctr.aggVecs {
		for j := range ctr.aggVecs[i].Executor {
			vec, err := ctr.aggVecs[i].Executor[j].Eval(proc, input, nil)
			if err != nil {
				return err
			}

			if ctr.aggVecs[i].Vec[j] != nil {
				ctr.aggVecs[i].Vec[j].CleanOnlyData()
				if err = ctr.aggVecs[i].Vec[j].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
					return err
				}
			} else {
				ctr.aggVecs[i].Vec[j], err = vec.Dup(proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func makeArgFs(window *Window) {
	window.ctr.desc = make([]bool, len(window.Fs))
	window.ctr.nullsLast = make([]bool, len(window.Fs))
	for i, f := range window.Fs {
		window.ctr.desc[i] = f.Flag&plan.OrderBySpec_DESC != 0
		if f.Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
			window.ctr.nullsLast[i] = false
		} else if f.Flag&plan.OrderBySpec_NULLS_LAST != 0 {
			window.ctr.nullsLast[i] = true
		} else {
			window.ctr.nullsLast[i] = window.ctr.desc[i]
		}
	}
}

func makeOrderBy(expr *plan.Expr) []*plan.OrderBySpec {
	w := expr.Expr.(*plan.Expr_W).W
	if len(w.PartitionBy) == 0 && len(w.OrderBy) == 0 {
		return nil
	}
	return w.OrderBy
}

func (ctr *container) evalOrderVector(bat *batch.Batch, proc *process.Process) (err error) {
	input := []*batch.Batch{bat}

	for i := range ctr.orderVecs {
		for j := range ctr.orderVecs[i].Executor {
			vec, err := ctr.orderVecs[i].Executor[j].Eval(proc, input, nil)
			if err != nil {
				return err
			}

			if ctr.orderVecs[i].Vec[j] != nil {
				ctr.orderVecs[i].Vec[j].CleanOnlyData()
				if err = ctr.orderVecs[i].Vec[j].UnionBatch(vec, 0, vec.Length(), nil, proc.Mp()); err != nil {
					return err
				}
			} else {
				ctr.orderVecs[i].Vec[j], err = vec.Dup(proc.Mp())
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ctr *container) processOrder(idx int, ap *Window, bat *batch.Batch, proc *process.Process) (bool, error) {
	makeArgFs(ap)

	if err := ctr.evalOrderVector(bat, proc); err != nil {
		return false, err
	}
	if bat.RowCount() < 2 {
		return false, nil
	}

	ovec := ctr.orderVecs[0].Vec[0]

	rowCount := bat.RowCount()
	//if ctr.sels == nil {
	//	ctr.sels = make([]int64, rowCount)
	//}
	ctr.sels = make([]int64, rowCount)
	for i := 0; i < rowCount; i++ {
		ctr.sels[i] = int64(i)
	}

	// skip sort for const vector
	if !ovec.IsConst() {
		nullCnt := ovec.GetNulls().Count()
		if nullCnt < ovec.Length() {
			sort.Sort(ctr.desc[0], ctr.nullsLast[0], nullCnt > 0, ctr.sels, ovec)
		}
	}

	ps := make([]int64, 0, 16)
	ds := make([]bool, len(ctr.sels))

	w := ap.WinSpecList[idx].Expr.(*plan.Expr_W).W
	n := len(w.PartitionBy)

	i, j := 1, len(ctr.orderVecs)
	for ; i < j; i++ {
		desc := ctr.desc[i]
		nullsLast := ctr.nullsLast[i]
		ps = partition.Partition(ctr.sels, ds, ps, ovec)
		vec := ctr.orderVecs[i].Vec[0]
		// skip sort for const vector
		if !vec.IsConst() {
			nullCnt := vec.GetNulls().Count()
			if nullCnt < vec.Length() {
				for i, j := 0, len(ps); i < j; i++ {
					if i == j-1 {
						sort.Sort(desc, nullsLast, nullCnt > 0, ctr.sels[ps[i]:], vec)
					} else {
						sort.Sort(desc, nullsLast, nullCnt > 0, ctr.sels[ps[i]:ps[i+1]], vec)
					}
				}
			}
		}
		ovec = vec
		if n == i {
			ctr.ps = make([]int64, len(ps))
			copy(ctr.ps, ps)
		}
	}

	if n == i {
		ps = partition.Partition(ctr.sels, ds, ps, ovec)
		ctr.ps = make([]int64, len(ps))
		copy(ctr.ps, ps)
	} else if n == 0 {
		ctr.ps = nil
	}

	if len(ap.WinSpecList[idx].Expr.(*plan.Expr_W).W.OrderBy) > 0 {
		ctr.os = partition.Partition(ctr.sels, ds, ps, ovec)
	} else {
		ctr.os = nil
	}

	if err := bat.Shuffle(ctr.sels, proc.Mp()); err != nil {
		panic(err)
	}

	// shuffle agg vector
	for k := idx; k < len(ctr.aggVecs); k++ {
		if len(ctr.aggVecs[k].Vec) > 0 {
			if err := ctr.aggVecs[k].Vec[0].Shuffle(ctr.sels, proc.Mp()); err != nil {
				panic(err)
			}
		}
	}

	t := len(ctr.orderVecs) - 1
	if len(ctr.orderVecs[t].Vec) > 0 {
		if err := ctr.orderVecs[t].Vec[0].Shuffle(ctr.sels, proc.Mp()); err != nil {
			panic(err)
		}
	}

	ctr.ps = nil

	return false, nil
}

func searchLeft(start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, plus bool) (int, error) {
	if vec.GetNulls().Contains(uint64(rowIdx)) {
		return rowIdx, nil
	}
	var left int
	switch vec.GetType().Oid {
	case types.T_bit:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint64], genericGreater[uint64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint64], genericGreater[uint64])
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint64], genericGreater[uint64])
			}
		}
	case types.T_int8:
		col := vector.MustFixedColNoTypeCheck[int8](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int8], genericGreater[int8])
		} else {
			c := int8(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I8Val).I8Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int8], genericGreater[int8])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int8], genericGreater[int8])
			}
		}
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int16], genericGreater[int16])
		} else {
			c := int16(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I16Val).I16Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int16], genericGreater[int16])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int16], genericGreater[int16])
			}
		}
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int32], genericGreater[int32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I32Val).I32Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int32], genericGreater[int32])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int32], genericGreater[int32])
			}
		}
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int64], genericGreater[int64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int64], genericGreater[int64])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int64], genericGreater[int64])
			}
		}
	case types.T_uint8:
		col := vector.MustFixedColNoTypeCheck[uint8](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint8], genericGreater[uint8])
		} else {
			c := uint8(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U8Val).U8Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint8], genericGreater[uint8])
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint8], genericGreater[uint8])
			}
		}
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint16], genericGreater[uint16])
		} else {
			c := uint16(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U16Val).U16Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint16], genericGreater[uint16])
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint16], genericGreater[uint16])
			}
		}
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint32], genericGreater[uint32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U32Val).U32Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint32], genericGreater[uint32])
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint32], genericGreater[uint32])
			}
		}
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint64], genericGreater[uint64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[uint64], genericGreater[uint64])
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[uint64], genericGreater[uint64])
			}
		}
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[float32], genericGreater[float32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Fval).Fval
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[float32], genericGreater[float32])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[float32], genericGreater[float32])
			}
		}
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[float64], genericGreater[float64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Dval).Dval
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[float64], genericGreater[float64])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[float64], genericGreater[float64])
			}
		}
	case types.T_decimal64:
		col := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], decimal64Equal, decimal64Greater)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Decimal64Val).Decimal64Val.A
			if plus {
				fol, err := col[rowIdx].Add64(types.Decimal64(c))
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, decimal64Equal, decimal64Greater)
			} else {
				fol, err := col[rowIdx].Sub64(types.Decimal64(c))
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, decimal64Equal, decimal64Greater)
			}
		}
	case types.T_decimal128:
		col := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], decimal128Equal, decimal128Greater)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Decimal128Val).Decimal128Val
			if plus {
				fol, err := col[rowIdx].Add128(types.Decimal128{B0_63: uint64(c.A), B64_127: uint64(c.B)})
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, decimal128Equal, decimal128Greater)
			} else {
				fol, err := col[rowIdx].Sub128(types.Decimal128{B0_63: uint64(c.A), B64_127: uint64(c.B)})
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, decimal128Equal, decimal128Greater)
			}
		}
	case types.T_date:
		col := vector.MustFixedColNoTypeCheck[types.Date](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Date], genericGreater[types.Date])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				fol, err := doDateAdd(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Date], genericGreater[types.Date])
			} else {
				fol, err := doDateSub(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Date], genericGreater[types.Date])
			}
		}
	case types.T_datetime:
		col := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Datetime], genericGreater[types.Datetime])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				fol, err := doDatetimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Datetime], genericGreater[types.Datetime])
			} else {
				fol, err := doDatetimeSub(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Datetime], genericGreater[types.Datetime])
			}
		}
	case types.T_time:
		col := vector.MustFixedColNoTypeCheck[types.Time](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Time], genericGreater[types.Time])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				fol, err := doTimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Time], genericGreater[types.Time])
			} else {
				fol, err := doTimeSub(col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Time], genericGreater[types.Time])
			}
		}
	case types.T_timestamp:
		col := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Timestamp], genericGreater[types.Timestamp])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if plus {
				fol, err := doTimestampAdd(time.Local, col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Timestamp], genericGreater[types.Timestamp])
			} else {
				fol, err := doTimestampSub(time.Local, col[rowIdx], diff, unit)
				if err != nil {
					return left, err
				}
				left = genericSearchLeft(start, end-1, col, fol, genericEqual[types.Timestamp], genericGreater[types.Timestamp])
			}
		}
	}
	return left, nil
}

func doDateSub(start types.Date, diff int64, unit int64) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(-diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimeSub(start types.Time, diff int64, unit int64) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(-diff, types.IntervalType(unit))
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeSub(start types.Datetime, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(-diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampSub(loc *time.Location, start types.Timestamp, diff int64, unit int64) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(-diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func searchRight(start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, sub bool) (int, error) {
	if vec.GetNulls().Contains(uint64(rowIdx)) {
		return rowIdx + 1, nil
	}
	var right int
	switch vec.GetType().Oid {
	case types.T_bit:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint64], genericGreater[uint64])
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint64], genericGreater[uint64])
			}
		}
	case types.T_int8:
		col := vector.MustFixedColNoTypeCheck[int8](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[int8])
		} else {
			c := int8(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I8Val).I8Val)
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int8], genericGreater[int8])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int8], genericGreater[int8])
			}
		}
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[int16])
		} else {
			c := int16(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I16Val).I16Val)
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int16], genericGreater[int16])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int16], genericGreater[int16])
			}
		}
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[int32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I32Val).I32Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int32], genericGreater[int32])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int32], genericGreater[int32])
			}
		}
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[int64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int64], genericGreater[int64])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int64], genericGreater[int64])
			}
		}
	case types.T_uint8:
		col := vector.MustFixedColNoTypeCheck[uint8](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint8])
		} else {
			c := uint8(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U8Val).U8Val)
			if sub {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint8], genericGreater[uint8])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint8], genericGreater[uint8])
			}
		}
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint16])
		} else {
			c := uint16(expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U16Val).U16Val)
			if sub {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint16], genericGreater[uint16])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint16], genericGreater[uint16])
			}
		}
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U32Val).U32Val
			if sub {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint32], genericGreater[uint32])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint32], genericGreater[uint32])
			}
		}
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[uint64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_U64Val).U64Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[uint64], genericGreater[uint64])
			} else {
				if col[rowIdx] <= c {
					return start, nil
				}
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[uint64], genericGreater[uint64])
			}
		}
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[float32])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Fval).Fval
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[float32], genericGreater[float32])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[float32], genericGreater[float32])
			}
		}
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[float64])
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Dval).Dval
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[float64], genericGreater[float64])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[float64], genericGreater[float64])
			}
		}
	case types.T_decimal64:
		col := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], decimal64Equal)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Decimal64Val).Decimal64Val.A
			if sub {
				fol, err := col[rowIdx].Sub64(types.Decimal64(c))
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, decimal64Equal, decimal64Greater)
			} else {
				fol, err := col[rowIdx].Add64(types.Decimal64(c))
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, decimal64Equal, decimal64Greater)
			}
		}
	case types.T_decimal128:
		col := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], decimal128Equal)
		} else {
			c := expr.Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_Decimal128Val).Decimal128Val
			if sub {
				fol, err := col[rowIdx].Sub128(types.Decimal128{B0_63: uint64(c.A), B64_127: uint64(c.B)})
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, decimal128Equal, decimal128Greater)
			} else {
				fol, err := col[rowIdx].Add128(types.Decimal128{B0_63: uint64(c.A), B64_127: uint64(c.B)})
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, decimal128Equal, decimal128Greater)
			}
		}
	case types.T_date:
		col := vector.MustFixedColNoTypeCheck[types.Date](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[types.Date])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				fol, err := doDateSub(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Date], genericGreater[types.Date])
			} else {
				fol, err := doDateAdd(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Date], genericGreater[types.Date])
			}
		}
	case types.T_datetime:
		col := vector.MustFixedColNoTypeCheck[types.Datetime](vec)
		i := start
		for ; i < end; i++ {
			if !vec.GetNulls().Contains(uint64(i)) {
				break
			}
		}
		for j := start; j < i; j++ {
			col[j] = col[i]
		}
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[types.Datetime])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				fol, err := doDatetimeSub(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Datetime], genericGreater[types.Datetime])
			} else {
				fol, err := doDatetimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Datetime], genericGreater[types.Datetime])
			}
		}
	case types.T_time:
		col := vector.MustFixedColNoTypeCheck[types.Time](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[types.Time])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				fol, err := doTimeSub(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Time], genericGreater[types.Time])
			} else {
				fol, err := doTimeAdd(col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Time], genericGreater[types.Time])
			}
		}
	case types.T_timestamp:
		col := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)
		if expr == nil {
			right = genericSearchEqualRight(rowIdx, end-1, col, col[rowIdx], genericEqual[types.Timestamp])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val
			if sub {
				fol, err := doTimestampSub(time.Local, col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Timestamp], genericGreater[types.Timestamp])
			} else {
				fol, err := doTimestampAdd(time.Local, col[rowIdx], diff, unit)
				if err != nil {
					return right, err
				}
				right = genericSearchRight(start, end-1, col, fol, genericEqual[types.Timestamp], genericGreater[types.Timestamp])
			}
		}
	}
	return right + 1, nil
}

func doDateAdd(start types.Date, diff int64, unit int64) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(diff, types.IntervalType(unit), types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimeAdd(start types.Time, diff int64, unit int64) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(diff, types.IntervalType(unit))
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeAdd(start types.Datetime, diff int64, unit int64) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doTimestampAdd(loc *time.Location, start types.Timestamp, diff int64, unit int64) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, types.IntervalType(unit))
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(diff, types.IntervalType(unit), types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func genericSearchLeft[T any](low, high int, nums []T, target T, equal func(a, b T) bool, greater func(a, b T) bool) int {
	for low <= high {
		mid := low + (high-low)/2
		if equal(nums[mid], target) {
			high = mid - 1
		} else if greater(nums[mid], target) {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return low
}

func genericSearchRight[T any](low, high int, nums []T, target T, equal func(a, b T) bool, greater func(a, b T) bool) int {
	for low <= high {
		mid := low + (high-low)/2
		if equal(nums[mid], target) {
			low = mid + 1
		} else if greater(nums[mid], target) {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	return high
}

func genericSearchEqualRight[T any](low, high int, nums []T, target T, equal func(a, b T) bool) int {
	i := low + 1
	for ; i <= high; i++ {
		if !equal(nums[i], target) {
			break
		}
	}
	return i - 1
}

func genericEqual[T types.OrderedT](a, b T) bool {
	return a == b
}

func genericGreater[T types.OrderedT](a, b T) bool {
	return a > b
}

func decimal64Equal(a, b types.Decimal64) bool {
	return a.Compare(b) == 0
}

func decimal64Greater(a, b types.Decimal64) bool {
	return a.Compare(b) == 1
}

func decimal128Equal(a, b types.Decimal128) bool {
	return a.Compare(b) == 0
}

func decimal128Greater(a, b types.Decimal128) bool {
	return a.Compare(b) == 1
}
