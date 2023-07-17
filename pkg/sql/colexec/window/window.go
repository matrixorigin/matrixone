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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("window")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, true)

	ctr := ap.ctr
	ctr.aggVecs = make([]evalVector, len(ap.Aggs))
	for i, ag := range ap.Aggs {
		if ag.E != nil {
			ctr.aggVecs[i].executor, err = colexec.NewExpressionExecutor(proc, ag.E)
			if err != nil {
				return err
			}
			// very bad code.
			exprTyp := ag.E.Typ
			typ := types.New(types.T(exprTyp.Id), exprTyp.Width, exprTyp.Scale)
			ctr.aggVecs[i].vec = vector.NewVec(typ)
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst, isLast bool) (bool, error) {
	var err error
	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	for {
		bat, end, err := ctr.ReceiveFromAllRegs(anal)
		if err != nil {
			return false, err
		}

		bat.FixedForRemoveZs()
		if end {
			break
		}
		anal.Input(bat, isFirst)

		if ctr.bat == nil {
			ctr.bat = bat
			continue
		}
		for i := range bat.Vecs {
			n := bat.Vecs[i].Length()
			err = ctr.bat.Vecs[i].UnionBatch(bat.Vecs[i], 0, n, makeFlagsOne(n), proc.Mp())
			if err != nil {
				return false, err
			}
		}
		ctr.bat.Zs = append(ctr.bat.Zs, bat.Zs...)
		ctr.bat.AddRowCount(bat.RowCount())
	}

	// init agg frame
	if ctr.bat == nil {
		proc.SetInputBatch(ctr.bat)
		return true, nil
	}
	n := ctr.bat.Vecs[0].Length()
	if err = ctr.evalAggVector(ctr.bat, proc); err != nil {
		return false, err
	}

	ctr.bat.Aggs = make([]agg.Agg[any], len(ap.Aggs))
	for i, ag := range ap.Aggs {
		if ctr.bat.Aggs[i], err = agg.New(ag.Op, ag.Dist, ap.Types[i]); err != nil {
			return false, err
		}
		if err = ctr.bat.Aggs[i].Grows(n, proc.Mp()); err != nil {
			return false, err
		}
	}

	// calculate
	for i, w := range ap.WinSpecList {
		// sort and partitions
		if ap.Fs = makeOrderBy(w); ap.Fs != nil {
			ctr.orderVecs = make([]evalVector, len(ap.Fs))
			for j := range ctr.orderVecs {
				ctr.orderVecs[j].executor, err = colexec.NewExpressionExecutor(proc, ap.Fs[j].Expr)
				if err != nil {
					return false, err
				}
			}
			_, err = ctr.processOrder(i, ap, ctr.bat, proc)
			if err != nil {
				ap.Free(proc, true)
				return false, err
			}
		}
		// evaluate func
		if err = ctr.processFunc(i, ap, proc, anal); err != nil {
			return false, err
		}

		// clean
		ctr.cleanOrderVectors(proc.Mp())
	}

	anal.Output(ctr.bat, isLast)

	ctr.bat.CheckForRemoveZs("window")
	proc.SetInputBatch(ctr.bat)
	return true, nil
}

func (ctr *container) processFunc(idx int, ap *Argument, proc *process.Process, anal process.Analyze) error {
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

					if err = ctr.bat.Aggs[idx].Fill(int64(p-1), int64(o), []*vector.Vector{vec}); err != nil {
						return err
					}

				} else {
					o--
					break
				}

			}
		}
	} else {
		nullVec := vector.NewConstNull(*ctr.aggVecs[idx].vec.GetType(), 1, proc.Mp())
		defer nullVec.Free(proc.Mp())

		// plan.Function_AGG, plan.Function_WIN_VALUE
		for j := 0; j < n; j++ {

			start, end := 0, ctr.bat.Vecs[0].Length()

			if ctr.ps != nil {
				start, end = buildPartitionInterval(ctr.ps, j, n)
			}

			left, right, err := ctr.buildInterval(j, start, end, ap.WinSpecList[idx].Expr.(*plan.Expr_W).W.Frame, proc)
			if err != nil {
				return err
			}

			if right < start || left > end || left >= right {
				if err = ctr.bat.Aggs[idx].Fill(int64(j), int64(0), []*vector.Vector{nullVec}); err != nil {
					return err
				}
				continue
			}

			if left < start {
				left = start
			}
			if right > end {
				right = end
			}

			for k := left; k < right; k++ {
				if err = ctr.bat.Aggs[idx].Fill(int64(j), int64(k), []*vector.Vector{ctr.aggVecs[idx].vec}); err != nil {
					return err
				}
			}

		}
	}

	vec, err := ctr.bat.Aggs[idx].Eval(proc.Mp())
	if err != nil {
		return err
	}
	if isWinOrder {
		vec.SetNulls(nil)
	}
	ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
	if vec != nil {
		anal.Alloc(int64(vec.Size()))
	}
	return nil
}

func (ctr *container) buildInterval(rowIdx, start, end int, frame *plan.FrameClause, proc *process.Process) (int, int, error) {
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
			pre := frame.Start.Val.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U64Val).U64Val
			start = rowIdx - int(pre)
		}
	case plan.FrameBound_FOLLOWING:
		fol := frame.Start.Val.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U64Val).U64Val
		start = rowIdx + int(fol)
	}

	switch frame.End.Type {
	case plan.FrameBound_CURRENT_ROW:
		end = rowIdx + 1
	case plan.FrameBound_PRECEDING:
		pre := frame.End.Val.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U64Val).U64Val
		end = rowIdx - int(pre) + 1
	case plan.FrameBound_FOLLOWING:
		if !frame.End.UnBounded {
			fol := frame.End.Val.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U64Val).U64Val
			end = rowIdx + int(fol) + 1
		}
	}
	return start, end
}

func (ctr *container) buildRangeInterval(rowIdx int, start, end int, frame *plan.FrameClause) (int, int, error) {
	var err error
	switch frame.Start.Type {
	case plan.FrameBound_CURRENT_ROW:
		start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].vec, nil, false)
		if err != nil {
			return start, end, err
		}
	case plan.FrameBound_PRECEDING:
		if !frame.Start.UnBounded {
			start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].vec, frame.Start.Val, false)
			if err != nil {
				return start, end, err
			}
		}
	case plan.FrameBound_FOLLOWING:
		start, err = searchLeft(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].vec, frame.Start.Val, true)
		if err != nil {
			return start, end, err
		}
	}

	switch frame.End.Type {
	case plan.FrameBound_CURRENT_ROW:
		end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].vec, nil, false)
		if err != nil {
			return start, end, err
		}
	case plan.FrameBound_PRECEDING:
		end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].vec, frame.End.Val, true)
		if err != nil {
			return start, end, err
		}
	case plan.FrameBound_FOLLOWING:
		if !frame.End.UnBounded {
			end, err = searchRight(start, end, rowIdx, ctr.orderVecs[len(ctr.orderVecs)-1].vec, frame.End.Val, false)
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

func (ctr *container) evalAggVector(bat *batch.Batch, proc *process.Process) error {
	for i := range ctr.aggVecs {
		if ctr.aggVecs[i].executor != nil {
			vec, err := ctr.aggVecs[i].executor.Eval(proc, []*batch.Batch{bat})
			if err != nil {
				return err
			}
			ctr.aggVecs[i].vec = vec
		}
	}
	return nil
}

func makeArgFs(ap *Argument) {
	ap.ctr.desc = make([]bool, len(ap.Fs))
	ap.ctr.nullsLast = make([]bool, len(ap.Fs))
	for i, f := range ap.Fs {
		ap.ctr.desc[i] = f.Flag&plan.OrderBySpec_DESC != 0
		if f.Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
			ap.ctr.nullsLast[i] = false
		} else if f.Flag&plan.OrderBySpec_NULLS_LAST != 0 {
			ap.ctr.nullsLast[i] = true
		} else {
			ap.ctr.nullsLast[i] = ap.ctr.desc[i]
		}
	}
}

func makeOrderBy(expr *plan.Expr) []*plan.OrderBySpec {
	w := expr.Expr.(*plan.Expr_W).W
	if w.PartitionBy == nil && w.OrderBy == nil {
		return nil
	}
	orderBy := make([]*plan.OrderBySpec, 0, len(w.PartitionBy)+len(w.OrderBy))
	for _, p := range w.PartitionBy {
		orderBy = append(orderBy, &plan.OrderBySpec{
			Expr: p,
			Flag: plan.OrderBySpec_INTERNAL,
		})
	}
	orderBy = append(orderBy, w.OrderBy...)
	return orderBy
}

func makeFlagsOne(n int) []uint8 {
	t := make([]uint8, n)
	for i := range t {
		t[i]++
	}
	return t
}

func (ctr *container) processOrder(idx int, ap *Argument, bat *batch.Batch, proc *process.Process) (bool, error) {
	makeArgFs(ap)

	for i := range ctr.orderVecs {
		vec, err := ctr.orderVecs[i].executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			return false, err
		}
		ctr.orderVecs[i].vec = vec
	}

	ovec := ctr.orderVecs[0].vec
	var strCol []string

	if ctr.sels == nil {
		ctr.sels = make([]int64, len(bat.Zs))
	}
	for i := 0; i < len(bat.Zs); i++ {
		ctr.sels[i] = int64(i)
	}

	// skip sort for const vector
	if !ovec.IsConst() {
		nullCnt := ovec.GetNulls().Count()
		if nullCnt < ovec.Length() {
			if ovec.GetType().IsVarlen() {
				strCol = vector.MustStrCol(ovec)
			} else {
				strCol = nil
			}
			sort.Sort(ctr.desc[0], ctr.nullsLast[0], nullCnt > 0, ctr.sels, ovec, strCol)
		}
	}

	ps := make([]int64, 0, 16)
	ds := make([]bool, len(ctr.sels))

	n := len(ap.WinSpecList[idx].Expr.(*plan.Expr_W).W.PartitionBy)

	i, j := 1, len(ctr.orderVecs)
	for ; i < j; i++ {
		desc := ctr.desc[i]
		nullsLast := ctr.nullsLast[i]
		ps = partition.Partition(ctr.sels, ds, ps, ovec)
		vec := ctr.orderVecs[i].vec
		// skip sort for const vector
		if !vec.IsConst() {
			nullCnt := vec.GetNulls().Count()
			if nullCnt < vec.Length() {
				if vec.GetType().IsVarlen() {
					strCol = vector.MustStrCol(vec)
				} else {
					strCol = nil
				}
				for i, j := 0, len(ps); i < j; i++ {
					if i == j-1 {
						sort.Sort(desc, nullsLast, nullCnt > 0, ctr.sels[ps[i]:], vec, strCol)
					} else {
						sort.Sort(desc, nullsLast, nullCnt > 0, ctr.sels[ps[i]:ps[i+1]], vec, strCol)
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
	if ctr.aggVecs[idx].vec != nil && !ctr.aggVecs[idx].executor.IsColumnExpr() {
		if err := ctr.aggVecs[idx].vec.Shuffle(ctr.sels, proc.Mp()); err != nil {
			panic(err)
		}
	}

	return false, nil
}

func searchLeft(start, end, rowIdx int, vec *vector.Vector, expr *plan.Expr, plus bool) (int, error) {
	if vec.GetNulls().Contains(uint64(rowIdx)) {
		return rowIdx, nil
	}
	var left int
	switch vec.GetType().Oid {
	case types.T_int8:
		col := vector.MustFixedCol[int8](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int8], genericGreater[int8])
		} else {
			c := int8(expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_I8Val).I8Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int8], genericGreater[int8])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int8], genericGreater[int8])
			}
		}
	case types.T_int16:
		col := vector.MustFixedCol[int16](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int16], genericGreater[int16])
		} else {
			c := int16(expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_I16Val).I16Val)
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int16], genericGreater[int16])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int16], genericGreater[int16])
			}
		}
	case types.T_int32:
		col := vector.MustFixedCol[int32](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int32], genericGreater[int32])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_I32Val).I32Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int32], genericGreater[int32])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int32], genericGreater[int32])
			}
		}
	case types.T_int64:
		col := vector.MustFixedCol[int64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[int64], genericGreater[int64])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[int64], genericGreater[int64])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[int64], genericGreater[int64])
			}
		}
	case types.T_uint8:
		col := vector.MustFixedCol[uint8](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint8], genericGreater[uint8])
		} else {
			c := uint8(expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U8Val).U8Val)
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
		col := vector.MustFixedCol[uint16](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint16], genericGreater[uint16])
		} else {
			c := uint16(expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U16Val).U16Val)
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
		col := vector.MustFixedCol[uint32](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint32], genericGreater[uint32])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U32Val).U32Val
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
		col := vector.MustFixedCol[uint64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[uint64], genericGreater[uint64])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U64Val).U64Val
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
		col := vector.MustFixedCol[float32](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[float32], genericGreater[float32])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Fval).Fval
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[float32], genericGreater[float32])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[float32], genericGreater[float32])
			}
		}
	case types.T_float64:
		col := vector.MustFixedCol[float64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[float64], genericGreater[float64])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Dval).Dval
			if plus {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]+c, genericEqual[float64], genericGreater[float64])
			} else {
				left = genericSearchLeft(start, end-1, col, col[rowIdx]-c, genericEqual[float64], genericGreater[float64])
			}
		}
	case types.T_decimal64:
		col := vector.MustFixedCol[types.Decimal64](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], decimal64Equal, decimal64Greater)
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Decimal64Val).Decimal64Val.A
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
		col := vector.MustFixedCol[types.Decimal128](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], decimal128Equal, decimal128Greater)
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Decimal128Val).Decimal128Val
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
		col := vector.MustFixedCol[types.Date](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Date], genericGreater[types.Date])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
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
		col := vector.MustFixedCol[types.Datetime](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Datetime], genericGreater[types.Datetime])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
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
		col := vector.MustFixedCol[types.Time](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Time], genericGreater[types.Time])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
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
		col := vector.MustFixedCol[types.Timestamp](vec)
		if expr == nil {
			left = genericSearchLeft(start, end-1, col, col[rowIdx], genericEqual[types.Timestamp], genericGreater[types.Timestamp])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
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
	case types.T_int8:
		col := vector.MustFixedCol[int8](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[int8], genericGreater[int8])
		} else {
			c := int8(expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_I8Val).I8Val)
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int8], genericGreater[int8])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int8], genericGreater[int8])
			}
		}
	case types.T_int16:
		col := vector.MustFixedCol[int16](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[int16], genericGreater[int16])
		} else {
			c := int16(expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_I16Val).I16Val)
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int16], genericGreater[int16])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int16], genericGreater[int16])
			}
		}
	case types.T_int32:
		col := vector.MustFixedCol[int32](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[int32], genericGreater[int32])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_I32Val).I32Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int32], genericGreater[int32])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int32], genericGreater[int32])
			}
		}
	case types.T_int64:
		col := vector.MustFixedCol[int64](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[int64], genericGreater[int64])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[int64], genericGreater[int64])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[int64], genericGreater[int64])
			}
		}
	case types.T_uint8:
		col := vector.MustFixedCol[uint8](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[uint8], genericGreater[uint8])
		} else {
			c := uint8(expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U8Val).U8Val)
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
		col := vector.MustFixedCol[uint16](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[uint16], genericGreater[uint16])
		} else {
			c := uint16(expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U16Val).U16Val)
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
		col := vector.MustFixedCol[uint32](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[uint32], genericGreater[uint32])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U32Val).U32Val
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
		col := vector.MustFixedCol[uint64](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[uint64], genericGreater[uint64])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_U64Val).U64Val
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
		col := vector.MustFixedCol[float32](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[float32], genericGreater[float32])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Fval).Fval
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[float32], genericGreater[float32])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[float32], genericGreater[float32])
			}
		}
	case types.T_float64:
		col := vector.MustFixedCol[float64](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[float64], genericGreater[float64])
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Dval).Dval
			if sub {
				right = genericSearchRight(start, end-1, col, col[rowIdx]-c, genericEqual[float64], genericGreater[float64])
			} else {
				right = genericSearchRight(start, end-1, col, col[rowIdx]+c, genericEqual[float64], genericGreater[float64])
			}
		}
	case types.T_decimal64:
		col := vector.MustFixedCol[types.Decimal64](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], decimal64Equal, decimal64Greater)
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Decimal64Val).Decimal64Val.A
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
		col := vector.MustFixedCol[types.Decimal128](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], decimal128Equal, decimal128Greater)
		} else {
			c := expr.Expr.(*plan.Expr_C).C.Value.(*plan.Const_Decimal128Val).Decimal128Val
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
		col := vector.MustFixedCol[types.Date](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[types.Date], genericGreater[types.Date])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
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
		col := vector.MustFixedCol[types.Datetime](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[types.Datetime], genericGreater[types.Datetime])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
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
		col := vector.MustFixedCol[types.Time](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[types.Time], genericGreater[types.Time])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
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
		col := vector.MustFixedCol[types.Timestamp](vec)
		if expr == nil {
			right = genericSearchRight(start, end-1, col, col[rowIdx], genericEqual[types.Timestamp], genericGreater[types.Timestamp])
		} else {
			diff := expr.Expr.(*plan.Expr_List).List.List[0].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
			unit := expr.Expr.(*plan.Expr_List).List.List[1].Expr.(*plan.Expr_C).C.Value.(*plan.Const_I64Val).I64Val
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
