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

package mergeorder

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxBatchSizeToSend = 64 * mpool.MB

type Argument struct {
	ctr *container

	OrderInformation []*plan.OrderBySpec
}

type container struct {
	colexec.ReceiverOperator

	// receiveOver and sendOver are the flags to indicate whether
	// this operator has received all the data from the child pipelines.
	// and send all the data to the next operator.
	receiveOver bool
	sendOver    bool

	// batchList is the data structure to store the all the received batches
	batchList []*batch.Batch
	orderCols [][]*vector.Vector
	// indexList[i] = k means the number of rows before k in batchList[i] has been merged and send.
	indexList []int64

	// expression executors for order columns.
	executors []colexec.ExpressionExecutor
	compares  []compare.Compare
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if arg.ctr != nil {
		mp := proc.Mp()
		ctr := arg.ctr
		for i := range ctr.batchList {
			if ctr.batchList[i] != nil {
				ctr.batchList[i].Clean(mp)
			}
		}
		for i := range ctr.orderCols {
			if ctr.orderCols[i] != nil {
				for j := range ctr.orderCols[i] {
					if ctr.orderCols[i][j] != nil {
						ctr.orderCols[i][j].Free(mp)
					}
				}
			}
		}
		for i := range ctr.executors {
			if ctr.executors[i] != nil {
				ctr.executors[i].Free()
			}
		}
	}
}

func (ctr *container) mergeAndEvaluateOrderColumn(proc *process.Process, bat *batch.Batch) error {
	ctr.batchList = append(ctr.batchList, bat)
	ctr.orderCols = append(ctr.orderCols, nil)
	// if only one batch, no need to evaluate the order column.
	if len(ctr.batchList) == 1 {
		return nil
	}

	index := len(ctr.orderCols) - 1
	return ctr.evaluateOrderColumn(proc, index)
}

func (ctr *container) evaluateOrderColumn(proc *process.Process, index int) error {
	inputs := []*batch.Batch{ctr.batchList[index]}

	ctr.orderCols[index] = make([]*vector.Vector, len(ctr.executors))
	for i := 0; i < len(ctr.executors); i++ {
		vec, err := ctr.executors[i].Eval(proc, inputs)
		if err != nil {
			return err
		}
		ctr.orderCols[index][i] = vec
	}
	return nil
}

func (ctr *container) generateCompares(fs []*plan.OrderBySpec) {
	var desc, nullsLast bool

	ctr.compares = make([]compare.Compare, len(fs))
	for i := range ctr.compares {
		desc = fs[i].Flag&plan2.OrderBySpec_DESC != 0
		if fs[i].Flag&plan2.OrderBySpec_NULLS_FIRST != 0 {
			nullsLast = false
		} else if fs[i].Flag&plan2.OrderBySpec_NULLS_LAST != 0 {
			nullsLast = true
		} else {
			nullsLast = desc
		}

		exprTyp := fs[i].Expr.Typ
		typ := types.New(types.T(exprTyp.Id), exprTyp.Width, exprTyp.Scale)
		ctr.compares[i] = compare.New(typ, desc, nullsLast)
	}
	return
}

func (ctr *container) pickAndSend(proc *process.Process) error {
	bat := batch.NewWithSize(ctr.batchList[0].VectorCount())
	mp := proc.Mp()

	for i := range bat.Vecs {
		bat.Vecs[i] = proc.GetVector(*ctr.batchList[0].Vecs[i].GetType())
	}

	for {
		choice := ctr.pickFirstRow()
		for j := range bat.Vecs {
			err := bat.Vecs[j].UnionOne(ctr.batchList[choice].Vecs[j], ctr.indexList[choice], mp)
			if err != nil {
				bat.Clean(mp)
				return err
			}
		}
		ctr.indexList[choice]++
		if ctr.indexList[choice] == int64(ctr.orderCols[choice][0].Length()) {
			ctr.removeBatch(proc, choice)
		}

		if len(ctr.indexList) == 0 {
			ctr.sendOver = true
			break
		}
		if bat.Size() >= maxBatchSizeToSend {
			break
		}
	}

	proc.SetInputBatch(bat)
	return nil
}

func (ctr *container) pickFirstRow() (batIndex int) {
	l := len(ctr.indexList)

	if l > 1 {
		i, j := 0, 1
		for j < l {
			for k := 0; k < len(ctr.compares); k++ {
				ctr.compares[k].Set(0, ctr.orderCols[i][k])
				ctr.compares[k].Set(1, ctr.orderCols[j][k])
				result := ctr.compares[k].Compare(0, 1, ctr.indexList[i], ctr.indexList[j])
				if result < 0 || k == len(ctr.compares)-1 {
					break
				} else if result > 0 {
					i = j
					break
				}
			}
			j++
		}
		return i
	}
	return 0
}

func (ctr *container) removeBatch(proc *process.Process, index int) {
	bat := ctr.batchList[index]
	for i := range bat.Vecs {
		proc.PutVector(bat.Vecs[i])
	}
	ctr.batchList = append(ctr.batchList[:index], ctr.batchList[index+1:]...)
	ctr.indexList = append(ctr.indexList[:index], ctr.indexList[index+1:]...)

	cols := ctr.orderCols[index]
	for i := range cols {
		proc.PutVector(cols[i])
	}
	ctr.orderCols = append(ctr.orderCols[:index], ctr.orderCols[index+1:]...)
}

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("mergeorder([")
	for i, f := range ap.OrderInformation {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func Prepare(proc *process.Process, arg any) (err error) {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ctr := ap.ctr

	length := 2 * len(proc.Reg.MergeReceivers)
	ctr.batchList = make([]*batch.Batch, 0, length)
	ctr.orderCols = make([][]*vector.Vector, 0, length)

	for i := range ap.ctr.executors {
		ap.ctr.executors[i], err = colexec.NewExpressionExecutor(proc, ap.OrderInformation[i].Expr)
		if err != nil {
			return err
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr

	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	// get batch from receiver and do merge sort.
	// but do not really sort, just get the order.
	if !ctr.receiveOver {
		for {
			bat, end, err := ctr.ReceiveFromAllRegs(anal)
			if err != nil {
				return false, err
			}
			if end {
				// If only one batch, no need to sort. just send it.
				if len(ctr.batchList) == 1 {
					proc.SetInputBatch(ctr.batchList[0])
					ctr.batchList[0] = nil
					return true, nil
				}

				// evaluate the first batch's order column.
				if err = ctr.evaluateOrderColumn(proc, 0); err != nil {
					return false, err
				}
				ctr.generateCompares(ap.OrderInformation)
				ctr.indexList = make([]int64, len(ctr.batchList))

				ctr.receiveOver = true
				break
			}
			if err = ctr.mergeAndEvaluateOrderColumn(proc, bat); err != nil {
				return false, err
			}
		}
	}

	// then use the order to get the data from the batch.
	err := ctr.pickAndSend(proc)
	return ctr.sendOver, err
}

//import (
//	"bytes"
//
//	"github.com/matrixorigin/matrixone/pkg/compare"
//	"github.com/matrixorigin/matrixone/pkg/container/batch"
//	"github.com/matrixorigin/matrixone/pkg/container/vector"
//	"github.com/matrixorigin/matrixone/pkg/pb/plan"
//	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
//	"github.com/matrixorigin/matrixone/pkg/vm/process"
//)
//
//func String(arg any, buf *bytes.Buffer) {
//	ap := arg.(*Argument)
//	buf.WriteString("mergeorder([")
//	for i, f := range ap.Fs {
//		if i > 0 {
//			buf.WriteString(", ")
//		}
//		buf.WriteString(f.String())
//	}
//	buf.WriteString("])")
//}
//
//func Prepare(proc *process.Process, arg any) (err error) {
//	ap := arg.(*Argument)
//	ap.ctr = new(container)
//	ap.ctr.InitReceiver(proc, true)
//	ap.ctr.poses = make([]int32, 0, len(ap.Fs))
//
//	ap.ctr.compare0Index = make([]int32, len(ap.Fs))
//	ap.ctr.compare1Index = make([]int32, len(ap.Fs))
//
//	ap.ctr.executorsForOrderList = make([]colexec.ExpressionExecutor, len(ap.Fs))
//	for i := range ap.ctr.executorsForOrderList {
//		ap.ctr.executorsForOrderList[i], err = colexec.NewExpressionExecutor(proc, ap.Fs[i].Expr)
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//}
//
//func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
//	var err error
//
//	ap := arg.(*Argument)
//	ctr := ap.ctr
//	anal := proc.GetAnalyze(idx)
//	anal.Start()
//	defer anal.Stop()
//
//	// get batch from merge receivers and do merge sort.
//	// save the unordered result in ctr.bat.
//	// save the ordered index list in ctr.finalSelectList
//	for {
//		bat, end, err := ctr.ReceiveFromAllRegs(anal)
//		if err != nil || end {
//			break
//		}
//
//		anal.Input(bat, isFirst)
//		if err = mergeSort(proc, bat, ap, ctr, anal); err != nil {
//			break
//		}
//	}
//	if err != nil {
//		ap.Free(proc, true)
//		return false, err
//	}
//
//	// remove and clean unnecessary vector
//	// shuffle the ctr.bat
//	if ctr.bat != nil {
//		for i := ctr.n; i < len(ctr.bat.Vecs); i++ {
//			ctr.bat.Vecs[i].Free(proc.Mp())
//		}
//		ctr.bat.Vecs = ctr.bat.Vecs[:ctr.n]
//	}
//	if err = ctr.bat.Shuffle(ctr.finalSelectList, proc.Mp()); err != nil {
//		ap.Free(proc, true)
//		return false, err
//	}
//
//	// output the sort result.
//	anal.Output(ctr.bat, isLast)
//	proc.SetInputBatch(ctr.bat)
//	ctr.bat = nil
//
//	// free and return
//	ap.Free(proc, false)
//	return true, nil
//}
//
//func mergeSort(proc *process.Process, bat2 *batch.Batch,
//	ap *Argument, ctr *container, anal process.Analyze) error {
//	ctr.n = len(bat2.Vecs)
//	ctr.poses = ctr.poses[:0]
//
//	// evaluate the order column.
//	for i := range ctr.executorsForOrderList {
//		vec, err := ctr.executorsForOrderList[i].Eval(proc, []*batch.Batch{bat2})
//		if err != nil {
//			return err
//		}
//		newColumn := true
//		for j := range bat2.Vecs {
//			if bat2.Vecs[j] == vec {
//				newColumn = false
//				ctr.poses = append(ctr.poses, int32(j))
//				break
//			}
//		}
//		if newColumn {
//			ctr.poses = append(ctr.poses, int32(len(bat2.Vecs)))
//			nv, err := colexec.SafeGetResult(proc, vec, ctr.executorsForOrderList[i])
//			if err != nil {
//				return err
//			}
//			bat2.Vecs = append(bat2.Vecs, nv)
//			anal.Alloc(int64(nv.Size()))
//		}
//	}
//	copy(ctr.compare1Index, ctr.poses)
//
//	// init the compare structure if first time.
//	if len(ctr.cmps) == 0 {
//		var desc, nullsLast bool
//		ctr.cmps = make([]compare.Compare, len(ap.Fs))
//		for i := range ctr.cmps {
//			desc = ap.Fs[i].Flag&plan.OrderBySpec_DESC != 0
//			if ap.Fs[i].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
//				nullsLast = false
//			} else if ap.Fs[i].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
//				nullsLast = true
//			} else {
//				nullsLast = desc
//			}
//			ctr.cmps[i] = compare.New(*bat2.Vecs[ctr.poses[i]].GetType(), desc, nullsLast)
//		}
//	}
//
//	return ctr.mergeSort2(bat2, proc)
//}
//
//func (ctr *container) mergeSort2(bat2 *batch.Batch, proc *process.Process) error {
//	if ctr.bat == nil {
//		ctr.bat = bat2
//		for i, vec := range ctr.bat.Vecs {
//			if vec.IsConst() {
//				typ := *vec.GetType()
//				rvec := vector.NewVec(typ)
//				if err := vector.GetUnionAllFunction(typ, proc.Mp())(rvec, vec); err != nil {
//					return err
//				}
//				ctr.bat.Vecs[i] = rvec
//				vec.Free(proc.Mp())
//			}
//		}
//		ctr.finalSelectList = generateSelectList(int64(ctr.bat.Length()))
//		copy(ctr.compare0Index, ctr.poses)
//		return nil
//	}
//	bat1 := ctr.bat
//	// union bat1 and bat2
//	// do merge sort, get order index list.
//	s1, s2 := int64(0), int64(bat1.Vecs[0].Length()) // startIndexOfBat1, startIndexOfBat2
//
//	for i := range bat1.Vecs {
//		n := bat2.Vecs[i].Length()
//		if cap(ctr.unionFlag) >= n {
//			ctr.unionFlag = ctr.unionFlag[:n:cap(ctr.unionFlag)]
//		} else {
//			ctr.unionFlag = makeFlagsOne(n)
//		}
//		err := bat1.Vecs[i].UnionBatch(bat2.Vecs[i], 0, n, ctr.unionFlag, proc.Mp())
//		if err != nil {
//			return err
//		}
//	}
//	bat1.Zs = append(bat1.Zs, bat2.Zs...)
//
//	// set cmp should after union work to avoid memory re-alloc while union.
//	for i, cmp := range ctr.cmps {
//		cmp.Set(0, bat1.GetVector(ctr.compare0Index[i]))
//		cmp.Set(1, bat2.GetVector(ctr.compare1Index[i]))
//	}
//
//	end1, end2 := s2, int64(bat1.Vecs[0].Length())
//	sels := make([]int64, 0, end2)
//
//	// set up cmp must happen after vector.UnionBatch.  UnionBatch may grow the vector
//	// in bat1, which could cause a realloc.  Depending on mpool has fixed pool, the old
//	// vector maybe destroyed, cmp then set a garbage vector.
//	for i, cmp := range ctr.cmps {
//		cmp.Set(0, bat1.GetVector(ctr.compare0Index[i]))
//		cmp.Set(1, bat2.GetVector(ctr.compare1Index[i]))
//	}
//
//	for s1 < end1 && s2 < end2 {
//		i := s1
//		j := s2 - end1
//		compareResult := 0
//		for k := range ctr.cmps {
//			compareResult = ctr.cmps[k].Compare(0, 1, ctr.finalSelectList[i], j)
//			if compareResult != 0 {
//				break
//			}
//		}
//		if compareResult <= 0 {
//			// weight of item1 is less or equal to item2
//			sels = append(sels, ctr.finalSelectList[s1])
//			s1++
//		} else {
//			sels = append(sels, s2)
//			s2++
//		}
//	}
//	for s1 < end1 {
//		sels = append(sels, ctr.finalSelectList[s1])
//		s1++
//	}
//	for s2 < end2 {
//		sels = append(sels, s2)
//		s2++
//	}
//	ctr.finalSelectList = sels
//	ctr.bat = bat1
//	proc.PutBatch(bat2)
//	return nil
//}
//
//func generateSelectList(j int64) []int64 {
//	list := make([]int64, j)
//	var i int64
//	for i = 0; i < j; i++ {
//		list[i] = i
//	}
//	return list
//}
//
//func makeFlagsOne(n int) []uint8 {
//	t := make([]uint8, n)
//	for i := range t {
//		t[i]++
//	}
//	return t
//}
