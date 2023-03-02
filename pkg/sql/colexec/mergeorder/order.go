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
	"reflect"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg any, buf *bytes.Buffer) {
	ap := arg.(*Argument)
	buf.WriteString("mergeorder([")
	for i, f := range ap.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	buf.WriteString("])")
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.poses = make([]int32, 0, len(ap.Fs))

	ap.ctr.receiverListener = make([]reflect.SelectCase, len(proc.Reg.MergeReceivers))
	for i, mr := range proc.Reg.MergeReceivers {
		ap.ctr.receiverListener[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(mr.Ch),
		}
	}
	ap.ctr.aliveMergeReceiver = len(proc.Reg.MergeReceivers)
	ap.ctr.compare0Index = make([]int32, len(ap.Fs))
	ap.ctr.compare1Index = make([]int32, len(ap.Fs))
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	var bat *batch.Batch
	var end bool
	var err error

	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	// get batch from merge receivers and do merge sort.
	// save the unordered result in ctr.bat.
	// save the ordered index list in ctr.finalSelectList
	for {
		start := time.Now()
		bat, end, err = receiveBatch(proc, ctr)
		if err != nil {
			break
		}
		anal.WaitStop(start)
		if end {
			break
		}

		if bat == nil || bat.Length() == 0 {
			continue
		}
		anal.Input(bat, isFirst)
		bat.ExpandNulls()

		if err = mergeSort(proc, bat, ap, ctr, anal); err != nil {
			break
		}
	}
	if err != nil {
		ap.Free(proc, true)
		return false, err
	}

	// remove and clean unnecessary vector
	// shuffle the ctr.bat
	if ctr.bat != nil {
		for i := ctr.n; i < len(ctr.bat.Vecs); i++ {
			vector.Clean(ctr.bat.Vecs[i], proc.Mp())
		}
		ctr.bat.Vecs = ctr.bat.Vecs[:ctr.n]
		ctr.bat.ExpandNulls()
	}
	if err = ctr.bat.Shuffle(ctr.finalSelectList, proc.Mp()); err != nil {
		ap.Free(proc, true)
		return false, err
	}

	// output the sort result.
	anal.Output(ctr.bat, isLast)
	proc.SetInputBatch(ctr.bat)
	ctr.bat = nil

	// free and return
	ap.Free(proc, false)
	return true, nil
}

// receiveBatch get a batch from receiver, return true if all batches have been got.
func receiveBatch(proc *process.Process, ctr *container) (*batch.Batch, bool, error) {
	if ctr.aliveMergeReceiver == 0 {
		return nil, true, nil
	}
	chosen, value, ok := reflect.Select(ctr.receiverListener)
	if !ok {
		return nil, false, moerr.NewInternalError(proc.Ctx, "pipeline closed unexpectedly")
	}
	pointer := value.UnsafePointer()
	bat := (*batch.Batch)(pointer)
	if bat == nil {
		ctr.receiverListener = append(ctr.receiverListener[:chosen], ctr.receiverListener[chosen+1:]...)
		ctr.aliveMergeReceiver--
	}
	return bat, false, nil
}

func mergeSort(proc *process.Process, bat2 *batch.Batch,
	ap *Argument, ctr *container, anal process.Analyze) error {
	ctr.n = len(bat2.Vecs)
	ctr.poses = ctr.poses[:0]

	// evaluate the order column.
	for _, f := range ap.Fs {
		vec, err := colexec.EvalExpr(bat2, proc, f.Expr)
		if err != nil {
			return err
		}
		newColumn := true
		for i := range bat2.Vecs {
			if bat2.Vecs[i] == vec {
				newColumn = false
				ctr.poses = append(ctr.poses, int32(i))
				break
			}
		}
		if newColumn {
			ctr.poses = append(ctr.poses, int32(len(bat2.Vecs)))
			bat2.Vecs = append(bat2.Vecs, vec)
			anal.Alloc(int64(vec.Size()))
		}
	}
	copy(ctr.compare1Index, ctr.poses)

	// init the compare structure if first time.
	if len(ctr.cmps) == 0 {
		var desc, nullsLast bool
		ctr.cmps = make([]compare.Compare, len(ap.Fs))
		for i := range ctr.cmps {
			desc = ap.Fs[i].Flag&plan.OrderBySpec_DESC != 0
			if ap.Fs[i].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
				nullsLast = false
			} else if ap.Fs[i].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
				nullsLast = true
			} else {
				nullsLast = desc
			}
			ctr.cmps[i] = compare.New(bat2.Vecs[ctr.poses[i]].Typ, desc, nullsLast)
		}
	}

	return ctr.mergeSort2(bat2, proc)
}

func (ctr *container) mergeSort2(bat2 *batch.Batch, proc *process.Process) error {
	if ctr.bat == nil {
		ctr.bat = bat2
		ctr.finalSelectList = generateSelectList(int64(ctr.bat.Length()))
		copy(ctr.compare0Index, ctr.poses)
		return nil
	}
	bat1 := ctr.bat
	// union bat1 and bat2
	// do merge sort, get order index list.
	s1, s2 := int64(0), int64(bat1.Vecs[0].Length()) // startIndexOfBat1, startIndexOfBat2

	for i := range bat1.Vecs {
		n := bat2.Vecs[i].Length()
		if cap(ctr.unionFlag) >= n {
			ctr.unionFlag = ctr.unionFlag[:n:cap(ctr.unionFlag)]
		} else {
			ctr.unionFlag = makeFlagsOne(n)
		}
		err := vector.UnionBatch(bat1.Vecs[i], bat2.Vecs[i], 0, n, ctr.unionFlag, proc.Mp())
		if err != nil {
			return err
		}
	}
	bat1.Zs = append(bat1.Zs, bat2.Zs...)

	// set cmp should after union work to avoid memory re-alloc while union.
	for i, cmp := range ctr.cmps {
		cmp.Set(0, bat1.GetVector(ctr.compare0Index[i]))
		cmp.Set(1, bat2.GetVector(ctr.compare1Index[i]))
	}

	end1, end2 := s2, int64(bat1.Vecs[0].Length())
	sels := make([]int64, 0, end2)

	// set up cmp must happen after vector.UnionBatch.  UnionBatch may grow the vector
	// in bat1, which could cause a realloc.  Depending on mpool has fixed pool, the old
	// vector maybe destroyed, cmp then set a garbage vector.
	for i, cmp := range ctr.cmps {
		cmp.Set(0, bat1.GetVector(ctr.compare0Index[i]))
		cmp.Set(1, bat2.GetVector(ctr.compare1Index[i]))
	}

	for s1 < end1 && s2 < end2 {
		i := s1
		j := s2 - end1
		compareResult := 0
		for k := range ctr.cmps {
			compareResult = ctr.cmps[k].Compare(0, 1, ctr.finalSelectList[i], j)
			if compareResult != 0 {
				break
			}
		}
		if compareResult <= 0 {
			// weight of item1 is less or equal to item2
			sels = append(sels, ctr.finalSelectList[s1])
			s1++
		} else {
			sels = append(sels, s2)
			s2++
		}
	}
	for s1 < end1 {
		sels = append(sels, ctr.finalSelectList[s1])
		s1++
	}
	for s2 < end2 {
		sels = append(sels, s2)
		s2++
	}
	ctr.finalSelectList = sels
	ctr.bat = bat1
	bat2.Clean(proc.Mp())
	return nil
}

func generateSelectList(j int64) []int64 {
	list := make([]int64, j)
	var i int64
	for i = 0; i < j; i++ {
		list[i] = i
	}
	return list
}

func makeFlagsOne(n int) []uint8 {
	t := make([]uint8, n)
	for i := range t {
		t[i]++
	}
	return t
}
