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
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	// get batch from merge receivers and do merge sort.
	// save the sort result in ctr.bat.
	if err := ctr.build(ap, proc, anal, isFirst); err != nil {
		ap.Free(proc, true)
		return false, err
	}

	// output the sort result.
	if ctr.bat != nil {
		for i := ctr.n; i < len(ctr.bat.Vecs); i++ {
			vector.Clean(ctr.bat.Vecs[i], proc.Mp())
		}
		ctr.bat.Vecs = ctr.bat.Vecs[:ctr.n]
		ctr.bat.ExpandNulls()
	}
	anal.Output(ctr.bat, isLast)
	proc.SetInputBatch(ctr.bat)
	ctr.bat = nil

	// free and return
	ap.Free(proc, false)
	return true, nil
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	for {
		if ctr.aliveMergeReceiver == 0 {
			return nil
		}

		start := time.Now()
		chosen, value, ok := reflect.Select(ctr.receiverListener)
		if !ok {
			return moerr.NewInternalError(proc.Ctx, "pipeline closed unexpectedly")
		}
		anal.WaitStop(start)

		pointer := value.UnsafePointer()
		bat := (*batch.Batch)(pointer)
		if bat == nil {
			ctr.receiverListener = append(ctr.receiverListener[:chosen], ctr.receiverListener[chosen+1:]...)
			ctr.aliveMergeReceiver--
			continue
		}

		if bat.Length() == 0 {
			continue
		}

		anal.Input(bat, isFirst)
		anal.Alloc(int64(bat.Size()))

		bat.ExpandNulls()
		ctr.n = len(bat.Vecs)
		ctr.poses = ctr.poses[:0]
		for _, f := range ap.Fs {
			vec, err := colexec.EvalExpr(bat, proc, f.Expr)
			if err != nil {
				return err
			}
			flg := true
			for i := range bat.Vecs {
				if bat.Vecs[i] == vec {
					flg = false
					ctr.poses = append(ctr.poses, int32(i))
					break
				}
			}
			if flg {
				ctr.poses = append(ctr.poses, int32(len(bat.Vecs)))
				bat.Vecs = append(bat.Vecs, vec)
			} else {
				if vec != nil {
					anal.Alloc(int64(vec.Size()))
				}
			}
		}
		if ctr.bat == nil {
			mp := make(map[int]int)
			for i, pos := range ctr.poses {
				mp[int(pos)] = i
			}
			ctr.bat = bat
			ctr.cmps = make([]compare.Compare, len(bat.Vecs))
			var desc, nullsLast bool
			for i := range ctr.cmps {
				if pos, ok := mp[i]; ok {
					desc = ap.Fs[pos].Flag&plan.OrderBySpec_DESC != 0
					if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
						nullsLast = false
					} else if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
						nullsLast = true
					} else {
						nullsLast = desc
					}
					ctr.cmps[i] = compare.New(bat.Vecs[i].Typ, desc, nullsLast)
				}
			}
		} else {
			if err := ctr.processBatch(bat, proc); err != nil {
				bat.Clean(proc.Mp())
				return err
			}
			bat.Clean(proc.Mp())
		}
	}
}

func (ctr *container) processBatch(bat2 *batch.Batch, proc *process.Process) error {
	bat1 := ctr.bat
	if bat1 == nil {
		bat1 = batch.NewWithSize(len(bat1.Vecs))
		for i, vec := range bat2.Vecs {
			bat1.Vecs[i] = vector.New(vec.Typ)
		}
	}
	// union bat1 and bat2
	// do merge sort, get final order number and shuffle the result batch.
	for i, cmp := range ctr.cmps {
		if cmp != nil {
			cmp.Set(0, bat1.GetVector(int32(i)))
			cmp.Set(1, bat2.GetVector(int32(i)))
		}
	}
	s1, s2 := int64(0), int64(bat1.Vecs[0].Length()) // startIndexOfBat1, startIndexOfBat2
	for i := range bat1.Vecs {
		n := bat2.Vecs[i].Length()
		err := vector.UnionBatch(bat1.Vecs[i], bat2.Vecs[i], 0, n, makeFlagsOne(n), proc.Mp())
		if err != nil {
			return err
		}
	}
	bat1.Zs = append(bat1.Zs, bat2.Zs...)

	end1, end2 := s2, int64(bat1.Vecs[0].Length())
	sels := make([]int64, 0, end2)
	for s1 < end1 && s2 < end2 {
		i := s1
		j := s2 - end1
		compareResult := 0
		for _, pos := range ctr.poses {
			compareResult = ctr.cmps[pos].Compare(0, 1, i, j)
			if compareResult != 0 {
				break
			}
		}
		if compareResult <= 0 {
			// weight of item1 is less or equal to item2
			sels = append(sels, s1)
			s1++
		} else {
			sels = append(sels, s2)
			s2++
		}
	}
	for s1 < end1 {
		sels = append(sels, s1)
		s1++
	}
	for s2 < end2 {
		sels = append(sels, s2)
		s2++
	}

	err := bat1.Shuffle(sels, proc.Mp())
	if err != nil {
		return err
	}

	ctr.bat = bat1
	return nil
}

func makeFlagsOne(n int) []uint8 {
	t := make([]uint8, n)
	for i := range t {
		t[i]++
	}
	return t
}
