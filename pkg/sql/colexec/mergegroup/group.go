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

package mergegroup

import (
	"bytes"
	"reflect"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("mergeroup()")
}

func Prepare(proc *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.zInserted = make([]uint8, hashmap.UnitLimit)

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

func Call(idx int, proc *process.Process, arg interface{}, isFirst bool, isLast bool) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	for {
		switch ctr.state {
		case Build:
			if end, err := ctr.build(proc, anal, isFirst); err != nil {
				return false, err
			} else if end {
				return true, nil
			}
			ctr.state = Eval
		case Eval:
			if ctr.bat != nil {
				if ap.NeedEval {
					for i, agg := range ctr.bat.Aggs {
						vec, err := agg.Eval(proc.Mp())
						if err != nil {
							ctr.state = End
							return false, err
						}
						ctr.bat.Aggs[i] = nil
						ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
						if vec != nil {
							anal.Alloc(int64(vec.Size()))
						}
					}
					ctr.bat.Aggs = nil
					for i := range ctr.bat.Zs { // reset zs
						ctr.bat.Zs[i] = 1
					}
				}
				anal.Output(ctr.bat, isLast)
				ctr.bat.ExpandNulls()
			}
			ctr.state = End
		case End:
			proc.SetInputBatch(ctr.bat)
			ctr.bat = nil
			ap.Free(proc, false)
			return true, nil
		}
	}
}

func (ctr *container) build(proc *process.Process, anal process.Analyze, isFirst bool) (bool, error) {
	var err error
	for {
		if ctr.aliveMergeReceiver == 0 {
			return false, nil
		}

		start := time.Now()
		chosen, value, ok := reflect.Select(ctr.receiverListener)
		if !ok {
			logutil.Errorf("pipeline closed unexpectedly")
			return true, nil
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
		if err = ctr.process(bat, proc); err != nil {
			bat.Clean(proc.Mp())
			return false, err
		}
	}
}

func (ctr *container) process(bat *batch.Batch, proc *process.Process) error {
	var err error

	if ctr.bat == nil {
		size := 0
		for _, vec := range bat.Vecs {
			switch vec.GetType().TypeSize() {
			case 1:
				size += 1 + 1
			case 2:
				size += 2 + 1
			case 4:
				size += 4 + 1
			case 8:
				size += 8 + 1
			case 16:
				size += 16 + 1
			default:
				size = 128
			}
		}
		switch {
		case size == 0:
			ctr.typ = H0
		case size <= 8:
			ctr.typ = H8
			if ctr.intHashMap, err = hashmap.NewIntHashMap(true, 0, 0, proc.Mp()); err != nil {
				return err
			}
		default:
			ctr.typ = HStr
			if ctr.strHashMap, err = hashmap.NewStrMap(true, 0, 0, proc.Mp()); err != nil {
				return err
			}
		}
	}
	switch ctr.typ {
	case H0:
		err = ctr.processH0(bat, proc)
	case H8:
		err = ctr.processH8(bat, proc)
	default:
		err = ctr.processHStr(bat, proc)
	}
	if err != nil {
		return err
	}
	return nil
}

func (ctr *container) processH0(bat *batch.Batch, proc *process.Process) error {
	if ctr.bat == nil {
		ctr.bat = bat
		return nil
	}
	defer bat.Clean(proc.Mp())
	for _, z := range bat.Zs {
		ctr.bat.Zs[0] += z
	}
	for i, agg := range ctr.bat.Aggs {
		err := agg.Merge(bat.Aggs[i], 0, 0)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctr *container) processH8(bat *batch.Batch, proc *process.Process) error {
	count := bat.Length()
	itr := ctr.intHashMap.NewIterator()
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp())
	}
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rowCount := ctr.intHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}
		if !flg {
			if err = ctr.batchFill(i, n, bat, vals, rowCount, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
	}
	return nil
}

func (ctr *container) processHStr(bat *batch.Batch, proc *process.Process) error {
	count := bat.Length()
	itr := ctr.strHashMap.NewIterator()
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp())
	}
	for i := 0; i < count; i += hashmap.UnitLimit { // batch
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		rowCount := ctr.strHashMap.GroupCount()
		vals, _, err := itr.Insert(i, n, bat.Vecs)
		if err != nil {
			return err
		}
		if !flg {
			if err := ctr.batchFill(i, n, bat, vals, rowCount, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
	}
	return nil
}

func (ctr *container) batchFill(i int, n int, bat *batch.Batch, vals []uint64, hashRows uint64, proc *process.Process) error {
	cnt := 0
	copy(ctr.inserted[:n], ctr.zInserted[:n])
	for k, v := range vals {
		if v > hashRows {
			ctr.inserted[k] = 1
			hashRows++
			cnt++
			ctr.bat.Zs = append(ctr.bat.Zs, 0)
		}
		ai := int64(v) - 1
		ctr.bat.Zs[ai] += bat.Zs[i+k]
	}
	if cnt > 0 {
		for j, vec := range ctr.bat.Vecs {
			if err := vector.UnionBatch(vec, bat.Vecs[j], int64(i), cnt, ctr.inserted[:n], proc.Mp()); err != nil {
				return err
			}
		}
		for _, agg := range ctr.bat.Aggs {
			if err := agg.Grows(cnt, proc.Mp()); err != nil {
				return err
			}
		}
	}
	for j, agg := range ctr.bat.Aggs {
		if err := agg.BatchMerge(bat.Aggs[j], int64(i), ctr.inserted[:n], vals); err != nil {
			return err
		}
	}
	return nil
}
