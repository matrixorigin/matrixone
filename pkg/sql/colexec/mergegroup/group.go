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

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("mergeroup()")
}

func Prepare(proc *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(container)
	ap.ctr.InitReceiver(proc, true)
	ap.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	ap.ctr.zInserted = make([]uint8, hashmap.UnitLimit)
	return nil
}

func Call(idx int, proc *process.Process, arg interface{}, isFirst bool, isLast bool) (process.ExecStatus, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	anal := proc.GetAnalyze(idx)
	anal.Start()
	defer anal.Stop()

	for {
		switch ctr.state {
		case Build:
			for {
				bat, end, err := ctr.ReceiveFromAllRegs(anal)
				if err != nil {
					return process.ExecStop, nil
				}

				if end {
					break
				}
				anal.Input(bat, isFirst)
				if err = ctr.process(bat, proc); err != nil {
					bat.Clean(proc.Mp())
					return process.ExecNext, err
				}
			}
			ctr.state = Eval

		case Eval:
			if ctr.bat != nil {
				if ap.NeedEval {
					for i, agg := range ctr.bat.Aggs {
						vec, err := agg.Eval(proc.Mp())
						if err != nil {
							ctr.state = End
							return process.ExecNext, err
						}
						ctr.bat.Aggs[i] = nil
						ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
						if vec != nil {
							anal.Alloc(int64(vec.Size()))
						}
					}
					ctr.bat.Aggs = nil
				}
				anal.Output(ctr.bat, isLast)
			}
			ctr.state = End

		case End:
			proc.SetInputBatch(ctr.bat)
			ctr.bat = nil
			return process.ExecStop, nil
		}
	}
}

func (ctr *container) process(bat *batch.Batch, proc *process.Process) error {
	var err error

	if ctr.bat == nil {
		keyWidth := 0
		groupVecsNullable := false

		for _, vec := range bat.Vecs {
			groupVecsNullable = groupVecsNullable || (!vec.GetType().GetNotNull())
		}

		for _, vec := range bat.Vecs {
			width := vec.GetType().TypeSize()
			if vec.GetType().IsVarlen() {
				if vec.GetType().Width == 0 {
					width = 128
				} else {
					width = int(vec.GetType().Width)
				}
			}
			keyWidth += width
			if groupVecsNullable {
				keyWidth += 1
			}
		}

		switch {
		case keyWidth == 0:
			// no group by.
			ctr.typ = H0

		case keyWidth <= 8:
			ctr.typ = H8
			if ctr.intHashMap, err = hashmap.NewIntHashMap(groupVecsNullable, 0, 0, proc.Mp()); err != nil {
				return err
			}
		default:
			ctr.typ = HStr
			if ctr.strHashMap, err = hashmap.NewStrMap(groupVecsNullable, 0, 0, proc.Mp()); err != nil {
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
	return err
}

func (ctr *container) processH0(bat *batch.Batch, proc *process.Process) error {
	if ctr.bat == nil {
		ctr.bat = bat
		return nil
	}
	defer proc.PutBatch(bat)
	ctr.bat.SetRowCount(1)

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
		defer proc.PutBatch(bat)
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
		defer proc.PutBatch(bat)
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
		}
	}
	ctr.bat.AddRowCount(cnt)

	if cnt > 0 {
		for j, vec := range ctr.bat.Vecs {
			if err := vec.UnionBatch(bat.Vecs[j], int64(i), cnt, ctr.inserted[:n], proc.Mp()); err != nil {
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
