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
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "merge_group"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": mergeroup()")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, true)
	arg.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	arg.ctr.zInserted = make([]uint8, hashmap.UnitLimit)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	ctr := arg.ctr
	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	result := vm.NewCallResult()
	for {
		switch ctr.state {
		case Build:
			for {
				bat, end, err := ctr.ReceiveFromAllRegs(anal)
				if err != nil {
					result.Status = vm.ExecStop
					return result, nil
				}

				if end {
					break
				}
				anal.Input(bat, arg.GetIsFirst())
				if err = ctr.process(bat, proc); err != nil {
					bat.Clean(proc.Mp())
					return result, err
				}
			}
			ctr.state = Eval

		case Eval:
			if ctr.bat != nil {
				if arg.NeedEval {
					for i, agg := range ctr.bat.Aggs {
						if len(arg.PartialResults) > i && arg.PartialResults[i] != nil {
							if err := agg.SetExtraInformation(arg.PartialResults[i], 0); err != nil {
								return result, err
							}
						}
						vec, err := agg.Flush()
						if err != nil {
							ctr.state = End
							return result, err
						}
						ctr.bat.Aggs[i] = nil
						ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
						if vec != nil {
							anal.Alloc(int64(vec.Size()))
						}

						agg.Free()
					}
					ctr.bat.Aggs = nil
				}
				anal.Output(ctr.bat, arg.GetIsLast())
				result.Batch = ctr.bat
			}
			ctr.state = End
			return result, nil

		case End:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
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
					switch vec.GetType().Oid {
					case types.T_array_float32:
						width = 128 * 4
					case types.T_array_float64:
						width = 128 * 8
					default:
						width = 128
					}
				} else {
					switch vec.GetType().Oid {
					case types.T_array_float32:
						width = int(vec.GetType().Width) * 4
					case types.T_array_float64:
						width = int(vec.GetType().Width) * 8
					default:
						width = int(vec.GetType().Width)
					}
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
	count := bat.RowCount()
	itr := ctr.intHashMap.NewIterator()
	flg := ctr.bat == nil
	if !flg {
		defer proc.PutBatch(bat)
	}
	for i := 0; i < count; i += hashmap.UnitLimit {
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}
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
	count := bat.RowCount()
	itr := ctr.strHashMap.NewIterator()
	flg := ctr.bat == nil
	if !flg {
		defer proc.PutBatch(bat)
	}
	for i := 0; i < count; i += hashmap.UnitLimit { // batch
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}
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
			if err := agg.GroupGrow(cnt); err != nil {
				return err
			}
		}
	}
	for j, agg := range ctr.bat.Aggs {
		if err := agg.BatchMerge(bat.Aggs[j], i, vals[:n]); err != nil {
			return err
		}
	}
	return nil
}
