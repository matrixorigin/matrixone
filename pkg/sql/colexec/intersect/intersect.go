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

package intersect

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "intersect"

func (intersect *Intersect) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": intersect ")
}

func (intersect *Intersect) OpType() vm.OpType {
	return vm.Intersect
}

func (intersect *Intersect) Prepare(proc *process.Process) error {
	var err error

	intersect.ctr = new(container)
	intersect.ctr.buf = nil
	intersect.ctr.hashTable, err = hashmap.NewStrMap(true, proc.Mp())
	if err != nil {
		return err
	}
	return nil
}

func (intersect *Intersect) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyze := proc.GetAnalyze(intersect.GetIdx(), intersect.GetParallelIdx(), intersect.GetParallelMajor())
	analyze.Start()
	defer analyze.Stop()

	for {
		switch intersect.ctr.state {
		case build:
			if err := intersect.buildHashTable(proc, analyze, 1, intersect.GetIsFirst()); err != nil {
				return vm.CancelResult, err
			}
			if intersect.ctr.hashTable != nil {
				analyze.Alloc(intersect.ctr.hashTable.Size())
			}
			intersect.ctr.state = probe

		case probe:
			var err error
			isLast := false
			result := vm.NewCallResult()
			if isLast, err = intersect.probeHashTable(proc, analyze, 0, intersect.GetIsFirst(), intersect.GetIsLast(), &result); err != nil {
				result.Status = vm.ExecStop
				return result, err
			}
			if isLast {
				intersect.ctr.state = end
				continue
			}

			return result, nil

		case end:
			return vm.CancelResult, nil
		}
	}
}

// build hash table
func (intersect *Intersect) buildHashTable(proc *process.Process, analyse process.Analyze, idx int, isFirst bool) error {
	c := intersect.ctr
	for {
		input, err := intersect.GetChildren(idx).Call(proc)
		if err != nil {
			return err
		}

		// last batch of block
		if input.Batch == nil {
			break
		}

		// empty batch
		if input.Batch.IsEmpty() {
			continue
		}

		analyse.Input(input.Batch, isFirst)

		cnt := input.Batch.RowCount()
		itr := c.hashTable.NewIterator()
		for i := 0; i < cnt; i += hashmap.UnitLimit {
			rowcnt := c.hashTable.GroupCount()

			n := cnt - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			vs, zs, err := itr.Insert(i, n, input.Batch.Vecs)
			if err != nil {
				return err
			}

			for j, v := range vs {
				if zs[j] == 0 {
					continue
				}

				if v > rowcnt {
					c.cnts = append(c.cnts, proc.Mp().GetSels())
					c.cnts[v-1] = append(c.cnts[v-1], 1)
					rowcnt++
				}
			}
		}
	}
	return nil
}

func (intersect *Intersect) probeHashTable(proc *process.Process, analyze process.Analyze, idx int, isFirst bool, isLast bool, result *vm.CallResult) (bool, error) {
	c := intersect.ctr
	for {
		input, err := intersect.GetChildren(idx).Call(proc)
		if err != nil {
			return false, err
		}

		// last batch of block
		if input.Batch == nil {
			return true, nil
		}

		// empty batch
		if input.Batch.IsEmpty() {
			continue
		}

		analyze.Input(input.Batch, isFirst)
		if c.buf == nil {
			c.buf = batch.NewWithSize(len(input.Batch.Vecs))
			for i := range input.Batch.Vecs {
				c.buf.Vecs[i] = vector.NewVec(*input.Batch.Vecs[i].GetType())
			}
		}
		c.buf.CleanOnlyData()
		needInsert := make([]uint8, hashmap.UnitLimit)
		resetsNeedInsert := make([]uint8, hashmap.UnitLimit)
		cnt := input.Batch.RowCount()
		itr := c.hashTable.NewIterator()
		for i := 0; i < cnt; i += hashmap.UnitLimit {
			n := cnt - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			copy(needInsert, resetsNeedInsert)
			insertcnt := 0

			vs, zs := itr.Find(i, n, input.Batch.Vecs)

			for j, v := range vs {

				// null value
				if zs[j] == 0 {
					continue
				}

				// not found
				if v == 0 {
					continue
				}

				// has been added into output batch
				if c.cnts[v-1][0] == 0 {
					continue
				}

				needInsert[j] = 1
				c.cnts[v-1][0] = 0
				insertcnt++
			}
			c.buf.AddRowCount(insertcnt)

			if insertcnt > 0 {
				for pos := range input.Batch.Vecs {
					if err := c.buf.Vecs[pos].UnionBatch(input.Batch.Vecs[pos], int64(i), insertcnt, needInsert, proc.Mp()); err != nil {
						return false, err
					}
				}
			}
		}

		analyze.Alloc(int64(c.buf.Size()))
		analyze.Output(c.buf, isLast)
		result.Batch = c.buf
		return false, nil
	}
}
