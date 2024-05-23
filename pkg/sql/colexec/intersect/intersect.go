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
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "intersect"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": intersect ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	var err error

	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, false)
	arg.ctr.btc = nil
	arg.ctr.hashTable, err = hashmap.NewStrMap(true, proc.Mp())
	if err != nil {
		return err
	}
	arg.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyze := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	analyze.Start()
	defer analyze.Stop()

	result := vm.NewCallResult()

	for {
		switch arg.ctr.state {
		case build:
			if err := arg.ctr.buildHashTable(proc, analyze, 1, arg.GetIsFirst()); err != nil {
				return result, err
			}
			if arg.ctr.hashTable != nil {
				analyze.Alloc(arg.ctr.hashTable.Size())
			}
			arg.ctr.state = probe

		case probe:
			var err error
			isLast := false
			if isLast, err = arg.ctr.probeHashTable(proc, analyze, 0, arg.GetIsFirst(), arg.GetIsLast(), &result); err != nil {
				result.Status = vm.ExecStop
				return result, err
			}
			if isLast {
				arg.ctr.state = end
				continue
			}

			return result, nil

		case end:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// build hash table
func (c *container) buildHashTable(proc *process.Process, analyse process.Analyze, idx int, isFirst bool) error {
	for {
		msg := c.ReceiveFromSingleReg(idx, analyse)
		if msg.Err != nil {
			return msg.Err
		}
		btc := msg.Batch

		// last batch of block
		if btc == nil {
			break
		}

		// empty batch
		if btc.IsEmpty() {
			proc.PutBatch(btc)
			continue
		}

		analyse.Input(btc, isFirst)

		cnt := btc.RowCount()
		itr := c.hashTable.NewIterator()
		for i := 0; i < cnt; i += hashmap.UnitLimit {
			rowcnt := c.hashTable.GroupCount()

			n := cnt - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			vs, zs, err := itr.Insert(i, n, btc.Vecs)
			if err != nil {
				btc.Clean(proc.Mp())
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
		proc.PutBatch(btc)
	}
	return nil
}

func (c *container) probeHashTable(proc *process.Process, analyze process.Analyze, idx int, isFirst bool, isLast bool, result *vm.CallResult) (bool, error) {
	for {
		msg := c.ReceiveFromSingleReg(idx, analyze)
		if msg.Err != nil {
			return false, msg.Err
		}
		btc := msg.Batch

		// last batch of block
		if btc == nil {
			return true, nil
		}

		// empty batch
		if btc.IsEmpty() {
			proc.PutBatch(btc)
			continue
		}

		analyze.Input(btc, isFirst)
		if c.btc != nil {
			proc.PutBatch(c.btc)
			c.btc = nil
		}
		c.btc = batch.NewWithSize(len(btc.Vecs))
		for i := range btc.Vecs {
			c.btc.Vecs[i] = proc.GetVector(*btc.Vecs[i].GetType())
		}
		needInsert := make([]uint8, hashmap.UnitLimit)
		resetsNeedInsert := make([]uint8, hashmap.UnitLimit)
		cnt := btc.RowCount()
		itr := c.hashTable.NewIterator()
		for i := 0; i < cnt; i += hashmap.UnitLimit {
			n := cnt - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			copy(c.inBuckets, hashmap.OneUInt8s)
			copy(needInsert, resetsNeedInsert)
			insertcnt := 0

			vs, zs := itr.Find(i, n, btc.Vecs, c.inBuckets)

			for j, v := range vs {
				// not in the processed bucket
				if c.inBuckets[j] == 0 {
					continue
				}

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
			c.btc.AddRowCount(insertcnt)

			if insertcnt > 0 {
				for pos := range btc.Vecs {
					if err := c.btc.Vecs[pos].UnionBatch(btc.Vecs[pos], int64(i), insertcnt, needInsert, proc.Mp()); err != nil {
						btc.Clean(proc.Mp())
						return false, err
					}
				}
			}
		}

		proc.PutBatch(btc)
		analyze.Alloc(int64(c.btc.Size()))
		analyze.Output(c.btc, isLast)

		result.Batch = c.btc
		return false, nil
	}
}
