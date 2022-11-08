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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString(" intersect ")
}

func Prepare(proc *process.Process, argument any) error {
	var err error
	arg := argument.(*Argument)
	arg.ctr.btc = nil
	arg.ctr.hashTable, err = hashmap.NewStrMap(true, arg.IBucket, arg.NBucket, proc.Mp())
	if err != nil {
		return err
	}
	arg.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	return nil
}

func Call(idx int, proc *process.Process, argument any) (bool, error) {
	arg := argument.(*Argument)

	analyze := proc.GetAnalyze(idx)
	analyze.Start()
	defer analyze.Stop()

	for {
		switch arg.ctr.state {
		case build:
			if err := arg.ctr.buildHashTable(proc, analyze, 1); err != nil {
				arg.Free(proc, true)
				return false, err
			}
			arg.ctr.state = probe

		case probe:
			var err error
			isLast := false
			if isLast, err = arg.ctr.probeHashTable(proc, analyze, 0); err != nil {
				return true, err
			}
			if isLast {
				arg.ctr.state = end
				continue
			}

			return false, nil

		case end:
			arg.Free(proc, false)
			proc.SetInputBatch(nil)
			return true, nil
		}
	}
}

// build hash table
func (c *container) buildHashTable(proc *process.Process, analyse process.Analyze, idx int) error {
	for {
		btc := <-proc.Reg.MergeReceivers[idx].Ch

		// last batch of block
		if btc == nil {
			break
		}

		// empty batch
		if btc.Length() == 0 {
			continue
		}

		analyse.Input(btc)

		cnt := btc.Length()
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
		btc.Clean(proc.Mp())
	}
	return nil
}

func (c *container) probeHashTable(proc *process.Process, analyze process.Analyze, idx int) (bool, error) {
	for {
		btc := <-proc.Reg.MergeReceivers[idx].Ch

		// last batch of block
		if btc == nil {
			return true, nil
		}

		// empty batch
		if btc.Length() == 0 {
			continue
		}

		analyze.Input(btc)

		c.btc = batch.NewWithSize(len(btc.Vecs))
		for i := range btc.Vecs {
			c.btc.Vecs[i] = vector.New(btc.Vecs[i].Typ)
		}
		needInsert := make([]uint8, hashmap.UnitLimit)
		resetsNeedInsert := make([]uint8, hashmap.UnitLimit)
		cnt := btc.Length()
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
				c.btc.Zs = append(c.btc.Zs, 1)
				insertcnt++
			}

			if insertcnt > 0 {
				for pos := range btc.Vecs {
					if err := vector.UnionBatch(c.btc.Vecs[pos], btc.Vecs[pos], int64(i), insertcnt, needInsert, proc.Mp()); err != nil {
						btc.Clean(proc.Mp())
						return false, err
					}
				}
			}
		}

		btc.Clean(proc.Mp())
		analyze.Output(c.btc)
		proc.SetInputBatch(c.btc)
		return false, nil
	}
}
