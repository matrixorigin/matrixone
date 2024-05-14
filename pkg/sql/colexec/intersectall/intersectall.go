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

package intersectall

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Probe
	End
)

const argName = "intersect_all"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": intersect all ")
}

func (arg *Argument) Prepare(proc *process.Process) error {
	var err error
	arg.ctr = new(container)
	arg.ctr.InitReceiver(proc, false)
	if arg.ctr.hashTable, err = hashmap.NewStrMap(true, arg.IBucket, arg.NBucket, proc.Mp()); err != nil {
		return err
	}
	arg.ctr.inBuckets = make([]uint8, hashmap.UnitLimit)
	arg.ctr.inserted = make([]uint8, hashmap.UnitLimit)
	arg.ctr.resetInserted = make([]uint8, hashmap.UnitLimit)
	return nil
}

// Call is the execute method of `intersect all` operator
// it built a hash table for right relation first.
// and use an array to record how many times each key appears in right relation.
// use values from left relation to probe and update the array.
// throw away values that do not exist in the hash table.
// preserve values that exist in the hash table (the minimum of the number of times that exist in either).
func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	var err error
	analyzer := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	analyzer.Start()
	defer analyzer.Stop()
	result := vm.NewCallResult()
	for {
		switch arg.ctr.state {
		case Build:
			if err = arg.ctr.build(proc, analyzer, arg.GetIsFirst()); err != nil {
				return result, err
			}
			if arg.ctr.hashTable != nil {
				analyzer.Alloc(arg.ctr.hashTable.Size())
			}
			arg.ctr.state = Probe

		case Probe:
			last := false
			last, err = arg.ctr.probe(proc, analyzer, arg.GetIsFirst(), arg.GetIsLast(), &result)
			if err != nil {
				return result, err
			}
			if last {
				arg.ctr.state = End
				continue
			}
			return result, nil

		case End:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// build use all batches from proc.Reg.MergeReceiver[1](right relation) to build the hash map.
func (ctr *container) build(proc *process.Process, analyzer process.Analyze, isFirst bool) error {
	for {
		bat, _, err := ctr.ReceiveFromSingleReg(1, analyzer)
		if err != nil {
			return err
		}

		if bat == nil {
			break
		}
		if bat.IsEmpty() {
			proc.PutBatch(bat)
			continue
		}

		analyzer.Input(bat, isFirst)
		// build hashTable and a counter to record how many times each key appears
		{
			itr := ctr.hashTable.NewIterator()
			count := bat.RowCount()
			for i := 0; i < count; i += hashmap.UnitLimit {

				n := count - i
				if n > hashmap.UnitLimit {
					n = hashmap.UnitLimit
				}
				vs, _, err := itr.Insert(i, n, bat.Vecs)
				if err != nil {
					bat.Clean(proc.Mp())
					return err
				}
				if uint64(cap(ctr.counter)) < ctr.hashTable.GroupCount() {
					gap := ctr.hashTable.GroupCount() - uint64(cap(ctr.counter))
					ctr.counter = append(ctr.counter, make([]uint64, gap)...)
				}
				for _, v := range vs {
					if v == 0 {
						continue
					}
					ctr.counter[v-1]++
				}
			}
			proc.PutBatch(bat)
		}

	}
	return nil
}

// probe uses a batch from proc.Reg.MergeReceivers[0](left relation) to probe the hash map and update the counter.
// If a row of the batch doesn't appear in the hash table, continue.
// If a row of the batch appears in the hash table and the value of it in the ctr.counter is greater than 0，
// send it to the next operator and counter--; else, continue.
// if batch is the last one, return true, else return false.
func (ctr *container) probe(proc *process.Process, analyzer process.Analyze, isFirst bool, isLast bool, result *vm.CallResult) (bool, error) {
	if ctr.buf != nil {
		proc.PutBatch(ctr.buf)
		ctr.buf = nil
	}
	for {
		bat, _, err := ctr.ReceiveFromSingleReg(0, analyzer)
		if err != nil {
			return false, err
		}
		if bat == nil {
			return true, nil
		}
		analyzer.Input(bat, isFirst)
		if bat.Last() {
			ctr.buf = bat
			result.Batch = ctr.buf
			return false, nil
		}
		if bat.IsEmpty() {
			proc.PutBatch(bat)
			continue
		}
		//counter to record whether a row should add to output batch or not
		var cnt int

		//init output batch
		ctr.buf = batch.NewWithSize(len(bat.Vecs))
		for i := range bat.Vecs {
			ctr.buf.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
		}

		// probe hashTable
		{
			itr := ctr.hashTable.NewIterator()
			count := bat.RowCount()
			for i := 0; i < count; i += hashmap.UnitLimit {
				n := count - i
				if n > hashmap.UnitLimit {
					n = hashmap.UnitLimit
				}

				copy(ctr.inBuckets, hashmap.OneUInt8s)
				copy(ctr.inserted[:n], ctr.resetInserted[:n])
				cnt = 0

				vs, _ := itr.Find(i, n, bat.Vecs, ctr.inBuckets)

				for j, v := range vs {
					// not in the processed bucket
					if ctr.inBuckets[j] == 0 {
						continue
					}

					// not found
					if v == 0 {
						continue
					}

					//  all common row has been added into output batch
					if ctr.counter[v-1] == 0 {
						continue
					}

					ctr.inserted[j] = 1
					ctr.counter[v-1]--
					cnt++

				}
				ctr.buf.AddRowCount(cnt)

				if cnt > 0 {
					for colNum := range bat.Vecs {
						if err := ctr.buf.Vecs[colNum].UnionBatch(bat.Vecs[colNum], int64(i), cnt, ctr.inserted[:n], proc.Mp()); err != nil {
							bat.Clean(proc.Mp())
							return false, err
						}
					}
				}
			}

		}
		analyzer.Alloc(int64(ctr.buf.Size()))
		analyzer.Output(ctr.buf, isLast)

		result.Batch = ctr.buf
		proc.PutBatch(bat)
		return false, nil
	}
}
