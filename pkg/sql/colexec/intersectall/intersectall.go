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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	Build = iota
	Probe
	End
)

const opName = "intersect_all"

func (intersectAll *IntersectAll) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": intersect all ")
}

func (intersectAll *IntersectAll) OpType() vm.OpType {
	return vm.IntersectAll
}

func (intersectAll *IntersectAll) Prepare(proc *process.Process) error {
	var err error

	if intersectAll.OpAnalyzer == nil {
		intersectAll.OpAnalyzer = process.NewAnalyzer(intersectAll.GetIdx(), intersectAll.IsFirst, intersectAll.IsLast, "intersectAll")
	} else {
		intersectAll.OpAnalyzer.Reset()
	}

	if intersectAll.ctr.hashTable, err = intersectAll.NewHashMap(proc); err != nil {
		return err
	}
	intersectAll.ctr.iterator = intersectAll.ctr.hashTable.NewIterator()
	if err = intersectAll.ctr.keyEvaluator.Prepare(proc, intersectAll.KeyExprs, intersectAll.ExpressionAllocation()); err != nil {
		return err
	}

	return nil
}

// Call is the execute method of `intersect all` operator
// it built a hash table for right relation first.
// and use an array to record how many times each key appears in right relation.
// use values from left relation to probe and update the array.
// throw away values that do not exist in the hash table.
// preserve values that exist in the hash table (the minimum of the number of times that exist in either).
func (intersectAll *IntersectAll) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := intersectAll.OpAnalyzer
	var err error
	for {
		switch intersectAll.ctr.state {
		case Build:
			done, err := intersectAll.build(proc, analyzer)
			if err != nil {
				return vm.CancelResult, err
			}
			if !done {
				return vm.CallResult{Status: vm.ExecYield}, nil
			}
			if intersectAll.ctr.hashTable != nil {
				analyzer.Alloc(intersectAll.ctr.hashTable.Size())
			}
			intersectAll.ctr.state = Probe

		case Probe:
			last := false
			result := vm.NewCallResult()
			last, err = intersectAll.probe(proc, analyzer, &result)
			if err != nil {
				return result, err
			}
			if last {
				intersectAll.ctr.state = End
				continue
			}
			return result, nil

		case End:
			return vm.CancelResult, nil
		}
	}
}

// build use all batches from proc.Reg.MergeReceiver[1](right relation) to build the hash map.
func (intersectAll *IntersectAll) build(proc *process.Process, analyzer process.Analyzer) (bool, error) {
	ctr := &intersectAll.ctr
	child, err := vm.GetChild(intersectAll, 1)
	if err != nil {
		return false, err
	}
	for {
		input, err := vm.ChildrenCall(child, proc, analyzer)
		if err != nil {
			return false, err
		}
		if input.Status == vm.ExecYield {
			return false, nil
		}

		if input.Batch == nil {
			break
		}
		if input.Batch.IsEmpty() {
			continue
		}

		// build hashTable and a counter to record how many times each key appears
		{
			keyVecs, err := ctr.keyEvaluator.Eval(proc, input.Batch)
			if err != nil {
				return false, err
			}
			count := input.Batch.RowCount()
			for i := 0; i < count; i += hashmap.UnitLimit {

				n := count - i
				if n > hashmap.UnitLimit {
					n = hashmap.UnitLimit
				}
				vs, _, err := ctr.iterator.Insert(i, n, keyVecs)
				if err != nil {
					return false, err
				}
				ctr.counter, err = colexec.GrowSetOperationSlice(ctr.counter, int(ctr.hashTable.GroupCount()), proc.Mp(), &intersectAll.SetOperationAllocation)
				if err != nil {
					return false, err
				}
				for _, v := range vs {
					if v == 0 {
						continue
					}
					ctr.counter[v-1]++
				}
			}
		}

	}
	return true, nil
}

// probe uses a batch from proc.Reg.MergeReceivers[0](left relation) to probe the hash map and update the counter.
// If a row of the batch doesn't appear in the hash table, continue.
// If a row of the batch appears in the hash table and the value of it in the ctr.counter is greater than 0，
// send it to the next operator and counter--; else, continue.
// if batch is the last one, return true, else return false.
func (intersectAll *IntersectAll) probe(proc *process.Process, analyzer process.Analyzer, result *vm.CallResult) (bool, error) {
	ctr := &intersectAll.ctr
	child, err := vm.GetChild(intersectAll, 0)
	if err != nil {
		return false, err
	}
	for {
		input, err := vm.ChildrenCall(child, proc, analyzer)
		if err != nil {
			return false, err
		}
		if input.Status == vm.ExecYield {
			*result = input
			return false, nil
		}
		if input.Batch == nil {
			return true, nil
		}
		if input.Batch.Last() {
			result.Batch = input.Batch
			return false, nil
		}
		if input.Batch.IsEmpty() {
			continue
		}
		//counter to record whether a row should add to output batch or not
		var cnt int

		//init output batch

		if ctr.buf == nil {
			ctr.buf, err = intersectAll.NewBatch(proc, input.Batch)
			if err != nil {
				return false, err
			}
		}
		ctr.buf.CleanOnlyData()

		// probe hashTable
		{
			keyVecs, err := ctr.keyEvaluator.Eval(proc, input.Batch)
			if err != nil {
				return false, err
			}
			count := input.Batch.RowCount()
			for i := 0; i < count; i += hashmap.UnitLimit {
				n := count - i
				if n > hashmap.UnitLimit {
					n = hashmap.UnitLimit
				}

				inserted, resetInserted := intersectAll.Scratch()
				copy(inserted[:n], resetInserted[:n])
				cnt = 0

				vs, _, err := ctr.iterator.Find(i, n, keyVecs)
				if err != nil {
					return false, err
				}

				for j, v := range vs {
					// not found
					if v == 0 {
						continue
					}

					//  all common row has been added into output batch
					if ctr.counter[v-1] == 0 {
						continue
					}

					inserted[j] = 1
					ctr.counter[v-1]--
					cnt++

				}
				ctr.buf.AddRowCount(cnt)

				if cnt > 0 {
					for colNum := range input.Batch.Vecs {
						if err := ctr.buf.Vecs[colNum].UnionBatch(input.Batch.Vecs[colNum], int64(i), cnt, inserted[:n], proc.Mp()); err != nil {
							return false, err
						}
					}
				}
			}

		}
		analyzer.Alloc(int64(ctr.buf.Size()))
		result.Batch = ctr.buf
		return false, nil
	}
}
