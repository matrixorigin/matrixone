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

package mergecte

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "merge_cte"

func (mergeCTE *MergeCTE) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": merge cte ")
}

func (mergeCTE *MergeCTE) OpType() vm.OpType {
	return vm.MergeCTE
}

func (mergeCTE *MergeCTE) Prepare(proc *process.Process) error {
	if mergeCTE.OpAnalyzer == nil {
		mergeCTE.OpAnalyzer = process.NewAnalyzer(mergeCTE.GetIdx(), mergeCTE.IsFirst, mergeCTE.IsLast, "merge cte")
	} else {
		mergeCTE.OpAnalyzer.Reset()
	}

	mergeCTE.ctr.curNodeCnt = int32(mergeCTE.NodeCnt)
	mergeCTE.ctr.status = sendInitial
	if err := mergeCTE.ctr.bindMemory(proc); err != nil {
		return err
	}
	if mergeCTE.Distinct && mergeCTE.ctr.hashTable == nil {
		hashTable, err := hashmap.NewStrHashMap(true, proc.Mp())
		if err != nil {
			mergeCTE.ctr.memory.Release()
			return err
		}
		mergeCTE.ctr.hashTable = hashTable
	}
	return nil
}

func (mergeCTE *MergeCTE) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := mergeCTE.OpAnalyzer

	result := vm.NewCallResult()
	var err error
	ctr := &mergeCTE.ctr

	switch ctr.status {
	case sendInitial:
		result, err = vm.ChildrenCall(mergeCTE.GetChildren(0), proc, analyzer)
		if err != nil {
			result.Status = vm.ExecStop
			return result, err
		}

		if result.Batch == nil {
			ctr.status = sendLastTag
		} else {
			appBat, err := ctr.cacheBatch(proc, analyzer, result.Batch)
			if err != nil {
				result.Status = vm.ExecStop
				return result, err
			}
			ctr.bats = append(ctr.bats, appBat)
		}

		fallthrough
	case sendLastTag:
		if mergeCTE.ctr.status == sendLastTag {
			mergeCTE.ctr.status = sendRecursive
			recursiveBatch, err := ctr.cacheRecursiveBatch(proc)
			if err != nil {
				result.Status = vm.ExecStop
				return result, err
			}
			if len(mergeCTE.ctr.bats) == 0 {
				mergeCTE.ctr.bats = append(mergeCTE.ctr.bats, recursiveBatch)
			} else {
				mergeCTE.ctr.bats[0] = recursiveBatch
			}
		}
	case sendRecursive:
		for !mergeCTE.ctr.last {
			result, err = vm.ChildrenCall(mergeCTE.GetChildren(1), proc, analyzer)
			if err != nil {
				result.Status = vm.ExecStop
				return result, err
			}
			if result.Batch == nil {
				result.Status = vm.ExecStop
				return result, nil
			}

			if result.Batch.Last() {
				mergeCTE.ctr.curNodeCnt--
				if mergeCTE.ctr.curNodeCnt == 0 {
					mergeCTE.ctr.last = true
					mergeCTE.ctr.curNodeCnt = int32(mergeCTE.NodeCnt)
					mergeCTE.ctr.recursiveLevel++
					maxRecursion := moDefaultRecursionMax
					if resolveFunc := proc.GetResolveVariableFunc(); resolveFunc != nil {
						if val, err := resolveFunc("cte_max_recursion_depth", true, false); err == nil {
							if v, ok := val.(int64); ok {
								maxRecursion = int(v)
							}
						}
					}
					if mergeCTE.ctr.recursiveLevel > maxRecursion {
						result.Status = vm.ExecStop
						return result, moerr.NewCheckRecursiveLevel(proc.Ctx)
					}
					appBat, err := ctr.cacheBatch(proc, analyzer, result.Batch)
					if err != nil {
						result.Status = vm.ExecStop
						return result, err
					}
					ctr.bats = append(ctr.bats, appBat)
					break
				}
			} else {
				appBat, err := ctr.cacheBatch(proc, analyzer, result.Batch)
				if err != nil {
					result.Status = vm.ExecStop
					return result, err
				}
				ctr.bats = append(ctr.bats, appBat)
			}

		}
	}

	mergeCTE.ctr.buf = mergeCTE.ctr.bats[0]
	mergeCTE.ctr.bats = mergeCTE.ctr.bats[1:]
	if mergeCTE.ctr.buf.Last() {
		mergeCTE.ctr.last = false
	}

	result.Batch = mergeCTE.ctr.buf
	result.Status = vm.ExecHasMore
	return result, nil
}

func (ctr *container) cacheBatch(
	proc *process.Process,
	analyzer process.Analyzer,
	src *batch.Batch,
) (*batch.Batch, error) {
	var insertedRows []int64
	if ctr.hashTable != nil && !src.Last() && !src.IsEmpty() {
		var err error
		insertedRows, err = ctr.filterInsertedRows(src, analyzer)
		if err != nil {
			return nil, err
		}
	}

	var cached *batch.Batch
	if ctr.i < len(ctr.freeBats) {
		cached = ctr.freeBats[ctr.i]
	}
	replacement, err := ctr.memory.BeginReplacement(proc.Ctx, cached, src)
	if err != nil {
		return nil, err
	}

	if ctr.i == len(ctr.freeBats) {
		appBat, err := src.Dup(proc.Mp())
		if err != nil {
			replacement.Rollback()
			return nil, err
		}
		if insertedRows != nil {
			appBat.Shrink(insertedRows, false)
		}
		if err = replacement.Commit(appBat); err != nil {
			appBat.Clean(proc.Mp())
			replacement.Discard()
			return nil, err
		}
		analyzer.Alloc(int64(appBat.Size()))
		ctr.freeBats = append(ctr.freeBats, appBat)
		ctr.i++
		return appBat, nil
	}

	if !src.Last() && sameBatchSchema(cached, src) {
		cached.CleanOnlyData()
		appBat, err := cached.AppendWithCopy(proc.Ctx, proc.Mp(), src)
		if err != nil {
			cached.Clean(proc.Mp())
			ctr.freeBats[ctr.i] = nil
			replacement.Discard()
			return nil, err
		}
		appBat.Recursive = src.Recursive
		appBat.ShuffleIDX = src.ShuffleIDX
		appBat.Attrs = append(appBat.Attrs[:0], src.Attrs...)
		appBat.SetRowCount(src.RowCount())
		if insertedRows != nil {
			appBat.Shrink(insertedRows, false)
		}
		if err = replacement.Commit(appBat); err != nil {
			appBat.Clean(proc.Mp())
			ctr.freeBats[ctr.i] = nil
			replacement.Discard()
			return nil, err
		}
		ctr.i++
		return appBat, nil
	}

	// The number of batches produced before this slot may change after Reset,
	// so a cache slot is not guaranteed to keep the same schema.
	appBat, err := src.Dup(proc.Mp())
	if err != nil {
		replacement.Rollback()
		return nil, err
	}
	if insertedRows != nil {
		appBat.Shrink(insertedRows, false)
	}
	if err = replacement.Commit(appBat); err != nil {
		appBat.Clean(proc.Mp())
		replacement.Rollback()
		return nil, err
	}
	analyzer.Alloc(int64(appBat.Size()))
	if cached != nil {
		cached.Clean(proc.Mp())
	}
	ctr.freeBats[ctr.i] = appBat
	ctr.i++
	return appBat, nil
}

func (ctr *container) filterInsertedRows(
	src *batch.Batch,
	analyzer process.Analyzer,
) ([]int64, error) {
	ctr.insertedRows = ctr.insertedRows[:0]
	itr := ctr.hashTable.NewIterator()
	count := src.RowCount()
	oldSize := ctr.hashTable.Size()
	for i := 0; i < count; i += hashmap.UnitLimit {
		n := count - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}
		oldGroupCount := ctr.hashTable.GroupCount()
		values, _, err := itr.Insert(i, n, src.Vecs)
		if err != nil {
			return nil, err
		}
		nextGroup := oldGroupCount
		for j, value := range values {
			if value > nextGroup {
				nextGroup++
				ctr.insertedRows = append(ctr.insertedRows, int64(i+j))
			}
		}
	}
	if allocated := ctr.hashTable.Size() - oldSize; allocated > 0 {
		analyzer.Alloc(allocated)
	}
	return ctr.insertedRows, nil
}

func sameBatchSchema(left, right *batch.Batch) bool {
	if left == nil || right == nil || len(left.Vecs) != len(right.Vecs) {
		return false
	}
	for i := range left.Vecs {
		if left.Vecs[i] == nil || right.Vecs[i] == nil ||
			!left.Vecs[i].GetType().Eq(*right.Vecs[i].GetType()) {
			return false
		}
	}
	return true
}

func (ctr *container) cacheRecursiveBatch(proc *process.Process) (*batch.Batch, error) {
	var cached *batch.Batch
	if ctr.i < len(ctr.freeBats) {
		cached = ctr.freeBats[ctr.i]
	}
	b, err := makeRecursiveBatch(proc)
	if err != nil {
		return nil, err
	}
	replacement, err := ctr.memory.BeginReplacement(proc.Ctx, cached, b)
	if err != nil {
		b.Clean(proc.Mp())
		return nil, err
	}
	if err = replacement.Commit(b); err != nil {
		b.Clean(proc.Mp())
		replacement.Rollback()
		return nil, err
	}
	if ctr.i == len(ctr.freeBats) {
		ctr.freeBats = append(ctr.freeBats, b)
	} else {
		if ctr.freeBats[ctr.i] != nil {
			ctr.freeBats[ctr.i].Clean(proc.Mp())
		}
		ctr.freeBats[ctr.i] = b
	}
	ctr.i++
	return b, nil
}

func (ctr *container) bindMemory(proc *process.Process) error {
	err := ctr.memory.Bind(proc, ctr.freeBats)
	if err == nil || !moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded) {
		return err
	}
	for _, bat := range ctr.freeBats {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}
	ctr.buf = nil
	ctr.bats = nil
	ctr.freeBats = nil
	ctr.i = 0
	ctr.memory.Release()
	return ctr.memory.Bind(proc, nil)
}

func makeRecursiveBatch(proc *process.Process) (*batch.Batch, error) {
	b := batch.NewWithSize(1)
	b.Attrs = []string{
		"recursive_col",
	}
	b.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
	if err := fillRecursiveBatch(proc, b); err != nil {
		b.Clean(proc.Mp())
		return nil, err
	}
	return b, nil
}

func fillRecursiveBatch(proc *process.Process, b *batch.Batch) error {
	if err := vector.AppendBytes(b.GetVector(0), []byte("check recursive status"), false, proc.GetMPool()); err != nil {
		return err
	}
	b.Attrs = append(b.Attrs[:0], "recursive_col")
	b.ShuffleIDX = 0
	batch.SetLength(b, 1)
	b.SetLast()
	return nil
}
