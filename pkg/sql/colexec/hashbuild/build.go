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

package hashbuild

import (
	"bytes"
	"runtime"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "hash_build"

func (hashBuild *HashBuild) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": hash build ")
}

func (hashBuild *HashBuild) OpType() vm.OpType {
	return vm.HashBuild
}

func (hashBuild *HashBuild) Prepare(proc *process.Process) (err error) {
	hashBuild.ctr = new(container)

	if hashBuild.NeedHashMap {
		hashBuild.ctr.vecs = make([][]*vector.Vector, 0)
		ctr := hashBuild.ctr
		ctr.executor = make([]colexec.ExpressionExecutor, len(hashBuild.Conditions))
		ctr.keyWidth = 0
		for i, expr := range hashBuild.Conditions {
			typ := expr.Typ
			width := types.T(typ.Id).TypeLen()
			// todo : for varlena type, always go strhashmap
			if types.T(typ.Id).FixedLength() < 0 {
				width = 128
			}
			ctr.keyWidth += width
			ctr.executor[i], err = colexec.NewExpressionExecutor(proc, hashBuild.Conditions[i])
			if err != nil {
				return err
			}
		}

		if ctr.keyWidth <= 8 {
			if ctr.intHashMap, err = hashmap.NewIntHashMap(false, proc.Mp()); err != nil {
				return err
			}
		} else {
			if ctr.strHashMap, err = hashmap.NewStrMap(false, proc.Mp()); err != nil {
				return err
			}
		}
	}

	hashBuild.ctr.batches = make([]*batch.Batch, 0)

	return nil
}

func (hashBuild *HashBuild) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(hashBuild.GetIdx(), hashBuild.GetParallelIdx(), hashBuild.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	result := vm.NewCallResult()
	ap := hashBuild
	ctr := ap.ctr
	for {
		switch ctr.state {
		case BuildHashMap:
			if err := ctr.build(ap, proc, anal, hashBuild.GetIsFirst()); err != nil {
				ctr.cleanHashMap()
				return result, err
			}
			if ap.ctr.intHashMap != nil {
				anal.Alloc(ap.ctr.intHashMap.Size())
			} else if ap.ctr.strHashMap != nil {
				anal.Alloc(ap.ctr.strHashMap.Size())
			}
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(ap, proc); err != nil {
				return result, err
			}
			ctr.state = SendJoinMap

		case SendJoinMap:
			var jm *message.JoinMap
			if ctr.inputBatchRowCount > 0 {
				jm = message.NewJoinMap(ctr.multiSels, ctr.intHashMap, ctr.strHashMap, ctr.batches, proc.Mp())
				jm.SetPushedRuntimeFilterIn(ctr.runtimeFilterIn)
				if ap.NeedMergedBatch {
					jm.SetRowCount(int64(ctr.inputBatchRowCount))
				}
				jm.IncRef(ap.JoinMapRefCnt)
			}
			if ap.JoinMapTag <= 0 {
				panic("wrong joinmap message tag!")
			}
			message.SendMessage(message.JoinMapMsg{JoinMapPtr: jm, Tag: ap.JoinMapTag}, proc.GetMessageBoard())

			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

// make sure src is not empty
func (ctr *container) mergeIntoBatches(src *batch.Batch, proc *process.Process) error {
	var err error
	if src.RowCount() == colexec.DefaultBatchSize {
		ctr.batches = append(ctr.batches, src)
		return nil
	} else {
		offset := 0
		appendRows := 0
		length := src.RowCount()
		for offset < length {
			ctr.tmpBatch, appendRows, err = proc.AppendToFixedSizeFromOffset(ctr.tmpBatch, src, offset)
			if err != nil {
				return err
			}
			if ctr.tmpBatch.RowCount() == colexec.DefaultBatchSize {
				ctr.batches = append(ctr.batches, ctr.tmpBatch)
				ctr.tmpBatch = nil
			}
			offset += appendRows
		}
		proc.PutBatch(src)
	}
	return nil
}

func (ctr *container) collectBuildBatches(hashBuild *HashBuild, proc *process.Process, anal process.Analyze, isFirst bool) error {
	var currentBatch *batch.Batch
	for {
		result, err := vm.ChildrenCall(hashBuild.GetChildren(0), proc, anal)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			break
		}
		currentBatch = result.Batch
		atomic.AddInt64(&currentBatch.Cnt, 1)
		if currentBatch.IsEmpty() {
			proc.PutBatch(currentBatch)
			continue
		}
		anal.Input(currentBatch, isFirst)
		anal.Alloc(int64(currentBatch.Size()))
		ctr.inputBatchRowCount += currentBatch.RowCount()
		err = ctr.mergeIntoBatches(currentBatch, proc)
		if err != nil {
			return err
		}
	}
	if ctr.tmpBatch != nil && ctr.tmpBatch.RowCount() > 0 {
		ctr.batches = append(ctr.batches, ctr.tmpBatch)
		ctr.tmpBatch = nil
	}
	return nil
}

func (ctr *container) buildHashmap(ap *HashBuild, proc *process.Process) error {
	if len(ctr.batches) == 0 || !ap.NeedHashMap {
		return nil
	}
	var err error
	if err = ctr.evalJoinCondition(proc); err != nil {
		return err
	}

	var itr hashmap.Iterator
	if ctr.keyWidth <= 8 {
		itr = ctr.intHashMap.NewIterator()
	} else {
		itr = ctr.strHashMap.NewIterator()
	}

	if ap.HashOnPK {
		// if hash on primary key, prealloc hashmap size to the count of batch
		if ctr.keyWidth <= 8 {
			err = ctr.intHashMap.PreAlloc(uint64(ctr.inputBatchRowCount), proc.Mp())
		} else {
			err = ctr.strHashMap.PreAlloc(uint64(ctr.inputBatchRowCount), proc.Mp())
		}
		if err != nil {
			return err
		}
	} else {
		if ap.NeedAllocateSels {
			ctr.multiSels = make([][]int32, ctr.inputBatchRowCount)
		}
	}

	var (
		cardinality uint64
		sels        []int32
	)

	for i := 0; i < ctr.inputBatchRowCount; i += hashmap.UnitLimit {
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}
		n := ctr.inputBatchRowCount - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		// if not hash on primary key, estimate the hashmap size after 8192 rows
		//preAlloc to improve performance and reduce memory reAlloc
		if !ap.HashOnPK && ctr.inputBatchRowCount > hashmap.HashMapSizeThreshHold && i == hashmap.HashMapSizeEstimate {
			if ctr.keyWidth <= 8 {
				groupCount := ctr.intHashMap.GroupCount()
				rate := float64(groupCount) / float64(i)
				hashmapCount := uint64(float64(ctr.inputBatchRowCount) * rate)
				if hashmapCount > groupCount {
					err = ctr.intHashMap.PreAlloc(hashmapCount-groupCount, proc.Mp())
				}
			} else {
				groupCount := ctr.strHashMap.GroupCount()
				rate := float64(groupCount) / float64(i)
				hashmapCount := uint64(float64(ctr.inputBatchRowCount) * rate)
				if hashmapCount > groupCount {
					err = ctr.strHashMap.PreAlloc(hashmapCount-groupCount, proc.Mp())
				}
			}
			if err != nil {
				return err
			}
		}

		vecIdx1 := i / colexec.DefaultBatchSize
		vecIdx2 := i % colexec.DefaultBatchSize
		vals, zvals, err := itr.Insert(vecIdx2, n, ctr.vecs[vecIdx1])
		if err != nil {
			return err
		}
		for k, v := range vals[:n] {
			if zvals[k] == 0 || v == 0 {
				continue
			}
			ai := int64(v) - 1

			if !ap.HashOnPK && ap.NeedAllocateSels {
				if ctr.multiSels[ai] == nil {
					ctr.multiSels[ai] = make([]int32, 0)
				}
				ctr.multiSels[ai] = append(ctr.multiSels[ai], int32(i+k))
			}
		}

		if ap.RuntimeFilterSpec != nil {
			if len(ap.ctr.uniqueJoinKeys) == 0 {
				ap.ctr.uniqueJoinKeys = make([]*vector.Vector, len(ctr.executor))
				for j, vec := range ctr.vecs[vecIdx1] {
					ap.ctr.uniqueJoinKeys[j] = proc.GetVector(*vec.GetType())
				}
			}

			if ap.HashOnPK {
				for j, vec := range ctr.vecs[vecIdx1] {
					err = ap.ctr.uniqueJoinKeys[j].UnionBatch(vec, int64(vecIdx2), n, nil, proc.Mp())
					if err != nil {
						return err
					}
				}
			} else {
				if sels == nil {
					sels = make([]int32, hashmap.UnitLimit)
				}

				sels = sels[:0]
				for j, v := range vals[:n] {
					if v > cardinality {
						sels = append(sels, int32(i+j))
						cardinality = v
					}
				}

				for j, vec := range ctr.vecs[vecIdx1] {
					for _, sel := range sels {
						_, idx2 := sel/colexec.DefaultBatchSize, sel%colexec.DefaultBatchSize
						err = ap.ctr.uniqueJoinKeys[j].UnionOne(vec, int64(idx2), proc.Mp())
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func (ctr *container) build(ap *HashBuild, proc *process.Process, anal process.Analyze, isFirst bool) error {
	err := ctr.collectBuildBatches(ap, proc, anal, isFirst)
	if err != nil {
		return err
	}
	err = ctr.buildHashmap(ap, proc)
	if err != nil {
		return err
	}
	if !ap.NeedMergedBatch {
		// if do not need merged batch, free it now to save memory
		for i := range ctr.batches {
			proc.PutBatch(ctr.batches[i])
		}
		ctr.batches = nil
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *HashBuild, proc *process.Process) error {
	if ap.RuntimeFilterSpec == nil {
		return nil
	}

	var runtimeFilter message.RuntimeFilterMessage
	runtimeFilter.Tag = ap.RuntimeFilterSpec.Tag

	if ap.RuntimeFilterSpec.Expr == nil {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else if ctr.inputBatchRowCount == 0 || len(ctr.uniqueJoinKeys) == 0 || ctr.uniqueJoinKeys[0].Length() == 0 {
		runtimeFilter.Typ = message.RuntimeFilter_DROP
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	}

	var hashmapCount uint64
	if ctr.keyWidth <= 8 {
		hashmapCount = ctr.intHashMap.GroupCount()
	} else {
		hashmapCount = ctr.strHashMap.GroupCount()
	}

	inFilterCardLimit := ap.RuntimeFilterSpec.UpperLimit
	//inFilterCardLimit := plan.GetInFilterCardLimit()
	//bloomFilterCardLimit := int64(plan.BloomFilterCardLimit)
	//v, ok = runtime.ProcessLevelRuntime().GetGlobalVariables("runtime_filter_limit_bloom_filter")
	//if ok {
	//	bloomFilterCardLimit = v.(int64)
	//}

	vec := ctr.uniqueJoinKeys[0]

	defer func() {
		vec.Free(proc.Mp())
		ctr.uniqueJoinKeys = nil
	}()

	if hashmapCount > uint64(inFilterCardLimit) {
		runtimeFilter.Typ = message.RuntimeFilter_PASS
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		return nil
	} else {
		// Composite primary key
		if ap.RuntimeFilterSpec.Expr.GetF() != nil {
			bat := batch.NewWithSize(len(ctr.uniqueJoinKeys))
			bat.SetRowCount(vec.Length())
			copy(bat.Vecs, ctr.uniqueJoinKeys)

			newVec, err := colexec.EvalExpressionOnce(proc, ap.RuntimeFilterSpec.Expr, []*batch.Batch{bat})
			if err != nil {
				return err
			}

			for i := range ctr.uniqueJoinKeys {
				ctr.uniqueJoinKeys[i].Free(proc.Mp())
			}

			vec = newVec
		}

		vec.InplaceSort()
		data, err := vec.MarshalBinary()
		if err != nil {
			return err
		}

		runtimeFilter.Typ = message.RuntimeFilter_IN
		runtimeFilter.Card = int32(vec.Length())
		runtimeFilter.Data = data
		message.SendRuntimeFilter(runtimeFilter, ap.RuntimeFilterSpec, proc.GetMessageBoard())
		ctr.runtimeFilterIn = true
	}
	return nil
}

func (ctr *container) evalJoinCondition(proc *process.Process) error {
	for idx1 := range ctr.batches {
		tmpVes := make([]*vector.Vector, len(ctr.executor))
		ctr.vecs = append(ctr.vecs, tmpVes)
		for idx2 := range ctr.executor {
			vec, err := ctr.executor[idx2].Eval(proc, []*batch.Batch{ctr.batches[idx1]}, nil)
			if err != nil {
				return err
			}
			ctr.vecs[idx1][idx2] = vec
		}
	}
	return nil
}
