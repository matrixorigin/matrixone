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

package colexec

import (
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/sql/plan"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Batches struct {
	Buf []*batch.Batch
}

func (bs *Batches) Clean(mp *mpool.MPool) {
	for i := range bs.Buf {
		bs.Buf[i].Clean(mp)
	}
}

func (bs *Batches) CleanOnlyData() {
	for i := range bs.Buf {
		bs.Buf[i].CleanOnlyData()
	}
}
func (bs *Batches) Reset() {
	if bs.Buf != nil {
		bs.Buf = bs.Buf[:0]
	}
}

// copy from input batch into batches
// the batches structure hold data in fix size 8192 rows, and continue to append from next batch
// if error return , the batches will clean itself
func (bs *Batches) CopyIntoBatches(src *batch.Batch, proc *process.Process) (err error) {
	defer func() {
		if err != nil {
			bs.Clean(proc.Mp())
		}
	}()

	if bs.Buf == nil {
		bs.Buf = make([]*batch.Batch, 0, 16)
	}

	var tmp *batch.Batch
	if src.RowCount() == DefaultBatchSize {
		tmp, err = src.Dup(proc.Mp())
		if err != nil {
			return err
		}
		bs.Buf = append(bs.Buf, tmp)
		lenBuf := len(bs.Buf)
		if lenBuf > 1 && bs.Buf[lenBuf-2].RowCount() != DefaultBatchSize {
			//swap the last 2 batches to get right order
			bs.Buf[lenBuf-2], bs.Buf[lenBuf-1] = bs.Buf[lenBuf-1], bs.Buf[lenBuf-2]
		}
		return nil
	}

	offset := 0
	appendRows := 0
	length := src.RowCount()

	for offset < length {
		lenBuf := len(bs.Buf)
		if lenBuf > 0 && bs.Buf[lenBuf-1].RowCount() != DefaultBatchSize {
			tmp = bs.Buf[lenBuf-1]
		} else {
			tmp, err = proc.NewBatchFromSrc(src, 0)
			if err != nil {
				return err
			}
			bs.Buf = append(bs.Buf, tmp)
		}
		appendRows, err = appendToFixedSizeFromOffset(tmp, src, offset, proc)
		if err != nil {
			return err
		}
		offset += appendRows
	}
	return nil
}

func appendToFixedSizeFromOffset(dst *batch.Batch, src *batch.Batch, offset int, proc *process.Process) (int, error) {
	var err error
	if dst == nil {
		panic("should not be nil")
	}
	if dst.RowCount() >= DefaultBatchSize {
		panic("can't call AppendToFixedSizeFromOffset when batch is full!")
	}
	if len(dst.Vecs) != len(src.Vecs) {
		return 0, moerr.NewInternalError(proc.Ctx, "unexpected error happens in batch append")
	}
	length := DefaultBatchSize - dst.RowCount()
	if length+offset > src.RowCount() {
		length = src.RowCount() - offset
	}
	for i := range dst.Vecs {
		if err = dst.Vecs[i].UnionBatch(src.Vecs[i], int64(offset), length, nil, proc.Mp()); err != nil {
			return 0, err
		}
		dst.Vecs[i].SetSorted(false)
	}
	dst.AddRowCount(length)
	return length, nil
}

type HashmapBuilder struct {
	InputBatchRowCount int
	vecs               [][]*vector.Vector
	IntHashMap         *hashmap.IntHashMap
	StrHashMap         *hashmap.StrHashMap
	MultiSels          [][]int32
	keyWidth           int // keyWidth is the width of hash columns, it determines which hash map to use.
	Batches            Batches
	executor           []ExpressionExecutor
	UniqueJoinKeys     []*vector.Vector
}

func (hb *HashmapBuilder) GetSize() int64 {
	if hb.IntHashMap != nil {
		return hb.IntHashMap.Size()
	} else if hb.StrHashMap != nil {
		return hb.StrHashMap.Size()
	}
	return 0
}

func (hb *HashmapBuilder) GetGroupCount() uint64 {
	if hb.IntHashMap != nil {
		return hb.IntHashMap.GroupCount()
	} else if hb.StrHashMap != nil {
		return hb.StrHashMap.GroupCount()
	}
	return 0
}

func (hb *HashmapBuilder) Prepare(Conditions []*plan.Expr, proc *process.Process) error {
	var err error
	if len(hb.executor) == 0 {
		hb.vecs = make([][]*vector.Vector, 0)
		hb.executor = make([]ExpressionExecutor, len(Conditions))
		hb.keyWidth = 0
		for i, expr := range Conditions {
			typ := expr.Typ
			width := types.T(typ.Id).TypeLen()
			// todo : for varlena type, always go strhashmap
			if types.T(typ.Id).FixedLength() < 0 {
				width = 128
			}
			hb.keyWidth += width
			hb.executor[i], err = NewExpressionExecutor(proc, Conditions[i])
			if err != nil {
				return err
			}
		}
	}
	if hb.keyWidth <= 8 {
		if hb.IntHashMap, err = hashmap.NewIntHashMap(false, proc.Mp()); err != nil {
			return err
		}
	} else {
		if hb.StrHashMap, err = hashmap.NewStrMap(false, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func (hb *HashmapBuilder) Reset() {
	hb.InputBatchRowCount = 0
	hb.Batches.Reset()
	hb.IntHashMap = nil
	hb.StrHashMap = nil
	if len(hb.MultiSels) > 0 {
		hb.MultiSels = hb.MultiSels[:0]
	}
	for i := range hb.executor {
		if hb.executor[i] != nil {
			hb.executor[i].ResetForNextQuery()
		}
	}
}

func (hb *HashmapBuilder) Free() {
	hb.Batches.Reset()
	hb.IntHashMap = nil
	hb.StrHashMap = nil
	hb.MultiSels = nil
	for i := range hb.executor {
		if hb.executor[i] != nil {
			hb.executor[i].Free()
		}
	}
	hb.executor = nil
	if hb.IntHashMap != nil {
		hb.IntHashMap.Free()
		hb.IntHashMap = nil
	}
	if hb.StrHashMap != nil {
		hb.StrHashMap.Free()
		hb.StrHashMap = nil
	}
}

func (hb *HashmapBuilder) evalJoinCondition(proc *process.Process) error {
	for idx1 := range hb.Batches.Buf {
		tmpVes := make([]*vector.Vector, len(hb.executor))
		hb.vecs = append(hb.vecs, tmpVes)
		for idx2 := range hb.executor {
			vec, err := hb.executor[idx2].Eval(proc, []*batch.Batch{hb.Batches.Buf[idx1]}, nil)
			if err != nil {
				return err
			}
			hb.vecs[idx1][idx2] = vec
		}
	}
	return nil
}

func (hb *HashmapBuilder) BuildHashmap(hashOnPK bool, needAllocateSels bool, runtimeFilterSpec *pbplan.RuntimeFilterSpec, proc *process.Process) error {
	if hb.InputBatchRowCount == 0 {
		return nil
	}

	if err := hb.evalJoinCondition(proc); err != nil {
		return err
	}

	var itr hashmap.Iterator
	if hb.keyWidth <= 8 {
		itr = hb.IntHashMap.NewIterator()
	} else {
		itr = hb.StrHashMap.NewIterator()
	}

	if hashOnPK {
		// if hash on primary key, prealloc hashmap size to the count of batch
		if hb.keyWidth <= 8 {
			err := hb.IntHashMap.PreAlloc(uint64(hb.InputBatchRowCount), proc.Mp())
			if err != nil {
				return err
			}
		} else {
			err := hb.StrHashMap.PreAlloc(uint64(hb.InputBatchRowCount), proc.Mp())
			if err != nil {
				return err
			}
		}
	} else {
		if needAllocateSels {
			hb.MultiSels = make([][]int32, hb.InputBatchRowCount)
		}
	}

	var (
		cardinality uint64
		sels        []int32
	)

	for i := 0; i < hb.InputBatchRowCount; i += hashmap.UnitLimit {
		if i%(hashmap.UnitLimit*32) == 0 {
			runtime.Gosched()
		}
		n := hb.InputBatchRowCount - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		// if not hash on primary key, estimate the hashmap size after 8192 rows
		//preAlloc to improve performance and reduce memory reAlloc
		if !hashOnPK && hb.InputBatchRowCount > hashmap.HashMapSizeThreshHold && i == hashmap.HashMapSizeEstimate {
			if hb.keyWidth <= 8 {
				groupCount := hb.IntHashMap.GroupCount()
				rate := float64(groupCount) / float64(i)
				hashmapCount := uint64(float64(hb.InputBatchRowCount) * rate)
				if hashmapCount > groupCount {
					err := hb.IntHashMap.PreAlloc(hashmapCount-groupCount, proc.Mp())
					if err != nil {
						return err
					}
				}
			} else {
				groupCount := hb.StrHashMap.GroupCount()
				rate := float64(groupCount) / float64(i)
				hashmapCount := uint64(float64(hb.InputBatchRowCount) * rate)
				if hashmapCount > groupCount {
					err := hb.StrHashMap.PreAlloc(hashmapCount-groupCount, proc.Mp())
					if err != nil {
						return err
					}
				}
			}
		}

		vecIdx1 := i / DefaultBatchSize
		vecIdx2 := i % DefaultBatchSize
		vals, zvals, err := itr.Insert(vecIdx2, n, hb.vecs[vecIdx1])
		if err != nil {
			return err
		}
		for k, v := range vals[:n] {
			if zvals[k] == 0 || v == 0 {
				continue
			}
			ai := int64(v) - 1

			if !hashOnPK && needAllocateSels {
				if hb.MultiSels[ai] == nil {
					hb.MultiSels[ai] = make([]int32, 0)
				}
				hb.MultiSels[ai] = append(hb.MultiSels[ai], int32(i+k))
			}
		}

		if runtimeFilterSpec != nil {
			if len(hb.UniqueJoinKeys) == 0 {
				hb.UniqueJoinKeys = make([]*vector.Vector, len(hb.executor))
				for j, vec := range hb.vecs[vecIdx1] {
					hb.UniqueJoinKeys[j] = vector.NewVec(*vec.GetType())
				}
			}

			if hashOnPK {
				for j, vec := range hb.vecs[vecIdx1] {
					err = hb.UniqueJoinKeys[j].UnionBatch(vec, int64(vecIdx2), n, nil, proc.Mp())
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

				for j, vec := range hb.vecs[vecIdx1] {
					for _, sel := range sels {
						_, idx2 := sel/DefaultBatchSize, sel%DefaultBatchSize
						err = hb.UniqueJoinKeys[j].UnionOne(vec, int64(idx2), proc.Mp())
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
