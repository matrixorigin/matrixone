// Copyright 2024 Matrix Origin
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

package hashmap_util

import (
	"runtime"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type HashmapBuilder struct {
	needDupVec         bool
	InputBatchRowCount int
	vecs               [][]*vector.Vector
	IntHashMap         *hashmap.IntHashMap
	StrHashMap         *hashmap.StrHashMap
	MultiSels          message.JoinSels
	keyWidth           int // keyWidth is the width of hash columns, it determines which hash map to use.
	Batches            colexec.Batches
	executors          []colexec.ExpressionExecutor
	UniqueJoinKeys     []*vector.Vector

	IsDedup           bool
	OnDuplicateAction plan.Node_OnDuplicateAction
	DedupColName      string
	DedupColTypes     []plan.Type

	IgnoreRows *bitmap.Bitmap

	delColIdx int32
	delVecs   []*vector.Vector
	DelRows   *bitmap.Bitmap
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

func (hb *HashmapBuilder) Prepare(keyCols []*plan.Expr, delColIdx int32, proc *process.Process) error {
	var err error
	if len(hb.executors) == 0 {
		hb.needDupVec = false
		hb.vecs = make([][]*vector.Vector, 0)
		hb.executors = make([]colexec.ExpressionExecutor, len(keyCols))
		hb.keyWidth = 0
		hb.InputBatchRowCount = 0
		for i, expr := range keyCols {
			if _, ok := keyCols[i].Expr.(*plan.Expr_Col); !ok {
				hb.needDupVec = true
			}
			typ := expr.Typ
			width := types.T(typ.Id).TypeLen()
			// todo : for varlena type, always go strhashmap
			if types.T(typ.Id).FixedLength() < 0 {
				width = 128
			}
			hb.keyWidth += width
			hb.executors[i], err = colexec.NewExpressionExecutor(proc, keyCols[i])
			if err != nil {
				return err
			}
		}
	}

	if hb.IsDedup {
		hb.delColIdx = delColIdx
	} else {
		hb.delColIdx = -1
	}

	return nil
}

func (hb *HashmapBuilder) Reset(proc *process.Process, hashTableHasNotSent bool) {
	if hashTableHasNotSent || hb.InputBatchRowCount == 0 {
		hb.FreeHashMapAndBatches(proc)
	}

	if hb.needDupVec {
		for i := range hb.vecs {
			for j := range hb.vecs[i] {
				hb.vecs[i][j].Free(proc.Mp())
			}
		}
	}
	hb.InputBatchRowCount = 0
	hb.Batches.Reset()
	hb.IntHashMap = nil
	hb.StrHashMap = nil
	hb.vecs = nil
	for i := range hb.UniqueJoinKeys {
		hb.UniqueJoinKeys[i].Free(proc.Mp())
	}
	hb.UniqueJoinKeys = nil
	hb.MultiSels.Free()
	for i := range hb.executors {
		if hb.executors[i] != nil {
			hb.executors[i].ResetForNextQuery()
		}
	}
}

func (hb *HashmapBuilder) Free(proc *process.Process) {
	hb.needDupVec = false
	hb.Batches.Reset()
	hb.IntHashMap = nil
	hb.StrHashMap = nil
	hb.MultiSels.Free()
	for i := range hb.executors {
		if hb.executors[i] != nil {
			hb.executors[i].Free()
		}
	}
	hb.executors = nil
	hb.vecs = nil
	for i := range hb.UniqueJoinKeys {
		hb.UniqueJoinKeys[i].Free(proc.Mp())
	}
	hb.UniqueJoinKeys = nil
}

func (hb *HashmapBuilder) FreeHashMapAndBatches(proc *process.Process) {
	if hb.IntHashMap != nil {
		hb.IntHashMap.Free()
		hb.IntHashMap = nil
	}
	if hb.StrHashMap != nil {
		hb.StrHashMap.Free()
		hb.StrHashMap = nil
	}
	hb.Batches.Clean(proc.Mp())
}

func (hb *HashmapBuilder) evalJoinCondition(proc *process.Process) error {
	for idx1 := range hb.Batches.Buf {
		tmpVes := make([]*vector.Vector, len(hb.executors))
		hb.vecs = append(hb.vecs, tmpVes)
		for idx2 := range hb.executors {
			vec, err := hb.executors[idx2].Eval(proc, []*batch.Batch{hb.Batches.Buf[idx1]}, nil)
			if err != nil {
				return err
			}
			if hb.needDupVec {
				hb.vecs[idx1][idx2], err = vec.Dup(proc.Mp())
				if err != nil {
					return err
				}
			} else {
				hb.vecs[idx1][idx2] = vec
			}
		}
	}

	if hb.delColIdx != -1 {
		hb.delVecs = make([]*vector.Vector, len(hb.Batches.Buf))
		for i := range hb.Batches.Buf {
			hb.delVecs[i] = hb.Batches.Buf[i].Vecs[hb.delColIdx]
		}
	}

	return nil
}

func (hb *HashmapBuilder) BuildHashmap(hashOnPK bool, needAllocateSels bool, needUniqueVec bool, proc *process.Process) error {
	if hb.InputBatchRowCount == 0 {
		return nil
	}

	var err error
	if err = hb.evalJoinCondition(proc); err != nil {
		return err
	}

	var itr hashmap.Iterator
	if hb.keyWidth <= 8 {
		if hb.IntHashMap, err = hashmap.NewIntHashMap(false, proc.Mp()); err != nil {
			return err
		}
		itr = hb.IntHashMap.NewIterator()
	} else {
		if hb.StrHashMap, err = hashmap.NewStrHashMap(false, proc.Mp()); err != nil {
			return err
		}
		itr = hb.StrHashMap.NewIterator()
	}

	if hashOnPK || hb.IsDedup {
		// if hash on primary key, prealloc hashmap size to the count of batch
		if hb.keyWidth <= 8 {
			err = hb.IntHashMap.PreAlloc(uint64(hb.InputBatchRowCount))
			if err != nil {
				return err
			}
		} else {
			err = hb.StrHashMap.PreAlloc(uint64(hb.InputBatchRowCount))
			if err != nil {
				return err
			}
		}
	}

	if needAllocateSels {
		hb.MultiSels.InitSel(hb.InputBatchRowCount)
	}

	if hb.IsDedup && hb.OnDuplicateAction == plan.Node_IGNORE {
		hb.IgnoreRows = &bitmap.Bitmap{}
		hb.IgnoreRows.InitWithSize(int64(hb.InputBatchRowCount))
	}

	var (
		vOld        uint64
		cardinality uint64
		newSels     []int64
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
		if !hashOnPK && !hb.IsDedup && hb.InputBatchRowCount > hashmap.HashMapSizeThreshHold && i == hashmap.HashMapSizeEstimate {
			if hb.keyWidth <= 8 {
				groupCount := hb.IntHashMap.GroupCount()
				rate := float64(groupCount) / float64(i)
				hashmapCount := uint64(float64(hb.InputBatchRowCount) * rate)
				if hashmapCount > groupCount {
					err := hb.IntHashMap.PreAlloc(hashmapCount - groupCount)
					if err != nil {
						return err
					}
				}
			} else {
				groupCount := hb.StrHashMap.GroupCount()
				rate := float64(groupCount) / float64(i)
				hashmapCount := uint64(float64(hb.InputBatchRowCount) * rate)
				if hashmapCount > groupCount {
					err := hb.StrHashMap.PreAlloc(hashmapCount - groupCount)
					if err != nil {
						return err
					}
				}
			}
		}

		vecIdx1 := i / colexec.DefaultBatchSize
		vecIdx2 := i % colexec.DefaultBatchSize
		vals, zvals, err := itr.Insert(vecIdx2, n, hb.vecs[vecIdx1])
		if err != nil {
			return err
		}
		for k, v := range vals[:n] {
			if hb.IsDedup && hb.OnDuplicateAction == plan.Node_UPDATE {
				hb.MultiSels.InsertSel(int32(v), int32(i+k))
				continue
			}

			if zvals[k] == 0 || v == 0 {
				continue
			}

			if hb.IsDedup {
				if v <= cardinality {
					switch hb.OnDuplicateAction {
					case plan.Node_FAIL:
						var rowStr string
						if len(hb.DedupColTypes) == 1 {
							if hb.DedupColName == catalog.IndexTableIndexColName {
								if hb.vecs[vecIdx1][0].GetType().Oid == types.T_varchar {
									t, _, schema, err := types.DecodeTuple(hb.vecs[vecIdx1][0].GetBytesAt(vecIdx2 + k))
									if err == nil && len(schema) > 1 {
										rowStr = t.ErrString(make([]int32, len(schema)))
									}
								}
							}

							if len(rowStr) == 0 {
								rowStr = hb.vecs[vecIdx1][0].RowToString(vecIdx2 + k)
							}
						} else {
							rowItems, err := types.StringifyTuple(hb.vecs[vecIdx1][0].GetBytesAt(vecIdx2+k), hb.DedupColTypes)
							if err != nil {
								return err
							}
							rowStr = "(" + strings.Join(rowItems, ",") + ")"
						}
						return moerr.NewDuplicateEntry(proc.Ctx, rowStr, hb.DedupColName)
					case plan.Node_IGNORE:
						hb.IgnoreRows.Add(uint64(i + k))
					}
				} else {
					cardinality = v
				}
			} else if !hashOnPK && needAllocateSels {
				hb.MultiSels.InsertSel(int32(v-1), int32(i+k))
			}
		}

		if needUniqueVec {
			if len(hb.UniqueJoinKeys) == 0 {
				hb.UniqueJoinKeys = make([]*vector.Vector, len(hb.executors))
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
				if newSels == nil {
					newSels = make([]int64, hashmap.UnitLimit)
				}

				newSels = newSels[:0]
				for j, v := range vals[:n] {
					if v > vOld {
						newSels = append(newSels, int64(vecIdx2+j))
						vOld = v
					}
				}

				for j, vec := range hb.vecs[vecIdx1] {
					err = hb.UniqueJoinKeys[j].Union(vec, newSels, proc.Mp())
					if err != nil {
						return err
					}
				}
			}
		}
	}

	if hb.delColIdx != -1 {
		hb.DelRows = &bitmap.Bitmap{}
		hb.DelRows.InitWithSize(int64(cardinality))

		tmpVecs := make([]*vector.Vector, 1)
		for i := 0; i < hb.InputBatchRowCount; i += hashmap.UnitLimit {
			if i%(hashmap.UnitLimit*32) == 0 {
				runtime.Gosched()
			}
			n := hb.InputBatchRowCount - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			vecIdx1 := i / colexec.DefaultBatchSize
			vecIdx2 := i % colexec.DefaultBatchSize
			tmpVecs[0] = hb.delVecs[vecIdx1]
			vals, zvals := itr.Find(vecIdx2, n, tmpVecs)

			for k, v := range vals[:n] {
				if zvals[k] != 0 && v != 0 {
					hb.DelRows.Add(v - 1)
				}
			}
		}
	}

	if hb.IsDedup && hb.OnDuplicateAction == plan.Node_IGNORE {
		err := hb.Batches.Shrink(hb.IgnoreRows, proc)
		if err != nil {
			return err
		}
		// Update InputBatchRowCount to reflect the actual row count after shrinking
		// This is critical because IgnoreRows removed duplicate rows, so the actual
		// row count in batches is now less than the original InputBatchRowCount
		hb.InputBatchRowCount = hb.Batches.RowCount()
	}

	// if groupcount == inputrowcount, it means building hashmap on unique rows
	// we can free sels now
	if hb.keyWidth <= 8 {
		if hb.InputBatchRowCount == int(hb.IntHashMap.GroupCount()) {
			hb.MultiSels.Free()
		}
	} else {
		if hb.InputBatchRowCount == int(hb.StrHashMap.GroupCount()) {
			hb.MultiSels.Free()
		}
	}

	return nil
}
