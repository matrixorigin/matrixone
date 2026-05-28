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

	// DedupBuildKeepLast: when true, fan-out duplicates in the build side (one new
	// row matching multiple old rows) are kept as delete-only rows rather than
	// discarded, so all conflicting old rows are deleted and the new row is inserted
	// exactly once. Used by REPLACE INTO with PK + unique key conflicts.
	DedupBuildKeepLast bool

	IgnoreRows *bitmap.Bitmap

	delColIdx                 int32
	dedupDeleteMarkerColIdx   int32
	dedupDeleteKeepColIdxList []int32
	delVecs                   []*vector.Vector
	DelRows                   *bitmap.Bitmap
}

func (hb *HashmapBuilder) GetSize() int64 {
	var sz int64
	if hb.IntHashMap != nil {
		sz += hb.IntHashMap.Size()
	} else if hb.StrHashMap != nil {
		sz += hb.StrHashMap.Size()
	}
	sz += hb.MultiSels.Size()
	for _, v := range hb.UniqueJoinKeys {
		if v != nil {
			sz += int64(v.Allocated())
		}
	}
	if hb.IgnoreRows != nil {
		sz += int64(hb.IgnoreRows.Size())
	}
	if hb.DelRows != nil {
		sz += int64(hb.DelRows.Size())
	}
	return sz
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
	hb.dedupDeleteMarkerColIdx = -1

	return nil
}

// SetDedupDeleteInfo configures the delete-marker and keep-column lists used by
// keepDiscardedRowsForDelete. Must be called after Prepare.
func (hb *HashmapBuilder) SetDedupDeleteInfo(markerColIdx int32, keepColIdxList []int32) {
	hb.dedupDeleteMarkerColIdx = markerColIdx
	hb.dedupDeleteKeepColIdxList = keepColIdxList
}

func (hb *HashmapBuilder) Reset(proc *process.Process, hashTableHasNotSent bool) {
	if hashTableHasNotSent || hb.InputBatchRowCount == 0 {
		hb.FreeHashMapAndBatches(proc)
	}

	if hb.needDupVec {
		for i := range hb.vecs {
			if hb.vecs[i] != nil {
				for j := range hb.vecs[i] {
					if hb.vecs[i][j] != nil {
						hb.vecs[i][j].Free(proc.Mp())
					}
				}
			}
		}
	}
	hb.InputBatchRowCount = 0
	hb.Batches.Reset()
	hb.IntHashMap = nil
	hb.StrHashMap = nil
	hb.vecs = nil
	for i := range hb.UniqueJoinKeys {
		if hb.UniqueJoinKeys[i] != nil {
			hb.UniqueJoinKeys[i].Free(proc.Mp())
		}
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
		if hb.UniqueJoinKeys[i] != nil {
			hb.UniqueJoinKeys[i].Free(proc.Mp())
		}
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

// cleanupPartiallyCreatedVecs frees all vectors in hb.vecs that were successfully created.
// This is used when an error occurs during evalJoinCondition to prevent memory leaks
// and nil pointer dereferences in Reset.
func (hb *HashmapBuilder) cleanupPartiallyCreatedVecs(proc *process.Process) {
	for i := 0; i < len(hb.vecs); i++ {
		if hb.vecs[i] != nil {
			for j := 0; j < len(hb.vecs[i]); j++ {
				if hb.vecs[i][j] != nil {
					hb.vecs[i][j].Free(proc.Mp())
				}
			}
		}
	}
	hb.vecs = nil
}

func (hb *HashmapBuilder) evalJoinCondition(proc *process.Process) error {
	for idx1 := range hb.Batches.Buf {
		tmpVes := make([]*vector.Vector, len(hb.executors))
		hb.vecs = append(hb.vecs, tmpVes)
		for idx2 := range hb.executors {
			vec, err := hb.executors[idx2].Eval(proc, []*batch.Batch{hb.Batches.Buf[idx1]}, nil)
			if err != nil {
				// Clean up partially created vecs to prevent nil pointer issues in Reset
				hb.cleanupPartiallyCreatedVecs(proc)
				return err
			}
			if hb.needDupVec {
				hb.vecs[idx1][idx2], err = vec.Dup(proc.Mp())
				if err != nil {
					// Clean up partially created vecs to prevent nil pointer issues in Reset
					hb.cleanupPartiallyCreatedVecs(proc)
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
	return hb.buildHashmapInternal(hashOnPK, needAllocateSels, needUniqueVec, hb.DedupBuildKeepLast, proc)
}

func (hb *HashmapBuilder) buildHashmapInternal(hashOnPK bool, needAllocateSels bool, needUniqueVec bool, dedupBuildKeepLast bool, proc *process.Process) error {
	var err error
	if err = hb.evalJoinCondition(proc); err != nil {
		return err
	}

	var itr hashmap.Iterator
	if hb.keyWidth <= 8 {
		if hb.IntHashMap, err = hashmap.NewIntHashMap(false); err != nil {
			return err
		}
		itr = hb.IntHashMap.NewIterator()
	} else {
		if hb.StrHashMap, err = hashmap.NewStrHashMap(false); err != nil {
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

	if hb.IsDedup && (hb.OnDuplicateAction == plan.Node_IGNORE || dedupBuildKeepLast) {
		hb.IgnoreRows = &bitmap.Bitmap{}
		hb.IgnoreRows.InitWithSize(int64(hb.InputBatchRowCount))
	}

	var (
		vOld        uint64
		cardinality uint64
		newSels     []int64
		lastRows    []int64
	)
	if dedupBuildKeepLast {
		lastRows = make([]int64, hb.InputBatchRowCount+1)
		for i := range lastRows {
			lastRows[i] = -1
		}
	}

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
						if dedupBuildKeepLast {
							// Fan-out: two build-side rows share the same key (one new row
							// matched multiple old rows). Keep the later one, discard the
							// earlier so only one copy is inserted.
							if lastRows[v] >= 0 {
								hb.IgnoreRows.Add(uint64(lastRows[v]))
							}
							lastRows[v] = int64(i + k)
							continue
						}
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
					if dedupBuildKeepLast {
						lastRows[v] = int64(i + k)
					}
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

	// Handle fan-out duplicates: discarded rows that still need to delete old rows.
	if dedupBuildKeepLast && hb.IgnoreRows != nil && hb.IgnoreRows.Count() > 0 {
		if err := hb.keepDiscardedRowsForDelete(proc); err != nil {
			return err
		}
		totalRowCount := hb.Batches.RowCount()
		delCount := 0
		if hb.DelRows != nil {
			delCount = int(hb.DelRows.Count())
		}
		hb.InputBatchRowCount = totalRowCount - delCount
		hb.resetHashmapForRebuild(proc)
		if err := hb.buildHashmapInternal(hashOnPK, needAllocateSels, needUniqueVec, false, proc); err != nil {
			return err
		}
		hb.InputBatchRowCount = totalRowCount
		return nil
	}

	if hb.delColIdx != -1 {
		hb.DelRows = &bitmap.Bitmap{}
		hb.DelRows.InitWithSize(int64(cardinality))

		// Scan every build row including delete-only rows appended by
		// keepDiscardedRowsForDelete. Use Batches.RowCount() instead of
		// InputBatchRowCount so appended rows are not missed.
		delScanRowCount := hb.Batches.RowCount()
		tmpVecs := make([]*vector.Vector, 1)
		for i := 0; i < delScanRowCount; i += hashmap.UnitLimit {
			if i%(hashmap.UnitLimit*32) == 0 {
				runtime.Gosched()
			}
			n := delScanRowCount - i
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

// resetHashmapForRebuild clears hashmap state so buildHashmapInternal can be
// called again after keepDiscardedRowsForDelete has reorganised the batches.
func (hb *HashmapBuilder) resetHashmapForRebuild(proc *process.Process) {
	if hb.IntHashMap != nil {
		hb.IntHashMap.Free()
		hb.IntHashMap = nil
	}
	if hb.StrHashMap != nil {
		hb.StrHashMap.Free()
		hb.StrHashMap = nil
	}
	hb.MultiSels.Free()
	if hb.needDupVec {
		for i := range hb.vecs {
			if hb.vecs[i] != nil {
				for j := range hb.vecs[i] {
					if hb.vecs[i][j] != nil {
						hb.vecs[i][j].Free(proc.Mp())
					}
				}
			}
		}
	}
	hb.vecs = nil
	hb.delVecs = nil
	for i := range hb.UniqueJoinKeys {
		if hb.UniqueJoinKeys[i] != nil {
			hb.UniqueJoinKeys[i].Free(proc.Mp())
		}
	}
	hb.UniqueJoinKeys = nil
	hb.IgnoreRows = nil
}

// keepDiscardedRowsForDelete separates build-side rows that were discarded as
// fan-out duplicates but still carry a valid old-row reference (non-null
// dedupDeleteMarkerColIdx). Those rows are moved to the end of Batches as
// delete-only rows so the delColIdx scan can mark the corresponding probe-side
// rows for deletion without re-inserting the new row a second time.
func (hb *HashmapBuilder) keepDiscardedRowsForDelete(proc *process.Process) error {
	if hb.dedupDeleteMarkerColIdx < 0 {
		return hb.Batches.Shrink(hb.IgnoreRows, proc)
	}

	activeRows := hb.IgnoreRows.Clone()
	activeRows.Negate()
	activeCount := activeRows.Count()

	discardedWithDeletes := make([]int32, 0, hb.IgnoreRows.Count())
	itr := hb.IgnoreRows.Iterator()
	for itr.HasNext() {
		row := itr.Next()
		batIdx := row / colexec.DefaultBatchSize
		rowIdx := row % colexec.DefaultBatchSize
		markerVec := hb.Batches.Buf[batIdx].Vecs[hb.dedupDeleteMarkerColIdx]
		if !markerVec.IsNull(rowIdx) {
			discardedWithDeletes = append(discardedWithDeletes, int32(row))
		}
	}

	if len(discardedWithDeletes) == 0 {
		return hb.Batches.Shrink(hb.IgnoreRows, proc)
	}

	if len(hb.dedupDeleteKeepColIdxList) == 0 {
		hb.dedupDeleteKeepColIdxList = []int32{hb.dedupDeleteMarkerColIdx}
		if hb.delColIdx >= 0 && hb.delColIdx != hb.dedupDeleteMarkerColIdx {
			hb.dedupDeleteKeepColIdxList = append(hb.dedupDeleteKeepColIdxList, hb.delColIdx)
		}
	}

	deleteOnlyBat, err := hb.makeDeleteOnlyBatch(discardedWithDeletes, proc)
	if err != nil {
		return err
	}
	defer deleteOnlyBat.Clean(proc.Mp())

	if err := hb.Batches.Shrink(hb.IgnoreRows, proc); err != nil {
		return err
	}
	if err := hb.Batches.CopyIntoBatches(deleteOnlyBat, proc); err != nil {
		return err
	}

	hb.DelRows = &bitmap.Bitmap{}
	hb.DelRows.InitWithSize(int64(activeCount + len(discardedWithDeletes)))
	for i := range discardedWithDeletes {
		hb.DelRows.Add(uint64(activeCount + i))
	}
	return nil
}

// makeDeleteOnlyBatch builds a batch containing only the columns needed for
// deletion (dedupDeleteKeepColIdxList) from the specified discarded rows.
// All other column slots are filled with nulls to keep the batch schema
// compatible with the existing Batches layout.
func (hb *HashmapBuilder) makeDeleteOnlyBatch(rows []int32, proc *process.Process) (*batch.Batch, error) {
	keepCols := make(map[int32]struct{}, len(hb.dedupDeleteKeepColIdxList))
	for _, colIdx := range hb.dedupDeleteKeepColIdxList {
		keepCols[colIdx] = struct{}{}
	}

	bat := batch.NewOffHeapWithSize(len(hb.Batches.Buf[0].Vecs))
	bat.Attrs = hb.Batches.Buf[0].Attrs
	for colIdx, vec := range hb.Batches.Buf[0].Vecs {
		bat.Vecs[colIdx] = vector.NewOffHeapVecWithType(*vec.GetType())
	}

	cleanOnErr := true
	defer func() {
		if cleanOnErr {
			bat.Clean(proc.Mp())
		}
	}()

	for _, row := range rows {
		srcBatIdx := int(row) / colexec.DefaultBatchSize
		srcRowIdx := int64(int(row) % colexec.DefaultBatchSize)
		for colIdx, dst := range bat.Vecs {
			src := hb.Batches.Buf[srcBatIdx].Vecs[colIdx]
			if _, keep := keepCols[int32(colIdx)]; keep || src.IsNull(uint64(srcRowIdx)) {
				if err := dst.UnionOne(src, srcRowIdx, proc.Mp()); err != nil {
					return nil, err
				}
			} else if err := dst.UnionNull(proc.Mp()); err != nil {
				return nil, err
			}
		}
		bat.AddRowCount(1)
	}

	cleanOnErr = false
	return bat, nil
}
