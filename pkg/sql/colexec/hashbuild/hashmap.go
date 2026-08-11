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

package hashbuild

import (
	"math"
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/runtimefilter"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type HashmapBuilder struct {
	needDupVec bool
	// InputBatchRowCount is the physical retained row count published with the
	// JoinMap. hashMapRowCount is the number of those rows that participate in
	// the current hashmap and its auxiliary-memory projection. REPLACE may
	// append delete-only rows which belong to the first count but not the
	// second.
	InputBatchRowCount int
	hashMapRowCount    int
	hashMapRowCountSet bool
	TrackNullKeys      bool
	HasNullKey         bool
	curVecs            []*vector.Vector // evaluated key vecs for the current batch
	IntHashMap         *hashmap.IntHashMap
	StrHashMap         *hashmap.StrHashMap
	Sels               message.GroupSels
	keyWidth           int // keyWidth is the width of hash columns, it determines which hash map to use.
	Batches            colexec.Batches
	executors          []colexec.ExpressionExecutor
	UniqueJoinKeys     []*vector.Vector
	uniqueKeySlots     []bool
	uniqueSels         []int64
	cachedIntIterator  hashmap.Iterator
	cachedStrIterator  hashmap.Iterator

	IsDedup            bool
	DedupBuildKeepLast bool
	OnDuplicateAction  plan.Node_OnDuplicateAction
	DedupColName       string
	DedupColTypes      []plan.Type

	IgnoreRows *bitmap.Bitmap

	delColIdx                 int32
	dedupDeleteMarkerColIdx   int32
	dedupDeleteKeepColIdxList []int32
	DelRows                   *bitmap.Bitmap
	budget                    *process.HashBuildBudgetGeneration
	keyExprs                  []*plan.Expr
	// retainedSpillTailSelected is the logical spill materialization of the
	// one partial CopyIntoBatches tail. It avoids rescanning that growing tail.
	retainedSpillTailSelected uint64
	// Exact runtime-filter keys are an optional owner inside the mandatory
	// JoinMap build. The fallback bit is observed by HashBuild for diagnostics.
	//
	// retainedBatchRecoverySafe is an ownership contract for every caller that
	// may replay or repartition Batches after BuildHashmap returns an admission
	// error. It becomes false before a destructive Dedup rewrite starts. From
	// that point, Batches are no longer equivalent to the original ingress and
	// must not be retried or re-spilled.
	runtimeFilterCollectionFallback bool
	retainedBatchRecoverySafe       bool
	mapAllocationAccount            *mpool.AllocationAccount
	mapAllocation                   *hashtable.AllocationAccountSelection
	iteratorAllocation              *hashmap.IteratorAllocation
	batchAllocation                 *vector.AllocationAccountSelection
	uniqueKeyAllocation             *vector.AllocationAccountSelection
	recoveryCapacityClass           mpool.AllocationCapacityClass
}

func (hb *HashmapBuilder) GetSize() int64 {
	var sz int64
	if hb.IntHashMap != nil {
		sz += hb.IntHashMap.Size()
	} else if hb.StrHashMap != nil {
		sz += hb.StrHashMap.Size()
	}
	sz += hb.Sels.Size()
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

func (hb *HashmapBuilder) GetJoinMap(mp *mpool.MPool) *message.JoinMap {
	if hb.InputBatchRowCount == 0 {
		return nil
	}
	sels := hb.Sels
	hb.Sels = message.GroupSels{}
	jmDelRows := hb.DelRows
	jm := message.NewJoinMap(sels, hb.IntHashMap, hb.StrHashMap, jmDelRows, hb.Batches.Buf, mp)
	jm.SetHasNullKey(hb.HasNullKey)
	hb.IntHashMap = nil
	hb.StrHashMap = nil
	hb.DelRows = nil
	hb.Batches.Reset()
	hb.retainedSpillTailSelected = 0
	// Iterators are producer scratch and are not part of JoinMap ownership.
	hb.detachAndPruneCachedIterators()
	hb.freeIgnoreRows(mp)
	hb.uniqueSels = nil
	hb.curVecs = nil
	jm.SetMemoryRelease(func() {
		releaseDedupBitmap(jmDelRows, mp)
	})
	return jm
}

func (hb *HashmapBuilder) GetGroupCount() uint64 {
	if hb.IntHashMap != nil {
		return hb.IntHashMap.GroupCount()
	} else if hb.StrHashMap != nil {
		return hb.StrHashMap.GroupCount()
	}
	return 0
}

// observeNullKeys records the global fact that at least one build key contains
// NULL. MARK joins need this fact even when the build rows are partitioned and
// the NULL row lives in a different spill bucket from the current probe row.
func (hb *HashmapBuilder) observeNullKeys(keyVecs []*vector.Vector) {
	if !hb.TrackNullKeys || hb.HasNullKey {
		return
	}
	for _, vec := range keyVecs {
		if vec == nil {
			continue
		}
		rows := uint64(vec.Length())
		if vec.IsConstNull() {
			if vec.GetGrouping().GetBitmap().CountRange(0, rows) < vec.Length() {
				hb.HasNullKey = true
				return
			}
			continue
		}
		if vec.GetNulls().GetBitmap().AnySetNotIn(
			vec.GetGrouping().GetBitmap(), 0, rows,
		) {
			hb.HasNullKey = true
			return
		}
	}
}

func (hb *HashmapBuilder) Prepare(
	keyCols []*plan.Expr,
	delColIdx int32,
	dedupDeleteMarkerColIdx int32,
	dedupDeleteKeepColIdxList []int32,
	proc *process.Process,
) error {
	if len(hb.executors) == 0 {
		needDupVec := false
		keyWidth := 0
		for _, expr := range keyCols {
			typ := expr.Typ
			width := types.T(typ.Id).TypeLen()
			// todo : for varlena type, always go strhashmap
			if types.T(typ.Id).FixedLength() < 0 {
				width = 128
			}
			keyWidth += width
		}
		executors, err := newExpressionExecutorsWithCapacityClass(
			proc,
			keyCols,
			hb.mapAllocationAccount,
			hb.recoveryCapacityClass,
		)
		if err != nil {
			return err
		}
		hb.needDupVec = needDupVec
		hb.executors = executors
		hb.keyExprs = keyCols
		hb.keyWidth = keyWidth
		hb.InputBatchRowCount = 0
		hb.hashMapRowCount = 0
		hb.hashMapRowCountSet = false
	}

	if hb.IsDedup {
		hb.delColIdx = delColIdx
		hb.dedupDeleteMarkerColIdx = dedupDeleteMarkerColIdx
		hb.dedupDeleteKeepColIdxList = dedupDeleteKeepColIdxList
	} else {
		hb.delColIdx = -1
		hb.dedupDeleteMarkerColIdx = -1
		hb.dedupDeleteKeepColIdxList = nil
	}

	return nil
}

func (hb *HashmapBuilder) Reset(proc *process.Process, hashTableHasNotSent bool) {
	hb.detachAndPruneCachedIterators()
	if hashTableHasNotSent || hb.InputBatchRowCount == 0 {
		hb.FreeHashMapAndBatches(proc)
	}

	hb.FreeTemporaryVectors(proc)
	hb.InputBatchRowCount = 0
	hb.hashMapRowCount = 0
	hb.hashMapRowCountSet = false
	hb.HasNullKey = false
	hb.Batches.Reset()
	hb.retainedSpillTailSelected = 0
	hb.IntHashMap = nil
	hb.StrHashMap = nil
	hb.freeIgnoreRows(proc.Mp())
	hb.freeDelRows(proc.Mp())
	for i := range hb.UniqueJoinKeys {
		if hb.UniqueJoinKeys[i] != nil {
			hb.UniqueJoinKeys[i].Free(proc.Mp())
		}
	}
	hb.UniqueJoinKeys = nil
	hb.uniqueKeySlots = nil
	// Function executors retain result-vector capacity across ResetForNextQuery.
	// Destroy them here; immutable allocation selections remain installed until
	// the statement lifecycle calls ClearAllocationAccount.
	hb.FreeExecutors()
}

func (hb *HashmapBuilder) Free(proc *process.Process) {
	hb.detachAndPruneCachedIterators()
	hb.cachedIntIterator = nil
	hb.cachedStrIterator = nil
	hb.FreeHashMapAndBatches(proc)
	hb.FreeTemporaryVectors(proc)
	hb.freeIgnoreRows(proc.Mp())
	hb.freeDelRows(proc.Mp())
	hb.needDupVec = false
	hb.HasNullKey = false
	hb.Batches.Reset()
	hb.retainedSpillTailSelected = 0
	hb.IntHashMap = nil
	hb.StrHashMap = nil
	hb.FreeExecutors()
	for i := range hb.UniqueJoinKeys {
		if hb.UniqueJoinKeys[i] != nil {
			hb.UniqueJoinKeys[i].Free(proc.Mp())
		}
	}
	hb.UniqueJoinKeys = nil
	hb.uniqueKeySlots = nil
	hb.runtimeFilterCollectionFallback = false
	hb.retainedBatchRecoverySafe = false
}

func (hb *HashmapBuilder) FreeExecutors() {
	for i := range hb.executors {
		if hb.executors[i] != nil {
			hb.executors[i].Free()
		}
	}
	hb.executors = nil
	hb.keyExprs = nil
}

func (hb *HashmapBuilder) FreeTemporaryVectors(proc *process.Process) {
	if hb.needDupVec {
		for i := range hb.curVecs {
			if hb.curVecs[i] != nil {
				hb.curVecs[i].Free(proc.Mp())
			}
		}
	}
	hb.curVecs = nil
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
	hb.Sels.Free(proc.Mp())
	hb.Batches.Clean(proc.Mp())
	hb.retainedSpillTailSelected = 0
	hb.freeIgnoreRows(proc.Mp())
	hb.freeDelRows(proc.Mp())
}

// evalBatch evaluates join key expressions for one batch, storing results in hb.curVecs.
// If needDupVec, the previous curVecs are freed first.
func (hb *HashmapBuilder) evalBatch(batchIdx int, proc *process.Process) error {
	bat := hb.Batches.Buf[batchIdx]
	if hb.curVecs == nil {
		hb.curVecs = make([]*vector.Vector, len(hb.executors))
	} else if hb.needDupVec {
		for i := range hb.curVecs {
			if hb.curVecs[i] != nil {
				hb.curVecs[i].Free(proc.Mp())
				hb.curVecs[i] = nil
			}
		}
	}
	evalOne := func(idx int) error {
		vec, evalErr := hb.executors[idx].Eval(proc, []*batch.Batch{bat}, nil)
		if evalErr != nil {
			return evalErr
		}
		if hb.needDupVec {
			hb.curVecs[idx], evalErr = vec.DupOffHeap(proc.Mp())
			if evalErr != nil {
				return evalErr
			}
		} else {
			hb.curVecs[idx] = vec
		}
		return nil
	}
	var err error
	for idx := range hb.executors {
		if err = evalOne(idx); err != nil {
			break
		}
	}
	if err != nil {
		hb.abortExpressionEval(proc)
		return err
	}
	return nil
}

func (hb *HashmapBuilder) abortExpressionEval(proc *process.Process) {
	// Eval may allocate cached child/result vectors before returning an error.
	// Destroy the complete executor tree so every exact allocation is released.
	hb.FreeTemporaryVectors(proc)
	hb.FreeExecutors()
}

// hasGroupingKey reports whether a direct build key column contains any
// GROUPING sentinel in any retained batch. Such maps use the string encoder's
// explicit key domains; an IntHashMap cannot represent a sentinel outside the
// complete uint64 raw-value domain without collisions.
func (hb *HashmapBuilder) hasGroupingKey() bool {
	for _, executor := range hb.executors {
		if !executor.IsColumnExpr() {
			continue
		}
		column, ok := executor.(*colexec.ColumnExpressionExecutor)
		if !ok {
			continue
		}
		// HashBuild evaluates every key against one retained build batch.
		// Some join planners preserve the original build relation index in a
		// direct column expression, but ColumnExpressionExecutor deliberately
		// resolves that expression against the only input batch.  Inspect the
		// same physical column here; filtering on RelPos would miss GROUPING
		// sentinels for DedupJoin and RightDedupJoin.
		position := column.GetColIndex()
		for _, bat := range hb.Batches.Buf {
			if bat != nil && position >= 0 && position < len(bat.Vecs) &&
				bat.Vecs[position] != nil &&
				bat.Vecs[position].GetGrouping().GetBitmap().CountRange(
					0, uint64(bat.Vecs[position].Length()),
				) > 0 {
				return true
			}
		}
	}
	return false
}

func (hb *HashmapBuilder) hasNonReflexiveFloatKey() bool {
	for _, expr := range hb.keyExprs {
		if expr == nil {
			continue
		}
		oid := types.T(expr.Typ.Id)
		if oid == types.T_float32 || oid == types.T_float64 ||
			oid == types.T_array_float32 || oid == types.T_array_float64 ||
			oid == types.T_array_bf16 || oid == types.T_array_float16 {
			return true
		}
	}
	return false
}

func (hb *HashmapBuilder) BuildHashmap(hashOnPK bool, needAllocateSels bool, needUniqueVec bool, proc *process.Process) (retErr error) {
	hb.runtimeFilterCollectionFallback = false
	hb.retainedBatchRecoverySafe = true
	return hb.buildHashmap(hashOnPK, needAllocateSels, needUniqueVec, hb.DedupBuildKeepLast, proc)
}

func (hb *HashmapBuilder) runtimeFilterFallbackState() (bool, bool) {
	return hb.runtimeFilterCollectionFallback,
		hb.retainedBatchRecoverySafe
}

func (hb *HashmapBuilder) collectUniqueKeySlot(slot int) bool {
	return len(hb.uniqueKeySlots) == 0 ||
		(slot >= 0 && slot < len(hb.uniqueKeySlots) &&
			hb.uniqueKeySlots[slot])
}

// RetainedBatchRecoverySafe reports whether the batches retained by this
// builder are still semantically equivalent to its original ingress and may be
// replayed or repartitioned after BuildHashmap fails.
//
// Callers must read this before freeing partial hashmap state. A false result
// is sticky for the current BuildHashmap generation.
func (hb *HashmapBuilder) RetainedBatchRecoverySafe() bool {
	return hb.retainedBatchRecoverySafe
}

func (hb *HashmapBuilder) buildHashmap(
	hashOnPK bool,
	needAllocateSels bool,
	needUniqueVec bool,
	dedupBuildKeepLast bool,
	proc *process.Process,
) (retErr error) {
	runtimeFilterRequested := needUniqueVec
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	// Every ordinary build starts with all retained rows participating in the
	// map. Canonical Dedup rewrites below update this count before resizing the
	// auxiliary owner and rebuilding.
	hb.hashMapRowCount = hb.InputBatchRowCount
	hb.hashMapRowCountSet = true
	if hb.InputBatchRowCount == 0 {
		return nil
	}
	dedupBuildKeepLast = dedupBuildKeepLast && hb.IsDedup && hb.OnDuplicateAction == plan.Node_FAIL
	defer func() {
		if retErr != nil {
			hashmap.IteratorClearOwner(hb.cachedIntIterator)
			hashmap.IteratorClearOwner(hb.cachedStrIterator)
			hb.cachedIntIterator = nil
			hb.cachedStrIterator = nil
		}
	}()

	// Defensive: cached iterators must not hold owners before reuse to avoid pinning old hashmaps.
	if hb.cachedIntIterator != nil {
		hashmap.IteratorClearOwner(hb.cachedIntIterator)
	}
	if hb.cachedStrIterator != nil {
		hashmap.IteratorClearOwner(hb.cachedStrIterator)
	}

	var err error
	var itr hashmap.Iterator
	hasGroupingKey := hb.hasGroupingKey()
	rejectNaN := hb.hasNonReflexiveFloatKey()
	useIntHashMap := hb.keyWidth <= 8 && !hasGroupingKey
	if hb.mapAllocation == nil || hb.mapAllocationAccount == nil ||
		hb.iteratorAllocation == nil || hb.batchAllocation == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if useIntHashMap {
		hb.IntHashMap, err = hashmap.NewIntHashMapWithAllocation(
			false,
			proc.Mp(),
			hb.mapAllocation,
		)
		if err != nil {
			if hb.IntHashMap != nil {
				hb.IntHashMap.Free()
				hb.IntHashMap = nil
			}
			return err
		}
		if rejectNaN {
			if err = hb.IntHashMap.SetRejectNaN(); err != nil {
				return err
			}
		}
		if hb.cachedIntIterator != nil {
			hashmap.IteratorChangeOwner(hb.cachedIntIterator, hb.IntHashMap)
			itr = hb.cachedIntIterator
		} else {
			itr = hb.IntHashMap.NewIterator()
			hb.cachedIntIterator = itr
		}
	} else {
		hb.StrHashMap, err = hashmap.NewStrHashMapWithAllocations(
			false,
			proc.Mp(),
			hb.mapAllocation,
			hb.iteratorAllocation,
		)
		if err != nil {
			if hb.StrHashMap != nil {
				hb.StrHashMap.Free()
				hb.StrHashMap = nil
			}
			return err
		}
		if rejectNaN {
			if err = hb.StrHashMap.SetRejectNaN(); err != nil {
				return err
			}
		}
		if hasGroupingKey {
			if err = hb.StrHashMap.SetGroupingAware(); err != nil {
				return err
			}
		}
		if hb.cachedStrIterator != nil {
			hashmap.IteratorChangeOwner(hb.cachedStrIterator, hb.StrHashMap)
			itr = hb.cachedStrIterator
		} else {
			itr = hb.StrHashMap.NewIterator()
			hb.cachedStrIterator = itr
		}
	}

	if hashOnPK || hb.IsDedup {
		// if hash on primary key, prealloc hashmap size to the count of batch
		if useIntHashMap {
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
		err = hb.Sels.InitWithAllocation(
			hb.InputBatchRowCount,
			proc.Mp(),
			hb.mapAllocationAccount,
			HashBuildAllocationOwner,
			HashBuildAllocationSiteGroupSels,
		)
		if err != nil {
			return err
		}
	}

	if hb.IsDedup && (hb.OnDuplicateAction == plan.Node_IGNORE || dedupBuildKeepLast) {
		hb.IgnoreRows, err = hb.newDedupBitmap(
			hb.InputBatchRowCount,
			proc.Mp(),
			HashBuildAllocationSiteDedupIgnoreBitmap,
		)
		if err != nil {
			return err
		}
	}
	if hb.delColIdx != -1 && hb.DelRows == nil {
		hb.DelRows, err = hb.newDedupBitmap(
			hb.InputBatchRowCount,
			proc.Mp(),
			HashBuildAllocationSiteDedupDeleteBitmap,
		)
		if err != nil {
			return err
		}
	}

	var (
		vOld                   uint64
		cardinality            uint64
		lastBatch              = -1
		lastRows               []int64
		ignoreSurvivorRows     []int64
		ignoreSurvivorOwnsKey  []bool
		ignoreBuildGroups      []uint64
		ignoreBuildZvals       []int64
		ignoreCandidateOwnsKey []bool
		ignoreCandidateOldKey  []*vector.Vector
	)
	cleanupDedupScratch := func() {
		freeDedupSlice(hb, lastRows, proc.Mp())
		lastRows = nil
		freeDedupSlice(hb, ignoreSurvivorRows, proc.Mp())
		ignoreSurvivorRows = nil
		freeDedupSlice(hb, ignoreSurvivorOwnsKey, proc.Mp())
		ignoreSurvivorOwnsKey = nil
	}
	defer cleanupDedupScratch()
	if dedupBuildKeepLast {
		lastRows, err = makeDedupSlice[int64](
			hb,
			hb.InputBatchRowCount+1,
			proc.Mp(),
			HashBuildAllocationSiteDedupLastRows,
		)
		if err != nil {
			return err
		}
		for i := range lastRows {
			lastRows[i] = -1
		}
	}
	if hb.IsDedup && hb.OnDuplicateAction == plan.Node_IGNORE && hb.delColIdx >= 0 {
		ignoreSurvivorRows, err = makeDedupSlice[int64](
			hb,
			hb.InputBatchRowCount+1,
			proc.Mp(),
			HashBuildAllocationSiteDedupSurvivorRows,
		)
		if err != nil {
			return err
		}
		ignoreSurvivorOwnsKey, err = makeDedupSlice[bool](
			hb,
			hb.InputBatchRowCount+1,
			proc.Mp(),
			HashBuildAllocationSiteDedupSurvivorOwnsKey,
		)
		if err != nil {
			return err
		}
		ignoreBuildGroups = make([]uint64, hashmap.UnitLimit)
		ignoreBuildZvals = make([]int64, hashmap.UnitLimit)
		ignoreCandidateOwnsKey = make([]bool, hashmap.UnitLimit)
		ignoreCandidateOldKey = make([]*vector.Vector, 1)
	}

buildUnits:
	for i := 0; i < hb.InputBatchRowCount; i += hashmap.UnitLimit {
		if i%(hashmap.UnitLimit*32) == 0 {
			if err := checkHashBuildCanceled(proc); err != nil {
				return err
			}
			runtime.Gosched()
		}
		n := hb.InputBatchRowCount - i
		if n > hashmap.UnitLimit {
			n = hashmap.UnitLimit
		}

		// if not hash on primary key, estimate the hashmap size after 8192 rows
		//preAlloc to improve performance and reduce memory reAlloc
		if !hashOnPK && !hb.IsDedup && hb.InputBatchRowCount > hashmap.HashMapSizeThreshHold && i == hashmap.HashMapSizeEstimate {
			if useIntHashMap {
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
		if vecIdx1 != lastBatch {
			if err = hb.evalBatch(vecIdx1, proc); err != nil {
				return err
			}
			hb.observeNullKeys(hb.curVecs)
			lastBatch = vecIdx1
		}
		vals, zvals, err := itr.Insert(vecIdx2, n, hb.curVecs)
		if err != nil {
			return err
		}
		if ignoreSurvivorRows != nil {
			// Iterator result buffers are reused by Find, so preserve the group
			// assignment from Insert before looking up each candidate's old key.
			copy(ignoreBuildGroups[:n], vals[:n])
			copy(ignoreBuildZvals[:n], zvals[:n])
			vals = ignoreBuildGroups[:n]
			zvals = ignoreBuildZvals[:n]
			clear(ignoreCandidateOwnsKey[:n])
			ignoreCandidateOldKey[0] = hb.Batches.Buf[vecIdx1].Vecs[hb.delColIdx]
			oldVals, oldZvals, findErr := itr.Find(
				vecIdx2,
				n,
				ignoreCandidateOldKey,
			)
			if findErr != nil {
				return findErr
			}
			for k := 0; k < n; k++ {
				ignoreCandidateOwnsKey[k] = zvals[k] != 0 && oldZvals[k] != 0 && vals[k] != 0 && oldVals[k] == vals[k]
			}
		}
		for k, v := range vals[:n] {
			if hb.IsDedup && hb.OnDuplicateAction == plan.Node_UPDATE {
				group := int32(v)
				if zvals[k] == 0 || v == 0 {
					group = 0
				}
				hb.Sels.Insert(group, int32(i+k))
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
							if lastRows[v] >= 0 {
								hb.IgnoreRows.Add(uint64(lastRows[v]))
							}
							lastRows[v] = int64(i + k)
							continue
						}

						var rowStr string
						if len(hb.DedupColTypes) == 1 {
							if hb.DedupColName == catalog.IndexTableIndexColName {
								if hb.curVecs[0].GetType().Oid == types.T_varchar {
									t, _, schema, err := types.DecodeTuple(hb.curVecs[0].GetBytesAt(vecIdx2 + k))
									if err == nil && len(schema) > 1 {
										rowStr = t.ErrString(make([]int32, len(schema)))
									}
								}
							}

							if len(rowStr) == 0 {
								rowStr, err = colexec.FormatDedupKey(hb.curVecs[0], vecIdx2+k, hb.DedupColTypes)
								if err != nil {
									return err
								}
							}
						} else {
							rowStr, err = colexec.FormatDedupKey(hb.curVecs[0], vecIdx2+k, hb.DedupColTypes)
							if err != nil {
								return err
							}
						}
						return moerr.NewDuplicateEntry(proc.Ctx, rowStr, hb.DedupColName)
					case plan.Node_IGNORE:
						if ignoreSurvivorRows != nil && ignoreCandidateOwnsKey[k] && !ignoreSurvivorOwnsKey[v] {
							previousRow := ignoreSurvivorRows[v]
							if previousRow > 0 {
								hb.IgnoreRows.Add(uint64(previousRow - 1))
							}
							ignoreSurvivorRows[v] = int64(i+k) + 1
							ignoreSurvivorOwnsKey[v] = true
						} else {
							hb.IgnoreRows.Add(uint64(i + k))
						}
					}
				} else {
					cardinality = v
					if ignoreSurvivorRows != nil {
						ignoreSurvivorRows[v] = int64(i+k) + 1
						ignoreSurvivorOwnsKey[v] = ignoreCandidateOwnsKey[k]
					}
					if hb.OnDuplicateAction == plan.Node_IGNORE && needAllocateSels {
						hb.Sels.Insert(int32(v), int32(i+k))
					}
					if dedupBuildKeepLast {
						lastRows[v] = int64(i + k)
					}
				}
			} else if !hashOnPK && needAllocateSels {
				hb.Sels.Insert(int32(v-1), int32(i+k))
			}
		}

		if needUniqueVec {
			if len(hb.UniqueJoinKeys) == 0 {
				if hb.uniqueKeyAllocation == nil {
					return mpool.ErrAllocationAccountInvalid
				}
				hb.UniqueJoinKeys = make([]*vector.Vector, len(hb.executors))
				for j, vec := range hb.curVecs {
					if !hb.collectUniqueKeySlot(j) {
						continue
					}
					hb.UniqueJoinKeys[j], err = vector.NewOffHeapVecWithTypeAndAllocation(
						*vec.GetType(),
						hb.uniqueKeyAllocation,
					)
					if err != nil {
						cause := err
						if mpool.IsRetryableAllocationCapacity(err) {
							cause = runtimefilter.MarkOptionalAllocationError(err)
						}
						if fatalErr := hb.fallbackOptionalRuntimeFilterCollection(
							proc,
							cause,
						); fatalErr != nil {
							return fatalErr
						}
						needUniqueVec = false
						continue buildUnits
					}
				}
			}

			if hashOnPK {
				for j, vec := range hb.curVecs {
					if !hb.collectUniqueKeySlot(j) {
						continue
					}
					err = hb.UniqueJoinKeys[j].UnionBatch(vec, int64(vecIdx2), n, nil, proc.Mp())
					if err != nil {
						allocationErr := err
						if mpool.IsRetryableAllocationCapacity(err) {
							allocationErr = runtimefilter.MarkOptionalAllocationError(err)
						}
						if fatalErr :=
							hb.fallbackOptionalRuntimeFilterCollection(
								proc, allocationErr); fatalErr != nil {
							return fatalErr
						}
						needUniqueVec = false
						break
					}
				}
			} else {
				if hb.uniqueSels == nil {
					hb.uniqueSels = make([]int64, 0, hashmap.UnitLimit)
				}
				newSels := hb.uniqueSels[:0]
				for j, v := range vals[:n] {
					if v > vOld {
						newSels = append(newSels, int64(vecIdx2+j))
						vOld = v
					}
				}
				hb.uniqueSels = newSels

				for j, vec := range hb.curVecs {
					if !hb.collectUniqueKeySlot(j) {
						continue
					}
					err = hb.UniqueJoinKeys[j].Union(vec, newSels, proc.Mp())
					if err != nil {
						allocationErr := err
						if mpool.IsRetryableAllocationCapacity(err) {
							allocationErr = runtimefilter.MarkOptionalAllocationError(err)
						}
						if fatalErr :=
							hb.fallbackOptionalRuntimeFilterCollection(
								proc, allocationErr); fatalErr != nil {
							return fatalErr
						}
						needUniqueVec = false
						break
					}
				}
			}
		}
	}

	if dedupBuildKeepLast && hb.IgnoreRows.Count() > 0 {
		if needUniqueVec {
			if err := hb.releaseOptionalRuntimeFilterKeys(proc); err != nil {
				return err
			}
		}
		// keepDiscardedRowsForDelete rewrites Batches in place before copying
		// delete-only rows. An admission failure after that boundary cannot be
		// recovered by replaying the original BuildHashmap call.
		hb.retainedBatchRecoverySafe = false
		if err := hb.keepDiscardedRowsForDelete(proc); err != nil {
			return err
		}
		totalRowCount := hb.Batches.RowCount()
		if hb.DelRows != nil {
			hb.InputBatchRowCount = totalRowCount - hb.DelRows.Count()
		} else {
			hb.InputBatchRowCount = totalRowCount
		}
		hb.hashMapRowCount = hb.InputBatchRowCount
		cleanupDedupScratch()
		hb.resetHashStateForRebuild(proc)
		needUniqueVec, err = hb.prepareCanonicalRuntimeFilterCollection(
			runtimeFilterRequested)
		if err != nil {
			return err
		}
		if err := hb.buildHashmap(hashOnPK, needAllocateSels, needUniqueVec, false, proc); err != nil {
			return err
		}
		hb.InputBatchRowCount = totalRowCount
		return nil
	}
	if hb.IsDedup && hb.OnDuplicateAction == plan.Node_IGNORE && hb.IgnoreRows.Count() > 0 {
		if needUniqueVec {
			if err := hb.releaseOptionalRuntimeFilterKeys(proc); err != nil {
				return err
			}
		}
		hb.retainedBatchRecoverySafe = false
		// Shrinking changes physical row indexes. Rebuild before producing
		// DelRows and GroupSels so bucket-to-row mappings address the compacted
		// batches, including when a later unchanged-key owner replaced an earlier
		// representative.
		if err := hb.Batches.Shrink(hb.IgnoreRows, proc); err != nil {
			return err
		}
		hb.InputBatchRowCount = hb.Batches.RowCount()
		hb.hashMapRowCount = hb.InputBatchRowCount
		cleanupDedupScratch()
		hb.freeDelRows(proc.Mp())
		hb.resetHashStateForRebuild(proc)
		needUniqueVec, err = hb.prepareCanonicalRuntimeFilterCollection(
			runtimeFilterRequested)
		if err != nil {
			return err
		}
		return hb.buildHashmap(hashOnPK, needAllocateSels, needUniqueVec, false, proc)
	}

	if hb.delColIdx != -1 {
		if hb.DelRows == nil {
			delRows := max(cardinality, uint64(hb.Batches.RowCount()))
			if delRows > uint64(math.MaxInt) {
				return process.ErrHashBuildBudgetInvalid
			}
			hb.DelRows, err = hb.newDedupBitmap(
				int(delRows),
				proc.Mp(),
				HashBuildAllocationSiteDedupDeleteBitmap,
			)
			if err != nil {
				return err
			}
		}

		// Scan every build row, including the delete-only rows appended by
		// keepDiscardedRowsForDelete (which preserve their old-PK column). Those
		// rows are excluded from hb.InputBatchRowCount, so iterate the full batch
		// row count here; otherwise a discarded fan-out copy carrying the
		// conflicting old PK could not mark the surviving bucket as deleted
		// (issue #24428). For non-keep-last paths the two counts are equal.
		delScanRowCount := hb.Batches.RowCount()
		tmpVecs := make([]*vector.Vector, 1)
		var buildGroups []uint64
		if hb.OnDuplicateAction == plan.Node_IGNORE {
			buildGroups = make([]uint64, hashmap.UnitLimit)
		}
		for i := 0; i < delScanRowCount; i += hashmap.UnitLimit {
			if i%(hashmap.UnitLimit*32) == 0 {
				if err := checkHashBuildCanceled(proc); err != nil {
					return err
				}
				runtime.Gosched()
			}
			n := delScanRowCount - i
			if n > hashmap.UnitLimit {
				n = hashmap.UnitLimit
			}

			vecIdx1 := i / colexec.DefaultBatchSize
			vecIdx2 := i % colexec.DefaultBatchSize
			if hb.OnDuplicateAction == plan.Node_IGNORE {
				// UPDATE IGNORE is evaluated against the original keys. Only
				// exclude a candidate's own old key; treating every target row's
				// old key as released can let another candidate consume a key whose
				// owner is later ignored by a different unique constraint.
				if err = hb.evalBatch(vecIdx1, proc); err != nil {
					return err
				}
				newVals, newZvals, findErr := itr.Find(
					vecIdx2,
					n,
					hb.curVecs,
				)
				if findErr != nil {
					return findErr
				}
				for k := 0; k < n; k++ {
					buildGroups[k] = 0
					if newZvals[k] != 0 {
						buildGroups[k] = newVals[k]
					}
				}
			}
			tmpVecs[0] = hb.Batches.Buf[vecIdx1].Vecs[hb.delColIdx]
			vals, zvals, findErr := itr.Find(vecIdx2, n, tmpVecs)
			if findErr != nil {
				return findErr
			}

			for k, v := range vals[:n] {
				if zvals[k] == 0 || v == 0 {
					continue
				}
				if hb.OnDuplicateAction == plan.Node_IGNORE {
					row := uint64(i + k)
					if hb.IgnoreRows.Contains(row) || buildGroups[k] != v {
						continue
					}
				}
				hb.DelRows.Add(v - 1)
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

	return hb.Sels.Finalize(int(hb.GetGroupCount()), hb.InputBatchRowCount, proc.Mp())
}

func (hb *HashmapBuilder) resetHashStateForRebuild(proc *process.Process) {
	hb.detachAndPruneCachedIterators()
	hb.cachedIntIterator = nil
	hb.cachedStrIterator = nil
	if hb.IntHashMap != nil {
		hb.IntHashMap.Free()
		hb.IntHashMap = nil
	}
	if hb.StrHashMap != nil {
		hb.StrHashMap.Free()
		hb.StrHashMap = nil
	}
	hb.Sels.Free(proc.Mp())
	for i := range hb.UniqueJoinKeys {
		if hb.UniqueJoinKeys[i] != nil {
			hb.UniqueJoinKeys[i].Free(proc.Mp())
		}
	}
	hb.UniqueJoinKeys = nil
	hb.FreeTemporaryVectors(proc)
	for i := range hb.executors {
		if hb.executors[i] != nil {
			hb.executors[i].ResetForNextQuery()
		}
	}
	hb.freeIgnoreRows(proc.Mp())
}

// FreeHashMapOnly discards a partial hash build while preserving the copied
// build batches for bounded spill recovery. It is the only supported
// transition from a failed BuildHashmap attempt to re-spill.
func (hb *HashmapBuilder) FreeHashMapOnly(proc *process.Process) {
	hb.resetHashStateForRebuild(proc)
	hb.freeDelRows(proc.Mp())
}

func (hb *HashmapBuilder) keepDiscardedRowsForDelete(proc *process.Process) error {
	if hb.dedupDeleteMarkerColIdx < 0 {
		return hb.Batches.Shrink(hb.IgnoreRows, proc)
	}

	activeCount := int(hb.IgnoreRows.Len()) - hb.IgnoreRows.Count()

	discardedStorage, err := makeDedupSlice[int32](
		hb,
		hb.IgnoreRows.Count(),
		proc.Mp(),
		HashBuildAllocationSiteDedupDiscardedRows,
	)
	if err != nil {
		return err
	}
	defer freeDedupSlice(hb, discardedStorage, proc.Mp())
	discardedWithDeletes := discardedStorage[:0]
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
		// Also preserve the old-PK (delColIdx) column on the delete-only rows.
		// The post-shrink delColIdx pass in BuildHashmap marks the build bucket
		// whose new key equals a build row's old PK as deleted (so the probe
		// side does not raise a false DuplicateEntry for an existing row that
		// REPLACE is removing). When one new row fans out to several old rows
		// via different unique keys AND its new PK also matches an existing row
		// (issue #24428), the fan-out copy carrying that old PK loses keep-last
		// and becomes a delete-only row; keeping its old PK lets that pass still
		// mark the surviving bucket.
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
	if err := hb.copyBuildBatch(deleteOnlyBat, proc); err != nil {
		return err
	}

	newRows := activeCount + len(discardedWithDeletes)
	if hb.DelRows == nil {
		hb.DelRows, err = hb.newDedupBitmap(
			newRows,
			proc.Mp(),
			HashBuildAllocationSiteDedupDeleteBitmap,
		)
		if err != nil {
			return err
		}
	} else {
		hb.DelRows.InitWithSize(int64(newRows))
	}
	for i := range discardedWithDeletes {
		hb.DelRows.Add(uint64(activeCount + i))
	}
	return nil
}

func (hb *HashmapBuilder) makeDeleteOnlyBatch(rows []int32, proc *process.Process) (*batch.Batch, error) {
	keepCols := make(map[int32]struct{}, len(hb.dedupDeleteKeepColIdxList))
	for _, colIdx := range hb.dedupDeleteKeepColIdxList {
		keepCols[colIdx] = struct{}{}
	}

	bat := batch.NewOffHeapWithSize(len(hb.Batches.Buf[0].Vecs))
	if hb.mapAllocationAccount == nil {
		bat.Clean(proc.Mp())
		return nil, mpool.ErrAllocationAccountInvalid
	}
	selection, err := vector.NewAllocationAccountSelection(
		hb.mapAllocationAccount,
		HashBuildAllocationOwner,
		HashBuildAllocationSiteDedupDeleteOnlyData,
		HashBuildAllocationSiteDedupDeleteOnlyArea,
		HashBuildAllocationSiteDedupDeleteOnlyNulls,
		HashBuildAllocationSiteDedupDeleteOnlyGrouping,
	)
	if err != nil {
		bat.Clean(proc.Mp())
		return nil, err
	}
	if err = bat.SetAllocationAccount(selection); err != nil {
		bat.Clean(proc.Mp())
		return nil, err
	}
	bat.Attrs = hb.Batches.Buf[0].Attrs
	for colIdx, vec := range hb.Batches.Buf[0].Vecs {
		bat.SetVector(int32(colIdx), vector.NewOffHeapVecWithType(*vec.GetType()))
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

// ExtractCachedIteratorsForReuse detaches and returns cached iterators so they
// can be preserved across object pool resets without retaining old hashmaps.
// After extraction the builder no longer holds references to the iterators.
func (hb *HashmapBuilder) ExtractCachedIteratorsForReuse() (hashmap.Iterator, hashmap.Iterator) {
	hb.detachAndPruneCachedIterators()
	intItr := hb.cachedIntIterator
	strItr := hb.cachedStrIterator
	hb.cachedIntIterator = nil
	hb.cachedStrIterator = nil
	return intItr, strItr
}

// RestoreCachedIterators reattaches cached iterators (if any) after a pool
// reset so they can be reused by future builds.
func (hb *HashmapBuilder) RestoreCachedIterators(intItr, strItr hashmap.Iterator) {
	hb.cachedIntIterator = intItr
	hb.cachedStrIterator = strItr
}

// detachAndPruneCachedIterators clears iterator owners to avoid retaining old
// hashmaps and drops oversized string iterators to prevent unbounded growth
// when they handled very large strings.
func (hb *HashmapBuilder) detachAndPruneCachedIterators() {
	if hb.cachedIntIterator != nil {
		hashmap.IteratorClearOwner(hb.cachedIntIterator)
	}
	if hb.cachedStrIterator != nil {
		if hashmap.StrIteratorCapacity(hb.cachedStrIterator) > hashmap.MaxStrIteratorCapacity {
			hashmap.IteratorClearOwner(hb.cachedStrIterator)
			hb.cachedStrIterator = nil
			return
		}
		hashmap.IteratorClearOwner(hb.cachedStrIterator)
	}
}
