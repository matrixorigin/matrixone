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
	"strings"

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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
	mapReservation            *hashMapReservationOwner
	batchReservations         []*process.HashBuildReservation
	auxReservation            *process.HashBuildReservation
	keyExprs                  []*plan.Expr
	expressionLease           *ExpressionMemoryLease
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
	batchAllocation                 *vector.AllocationAccountSelection
	uniqueKeyAllocation             *vector.AllocationAccountSelection
	expressionAllocation            *colexec.ExpressionAllocationAccount
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
	// Iterators are producer scratch and are not part of JoinMap ownership.
	// Drop budgeted cached backing before transferring the encompassing aux
	// reservation to a consumer that may free it immediately after publication.
	hb.detachAndPruneCachedIterators()
	hb.freeIgnoreRows(mp)
	hb.uniqueSels = nil
	hb.curVecs = nil
	release := hb.detachReservations()
	jm.SetMemoryRelease(func() {
		releaseDedupBitmap(jmDelRows, mp)
		release()
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
		if vec.HasNull() {
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
		var (
			executors       []colexec.ExpressionExecutor
			expressionLease *ExpressionMemoryLease
			err             error
		)
		if hb.expressionAllocation != nil &&
			expressionSetAllocationClosed(keyCols) {
			executors, err = NewAllocationAccountedExpressionExecutors(
				proc,
				keyCols,
				hb.expressionAllocation,
			)
		} else {
			executors, expressionLease, err = NewBudgetedExpressionExecutors(
				proc,
				hb.budget,
				keyCols,
				needDupVec,
			)
		}
		if err != nil {
			return err
		}
		hb.needDupVec = needDupVec
		hb.executors = executors
		hb.keyExprs = keyCols
		hb.expressionLease = expressionLease
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
	// Free them before releasing expression reservations; Prepare recreates the
	// executor set for the next generation.
	hb.FreeExecutors()
	hb.mapAllocationAccount = nil
	hb.mapAllocation = nil
	hb.batchAllocation = nil
	hb.uniqueKeyAllocation = nil
	hb.expressionAllocation = nil
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
	hb.mapAllocationAccount = nil
	hb.mapAllocation = nil
	hb.batchAllocation = nil
	hb.uniqueKeyAllocation = nil
	hb.expressionAllocation = nil
}

func (hb *HashmapBuilder) FreeExecutors() {
	for i := range hb.executors {
		if hb.executors[i] != nil {
			hb.executors[i].Free()
		}
	}
	hb.executors = nil
	hb.keyExprs = nil
	hb.releaseExpressionLease()
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
	hb.freeIgnoreRows(proc.Mp())
	hb.freeDelRows(proc.Mp())
	hb.releaseReservations()
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
	if hb.expressionLease != nil {
		err = hb.expressionLease.Run(proc, bat.RowCount(), evalOne)
	} else {
		for idx := range hb.executors {
			if err = evalOne(idx); err != nil {
				break
			}
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
	// Destroy the complete executor tree before releasing its retained lease.
	hb.FreeTemporaryVectors(proc)
	hb.FreeExecutors()
}

// expressionVectorPeak is an execution-before-allocation upper bound based on
// the SQL result type. Varlena widths use the declared maximum (or the engine
// maximum when absent), so input-dependent expanding functions are rejected
// by admission before Eval instead of allocating first.
func expressionVectorPeak(proc *process.Process, expr *plan.Expr, rows int, duplicate bool) (uint64, error) {
	if expr == nil || rows < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	total, root, err := expressionTreePeak(proc, expr, uint64(rows))
	if err != nil {
		return 0, err
	}
	if duplicate {
		if total > math.MaxUint64-root {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		total += root
	}
	return total, nil
}

// ExpressionVectorPeak exposes the same execution-before-allocation bound used
// by HashmapBuilder to spill/re-spill callers. Expression evaluators cache
// intermediate and result vectors, so callers must keep the returned amount
// reserved until the corresponding executor tree is freed or evaluated again
// under a replacement reservation.
func ExpressionVectorPeak(proc *process.Process, expr *plan.Expr, rows int, duplicate bool) (uint64, error) {
	return expressionVectorPeak(proc, expr, rows, duplicate)
}

func expressionTreePeak(proc *process.Process, expr *plan.Expr, rows uint64) (total uint64, output uint64, err error) {
	return expressionTreePeakWithSelection(proc, expr, rows, false)
}

func expressionTreePeakWithSelection(
	proc *process.Process,
	expr *plan.Expr,
	rows uint64,
	mayReceivePartialSelection bool,
) (total uint64, output uint64, err error) {
	if expr == nil {
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	switch node := expr.Expr.(type) {
	case *plan.Expr_Col:
		return 0, 0, nil
	case *plan.Expr_F:
		if node.F == nil {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		var fid int32 = -1
		if node.F.Func != nil {
			fid, _ = function.DecodeOverloadID(node.F.Func.Obj)
		}
		for i, arg := range node.F.Args {
			childMayReceivePartialSelection := mayReceivePartialSelection
			switch fid {
			case function.IFF:
				// IFF evaluates only its value branches through generated
				// selection masks. Its condition inherits the caller mask.
				childMayReceivePartialSelection = mayReceivePartialSelection || i > 0
			case function.CASE, function.COALESCE:
				childMayReceivePartialSelection = true
			}
			child, _, childErr := expressionTreePeakWithSelection(
				proc,
				arg,
				rows,
				childMayReceivePartialSelection,
			)
			if childErr != nil || total > math.MaxUint64-child {
				return 0, 0, process.ErrHashBuildBudgetInvalid
			}
			total += child
		}
	case *plan.Expr_P:
		if node.P == nil || proc == nil || proc.GetPrepareParams() == nil {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		paramPeak, paramErr := expressionParamPeak(proc, node.P.Pos)
		if paramErr != nil {
			return 0, 0, paramErr
		}
		typePeak, typeErr := expressionTypePeak(expr.Typ, 1)
		if typeErr != nil {
			return 0, 0, typeErr
		}
		if paramPeak > typePeak {
			output = paramPeak
		} else {
			output = typePeak
		}
		return output, output, nil
	case *plan.Expr_Lit, *plan.Expr_V, *plan.Expr_Raw, *plan.Expr_Vec, *plan.Expr_Fold, *plan.Expr_T:
		// These executors may materialize a vector but have no child expression
		// tree. Expr_T is the target-type argument used by CAST/bit_cast and is
		// evaluated as a fixed vector. Charge their declared output below.
	default:
		// Window, subquery, correlated, list and max nodes do not
		// expose a bounded vector-evaluator tree here.
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	output, err = expressionTypePeak(expr.Typ, rows)
	if err != nil || total > math.MaxUint64-output {
		return 0, 0, process.ErrHashBuildBudgetInvalid
	}
	total += output

	if _, isFunction := expr.Expr.(*plan.Expr_F); mayReceivePartialSelection && isFunction {
		// A partially selected function retains both its ordinary full-row
		// result and a selected-result scratch vector. Row-aligned column and
		// non-folded function parameters are also copied into retained selected
		// parameter vectors before the function executes.
		if total > math.MaxUint64-output {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		total += output
		for _, arg := range nodeFunctionArgs(expr) {
			switch arg.Expr.(type) {
			case *plan.Expr_Col, *plan.Expr_F:
				selectedParameter, selectedErr := expressionTypePeak(arg.Typ, rows)
				if selectedErr != nil || total > math.MaxUint64-selectedParameter {
					return 0, 0, process.ErrHashBuildBudgetInvalid
				}
				total += selectedParameter
			}
		}
	}
	return total, output, nil
}

func nodeFunctionArgs(expr *plan.Expr) []*plan.Expr {
	if node, ok := expr.Expr.(*plan.Expr_F); ok && node.F != nil {
		return node.F.Args
	}
	return nil
}

// expressionParamPeak returns an upper bound for the allocations made by a
// non-null ParamExpressionExecutor. Params are materialized as one-element
// const vectors, whose data is one varlena header and whose area is allocated
// only for payloads that do not fit in that header.
func expressionParamPeak(proc *process.Process, pos int32) (uint64, error) {
	val, err := proc.GetPrepareParamsAt(int(pos))
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}

	headerCap, ok := mpool.GrowCapacity(0, int64(types.VarlenaSize))
	if !ok {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	peak := uint64(headerCap)
	if len(val) <= types.VarlenaInlineSize {
		return peak, nil
	}

	areaCap, ok := mpool.GrowCapacity(0, int64(len(val)))
	if !ok || areaCap < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if uint64(areaCap) > math.MaxUint64-peak {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return peak + uint64(areaCap), nil
}

func expressionTypePeak(typ plan.Type, rows uint64) (uint64, error) {
	oid := types.T(typ.Id)
	width := int64(oid.FixedLength())
	if width < 0 {
		width = int64(typ.Width)
		hardMax := int64(types.MaxVarcharLen)
		if oid.IsArrayRelate() {
			elementWidth := int64(oid.ToType().GetArrayElementSize())
			width *= elementWidth
			hardMax = int64(types.MaxArrayDimension) * elementWidth
		} else {
			switch oid {
			case types.T_blob, types.T_text, types.T_json, types.T_datalink,
				types.T_geometry, types.T_geometry32:
				hardMax = int64(types.MaxBlobLen)
			}
		}
		if width > hardMax {
			// Never clamp a declared bound downward. Array width is declared
			// in elements, while every other varlena width is in bytes.
			hardMax = width
		}
		width = hardMax
	}
	if width < 1 {
		width = 1
	}
	perRow := uint64(width) + 32
	if rows > (math.MaxUint64-(64<<10))/perRow {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return rows*perRow + (64 << 10), nil
}

func (hb *HashmapBuilder) releaseExpressionLease() {
	if hb.expressionLease != nil {
		hb.expressionLease.Release()
		hb.expressionLease = nil
	}
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
	if err := hb.reserveBuildAux(needUniqueVec, needAllocateSels); err != nil {
		if !needUniqueVec {
			return err
		}
		if runtimefilter.ClassifyOptionalFallback(err) !=
			runtimefilter.OptionalFallbackBudgetAdmission {
			return err
		}
		// The extra auxiliary charge exists only for optional exact-filter key
		// retention. Retry the admission in place without that owner before
		// allocating or mutating the mandatory map.
		needUniqueVec = false
		if err = hb.reserveBuildAux(false, needAllocateSels); err != nil {
			return err
		}
		// Linearize the fallback only after mandatory admission succeeds. A
		// failed retry is a fatal build, not a successful optional downgrade.
		hb.runtimeFilterCollectionFallback = true
	}
	dedupBuildKeepLast = dedupBuildKeepLast && hb.IsDedup && hb.OnDuplicateAction == plan.Node_FAIL
	defer func() {
		if retErr != nil {
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
	if hb.keyWidth <= 8 {
		if hb.mapAllocation == nil {
			if err = hb.reserveInitialMap(int64(hashtable.Int64HashMapInitialAllocationBytes())); err != nil {
				return err
			}
			hb.IntHashMap, err = hashmap.NewIntHashMap(false, proc.Mp())
			if err == nil {
				err = hb.attachIntHashMapAdmission(hb.IntHashMap)
			}
		} else {
			hb.IntHashMap, err = hashmap.NewIntHashMapWithAllocation(
				false,
				proc.Mp(),
				hb.mapAllocation,
			)
		}
		if err != nil {
			if hb.IntHashMap != nil {
				hb.IntHashMap.Free()
				hb.IntHashMap = nil
			}
			hb.releaseMapReservation()
			return err
		}
		if hb.cachedIntIterator != nil {
			hashmap.IteratorChangeOwner(hb.cachedIntIterator, hb.IntHashMap)
			itr = hb.cachedIntIterator
		} else {
			itr = hb.IntHashMap.NewIterator()
			hb.cachedIntIterator = itr
		}
	} else {
		if hb.mapAllocation == nil {
			if err = hb.reserveInitialMap(int64(hashtable.StringHashMapInitialAllocationBytes())); err != nil {
				return err
			}
			hb.StrHashMap, err = hashmap.NewStrHashMap(false, proc.Mp())
			if err == nil {
				err = hb.attachStrHashMapAdmission(hb.StrHashMap)
			}
		} else {
			hb.StrHashMap, err = hashmap.NewStrHashMapWithAllocation(
				false,
				proc.Mp(),
				hb.mapAllocation,
			)
		}
		if err != nil {
			if hb.StrHashMap != nil {
				hb.StrHashMap.Free()
				hb.StrHashMap = nil
			}
			hb.releaseMapReservation()
			return err
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
		var err error
		if hb.batchAllocation == nil {
			err = hb.Sels.Init(hb.InputBatchRowCount, proc.Mp())
		} else {
			err = hb.Sels.InitWithAllocation(
				hb.InputBatchRowCount,
				proc.Mp(),
				hb.mapAllocationAccount,
				HashBuildAllocationOwner,
				HashBuildAllocationSiteGroupSels,
			)
		}
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
			oldVals, oldZvals := itr.Find(vecIdx2, n, ignoreCandidateOldKey)
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
								rowStr = hb.curVecs[0].RowToString(vecIdx2 + k)
							}
						} else {
							rowItems, err := types.StringifyTuple(hb.curVecs[0].GetBytesAt(vecIdx2+k), hb.DedupColTypes)
							if err != nil {
								return err
							}
							rowStr = "(" + strings.Join(rowItems, ",") + ")"
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
				hb.UniqueJoinKeys = make([]*vector.Vector, len(hb.executors))
				for j, vec := range hb.curVecs {
					if !hb.collectUniqueKeySlot(j) {
						continue
					}
					if hb.uniqueKeyAllocation == nil {
						hb.UniqueJoinKeys[j] = vector.NewOffHeapVecWithType(*vec.GetType())
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
					areaBytes, reserveErr :=
						unionBatchAreaBytes(vec, vecIdx2, n)
					if reserveErr != nil {
						// Range and overflow failures contradict the collection
						// oracle; they are never optional allocation failures.
						return reserveErr
					}
					overlap, reserveErr := hb.reserveUniqueAppendOverlap(hb.UniqueJoinKeys[j], n, areaBytes)
					if reserveErr != nil {
						if fatalErr :=
							hb.fallbackOptionalRuntimeFilterCollection(
								proc, reserveErr); fatalErr != nil {
							return fatalErr
						}
						needUniqueVec = false
						break
					}
					err = hb.UniqueJoinKeys[j].UnionBatch(vec, int64(vecIdx2), n, nil, proc.Mp())
					if overlap != nil {
						overlap.Release()
					}
					if err != nil {
						// With the range and capacity oracle above satisfied,
						// UnionBatch error returns are only mpool growth failures.
						allocationErr :=
							runtimefilter.MarkOptionalAllocationError(err)
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
					areaBytes, reserveErr := uniqueAppendAreaBytes(vec, 0, len(newSels), newSels)
					if reserveErr != nil {
						// Selector/range/overflow failures are collection
						// contract errors and remain fatal.
						return reserveErr
					}
					overlap, reserveErr := hb.reserveUniqueAppendOverlap(hb.UniqueJoinKeys[j], len(newSels), areaBytes)
					if reserveErr != nil {
						if fatalErr :=
							hb.fallbackOptionalRuntimeFilterCollection(
								proc, reserveErr); fatalErr != nil {
							return fatalErr
						}
						needUniqueVec = false
						break
					}
					err = hb.UniqueJoinKeys[j].Union(vec, newSels, proc.Mp())
					if overlap != nil {
						overlap.Release()
					}
					if err != nil {
						// With generated selectors and the capacity oracle above
						// satisfied, Union error returns are mpool growth failures.
						allocationErr :=
							runtimefilter.MarkOptionalAllocationError(err)
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
				newVals, newZvals := itr.Find(vecIdx2, n, hb.curVecs)
				for k := 0; k < n; k++ {
					buildGroups[k] = 0
					if newZvals[k] != 0 {
						buildGroups[k] = newVals[k]
					}
				}
			}
			tmpVecs[0] = hb.Batches.Buf[vecIdx1].Vecs[hb.delColIdx]
			vals, zvals := itr.Find(vecIdx2, n, tmpVecs)

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
	hb.releaseMapReservation()
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
// build batches and their reservations. It is the supported transition from a
// failed BuildHashmap attempt to either a less memory-intensive rebuild or
// bounded spill recovery.
func (hb *HashmapBuilder) FreeHashMapOnly(proc *process.Process) {
	hb.resetHashStateForRebuild(proc)
	hb.freeDelRows(proc.Mp())
	if hb.auxReservation != nil {
		hb.auxReservation.Release()
		hb.auxReservation = nil
	}
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
	if hb.mapAllocationAccount != nil {
		selection, err := vector.NewAllocationAccountSelectionWithBitmaps(
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
			hb.cachedStrIterator = nil
			return
		}
		hashmap.IteratorClearOwner(hb.cachedStrIterator)
	}
	if hb.budget != nil {
		// Budgeted builds charge iterator scratch only for the execution that
		// allocated it. Do not retain Go backing arrays in the pooled operator
		// after that reservation is released or transferred.
		hb.cachedIntIterator = nil
		hb.cachedStrIterator = nil
	}
}
