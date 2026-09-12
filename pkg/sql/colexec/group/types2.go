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

package group

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/list"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const (
	H0 = iota
	H8
	HStr
)

const (
	thisOperatorName = "group"
)

const (
	GroupAllocationSiteHashCell mpool.AllocationSite = iota + 24
	GroupAllocationSiteHashDescriptor
	GroupAllocationSiteHashIterator
	GroupAllocationSiteKeyData
	GroupAllocationSiteKeyArea
	GroupAllocationSiteKeyNulls
	GroupAllocationSiteKeyGrouping
	GroupAllocationSiteExpressionData
	GroupAllocationSiteExpressionArea
	GroupAllocationSiteExpressionNulls
	GroupAllocationSiteExpressionGrouping
	GroupAllocationSiteAggregateData
	GroupAllocationSiteAggregateArea
	GroupAllocationSiteAggregateNulls
	GroupAllocationSiteAggregateGrouping
	GroupAllocationSiteAggregateArgumentCount
	GroupAllocationSiteAggregateArgumentArena
	GroupAllocationSitePartialOutput
	GroupAllocationSiteSpillHashCodes
	GroupAllocationSiteSpillFlags
	GroupAllocationSiteSpillMetadata
	GroupAllocationSiteSpillRead
	GroupAllocationSiteSpillRows
	GroupAllocationSiteDistinctRecord
	GroupAllocationSiteDistinctCopy
)

var _ vm.Operator = &Group{}

// Group
// the group operator using new implement.
type Group struct {
	vm.OperatorBase
	colexec.Projection

	ctr      container
	NeedEval bool
	SpillMem int64

	// group-by column.
	GroupBy      []*plan.Expr
	GroupingFlag []bool
	// DynamicGrouping means grouping metadata is carried by the input vectors
	// instead of being described by one static GroupingFlag. This is used by a
	// shared grouping-set aggregate whose input switches grouping sets between
	// batches.
	DynamicGrouping bool
	GroupByHashKey  []int32

	Aggs []aggexec.AggFuncExecExpression

	diagnosticsLogged bool
}

type spillBucket struct {
	lv        int       // spill level
	name      string    // spill bucket name
	cnt       int64     // number of rows in this spill bucket
	file      *os.File  // spill file
	writer    io.Writer // file writer; tests may inject a failing writer
	fdToken   *process.ExecutionSpillFDReservation
	diskToken *process.ExecutionSpillDiskReservation
	path      [spillMaxPass]uint8
	pathLen   int
}

type reusableSpillBuffer interface {
	io.Writer
	Bytes() []byte
	Len() int
	Reset()
	Cap() int
	Resize(int) error
	Free()
}

type unaccountedSpillBuffer struct {
	data []byte
}

func (b *unaccountedSpillBuffer) Write(value []byte) (int, error) {
	b.data = append(b.data, value...)
	return len(value), nil
}

func (b *unaccountedSpillBuffer) Bytes() []byte { return b.data }
func (b *unaccountedSpillBuffer) Len() int      { return len(b.data) }
func (b *unaccountedSpillBuffer) Cap() int      { return cap(b.data) }
func (b *unaccountedSpillBuffer) Reset()        { b.data = b.data[:0] }
func (b *unaccountedSpillBuffer) Resize(length int) error {
	if length < 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	if cap(b.data) < length {
		next := make([]byte, length)
		copy(next, b.data)
		b.data = next
	} else {
		b.data = b.data[:length]
	}
	return nil
}
func (b *unaccountedSpillBuffer) Free() {
	b.data = nil
}

func (bkt *spillBucket) flushWriter() error {
	if bkt.writer != nil {
		var err error
		if flusher, ok := bkt.writer.(interface{ Flush() error }); ok {
			err = flusher.Flush()
		}
		if releaser, ok := bkt.writer.(interface{ Free() }); ok {
			releaser.Free()
		}
		bkt.writer = nil
		return err
	}
	return nil
}

func (bkt *spillBucket) free() error {
	if bkt == nil {
		return nil
	}
	err := bkt.flushWriter()
	if bkt.file != nil {
		if closeErr := bkt.file.Close(); err == nil {
			err = closeErr
		}
		bkt.file = nil
	}
	if bkt.fdToken != nil {
		bkt.fdToken.Release()
		bkt.fdToken = nil
	}
	if bkt.diskToken != nil {
		bkt.diskToken.Release()
		bkt.diskToken = nil
	}
	return err
}

// container running context.
type container struct {
	state  vm.CtrState
	mp     *mpool.MPool
	budget *process.ExecutionResourceGeneration

	allocationAccount    *mpool.AllocationAccount
	hashAllocation       *hashtable.AllocationAccountSelection
	hashIterator         *hashmap.IteratorAllocation
	groupByAllocation    *vector.AllocationAccountSelection
	expressionAllocation *vector.AllocationAccountSelection
	aggregateAllocation  *aggexec.AllocationAccount

	recoveryCapacity         *process.ExecutionRecoveryCapacity
	recoveryCapacityClass    mpool.AllocationCapacityClass
	recoveryCapacityActive   bool
	recoveryCapacityFloor    uint64
	spillGroupByAllocation   *vector.AllocationAccountSelection
	spillAggregateAllocation *aggexec.AllocationAccount

	inputDone    bool
	currBatchIdx int

	// hash.
	hr          ResHashRelated
	mtyp        int32
	keyWidth    int32
	keyNullable bool
	// groupingAware selects the collision-free HStr key grammar whenever a
	// grouping-set rollup sentinel can appear, including NOT NULL input keys.
	groupingAware bool

	// x, y of `group by x, y`.
	groupByEvaluate colexec.ExprEvalVector
	// m, n of `select agg1(m, n), agg2(m, n)`.
	aggArgEvaluate []colexec.ExprEvalVector

	// group by columns
	groupByTypes                 []types.Type
	groupByBatches               []*batch.Batch
	groupByStandby               *batch.Batch
	groupingRollup               []*vector.Vector
	groupByHashKey               []int32
	hashKeyVecs                  []*vector.Vector
	groupKeyStringSourceMetadata bool

	// MergeGroup locks the partial wire metadata on the first input. It must
	// survive resident spills, because later partials and queued spill records
	// still belong to that same hash-key domain.
	mergePartialMetadataSet bool

	// aggs, which holds the intermediate state of agg functions.
	aggList                []aggexec.GroupAggFuncExec
	aggExprs               []aggexec.AggFuncExecExpression
	prepareParamKind       aggexec.PrepareParamKindStates
	prepareParamKindWireV1 bool
	legacyTextMinMax       bool
	legacyVarianceState    bool

	// spill, agglist to load spilled data.
	spillMem        int64
	spillAggList    []aggexec.GroupAggFuncExec
	spillBkts       list.Deque[*spillBucket]
	currentSpillBkt []*spillBucket

	// reusable buffers for spill to avoid per-call allocations
	spillFlagFlat   []uint8           // scratch 0/1 flags for one batch's rows during spill
	spillHashCodes  []uint64          // reused buffer for AllGroupHash output
	spillBucketRows []int32           // counting-partitioned row ids, total capacity O(batch rows)
	spillReader     *groupSpillReader // reused across loadSpilledData calls
	spillGbBatch    *batch.Batch      // reused staging batch across spillDataToDisk calls

	// Largest number of groups already held by this operator before a spill.
	// A spill reload may preallocate up to this proven in-memory high-water mark,
	// but never up to an unbounded bucket row count.
	spillHashPreAllocSize uint64

	// distinctSpill survives ordinary group spill generations. It owns exact
	// COUNT(DISTINCT ...) keys after a successful prepared drain and is closed
	// only by the outer Group execution generation.
	distinctSpill                 *distinctSpillController
	distinctFinalized             bool
	distinctGroupReset            bool
	distinctDrainKeysForUT        uint64
	distinctContributionsPrepared bool
}

func (ctr *container) setAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if ctr == nil || account == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.allocationAccount != nil {
		if ctr.allocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.mp != nil {
		return mpool.ErrAllocationAccountInvariant
	}

	hashAllocation, err := hashtable.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerGroup,
		GroupAllocationSiteHashCell,
		GroupAllocationSiteHashDescriptor,
	)
	if err != nil {
		return err
	}
	hashIterator, err := hashmap.NewIteratorAllocation(
		account,
		mpool.AllocationOwnerGroup,
		GroupAllocationSiteHashIterator,
	)
	if err != nil {
		return err
	}
	groupByAllocation, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerGroup,
		GroupAllocationSiteKeyData,
		GroupAllocationSiteKeyArea,
		GroupAllocationSiteKeyNulls,
		GroupAllocationSiteKeyGrouping,
	)
	if err != nil {
		return err
	}
	expressionAllocation, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerGroup,
		GroupAllocationSiteExpressionData,
		GroupAllocationSiteExpressionArea,
		GroupAllocationSiteExpressionNulls,
		GroupAllocationSiteExpressionGrouping,
	)
	if err != nil {
		return err
	}
	aggregateAllocation, err := aggexec.NewAllocationAccount(
		account,
		mpool.AllocationOwnerGroup,
		aggexec.AllocationAccountSites{
			VectorData:     GroupAllocationSiteAggregateData,
			VectorArea:     GroupAllocationSiteAggregateArea,
			VectorNulls:    GroupAllocationSiteAggregateNulls,
			VectorGrouping: GroupAllocationSiteAggregateGrouping,
			ArgumentCount:  GroupAllocationSiteAggregateArgumentCount,
			ArgumentArena:  GroupAllocationSiteAggregateArgumentArena,
		},
	)
	if err != nil {
		return err
	}

	// The recovery class belongs to the same allocation-owner lifecycle as the
	// ordinary selections. Register its stable controller once, before Prepare;
	// Prepare only activates it for the current execution generation.
	recoveryCapacity := process.NewExecutionRecoveryCapacitySlot()
	recoveryClass, err := account.RegisterCapacityController(recoveryCapacity)
	if err != nil {
		return err
	}
	rollbackRecovery := func(cause error) error {
		_ = account.UnregisterCapacityController(recoveryClass, recoveryCapacity)
		return cause
	}
	spillGroupByAllocation, err := vector.NewAllocationAccountSelectionWithCapacityClass(
		account,
		mpool.AllocationOwnerGroup,
		GroupAllocationSiteKeyData,
		GroupAllocationSiteKeyArea,
		GroupAllocationSiteKeyNulls,
		GroupAllocationSiteKeyGrouping,
		recoveryClass,
	)
	if err != nil {
		return rollbackRecovery(err)
	}
	spillAggregateAllocation, err := aggexec.NewAllocationAccountWithCapacityClass(
		account,
		mpool.AllocationOwnerGroup,
		aggexec.AllocationAccountSites{
			VectorData:     GroupAllocationSiteAggregateData,
			VectorArea:     GroupAllocationSiteAggregateArea,
			VectorNulls:    GroupAllocationSiteAggregateNulls,
			VectorGrouping: GroupAllocationSiteAggregateGrouping,
			ArgumentCount:  GroupAllocationSiteAggregateArgumentCount,
			ArgumentArena:  GroupAllocationSiteAggregateArgumentArena,
		},
		recoveryClass,
	)
	if err != nil {
		return rollbackRecovery(err)
	}

	ctr.allocationAccount = account
	ctr.hashAllocation = hashAllocation
	ctr.hashIterator = hashIterator
	ctr.groupByAllocation = groupByAllocation
	ctr.expressionAllocation = expressionAllocation
	ctr.aggregateAllocation = aggregateAllocation
	ctr.recoveryCapacity = recoveryCapacity
	ctr.recoveryCapacityClass = recoveryClass
	ctr.spillGroupByAllocation = spillGroupByAllocation
	ctr.spillAggregateAllocation = spillAggregateAllocation
	return nil
}

func (ctr *container) installRecoveryCapacity() error {
	if ctr == nil || ctr.allocationAccount == nil || ctr.budget == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.recoveryCapacity == nil ||
		ctr.recoveryCapacityClass == mpool.AllocationCapacityClassDefault ||
		ctr.spillGroupByAllocation == nil || ctr.spillAggregateAllocation == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if ctr.recoveryCapacityActive {
		return nil
	}
	if err := ctr.recoveryCapacity.Activate(ctr.budget); err != nil {
		return err
	}
	ctr.recoveryCapacityActive = true
	return nil
}

func (ctr *container) releaseRecoveryCapacity(
	account *mpool.AllocationAccount,
) error {
	if ctr == nil || ctr.recoveryCapacity == nil || !ctr.recoveryCapacityActive {
		return nil
	}
	if account == nil || account != ctr.allocationAccount ||
		ctr.recoveryCapacityClass == mpool.AllocationCapacityClassDefault {
		return mpool.ErrAllocationAccountInvariant
	}
	if err := ctr.recoveryCapacity.Close(); err != nil {
		return err
	}
	ctr.recoveryCapacityActive = false
	ctr.recoveryCapacityFloor = 0
	return nil
}

func (ctr *container) clearRecoveryCapacity(
	account *mpool.AllocationAccount,
) error {
	if ctr == nil || ctr.recoveryCapacity == nil {
		return nil
	}
	if account == nil || account != ctr.allocationAccount ||
		ctr.recoveryCapacityClass == mpool.AllocationCapacityClassDefault {
		return mpool.ErrAllocationAccountInvariant
	}
	capacity := ctr.recoveryCapacity
	class := ctr.recoveryCapacityClass
	if ctr.recoveryCapacityActive {
		if err := capacity.Close(); err != nil {
			return err
		}
		ctr.recoveryCapacityActive = false
	}
	if err := account.UnregisterCapacityController(class, capacity); err != nil {
		return err
	}
	ctr.recoveryCapacity = nil
	ctr.recoveryCapacityClass = mpool.AllocationCapacityClassDefault
	ctr.recoveryCapacityActive = false
	ctr.recoveryCapacityFloor = 0
	ctr.spillGroupByAllocation = nil
	ctr.spillAggregateAllocation = nil
	return nil
}

// releaseFinalRecoveryCapacity narrows the recovery floor to the phase which
// can still spill or reload resident state. Once no bucket remains, release
// every recovery-class borrower and return the floor before aggregate Flush
// allocates final result vectors.
func (ctr *container) releaseFinalRecoveryCapacity() error {
	if ctr == nil || ctr.recoveryCapacity == nil {
		return nil
	}
	if ctr.currentSpillBkt != nil ||
		ctr.spillBkts != nil && ctr.spillBkts.Len() != 0 {
		return nil
	}
	ctr.freeSpillReloadStaging()
	if ctr.spillReader != nil {
		ctr.spillReader.DropReadAhead()
	}
	freeGroupScratch(ctr, ctr.spillHashCodes)
	ctr.spillHashCodes = nil
	freeGroupScratch(ctr, ctr.spillFlagFlat)
	ctr.spillFlagFlat = nil
	freeGroupScratch(ctr, ctr.spillBucketRows)
	ctr.spillBucketRows = nil
	return ctr.releaseRecoveryCapacity(ctr.allocationAccount)
}

func (ctr *container) clearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if ctr == nil || ctr.allocationAccount == nil {
		return nil
	}
	if ctr.allocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if ctr.mp != nil || !ctr.hr.IsEmpty() || len(ctr.groupByBatches) != 0 ||
		ctr.groupByStandby != nil ||
		len(ctr.aggList) != 0 || len(ctr.spillAggList) != 0 ||
		len(ctr.groupByEvaluate.Executor) != 0 || len(ctr.aggArgEvaluate) != 0 ||
		ctr.spillGbBatch != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if err := ctr.clearRecoveryCapacity(account); err != nil {
		return err
	}
	ctr.allocationAccount = nil
	ctr.hashAllocation = nil
	ctr.hashIterator = nil
	ctr.groupByAllocation = nil
	ctr.expressionAllocation = nil
	ctr.aggregateAllocation = nil
	return nil
}

func (ctr *container) isSpilling() bool {
	return len(ctr.currentSpillBkt) > 0
}

func (ctr *container) setSpillMem(m int64) {
	if m == 0 {
		// 0 means auto config.   Here the formula is made up on the fly.
		fileCacheMem := fileservice.GlobalMemoryCacheSizeHint.Load()
		mem := (int64(system.MemoryTotal()) - fileCacheMem) / int64(system.GoMaxProcs()) / 8
		// min 128MB
		if mem < common.MiB*128 {
			mem = common.MiB * 128
		}
		ctr.spillMem = mem
	} else {
		ctr.spillMem = m
	}
}

func (ctr *container) freeAggList() {
	for i := range ctr.aggList {
		if ctr.aggList[i] != nil {
			ctr.aggList[i].Free()
			ctr.aggList[i] = nil
		}
	}
	ctr.aggList = nil
}

func (ctr *container) freeSpillAggList() {
	for i := range ctr.spillAggList {
		if ctr.spillAggList[i] != nil {
			ctr.spillAggList[i].Free()
			ctr.spillAggList[i] = nil
		}
	}
	ctr.spillAggList = nil
}

func (ctr *container) freeSpillReloadStaging() {
	ctr.freeSpillAggList()
	if ctr.spillGbBatch != nil {
		ctr.spillGbBatch.Clean(ctr.mp)
		ctr.spillGbBatch = nil
	}
}

func (ctr *container) freeSpillBkts() {
	// free all spill buckets.
	if ctr.spillBkts != nil {
		ctr.spillBkts.Iter(0, func(bkt *spillBucket) bool {
			bkt.free()
			return true
		})
		ctr.spillBkts.Clear()
	}

	for _, bkt := range ctr.currentSpillBkt {
		bkt.free()
	}
	ctr.currentSpillBkt = nil
}

func (ctr *container) freeGroupByBatches() {
	for i := range ctr.groupByBatches {
		if ctr.groupByBatches[i] != nil {
			ctr.groupByBatches[i].Clean(ctr.mp)
			ctr.groupByBatches[i] = nil
		}
	}
	ctr.groupByBatches = nil
	ctr.groupKeyStringSourceMetadata = false
	if ctr.groupByStandby != nil {
		ctr.groupByStandby.Clean(ctr.mp)
		ctr.groupByStandby = nil
	}
	ctr.currBatchIdx = 0
}

func (ctr *container) freeGroupingRollups() {
	if ctr == nil {
		return
	}
	for i := range ctr.groupingRollup {
		if ctr.groupingRollup[i] != nil {
			ctr.groupingRollup[i].Free(ctr.mp)
			ctr.groupingRollup[i] = nil
		}
	}
	ctr.groupingRollup = nil
}

func (ctr *container) free() {
	// free container stuff, WTH is the Free0?
	ctr.inputDone = false
	ctr.hr.Free0()

	ctr.groupByEvaluate.Free()

	for i := range ctr.aggArgEvaluate {
		ctr.aggArgEvaluate[i].Free()
	}
	ctr.aggArgEvaluate = nil

	ctr.freeGroupByBatches()
	ctr.freeGroupingRollups()
	ctr.freeAggList()
	ctr.prepareParamKind.Reset(nil)
	ctr.aggExprs = nil
	ctr.prepareParamKindWireV1 = false
	ctr.freeSpillReloadStaging()
	ctr.freeSpillBkts()
	if ctr.distinctSpill != nil {
		ctr.distinctSpill.close()
		ctr.distinctSpill = nil
	}
	ctr.distinctFinalized = false
	ctr.distinctGroupReset = false
	ctr.distinctDrainKeysForUT = 0
	ctr.distinctContributionsPrepared = false
	if ctr.spillReader != nil {
		ctr.spillReader.Free()
		ctr.spillReader = nil
	}
	freeGroupScratch(ctr, ctr.spillHashCodes)
	ctr.spillHashCodes = nil
	freeGroupScratch(ctr, ctr.spillFlagFlat)
	ctr.spillFlagFlat = nil
	freeGroupScratch(ctr, ctr.spillBucketRows)
	ctr.spillBucketRows = nil
	ctr.spillHashPreAllocSize = 0
	ctr.groupByHashKey = nil
	ctr.hashKeyVecs = nil
	ctr.mergePartialMetadataSet = false
	ctr.budget = nil

	mpool.DeleteMPool(ctr.mp)
	ctr.mp = nil
}

func (ctr *container) reset() {
	ctr.free()
}

func (ctr *container) resetForSpill() {
	if ctr.distinctSpill != nil {
		ctr.distinctGroupReset = true
	}
	// Reset also frees the hash related stuff.
	ctr.hr.Free0()

	ctr.groupByEvaluate.ResetForNextQuery()

	for i := range ctr.aggArgEvaluate {
		ctr.aggArgEvaluate[i].ResetForNextQuery()
	}
	// Grouping-set sentinel vectors are owned outside the expression
	// executors. Release the just-consumed input's sentinels before recovery
	// reload borrows the same account capacity.
	ctr.freeGroupingRollups()
	// free group by batches, agg list and spill buckets, do not reuse for now.
	ctr.freeGroupByBatches()
	ctr.currBatchIdx = 0

	ctr.freeAggList()
	ctr.freeSpillAggList()
}

func (ctr *container) setGroupByHashKey(hashKey []int32) {
	ctr.groupByHashKey = hashKey
	ctr.hashKeyVecs = ctr.hashKeyVecs[:0]
}

func (ctr *container) validateGroupByHashKey(groupByCount int) error {
	if len(ctr.groupByHashKey) == 0 {
		return nil
	}
	if len(ctr.groupByHashKey) >= groupByCount {
		return moerr.NewInternalErrorNoCtx("group-by hash key must be a strict subset of group-by columns")
	}
	previous := int32(-1)
	for _, idx := range ctr.groupByHashKey {
		if idx <= previous || idx < 0 || int(idx) >= groupByCount {
			return moerr.NewInternalErrorNoCtxf("invalid group-by hash key index %d", idx)
		}
		previous = idx
	}
	return nil
}

func (ctr *container) hashKeyVectors(vs []*vector.Vector) []*vector.Vector {
	if len(ctr.groupByHashKey) == 0 {
		return vs
	}
	if cap(ctr.hashKeyVecs) < len(ctr.groupByHashKey) {
		ctr.hashKeyVecs = make([]*vector.Vector, len(ctr.groupByHashKey))
	} else {
		ctr.hashKeyVecs = ctr.hashKeyVecs[:len(ctr.groupByHashKey)]
	}
	for i, idx := range ctr.groupByHashKey {
		ctr.hashKeyVecs[i] = vs[idx]
	}
	return ctr.hashKeyVecs
}

func (group *Group) evaluateGroupByAndAggArgs(proc *process.Process, bat *batch.Batch) (err error) {
	input := []*batch.Batch{bat}
	group.ctr.freeGroupingRollups()

	// group.
	for i := range group.ctr.groupByEvaluate.Vec {
		if group.ctr.groupByEvaluate.Vec[i], err =
			group.ctr.groupByEvaluate.Executor[i].Eval(proc, input, nil); err != nil {
			return err
		}
	}

	// agg args.
	for i := range group.ctr.aggArgEvaluate {
		for j := range group.ctr.aggArgEvaluate[i].Vec {
			if group.ctr.aggArgEvaluate[i].Vec[j], err =
				group.ctr.aggArgEvaluate[i].Executor[j].Eval(proc, input, nil); err != nil {
				return err
			}
		}
	}

	// grouping flag
	for i, flag := range group.GroupingFlag {
		if !flag {
			rollup, err := vector.NewRollupConstWithAllocation(
				group.ctr.groupByEvaluate.Typ[i],
				group.ctr.groupByEvaluate.Vec[i].Length(),
				group.ctr.mp,
				group.ctr.expressionAllocation,
			)
			if err != nil {
				group.ctr.freeGroupingRollups()
				return err
			}
			if group.ctr.groupingRollup == nil {
				group.ctr.groupingRollup = make([]*vector.Vector, len(group.GroupingFlag))
			}
			group.ctr.groupingRollup[i] = rollup
			group.ctr.groupByEvaluate.Vec[i] = rollup
		}
	}

	return nil
}

func (group *Group) AnyDistinctAgg() bool {
	for _, agg := range group.Aggs {
		if agg.IsDistinct() {
			return true
		}
	}
	return false
}

func (group *Group) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if group.ProjectList == nil {
		return input, nil
	}
	return group.EvalProjection(input, proc)
}

func (group *Group) Free(proc *process.Process, pipelineFailed bool, err error) {
	group.logDiagnostics(proc, pipelineFailed, err)
	group.ctr.free()
	// free projection stuff,
	group.FreeProjection(proc)
}

func (group *Group) Reset(proc *process.Process, pipelineFailed bool, err error) {
	group.logDiagnostics(proc, pipelineFailed, err)
	group.ctr.reset()
	if group.ctr.allocationAccount != nil {
		// Account selections are immutable per execution attempt, and function
		// projections retain result capacity across ResetForNextQuery. Destroy
		// them before terminal account closure; Prepare rebuilds them for reuse.
		group.FreeProjection(proc)
	} else {
		group.ResetProjection(proc)
	}
}

func (group *Group) logDiagnostics(proc *process.Process, pipelineFailed bool, err error) {
	if group.diagnosticsLogged {
		return
	}
	group.diagnosticsLogged = true
	if proc == nil || group.OpAnalyzer == nil {
		return
	}
	extra := group.OpAnalyzer.GetOpStats().ExtraStats
	if extra["GroupSpillWriteCalls"] == 0 && extra["GroupSpillReloadBuckets"] == 0 {
		return
	}
	logutil.Info("operator diagnostic summary",
		trace.ContextField(proc.Ctx),
		zap.String("query_id", proc.QueryId()),
		zap.String("operator", thisOperatorName),
		zap.Int("node_idx", group.GetIdx()),
		zap.Bool("pipeline_failed", pipelineFailed),
		zap.Error(err),
		zap.Any("extra_stats", extra))
}

func (group *Group) OpType() vm.OpType {
	return vm.Group
}

func (group Group) TypeName() string {
	return thisOperatorName
}

func (group *Group) GetOperatorBase() *vm.OperatorBase {
	return &group.OperatorBase
}

// SetAllocationAccount attaches all data-scaled Group storage to the current
// statement attempt before Prepare performs the first physical allocation.
func (group *Group) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if group == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return group.ctr.setAllocationAccount(account)
}

func (group *Group) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if group == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return group.ctr.clearAllocationAccount(account)
}

func NewArgument() *Group {
	return reuse.Alloc[Group](nil)
}

func (group *Group) Release() {
	if group != nil {
		reuse.Free(group, nil)
	}
}

func (group *Group) String(buf *bytes.Buffer) {
	buf.WriteString(thisOperatorName + ": group([")
	for i, expr := range group.GroupBy {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v", expr))
	}
	buf.WriteString("], [")

	for i, ag := range group.Aggs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%v(%v)", function.GetAggFunctionNameByID(ag.GetAggID()), ag.GetArgExpressions()))
	}
	buf.WriteString("])")
}

const (
	mergeGroupOperatorName = "merge_group"
)

type MergeGroup struct {
	vm.OperatorBase
	colexec.Projection

	ctr      container
	SpillMem int64

	Aggs []aggexec.AggFuncExecExpression

	GroupByHashKey []int32
	// GroupingAware is fixed by the producer plan, not inferred from one
	// partial's data. Dynamic grouping streams can legally alternate partials
	// with and without an actual rollup sentinel.
	GroupingAware bool
	// EmptyGroupingSet declares that a legacy/static all-rolled branch must
	// produce one row even when no partial reaches this final merge boundary.
	EmptyGroupingSet bool
	// EmptyGroupingSetIDs declares the corresponding dynamic grouping-set rows.
	EmptyGroupingSetIDs []int64
	// GroupByTypes makes those rows constructible when no partial supplies
	// runtime vector types.
	GroupByTypes []types.Type

	PartialResults     []any
	PartialResultTypes []types.T
}

func (mergeGroup *MergeGroup) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	if mergeGroup.ProjectList == nil {
		return input, nil
	}
	return mergeGroup.EvalProjection(input, proc)
}

func (mergeGroup *MergeGroup) Reset(proc *process.Process, _ bool, _ error) {
	mergeGroup.ctr.reset()
	if mergeGroup.ctr.allocationAccount != nil {
		mergeGroup.FreeProjection(proc)
	} else {
		mergeGroup.ResetProjection(proc)
	}
}

func (mergeGroup *MergeGroup) Free(proc *process.Process, _ bool, _ error) {
	mergeGroup.ctr.free()
	mergeGroup.FreeProjection(proc)
}

func (mergeGroup *MergeGroup) GetOperatorBase() *vm.OperatorBase {
	return &mergeGroup.OperatorBase
}

func (mergeGroup *MergeGroup) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if mergeGroup == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return mergeGroup.ctr.setAllocationAccount(account)
}

func (mergeGroup *MergeGroup) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if mergeGroup == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return mergeGroup.ctr.clearAllocationAccount(account)
}

func (mergeGroup *MergeGroup) OpType() vm.OpType {
	return vm.MergeGroup
}

func (mergeGroup MergeGroup) TypeName() string {
	return mergeGroupOperatorName
}

func (mergeGroup *MergeGroup) String(buf *bytes.Buffer) {
	buf.WriteString(mergeGroupOperatorName)
}

func NewArgumentMergeGroup() *MergeGroup {
	return reuse.Alloc[MergeGroup](nil)
}

func (mergeGroup *MergeGroup) Release() {
	if mergeGroup != nil {
		reuse.Free(mergeGroup, nil)
	}
}

func init() {
	reuse.CreatePool(
		func() *Group {
			return &Group{}
		},
		func(a *Group) {
			*a = Group{}
		},
		reuse.DefaultOptions[Group]().
			WithEnableChecker(),
	)

	reuse.CreatePool(
		func() *MergeGroup {
			return &MergeGroup{}
		},
		func(a *MergeGroup) {
			*a = MergeGroup{}
		},
		reuse.DefaultOptions[MergeGroup]().
			WithEnableChecker(),
	)
}
