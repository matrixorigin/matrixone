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
	"context"
	"fmt"
	"io"
	"os"
	"time"

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
	_ // reserved: was GroupAllocationSiteKeyNulls
	_ // reserved: was GroupAllocationSiteKeyGrouping
	GroupAllocationSiteExpressionData
	GroupAllocationSiteExpressionArea
	_ // reserved: was GroupAllocationSiteExpressionNulls
	_ // reserved: was GroupAllocationSiteExpressionGrouping
	GroupAllocationSiteAggregateData
	GroupAllocationSiteAggregateArea
	_ // reserved: was GroupAllocationSiteAggregateNulls
	_ // reserved: was GroupAllocationSiteAggregateGrouping
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
	GroupAllocationSiteCompactLookup
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
	// SortRollup selects the single-stage ordered ROLLUP executor. Its input
	// must already be sorted by the grouping equality keys.
	SortRollup bool

	Aggs []aggexec.AggFuncExecExpression

	diagnosticsLogged bool
}

type spillBucket struct {
	lv        int                            // spill level
	name      string                         // spill bucket name
	cnt       int64                          // number of rows in this spill bucket
	file      *os.File                       // spill file
	writer    io.Writer                      // file writer; tests may inject a failing writer
	fileFS    fileservice.MutableFileService // owns the named file until free
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

func (bkt *spillBucket) closeFile() error {
	var err error
	if bkt.file != nil {
		err = bkt.file.Close()
		bkt.file = nil
	}
	if bkt.fdToken != nil {
		bkt.fdToken.Release()
		bkt.fdToken = nil
	}
	return err
}

// A queued bucket owns disk bytes, not an open descriptor. Retaining every
// waiting sibling's FD can prevent the active bucket from being repartitioned.
func (bkt *spillBucket) finishWriting() error {
	if err := bkt.flushWriter(); err != nil {
		return err
	}
	if bkt.fileFS != nil {
		return bkt.closeFile()
	}
	return nil // Externally supplied anonymous files cannot be reopened.
}

func (bkt *spillBucket) openReader(ctx context.Context, budget *process.ExecutionResourceGeneration) error {
	if bkt.file != nil {
		return nil
	}
	if bkt.fileFS == nil {
		return moerr.NewInternalErrorNoCtx("group spill bucket has no file")
	}
	var err error
	if budget != nil {
		bkt.fdToken, err = budget.ReserveSpillFD(1)
		if err != nil {
			return err
		}
	}
	bkt.file, err = bkt.fileFS.OpenFile(ctx, bkt.name)
	if err != nil {
		_ = bkt.closeFile()
	}
	return err
}

func (bkt *spillBucket) free() error {
	if bkt == nil {
		return nil
	}
	err := bkt.flushWriter()
	if closeErr := bkt.closeFile(); err == nil {
		err = closeErr
	}
	if bkt.fileFS != nil {
		// Cancellation must not prevent deletion of a bucket owned by the query.
		if removeErr := bkt.fileFS.RemoveFile(context.Background(), bkt.name); err == nil {
			err = removeErr
		}
		bkt.fileFS = nil
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

	allocationAccount *mpool.AllocationAccount
	hashAllocation    *hashtable.AllocationAccountSelection
	hashIterator      *hashmap.IteratorAllocation
	// groupByAllocation, rollupAllocation and spillGroupByAllocation are
	// accounted views of ctr.mp; expressionAllocation is an accounted view of
	// the process pool used by expression executors and the projection. All
	// are rebuilt by bindAllocationViews for each execution generation.
	groupByAllocation    *mpool.MPool
	expressionAllocation *mpool.MPool
	rollupAllocation     *mpool.MPool
	aggregateAllocation  *aggexec.AllocationAccount

	recoveryCapacity         *process.ExecutionRecoveryCapacity
	recoveryCapacityClass    mpool.AllocationCapacityClass
	recoveryCapacityActive   bool
	recoveryCapacityFloor    uint64
	spillGroupByAllocation   *mpool.MPool
	spillAggregateAllocation *aggexec.AllocationAccount

	inputDone    bool
	currBatchIdx int
	// inputRowCount is the source-row cursor used by GROUP_CONCAT diagnostics.
	// It survives resident spills/reloads but is reset for a new execution.
	inputRowCount uint64

	// hash.
	hr          ResHashRelated
	mtyp        int32
	keyWidth    int32
	keyNullable bool

	// x, y of `group by x, y`.
	groupByEvaluate colexec.ExprEvalVector
	// m, n of `select agg1(m, n), agg2(m, n)`.
	aggArgEvaluate []colexec.ExprEvalVector

	// group by columns
	groupByTypes   []types.Type
	groupByBatches []*batch.Batch
	groupByStandby *batch.Batch
	// groupByPreflights holds, for the current (last) group-by batch and the
	// standby batch, each column's SelectedBatchPreflight for the next
	// append into that batch.
	groupByPreflights [2]groupByPreflight
	groupingRollup    []*vector.Vector
	groupByHashKey    []int32
	hashKeyVecs       []*vector.Vector

	// MergeGroup locks the partial wire metadata on the first input. It must
	// survive resident spills, because later partials and queued spill records
	// still belong to that same hash-key domain.
	mergePartialMetadataSet bool
	// groupConcatSourceRowsUntrusted is a statement-wide, monotonic fallback
	// mode. It survives resident spills and aggregate rebuilds; once a remote
	// or legacy partial is observed, every later finalization uses the legacy
	// local warning counter.
	groupConcatSourceRowsUntrusted bool

	// aggs, which holds the intermediate state of agg functions.
	aggList               []aggexec.GroupAggFuncExec
	aggExprs              []aggexec.AggFuncExecExpression
	warningRetentionLimit int
	warningRetentionSet   bool
	groupConcatWarnings   aggexec.GroupConcatWarningAccumulator
	sortRollup            *sortRollupState
	// legacyVectorHLLState keeps vector HLL_ADD_AGG on the raw v2 hash domain
	// while a pre-v88 peer may consume that state.
	// legacyTextHLLAddState keeps CHAR and JSON HLL_ADD_AGG on the raw v2 hash
	// domain while a pre-v91 peer may consume that state.
	// legacyFloatHLLAddState keeps scalar FLOAT/DOUBLE HLL_ADD_AGG on the raw
	// v2 hash domain while a pre-v92 peer may consume that state.
	// legacyDistinctFloatKeys is frozen before aggregate groups are admitted.
	// Pre-v79 remote producers keep the compatibility FLOAT key policy, which
	// preserves every non-zero bit pattern in the fixed index and wire output.
	timeZone *time.Location

	// spill, agglist to load spilled data.
	spillMem                int64
	autoSpill               bool
	memoryGrowthParticipant *process.ExecutionMemoryGrowthParticipant
	spillAggList            []aggexec.GroupAggFuncExec
	spillBkts               []*spillBucket
	currentSpillBkt         []*spillBucket

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
	aggregateAllocation, err := aggexec.NewAllocationAccount(
		account,
		mpool.AllocationOwnerGroup,
		aggexec.AllocationAccountSites{
			VectorData:    GroupAllocationSiteAggregateData,
			VectorArea:    GroupAllocationSiteAggregateArea,
			ArgumentCount: GroupAllocationSiteAggregateArgumentCount,
			ArgumentArena: GroupAllocationSiteAggregateArgumentArena,
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
	spillAggregateAllocation, err := aggexec.NewAllocationAccountWithCapacityClass(
		account,
		mpool.AllocationOwnerGroup,
		aggexec.AllocationAccountSites{
			VectorData:    GroupAllocationSiteAggregateData,
			VectorArea:    GroupAllocationSiteAggregateArea,
			ArgumentCount: GroupAllocationSiteAggregateArgumentCount,
			ArgumentArena: GroupAllocationSiteAggregateArgumentArena,
		},
		recoveryClass,
	)
	if err != nil {
		return rollbackRecovery(err)
	}

	ctr.allocationAccount = account
	ctr.hashAllocation = hashAllocation
	ctr.hashIterator = hashIterator
	ctr.aggregateAllocation = aggregateAllocation
	ctr.recoveryCapacity = recoveryCapacity
	ctr.recoveryCapacityClass = recoveryClass
	ctr.spillAggregateAllocation = spillAggregateAllocation
	return nil
}

// bindAllocationViews builds the accounted MPool views of the bound account
// for the current execution generation. The key, rollup and recovery views
// are views of the private ctr.mp; the expression view is a view of the
// process pool, from which expression executors allocate.
func (ctr *container) bindAllocationViews(proc *process.Process) error {
	ctr.unbindAllocationViews()
	if ctr.allocationAccount == nil {
		return nil
	}
	if ctr.mp == nil || proc == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	account := ctr.allocationAccount
	groupBy, err := ctr.mp.WithAccount(account, mpool.AllocationOwnerGroup,
		GroupAllocationSiteKeyData, mpool.AllocationCapacityClassDefault)
	if err != nil {
		return err
	}
	rollup, err := ctr.mp.WithAccount(account, mpool.AllocationOwnerGroup,
		GroupAllocationSiteExpressionData, mpool.AllocationCapacityClassDefault)
	if err != nil {
		return err
	}
	expression, err := proc.Mp().WithAccount(account, mpool.AllocationOwnerGroup,
		GroupAllocationSiteExpressionData, mpool.AllocationCapacityClassDefault)
	if err != nil {
		return err
	}
	var spillGroupBy *mpool.MPool
	if ctr.recoveryCapacity != nil {
		spillGroupBy, err = ctr.mp.WithAccount(account, mpool.AllocationOwnerGroup,
			GroupAllocationSiteKeyData, ctr.recoveryCapacityClass)
		if err != nil {
			return err
		}
	}
	ctr.groupByAllocation = groupBy
	ctr.rollupAllocation = rollup
	ctr.expressionAllocation = expression
	ctr.spillGroupByAllocation = spillGroupBy
	return nil
}

// unbindAllocationViews drops the views of ctr.mp. The expression view is
// kept: executors built from it may outlive the private pool generation.
func (ctr *container) unbindAllocationViews() {
	ctr.groupByAllocation = nil
	ctr.rollupAllocation = nil
	ctr.spillGroupByAllocation = nil
}

// ownPool returns the operator pool vec belongs to: the private pool or one
// of its accounted views. A vector of any other pool is returned ctr.mp, so
// freeing it reports the mismatch.
func (ctr *container) ownPool(vec *vector.Vector) *mpool.MPool {
	mp := vec.MPool()
	if mp == nil {
		return ctr.mp
	}
	switch mp {
	case ctr.mp, ctr.groupByAllocation, ctr.rollupAllocation, ctr.spillGroupByAllocation:
		return mp
	}
	if ctr.aggregateAllocation.OwnsVectorMP(mp) || ctr.spillAggregateAllocation.OwnsVectorMP(mp) {
		return mp
	}
	return ctr.mp
}

// freeGroupVector frees a vector of the operator's buffered data.
func (ctr *container) freeGroupVector(vec *vector.Vector) {
	if vec != nil {
		vec.Free(ctr.ownPool(vec))
	}
}

// cleanGroupBatch frees a batch of the operator's buffered data. After the
// final flush a group-by batch holds key vectors of the group-by view and
// aggregate results of the aggregates' pool; each is freed with its own.
func (ctr *container) cleanGroupBatch(bat *batch.Batch) {
	if bat == nil {
		return
	}
	seen := make(map[*vector.Vector]struct{}, len(bat.Vecs))
	for i, vec := range bat.Vecs {
		if vec == nil {
			continue
		}
		if _, ok := seen[vec]; !ok {
			seen[vec] = struct{}{}
			ctr.freeGroupVector(vec)
		}
		bat.Vecs[i] = nil
	}
	// Every vector is released; Clean drops the batch's remaining state.
	bat.Clean(ctr.mp)
}

// groupByMP returns the pool through which resident group-by key storage
// allocates and grows.
func (ctr *container) groupByMP() *mpool.MPool {
	if ctr.groupByAllocation != nil {
		return ctr.groupByAllocation
	}
	return ctr.mp
}

// spillGroupByMP returns the pool through which recovery-class group-by
// key storage allocates and grows.
func (ctr *container) spillGroupByMP() *mpool.MPool {
	if ctr.spillGroupByAllocation != nil {
		return ctr.spillGroupByAllocation
	}
	return ctr.mp
}

// rollupMP returns the pool through which expression-scaled storage owned by
// the private pool (rollup constants, sort-rollup keys) allocates.
func (ctr *container) rollupMP() *mpool.MPool {
	if ctr.rollupAllocation != nil {
		return ctr.rollupAllocation
	}
	return ctr.mp
}

func (ctr *container) installRecoveryCapacity() error {
	if ctr == nil || ctr.allocationAccount == nil || ctr.budget == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.recoveryCapacity == nil ||
		ctr.recoveryCapacityClass == mpool.AllocationCapacityClassDefault ||
		ctr.spillAggregateAllocation == nil {
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
		len(ctr.spillBkts) != 0 {
		return nil
	}
	ctr.releaseMemoryGrowthParticipant()
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
	ctr.rollupAllocation = nil
	ctr.aggregateAllocation = nil
	return nil
}

func (ctr *container) isSpilling() bool {
	return len(ctr.currentSpillBkt) > 0
}

func (ctr *container) setSpillMem(m int64) {
	ctr.autoSpill = m == 0
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

func (ctr *container) ensureMemoryGrowthParticipant() error {
	if ctr == nil || !ctr.autoSpill || ctr.mtyp == H0 || ctr.budget == nil {
		return nil
	}
	if ctr.memoryGrowthParticipant != nil {
		return nil
	}
	participant, err := ctr.budget.RegisterMemoryGrowthParticipant()
	if err != nil {
		return err
	}
	ctr.memoryGrowthParticipant = participant
	return nil
}

func (ctr *container) releaseMemoryGrowthParticipant() {
	if ctr == nil || ctr.memoryGrowthParticipant == nil {
		return
	}
	ctr.memoryGrowthParticipant.Release()
	ctr.memoryGrowthParticipant = nil
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
		ctr.cleanGroupBatch(ctr.spillGbBatch)
		ctr.spillGbBatch = nil
	}
}

func (ctr *container) freeSpillBkts() {
	// free all spill buckets.
	for _, bkt := range ctr.spillBkts {
		bkt.free()
	}
	ctr.spillBkts = nil

	for _, bkt := range ctr.currentSpillBkt {
		bkt.free()
	}
	ctr.currentSpillBkt = nil
}

func (ctr *container) freeGroupByBatches() {
	for i := range ctr.groupByBatches {
		if ctr.groupByBatches[i] != nil {
			ctr.cleanGroupBatch(ctr.groupByBatches[i])
			ctr.groupByBatches[i] = nil
		}
	}
	ctr.groupByBatches = nil
	if ctr.groupByStandby != nil {
		ctr.cleanGroupBatch(ctr.groupByStandby)
		ctr.groupByStandby = nil
	}
	ctr.clearGroupByPreflights()
	ctr.currBatchIdx = 0
}

// groupByPreflight is the per-column preflight of one group-by batch.
type groupByPreflight struct {
	batch *batch.Batch
	vecs  []vector.SelectedBatchPreflight
}

func (p *groupByPreflight) reset(b *batch.Batch) {
	p.batch = b
	if b == nil {
		p.vecs = p.vecs[:0]
		return
	}
	if cap(p.vecs) < len(b.Vecs) {
		p.vecs = make([]vector.SelectedBatchPreflight, len(b.Vecs))
		return
	}
	p.vecs = p.vecs[:len(b.Vecs)]
	clear(p.vecs)
}

// groupByPreflightsFor returns the preflights retained for batch b, or nil.
func (ctr *container) groupByPreflightsFor(b *batch.Batch) []vector.SelectedBatchPreflight {
	for i := range ctr.groupByPreflights {
		if p := &ctr.groupByPreflights[i]; b != nil && p.batch == b {
			return p.vecs
		}
	}
	return nil
}

func (ctr *container) clearGroupByPreflights() {
	for i := range ctr.groupByPreflights {
		ctr.groupByPreflights[i].reset(nil)
	}
}

func (ctr *container) freeGroupingRollups() {
	if ctr == nil {
		return
	}
	for i := range ctr.groupingRollup {
		if ctr.groupingRollup[i] != nil {
			ctr.freeGroupVector(ctr.groupingRollup[i])
			ctr.groupingRollup[i] = nil
		}
	}
	ctr.groupingRollup = nil
}

func (ctr *container) free(expressionMP *mpool.MPool) {
	// free container stuff, WTH is the Free0?
	ctr.releaseMemoryGrowthParticipant()
	ctr.inputDone = false
	ctr.inputRowCount = 0
	ctr.hr.Free0()

	ctr.groupByEvaluate.Free()

	for i := range ctr.aggArgEvaluate {
		ctr.aggArgEvaluate[i].Free()
	}
	ctr.aggArgEvaluate = nil

	ctr.freeGroupByBatches()
	ctr.freeGroupingRollups()
	if ctr.sortRollup != nil {
		ctr.sortRollup.free(ctr)
		ctr.sortRollup = nil
	}
	ctr.freeAggList()
	ctr.groupConcatWarnings.Reset()
	ctr.aggExprs = nil
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
	ctr.groupConcatSourceRowsUntrusted = false
	ctr.budget = nil

	ctr.unbindAllocationViews()
	mpool.DeleteMPool(ctr.mp)
	ctr.mp = nil
}

// refreshGroupConcatSourceRowTrust folds the current aggregate modes into the
// logical Group/MergeGroup mode. The reduction is monotonic: a legacy or
// independent remote partial permanently selects the fallback for this
// execution, including all later spill rebuilds.
func (ctr *container) refreshGroupConcatSourceRowTrust() {
	if ctr == nil || ctr.groupConcatSourceRowsUntrusted {
		return
	}
	for _, agg := range ctr.aggList {
		if !aggexec.GroupConcatSourceRowsTrusted(agg) {
			ctr.groupConcatSourceRowsUntrusted = true
			return
		}
	}
}

func (ctr *container) reset(expressionMP *mpool.MPool) {
	ctr.free(expressionMP)
}

func (ctr *container) resetForSpill() {
	ctr.resetSpillState(false)
}

// resetSpillState releases resident aggregation state after a spill. A build
// retry must keep the current input vectors until its unpublished chunk has
// been consumed; other spill callers can release the expression state too.
func (ctr *container) resetSpillState(retainInput bool) {
	if ctr.distinctSpill != nil {
		ctr.distinctGroupReset = true
	}
	// Reset also frees the hash related stuff.
	ctr.hr.Free0()

	if !retainInput {
		ctr.groupByEvaluate.ResetForNextQuery()
		for i := range ctr.aggArgEvaluate {
			ctr.aggArgEvaluate[i].ResetForNextQuery()
		}
		// Grouping-set sentinel vectors are owned outside the expression
		// executors. Release them before recovery reload borrows capacity.
		ctr.freeGroupingRollups()
	}
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
			rollup := vector.NewConstNull(
				group.ctr.groupByEvaluate.Typ[i],
				group.ctr.groupByEvaluate.Vec[i].Length(),
				group.ctr.rollupMP(),
			)
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
	if !pipelineFailed && err == nil && proc != nil {
		group.ctr.groupConcatWarnings.Report(proc.GetWarningSink())
	}
	group.ctr.free(proc.Mp())
	// free projection stuff,
	group.FreeProjection(proc)
}

func (group *Group) Reset(proc *process.Process, pipelineFailed bool, err error) {
	group.logDiagnostics(proc, pipelineFailed, err)
	if !pipelineFailed && err == nil && proc != nil {
		group.ctr.groupConcatWarnings.Report(proc.GetWarningSink())
	}
	group.ctr.reset(proc.Mp())
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
	// GroupingAware declares, from the producer plan, that partials carry
	// grouping-set rows. Dynamic grouping streams can legally alternate partials
	// with and without an actual rolled-up key.
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

func (mergeGroup *MergeGroup) Reset(proc *process.Process, pipelineFailed bool, err error) {
	if !pipelineFailed && err == nil && proc != nil {
		mergeGroup.ctr.groupConcatWarnings.Report(proc.GetWarningSink())
	}
	mergeGroup.ctr.reset(proc.Mp())
	if mergeGroup.ctr.allocationAccount != nil {
		mergeGroup.FreeProjection(proc)
	} else {
		mergeGroup.ResetProjection(proc)
	}
}

func (mergeGroup *MergeGroup) Free(proc *process.Process, pipelineFailed bool, err error) {
	if !pipelineFailed && err == nil && proc != nil {
		mergeGroup.ctr.groupConcatWarnings.Report(proc.GetWarningSink())
	}
	mergeGroup.ctr.free(proc.Mp())
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

func newGroupArgument() *Group {
	return &Group{}
}

func resetGroupArgument(a *Group) {
	*a = Group{}
}

func init() {
	reuse.CreatePool(
		newGroupArgument,
		resetGroupArgument,
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
