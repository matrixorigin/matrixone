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
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillio"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

var _ vm.Operator = new(HashBuild)

var hashBuildSpillSequence atomic.Uint64

const (
	BuildHashMap = iota
	HandleRuntimeFilter
	SendJoinMap
	SendSucceed
)

const (
	HashBuildSpillAllocationSiteSelectedData mpool.AllocationSite = iota + 64
	HashBuildSpillAllocationSiteSelectedArea
	HashBuildSpillAllocationSiteSelectedNulls
	HashBuildSpillAllocationSiteSelectedGrouping
	HashBuildSpillAllocationSiteHashValues
	HashBuildSpillAllocationSiteRowIDs
	HashBuildSpillAllocationSiteMarshalBuffer
	HashBuildSpillAllocationSiteCoalesceBuffer
)

const (
	HashBuildAllocationSiteHashCell mpool.AllocationSite = iota + 24
	HashBuildAllocationSiteHashDescriptor
	HashBuildAllocationSiteBatchData
	HashBuildAllocationSiteBatchArea
	HashBuildAllocationSiteBatchNulls
	HashBuildAllocationSiteBatchGrouping
	HashBuildAllocationSiteGroupSels
	HashBuildAllocationSiteHashIterator
)

// Runtime-filter keys and their published wire payload have lifetimes that
// differ from copied build batches: keys die after publication while the
// payload lives on the message board until every receiver destroys it. Keep
// their sites distinct from both the builder and SpillEngine ranges.
const (
	HashBuildAllocationSiteUniqueKeyData mpool.AllocationSite = iota + 44
	HashBuildAllocationSiteUniqueKeyArea
	HashBuildAllocationSiteUniqueKeyNulls
	HashBuildAllocationSiteUniqueKeyGrouping
	HashBuildAllocationSiteRuntimeFilterPayload
	HashBuildAllocationSiteRuntimeFilterScratch
	HashBuildAllocationSiteDedupIgnoreBitmap
	HashBuildAllocationSiteDedupDeleteBitmap
	HashBuildAllocationSiteDedupLastRows
	HashBuildAllocationSiteDedupSurvivorRows
	HashBuildAllocationSiteDedupSurvivorOwnsKey
	HashBuildAllocationSiteDedupDiscardedRows
	HashBuildAllocationSiteDedupDeleteOnlyData
	HashBuildAllocationSiteDedupDeleteOnlyArea
	HashBuildAllocationSiteDedupDeleteOnlyNulls
	HashBuildAllocationSiteDedupDeleteOnlyGrouping
)

type container struct {
	state           int
	runtimeFilterIn bool
	// terminalPublished is the per-execution generation gate for the JoinMap
	// dependency.  A successful JoinMap or a BuildError wins this gate exactly
	// once; Reset/Free/cancel paths cannot replace or duplicate it.
	terminalPublished uint32
	terminalMu        sync.Mutex
	runtimeFilterDone bool
	diagnosticsLogged bool
	hashmapBuilder    HashmapBuilder
	// Build owns the dormant named-file bundle until JoinMap publication wins;
	// after that JoinMap/SpillEngine owns it and invokes release exactly once.
	spillBundle    *spillFileBundle
	spillFS        fileservice.MutableFileService
	spillUUID      string // unique prefix for named spill paths
	spillThreshold int64
	// A zero configured threshold uses the statement/CN budget which remains
	// after admitted allocations and exact recovery floors. The participant is
	// live only while build() can still choose to retain another input batch.
	autoSpill               bool
	memoryGrowthParticipant *process.ExecutionMemoryGrowthParticipant
	autoSpillTriggered      bool
	autoSpillLimitAtTrigger uint64
	// Monotone while ingress can retain batches; Prepare starts a new generation.
	// Only the spill projection uses this bit, not the final hashmap selection.
	autoSpillHasGrouping bool

	// reusable buffers for spill operations
	spillHashValues []uint64
	// spillBucketRowIds stores one contiguous row-id array for the current
	// input batch.  spillBucketOffsets identifies each bucket's sub-slice;
	// keeping one array avoids the 32 independent append/growth paths used by
	// the old scatter implementation.
	spillBucketRowIds      []int32
	spillBucketCounts      [spillNumBuckets]int32
	spillBucketOffsets     [spillNumBuckets + 1]int32
	spillBucketWriteRows   [spillNumBuckets]int64
	spillKeyVecs           []*vector.Vector
	spillBatchAllocation   *vector.AllocationAccountSelection
	spillAllocationMP      *mpool.MPool
	spillAccountedWrite    *mpool.AccountedBuffer
	spillAccountedBuckets  [spillNumBuckets]*mpool.AccountedBuffer
	spillCoalesceDisabled  bool
	recoveryCapacity       *process.ExecutionRecoveryCapacity
	recoveryCapacityClass  mpool.AllocationCapacityClass
	expressionRecoveryPeak uint64
	expressionRecoveryRows int
	spillRecoveryPeak      uint64
	// cached expression executors for spill (reused across batches)
	spillExprExecs  []colexec.ExpressionExecutor
	spillConditions []*plan.Expr
}

// spillFileBundle is deliberately owned by hashbuild. A bucket retains its
// durable name and disk reservation, but owns an FD only around one physical
// read/write. Build converts each dormant entry to message.SpillFile at
// handoff. Keeping all tokens together prevents a file from becoming an
// unaccounted orphan on partial failures.
type spillFileBundle struct {
	mu       sync.Mutex
	entries  map[int]*spillFileEntry
	released bool
}

type spillFileEntry struct {
	file       *os.File
	fdToken    *process.ExecutionSpillFDReservation
	diskToken  *process.ExecutionSpillDiskReservation
	fs         fileservice.MutableFileService
	name       string
	created    bool
	rows       int64
	bytes      uint64
	bucket     int
	writeCache spillio.SequentialWriteCache
}

func (b *spillFileBundle) release() {
	if b == nil {
		return
	}
	b.mu.Lock()
	if b.released {
		b.mu.Unlock()
		return
	}
	b.released = true
	entries := b.entries
	b.entries = nil
	b.mu.Unlock()
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		if entry.file != nil {
			_ = entry.file.Close()
		}
		if entry.fs != nil && entry.name != "" {
			_ = entry.fs.RemoveFile(context.Background(), entry.name)
		}
		if entry.fdToken != nil {
			entry.fdToken.Release()
		}
		if entry.diskToken != nil {
			entry.diskToken.Release()
		}
	}
}

func (b *spillFileBundle) openFile(
	ctx context.Context,
	budget *process.ExecutionResourceGeneration,
	bucket int,
	fs fileservice.MutableFileService,
	name string,
) (*os.File, error) {
	if b == nil || budget == nil || fs == nil || name == "" ||
		bucket < 0 || bucket >= spillNumBuckets {
		return nil, process.ErrExecutionResourceInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.released {
		return nil, process.ErrExecutionSpillReservationInactive
	}
	if b.entries == nil {
		b.entries = make(map[int]*spillFileEntry)
	}
	entry := b.entries[bucket]
	if entry == nil {
		entry = &spillFileEntry{bucket: bucket, fs: fs, name: name}
		b.entries[bucket] = entry
	}
	if entry.file != nil || entry.fdToken != nil || entry.name != name {
		return nil, process.ErrExecutionResourceInvalid
	}
	token, err := budget.ReserveSpillFD(1)
	if err != nil {
		return nil, err
	}
	var file *os.File
	if entry.created {
		file, err = fs.OpenFile(ctx, name)
		if err == nil {
			_, err = file.Seek(0, io.SeekEnd)
		}
	} else {
		file, err = fs.CreateFile(ctx, name)
	}
	if err != nil {
		if file != nil {
			_ = file.Close()
		}
		token.Release()
		return nil, err
	}
	entry.file = file
	entry.fdToken = token
	entry.created = true
	return file, nil
}

func (b *spillFileBundle) closeFile(bucket int) error {
	if b == nil || bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrExecutionResourceInvalid
	}
	b.mu.Lock()
	entry := b.entries[bucket]
	if entry == nil || entry.file == nil || entry.fdToken == nil {
		b.mu.Unlock()
		return process.ErrExecutionResourceInvalid
	}
	file := entry.file
	token := entry.fdToken
	entry.file = nil
	entry.fdToken = nil
	b.mu.Unlock()
	err := file.Close()
	token.Release()
	return err
}

func (b *spillFileBundle) growDisk(bucket int, budget *process.ExecutionResourceGeneration, bytes uint64) (uint64, bool, error) {
	if b == nil || budget == nil || bucket < 0 || bucket >= spillNumBuckets {
		return 0, false, process.ErrExecutionResourceInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.released {
		return 0, false, process.ErrExecutionSpillReservationInactive
	}
	entry := b.entries[bucket]
	if entry == nil {
		return 0, false, process.ErrExecutionResourceInvalid
	}
	if entry.diskToken == nil {
		token, err := budget.ReserveSpillDisk(bytes)
		if err != nil {
			return 0, false, err
		}
		entry.diskToken = token
		return 0, true, nil
	}
	old := entry.diskToken.Size()
	if err := entry.diskToken.Grow(bytes); err != nil {
		return 0, false, err
	}
	return old, false, nil
}

func (b *spillFileBundle) recordDiskWrite(
	bucket int,
	file *os.File,
	rows int64,
	bytes uint64,
) error {
	if b == nil || bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrExecutionResourceInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.entries[bucket]
	if entry == nil || entry.file != file || bytes > uint64(^uint(0)>>1) {
		return process.ErrExecutionResourceInvalid
	}
	if err := entry.writeCache.RecordWrite(file, int(bytes)); err != nil {
		return err
	}
	entry.rows += rows
	if ^uint64(0)-entry.bytes >= bytes {
		entry.bytes += bytes
	} else {
		entry.bytes = ^uint64(0)
	}
	return nil
}

func (b *spillFileBundle) accountedFiles(
	ctx context.Context,
	budget *process.ExecutionResourceGeneration,
) ([]*message.SpillFile, error) {
	if b == nil {
		return nil, nil
	}
	if budget == nil {
		return nil, process.ErrExecutionResourceInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	files := make([]*message.SpillFile, spillNumBuckets)
	for bucket, entry := range b.entries {
		if entry == nil || entry.fs == nil || entry.name == "" ||
			entry.file != nil || entry.fdToken != nil || !entry.created ||
			entry.bucket != bucket {
			return nil, process.ErrExecutionResourceInvalid
		}
		fdToken, err := budget.ReserveSpillFD(1)
		if err != nil {
			return nil, err
		}
		file, err := entry.fs.OpenFile(ctx, entry.name)
		if err != nil {
			fdToken.Release()
			return nil, err
		}
		if err := validateSpillFileSize(file, entry.bytes); err != nil {
			_ = file.Close()
			fdToken.Release()
			return nil, err
		}
		entry.writeCache.Finish(file)
		if err := file.Close(); err != nil {
			fdToken.Release()
			return nil, err
		}
		fdToken.Release()
		e := entry
		fs := entry.fs
		name := entry.name
		accounted := message.NewReopenableSpillFile(
			func(ctx context.Context) (*os.File, error) {
				return fs.OpenFile(ctx, name)
			},
			func() error {
				return fs.RemoveFile(context.Background(), name)
			},
			e.rows,
			e.bytes,
			func() {
				if e.diskToken != nil {
					e.diskToken.Release()
					e.diskToken = nil
				}
			},
		)
		files[bucket] = accounted
	}
	b.entries = nil
	b.released = true
	return files, nil
}

func validateSpillFileSize(file *os.File, expected uint64) error {
	if file == nil || expected == 0 {
		return process.ErrExecutionResourceInvalid
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 0 || uint64(info.Size()) != expected {
		return moerr.NewInternalErrorf(
			context.Background(),
			"corrupted spill file size: expected=%d actual=%d",
			expected,
			info.Size(),
		)
	}
	return nil
}

type HashBuild struct {
	ctr                           container
	NeedHashMap                   bool
	HashOnPK                      bool
	NeedBatches                   bool
	NeedAllocateSels              bool
	TrackNullKeys                 bool
	IsShuffle                     bool
	Conditions                    []*plan.Expr
	OwnsConstantFilterDiagnostics bool
	JoinDiagnostic                *colexec.DeferredJoinDiagnostic
	JoinMapTag                    int32
	JoinMapRefCnt                 int32
	ShuffleIdx                    int32
	RuntimeFilterSpec             *plan.RuntimeFilterSpec
	SpillThreshold                int64

	IsDedup                   bool
	DedupBuildKeepLast        bool
	DelColIdx                 int32
	OnDuplicateAction         plan.Node_OnDuplicateAction
	DedupColName              string
	DedupColTypes             []plan.Type
	DedupDeleteMarkerColIdx   int32
	DedupDeleteKeepColIdxList []int32

	vm.OperatorBase
}

func (hashBuild *HashBuild) GetOperatorBase() *vm.OperatorBase {
	return &hashBuild.OperatorBase
}

// SetAllocationAccount selects immutable provenance for the hash-table owner
// before Prepare. Compile invokes it once for each execution attempt; Reset
// clears the selection only after producer or JoinMap ownership has moved on.
func (hashBuild *HashBuild) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	selection, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerHashBuild,
		HashBuildSpillAllocationSiteSelectedData,
		HashBuildSpillAllocationSiteSelectedArea,
		HashBuildSpillAllocationSiteSelectedNulls,
		HashBuildSpillAllocationSiteSelectedGrouping,
	)
	if err != nil {
		return err
	}
	if err := hashBuild.ctr.hashmapBuilder.SetAllocationAccount(account); err != nil {
		return err
	}
	hashBuild.ctr.spillBatchAllocation = selection
	return nil
}

func (hashBuild *HashBuild) installRecoveryCapacity(
	budget *process.ExecutionResourceGeneration,
) error {
	ctr := &hashBuild.ctr
	account := ctr.hashmapBuilder.mapAllocationAccount
	if account == nil {
		return mpool.ErrAllocationAccountInvariant
	}
	if ctr.recoveryCapacity != nil {
		if ctr.recoveryCapacityClass == mpool.AllocationCapacityClassDefault ||
			ctr.hashmapBuilder.recoveryCapacityClass != ctr.recoveryCapacityClass {
			return mpool.ErrAllocationAccountInvariant
		}
		return nil
	}
	if ctr.recoveryCapacityClass != mpool.AllocationCapacityClassDefault {
		return mpool.ErrAllocationAccountInvariant
	}
	capacity, err := process.NewExecutionRecoveryCapacity(budget)
	if err != nil {
		return err
	}
	class, err := account.RegisterCapacityController(capacity)
	if err != nil {
		_ = capacity.Close()
		return err
	}
	selection, err := vector.NewAllocationAccountSelectionWithCapacityClass(
		account,
		mpool.AllocationOwnerHashBuild,
		HashBuildSpillAllocationSiteSelectedData,
		HashBuildSpillAllocationSiteSelectedArea,
		HashBuildSpillAllocationSiteSelectedNulls,
		HashBuildSpillAllocationSiteSelectedGrouping,
		class,
	)
	if err != nil {
		_ = account.UnregisterCapacityController(class, capacity)
		_ = capacity.Close()
		return err
	}
	ctr.recoveryCapacity = capacity
	ctr.recoveryCapacityClass = class
	ctr.spillBatchAllocation = selection
	ctr.hashmapBuilder.recoveryCapacityClass = class
	return nil
}

// releaseRecoveryCapacity returns recovery headroom at a terminal build result,
// after all scratch borrowers have been dropped. restoreDefault keeps later
// test/reuse allocations on the statement's ordinary controller;
// statement teardown passes false and drops the selection immediately afterward.
func (hashBuild *HashBuild) releaseRecoveryCapacity(
	account *mpool.AllocationAccount,
	restoreDefault bool,
) error {
	ctr := &hashBuild.ctr
	if ctr.recoveryCapacity == nil {
		return nil
	}
	if account == nil || ctr.recoveryCapacityClass ==
		mpool.AllocationCapacityClassDefault {
		return mpool.ErrAllocationAccountInvariant
	}
	capacity := ctr.recoveryCapacity
	class := ctr.recoveryCapacityClass
	if err := capacity.Close(); err != nil {
		return err
	}
	if err := account.UnregisterCapacityController(class, capacity); err != nil {
		return err
	}
	ctr.recoveryCapacity = nil
	ctr.recoveryCapacityClass = mpool.AllocationCapacityClassDefault
	ctr.expressionRecoveryPeak = 0
	ctr.expressionRecoveryRows = 0
	ctr.spillRecoveryPeak = 0
	ctr.hashmapBuilder.recoveryCapacityClass =
		mpool.AllocationCapacityClassDefault
	if !restoreDefault {
		ctr.spillBatchAllocation = nil
		return nil
	}
	selection, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerHashBuild,
		HashBuildSpillAllocationSiteSelectedData,
		HashBuildSpillAllocationSiteSelectedArea,
		HashBuildSpillAllocationSiteSelectedNulls,
		HashBuildSpillAllocationSiteSelectedGrouping,
	)
	if err != nil {
		ctr.spillBatchAllocation = nil
		return err
	}
	ctr.spillBatchAllocation = selection
	return nil
}

// SetAllocationAccount installs the physical allocation provenance shared by
// the producer HashBuild and SpillEngine rebuild builders. A builder is always
// single-generation and clears the selection only after all owned resources
// have either been freed or transferred to a JoinMap.
func (hb *HashmapBuilder) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	builder := hb
	if builder.mapAllocationAccount != nil {
		if builder.mapAllocationAccount == account {
			return nil
		}
		return mpool.ErrAllocationAccountMismatch
	}
	selection, err := hashtable.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerHashBuild,
		HashBuildAllocationSiteHashCell,
		HashBuildAllocationSiteHashDescriptor,
	)
	if err != nil {
		return err
	}
	iteratorAllocation, err := hashmap.NewIteratorAllocation(
		account,
		mpool.AllocationOwnerHashBuild,
		HashBuildAllocationSiteHashIterator,
	)
	if err != nil {
		return err
	}
	batchSelection, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerHashBuild,
		HashBuildAllocationSiteBatchData,
		HashBuildAllocationSiteBatchArea,
		HashBuildAllocationSiteBatchNulls,
		HashBuildAllocationSiteBatchGrouping,
	)
	if err != nil {
		return err
	}
	uniqueKeySelection, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerHashBuild,
		HashBuildAllocationSiteUniqueKeyData,
		HashBuildAllocationSiteUniqueKeyArea,
		HashBuildAllocationSiteUniqueKeyNulls,
		HashBuildAllocationSiteUniqueKeyGrouping,
	)
	if err != nil {
		return err
	}
	builder.mapAllocationAccount = account
	builder.mapAllocation = selection
	builder.iteratorAllocation = iteratorAllocation
	builder.batchAllocation = batchSelection
	builder.uniqueKeyAllocation = uniqueKeySelection
	return nil
}

func (hashBuild *HashBuild) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	builder := &hashBuild.ctr.hashmapBuilder
	if len(hashBuild.ctr.spillExprExecs) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	if hashBuild.ctr.spillAllocationMP != nil ||
		hashBuild.ctr.spillAccountedWrite != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	for _, buffer := range hashBuild.ctr.spillAccountedBuckets {
		if buffer != nil {
			return mpool.ErrAllocationAccountInvariant
		}
	}
	if hashBuild.ctr.recoveryCapacity != nil {
		if err := hashBuild.releaseRecoveryCapacity(account, false); err != nil {
			return err
		}
	}
	if err := builder.ClearAllocationAccount(account); err != nil {
		return err
	}
	hashBuild.ctr.spillBatchAllocation = nil
	return nil
}

// ClearAllocationAccount verifies that no builder-owned object can allocate
// through the generation before dropping its selections.
func (hb *HashmapBuilder) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	builder := hb
	if builder.mapAllocationAccount == nil {
		return nil
	}
	if builder.mapAllocationAccount != account {
		return mpool.ErrAllocationAccountMismatch
	}
	if builder.IntHashMap != nil || builder.StrHashMap != nil ||
		len(builder.Batches.Buf) != 0 || builder.Sels.Size() != 0 ||
		len(builder.executors) != 0 || len(builder.curVecs) != 0 ||
		len(builder.UniqueJoinKeys) != 0 ||
		builder.IgnoreRows != nil || builder.DelRows != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	builder.mapAllocationAccount = nil
	builder.mapAllocation = nil
	builder.iteratorAllocation = nil
	builder.batchAllocation = nil
	builder.uniqueKeyAllocation = nil
	builder.recoveryCapacityClass = mpool.AllocationCapacityClassDefault
	return nil
}

func init() {
	reuse.CreatePool[HashBuild](
		func() *HashBuild {
			return &HashBuild{}
		},
		func(a *HashBuild) {
			// Preserve cached iterators across pool resets to avoid reallocating
			// short-lived iterator buffers. Detach owners so old hashmaps can be GC'd.
			intItr, strItr := a.ctr.hashmapBuilder.ExtractCachedIteratorsForReuse()

			*a = HashBuild{}
			a.ctr.hashmapBuilder.RestoreCachedIterators(intItr, strItr)
		},
		reuse.DefaultOptions[HashBuild]().
			WithEnableChecker(),
	)
}

func (hashBuild *HashBuild) TypeName() string {
	return opName
}

func NewArgument() *HashBuild {
	return reuse.Alloc[HashBuild](nil)
}

func (hashBuild *HashBuild) Release() {
	if hashBuild != nil {
		reuse.Free[HashBuild](hashBuild, nil)
	}
}

func (hashBuild *HashBuild) Reset(proc *process.Process, pipelineFailed bool, err error) {
	hashBuild.ctr.terminalMu.Lock()
	defer hashBuild.ctr.terminalMu.Unlock()
	hashBuild.logDiagnostics(proc, pipelineFailed, err)
	hashBuild.ctr.releaseMemoryGrowthParticipant()
	runtimeSucceed := hashBuild.ctr.state > HandleRuntimeFilter
	mapSucceed := hashBuild.ctr.state == SendSucceed

	// Call does not publish pipeline terminal signals.  Reset owns dependency
	// finalization and is intentionally non-blocking: publication only appends
	// one immutable value to the current-CN MessageBoard.
	if !mapSucceed {
		if pipelineFailed || err != nil {
			if err == nil {
				err = moerr.NewQueryInterrupted(proc.Ctx)
			}
			hashBuild.publishBuildError(proc, err)
		} else {
			// Preserve the established nil JoinMap convention for a true empty
			// build and for cleanup paths that completed without a map.
			hashBuild.publishJoinMap(proc, nil)
		}
	}

	hashBuild.ctr.hashmapBuilder.Reset(proc, !mapSucceed)
	hashBuild.ctr.dropSpillScratchBuffers()
	// Only clean up build files when the join map was NOT successfully sent.
	// When mapSucceed=true, hashjoin owns the files and deletes them after reading.
	if !mapSucceed {
		hashBuild.cleanupSpillFiles(proc)
	}
	hashBuild.ctr.spillFS = nil
	hashBuild.ctr.state = BuildHashMap
	hashBuild.ctr.runtimeFilterIn = false
	if !hashBuild.ctr.runtimeFilterDone {
		if pipelineFailed || err != nil {
			// A failed build must complete the runtime-filter dependency with
			// PASS.  DROP would incorrectly filter all probe rows because no
			// unique keys were published.
			message.FinalizeRuntimeFilterOnBuildError(hashBuild.RuntimeFilterSpec, proc.GetMessageBoard())
		} else {
			message.FinalizeRuntimeFilter(hashBuild.RuntimeFilterSpec, runtimeSucceed, proc.GetMessageBoard())
		}
		hashBuild.ctr.runtimeFilterDone = hashBuild.RuntimeFilterSpec != nil
	}
	// Keep the terminal gate closed for this execution generation. Prepare is
	// the only boundary that opens it for the next generation, which makes
	// repeated Reset calls idempotent.
}
func (hashBuild *HashBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	hashBuild.ctr.terminalMu.Lock()
	defer hashBuild.ctr.terminalMu.Unlock()
	hashBuild.logDiagnostics(proc, pipelineFailed, err)
	hashBuild.ctr.releaseMemoryGrowthParticipant()
	// Normally Reset runs before Free.  Keep Free as a safe fallback for
	// cancellation/error cleanup paths that bypass Reset, while preserving the
	// exactly-once generation gate.
	if atomic.LoadUint32(&hashBuild.ctr.terminalPublished) == 0 && (pipelineFailed || err != nil) {
		if err == nil {
			err = moerr.NewQueryInterrupted(proc.Ctx)
		}
		hashBuild.publishBuildError(proc, err)
	}
	hashBuild.cleanupSpillFiles(proc)
	hashBuild.ctr.spillFS = nil
	hashBuild.ctr.hashmapBuilder.Free(proc)
	hashBuild.ctr.freeSpillExprExecs()
	hashBuild.ctr.dropSpillScratchBuffers()
}

func (hashBuild *HashBuild) logDiagnostics(proc *process.Process, pipelineFailed bool, err error) {
	if hashBuild.ctr.diagnosticsLogged {
		return
	}
	hashBuild.ctr.diagnosticsLogged = true
	if proc == nil || hashBuild.OpAnalyzer == nil {
		return
	}
	extra := hashBuild.OpAnalyzer.GetOpStats().ExtraStats
	if !hasHashBuildDiagnosticStats(extra) {
		return
	}
	logutil.Info("operator diagnostic summary",
		trace.ContextField(proc.Ctx),
		zap.String("query_id", proc.QueryId()),
		zap.String("operator", opName),
		zap.Int("node_idx", hashBuild.GetIdx()),
		zap.Int32("shuffle_idx", hashBuild.ShuffleIdx),
		zap.Bool("pipeline_failed", pipelineFailed),
		zap.Error(err),
		zap.Any("extra_stats", extra))
}

func hasHashBuildDiagnosticStats(extra map[string]int64) bool {
	return extra["HashBuildSpillStarts"] != 0 ||
		extra["HashBuildAdaptiveSpillStarts"] != 0 ||
		extra["QueryHashBudgetRejects"] != 0 ||
		extra["HashBuildRuntimeFilterCollectionFallbacks"] != 0 ||
		extra["HashBuildRuntimeFilterBudgetFallbacks"] != 0 ||
		extra["HashBuildRuntimeFilterAllocationFallbacks"] != 0 ||
		extra["HashBuildSpillRecoveryReserveRejects"] != 0
}

func (hashBuild *HashBuild) publishJoinMap(proc *process.Process, jm *message.JoinMap) bool {
	if !atomic.CompareAndSwapUint32(&hashBuild.ctr.terminalPublished, 0, 1) {
		return false
	}
	if !message.SendJoinMapResult(
		message.NewJoinMapResult(jm),
		hashBuild.JoinMapTag,
		hashBuild.IsShuffle,
		hashBuild.ShuffleIdx,
		proc.GetMessageBoard(),
	) {
		atomic.StoreUint32(&hashBuild.ctr.terminalPublished, 0)
		return false
	}
	return true
}

func (hashBuild *HashBuild) publishBuildError(proc *process.Process, err error) bool {
	if !atomic.CompareAndSwapUint32(&hashBuild.ctr.terminalPublished, 0, 1) {
		return false
	}
	if !message.FinalizeJoinMapBuildError(
		proc.GetMessageBoard(),
		hashBuild.JoinMapTag,
		hashBuild.IsShuffle,
		hashBuild.ShuffleIdx,
		err,
	) {
		atomic.StoreUint32(&hashBuild.ctr.terminalPublished, 0)
		return false
	}
	return true
}

func (hashBuild *HashBuild) cleanupSpillFiles(proc *process.Process) {
	// Release physical descriptors before their FD tokens, then remove durable
	// names and return disk ownership.
	if hashBuild.ctr.spillBundle != nil {
		hashBuild.ctr.spillBundle.release()
		hashBuild.ctr.spillBundle = nil
	}
}

// CleanCopiedBatchAt releases one retained build batch after it has been
// durably transferred to spill storage.
func (hb *HashmapBuilder) CleanCopiedBatchAt(idx int, proc *process.Process) error {
	if idx < 0 || idx >= len(hb.Batches.Buf) {
		return process.ErrExecutionResourceInvalid
	}
	if bat := hb.Batches.Buf[idx]; bat != nil {
		bat.Clean(proc.Mp())
	}
	copy(hb.Batches.Buf[idx:], hb.Batches.Buf[idx+1:])
	hb.Batches.Buf = hb.Batches.Buf[:len(hb.Batches.Buf)-1]
	hb.Batches.MemSize = 0
	for _, bat := range hb.Batches.Buf {
		if bat != nil {
			hb.Batches.MemSize += int64(bat.Size())
		}
	}
	if len(hb.Batches.Buf) == 0 {
		hb.retainedSpillTailSelected = 0
	}
	return nil
}

// DrainCopiedBatches visits and then releases every retained physical build
// batch. A failed visit leaves the current and remaining batches owned by the
// builder so its normal cleanup path can release them.
func (hb *HashmapBuilder) DrainCopiedBatches(
	proc *process.Process,
	visit func(*batch.Batch) error,
) error {
	for idx, bat := range hb.Batches.Buf {
		if visit != nil {
			if err := visit(bat); err != nil {
				remaining := hb.Batches.Buf[idx:]
				copy(hb.Batches.Buf, remaining)
				clear(hb.Batches.Buf[len(remaining):])
				hb.Batches.Buf = hb.Batches.Buf[:len(remaining)]
				hb.Batches.MemSize = 0
				for _, retained := range hb.Batches.Buf {
					if retained != nil {
						hb.Batches.MemSize += int64(retained.Size())
					}
				}
				return err
			}
		}
		if bat != nil {
			bat.Clean(proc.Mp())
			hb.Batches.Buf[idx] = nil
		}
	}
	hb.Batches.Buf = nil
	hb.Batches.MemSize = 0
	hb.retainedSpillTailSelected = 0
	return nil
}

func (hashBuild *HashBuild) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) setSpillThreshold(threshold int64) {
	ctr.autoSpill = threshold == 0
	ctr.spillThreshold = colexec.ResolveSpillThreshold(threshold)
}

func (ctr *container) releaseMemoryGrowthParticipant() {
	if ctr == nil || ctr.memoryGrowthParticipant == nil {
		return
	}
	ctr.memoryGrowthParticipant.Release()
	ctr.memoryGrowthParticipant = nil
}
