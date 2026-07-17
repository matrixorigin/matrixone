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
	"os"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(HashBuild)

var hashBuildSpillSequence atomic.Uint64

const (
	BuildHashMap = iota
	HandleRuntimeFilter
	SendJoinMap
	SendSucceed
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
	hashmapBuilder    HashmapBuilder
	spilledFds        []*os.File // anonymous build-side spill fds (ownership transferred to JoinMap)
	// spillBundle keeps the resource reservations associated with spilledFds.
	// Build owns the bundle until the JoinMap publication wins; after that the
	// JoinMap/SpillEngine handoff owns it and invokes release exactly once.
	spillBundle    *spillFileBundle
	spillFS        fileservice.MutableFileService
	spillUUID      string // unique prefix for anonymous file paths
	spillThreshold int64

	// reusable buffers for spill operations
	spillHashValues   []uint64
	spillBucketRowIds [][]int32
	spillSelection    []int32
	spillWriteBuf     bytes.Buffer
	spillKeyVecs      []*vector.Vector

	// cached expression executors for spill (reused across batches)
	spillExprExecs []colexec.ExpressionExecutor
}

// spillFileBundle is deliberately owned by hashbuild.  Build converts each
// entry to message.SpillFile only after every file has been rewound; the
// resulting file object carries its token release closure through JoinMap and
// the SpillEngine. Keeping all tokens together prevents a file from becoming
// an unaccounted orphan on partial failures.
type spillFileBundle struct {
	mu       sync.Mutex
	entries  map[*os.File]*spillFileEntry
	released bool
}

type spillFileEntry struct {
	fdToken    *process.HashBuildSpillFDReservation
	diskTokens []*process.HashBuildSpillDiskReservation
	rows       int64
	bytes      uint64
	bucket     int
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
		if entry.fdToken != nil {
			entry.fdToken.Release()
		}
		for _, token := range entry.diskTokens {
			if token != nil {
				token.Release()
			}
		}
	}
}

func (b *spillFileBundle) addFD(file *os.File, bucket int, token *process.HashBuildSpillFDReservation) {
	if b == nil || file == nil {
		return
	}
	b.mu.Lock()
	if b.released {
		b.mu.Unlock()
		if token != nil {
			token.Release()
		}
		return
	}
	if b.entries == nil {
		b.entries = make(map[*os.File]*spillFileEntry)
	}
	entry := b.entries[file]
	if entry == nil {
		entry = &spillFileEntry{bucket: bucket}
		b.entries[file] = entry
	}
	entry.fdToken = token
	b.mu.Unlock()
}

func (b *spillFileBundle) addDisk(file *os.File, token *process.HashBuildSpillDiskReservation, rows int64, bytes uint64) {
	if b == nil || file == nil || token == nil {
		return
	}
	b.mu.Lock()
	if b.released {
		b.mu.Unlock()
		token.Release()
		return
	}
	if b.entries == nil {
		b.entries = make(map[*os.File]*spillFileEntry)
	}
	entry := b.entries[file]
	if entry == nil {
		entry = &spillFileEntry{bucket: -1}
		b.entries[file] = entry
	}
	entry.diskTokens = append(entry.diskTokens, token)
	entry.rows += rows
	if ^uint64(0)-entry.bytes >= bytes {
		entry.bytes += bytes
	} else {
		entry.bytes = ^uint64(0)
	}
	b.mu.Unlock()
}

func (b *spillFileBundle) accountedFiles() []*message.SpillFile {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	files := make([]*message.SpillFile, spillNumBuckets)
	for file, entry := range b.entries {
		f, e := file, entry
		accounted := message.NewSpillFile(f, e.rows, e.bytes, func() {
			if e.fdToken != nil {
				e.fdToken.Release()
			}
			for _, token := range e.diskTokens {
				if token != nil {
					token.Release()
				}
			}
		})
		if e.bucket >= 0 && e.bucket < len(files) {
			files[e.bucket] = accounted
		}
	}
	return files
}

type HashBuild struct {
	ctr               container
	NeedHashMap       bool
	HashOnPK          bool
	NeedBatches       bool
	NeedAllocateSels  bool
	TrackNullKeys     bool
	IsShuffle         bool
	Conditions        []*plan.Expr
	JoinMapTag        int32
	JoinMapRefCnt     int32
	ShuffleIdx        int32
	RuntimeFilterSpec *plan.RuntimeFilterSpec
	SpillThreshold    int64

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
			// build and for legacy cleanup paths that completed without a map.
			hashBuild.publishJoinMap(proc, nil)
		}
	}

	hashBuild.ctr.hashmapBuilder.Reset(proc, !mapSucceed)
	// Only clean up build files when the join map was NOT successfully sent.
	// When mapSucceed=true, hashjoin owns the files and deletes them after reading.
	if !mapSucceed {
		hashBuild.cleanupSpillFiles(proc)
	}
	hashBuild.ctr.spilledFds = nil
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
	}
	hashBuild.ctr.runtimeFilterDone = false
}
func (hashBuild *HashBuild) Free(proc *process.Process, pipelineFailed bool, err error) {
	hashBuild.ctr.terminalMu.Lock()
	defer hashBuild.ctr.terminalMu.Unlock()
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
	hashBuild.ctr.hashmapBuilder.Free(proc)
	hashBuild.ctr.freeSpillExprExecs()
	hashBuild.ctr.spillKeyVecs = nil
	hashBuild.ctr.spillHashValues = nil
	hashBuild.ctr.spillBucketRowIds = nil
	hashBuild.ctr.spillSelection = nil
}

func (hashBuild *HashBuild) publishJoinMap(proc *process.Process, jm *message.JoinMap) bool {
	if !atomic.CompareAndSwapUint32(&hashBuild.ctr.terminalPublished, 0, 1) {
		return false
	}
	message.SendJoinMapResult(
		message.NewJoinMapResult(jm),
		hashBuild.JoinMapTag,
		hashBuild.IsShuffle,
		hashBuild.ShuffleIdx,
		proc.GetMessageBoard(),
	)
	return true
}

func (hashBuild *HashBuild) publishBuildError(proc *process.Process, err error) bool {
	if !atomic.CompareAndSwapUint32(&hashBuild.ctr.terminalPublished, 0, 1) {
		return false
	}
	message.FinalizeJoinMapBuildError(
		proc.GetMessageBoard(),
		hashBuild.JoinMapTag,
		hashBuild.IsShuffle,
		hashBuild.ShuffleIdx,
		err,
	)
	return true
}

func (hashBuild *HashBuild) cleanupSpillFiles(proc *process.Process) {
	for i, fd := range hashBuild.ctr.spilledFds {
		if fd != nil {
			fd.Close()
			hashBuild.ctr.spilledFds[i] = nil
		}
	}
	hashBuild.ctr.spilledFds = nil
	// Release FD and disk charges only after the physical descriptors have
	// closed. This preserves the ledger invariant even during cancellation.
	if hashBuild.ctr.spillBundle != nil {
		hashBuild.ctr.spillBundle.release()
		hashBuild.ctr.spillBundle = nil
	}
}

// CleanCopiedBatchAt is the lifecycle hook used by bounded initial spill.
// HashBuild keeps this wrapper on the operator side so batch reservation
// ownership remains private to the hashbuild package.
func (hb *HashmapBuilder) CleanCopiedBatchAt(idx int, proc *process.Process) error {
	if idx < 0 || idx >= len(hb.Batches.Buf) {
		return process.ErrHashBuildBudgetInvalid
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
	if idx < len(hb.batchReservations) {
		if hb.batchReservations[idx] != nil {
			hb.batchReservations[idx].Release()
		}
		copy(hb.batchReservations[idx:], hb.batchReservations[idx+1:])
		hb.batchReservations = hb.batchReservations[:len(hb.batchReservations)-1]
	}
	return nil
}

func (hashBuild *HashBuild) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (ctr *container) setSpillThreshold(threshold int64) {
	ctr.spillThreshold = colexec.ResolveSpillThreshold(threshold)
}
