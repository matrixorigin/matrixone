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

package mergeorder

import (
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxBatchSizeToSend = 64 * mpool.MB
const defaultCacheBatchSize = 16
const spillMergeFanIn = 32
const spillMagic = 0x12345678DEADBEEF
const spillAppendDisableThreshold = int64(16 * common.MiB)
const spillAppendTargetMin = int64(32 * common.MiB)
const spillAppendTargetMax = int64(128 * common.MiB)
const spillAppendHardCapMax = int64(256 * common.MiB)
const batchSizeCheckInterval = 64
const maxDrainChunkRows = 256
const maxVarlenDrainChunkRows = 32
const maxWinnerChunkRows = 64

// Bound resident batch/index metadata to two external-merge fan-in levels.
const maxResidentBatches = spillMergeFanIn * spillMergeFanIn

const (
	// Sites 32-43 and 60 are shared spillutil sites. Keep MergeOrder-specific
	// storage in a disjoint range under the same Order owner.
	mergeOrderAllocationSiteRetainedData mpool.AllocationSite = iota + 64
	mergeOrderAllocationSiteRetainedArea
	mergeOrderAllocationSiteRetainedNulls
	mergeOrderAllocationSiteRetainedGrouping
	mergeOrderAllocationSiteExpressionData
	mergeOrderAllocationSiteExpressionArea
	mergeOrderAllocationSiteExpressionNulls
	mergeOrderAllocationSiteExpressionGrouping
	mergeOrderAllocationSiteOutputData
	mergeOrderAllocationSiteOutputArea
	mergeOrderAllocationSiteOutputNulls
	mergeOrderAllocationSiteOutputGrouping
	mergeOrderAllocationSiteSpillWriteBuffer
)

var _ vm.Operator = new(MergeOrder)

var _ interface {
	SetAllocationAccount(*mpool.AllocationAccount) error
	ClearAllocationAccount(*mpool.AllocationAccount) error
} = new(MergeOrder)

const (
	receiving = iota
	normalSending
	pickUpSending
	spillSending
	finish
)

type MergeOrder struct {
	ctr container

	OrderBySpecs   []*plan.OrderBySpec
	SpillThreshold int64

	vm.OperatorBase
}

func (mergeOrder *MergeOrder) GetOperatorBase() *vm.OperatorBase {
	return &mergeOrder.OperatorBase
}

func init() {
	reuse.CreatePool[MergeOrder](
		func() *MergeOrder {
			return &MergeOrder{}
		},
		func(a *MergeOrder) {
			*a = MergeOrder{}
		},
		reuse.DefaultOptions[MergeOrder]().
			WithEnableChecker(),
	)
}

func (mergeOrder MergeOrder) TypeName() string {
	return opName
}

func NewArgument() *MergeOrder {
	return reuse.Alloc[MergeOrder](nil)
}

func (mergeOrder *MergeOrder) Release() {
	if mergeOrder != nil {
		reuse.Free[MergeOrder](mergeOrder, nil)
	}
}

type container struct {
	// operator status
	status int

	// batchList is the data structure to store the all the received batches
	batchList []*batch.Batch
	orderCols [][]*vector.Vector
	// indexList[i] = k means the number of rows before k in batchList[i] has been merged and send.
	indexList []int64

	// expression executors for order columns.
	executors []colexec.ExpressionExecutor
	compares  []compare.Compare

	buf *batch.Batch

	allocationAccount    *mpool.AllocationAccount
	retainedAllocation   *vector.AllocationAccountSelection
	expressionAllocation *vector.AllocationAccountSelection
	outputAllocation     *vector.AllocationAccountSelection
	spillAllocation      *spillutil.SpillAllocationAccount
	budget               *process.ExecutionResourceGeneration

	inMemoryHeap    *inMemoryMergeHeap
	inMemoryHeapPos []int

	// spill support
	spilling           bool
	spillThreshold     int64
	spillMemUsage      int64
	spillFS            fileservice.MutableFileService
	spillKeyIndexes    []int
	spillKeyCols       []*vector.Vector
	spillColPos        []int32
	spillRuns          []*spillRun
	spillReaders       []*spillRunReader
	spillActiveRun     *spillRun
	spillActiveWriter  spillWriteFlusher
	spillActiveBytes   int64
	spillAppendEnabled bool
	spillAppendTarget  int64
	spillTailCols      []*vector.Vector
	spillTailReady     bool
}

type spillRun struct {
	file       *os.File
	rowCount   int64
	batchCount int
	fdToken    *process.ExecutionSpillFDReservation
	diskToken  *process.ExecutionSpillDiskReservation
}

type spillWriteFlusher interface {
	io.Writer
	Flush() error
	Free()
}

type spillReader interface {
	io.Reader
	Buffered() int
	Free()
}

type spillRunReader struct {
	file        *os.File
	reader      spillReader
	fdToken     *process.ExecutionSpillFDReservation
	diskToken   *process.ExecutionSpillDiskReservation
	batch       *batch.Batch
	keyBatch    *batch.Batch
	orderCols   []*vector.Vector
	rowIdx      int64
	heapIdx     int
	fixedWidth  bool
	rowBytes    int
	avgRowBytes int
}

func (mergeOrder *MergeOrder) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeOrder.cleanBatchAndCol(proc)
	ctr := &mergeOrder.ctr
	clear(ctr.batchList)
	clear(ctr.orderCols)
	ctr.batchList = ctr.batchList[:0]
	ctr.orderCols = ctr.orderCols[:0]
	ctr.indexList = nil
	ctr.inMemoryHeap = nil
	ctr.inMemoryHeapPos = nil
	ctr.status = receiving
	ctr.cleanupSpill(proc)

	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			if ctr.allocationAccount != nil {
				ctr.executors[i].Free()
				ctr.executors[i] = nil
			} else {
				ctr.executors[i].ResetForNextQuery()
			}
		}
	}
	if ctr.allocationAccount != nil {
		ctr.executors = nil
	}
	if ctr.buf != nil {
		if ctr.buf.HasAllocationAccount() {
			// Accounted output cannot survive its statement-attempt boundary.
			ctr.buf.Clean(proc.Mp())
			ctr.buf = nil
		} else {
			ctr.buf.CleanOnlyData()
		}
	}
	ctr.budget = nil
}

func (mergeOrder *MergeOrder) Free(proc *process.Process, pipelineFailed bool, err error) {
	mergeOrder.cleanBatchAndCol(proc)
	ctr := &mergeOrder.ctr
	ctr.batchList = nil
	ctr.orderCols = nil
	ctr.indexList = nil
	ctr.inMemoryHeap = nil
	ctr.inMemoryHeapPos = nil
	ctr.cleanupSpill(proc)
	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			ctr.executors[i].Free()
		}
	}
	ctr.executors = nil

	if ctr.buf != nil {
		ctr.buf.Clean(proc.Mp())
		ctr.buf = nil
	}
	ctr.budget = nil
}

func (mergeOrder *MergeOrder) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if mergeOrder == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return mergeOrder.ctr.setAllocationAccount(account)
}

func (mergeOrder *MergeOrder) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if mergeOrder == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	return mergeOrder.ctr.clearAllocationAccount(account)
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
	if len(ctr.batchList) != 0 || len(ctr.executors) != 0 || ctr.buf != nil ||
		ctr.spillActiveRun != nil || len(ctr.spillRuns) != 0 ||
		len(ctr.spillReaders) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	retained, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerOrder,
		mergeOrderAllocationSiteRetainedData,
		mergeOrderAllocationSiteRetainedArea,
		mergeOrderAllocationSiteRetainedNulls,
		mergeOrderAllocationSiteRetainedGrouping,
	)
	if err != nil {
		return err
	}
	expression, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerOrder,
		mergeOrderAllocationSiteExpressionData,
		mergeOrderAllocationSiteExpressionArea,
		mergeOrderAllocationSiteExpressionNulls,
		mergeOrderAllocationSiteExpressionGrouping,
	)
	if err != nil {
		return err
	}
	output, err := vector.NewAllocationAccountSelection(
		account,
		mpool.AllocationOwnerOrder,
		mergeOrderAllocationSiteOutputData,
		mergeOrderAllocationSiteOutputArea,
		mergeOrderAllocationSiteOutputNulls,
		mergeOrderAllocationSiteOutputGrouping,
	)
	if err != nil {
		return err
	}
	spill, err := spillutil.NewSpillAllocationAccount(
		account,
		mpool.AllocationOwnerOrder,
	)
	if err != nil {
		return err
	}
	ctr.allocationAccount = account
	ctr.retainedAllocation = retained
	ctr.expressionAllocation = expression
	ctr.outputAllocation = output
	ctr.spillAllocation = spill
	return nil
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
	if len(ctr.batchList) != 0 || len(ctr.orderCols) != 0 ||
		len(ctr.executors) != 0 || ctr.buf != nil || ctr.spillActiveRun != nil ||
		len(ctr.spillRuns) != 0 || len(ctr.spillReaders) != 0 ||
		ctr.spillActiveWriter != nil || len(ctr.spillTailCols) != 0 {
		return mpool.ErrAllocationAccountInvariant
	}
	ctr.allocationAccount = nil
	ctr.retainedAllocation = nil
	ctr.expressionAllocation = nil
	ctr.outputAllocation = nil
	ctr.spillAllocation = nil
	ctr.budget = nil
	return nil
}

func (mergeOrder *MergeOrder) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (mergeOrder *MergeOrder) cleanBatchAndCol(proc *process.Process) {
	mp := proc.Mp()
	ctr := &mergeOrder.ctr
	for i := range ctr.batchList {
		if ctr.batchList[i] != nil && i < len(ctr.orderCols) && ctr.orderCols[i] != nil {
			freeOrderColumns(mp, ctr.batchList[i], ctr.orderCols[i])
		}
		if ctr.batchList[i] != nil {
			ctr.batchList[i].Clean(mp)
		}
	}
}

func (ctr *container) cleanupSpill(proc *process.Process) {
	// An active run has not been committed to spillRuns yet. Cleanup discards
	// it, so flushing would only perform useless I/O after cancellation/error.
	if ctr.spillActiveWriter != nil {
		ctr.spillActiveWriter.Free()
		ctr.spillActiveWriter = nil
	}
	if ctr.spillActiveRun != nil {
		ctr.spillActiveRun.close()
	}
	ctr.spillActiveRun = nil
	ctr.spillActiveBytes = 0
	ctr.clearSpillTailColumns(proc.Mp())
	for i := range ctr.spillReaders {
		ctr.spillReaders[i].close(proc)
	}
	ctr.spillReaders = nil
	closeSpillRuns(ctr.spillRuns)
	ctr.spillRuns = nil
	ctr.spilling = false
	ctr.spillMemUsage = 0
	ctr.spillFS = nil
	clear(ctr.spillKeyCols)
	ctr.spillKeyCols = nil
}

func closeSpillRuns(runs []*spillRun) {
	for i := range runs {
		if runs[i] != nil {
			runs[i].close()
		}
	}
}

func (r *spillRun) close() {
	if r == nil {
		return
	}
	if r.file != nil {
		_ = r.file.Close()
		r.file = nil
	}
	if r.diskToken != nil {
		r.diskToken.Release()
		r.diskToken = nil
	}
	if r.fdToken != nil {
		r.fdToken.Release()
		r.fdToken = nil
	}
}

func (ctr *container) setSpillThreshold(threshold int64) {
	if threshold == 0 {
		fileCacheMem := fileservice.GlobalMemoryCacheSizeHint.Load()
		mem := (int64(system.MemoryTotal()) - fileCacheMem) / int64(system.GoMaxProcs()) / 8
		if mem < common.MiB*128 {
			mem = common.MiB * 128
		}
		ctr.spillThreshold = mem
	} else {
		ctr.spillThreshold = threshold
	}
	ctr.setSpillAppendPolicy()
}

func (ctr *container) setSpillAppendPolicy() {
	ctr.spillAppendEnabled = false
	ctr.spillAppendTarget = 0
	if ctr.spillThreshold <= spillAppendDisableThreshold {
		return
	}

	hardCap := ctr.spillThreshold
	if hardCap > spillAppendHardCapMax {
		hardCap = spillAppendHardCapMax
	}
	target := ctr.spillThreshold / 4
	if target < spillAppendTargetMin {
		target = spillAppendTargetMin
	}
	if target > spillAppendTargetMax {
		target = spillAppendTargetMax
	}
	if target > hardCap {
		target = hardCap
	}
	ctr.spillAppendEnabled = true
	ctr.spillAppendTarget = target
}

func freeOrderColumns(mp *mpool.MPool, bat *batch.Batch, cols []*vector.Vector) {
	if len(cols) == 0 {
		return
	}
	for _, vec := range cols {
		if vec == nil {
			continue
		}
		if batchContainsVector(bat, vec) {
			continue
		}
		vec.Free(mp)
	}
}

func batchContainsVector(bat *batch.Batch, vec *vector.Vector) bool {
	for _, batVec := range bat.Vecs {
		if batVec == vec {
			return true
		}
	}
	return false
}

func (r *spillRunReader) close(proc *process.Process) {
	if r == nil {
		return
	}
	if r.batch != nil {
		r.batch.Clean(proc.Mp())
		r.batch = nil
	}
	if r.keyBatch != nil {
		r.keyBatch.Clean(proc.Mp())
		r.keyBatch = nil
	}
	r.orderCols = nil
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
	if r.reader != nil {
		r.reader.Free()
	}
	if r.diskToken != nil {
		r.diskToken.Release()
		r.diskToken = nil
	}
	if r.fdToken != nil {
		r.fdToken.Release()
		r.fdToken = nil
	}
	r.reader = nil
	r.rowIdx = 0
	r.heapIdx = -1
	r.fixedWidth = false
	r.rowBytes = 0
	r.avgRowBytes = 0
}

func (r *spillRunReader) reset(
	proc *process.Process,
	ctr *container,
	file *os.File,
) error {
	if r.reader != nil {
		r.reader.Free()
	}
	r.file = file
	r.rowIdx = 0
	reader, err := spillutil.NewAccountedFileReader(
		proc.Mp(),
		ctr.spillAllocation,
		file,
	)
	if err != nil {
		return err
	}
	r.reader = reader
	return nil
}

func (r *spillRunReader) refreshDrainProfile() {
	r.fixedWidth = true
	r.rowBytes = 0
	for _, vec := range r.batch.Vecs {
		typ := vec.GetType()
		if typ.IsVarlen() {
			r.fixedWidth = false
			r.rowBytes = 0
			break
		}
		r.rowBytes += typ.TypeSize()
	}
	if r.fixedWidth {
		if r.rowBytes < 1 {
			r.rowBytes = 1
		}
		r.avgRowBytes = r.rowBytes
		return
	}

	avg := r.batch.Size() / max(1, r.batch.RowCount())
	if avg < 1 {
		avg = 1
	}
	r.avgRowBytes = avg
}

func (r *spillRunReader) readNextBatch(proc *process.Process, ctr *container) (bool, error) {
	// A serialized spill batch is the reader's bounded I/O work unit. Check
	// before releasing the current batch contents so a known cancellation does
	// not consume or publish the next unit.
	if err, canceled := vm.CancelCheck(proc); canceled {
		return false, err
	}

	if r.batch != nil {
		r.batch.CleanOnlyData()
	}
	if r.keyBatch != nil {
		r.keyBatch.CleanOnlyData()
	}
	if r.batch == nil {
		r.batch = batch.NewOffHeapWithSize(0)
		if ctr.spillAllocation != nil {
			if err := ctr.spillAllocation.ConfigureDecodedBatch(r.batch); err != nil {
				return false, err
			}
		}
	}
	if r.keyBatch == nil {
		r.keyBatch = batch.NewOffHeapWithSize(0)
		if ctr.spillAllocation != nil {
			if err := ctr.spillAllocation.ConfigureDecodedBatch(r.keyBatch); err != nil {
				return false, err
			}
		}
	}

	bat, keyBatch, err := readSpillBatches(proc, r.reader, r.batch, r.keyBatch)
	if err != nil {
		if err == io.EOF {
			return false, nil
		}
		return false, err
	}
	r.batch = bat
	r.keyBatch = keyBatch
	r.orderCols, err = ctr.restoreSpillOrderColumns(proc, r.batch, r.keyBatch, r.orderCols)
	if err != nil {
		return false, err
	}
	r.refreshDrainProfile()
	r.rowIdx = 0
	// The synchronous read/unmarshal cannot be interrupted. Observe cancellation
	// once it completes, before the refilled batch becomes visible to heap/copy
	// work in the caller.
	if err, canceled := vm.CancelCheck(proc); canceled {
		return false, err
	}
	return true, nil
}
