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
	"bufio"
	"bytes"
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
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxBatchSizeToSend = 64 * mpool.MB
const defaultCacheBatchSize = 16
const spillMergeFanIn = 32
const spillIOBufferSize = 4 * 1024 * 1024
const spillMagic = 0x12345678DEADBEEF
const spillAppendDisableThreshold = int64(16 * common.MiB)
const spillAppendTargetMin = int64(32 * common.MiB)
const spillAppendTargetMax = int64(128 * common.MiB)
const spillAppendHardCapMax = int64(256 * common.MiB)
const batchSizeCheckInterval = 64

var _ vm.Operator = new(MergeOrder)

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
	spillWriteBuf      bytes.Buffer
	spillActiveRun     *spillRun
	spillActiveWriter  *bufio.Writer
	spillActiveBytes   int64
	spillAppendEnabled bool
	spillAppendTarget  int64
	spillTailCols      []*vector.Vector
	spillTailReady     bool
	spillIncomingCols  []*vector.Vector
}

type spillRun struct {
	file       *os.File
	rowCount   int64
	batchCount int
}

type spillRunReader struct {
	file      *os.File
	reader    *bufio.Reader
	batch     *batch.Batch
	keyBatch  *batch.Batch
	orderCols []*vector.Vector
	rowIdx    int64
	heapIdx   int
}

func (mergeOrder *MergeOrder) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeOrder.cleanBatchAndCol(proc)
	ctr := &mergeOrder.ctr
	ctr.batchList = ctr.batchList[:0]
	ctr.orderCols = ctr.orderCols[:0]
	ctr.indexList = nil
	ctr.status = receiving
	ctr.cleanupSpill(proc)

	for i := range ctr.executors {
		if ctr.executors[i] != nil {
			ctr.executors[i].ResetForNextQuery()
		}
	}
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
}

func (mergeOrder *MergeOrder) Free(proc *process.Process, pipelineFailed bool, err error) {
	mergeOrder.cleanBatchAndCol(proc)
	ctr := &mergeOrder.ctr
	ctr.batchList = nil
	ctr.orderCols = nil
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

}

func (mergeOrder *MergeOrder) ExecProjection(proc *process.Process, input *batch.Batch) (*batch.Batch, error) {
	return input, nil
}

func (mergeOrder *MergeOrder) cleanBatchAndCol(proc *process.Process) {
	mp := proc.Mp()
	ctr := &mergeOrder.ctr
	for i := range ctr.batchList {
		if ctr.batchList[i] != nil {
			ctr.batchList[i].Clean(mp)
		}
	}
	for i := range ctr.orderCols {
		if ctr.orderCols[i] != nil {
			freeOrderColumns(mp, ctr.batchList[i], ctr.orderCols[i])
		}
	}
}

func (ctr *container) cleanupSpill(proc *process.Process) {
	if ctr.spillActiveWriter != nil {
		_ = ctr.spillActiveWriter.Flush()
		ctr.spillActiveWriter = nil
	}
	if ctr.spillActiveRun != nil && ctr.spillActiveRun.file != nil {
		ctr.spillActiveRun.file.Close()
		ctr.spillActiveRun.file = nil
	}
	ctr.spillActiveRun = nil
	ctr.spillActiveBytes = 0
	ctr.spillTailReady = false
	for i := range ctr.spillTailCols {
		if ctr.spillTailCols[i] != nil {
			ctr.spillTailCols[i].Free(proc.Mp())
		}
	}
	ctr.spillTailCols = nil
	ctr.spillIncomingCols = nil
	for i := range ctr.spillReaders {
		ctr.spillReaders[i].close(proc)
	}
	ctr.spillReaders = nil
	for i := range ctr.spillRuns {
		if ctr.spillRuns[i] != nil && ctr.spillRuns[i].file != nil {
			ctr.spillRuns[i].file.Close()
			ctr.spillRuns[i].file = nil
		}
	}
	ctr.spillRuns = nil
	ctr.spilling = false
	ctr.spillMemUsage = 0
	ctr.spillWriteBuf.Reset()
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
	r.reader = nil
	r.rowIdx = 0
	r.heapIdx = -1
}

func (r *spillRunReader) reset(file *os.File) {
	r.file = file
	r.rowIdx = 0
	if r.reader == nil {
		r.reader = bufio.NewReaderSize(file, spillIOBufferSize)
	} else {
		r.reader.Reset(file)
	}
}

func (r *spillRunReader) readNextBatch(proc *process.Process, ctr *container) (bool, error) {
	if r.batch != nil {
		r.batch.CleanOnlyData()
	}
	if r.keyBatch != nil {
		r.keyBatch.CleanOnlyData()
	}
	if r.batch == nil {
		r.batch = batch.NewOffHeapWithSize(0)
	}
	if r.keyBatch == nil {
		r.keyBatch = batch.NewOffHeapWithSize(0)
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
	r.rowIdx = 0
	return true, nil
}
