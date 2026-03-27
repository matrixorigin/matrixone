// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mergesort

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
)

// tmNode is a node in the per-bucket free list. It holds a pooled []TransferDestPos
// and a pointer to the next node. The next field is updated atomically so the free
// list is lock-free for the common path.
//
// Using a linked list of tmNodes (rather than sync.Pool) makes this GC-immune:
// sync.Pool is cleared on each GC in Go 1.21+, defeating the pooling.
// A plain Go heap object linked list survives GC because the GC sees
// plain [] slices with no special runtime hooks.
type tmNode struct {
	slice []api.TransferDestPos
	next  unsafe.Pointer // *tmNode
}

// tmFreeList is a lock-free stack for a given capacity bucket.
// maxFreeControls how many slices per bucket before we stop accepting more.
type tmFreeList struct {
	mu    sync.Mutex
	head  unsafe.Pointer // *tmNode (top of stack)
	count int            // rough count to bound memory usage
}

const tmMaxFree = 64 // max slices per bucket before we stop recycling

var tmFreeLists [8]tmFreeList // buckets: 1K, 2K, 4K, 8K, 16K, 32K, 64K, >64K

// bucketFor returns the free list bucket index for a given row count.
func bucketFor(rowCnt int) int {
	bucket := 0
	cap_ := 1024
	for bucket < 7 && cap_ < rowCnt {
		cap_ <<= 1
		bucket++
	}
	return bucket
}

// Push adds a []TransferDestPos to the free list stack.
// If the bucket is full (>= tmMaxFree), the slice is dropped (let GC reclaim).
// Lock-free for concurrent callers: it uses CAS on the head pointer.
func (f *tmFreeList) Push(slice []api.TransferDestPos) {
	if cap(slice) == 0 {
		return
	}
	node := &tmNode{slice: slice}

	// CAS loop: push onto stack head
	for {
		oldHead := atomic.LoadPointer(&f.head)
		node.next = oldHead
		if atomic.CompareAndSwapPointer(&f.head, oldHead, unsafe.Pointer(node)) {
			f.mu.Lock()
			f.count++
			f.mu.Unlock()
			return
		}
		// Contention; retry
	}
}

// Pop removes and returns the top slice from the free list, or (nil, false) if empty.
// Lock-free: uses CAS so concurrent Pops don't interfere with each other.
func (f *tmFreeList) Pop() ([]api.TransferDestPos, bool) {
	for {
		head := atomic.LoadPointer(&f.head)
		if head == nil {
			return nil, false
		}
		node := (*tmNode)(head)
		next := atomic.LoadPointer(&node.next)
		if atomic.CompareAndSwapPointer(&f.head, head, next) {
			f.mu.Lock()
			f.count--
			f.mu.Unlock()
			return node.slice, true
		}
		// Contention; retry
	}
}

// GetTransferMap returns a pooled []TransferDestPos sized to rowCnt.
// The returned slice has all entries initialized to NoTransfer sentinel.
// If no pooled slice is available, a fresh one is allocated with the
// bucket-standard capacity (power of 2) so that all slices in the same
// bucket share the same cap, preventing cap-mismatch leaks on Pop.
// This function is thread-safe and lock-free for the fast path.
func GetTransferMap(rowCnt int) []api.TransferDestPos {
	if rowCnt == 0 {
		return nil
	}
	bucket := bucketFor(rowCnt)
	f := &tmFreeLists[bucket]

	slice, ok := f.Pop()
	if ok && cap(slice) >= rowCnt {
		slice = slice[:rowCnt]
		for i := range slice {
			slice[i] = api.TransferDestPos{ObjIdx: api.NoTransfer}
		}
		return slice
	}
	// Bucket empty or capacity too small; allocate fresh.
	// Round up capacity to the bucket boundary so future Put/Pop
	// cycles never encounter a cap < rowCnt mismatch.
	allocCap := 1024
	for allocCap < rowCnt {
		allocCap <<= 1
	}
	s := make([]api.TransferDestPos, rowCnt, allocCap)
	for i := range s {
		s[i].ObjIdx = api.NoTransfer
	}
	return s
}

// PutTransferMap returns a TransferMap to the appropriate free list for reuse.
// Passing nil, zero-capacity, or >64K-capacity slices is a no-op.
// This function is thread-safe.
func PutTransferMap(tm []api.TransferDestPos) {
	if tm == nil {
		return
	}
	c := cap(tm)
	if c == 0 || c > 64*1024 {
		return
	}
	bucket := bucketFor(c)
	f := &tmFreeLists[bucket]

	f.mu.Lock()
	// Only pool up to tmMaxFree per bucket to bound memory usage.
	if f.count < tmMaxFree {
		// Slice the length to 0 to keep the capacity for reuse.
		tm = tm[:0]
		f.mu.Unlock()
		f.Push(tm)
	} else {
		f.mu.Unlock()
	}
}

var ErrNoMoreBlocks = moerr.NewInternalErrorNoCtx("no more blocks")

// DisposableVecPool bridge the gap between the vector pools in cn and tn
type DisposableVecPool interface {
	GetVector(*types.Type) (ret *vector.Vector, release func())
	GetMPool() *mpool.MPool
}

type MergeTaskHost interface {
	DisposableVecPool
	Name() string
	HostHintName() string
	TaskSourceNote() string
	GetCommitEntry() *api.MergeCommitEntry
	HasBigDelEvent() bool
	InitTransferMaps(blkCnt int)
	GetTransferMaps() api.TransferMaps
	PrepareNewWriter() *ioutil.BlockWriter
	DoTransfer() bool
	GetObjectCnt() int
	GetBlkCnts() []int
	GetAccBlkCnts() []int
	GetSortKeyType() types.Type
	LoadNextBatch(ctx context.Context, objIdx uint32, reuseBatch *batch.Batch) (*batch.Batch, *nulls.Nulls, func(), error)
	GetTotalSize() uint64 // total size of all objects, definitely there are cases where the size exceeds 4G, so use uint64
	GetTotalRowCnt() uint32
	GetBlockMaxRows() uint32
	GetObjectMaxBlocks() uint16
	GetTargetObjSize() uint32
}

func getSimilarBatch(bat *batch.Batch, capacity int, vpool DisposableVecPool) (*batch.Batch, func()) {
	newBat := batch.NewWithSize(len(bat.Vecs))
	newBat.Attrs = bat.Attrs
	rfs := make([]func(), len(bat.Vecs))
	releaseF := func() {
		for _, f := range rfs {
			f()
		}
	}
	for i := range bat.Vecs {
		vec, release := vpool.GetVector(bat.Vecs[i].GetType())
		if capacity > 0 {
			vec.PreExtend(capacity, vpool.GetMPool())
		}
		newBat.Vecs[i] = vec
		rfs[i] = release
	}
	return newBat, releaseF
}

func DoMergeAndWrite(
	ctx context.Context,
	txnInfo string,
	sortkeyPos int,
	mergehost MergeTaskHost,
) (err error) {
	start := time.Now()
	/*out args, keep the transfer information*/
	commitEntry := mergehost.GetCommitEntry()
	logMergeStart(
		mergehost.TaskSourceNote(),
		mergehost.Name(),
		txnInfo,
		mergehost.HostHintName(),
		commitEntry.StartTs.DebugString(),
		commitEntry.MergedObjs,
		int8(commitEntry.Level),
	)
	defer func() {
		if err != nil {
			logutil.Error(
				"[MERGE-ERROR]",
				zap.String("task", mergehost.Name()),
				zap.Error(err),
			)
		}
	}()

	if sortkeyPos >= 0 {
		if err = mergeObjs(ctx, mergehost, sortkeyPos); err != nil {
			return err
		}
	} else {
		if err = reshape(ctx, mergehost); err != nil {
			return err
		}
	}

	logMergeEnd(mergehost.Name(), start, commitEntry.CreatedObjs)
	return nil
}

// not defined in api.go to avoid import cycle

func CleanTransMapping(b api.TransferMaps) {
	for i := range b {
		PutTransferMap(b[i])
		b[i] = nil
	}
}

// AddSortPhaseMapping creates and populates the transfer slice for source block mapIdx.
// rowCnt is the number of rows in the source block.
// mapping[sortedPos] = originalPos; nil mapping means the data is already in order.
func AddSortPhaseMapping(b api.TransferMaps, mapIdx int, rowCnt int, mapping []int64) {
	if rowCnt == 0 {
		return
	}
	m := GetTransferMap(rowCnt)
	if mapping == nil {
		for i := range rowCnt {
			m[i] = api.TransferDestPos{RowIdx: uint32(i)}
		}
	} else {
		// mapping[sortedPos] = originalPos  →  m[originalPos] = {RowIdx: sortedPos}
		for sortedPos, originalPos := range mapping {
			m[uint32(originalPos)] = api.TransferDestPos{RowIdx: uint32(sortedPos)}
		}
	}
	b[mapIdx] = m
}

func UpdateMappingAfterMerge(b api.TransferMaps, mapping []int, toLayout []uint32) {
	bisectHaystack := make([]uint32, 0, len(toLayout)+1)
	bisectHaystack = append(bisectHaystack, 0)
	for _, x := range toLayout {
		bisectHaystack = append(bisectHaystack, bisectHaystack[len(bisectHaystack)-1]+x)
	}

	// given toLayout and a needle, find its corresponding block index and row index in the block
	// For example, toLayout [8192, 8192, 1024], needle = 0 -> (0, 0); needle = 8192 -> (1, 0); needle = 8193 -> (1, 1)
	bisectPinpoint := func(needle uint32) (int, uint32) {
		i, j := 0, len(bisectHaystack)
		for i < j {
			m := (i + j) / 2
			if bisectHaystack[m] > needle {
				j = m
			} else {
				i = m + 1
			}
		}
		// bisectHaystack[i] is the first number > needle, so the needle falls into i-1 th block
		blkIdx := i - 1
		rows := needle - bisectHaystack[blkIdx]
		return blkIdx, rows
	}

	var totalHandledRows uint32

	for _, m := range b {
		if len(m) == 0 {
			continue
		}
		// Count rows that entered the merge from this block (non-sentinel entries).
		// This must be computed before the update loop because totalHandledRows is
		// the cumulative offset into the merge output for the current block.
		var size uint32
		for _, pos := range m {
			if pos.ObjIdx != api.NoTransfer {
				size++
			}
		}
		for srcRow, destPos := range m {
			if destPos.ObjIdx == api.NoTransfer {
				continue // pre-deleted row, not in merge output
			}
			curTotal := totalHandledRows + destPos.RowIdx
			destTotal := mapping[curTotal]
			if destTotal == -1 {
				m[srcRow] = api.TransferDestPos{ObjIdx: api.NoTransfer}
			} else {
				destBlkIdx, destRowIdx := bisectPinpoint(uint32(destTotal))
				m[srcRow] = api.TransferDestPos{BlkIdx: uint16(destBlkIdx), RowIdx: destRowIdx}
			}
		}
		totalHandledRows += size
	}
}

func iterStatsBs(bss [][]byte) iter.Seq[*objectio.ObjectStats] {
	return func(yield func(*objectio.ObjectStats) bool) {
		for _, bs := range bss {
			stat := objectio.ObjectStats(bs)
			yield(&stat)
		}
	}
}

func logMergeStart(tasksource, name, txnInfo, host, startTS string, mergedObjs [][]byte, level int8) {
	var fromObjsDescBuilder strings.Builder
	fromSize, estSize := float64(0), float64(0)
	rows, blkn := 0, 0
	isTombstone := false
	for _, o := range mergedObjs {
		obj := objectio.ObjectStats(o)
		zm := obj.SortKeyZoneMap()
		if strings.Contains(name, "tombstone") {
			isTombstone = true
			fromObjsDescBuilder.WriteString(fmt.Sprintf("%s(%v, %s)Rows(%v),",
				obj.ObjectName().ObjectId().ShortStringEx(),
				obj.BlkCnt(),
				units.BytesSize(float64(obj.OriginSize())),
				obj.Rows()))
		} else {
			isStatement := strings.Contains(name, "statement_info")
			fromObjsDescBuilder.WriteString(fmt.Sprintf("%s(%v, %s)Rows(%v)[%v, %v],",
				obj.ObjectName().ObjectId().ShortStringEx(),
				obj.BlkCnt(),
				units.BytesSize(float64(obj.OriginSize())),
				obj.Rows(),
				cutIfByteSlice(zm.GetMin(), isStatement),
				cutIfByteSlice(zm.GetMax(), isStatement)))
		}

		fromSize += float64(obj.OriginSize())
		rows += int(obj.Rows())
		blkn += int(obj.BlkCnt())
	}

	estSize = float64(EstimateMergeSize(iterStatsBs(mergedObjs)))

	logutil.Info(
		"[MERGE-START]",
		zap.String("task", name),
		common.AnyField("txn-info", txnInfo),
		common.AnyField("host", host),
		common.AnyField("timestamp", startTS),
		zap.String("from-objs", fromObjsDescBuilder.String()),
		zap.String("from-size", units.BytesSize(fromSize)),
		zap.String("est-size", units.BytesSize(estSize)),
		zap.Int("num-obj", len(mergedObjs)),
		common.AnyField("num-blk", blkn),
		common.AnyField("rows", rows),
		common.AnyField("task-source-note", tasksource),
		common.AnyField("level", level),
	)

	if host == "TN" {
		if isTombstone {
			v2.TaskTombstoneMergeSizeCounter.Add(fromSize)
		} else {
			v2.TaskDataMergeSizeCounter.Add(fromSize)
		}
	}
}

func logMergeEnd(name string, start time.Time, objs [][]byte) {
	toObjsDesc := ""
	toSize := float64(0)
	isStatement := strings.Contains(name, "statement_info")
	for _, o := range objs {
		obj := objectio.ObjectStats(o)
		toObjsDesc += fmt.Sprintf("%s(%v, %s)Rows(%v),",
			obj.ObjectName().ObjectId().ShortStringEx(),
			obj.BlkCnt(),
			units.BytesSize(float64(obj.OriginSize())),
			obj.Rows())
		if isStatement {
			toObjsDesc += fmt.Sprintf("[%v, %v],",
				cutIfByteSlice(obj.SortKeyZoneMap().GetMin(), isStatement),
				cutIfByteSlice(obj.SortKeyZoneMap().GetMax(), isStatement))
		}
		toSize += float64(obj.OriginSize())
	}

	logutil.Info(
		"[MERGE-END]",
		zap.String("task", name),
		common.AnyField("to-objs", toObjsDesc),
		common.AnyField("to-size", units.BytesSize(toSize)),
		common.DurationField(time.Since(start)),
	)
}

func cutIfByteSlice(value any, forCompose bool) any {
	switch v := value.(type) {
	case []byte:
		if !forCompose {
			return "-"
		} else {
			t, _, _, _ := types.DecodeTuple(v)
			return t.ErrString(nil)
		}
	default:
	}
	return value
}

func EstimateMergeSize(objs iter.Seq[*objectio.ObjectStats]) int {
	estSize := 0
	totalSize := 0
	for obj := range objs {
		blkRowCnt := 8192
		if r := obj.Rows(); r == 0 {
			continue
		} else if r < 8192 {
			blkRowCnt = int(r)
		}
		// read one block, x 2 factor
		estSize += blkRowCnt * int(obj.OriginSize()/obj.Rows()) * 2
		// transfer page, 4-byte key + (4+2+1)byte value, x 1.5 factor
		estSize += int(obj.Rows()) * 30
		totalSize += int(obj.OriginSize()) / 2
	}
	estSize += totalSize / 2 * 3 // leave some margin for gc
	return estSize
}
