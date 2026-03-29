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
	"runtime"
	"strings"
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

// Transfer slab pool: two size-bucketed tiers of off-heap slabs, similar
// to the arena pool's Small/Large tiers.  Each tier uses a lock-free stack
// so get/put never block concurrent merge workers.

const (
	transferSlabSmall = 0 // ≤ 4 MB slabs
	transferSlabLarge = 1 // 4 MB – 16 MB slabs
)

const (
	// transferSlabSmallMax is the upper bound (in entries) for the small tier.
	// 4 MB / 8 bytes = 524,288 entries (~64 blocks × 8192 rows).
	transferSlabSmallMax = 4 * 1024 * 1024 / 8

	// transferSlabLargeMax is the upper bound (in entries) for the large tier.
	// 16 MB / 8 bytes = 2,097,152 entries (~256 blocks × 8192 rows).
	// Slabs exceeding this are freed immediately.
	transferSlabLargeMax = 16 * 1024 * 1024 / 8
)

type transferSlabNode struct {
	slab []api.TransferDestPos
	next unsafe.Pointer // *transferSlabNode
}

type transferSlabBucket struct {
	head    unsafe.Pointer // *transferSlabNode
	count   atomic.Int32
	maxIdle int32
	size    int // quantised slab capacity for this tier
}

var (
	transferSlabBuckets [2]transferSlabBucket
	transferSlabMPool   = mpool.MustNew("transfer-slab")
)

func init() {
	procs := int32(runtime.GOMAXPROCS(0))
	idle := procs * 2
	if idle < 4 {
		idle = 4
	}
	transferSlabBuckets[transferSlabSmall] = transferSlabBucket{
		size: transferSlabSmallMax, maxIdle: idle,
	}
	transferSlabBuckets[transferSlabLarge] = transferSlabBucket{
		size: transferSlabLargeMax, maxIdle: idle,
	}
}

// getTransferSlab returns a slab of at least size entries, with all ObjIdx
// fields set to NoTransfer.  It pops from the appropriate tier's lock-free
// stack, or allocates a fresh off-heap slab via mpool (C.calloc).
func getTransferSlab(size int) []api.TransferDestPos {
	tier := transferSlabTierFor(size)
	if tier >= 0 {
		bkt := &transferSlabBuckets[tier]
		for {
			head := atomic.LoadPointer(&bkt.head)
			if head == nil {
				break
			}
			node := (*transferSlabNode)(head)
			next := atomic.LoadPointer(&node.next)
			if atomic.CompareAndSwapPointer(&bkt.head, head, next) {
				bkt.count.Add(-1)
				slab := node.slab[:size]
				for i := range slab {
					slab[i] = api.TransferDestPos{ObjIdx: api.NoTransfer}
				}
				return slab
			}
		}
		// stack empty – allocate at the tier's quantised size
		size = bkt.size
	}
	slab, err := mpool.MakeSlice[api.TransferDestPos](size, transferSlabMPool, true)
	if err != nil {
		panic(err)
	}
	for i := range slab {
		slab[i].ObjIdx = api.NoTransfer
	}
	return slab
}

// putTransferSlab returns a slab to the appropriate tier's stack if it
// fits.  Oversized or excess slabs are freed via mpool (C.free).
func putTransferSlab(slab []api.TransferDestPos) {
	if slab == nil {
		return
	}
	tier := transferSlabTierFor(cap(slab))
	if tier < 0 {
		mpool.FreeSlice(transferSlabMPool, slab)
		return
	}
	bkt := &transferSlabBuckets[tier]
	if bkt.count.Load() >= bkt.maxIdle {
		mpool.FreeSlice(transferSlabMPool, slab)
		return
	}
	node := &transferSlabNode{slab: slab}
	for {
		oldHead := atomic.LoadPointer(&bkt.head)
		node.next = oldHead
		if atomic.CompareAndSwapPointer(&bkt.head, oldHead, unsafe.Pointer(node)) {
			bkt.count.Add(1)
			return
		}
	}
}

// DrainTransferSlabPool frees all idle slabs in every tier.
func DrainTransferSlabPool() {
	for i := range transferSlabBuckets {
		bkt := &transferSlabBuckets[i]
		for {
			head := atomic.LoadPointer(&bkt.head)
			if head == nil {
				break
			}
			if atomic.CompareAndSwapPointer(&bkt.head, head, nil) {
				bkt.count.Store(0)
				for node := (*transferSlabNode)(head); node != nil; node = (*transferSlabNode)(node.next) {
					mpool.FreeSlice(transferSlabMPool, node.slab)
				}
				break
			}
		}
	}
}

// transferSlabTierFor returns the tier index for a given entry count,
// or -1 if the slab is too large to cache.
func transferSlabTierFor(entries int) int {
	if entries <= transferSlabSmallMax {
		return transferSlabSmall
	}
	if entries <= transferSlabLargeMax {
		return transferSlabLarge
	}
	return -1
}

// GetTransferMap allocates a []TransferDestPos of the given size
// with all entries initialized to the NoTransfer sentinel.
func GetTransferMap(rowCnt int) []api.TransferDestPos {
	if rowCnt == 0 {
		return nil
	}
	s := make([]api.TransferDestPos, rowCnt)
	for i := range s {
		s[i].ObjIdx = api.NoTransfer
	}
	return s
}

var ErrNoMoreBlocks = moerr.NewInternalErrorNoCtx("no more blocks")

// TransferTable holds the block-to-block row mapping produced by a merge or flush.
// It supports two formats:
//   - Slab-based (from merger): a single contiguous allocation indexed by
//     blockIdx*Stride + rowIdx, with BlockActive tracking which blocks
//     have any transferred rows.
//   - Legacy (from flush / debug RPC): a plain api.TransferMaps.
//
// Use GetBlockMap to access a block's transfer map regardless of format.
type TransferTable struct {
	Slab        []api.TransferDestPos
	Stride      int
	BlockActive []bool

	Maps api.TransferMaps
}

// GetBlockMap returns the transfer map for block idx, or nil if the block
// was fully deleted (no rows transferred).
func (t *TransferTable) GetBlockMap(idx int) api.TransferMap {
	if t.Slab != nil {
		if !t.BlockActive[idx] {
			return nil
		}
		start := idx * t.Stride
		return t.Slab[start : start+t.Stride]
	}
	return t.Maps[idx]
}

// Len returns the total number of block slots.
func (t *TransferTable) Len() int {
	if t.Slab != nil {
		return len(t.BlockActive)
	}
	return len(t.Maps)
}

// Release nils all references and returns the slab to the pool.
func (t *TransferTable) Release() {
	putTransferSlab(t.Slab)
	t.Slab = nil
	t.BlockActive = nil
	for i := range t.Maps {
		t.Maps[i] = nil
	}
	t.Maps = nil
}

// NewTransferTableFromMaps wraps a legacy api.TransferMaps into a TransferTable.
func NewTransferTableFromMaps(maps api.TransferMaps) *TransferTable {
	return &TransferTable{Maps: maps}
}

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
	SetTransferTable(t *TransferTable)
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

// ReleaseTransferMaps nils all transfer map entries (used by flush path).
func ReleaseTransferMaps(b api.TransferMaps) {
	for i := range b {
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
