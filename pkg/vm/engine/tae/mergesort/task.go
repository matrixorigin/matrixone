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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
)

var ErrNoMoreBlocks = moerr.NewInternalErrorNoCtx("no more blocks")

// DisposableVecPool bridge the gap between the vector pools in cn and tn
type DisposableVecPool interface {
	GetVector(*types.Type) (ret *vector.Vector, release func())
	GetMPool() *mpool.MPool
}

type MergeTaskHost interface {
	DisposableVecPool
	HostHintName() string
	PrepareData(context.Context) ([]*batch.Batch, []*nulls.Nulls, func(), error)
	GetCommitEntry() *api.MergeCommitEntry
	PrepareNewWriter() *blockio.BlockWriter
	DoTransfer() bool
	GetObjectCnt() int
	GetBlkCnts() []int
	GetAccBlkCnts() []int
	GetSortKeyType() types.Type
	LoadNextBatch(ctx context.Context, objIdx uint32) (*batch.Batch, *nulls.Nulls, func(), error)
	GetTotalSize() uint32
	GetTotalRowCnt() uint32
	GetBlockMaxRows() uint32
	GetObjectMaxBlocks() uint16
	GetTargetObjSize() uint32
}

func initTransferMapping(e *api.MergeCommitEntry, blkcnt int) {
	e.Booking = NewBlkTransferBooking(blkcnt)
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

func GetNewWriter(
	fs fileservice.FileService,
	ver uint32, seqnums []uint16,
	sortkeyPos int, sortkeyIsPK bool,
) *blockio.BlockWriter {
	name := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err := blockio.NewBlockWriterNew(fs, name, ver, seqnums)
	if err != nil {
		panic(err) // it is impossible
	}
	// has sortkey
	if sortkeyPos >= 0 {
		if sortkeyIsPK {
			writer.SetPrimaryKey(uint16(sortkeyPos))
		} else { // cluster by
			writer.SetSortKey(uint16(sortkeyPos))
		}
	}
	return writer
}

func DoMergeAndWrite(
	ctx context.Context,
	sortkeyPos int,
	blkMaxRow int,
	mergehost MergeTaskHost,
) (err error) {
	now := time.Now()
	/*out args, keep the transfer infomation*/
	commitEntry := mergehost.GetCommitEntry()
	fromObjsDesc := ""
	for _, o := range commitEntry.MergedObjs {
		obj := objectio.ObjectStats(o)
		fromObjsDesc = fmt.Sprintf("%s%s,", fromObjsDesc, common.ShortObjId(*obj.ObjectName().ObjectId()))
	}
	tableDesc := fmt.Sprintf("%v-%v", commitEntry.TblId, commitEntry.TableName)
	logutil.Info("[Start] Mergeblocks",
		zap.String("table", tableDesc),
		zap.String("on", mergehost.HostHintName()),
		zap.String("txn-start-ts", commitEntry.StartTs.DebugString()),
		zap.String("from-objs", fromObjsDesc),
	)
	phaseDesc := "prepare data"
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr] Mergeblocks",
				zap.String("table", tableDesc),
				zap.Error(err),
				zap.String("phase", phaseDesc),
			)
		}
	}()

	hasSortKey := sortkeyPos >= 0
	if !hasSortKey {
		sortkeyPos = 0 // no sort key, use the first column to do reshape
	}

	if hasSortKey {
		if err := mergeObjs(ctx, mergehost, sortkeyPos); err != nil {
			return err
		}

		toObjsDesc := ""
		for _, o := range commitEntry.CreatedObjs {
			obj := objectio.ObjectStats(o)
			toObjsDesc += fmt.Sprintf("%s(%v)Rows(%v),",
				common.ShortObjId(*obj.ObjectName().ObjectId()),
				obj.BlkCnt(),
				obj.Rows())
		}

		logutil.Info("[Done] Mergeblocks",
			zap.String("table", tableDesc),
			zap.String("on", mergehost.HostHintName()),
			zap.String("txn-start-ts", commitEntry.StartTs.DebugString()),
			zap.String("to-objs", toObjsDesc),
			common.DurationField(time.Since(now)))
		return
	}

	// batches is read from disk, dels is read from disk and memory
	//
	// batches[i] means the i-th non-appendable block to be merged and
	// it has no rowid
	batches, dels, release, err := mergehost.PrepareData(ctx)
	if err != nil {
		return err
	}
	defer release()

	if mergehost.DoTransfer() {
		initTransferMapping(commitEntry, len(batches))
	}

	fromLayout := make([]uint32, len(batches))
	totalRowCount := 0

	mpool := mergehost.GetMPool()
	// iter all block to get basic info, do shrink if needed
	for i := range batches {
		rowCntBeforeApplyDelete := batches[i].RowCount()
		del := dels[i]
		if del != nil && del.Count() > 0 {
			// dup vector before apply delete. old b will be freed in releaseF
			newb, err := batches[i].Dup(mpool)
			if err != nil {
				return err
			}
			defer newb.Clean(mpool) // whoever create new vector, should clean it
			batches[i] = newb
			batches[i].Shrink(del.ToI64Arrary(), true)
			// skip empty batch
			if batches[i].RowCount() == 0 {
				continue
			}
		}
		if mergehost.DoTransfer() {
			AddSortPhaseMapping(commitEntry.Booking, i, rowCntBeforeApplyDelete, del, nil)
		}
		fromLayout[i] = uint32(batches[i].RowCount())
		totalRowCount += batches[i].RowCount()
	}

	if totalRowCount == 0 {
		logutil.Info("[Done] Mergeblocks due to all deleted",
			zap.String("table", tableDesc),
			zap.String("txn-start-ts", commitEntry.StartTs.DebugString()))
		if mergehost.DoTransfer() {
			CleanTransMapping(commitEntry.Booking)
		}
		return
	}

	// -------------------------- phase 1
	phaseDesc = "reshape, one column"
	toLayout := arrangeToLayout(totalRowCount, blkMaxRow)

	retBatches, releaseF := ReshapeBatches(batches, fromLayout, toLayout, mergehost)
	defer releaseF()
	if mergehost.DoTransfer() {
		UpdateMappingAfterMerge(commitEntry.Booking, nil, toLayout)
	}

	// -------------------------- phase 2
	phaseDesc = "new writer to write down"
	writer := mergehost.PrepareNewWriter()
	for _, bat := range retBatches {
		_, err = writer.WriteBatch(bat)
		if err != nil {
			return err
		}
	}

	if _, _, err = writer.Sync(ctx); err != nil {
		return err
	}

	// no tomestone actually
	cobjstats := writer.GetObjectStats()[:objectio.SchemaTombstone]
	for _, cobj := range cobjstats {
		commitEntry.CreatedObjs = append(commitEntry.CreatedObjs, cobj.Clone().Marshal())
	}
	cobj := fmt.Sprintf("%s(%v)Rows(%v)",
		common.ShortObjId(*cobjstats[0].ObjectName().ObjectId()),
		cobjstats[0].BlkCnt(),
		cobjstats[0].Rows())
	logutil.Info("[Done] Mergeblocks",
		zap.String("table", tableDesc),
		zap.String("on", mergehost.HostHintName()),
		zap.String("txn-start-ts", commitEntry.StartTs.DebugString()),
		zap.String("to-objs", cobj),
		common.DurationField(time.Since(now)))

	return nil

}

// layout [blkMaxRow, blkMaxRow, blkMaxRow,..., blkMaxRow, totalRowCount - blkMaxRow*N]
func arrangeToLayout(totalRowCount int, blkMaxRow int) []uint32 {
	toLayout := make([]uint32, 0, totalRowCount/blkMaxRow)
	unconsumed := totalRowCount
	for unconsumed > 0 {
		if unconsumed > blkMaxRow {
			toLayout = append(toLayout, uint32(blkMaxRow))
			unconsumed -= blkMaxRow
		} else {
			toLayout = append(toLayout, uint32(unconsumed))
			unconsumed = 0
		}
	}
	return toLayout
}

// not defined in api.go to avoid import cycle

func NewBlkTransferBooking(size int) *api.BlkTransferBooking {
	mappings := make([]api.BlkTransMap, size)
	for i := 0; i < size; i++ {
		mappings[i] = api.BlkTransMap{
			M: make(map[int32]api.TransDestPos),
		}
	}
	return &api.BlkTransferBooking{
		Mappings: mappings,
	}
}

func CleanTransMapping(b *api.BlkTransferBooking) {
	for i := 0; i < len(b.Mappings); i++ {
		b.Mappings[i] = api.BlkTransMap{
			M: make(map[int32]api.TransDestPos),
		}
	}
}

func AddSortPhaseMapping(b *api.BlkTransferBooking, idx int, originRowCnt int, deletes *nulls.Nulls, mapping []int64) {
	// TODO: remove panic check
	if mapping != nil {
		deletecnt := 0
		if deletes != nil {
			deletecnt = deletes.GetCardinality()
		}
		if len(mapping) != originRowCnt-deletecnt {
			panic(fmt.Sprintf("mapping length %d != originRowCnt %d - deletes %s", len(mapping), originRowCnt, deletes))
		}
		// mapping sortedVec[i] = originalVec[sortMapping[i]]
		// transpose it, originalVec[sortMapping[i]] = sortedVec[i]
		// [9 4 8 5 2 6 0 7 3 1](orignVec)  -> [6 9 4 8 1 3 5 7 2 0](sortedVec)
		// [0 1 2 3 4 5 6 7 8 9](sortedVec) -> [0 1 2 3 4 5 6 7 8 9](originalVec)
		// TODO: use a more efficient way to transpose, in place
		transposedMapping := make([]int64, len(mapping))
		for sortedPos, originalPos := range mapping {
			transposedMapping[originalPos] = int64(sortedPos)
		}
		mapping = transposedMapping
	}
	posInVecApplyDeletes := 0
	targetMapping := b.Mappings[idx].M
	for origRow := 0; origRow < originRowCnt; origRow++ {
		if deletes != nil && deletes.Contains(uint64(origRow)) {
			// this row has been deleted, skip its mapping
			continue
		}
		if mapping == nil {
			// no sort phase, the mapping is 1:1, just use posInVecApplyDeletes
			targetMapping[int32(origRow)] = api.TransDestPos{BlkIdx: -1, RowIdx: int32(posInVecApplyDeletes)}
		} else {
			targetMapping[int32(origRow)] = api.TransDestPos{BlkIdx: -1, RowIdx: int32(mapping[posInVecApplyDeletes])}
		}
		posInVecApplyDeletes++
	}
}

func UpdateMappingAfterMerge(b *api.BlkTransferBooking, mapping, toLayout []uint32) {
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

	var totalHandledRows int32

	for _, mcontainer := range b.Mappings {
		m := mcontainer.M
		var curTotal int32   // index in the flatten src array
		var destTotal uint32 // index in the flatten merged array
		for srcRow := range m {
			curTotal = totalHandledRows + m[srcRow].RowIdx
			if mapping == nil {
				destTotal = uint32(curTotal)
			} else {
				destTotal = mapping[curTotal]
			}
			destBlkIdx, destRowIdx := bisectPinpoint(destTotal)
			m[srcRow] = api.TransDestPos{BlkIdx: int32(destBlkIdx), RowIdx: int32(destRowIdx)}
		}
		totalHandledRows += int32(len(m))
	}
}
