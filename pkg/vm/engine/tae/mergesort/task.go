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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
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
	Name() string
	HostHintName() string
	GetCommitEntry() *api.MergeCommitEntry
	InitTransferMaps(blkCnt int)
	GetTransferMaps() api.TransferMaps
	PrepareNewWriter() *blockio.BlockWriter
	DoTransfer() bool
	GetObjectCnt() int
	GetBlkCnts() []int
	GetAccBlkCnts() []int
	GetSortKeyType() types.Type
	LoadNextBatch(ctx context.Context, objIdx uint32) (*batch.Batch, *nulls.Nulls, func(), error)
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

func GetNewWriter(
	fs fileservice.FileService,
	ver uint32, seqnums []uint16,
	sortkeyPos int, sortkeyIsPK bool, isTombstone bool,
) *blockio.BlockWriter {
	name := objectio.BuildObjectNameWithObjectID(objectio.NewObjectid())
	writer, err := blockio.NewBlockWriterNew(fs, name, ver, seqnums)
	if err != nil {
		panic(err) // it is impossible
	}
	// has sortkey
	if sortkeyPos >= 0 {
		if sortkeyIsPK {
			if isTombstone {
				writer.SetPrimaryKeyWithType(
					uint16(catalog.TombstonePrimaryKeyIdx),
					index.HBF,
					index.ObjectPrefixFn,
					index.BlockPrefixFn,
				)
			} else {
				writer.SetPrimaryKey(uint16(sortkeyPos))
			}
		} else { // cluster by
			writer.SetSortKey(uint16(sortkeyPos))
		}
	}
	return writer
}

func DoMergeAndWrite(
	ctx context.Context,
	txnInfo string,
	sortkeyPos int,
	mergehost MergeTaskHost,
	isTombstone bool,
) (err error) {
	now := time.Now()
	/*out args, keep the transfer information*/
	commitEntry := mergehost.GetCommitEntry()
	fromObjsDesc := ""
	for _, o := range commitEntry.MergedObjs {
		obj := objectio.ObjectStats(o)
		fromObjsDesc = fmt.Sprintf("%s%s,", fromObjsDesc, obj.ObjectName().ObjectId().ShortStringEx())
	}
	logutil.Info(
		"[MERGE-START]",
		zap.String("task", mergehost.Name()),
		common.AnyField("txn-info", txnInfo),
		common.AnyField("host", mergehost.HostHintName()),
		common.AnyField("timestamp", commitEntry.StartTs.DebugString()),
		common.AnyField("objs", fromObjsDesc),
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

	hasSortKey := sortkeyPos >= 0
	if !hasSortKey {
		sortkeyPos = 0 // no sort key, use the first column to do reshape
	}

	if hasSortKey {
		if err = mergeObjs(ctx, mergehost, sortkeyPos, isTombstone); err != nil {
			return err
		}
	} else {
		if err = reshape(ctx, mergehost); err != nil {
			return err
		}
	}

	toObjsDesc := ""
	for _, o := range commitEntry.CreatedObjs {
		obj := objectio.ObjectStats(o)
		toObjsDesc += fmt.Sprintf("%s(%v)Rows(%v),",
			obj.ObjectName().ObjectId().ShortStringEx(),
			obj.BlkCnt(),
			obj.Rows())
	}

	logutil.Info(
		"[MERGE-END]",
		zap.String("task", mergehost.Name()),
		common.AnyField("to-objs", toObjsDesc),
		common.DurationField(time.Since(now)),
	)
	return nil
}

// not defined in api.go to avoid import cycle

func CleanTransMapping(b api.TransferMaps) {
	for i := 0; i < len(b); i++ {
		b[i] = make(api.TransferMap)
	}
}

func AddSortPhaseMapping(m api.TransferMap, rowCnt int, mapping []int64) {
	if mapping == nil {
		for i := range rowCnt {
			m[uint32(i)] = api.TransferDestPos{RowIdx: uint32(i)}
		}
		return
	}

	if len(mapping) != rowCnt {
		panic(fmt.Sprintf("mapping length %d != originRowCnt %d", len(mapping), rowCnt))
	}

	// mapping sortedVec[i] = originalVec[sortMapping[i]]
	// transpose it, sortedVec[sortMapping[i]] = originalVec[i]
	// [9 4 8 5 2 6 0 7 3 1](originalVec)  -> [6 9 4 8 1 3 5 7 2 0](sortedVec)
	// [0 1 2 3 4 5 6 7 8 9](sortedVec) -> [0 1 2 3 4 5 6 7 8 9](originalVec)
	// TODO: use a more efficient way to transpose, in place
	transposedMapping := make([]uint32, len(mapping))
	for sortedPos, originalPos := range mapping {
		transposedMapping[originalPos] = uint32(sortedPos)
	}

	for i := range rowCnt {
		m[uint32(i)] = api.TransferDestPos{RowIdx: transposedMapping[i]}
	}
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
		size := len(m)
		var curTotal uint32 // index in the flatten src array
		for srcRow := range m {
			curTotal = totalHandledRows + m[srcRow].RowIdx
			destTotal := mapping[curTotal]
			if destTotal == -1 {
				delete(m, srcRow)
			} else {
				destBlkIdx, destRowIdx := bisectPinpoint(uint32(destTotal))
				m[srcRow] = api.TransferDestPos{BlkIdx: uint16(destBlkIdx), RowIdx: destRowIdx}
			}
		}
		totalHandledRows += uint32(size)
	}
}
