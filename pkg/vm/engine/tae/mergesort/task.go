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
	"time"

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
	estSize += totalSize / 3 * 2 // leave some margin for gc
	return estSize
}
