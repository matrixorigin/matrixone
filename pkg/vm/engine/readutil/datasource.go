// Copyright 2021-2024 Matrix Origin
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

package readutil

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"slices"
	"sort"
)

const (
	BatchPrefetchSize = 1000
)

// --------------------------------------------------------------------------------
//	RemoteDataSource defines and APIs
// --------------------------------------------------------------------------------

type RemoteDataSource struct {
	ctx  context.Context
	proc *process.Process

	fs fileservice.FileService
	ts types.TS

	batchPrefetchCursor int
	cursor              int
	data                engine.RelData
}

func (rs *RemoteDataSource) String() string {
	return "RemoteDataSource"
}

func (rs *RemoteDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	seqNums []uint16,
	_ int32,
	_ any,
	_ *mpool.MPool,
	_ *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {

	rs.batchPrefetch(seqNums)

	if rs.cursor >= rs.data.DataCnt() {
		return nil, engine.End, nil
	}
	rs.cursor++
	cur := rs.data.GetBlockInfo(rs.cursor - 1)
	return &cur, engine.Persisted, nil
}

func (rs *RemoteDataSource) batchPrefetch(seqNums []uint16) {
	// TODO: remove proc and don't GetService
	if rs.proc == nil {
		return
	}
	if rs.batchPrefetchCursor >= rs.data.DataCnt() ||
		rs.cursor < rs.batchPrefetchCursor {
		return
	}

	bathSize := min(BatchPrefetchSize, rs.data.DataCnt()-rs.cursor)

	begin := rs.batchPrefetchCursor
	end := begin + bathSize

	bids := make([]objectio.Blockid, end-begin)
	blks := make([]*objectio.BlockInfo, end-begin)
	for idx := begin; idx < end; idx++ {
		blk := rs.data.GetBlockInfo(idx)
		blks[idx-begin] = &blk
		bids[idx-begin] = blk.BlockID
	}

	err := ioutil.Prefetch(
		rs.proc.GetService(), rs.fs, blks[0].MetaLocation())
	if err != nil {
		logutil.Errorf("pefetch block data: %s", err.Error())
	}

	tombstoner := rs.data.GetTombstones()
	if tombstoner != nil {
		rs.data.GetTombstones().PrefetchTombstones(rs.proc.GetService(), rs.fs, bids)
	}

	rs.batchPrefetchCursor = end
}

func (rs *RemoteDataSource) Close() {
	rs.cursor = 0
}

func (rs *RemoteDataSource) applyInMemTombstones(
	bid *objectio.Blockid,
	rowsOffset []int64,
	deletedRows *objectio.Bitmap,
) (leftRows []int64) {
	tombstones := rs.data.GetTombstones()
	if tombstones == nil || !tombstones.HasAnyInMemoryTombstone() {
		return rowsOffset
	}
	return rs.data.GetTombstones().ApplyInMemTombstones(
		bid,
		rowsOffset,
		deletedRows)
}

func (rs *RemoteDataSource) applyPersistedTombstones(
	ctx context.Context,
	bid *objectio.Blockid,
	rowsOffset []int64,
	mask *objectio.Bitmap,
) (leftRows []int64, err error) {
	tombstones := rs.data.GetTombstones()
	if tombstones == nil || !tombstones.HasAnyTombstoneFile() {
		return rowsOffset, nil
	}

	return rs.data.GetTombstones().ApplyPersistedTombstones(
		ctx,
		rs.fs,
		&rs.ts,
		bid,
		rowsOffset,
		mask)
}

func (rs *RemoteDataSource) ApplyTombstones(
	ctx context.Context,
	bid *objectio.Blockid,
	rowsOffset []int64,
	applyPolicy engine.TombstoneApplyPolicy,
) (left []int64, err error) {

	slices.SortFunc(rowsOffset, func(a, b int64) int {
		return int(a - b)
	})

	left = rs.applyInMemTombstones(bid, rowsOffset, nil)

	left, err = rs.applyPersistedTombstones(ctx, bid, left, nil)
	if err != nil {
		return
	}
	return
}

func (rs *RemoteDataSource) GetTombstones(
	ctx context.Context, bid *objectio.Blockid,
) (mask objectio.Bitmap, err error) {

	mask = objectio.GetReusableBitmap()

	rs.applyInMemTombstones(bid, nil, &mask)

	if _, err = rs.applyPersistedTombstones(ctx, bid, nil, &mask); err != nil {
		mask.Release()
		return
	}

	return
}

func (rs *RemoteDataSource) SetOrderBy(_ []*plan.OrderBySpec) {

}

func (rs *RemoteDataSource) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (rs *RemoteDataSource) SetFilterZM(_ objectio.ZoneMap) {

}

func NewRemoteDataSource(
	ctx context.Context,
	fs fileservice.FileService,
	snapshotTS timestamp.Timestamp,
	relData engine.RelData,
) (source *RemoteDataSource) {
	return &RemoteDataSource{
		data: relData,
		ctx:  ctx,
		fs:   fs,
		ts:   types.TimestampToTS(snapshotTS),
	}
}

// --------------------------------------------------------------------------------
//	util functions
// --------------------------------------------------------------------------------

// FastApplyDeletesByRowIds apply deleted RowIds on the leftRows or deletedMask if leftRows is nil.
// Note that this function is stable,
// means the relative order between elements in the leftRows would not change after applies.
// Example: [3,2,5,6,1,4,1] ==> after remove the even numbers ==> [3,5,1,1]
func FastApplyDeletesByRowIds(
	debug bool,
	checkBid *objectio.Blockid,
	leftRows *[]int64,
	deletesMask *objectio.Bitmap,
	deletedRowIds []objectio.Rowid,
	IsDeletedRowIdsSorted bool,
) {
	var (
		ptr int
		hit bool
		cur types.Rowid

		lb int
		ub int

		first = true
		// liner search may have better performance if there exists a few items.
		locateBlkInterval = len(deletedRowIds) >= 5
	)

	wayA := func() {
		goFastPath := IsDeletedRowIdsSorted && locateBlkInterval

		if goFastPath {
			cur = types.NewRowid(checkBid, 0)
		}

		for _, o := range *leftRows {
			hit = false

			if goFastPath {
				cur.SetRowOffset(uint32(o))
				_, hit = sort.Find(len(deletedRowIds), func(i int) int { return cur.Compare(&deletedRowIds[i]) })
			} else {
				if first {
					first = false
					if locateBlkInterval {
						lb, ub = ioutil.FindStartEndOfBlockFromSortedRowids(deletedRowIds, checkBid)
					} else {
						lb, ub = 0, len(deletedRowIds)
					}
				}

				for i := lb; i < ub; i++ {
					b, offset := deletedRowIds[i].Decode()
					// if the blk interval located, no need to check the block id again.
					if (locateBlkInterval || b.EQ(checkBid)) && o == int64(offset) {
						hit = true
						break
					}
				}
			}

			if !hit {
				(*leftRows)[ptr] = o
				ptr++
			}
		}

		*leftRows = (*leftRows)[:ptr]
	}

	wayB := func() {
		if locateBlkInterval {
			lb, ub = ioutil.FindStartEndOfBlockFromSortedRowids(deletedRowIds, checkBid)
		} else {
			lb, ub = 0, len(deletedRowIds)
		}

		for i := lb; i < ub; i++ {
			bid, o := deletedRowIds[i].Decode()
			idx, found := sort.Find(len(*leftRows), func(x int) int { return int(int64(o) - (*leftRows)[x]) })

			// if the binary search applied, no need to check the block id again.
			if found && (locateBlkInterval || bid.EQ(checkBid)) {
				copy((*leftRows)[idx:], (*leftRows)[idx+1:])
				*leftRows = (*leftRows)[:len(*leftRows)-1]
			}

			if len(*leftRows) == 0 {
				break
			}
		}
	}

	// special cases:
	// 		wayA ==> leftRows.len = 1 and deletedRowIds.len = 1
	// 		wayB ==> leftRows.len = 8192 and deletedRowIds.len = 1
	// 		wayA ==> leftRows.len = 1 and deletedRowIds.len = 8192
	// 		wayA ==> leftRows.len = 8192 and deletedRowIds.len = 8192

	if len(*leftRows) != 0 {
		// how many items are we going to remove from the leftRows?
		if len(deletedRowIds) <= len(*leftRows)/10 {
			// expected a few removals to happen
			wayB()
		} else {
			wayA()
		}

	} else if deletesMask != nil {
		lb, ub = ioutil.FindStartEndOfBlockFromSortedRowids(deletedRowIds, checkBid)
		for i := lb; i < ub; i++ {
			_, o := deletedRowIds[i].Decode()
			deletesMask.Add(uint64(o))
		}
	}
}

// FastApplyDeletesByRowOffsets apply deleted RowIds on the leftRows or deletedMask if leftRows is nil.
// Note that this function is stable,
// means the relative order between elements in the leftRows would not change after applies.
// Example: [3,2,5,6,1,4,1] ==> after remove the even numbers ==> [3,5,1,1]
func FastApplyDeletesByRowOffsets(
	leftRows *[]int64,
	deletedMask *objectio.Bitmap,
	offsets []int64,
) {
	var (
		ptr int
		hit bool
	)

	wayA := func() {
		for _, cur := range *leftRows {
			hit = false
			for _, o := range offsets {
				if cur == o {
					hit = true
					break
				}
			}

			if !hit {
				(*leftRows)[ptr] = cur
				ptr++
			}
		}

		*leftRows = (*leftRows)[:ptr]
	}

	wayB := func() {
		for i := 0; i < len(offsets); i++ {
			idx, found := sort.Find(len(*leftRows), func(x int) int { return int(offsets[i] - (*leftRows)[x]) })

			// if the binary search applied, no need to check the block id again.
			if found {
				copy((*leftRows)[idx:], (*leftRows)[idx+1:])
				*leftRows = (*leftRows)[:len(*leftRows)-1]
			}

			if len(*leftRows) == 0 {
				break
			}
		}
	}

	if len(*leftRows) != 0 {
		// how many items are we going to remove from the leftRows?
		if len(offsets) <= len(*leftRows)/10 {
			// expected a few removals to happen
			wayB()
		} else {
			wayA()
		}

	} else if deletedMask != nil {
		for i := 0; i < len(offsets); i++ {
			deletedMask.Add(uint64(offsets[i]))
		}
	}
}

// RemoveIf removes the elements that pred is true.
func RemoveIf[T any](data []T, pred func(t T) bool) []T {
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}
