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
	"slices"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
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

	orderBy []*plan.OrderBySpec
	// The permutation belongs to this reader; never reorder shared RelData.
	orderedBlocks []orderedRemoteBlock
	filterZM      objectio.ZoneMap
}

type orderedRemoteBlock struct {
	index int
	zm    objectio.ZoneMap
}

// UsableOrderZoneMap reports whether a block bound can safely participate in
// ordered pruning. Missing or incompatible metadata must leave the block in
// the scan. A truncated string maximum can be smaller than an actual value.
func UsableOrderZoneMap(zm objectio.ZoneMap, typ plan.Type, desc bool) bool {
	if !zm.IsInited() || int32(zm.GetType()) != typ.Id || (desc && zm.MaxTruncated()) {
		return false
	}
	if zm.GetType() == types.T_decimal64 || zm.GetType() == types.T_decimal128 {
		return zm.GetScale() == int32(uint8(typ.Scale)&0x3f)
	}
	return true
}

func (rs *RemoteDataSource) String() string {
	return "RemoteDataSource"
}

func (rs *RemoteDataSource) Next(
	ctx context.Context,
	cols []string,
	colTypes []types.Type,
	seqNums []uint16,
	_ int32,
	_ any,
	_ *mpool.MPool,
	_ *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {

	if err := ctx.Err(); err != nil {
		return nil, engine.End, err
	}
	if len(rs.orderBy) > 0 && rs.orderedBlocks == nil {
		if err := rs.orderBlocks(ctx, cols, colTypes, seqNums); err != nil {
			return nil, engine.End, err
		}
	}
	for rs.cursor < rs.data.DataCnt() {
		if rs.orderedBlocks == nil || !rs.filterZM.IsInited() {
			break
		}
		zm := rs.orderedBlocks[rs.cursor].zm
		if !zm.IsInited() ||
			(rs.descending() && rs.filterZM.CompareMax(zm) <= 0) ||
			(!rs.descending() && rs.filterZM.CompareMin(zm) >= 0) {
			break
		}
		rs.cursor++
	}
	if rs.cursor >= rs.data.DataCnt() {
		return nil, engine.End, nil
	}
	rs.batchPrefetch(ctx, seqNums)
	cur := rs.blockAt(rs.cursor)
	rs.cursor++
	return &cur, engine.Persisted, nil
}

func (rs *RemoteDataSource) blockAt(i int) objectio.BlockInfo {
	if rs.orderedBlocks != nil {
		i = rs.orderedBlocks[i].index
	}
	return rs.data.GetBlockInfo(i)
}

func (rs *RemoteDataSource) batchPrefetch(ctx context.Context, seqNums []uint16) {
	// TODO: remove proc and don't GetService
	if rs.proc == nil {
		return
	}
	if rs.batchPrefetchCursor >= rs.data.DataCnt() ||
		rs.cursor < rs.batchPrefetchCursor {
		return
	}

	bathSize := min(BatchPrefetchSize, rs.data.DataCnt()-rs.cursor)

	begin := rs.cursor
	end := begin + bathSize

	bids := make([]objectio.Blockid, end-begin)
	blks := make([]*objectio.BlockInfo, end-begin)
	for idx := begin; idx < end; idx++ {
		blk := rs.blockAt(idx)
		blks[idx-begin] = &blk
		bids[idx-begin] = blk.BlockID
	}

	if !ioutil.PrefetchSkipped(ctx, rs.fs, fileservice.GetFileServicePolicy(ctx), blks[0]) {
		err := ioutil.Prefetch(
			rs.proc.GetService(), rs.fs, blks[0].MetaLocation())
		if err != nil {
			logutil.Errorf("pefetch block data: %s", err.Error())
		}
	}

	tombstoner := rs.data.GetTombstones()
	if tombstoner != nil {
		rs.data.GetTombstones().PrefetchTombstones(rs.ctx, rs.proc.GetService(), rs.fs, bids)
	}

	rs.batchPrefetchCursor = end
}

func (rs *RemoteDataSource) Close() {
	rs.cursor = 0
	rs.batchPrefetchCursor = 0
	rs.orderedBlocks = nil
	rs.filterZM = nil
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

func (rs *RemoteDataSource) SetOrderBy(orderBy []*plan.OrderBySpec) {
	// Zonemaps alone cannot exclude a block containing a better NULL key.
	rs.orderBy = nil
	if len(orderBy) > 0 && orderBy[0] != nil && orderBy[0].Expr != nil &&
		orderBy[0].Expr.GetCol() != nil && orderBy[0].Expr.Typ.NotNullable {
		rs.orderBy = orderBy
	}
}

func (rs *RemoteDataSource) GetOrderBy() []*plan.OrderBySpec {
	return rs.orderBy
}

func (rs *RemoteDataSource) descending() bool {
	return rs.orderBy[0].Flag&plan.OrderBySpec_DESC != 0
}

func (rs *RemoteDataSource) SetFilterZM(zm objectio.ZoneMap) {
	if len(rs.orderBy) == 0 || !UsableOrderZoneMap(zm, rs.orderBy[0].Expr.Typ, rs.descending()) {
		return
	}
	if !rs.filterZM.IsInited() ||
		(rs.descending() && rs.filterZM.CompareMax(zm) < 0) ||
		(!rs.descending() && rs.filterZM.CompareMin(zm) > 0) {
		rs.filterZM = zm.Clone()
	}
}

func (rs *RemoteDataSource) orderBlocks(ctx context.Context, cols []string, colTypes []types.Type, seqNums []uint16) error {
	// A literal dotted column can alias another column's qualified name.
	// Without an unambiguous mapping, never use either column's zonemap.
	if slices.ContainsFunc(cols, func(name string) bool { return strings.Contains(name, ".") }) {
		rs.orderBy = nil
		rs.filterZM = nil
		return nil
	}
	orderExpr := rs.orderBy[0].Expr
	name := strings.ToLower(orderExpr.GetCol().Name)
	// ColPos is relative to the compiled projection, not the storage schema.
	// Match the schema's lower-case normalization, not broader Unicode folding.
	col := slices.IndexFunc(cols, func(candidate string) bool { return strings.ToLower(candidate) == name })
	if col < 0 {
		name = name[strings.LastIndex(name, ".")+1:]
		col = slices.IndexFunc(cols, func(candidate string) bool { return strings.ToLower(candidate) == name })
	}
	if col < 0 || col >= len(seqNums) || col >= len(colTypes) || int32(colTypes[col].Oid) != orderExpr.Typ.Id {
		rs.orderBy = nil
		rs.filterZM = nil
		return nil
	}

	blocks := make([]orderedRemoteBlock, rs.data.DataCnt())
	var meta objectio.ObjectDataMeta
	for i := range blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		blk := rs.data.GetBlockInfo(i)
		blocks[i].index = i
		if blk.IsAppendable() {
			continue
		}
		location := blk.MetaLocation()
		if !objectio.IsSameObjectLocVsMeta(location, meta) {
			loaded, err := objectio.FastLoadObjectMeta(ctx, &location, false, rs.fs)
			if err != nil {
				return err
			}
			meta = loaded.MustDataMeta()
		}
		blockMeta := meta.GetBlockMeta(uint32(location.ID()))
		if seqNums[col] <= blockMeta.GetMaxSeqnum() {
			zm := blockMeta.ColumnMeta(seqNums[col]).ZoneMap()
			if UsableOrderZoneMap(zm, orderExpr.Typ, rs.descending()) {
				blocks[i].zm = zm
			}
		}
	}
	slices.SortFunc(blocks, func(a, b orderedRemoteBlock) int {
		ai, bi := a.zm.IsInited(), b.zm.IsInited()
		if !ai && !bi {
			return 0
		}
		if !ai {
			return -1
		}
		if !bi {
			return 1
		}
		if rs.descending() {
			return b.zm.CompareMax(a.zm)
		}
		return a.zm.CompareMin(b.zm)
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	rs.orderedBlocks = blocks
	return nil
}

func NewRemoteDataSource(
	ctx context.Context,
	fs fileservice.FileService,
	snapshotTS timestamp.Timestamp,
	relData engine.RelData,
) (source *RemoteDataSource) {
	return &RemoteDataSource{
		data: stripFirstEmptyBlock(relData),
		ctx:  ctx,
		fs:   fs,
		ts:   types.TimestampToTS(snapshotTS),
	}
}

func stripFirstEmptyBlock(relData engine.RelData) engine.RelData {
	if relData == nil {
		return relData
	}
	switch rd := relData.(type) {
	case *ObjListRelData:
		if rd == nil {
			return nil
		}
		rd.expand()
		relData = &rd.blocklistRelData
	case *BlockListRelData:
		if rd == nil {
			return nil
		}
		relData = rd
	default:
		return relData
	}
	if relData.DataCnt() == 0 {
		return relData
	}
	blk := relData.GetBlockInfo(0)
	if blk.IsMemBlk() {
		return relData.DataSlice(1, relData.DataCnt())
	}
	return relData
}

// --------------------------------------------------------------------------------
//	util functions
// --------------------------------------------------------------------------------

// FastApplyDeletesByRowIds apply deleted RowIds on the leftRows or deletedMask if leftRows is nil.
// Note that this function is stable,
// means the relative order between elements in the leftRows would not change after applies.
// Example: [3,2,5,6,1,4,1] ==> after remove the even numbers ==> [3,5,1,1]
func FastApplyDeletesByRowIds(
	checkBid *objectio.Blockid,
	leftRows *[]int64,
	deletesMask *objectio.Bitmap,
	deletedRowIds []objectio.Rowid,
	isDeletedRowIdsSorted bool,
) {

	if isDeletedRowIdsSorted {
		panicIfRowIdsUnsortedIfRaceDetectorEnabled(deletedRowIds)
	}
	if len(deletedRowIds) >= 32 && isDeletedRowIdsSorted && len(*leftRows) == 1 {
		// Catalog point reads can revisit a block with many transaction-local
		// deletes. Avoid scanning the whole sorted delete list for one row.
		rowID := types.NewRowid(checkBid, uint32((*leftRows)[0]))
		_, found := sort.Find(len(deletedRowIds), func(i int) int {
			return rowID.Compare(&deletedRowIds[i])
		})
		if found {
			*leftRows = (*leftRows)[:0]
		}
		return
	}

	var (
		ptr int
		cur types.Rowid
	)

	wayA := func() {
		n := len(deletedRowIds)
		m := len(*leftRows)

		updateOffset := func(idx int) {
			if idx < m {
				cur.SetRowOffset(uint32((*leftRows)[idx]))
			}
		}

		i, j := 0, 0
		cur = types.NewRowid(checkBid, uint32((*leftRows)[0]))

		for i < m && j < n {
			if cur.GT(&deletedRowIds[j]) {
				j++
				continue
			}

			if cur.LT(&deletedRowIds[j]) {
				(*leftRows)[ptr] = (*leftRows)[i]
				i++
				ptr++
				updateOffset(i)
				continue
			}

			// equal
			i++
			j++

			updateOffset(i)
		}

		for i < len(*leftRows) {
			(*leftRows)[ptr] = (*leftRows)[i]
			i++
			ptr++
		}

		*leftRows = (*leftRows)[:ptr]
	}

	wayB := func() {
		for i := 0; i < len(deletedRowIds); i++ {
			bid, o := deletedRowIds[i].Decode()
			idx, found := sort.Find(len(*leftRows), func(x int) int { return int(int64(o) - (*leftRows)[x]) })

			if found && bid.EQ(checkBid) {
				copy((*leftRows)[idx:], (*leftRows)[idx+1:])
				*leftRows = (*leftRows)[:len(*leftRows)-1]
			}

			if len(*leftRows) == 0 {
				break
			}
		}
	}

	if len(*leftRows) != 0 {
		if isDeletedRowIdsSorted {
			wayA()
		} else {
			wayB()
		}
	} else if deletesMask != nil {
		if isDeletedRowIdsSorted {
			s, e := ioutil.FindStartEndOfBlockFromSortedRowids(deletedRowIds, checkBid)
			for i := s; i < e; i++ {
				bid, o := deletedRowIds[i].Decode()
				if bid.EQ(checkBid) {
					deletesMask.Add(uint64(o))
				}
			}
		} else {
			for i := 0; i < len(deletedRowIds); i++ {
				bid, o := deletedRowIds[i].Decode()
				if bid.EQ(checkBid) {
					deletesMask.Add(uint64(o))
				}
			}
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
	return commonutil.RemoveIf(data, pred)
}
