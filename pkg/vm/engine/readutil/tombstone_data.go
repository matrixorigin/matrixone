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
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func UnmarshalTombstoneData(data []byte) (engine.Tombstoner, error) {
	typ := engine.TombstoneType(data[0])
	switch typ {
	case engine.TombstoneData:
		tomb := new(tombstoneData)
		if err := tomb.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return tomb, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("unsupported tombstone type")
	}
}

func NewEmptyTombstoneData() *tombstoneData {
	return new(tombstoneData)
}

// NewBlockScopedTombstoneData returns tombstone data that retains only deletes
// which may affect one of blocks. Row-id deletes are filtered exactly. Object
// tombstones use their Rowid zone map and therefore deliberately retain
// conservative false positives.
func NewBlockScopedTombstoneData(blocks []objectio.Blockid) *tombstoneData {
	return &tombstoneData{blockFilter: newBlockIDFilter(blocks)}
}

type tombstoneData struct {
	rowids      []types.Rowid
	files       objectio.ObjectStatsSlice
	blockFilter *blockIDFilter
}

type blockIDFilter struct {
	blocks []objectio.Blockid
}

func newBlockIDFilter(blocks []objectio.Blockid) *blockIDFilter {
	filter := &blockIDFilter{blocks: slices.Clone(blocks)}
	slices.SortFunc(filter.blocks, func(a, b objectio.Blockid) int {
		return a.Compare(&b)
	})
	filter.blocks = slices.CompactFunc(filter.blocks, func(a, b objectio.Blockid) bool {
		return a.EQ(&b)
	})
	return filter
}

func (filter *blockIDFilter) contains(block *objectio.Blockid) bool {
	if filter == nil {
		return true
	}
	pos := sort.Search(len(filter.blocks), func(pos int) bool {
		return !filter.blocks[pos].LT(block)
	})
	return pos < len(filter.blocks) && filter.blocks[pos].EQ(block)
}

// mayContainZoneMap fails open for legacy or malformed metadata. For a valid
// Rowid zone map it finds the first selected block at or above the lower bound
// and checks whether that block is still below the upper bound.
func (filter *blockIDFilter) mayContainZoneMap(zm objectio.ZoneMap) bool {
	if filter == nil {
		return true
	}
	if len(filter.blocks) == 0 {
		return false
	}
	if !zm.IsInited() || zm.GetType() != types.T_Rowid {
		return true
	}
	minBuf, maxBuf := zm.GetMinBuf(), zm.GetMaxBuf()
	if len(minBuf) != types.RowidSize || len(maxBuf) != types.RowidSize {
		return true
	}
	var minRow, maxRow types.Rowid
	copy(minRow[:], minBuf)
	copy(maxRow[:], maxBuf)
	minBlock, maxBlock := minRow.CloneBlockID(), maxRow.CloneBlockID()
	if minBlock.GT(&maxBlock) {
		return true
	}
	pos := sort.Search(len(filter.blocks), func(pos int) bool {
		return !filter.blocks[pos].LT(&minBlock)
	})
	return pos < len(filter.blocks) && !filter.blocks[pos].GT(&maxBlock)
}

func (tomb *tombstoneData) MarshalBinaryWithBuffer(buf *bytes.Buffer) (err error) {
	buf.Grow(1 + 4*2 + len(tomb.rowids)*types.RowidSize + len(tomb.files)*objectio.LocationLen)

	typ := uint8(tomb.Type())
	if _, err = buf.Write(types.EncodeUint8(&typ)); err != nil {
		return
	}

	size := uint32(len(tomb.rowids))
	if _, err = buf.Write(types.EncodeUint32(&size)); err != nil {
		return
	}
	if _, err = buf.Write(types.EncodeSlice[types.Rowid](tomb.rowids)); err != nil {
		return
	}

	size = uint32(len(tomb.files))
	if _, err = buf.Write(types.EncodeUint32(&size)); err != nil {
		return
	}
	_, err = buf.Write(tomb.files[:])
	return
}

func (tomb *tombstoneData) UnmarshalBinary(buf []byte) error {
	tomb.blockFilter = nil
	typ := engine.TombstoneType(types.DecodeUint8(buf))
	if typ != engine.TombstoneData {
		return moerr.NewInternalErrorNoCtxf("UnmarshalBinary TombstoneData with %v", typ)
	}
	buf = buf[1:]

	size := types.DecodeUint32(buf)
	buf = buf[4:]
	tomb.rowids = types.DecodeSlice[types.Rowid](buf[:size*types.RowidSize])
	buf = buf[size*types.RowidSize:]
	buf = buf[4:]
	tomb.files = objectio.ObjectStatsSlice(buf[:])
	return nil
}

func (tomb *tombstoneData) AppendInMemory(rowids ...types.Rowid) error {
	if tomb.blockFilter == nil {
		tomb.rowids = append(tomb.rowids, rowids...)
		return nil
	}
	for i := range rowids {
		if tomb.blockFilter.contains(rowids[i].BorrowBlockID()) {
			tomb.rowids = append(tomb.rowids, rowids[i])
		}
	}
	return nil
}

func (tomb *tombstoneData) AppendFiles(stats ...objectio.ObjectStats) error {
	for _, ss := range stats {
		if !tomb.blockFilter.mayContainZoneMap(ss.SortKeyZoneMap()) {
			continue
		}
		tomb.files.Append(ss[:])
	}
	return nil
}

func (tomb *tombstoneData) String() string {
	return tomb.StringWithPrefix("")
}

func (tomb *tombstoneData) StringWithPrefix(prefix string) string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("%sTombstone[%d]<\n", prefix, tomb.Type()))
	w.WriteString(fmt.Sprintf("\t%sInMemTombstones: \n", prefix))
	count := 0
	for _, rowId := range tomb.rowids {
		if count%2 == 0 && count != 0 {
			w.WriteByte('\n')
		}
		if count%2 == 0 {
			w.WriteString(fmt.Sprintf("\t\t%s", prefix))
		}
		w.WriteString(fmt.Sprintf("%s, ", rowId.String()))
		count++
	}

	w.WriteString(fmt.Sprintf("\n\t%sTombstoneFiles: \n", prefix))
	for i := 0; i < tomb.files.Len(); i++ {
		w.WriteString(fmt.Sprintf("\t\t%s%s\n", prefix, tomb.files.Get(i).String()))
	}

	return w.String()
}

func (tomb *tombstoneData) Type() engine.TombstoneType {
	return engine.TombstoneData
}

func (tomb *tombstoneData) HasAnyInMemoryTombstone() bool {
	return tomb != nil && len(tomb.rowids) > 0
}

func (tomb *tombstoneData) HasAnyTombstoneFile() bool {
	return tomb != nil && len(tomb.files) > 0
}

// false positive check
func (tomb *tombstoneData) HasBlockTombstone(
	ctx context.Context,
	blockId *objectio.Blockid,
	fs fileservice.FileService,
) (bool, error) {
	if tomb == nil {
		return false, nil
	}
	if len(tomb.rowids) > 0 {
		// TODO: optimize binary search once
		start, end := ioutil.FindStartEndOfBlockFromSortedRowids(tomb.rowids, blockId)
		if end > start {
			return true, nil
		}
	}
	if len(tomb.files) == 0 {
		return false, nil
	}
	for i, end := 0, tomb.files.Len(); i < end; i++ {
		objectStats := tomb.files.Get(i)
		zm := objectStats.SortKeyZoneMap()
		if !zm.RowidPrefixEq(blockId[:]) {
			continue
		}
		location := objectStats.ObjectLocation()
		objectMeta, err := objectio.FastLoadObjectMeta(
			ctx, &location, false, fs,
		)
		if err != nil {
			return false, err
		}

		dataMeta := objectMeta.MustDataMeta()

		blkCnt := int(dataMeta.BlockCount())

		startIdx := sort.Search(blkCnt, func(i int) bool {
			return dataMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap().AnyGEByValue(blockId[:])
		})

		for pos := startIdx; pos < blkCnt; pos++ {
			blkMeta := dataMeta.GetBlockMeta(uint32(pos))
			columnZonemap := blkMeta.MustGetColumn(0).ZoneMap()
			if !columnZonemap.RowidPrefixEq(blockId[:]) {
				if columnZonemap.RowidPrefixGT(blockId[:]) {
					break
				}
				continue
			}
			return true, nil
		}
	}
	return false, nil
}

// FIXME:
func (tomb *tombstoneData) PrefetchTombstones(
	ctx context.Context,
	srvId string,
	fs fileservice.FileService,
	bids []objectio.Blockid,
) {
	if len(bids) == 0 {
		for i, end := 0, tomb.files.Len(); i < end; i++ {
			if ctx.Err() != nil {
				return
			}
			stats := tomb.files.Get(i)
			for j := 0; j < int(stats.BlkCnt()); j++ {
				loc := stats.BlockLocation(uint16(j), objectio.BlockMaxRows)
				if err := ioutil.Prefetch(srvId, fs, loc); err != nil {
					logutil.Errorf("prefetch block delta location: %s", err.Error())
				}
			}
		}
		return
	}
	filter := newBlockIDFilter(bids)
	for i, end := 0, tomb.files.Len(); i < end; i++ {
		if ctx.Err() != nil {
			return
		}
		stats := tomb.files.Get(i)
		if !filter.mayContainZoneMap(stats.SortKeyZoneMap()) {
			continue
		}
		location := stats.ObjectLocation()
		objectMeta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if err != nil {
			logutil.Errorf("load tombstone object metadata for prefetch: %s", err.Error())
			continue
		}
		dataMeta := objectMeta.MustDataMeta()
		for j := 0; j < int(dataMeta.BlockCount()); j++ {
			if ctx.Err() != nil {
				return
			}
			zoneMap := dataMeta.GetBlockMeta(uint32(j)).MustGetColumn(0).ZoneMap()
			if !filter.mayContainZoneMap(zoneMap) {
				continue
			}
			loc := stats.BlockLocation(uint16(j), objectio.BlockMaxRows)
			if err := ioutil.Prefetch(
				srvId,
				fs,
				loc,
			); err != nil {
				logutil.Errorf("prefetch block delta location: %s", err.Error())
			}
		}
	}
}

func (tomb *tombstoneData) ApplyInMemTombstones(
	bid *types.Blockid,
	rowsOffset []int64,
	deleted *objectio.Bitmap,
) (left []int64) {

	left = rowsOffset

	if len(tomb.rowids) == 0 {
		return
	}

	// is the tomb.rowIds sorted?
	FastApplyDeletesByRowIds(bid, &left, deleted, tomb.rowids, true)

	return
}

func (tomb *tombstoneData) ApplyPersistedTombstones(
	ctx context.Context,
	fs fileservice.FileService,
	snapshot *types.TS,
	bid *types.Blockid,
	rowsOffset []int64,
	deletedMask *objectio.Bitmap,
) (left []int64, err error) {

	left = rowsOffset
	if tomb.files.Len() == 0 {
		return
	}

	var curr int
	getTombstone := func() (*objectio.ObjectStats, error) {
		if curr >= tomb.files.Len() {
			return nil, nil
		}
		i := curr
		curr++
		return tomb.files.Get(i), nil
	}

	release := func() {}
	if deletedMask == nil {
		bm := objectio.GetReusableBitmap()
		deletedMask = &bm
		release = bm.Release
	}
	defer release()

	if err = ioutil.GetTombstonesByBlockId(
		ctx,
		snapshot,
		bid,
		getTombstone,
		deletedMask,
		fs,
	); err != nil {
		return nil, err
	}

	if len(rowsOffset) != 0 {
		left = RemoveIf(rowsOffset, func(t int64) bool {
			return deletedMask.Contains(uint64(t))
		})
	}

	return left, nil
}

func (tomb *tombstoneData) SortInMemory() {
	slices.SortFunc(tomb.rowids, func(a, b types.Rowid) int {
		return a.Compare(&b)
	})
}

func (tomb *tombstoneData) Merge(other engine.Tombstoner) error {
	if v, ok := other.(*tombstoneData); ok {
		if err := tomb.AppendInMemory(v.rowids...); err != nil {
			return err
		}
		for i := 0; i < v.files.Len(); i++ {
			if err := tomb.AppendFiles(*v.files.Get(i)); err != nil {
				return err
			}
		}
		tomb.SortInMemory()
		return nil
	}
	return moerr.NewInternalErrorNoCtxf(
		"tombstone type mismatch %d, %d", tomb.Type(), other.Type(),
	)
}

func RowIdsToOffset(
	rowIds []types.Rowid,
	skipMask objectio.Bitmap,
) []int64 {

	ret := make([]int64, 0, 10)
	for i, rowId := range rowIds {
		if skipMask.Contains(uint64(i)) {
			continue
		}
		offset := rowId.GetRowOffset()
		ret = append(ret, int64(offset))
	}

	return ret
}
