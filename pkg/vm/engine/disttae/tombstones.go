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

package disttae

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
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

type tombstoneData struct {
	rowids []types.Rowid
	files  objectio.ObjectStatsSlice
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
	tomb.rowids = append(tomb.rowids, rowids...)
	return nil
}

func (tomb *tombstoneData) AppendFiles(stats ...objectio.ObjectStats) error {
	for _, ss := range stats {
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
		start, end := blockio.FindStartEndOfBlockFromSortedRowids(tomb.rowids, blockId)
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
	srvId string,
	fs fileservice.FileService,
	bids []objectio.Blockid,
) {
	for i, end := 0, tomb.files.Len(); i < end; i++ {
		stats := tomb.files.Get(i)
		for j := 0; j < int(stats.BlkCnt()); j++ {
			loc := stats.BlockLocation(uint16(j), objectio.BlockMaxRows)
			if err := blockio.Prefetch(
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
	deleted *nulls.Nulls,
) (left []int64) {

	left = rowsOffset

	if len(tomb.rowids) == 0 {
		return
	}

	start, end := blockio.FindStartEndOfBlockFromSortedRowids(tomb.rowids, bid)

	for i := start; i < end; i++ {
		offset := tomb.rowids[i].GetRowOffset()
		left = fastApplyDeletedRows(left, deleted, offset)
	}

	return
}

func (tomb *tombstoneData) ApplyPersistedTombstones(
	ctx context.Context,
	fs fileservice.FileService,
	snapshot *types.TS,
	bid *types.Blockid,
	rowsOffset []int64,
	deletedMask *nulls.Nulls,
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

	if deletedMask == nil {
		deletedMask = &nulls.Nulls{}
		deletedMask.InitWithSize(8192)
	}

	if err = blockio.GetTombstonesByBlockId(
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
		left = removeIf(rowsOffset, func(t int64) bool {
			return deletedMask.Contains(uint64(t))
		})
	}

	return left, nil
}

func (tomb *tombstoneData) SortInMemory() {
	sort.Slice(tomb.rowids, func(i, j int) bool {
		return tomb.rowids[i].LT(&tomb.rowids[j])
	})
}

func (tomb *tombstoneData) Merge(other engine.Tombstoner) error {
	if v, ok := other.(*tombstoneData); ok {
		tomb.rowids = append(tomb.rowids, v.rowids...)
		tomb.files = append(tomb.files, v.files...)
		tomb.SortInMemory()
		return nil
	}
	return moerr.NewInternalErrorNoCtxf(
		"tombstone type mismatch %d, %d", tomb.Type(), other.Type(),
	)
}

func rowIdsToOffset(rowIds []types.Rowid, wantedType any) any {
	switch wantedType.(type) {
	case int32:
		var ret []int32
		for _, rowId := range rowIds {
			offset := rowId.GetRowOffset()
			ret = append(ret, int32(offset))
		}
		return ret

	case uint32:
		var ret []uint32
		for _, rowId := range rowIds {
			offset := rowId.GetRowOffset()
			ret = append(ret, uint32(offset))
		}
		return ret

	case uint64:
		var ret []uint64
		for _, rowId := range rowIds {
			offset := rowId.GetRowOffset()
			ret = append(ret, uint64(offset))
		}
		return ret

	case int64:
		var ret []int64
		for _, rowId := range rowIds {
			offset := rowId.GetRowOffset()
			ret = append(ret, int64(offset))
		}
		return ret
	}

	return nil
}
