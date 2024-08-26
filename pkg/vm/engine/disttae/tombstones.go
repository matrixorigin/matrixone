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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
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
	id objectio.Blockid,
	fs fileservice.FileService,
) (bool, error) {
	if tomb == nil {
		return false, nil
	}
	if len(tomb.rowids) > 0 {
		// TODO: optimize binary search once
		start, end := blockio.FindIntervalForBlock(tomb.rowids, &id)
		if end > start {
			return true, nil
		}
	}
	if len(tomb.files) > 0 {
		for i, end := 0, tomb.files.Len(); i < end; i++ {
			objectStats := tomb.files.Get(i)
			zm := objectStats.SortKeyZoneMap()
			if zm.PrefixEq(id[:]) {
				return true, nil
			}
			bf, err := objectio.FastLoadBF(
				ctx,
				objectStats.ObjectLocation(),
				false,
				fs,
			)
			if err != nil {
				logutil.Error(
					"LOAD-BF-ERROR",
					zap.String("location", objectStats.ObjectLocation().String()),
					zap.Error(err),
				)
				return false, err
			}
			oneBlockBF := index.NewEmptyBloomFilterWithType(index.HBF)
			for idx, end := 0, int(objectStats.BlkCnt()); idx < end; idx++ {
				buf := bf.GetBloomFilter(uint32(idx))
				if err := index.DecodeBloomFilter(oneBlockBF, buf); err != nil {
					logutil.Error(
						"DECODE-BF-ERROR",
						zap.String("location", objectStats.ObjectLocation().String()),
						zap.Error(err),
					)
					return false, err
				}
				if exist, err := oneBlockBF.PrefixMayContainsKey(
					id[:],
					index.PrefixFnID_Block,
					2,
				); err != nil {
					logutil.Error(
						"PREFIX-MAY-CONTAINS-ERROR",
						zap.String("location", objectStats.ObjectLocation().String()),
						zap.Error(err),
					)
					return false, err
				} else if exist {
					return true, nil
				}
			}
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
			loc := catalog.BuildLocation(*stats, uint16(j), options.DefaultBlockMaxRows)
			if err := blockio.Prefetch(
				srvId,
				[]uint16{0, 1, 2},
				[]uint16{loc.ID()},
				fs,
				loc); err != nil {
				logutil.Errorf("prefetch block delta location: %s", err.Error())
			}
		}
	}
}

func (tomb *tombstoneData) ApplyInMemTombstones(
	bid types.Blockid,
	rowsOffset []int64,
	deleted *nulls.Nulls,
) (left []int64) {

	left = rowsOffset

	if len(tomb.rowids) == 0 {
		return
	}

	start, end := blockio.FindIntervalForBlock(tomb.rowids, &bid)

	for i := start; i < end; i++ {
		offset := tomb.rowids[i].GetRowOffset()
		left = fastApplyDeletedRows(left, deleted, offset)
	}

	return
}

func (tomb *tombstoneData) ApplyPersistedTombstones(
	ctx context.Context,
	fs fileservice.FileService,
	snapshot types.TS,
	bid types.Blockid,
	rowsOffset []int64,
	deletedMask *nulls.Nulls,
) (left []int64, err error) {

	left = rowsOffset
	if tomb.files.Len() == 0 {
		return
	}

	var obj logtailreplay.ObjectEntry
	scanOp := func(onTombstone func(tombstone logtailreplay.ObjectEntry) (bool, error)) error {
		for i, end := 0, tomb.files.Len(); i < end; i++ {
			stats := tomb.files.Get(i)
			obj.ObjectStats = *stats
			if goOn, err := onTombstone(obj); err != nil || !goOn {
				return err
			}
		}
		return nil
	}

	if deletedMask == nil {
		deletedMask = &nulls.Nulls{}
		deletedMask.InitWithSize(8192)
	}

	if err = GetTombstonesByBlockId(
		ctx,
		fs,
		bid,
		snapshot,
		deletedMask,
		scanOp); err != nil {
		return nil, err
	}

	if len(rowsOffset) != 0 {
		left = removeIf(rowsOffset, func(t int64) bool {
			if deletedMask.Contains(uint64(t)) {
				return true
			}
			return false
		})
	}

	return left, nil
}

func (tomb *tombstoneData) SortInMemory() {
	sort.Slice(tomb.rowids, func(i, j int) bool {
		return tomb.rowids[i].Less(tomb.rowids[j])
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
			_, offset := rowId.Decode()
			ret = append(ret, int32(offset))
		}
		return ret

	case uint32:
		var ret []uint32
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, uint32(offset))
		}
		return ret

	case uint64:
		var ret []uint64
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, uint64(offset))
		}
		return ret

	case int64:
		var ret []int64
		for _, rowId := range rowIds {
			_, offset := rowId.Decode()
			ret = append(ret, int64(offset))
		}
		return ret
	}

	return nil
}
