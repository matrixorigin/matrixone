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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

var _ engine.Tombstoner = new(tombstoneDataWithDeltaLoc)

func UnmarshalTombstoneData(data []byte) (engine.Tombstoner, error) {
	typ := engine.TombstoneType(data[0])
	switch typ {
	case engine.TombstoneWithDeltaLoc:
		tomb := new(tombstoneDataWithDeltaLoc)
		if err := tomb.UnmarshalBinary(data); err != nil {
			return nil, err
		}
		return tomb, nil
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

func NewEmptyTombstoneWithDeltaLoc() *tombstoneDataWithDeltaLoc {
	return &tombstoneDataWithDeltaLoc{
		inMemTombstones: make(map[types.Blockid][]int32),
		blk2UncommitLoc: make(map[types.Blockid][]objectio.Location),
		blk2CommitLoc:   make(map[types.Blockid]logtailreplay.BlockDeltaInfo),
	}
}

// =======================================================================================
// tombstoneDataWithDeltaLoc define and APIs
// =======================================================================================

type tombstoneData struct {
	rowids []types.Rowid
	files  objectio.LocationSlice
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
		return moerr.NewInternalErrorNoCtx("UnmarshalBinary TombstoneData with %v", typ)
	}
	buf = buf[1:]

	size := types.DecodeUint32(buf)
	buf = buf[4:]
	tomb.rowids = types.DecodeSlice[types.Rowid](buf[:size*types.RowidSize])
	buf = buf[size*types.RowidSize:]
	buf = buf[4:]
	tomb.files = objectio.LocationSlice(buf[:])
	return nil
}

func (tomb *tombstoneData) AppendInMemory(rowids ...types.Rowid) error {
	tomb.rowids = append(tomb.rowids, rowids...)
	return nil
}

func (tomb *tombstoneData) AppendFiles(locs ...objectio.Location) error {
	for _, loc := range locs {
		tomb.files = append(tomb.files, loc[:]...)
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

func (tomb *tombstoneData) HasTombstones(bid types.Blockid) bool {
	panic("Not Support")
}

// FIXME:
func (tomb *tombstoneData) PrefetchTombstones(
	srvId string,
	fs fileservice.FileService,
	bids []objectio.Blockid,
) {
	for i, end := 0, tomb.files.Len(); i < end; i++ {
		loc := tomb.files.Get(i)
		if err := blockio.PrefetchTombstone(
			srvId,
			[]uint16{0, 1, 2},
			[]uint16{loc.ID()},
			fs,
			*loc,
		); err != nil {
			logutil.Errorf("prefetch block delta location: %s", err.Error())
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
	bid types.Blockid,
	rowsOffset []int64,
	mask *nulls.Nulls,
	apply func(
		ctx context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int64,
		deleted *nulls.Nulls,
	) (left []int64, err error),
) (left []int64, err error) {

	left = rowsOffset
	if len(tomb.files) == 0 {
		return
	}

	for i, end := 0, tomb.files.Len(); i < end; i++ {
		loc := tomb.files.Get(i)
		left, err = apply(ctx, *loc, types.TS{}, left, mask)
		if err != nil {
			return
		}
	}

	return
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
	return moerr.NewInternalErrorNoCtx(
		"tombstone type mismatch %d, %d", tomb.Type(), other.Type(),
	)
}

// =======================================================================================
// tombstoneDataWithDeltaLoc define and APIs
// =======================================================================================

// TODO: DECRYPTED
type tombstoneDataWithDeltaLoc struct {
	//in memory tombstones
	inMemTombstones map[types.Blockid][]int32

	//persisted tombstones
	// uncommitted tombstones, written by CN, one block maybe respond to multi deltaLocs.
	blk2UncommitLoc map[types.Blockid][]objectio.Location
	//committed tombstones.
	blk2CommitLoc map[types.Blockid]logtailreplay.BlockDeltaInfo
}

// FIXME
func (tomb *tombstoneDataWithDeltaLoc) PrefetchTombstones(
	srvId string,
	fs fileservice.FileService,
	bids []objectio.Blockid,
) {
	// prefetch blk delta location
	for idx := 0; idx < len(bids); idx++ {
		for _, loc := range tomb.blk2UncommitLoc[bids[idx]] {
			if err := blockio.PrefetchTombstone(
				srvId,
				[]uint16{0, 1, 2},
				[]uint16{loc.ID()},
				fs,
				objectio.Location(loc[:]),
			); err != nil {
				logutil.Errorf("prefetch block delta location: %s", err.Error())
			}
		}

		if info, ok := tomb.blk2CommitLoc[bids[idx]]; ok {
			loc := info.Loc
			if err := blockio.PrefetchTombstone(
				srvId,
				[]uint16{0, 1, 2},
				[]uint16{loc.ID()},
				fs,
				objectio.Location(loc[:]),
			); err != nil {
				logutil.Errorf("prefetch block delta location: %s", err.Error())
			}
		}
	}
}

func (tomb *tombstoneDataWithDeltaLoc) HasAnyInMemoryTombstone() bool {
	return tomb != nil && len(tomb.inMemTombstones) > 0
}

func (tomb *tombstoneDataWithDeltaLoc) HasAnyTombstoneFile() bool {
	return tomb != nil && (len(tomb.blk2CommitLoc) > 0 || len(tomb.blk2UncommitLoc) > 0)
}

func (tomb *tombstoneDataWithDeltaLoc) String() string {
	return tomb.StringWithPrefix("")
}

func (tomb *tombstoneDataWithDeltaLoc) StringWithPrefix(prefix string) string {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf("%sTombstone[%d]<\n", prefix, tomb.Type()))
	w.WriteString(fmt.Sprintf("\t%sInMemTombstones: \n", prefix))
	for bid, offsets := range tomb.inMemTombstones {
		w.WriteString(fmt.Sprintf("\t\t%sblk:%s, offsets:%v\n", prefix, bid.String(), offsets))
	}
	w.WriteString(fmt.Sprintf("\t%sBlk2UncommitLoc: \n", prefix))
	for bid, locs := range tomb.blk2UncommitLoc {
		w.WriteString(fmt.Sprintf("\t\t%sblk:%s, locs:%v\n", prefix, bid.String(), locs))
	}
	w.WriteString(fmt.Sprintf("\t%sBlk2CommitLoc: \n", prefix))
	for bid, loc := range tomb.blk2CommitLoc {
		w.WriteString(fmt.Sprintf("\t\t%sblk:%s, loc:%v, cts:%d\n", prefix, bid.String(), loc.Loc, loc.Cts))
	}
	w.WriteString(fmt.Sprintf("%s>\n", prefix))
	return w.String()
}

func (tomb *tombstoneDataWithDeltaLoc) HasTombstones(bid types.Blockid) bool {
	if _, ok := tomb.inMemTombstones[bid]; ok {
		return true
	}
	if _, ok := tomb.blk2UncommitLoc[bid]; ok {
		return true
	}
	if _, ok := tomb.blk2CommitLoc[bid]; ok {
		return true
	}
	return false
}

func (tomb *tombstoneDataWithDeltaLoc) UnmarshalBinary(buf []byte) error {
	typ := engine.TombstoneType(types.DecodeUint8(buf))
	if typ != engine.TombstoneWithDeltaLoc {
		return moerr.NewInternalErrorNoCtx("UnmarshalBinary TombstoneWithDeltaLoc with %v", typ)
	}
	buf = buf[1:]

	cnt := types.DecodeUint32(buf)
	buf = buf[4:]

	tomb.inMemTombstones = make(map[types.Blockid][]int32, int(cnt))
	for i := 0; i < int(cnt); i++ {
		bid := types.DecodeFixed[types.Blockid](buf[:types.BlockidSize])
		buf = buf[types.BlockidSize:]

		offsetLen := types.DecodeUint32(buf)
		buf = buf[4:]
		if offsetLen > 0 {
			tomb.inMemTombstones[bid] = types.DecodeSlice[int32](buf[:offsetLen])
			buf = buf[offsetLen:]
		}
	}

	cnt = types.DecodeUint32(buf)
	buf = buf[4:]
	tomb.blk2UncommitLoc = make(map[types.Blockid][]objectio.Location, int(cnt))
	for i := 0; i < int(cnt); i++ {
		bid := types.DecodeFixed[types.Blockid](buf[:types.BlockidSize])
		buf = buf[types.BlockidSize:]
		locLen := types.DecodeUint32(buf)
		buf = buf[4:]
		if locLen > 0 {
			locs := make([]objectio.Location, int(locLen)/objectio.LocationLen)
			for j := range locs {
				locs[j] = buf[:objectio.LocationLen]
				buf = buf[objectio.LocationLen:]
			}
			tomb.blk2UncommitLoc[bid] = locs
		}
	}

	cnt = types.DecodeUint32(buf)
	buf = buf[4:]
	tomb.blk2CommitLoc = make(map[types.Blockid]logtailreplay.BlockDeltaInfo, int(cnt))

	for i := 0; i < int(cnt); i++ {
		bid := types.DecodeFixed[types.Blockid](buf[:types.BlockidSize])
		buf = buf[types.BlockidSize:]

		loc := buf[:objectio.LocationLen]
		buf = buf[objectio.LocationLen:]

		cts := types.DecodeFixed[types.TS](buf[:types.TxnTsSize])
		buf = buf[types.TxnTsSize:]

		tomb.blk2CommitLoc[bid] = logtailreplay.BlockDeltaInfo{
			Cts: cts,
			Loc: loc,
		}
	}

	return nil
}

func (tomb *tombstoneDataWithDeltaLoc) MarshalBinaryWithBuffer(w *bytes.Buffer) (err error) {
	typ := uint8(tomb.Type())
	if _, err = w.Write(types.EncodeUint8(&typ)); err != nil {
		return
	}

	length := uint32(len(tomb.inMemTombstones))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}

	w.Grow(int(length * types.BlockidSize))

	for bid, offsets := range tomb.inMemTombstones {
		if _, err = w.Write(bid[:]); err != nil {
			return
		}

		buf := types.EncodeSlice[int32](offsets)
		bufLen := uint32(len(buf))
		if _, err = w.Write(types.EncodeUint32(&bufLen)); err != nil {
			return
		}
		if _, err = w.Write(buf); err != nil {
			return
		}
	}

	length = uint32(len(tomb.blk2UncommitLoc))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}

	w.Grow(int(length * types.BlockidSize))

	for bid, locs := range tomb.blk2UncommitLoc {
		if _, err = w.Write(bid[:]); err != nil {
			return
		}

		bufLen := uint32(len(locs) * objectio.LocationLen)
		if _, err = w.Write(types.EncodeUint32(&bufLen)); err != nil {
			return
		}
		for _, loc := range locs {
			if _, err = w.Write(loc[:]); err != nil {
				return
			}
		}
	}

	length = uint32(len(tomb.blk2CommitLoc))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}
	w.Grow(int(length) * (types.BlockidSize + objectio.LocationLen + types.TxnTsSize))

	for bid, loc := range tomb.blk2CommitLoc {
		if _, err = w.Write(bid[:]); err != nil {
			return
		}

		if _, err = w.Write(loc.Loc[:]); err != nil {
			return
		}

		if _, err = w.Write(types.EncodeTxnTS(&loc.Cts)); err != nil {
			return
		}
	}

	return
}

func (tomb *tombstoneDataWithDeltaLoc) ApplyInMemTombstones(
	bid types.Blockid,
	rowsOffset []int64,
	deleted *nulls.Nulls,
) (left []int64) {
	left = rowsOffset

	if rowOffsets, ok := tomb.inMemTombstones[bid]; ok {
		for _, o := range rowOffsets {
			left = fastApplyDeletedRows(left, deleted, uint32(o))
		}
	}

	return
}

func (tomb *tombstoneDataWithDeltaLoc) ApplyPersistedTombstones(
	ctx context.Context,
	bid types.Blockid,
	rowsOffset []int64,
	mask *nulls.Nulls,
	apply func(
		ctx context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int64,
		deleted *nulls.Nulls,
	) (left []int64, err error),
) (left []int64, err error) {

	left = rowsOffset

	if locs, ok := tomb.blk2UncommitLoc[bid]; ok {
		for _, loc := range locs {
			left, err = apply(ctx, loc, types.TS{}, left, mask)
			if err != nil {
				return
			}
		}
	}

	if loc, ok := tomb.blk2CommitLoc[bid]; ok {
		left, err = apply(ctx, loc.Loc, loc.Cts, left, mask)
		if err != nil {
			return
		}
	}

	return
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

func (tomb *tombstoneDataWithDeltaLoc) Type() engine.TombstoneType {
	return engine.TombstoneWithDeltaLoc
}

func (tomb *tombstoneDataWithDeltaLoc) SortInMemory() {}

func (tomb *tombstoneDataWithDeltaLoc) Merge(other engine.Tombstoner) error {
	if v, ok := other.(*tombstoneDataWithDeltaLoc); ok {
		for blkID, rows := range v.inMemTombstones {
			tomb.inMemTombstones[blkID] = append(tomb.inMemTombstones[blkID], rows...)
		}
		for blkID, locs := range v.blk2UncommitLoc {
			tomb.blk2UncommitLoc[blkID] = append(tomb.blk2UncommitLoc[blkID], locs...)
		}
		for blkID, loc := range v.blk2CommitLoc {
			tomb.blk2CommitLoc[blkID] = loc
		}
		return nil
	}
	return moerr.NewInternalErrorNoCtx("tombstone type mismatch")
}
