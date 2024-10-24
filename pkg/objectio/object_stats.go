// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type ObjectDescriber interface {
	DescribeObject() (ObjectStats, error)
}

const (
	rowCntLen = 4
	blkCntLen = 4

	objectNameOffset       = 0
	extentOffset           = objectNameOffset + ObjectNameLen
	rowCntOffset           = extentOffset + ExtentLen
	blkCntOffset           = rowCntOffset + rowCntLen
	zoneMapOffset          = blkCntOffset + blkCntLen
	objectSizeOffset       = zoneMapOffset + ZoneMapSize
	objectSizeLen          = 4
	objectOriginSizeOffset = objectSizeOffset + objectSizeLen
	objectOriginSizeLen    = 4
	reservedOffset         = objectOriginSizeOffset + objectOriginSizeLen
	reservedLen            = 1
	ObjectStatsLen         = reservedOffset + reservedLen
)

const (
	ObjectFlag_Appendable = 1 << iota
	ObjectFlag_Sorted
	ObjectFlag_CNCreated
)

var ZeroObjectStats ObjectStats

// ObjectStats has format:
// +------------------------------------------------------------------------------------------------+--------+
// |object_name(60B)|extent(13B)|row_cnt(4B)|block_cnt(4B)|zone_map(64B)|objectSize|objectOriginSize|reserved|
// +------------------------------------------------------------------------------------------------+--------+
type ObjectStats [ObjectStatsLen]byte

type ObjectStatsOptions func(*ObjectStats)

func WithCNCreated() ObjectStatsOptions {
	return func(o *ObjectStats) {
		o[reservedOffset] |= ObjectFlag_CNCreated
	}
}

func WithSorted() ObjectStatsOptions {
	return func(o *ObjectStats) {
		o[reservedOffset] |= ObjectFlag_Sorted
	}
}

func WithAppendable() ObjectStatsOptions {
	return func(o *ObjectStats) {
		o[reservedOffset] |= ObjectFlag_Appendable
	}
}

func NewObjectStats() *ObjectStats {
	return new(ObjectStats)
}

func NewObjectStatsWithObjectID(id *ObjectId, appendable, sorted, cnCreated bool) *ObjectStats {
	stats := new(ObjectStats)
	SetObjectStatsObjectName(stats, BuildObjectNameWithObjectID(id))
	if appendable {
		stats[reservedOffset] = stats[reservedOffset] | ObjectFlag_Appendable
	}

	if sorted {
		stats[reservedOffset] = stats[reservedOffset] | ObjectFlag_Sorted
	}

	if cnCreated {
		stats[reservedOffset] = stats[reservedOffset] | ObjectFlag_CNCreated
	}

	return stats
}
func (des *ObjectStats) Marshal() []byte {
	return des[:]
}

func (des *ObjectStats) UnMarshal(data []byte) {
	copy(des[:], data)
}

func (des *ObjectStats) GetFlag() int8 {
	return int8(des[reservedOffset])
}

// Clone deep copies the stats and returns its pointer
func (des *ObjectStats) Clone() *ObjectStats {
	copied := NewObjectStats()
	copy(copied[:], des[:])
	return copied
}

func (des *ObjectStats) GetAppendable() bool {
	return des[reservedOffset]&ObjectFlag_Appendable != 0
}

func (des *ObjectStats) GetSorted() bool {
	return des[reservedOffset]&ObjectFlag_Sorted != 0
}

func (des *ObjectStats) GetCNCreated() bool {
	return des[reservedOffset]&ObjectFlag_CNCreated != 0
}
func (des *ObjectStats) IsZero() bool {
	return bytes.Equal(des[:], ZeroObjectStats[:])
}

func (des *ObjectStats) ZMIsEmpty() bool {
	return bytes.Equal(des[zoneMapOffset:zoneMapOffset+zoneMapLen],
		ZeroObjectStats[zoneMapOffset:zoneMapOffset+zoneMapLen])
}

func (des *ObjectStats) ObjectShortName() *ObjectNameShort {
	return des.ObjectName().Short()
}

func (des *ObjectStats) ObjectLocation() Location {
	return BuildLocation(des.ObjectName(), des.Extent(), 0, 0)
}

func (des *ObjectStats) ConstructBlockId(id uint16) Blockid {
	var blockId Blockid
	BuildObjectBlockidTo(des.ObjectName(), id, blockId[:])
	return blockId
}

func (des *ObjectStats) ConstructBlockInfoTo(id uint16, blk *BlockInfo) {
	des.BlockLocationTo(id, BlockMaxRows, blk.MetaLoc[:])
	blk.ConstructBlockID(des.ObjectName(), id)
	blk.SetFlagByObjStats(des)
}

func (des *ObjectStats) ConstructBlockInfo(id uint16) BlockInfo {
	var blk BlockInfo
	des.BlockLocationTo(id, BlockMaxRows, blk.MetaLoc[:])
	blk.ConstructBlockID(des.ObjectName(), id)
	blk.SetFlagByObjStats(des)
	return blk
}

func (des *ObjectStats) BlockLocationTo(
	blk uint16,
	maxRows uint32,
	toLoc []byte,
) {
	row := maxRows
	if blk == uint16(des.BlkCnt())-1 {
		row = des.Rows() - uint32(blk)*maxRows
	}
	BuildLocationTo(des.ObjectName(), des.Extent(), row, blk, toLoc)
}

func (des *ObjectStats) BlockLocation(blk uint16, maxRows uint32) Location {
	row := maxRows
	if blk == uint16(des.BlkCnt())-1 {
		row = des.Rows() - uint32(blk)*maxRows
	}
	return BuildLocation(des.ObjectName(), des.Extent(), row, blk)
}

func (des *ObjectStats) ObjectName() ObjectName {
	return ObjectName(des[objectNameOffset : objectNameOffset+ObjectNameLen])
}

func (des *ObjectStats) Size() uint32 {
	return types.DecodeUint32(des[objectSizeOffset : objectSizeOffset+objectSizeLen])
}

func (des *ObjectStats) OriginSize() uint32 {
	return types.DecodeUint32(des[objectOriginSizeOffset : objectOriginSizeOffset+objectOriginSizeLen])
}

func (des *ObjectStats) BlkCnt() uint32 {
	return types.DecodeUint32(des[blkCntOffset : blkCntOffset+blkCntLen])
}

func (des *ObjectStats) SortKeyZoneMap() ZoneMap {
	return ZoneMap(des[zoneMapOffset : zoneMapOffset+zoneMapLen])
}

func (des *ObjectStats) Extent() Extent {
	return des[extentOffset : extentOffset+ExtentLen]
}

func (des *ObjectStats) Rows() uint32 {
	return types.DecodeUint32(des[rowCntOffset : rowCntOffset+rowCntLen])
}

func (des *ObjectStats) FlagString() string {
	flags := ""
	if des.GetAppendable() {
		flags += "1"
	} else {
		flags += "0"
	}
	if des.GetSorted() {
		flags += "1"
	} else {
		flags += "0"
	}
	if des.GetCNCreated() {
		flags += "1"
	} else {
		flags += "0"
	}
	return flags
}

func (des *ObjectStats) String() string {
	flags := des.FlagString()
	if des.Extent().Length() == 0 {
		return fmt.Sprintf("[OBJ(%s)-(%s)|NoExt]", flags, des.ObjectName().String())
	}
	return fmt.Sprintf(
		"[OBJ(%s)-(%s)|Ext(%s)|Rows(%d)|Blks(%d)|Size(%d)|OSize(%d)]",
		flags, des.ObjectName().String(), des.Extent().String(),
		des.Rows(), des.BlkCnt(), des.Size(), des.OriginSize(),
	)
}

func setHelper(stats *ObjectStats, offset int, data []byte) error {
	if stats == nil {
		return moerr.NewInternalErrorNoCtx("invalid object stats")
	}

	if data == nil {
		return moerr.NewInternalErrorNoCtx("invalid input data")
	}
	copy(stats[offset:], data)
	return nil
}

func SetObjectStats(stats, o *ObjectStats) error {
	return setHelper(stats, extentOffset, o[extentOffset:reservedOffset])
}
func SetObjectStatsRowCnt(stats *ObjectStats, cnt uint32) error {
	return setHelper(stats, rowCntOffset, types.EncodeUint32(&cnt))
}

func SetObjectStatsBlkCnt(stats *ObjectStats, cnt uint32) error {
	return setHelper(stats, blkCntOffset, types.EncodeUint32(&cnt))
}

func SetObjectStatsObjectName(stats *ObjectStats, name ObjectName) error {
	return setHelper(stats, objectNameOffset, name)
}

func SetObjectStatsShortName(stats *ObjectStats, name *ObjectNameShort) error {
	return setHelper(stats, objectNameOffset, name[:])
}

func SetObjectStatsExtent(stats *ObjectStats, extent Extent) error {
	return setHelper(stats, extentOffset, extent)
}

func SetObjectStatsSortKeyZoneMap(stats *ObjectStats, zoneMap ZoneMap) error {
	return setHelper(stats, zoneMapOffset, zoneMap)
}

func SetObjectStatsLocation(stats *ObjectStats, location Location) error {
	return setHelper(stats, objectNameOffset, location[:ObjectNameLen+ExtentLen])
}

func SetObjectStatsSize(stats *ObjectStats, size uint32) error {
	return setHelper(stats, objectSizeOffset, types.EncodeUint32(&size))
}

func SetObjectStatsOriginSize(stats *ObjectStats, size uint32) error {
	return setHelper(stats, objectOriginSizeOffset, types.EncodeUint32(&size))
}

// ForeachObjectStats executes onStats on each object stats until onStats returns false
// or all object stats have been visited
func ForeachObjectStats(onStats func(stats *ObjectStats) bool, statsList ...ObjectStats) {
	statsLen := len(statsList)
	for idx := 0; idx < statsLen; idx++ {
		if !onStats(&statsList[idx]) {
			return
		}
	}
}

type ObjectStatsSlice []byte

func (o *ObjectStatsSlice) Get(i int) *ObjectStats {
	stats := ObjectStats((*o)[i*ObjectStatsLen : (i+1)*ObjectStatsLen])
	return &stats
}

func (o *ObjectStatsSlice) Len() int {
	return len(*o) / ObjectStatsLen
}

func (o *ObjectStatsSlice) Append(stats []byte) {
	*o = append(*o, stats...)
}

func (o *ObjectStatsSlice) Size() int {
	return len(*o)
}

func (o *ObjectStatsSlice) Slice(i, j int) []byte {
	return (*o)[i*ObjectStatsLen : j*ObjectStatsLen]
}

func (o *ObjectStatsSlice) Set(i int, stats []byte) {
	copy((*o)[i*ObjectStatsLen:(i+1)*ObjectStatsLen], stats)
}
