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
	DescribeObject() ([]ObjectStats, error)
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

var ZeroObjectStats ObjectStats

// ObjectStats has format:
// +------------------------------------------------------------------------------------------------+--------+
// |object_name(60B)|extent(13B)|row_cnt(4B)|block_cnt(4B)|zone_map(64B)|objectSize|objectOriginSize|reserved|
// +------------------------------------------------------------------------------------------------+--------+
type ObjectStats [ObjectStatsLen]byte

func NewObjectStats() *ObjectStats {
	return new(ObjectStats)
}

func NewObjectStatsWithObjectID(id *ObjectId, appendable, sorted, cnCreated bool) *ObjectStats {
	stats := new(ObjectStats)
	SetObjectStatsObjectName(stats, BuildObjectNameWithObjectID(id))
	if appendable {
		stats.setAppendable()
	}
	if sorted {
		stats.SetSorted()
	}
	if cnCreated {
		stats.SetCNCreated()
	}
	return stats
}
func (des *ObjectStats) Marshal() []byte {
	return des[:]
}

func (des *ObjectStats) UnMarshal(data []byte) {
	copy(des[:], data)
}

// Clone deep copies the stats and returns its pointer
func (des *ObjectStats) Clone() *ObjectStats {
	copied := NewObjectStats()
	copy(copied[:], des[:])
	return copied
}
func (des *ObjectStats) setAppendable() {
	des[reservedOffset] = des[reservedOffset] | 0x1
}
func (des *ObjectStats) GetAppendable() bool {
	return des[reservedOffset]&0x1 != 0
}
func (des *ObjectStats) SetSorted() {
	des[reservedOffset] = des[reservedOffset] | 0x2
}
func (des *ObjectStats) GetSorted() bool {
	return des[reservedOffset]&0x2 != 0
}
func (des *ObjectStats) SetCNCreated() {
	des[reservedOffset] = des[reservedOffset] | 0x4
}
func (des *ObjectStats) GetCNCreated() bool {
	return des[reservedOffset]&0x4 != 0
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

func (des *ObjectStats) String() string {
	reserved := ""
	if des.GetAppendable() {
		reserved = reserved + "A"
	}
	if des.GetSorted() {
		reserved = reserved + "S"
	}
	if des.GetCNCreated() {
		reserved = reserved + "C"
	}
	return fmt.Sprintf("[object stats]: %v; objName: %s; extent: %v; "+
		"rowCnt: %d; blkCnt: %d; sortKey zoneMap: %v; size: %d; originSize: %d",
		reserved, des.ObjectName().String(), des.Extent().String(),
		des.Rows(), des.BlkCnt(), des.SortKeyZoneMap(),
		des.Size(), des.OriginSize())
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

func (o *ObjectStatsSlice) GetBytes(i int) []byte {
	return (*o)[i*ObjectStatsLen : (i+1)*ObjectStatsLen]
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

func (o *ObjectStatsSlice) SetBytes(stats []byte) {
	*o = stats
}

func (o *ObjectStatsSlice) GetAllBytes() []byte {
	return (*o)[:]
}

func (o *ObjectStatsSlice) Slice(i, j int) []byte {
	return (*o)[i*ObjectStatsLen : j*ObjectStatsLen]
}

func (o *ObjectStatsSlice) Set(i int, stats []byte) {
	copy((*o)[i*ObjectStatsLen:(i+1)*ObjectStatsLen], stats)
}
