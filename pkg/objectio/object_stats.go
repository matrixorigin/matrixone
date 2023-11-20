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

const ObjectStatsLen = ObjectNameLen + ExtentLen + rowCntLen + blkCntLen + ZoneMapSize

const (
	rowCntLen = 4
	blkCntLen = 4

	objectNameOffset = 0
	extentOffset     = objectNameOffset + ObjectNameLen
	rowCntOffset     = extentOffset + ExtentLen
	blkCntOffset     = rowCntOffset + 4
	zoneMapOffset    = blkCntOffset + 4
)

var ZeroObjectStats ObjectStats

// ObjectStats has format:
// +--------------------------------------------------------------------+
// |object_name(60B)|extent(13B)|row_cnt(4B)|block_cnt(4B)|zone_map(64B)|
// +--------------------------------------------------------------------+
type ObjectStats [ObjectStatsLen]byte

func NewObjectStats() *ObjectStats {
	return new(ObjectStats)
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

func (des *ObjectStats) IsZero() bool {
	return bytes.Equal(des[:], ZeroObjectStats[:])
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

func (des *ObjectStats) OriginSize() uint32 {
	return Extent(des[ExtentOff : ExtentOff+ExtentLen]).OriginSize()
}

func (des *ObjectStats) CompSize() uint32 {
	return Extent(des[ExtentOff : ExtentOff+ExtentLen]).Length()
}

func (des *ObjectStats) BlkCnt() uint32 {
	return types.DecodeUint32(des[blkCntOffset : blkCntOffset+blkCntLen])
}

func (des *ObjectStats) SortKeyZoneMap() ZoneMap {
	return ZoneMap(des[zoneMapOffset : zoneMapOffset+zoneMapLen])
}

func (des *ObjectStats) Extent() Extent {
	return Extent(des[extentOffset : extentOffset+ExtentLen])
}

func (des *ObjectStats) Rows() uint32 {
	return types.DecodeUint32(des[rowCntOffset : rowCntOffset+rowCntLen])
}

func (des *ObjectStats) String() string {
	return fmt.Sprintf("[object stats]: objName: %s; extent: %v; "+
		"rowCnt: %d; blkCnt: %d; sortKey zoneMap: %v",
		des.ObjectName().String(), des.Extent().String(),
		des.Rows(), des.BlkCnt(), des.SortKeyZoneMap())
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
