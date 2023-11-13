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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type ObjectDescriber interface {
	DescribeObject() (*ObjectStats, error)
}

// ObjectStatsLen has format:
// | object_name | extent | row_cnt(4B) | block_cnt(4B) | data_zone_map | tombstone_zone_map | sort_key_idx(2B) |
const ObjectStatsLen = ObjectNameLen + ExtentLen + rowCntLen + blkCntLen + ZoneMapSize*2 + sortKeyIdxLen

const (
	rowCntLen     = 4
	blkCntLen     = 4
	sortKeyIdxLen = 2

	objectNameOffset       = 0
	extentOffset           = objectNameOffset + ObjectNameLen
	rowCntOffset           = extentOffset + ExtentLen
	blkCntOffset           = rowCntOffset + 4
	dataZoneMapOffset      = blkCntOffset + 4
	tombStoneZoneMapOffset = dataZoneMapOffset + ZoneMapSize
	sortKeyIdxOffset       = tombStoneZoneMapOffset + ZoneMapSize
)

type ObjectStats []byte

func newObjectStats() ObjectStats {
	stats := make([]byte, ObjectStatsLen)
	return stats
}

func (des ObjectStats) Marshal() []byte {
	return des
}

func (des ObjectStats) UnMarshal(data []byte) {
	copy(des, data)
}

// Clone deep copies the stats and returns its pointer
func (des ObjectStats) Clone() ObjectStats {
	copied := newObjectStats()
	copy(copied, des)
	return copied
}

func (des ObjectStats) ObjectName() ObjectName {
	return ObjectName(des[objectNameOffset : objectNameOffset+ObjectNameLen])
}

func (des ObjectStats) SortKeyIdx() uint16 {
	return types.DecodeUint16(des[sortKeyIdxOffset : sortKeyIdxOffset+sortKeyIdxLen])
}

func (des ObjectStats) OriginSize() uint32 {
	return Extent(des[ExtentOff : ExtentOff+ExtentLen]).OriginSize()
}

func (des ObjectStats) CompSize() uint32 {
	return Extent(des[ExtentOff : ExtentOff+ExtentLen]).Length()
}

func (des ObjectStats) BlkCnt() uint32 {
	return types.DecodeUint32(des[blkCntOffset : blkCntOffset+blkCntLen])
}

func (des ObjectStats) DataSortKeyZoneMap() ZoneMap {
	return ZoneMap(des[dataZoneMapOffset : dataZoneMapOffset+zoneMapLen])
}

func (des ObjectStats) TombstoneSortKeyZoneMap() ZoneMap {
	return ZoneMap(des[tombStoneZoneMapOffset : tombStoneZoneMapOffset+zoneMapLen])
}

func (des ObjectStats) Extent() Extent {
	return Extent(des[extentOffset : extentOffset+ExtentLen])
}

func (des ObjectStats) Rows() uint32 {
	return types.DecodeUint32(des[rowCntOffset : rowCntOffset+rowCntLen])
}

func (des ObjectStats) String() string {
	return fmt.Sprintf("[object stats]: objName: %s; extent: %v; "+
		"blkCnt: %d; data zoneMap: %v; tombstone zoneMap: %v",
		des.ObjectName().String(), des.Extent().String(), des.BlkCnt(),
		des.DataSortKeyZoneMap(), des.TombstoneSortKeyZoneMap())
}
