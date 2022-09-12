// Copyright 2021 Matrix Origin
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

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"

// +---------------+---------------+----------------+----
// | <BlockMeta-1> | <BlockMeta-2> |  <BlockMeta-3> |....
// +---------------+---------------+----------------+----
//        |
//        |
// +---------------------------------------------------------------------------------------------+
// |                                           Header                                            |
// +-------------+---------------+--------------+---------------+------------+-------------------+
// | TableID(8B) | SegmentID(8B) | BlockID(8B)  | ColumnCnt(2B) | Chksum(4B) |  Reserved(33B)    |
// +-------------+---------------+--------------+---------------+------------+-------------------+
// |                                         ColumnMeta                                          |
// +---------------------------------------------------------------------------------------------+
// |                                         ColumnMeta                                          |
// +---------------------------------------------------------------------------------------------+
// |                                         ColumnMeta                                          |
// +---------------------------------------------------------------------------------------------+
// |                                         ..........                                          |
// +---------------------------------------------------------------------------------------------+
// Header Size = 64B
// TableID = Table ID for Block
// SegmentID = Segment ID
// BlockID = Block ID
// ColumnCnt = The number of column in the block
// Chksum = Block metadata checksum
// Reserved = 41 bytes reserved space
type BlockMeta struct {
	header *BlockHeader
	//columns []*ColumnMeta
}

type BlockHeader struct {
	tableId     uint64
	segmentId   uint64
	blockId     uint64
	columnCount uint16
	checksum    uint32
}

// +---------------------------------------------------------------------------------------------------------------+
// |                                                    ColumnMeta                                                 |
// +--------+-------+----------+--------+---------+--------+--------+------------+---------+-----------+-----------+
// |Type(1B)|Idx(2B)|Offset(4B)|Size(4B)|oSize(4B)|Min(32B)|Max(32B)|BFoffset(4b)|BFlen(4b)|BFoSize(4B)|Chksum(4B) |
// +--------+-------+----------+--------+---------+--------+--------+------------+---------+-----------+-----------+
// |                                                    Reserved(33B)                                              |
// +---------------------------------------------------------------------------------------------------------------+
// ColumnMeta Size = 128B
// Type = Metadata type, always 0, representing column meta, used for extension.
// Idx = Column index
// Algo = Type of compression algorithm for column data
// Offset = Offset of column data
// Size = Size of column data
// oSize = Original data size
// Min = Column min value
// Max = Column Max value
// BFoffset = Bloomfilter data offset
// Bflen = Bloomfilter data size
// BFoSize = Bloomfilter original data size
// Chksum = Data checksum
// Reserved = 33 bytes reserved space
type ColumnMeta struct {
	typ         uint8
	idx         uint16
	alg         uint8
	location    Extent
	zoneMap     *index.ZoneMap
	bloomFilter Extent
	checksum    uint32
}

type Header struct {
	magic   uint64
	version uint16
}

/*type Footer struct {
	alg   uint8
	metas []Extent
	magic uint64
}*/
