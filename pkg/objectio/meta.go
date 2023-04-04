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

import (
	"bytes"
	"io"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const HeaderSize = 64
const ColumnMetaSize = 136
const ExtentSize = 16

const ObjectColumnMetaSize = 72
const FooterSize = 8 /*Magic*/ + 4 /*metaStart*/ + 4 /*metaLen*/

type ObjectMeta struct {
	Rows     uint32             // total rows
	ColMetas []ObjectColumnMeta // meta for every column of all blocks
	BlkMetas []BlockObject      // meta for every block
}

// 4 + 4 + 64 = 72 bytes
type ObjectColumnMeta struct {
	Ndv     uint32
	NullCnt uint32
	Zonemap ZoneMap
}

func (meta *ObjectColumnMeta) Write(w io.Writer) (err error) {
	if _, err = w.Write(types.EncodeUint32(&meta.Ndv)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint32(&meta.NullCnt)); err != nil {
		return
	}

	var zmbuf []byte
	if meta.Zonemap.data == nil {
		var buf [ZoneMapMinSize + ZoneMapMaxSize]byte
		zmbuf = buf[:]
	} else {
		zmbuf = meta.Zonemap.data.([]byte)
	}
	if _, err = w.Write(zmbuf); err != nil {
		return
	}
	return
}

func (meta *ObjectColumnMeta) Read(bs []byte) (err error) {
	meta.Ndv = types.DecodeUint32(bs)
	meta.NullCnt = types.DecodeUint32(bs[4:])
	bs = bs[8:]
	meta.Zonemap.data = bs[:64]
	return
}

// +---------------------------------------------------------------------------------------------+
// |                                           Header                                            |
// +-------------+---------------+--------------+---------------+---------------+----------------+
// | TableID(8B) | SegmentID(8B) | BlockID(8B)  | ColumnCnt(2B) | Reserved(34B) |  Chksum(4B)    |
// +-------------+---------------+--------------+---------------+---------------+----------------+
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
// Reserved = 34 bytes reserved space
type BlockMeta struct {
	header BlockHeader
	name   string
}

func (bm *BlockMeta) GetName() string {
	return bm.name
}

func (bm *BlockMeta) GetHeader() BlockHeader {
	return bm.header
}

type BlockHeader struct {
	tableId     uint64
	segmentId   uint64
	blockId     uint64
	columnCount uint16
	dummy       [34]byte
	checksum    uint32
}

func (bh *BlockHeader) GetTableId() uint64 {
	return bh.tableId
}
func (bh *BlockHeader) GetSegmentId() uint64 {
	return bh.segmentId
}
func (bh *BlockHeader) GetBlockId() uint64 {
	return bh.blockId
}
func (bh *BlockHeader) GetColumnCount() uint16 {
	return bh.columnCount
}

func (bh *BlockHeader) Marshal() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(bh)), HeaderSize)
}

func (bh *BlockHeader) Unmarshal(data []byte) {
	h := *(*BlockHeader)(unsafe.Pointer(&data[0]))
	bh.tableId = h.tableId
	bh.segmentId = h.segmentId
	bh.blockId = h.blockId
	bh.columnCount = h.columnCount
	bh.dummy = h.dummy
	bh.checksum = h.checksum
}

// +---------------------------------------------------------------------------------------------------------------+
// |                                                    ColumnMeta                                                 |
// +--------+-------+----------+--------+---------+--------+--------+------------+---------+----------+------------+
// |Type(1B)|Idx(2B)| Algo(1B) |Offset(4B)|Size(4B)|oSize(4B)|Min(32B)|Max(32B)|BFoffset(4b)|BFlen(4b)|BFoSize(4B) |
// +--------+-------+----------+--------+---------+--------+--------+------------+---------+----------+------------+
// |                                        Reserved(32B)                                             | Chksum(4B) |
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
// Reserved = 32 bytes reserved space
type ColumnMeta struct {
	typ         uint8
	idx         uint16
	alg         uint8
	location    Extent
	zoneMap     ZoneMap
	bloomFilter Extent
	//dummy       [32]byte
	checksum uint32
}

func (cm *ColumnMeta) GetType() uint8 {
	return cm.typ
}

func (cm *ColumnMeta) GetIdx() uint16 {
	return cm.idx
}

func (cm *ColumnMeta) GetAlg() uint8 {
	return cm.alg
}

func (cm *ColumnMeta) GetLocation() Extent {
	return cm.location
}

func (cm *ColumnMeta) GetZoneMap() ZoneMap {
	return cm.zoneMap
}

func (cm *ColumnMeta) GetBloomFilter() Extent {
	return cm.bloomFilter
}

func (cm *ColumnMeta) Marshal() []byte {
	var buffer bytes.Buffer
	buffer.Write(types.EncodeFixed(cm.typ))
	buffer.Write(types.EncodeFixed(cm.alg))
	buffer.Write(types.EncodeFixed(cm.idx))
	buffer.Write(cm.location.Marshal())
	var buf []byte
	if cm.zoneMap.data == nil {
		buf = make([]byte, ZoneMapMinSize+ZoneMapMaxSize)
		cm.zoneMap.data = buf
	}
	buffer.Write(cm.zoneMap.data.([]byte))
	buffer.Write(cm.bloomFilter.Marshal())
	dummy := make([]byte, 32)
	buffer.Write(dummy)
	buffer.Write(types.EncodeFixed(cm.checksum))
	return buffer.Bytes()
}

func (cm *ColumnMeta) Unmarshal(data []byte) error {
	cm.typ = types.DecodeUint8(data[:1])
	data = data[1:]
	cm.alg = types.DecodeUint8(data[:1])
	data = data[1:]
	cm.idx = types.DecodeUint16(data[:2])
	data = data[2:]
	cm.location.Unmarshal(data)
	data = data[ExtentSize:]
	cm.zoneMap.idx = cm.idx
	t := types.T(cm.typ).ToType()
	if err := cm.zoneMap.Unmarshal(data[:64], t); err != nil {
		return err
	}
	data = data[64:]
	cm.bloomFilter.Unmarshal(data)
	// 32 skip dummy
	data = data[ExtentSize+32:]
	cm.checksum = types.DecodeUint32(data[:4])
	return nil
}

type Header struct {
	magic   uint64
	version uint16
	//dummy   [22]byte
}

type Footer struct {
	magic     uint64
	metaStart uint32
	metaLen   uint32
}

func (f *Footer) Marshal() []byte {
	var buffer bytes.Buffer
	buffer.Write(types.EncodeUint32(&f.metaStart))
	buffer.Write(types.EncodeUint32(&f.metaLen))
	buffer.Write(types.EncodeUint64(&f.magic))
	return buffer.Bytes()
}

func (f *Footer) Unmarshal(data []byte) error {
	f.metaStart = types.DecodeUint32(data)
	data = data[4:]
	f.metaLen = types.DecodeUint32(data)
	data = data[4:]
	f.magic = types.DecodeUint64(data)
	return nil
}
