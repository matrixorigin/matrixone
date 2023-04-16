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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const FooterSize = 64
const HeaderSize = 64

type ObjectMeta []byte

func BuildObjectMeta(count uint16) ObjectMeta {
	length := headerLen + uint32(count)*colMetaLen
	buf := make([]byte, length)
	meta := ObjectMeta(buf)
	meta.BlockHeader().setVersion(Version)
	return buf[:]
}

func (o ObjectMeta) BlockHeader() BlockHeader {
	return BlockHeader(o[:headerLen])
}

func (o ObjectMeta) ObjectColumnMeta(idx uint16) ColumnMeta {
	return GetObjectColumnMeta(idx, o[headerLen:])
}

func (o ObjectMeta) AddColumnMeta(idx uint16, col ColumnMeta) {
	offset := headerLen + uint32(idx)*colMetaLen
	copy(o[offset:offset+colMetaLen], col)
}

func (o ObjectMeta) Length() uint32 {
	return headerLen + uint32(o.BlockHeader().ColumnCount())*colMetaLen
}

func (o ObjectMeta) BlockCount() uint32 {
	return uint32(o.BlockHeader().Sequence())
}

func (o ObjectMeta) BlockIndex() BlockIndex {
	offset := o.Length()
	return BlockIndex(o[offset:])
}

func (o ObjectMeta) GetBlockMeta(id uint32) BlockObject {
	offset, length := o.BlockIndex().BlockMetaPos(id)
	return BlockObject(o[offset : offset+length])
}

func (o ObjectMeta) GetColumnMeta(idx uint16, id uint32) ColumnMeta {
	return o.GetBlockMeta(id).ColumnMeta(idx)
}

const (
	blockCountLen = 4
	blockOffset   = 4
	blockLen      = 4
	posLen        = blockOffset + blockLen
)

type BlockIndex []byte

func BuildBlockIndex(count uint32) BlockIndex {
	length := blockCountLen + uint32(count)*posLen
	buf := make([]byte, length)
	return buf[:]
}

func (oh BlockIndex) BlockCount() uint32 {
	return types.DecodeUint32(oh[:blockCountLen])
}

func (oh BlockIndex) SetBlockCount(cnt uint32) {
	copy(oh[:blockCountLen], types.EncodeUint32(&cnt))
}

func (oh BlockIndex) BlockMetaPos(BlockID uint32) (uint32, uint32) {
	offStart := blockCountLen + BlockID*posLen
	offEnd := blockCountLen + BlockID*posLen + blockOffset
	return types.DecodeUint32(oh[offStart:offEnd]), types.DecodeUint32(oh[offStart+blockLen : offEnd+blockLen])
}

func (oh BlockIndex) SetBlockMetaPos(BlockID uint32, offset, length uint32) {
	offStart := blockCountLen + BlockID*posLen
	offEnd := blockCountLen + BlockID*posLen + blockOffset
	copy(oh[offStart:offEnd], types.EncodeUint32(&offset))
	copy(oh[offStart+blockLen:offEnd+blockLen], types.EncodeUint32(&length))
}

func (oh BlockIndex) Length() uint32 {
	return oh.BlockCount()*posLen + blockCountLen
}

func GetObjectColumnMeta(idx uint16, data []byte) ColumnMeta {
	offset := uint32(idx) * colMetaLen
	return data[offset : offset+colMetaLen]
}

func BuildObjectColumnMeta() ColumnMeta {
	var buf [colMetaLen]byte
	return buf[:]
}

const (
	sequenceLen        = 2
	dbIDOff            = versionOff + versionLen
	dbIDLen            = 8
	tableIDOff         = dbIDOff + dbIDLen
	tableIDLen         = 8
	blockIDOff         = tableIDOff + tableIDLen
	blockIDLen         = types.BlockidSize
	rowsOff            = blockIDOff + blockIDLen
	rowsLen            = 4
	columnCountOff     = rowsOff + rowsLen
	columnCountLen     = 2
	metaLocationOff    = columnCountOff + columnCountLen
	metaLocationLen    = ExtentSize
	bloomFilterOff     = metaLocationOff + metaLocationLen
	bloomFilterLen     = ExtentSize
	bloomCheckSumOff   = bloomFilterOff + bloomFilterLen
	bloomCheckSumLen   = 4
	zoneMapAreaOff     = bloomCheckSumOff + bloomCheckSumLen
	zoneMapAreaLen     = ZoneMapSize
	zoneMapCheckSumOff = zoneMapAreaOff + zoneMapAreaLen
	zoneMapCheckSumLen = 4
	headerDummyOff     = zoneMapCheckSumOff + zoneMapCheckSumLen
	headerDummyLen     = 39
	headerLen          = headerDummyOff + headerDummyLen
)

type BlockObject []byte

func BuildBlockMeta(count uint16) BlockObject {
	length := headerLen + uint32(count)*colMetaLen
	buf := make([]byte, length)
	meta := BlockObject(buf)
	meta.BlockHeader().setVersion(Version)
	return meta
}

func (bm BlockObject) BlockHeader() BlockHeader {
	return BlockHeader(bm[:headerLen])
}

func (bm BlockObject) SetBlockMetaHeader(header BlockHeader) {
	copy(bm[:headerLen], header)
}

func (bm BlockObject) ColumnMeta(idx uint16) ColumnMeta {
	return GetColumnMeta(idx, bm)
}

func (bm BlockObject) AddColumnMeta(idx uint16, col ColumnMeta) {
	offset := headerLen + uint32(idx)*colMetaLen
	copy(bm[offset:offset+colMetaLen], col)
}

func (bm BlockObject) IsEmpty() bool {
	return len(bm) == 0
}

type BlockHeader []byte

func BuildBlockHeader() BlockHeader {
	var buf [headerLen]byte
	return buf[:]
}

func (bh BlockHeader) Version() uint16 {
	return types.DecodeUint16(bh[:typeLen])
}

func (bh BlockHeader) setVersion(version uint16) {
	copy(bh[:typeLen], types.EncodeUint16(&version))
}

func (bh BlockHeader) TableID() uint64 {
	return types.DecodeUint64(bh[:tableIDLen])
}

func (bh BlockHeader) SetTableID(id uint64) {
	copy(bh[:tableIDLen], types.EncodeUint64(&id))
}

func (bh BlockHeader) BlockID() *Blockid {
	return (*Blockid)(unsafe.Pointer(&bh[blockIDOff]))
}

func (bh BlockHeader) SetBlockID(id *Blockid) {
	copy(bh[blockIDOff:blockIDOff+blockIDLen], id[:])
}

func (bh BlockHeader) Sequence() uint16 {
	return types.DecodeUint16(bh[rowsOff-sequenceLen : rowsOff])
}

func (bh BlockHeader) SetSequence(seq uint16) {
	copy(bh[rowsOff-sequenceLen:rowsOff], types.EncodeUint16(&seq))
}

func (bh BlockHeader) Rows() uint32 {
	return types.DecodeUint32(bh[rowsOff : rowsOff+rowsLen])
}

func (bh BlockHeader) SetRows(rows uint32) {
	copy(bh[rowsOff:rowsOff+rowsLen], types.EncodeUint32(&rows))
}

func (bh BlockHeader) ColumnCount() uint16 {
	return types.DecodeUint16(bh[columnCountOff : columnCountOff+columnCountLen])
}

func (bh BlockHeader) SetColumnCount(count uint16) {
	copy(bh[columnCountOff:columnCountOff+columnCountLen], types.EncodeUint16(&count))
}

func (bh BlockHeader) MetaLocation() Extent {
	return Extent(bh[metaLocationOff : metaLocationOff+metaLocationLen])
}

func (bh BlockHeader) SetMetaLocation(location Extent) {
	copy(bh[metaLocationOff:metaLocationOff+metaLocationLen], location)
}

func (bh BlockHeader) ZoneMapArea() Extent {
	return Extent(bh[zoneMapAreaOff : zoneMapAreaOff+zoneMapAreaLen])
}

func (bh BlockHeader) SetZoneMapArea(location Extent) {
	copy(bh[zoneMapAreaOff:zoneMapAreaOff+zoneMapAreaLen], location)
}

func (bh BlockHeader) BloomFilter() Extent {
	return Extent(bh[bloomFilterOff : bloomFilterOff+bloomFilterLen])
}

func (bh BlockHeader) SetBloomFilter(location Extent) {
	copy(bh[bloomFilterOff:bloomFilterOff+bloomFilterLen], location)
}

func (bh BlockHeader) IsEmpty() bool {
	return len(bh) == 0
}

type BloomFilter []byte

func (bf BloomFilter) BlockCount() uint32 {
	return types.DecodeUint32(bf[:blockCountLen])
}

func (bf BloomFilter) GetBloomFilter(BlockID uint32) []byte {
	offStart := blockCountLen + BlockID*posLen
	offEnd := blockCountLen + BlockID*posLen + blockOffset
	offset := types.DecodeUint32(bf[offStart:offEnd])
	length := types.DecodeUint32(bf[offStart+blockLen : offEnd+blockLen])
	return bf[offset : offset+length]
}

type ZoneMapArea []byte

func (zma ZoneMapArea) BlockCount() uint32 {
	return types.DecodeUint32(zma[:blockCountLen])
}

func (zma ZoneMapArea) GetZoneMap(idx uint16, BlockID uint32) ZoneMap {
	offStart := blockCountLen + BlockID*posLen
	offEnd := blockCountLen + BlockID*posLen + blockOffset
	blockOff := types.DecodeUint32(zma[offStart:offEnd])
	blockLength := types.DecodeUint32(zma[offStart+blockLen : offEnd+blockLen])
	offset := blockOff + uint32(idx)*ZoneMapSize
	return ZoneMap(zma[offset : offset+blockLength])

}

type Header []byte

func BuildHeader() Header {
	var buf [HeaderSize]byte
	magic := uint64(Magic)
	version := uint16(Version)
	copy(buf[:8], types.EncodeUint64(&magic))
	copy(buf[8:8+2], types.EncodeUint16(&version))
	return buf[:]
}

func (h Header) SetLocation(location Extent) {
	copy(h[8+2:8+2+ExtentSize], location)
}

func (h Header) Location() Extent {
	return Extent(h[8+2 : 8+2+ExtentSize])
}

type Footer struct {
	dummy      [37]byte
	checksum   uint32
	metaExtent Extent
	version    uint16
	magic      uint64
}

func (f Footer) Marshal() []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(&f)), FooterSize)
}
