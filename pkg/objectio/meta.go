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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var EmptyZm = [64]byte{}

const FooterSize = 64
const HeaderSize = 64

type objectDataMetaV1 []byte

func buildObjectDataMetaV1(count uint16) objectDataMetaV1 {
	length := headerLen + uint32(count)*colMetaLen
	buf := make([]byte, length)
	return buf[:]
}

func (o objectDataMetaV1) BlockHeader() BlockHeader {
	return BlockHeader(o[:headerLen])
}

func (o objectDataMetaV1) MustGetColumn(seqnum uint16) ColumnMeta {
	if seqnum > o.BlockHeader().MaxSeqnum() {
		return BuildObjectColumnMeta()
	}
	return GetObjectColumnMeta(seqnum, o[headerLen:])
}

func (o objectDataMetaV1) AddColumnMeta(idx uint16, col ColumnMeta) {
	offset := headerLen + uint32(idx)*colMetaLen
	copy(o[offset:offset+colMetaLen], col)
}

func (o objectDataMetaV1) Length() uint32 {
	return headerLen + uint32(o.BlockHeader().MetaColumnCount())*colMetaLen
}

func (o objectDataMetaV1) BlockCount() uint32 {
	return uint32(o.BlockHeader().Sequence())
}

func (o objectDataMetaV1) BlockIndex() BlockIndex {
	offset := o.Length()
	return BlockIndex(o[offset:])
}

func (o objectDataMetaV1) GetBlockMeta(id uint32) BlockObject {
	BlockID := id - uint32(o.BlockHeader().StartID())
	offset, length := o.BlockIndex().BlockMetaPos(BlockID)
	return BlockObject(o[offset : offset+length])
}

func (o objectDataMetaV1) GetColumnMeta(blk uint32, seqnum uint16) ColumnMeta {
	return o.GetBlockMeta(blk).MustGetColumn(seqnum)
}

func (o objectDataMetaV1) IsEmpty() bool {
	return len(o) == 0
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

// caller makes sure the data has column meta fot the given seqnum
func GetObjectColumnMeta(seqnum uint16, data []byte) ColumnMeta {
	offset := uint32(seqnum) * colMetaLen
	return data[offset : offset+colMetaLen]
}

func BuildObjectColumnMeta() ColumnMeta {
	var buf [colMetaLen]byte
	return buf[:]
}

const (
	sequenceLen        = 2
	dbIDLen            = 8
	tableIDOff         = dbIDLen
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
	metaColCntOff      = zoneMapCheckSumOff + zoneMapCheckSumLen
	metaColCntLen      = 2
	maxSeqOff          = metaColCntOff + metaColCntLen
	maxSeqLen          = 2
	startIDOff         = maxSeqOff + maxSeqLen
	startIDLen         = 2
	appendableOff      = startIDOff + startIDLen
	appendableLen      = 1
	sortKeyOff         = appendableOff + appendableLen
	sortKeyLen         = 2
	bloomFilterTypeOff = sortKeyOff + sortKeyLen
	bloomFilterTypeLen = 1
	headerDummyOff     = bloomFilterTypeOff + bloomFilterTypeLen
	headerDummyLen     = 29
	headerLen          = headerDummyOff + headerDummyLen
)

type BlockHeader []byte

func BuildBlockHeader() BlockHeader {
	var buf [headerLen]byte
	return buf[:]
}

func (bh BlockHeader) TableID() uint64 {
	return types.DecodeUint64(bh[tableIDOff:])
}

func (bh BlockHeader) SetTableID(id uint64) {
	copy(bh[tableIDOff:], types.EncodeUint64(&id))
}

func (bh BlockHeader) BlockID() *Blockid {
	return (*Blockid)(unsafe.Pointer(&bh[blockIDOff]))
}

func (bh BlockHeader) SetBlockID(id *Blockid) {
	copy(bh[blockIDOff:blockIDOff+blockIDLen], id[:])
}

func (bh BlockHeader) ShortName() *ObjectNameShort {
	return (*ObjectNameShort)(unsafe.Pointer(&bh[blockIDOff]))
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

func (bh BlockHeader) MetaColumnCount() uint16 {
	return types.DecodeUint16(bh[metaColCntOff : metaColCntOff+metaColCntLen])
}

func (bh BlockHeader) SetMetaColumnCount(count uint16) {
	copy(bh[metaColCntOff:metaColCntOff+metaColCntLen], types.EncodeUint16(&count))
}

func (bh BlockHeader) MaxSeqnum() uint16 {
	return types.DecodeUint16(bh[maxSeqOff : maxSeqOff+maxSeqLen])
}

func (bh BlockHeader) SetMaxSeqnum(seqnum uint16) {
	copy(bh[maxSeqOff:maxSeqOff+maxSeqLen], types.EncodeUint16(&seqnum))
}

func (bh BlockHeader) StartID() uint16 {
	return types.DecodeUint16(bh[startIDOff : startIDOff+startIDLen])
}

func (bh BlockHeader) SetStartID(id uint16) {
	copy(bh[startIDOff:startIDOff+startIDLen], types.EncodeUint16(&id))
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

func (bh BlockHeader) BFExtent() Extent {
	return Extent(bh[bloomFilterOff : bloomFilterOff+bloomFilterLen])
}

func (bh BlockHeader) SetBFExtent(location Extent) {
	copy(bh[bloomFilterOff:bloomFilterOff+bloomFilterLen], location)
}

func (bh BlockHeader) SetAppendable(appendable bool) {
	copy(bh[appendableOff:appendableOff+appendableLen], types.EncodeBool(&appendable))
}

func (bh BlockHeader) Appendable() bool {
	return types.DecodeBool(bh[appendableOff : appendableOff+appendableLen])
}

func (bh BlockHeader) SetSortKey(idx uint16) {
	copy(bh[sortKeyOff:sortKeyOff+sortKeyLen], types.EncodeUint16(&idx))
}

func (bh BlockHeader) SortKey() uint16 {
	return types.DecodeUint16(bh[sortKeyOff : sortKeyOff+sortKeyLen])
}

func (bh BlockHeader) SetBloomFilterType(typ uint8) {
	copy(bh[bloomFilterTypeOff:bloomFilterTypeOff+bloomFilterTypeLen], types.EncodeUint8(&typ))
}

func (bh BlockHeader) BloomFilterType() uint8 {
	return types.DecodeUint8(bh[bloomFilterTypeOff : bloomFilterTypeOff+bloomFilterTypeLen])
}

func (bh BlockHeader) IsEmpty() bool {
	return len(bh) == 0
}

type BloomFilter []byte

func (bf BloomFilter) Size() int {
	return len(bf)
}

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

func (bf BloomFilter) GetObjectBloomFilter() []byte {
	return bf.GetBloomFilter(bf.BlockCount())
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

func (h Header) SetExtent(location Extent) {
	copy(h[8+2:8+2+ExtentSize], location)
}

func (h Header) Extent() Extent {
	return Extent(h[8+2 : 8+2+ExtentSize])
}

func (h Header) SetSchemaVersion(ver uint32) {
	copy(h[8+2+ExtentSize:8+2+ExtentSize+4], types.EncodeUint32(&ver))
}

func (h Header) SchemaVersion(ver uint32) {
	types.DecodeUint32(h[8+2+ExtentSize : 8+2+ExtentSize+4])
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

func IsSameObjectLocVsMeta(location Location, meta ObjectDataMeta) bool {
	if len(location) == 0 || len(meta) == 0 {
		return false
	}
	return location.ShortName().Equal(meta.BlockHeader().ShortName()[:])
}

func IsSameObjectLocVsShort(location Location, short *ObjectNameShort) bool {
	if len(location) == 0 || short == nil {
		return false
	}
	return location.ShortName().Equal(short[:])
}

// test used
func BuildMetaData(blkCount, colCount uint16) objectDataMetaV1 {
	var meta bytes.Buffer
	length := uint32(0)
	seqnum := NewSeqnums(nil)
	seqnum.InitWithColCnt(int(colCount))
	objectMeta := BuildObjectMeta(colCount)
	objectMeta.BlockHeader().SetColumnCount(colCount)
	objectMeta.BlockHeader().SetMetaColumnCount(colCount)
	objectMeta.BlockHeader().SetSequence(blkCount)
	length += objectMeta.Length()
	blockIndex := BuildBlockIndex(uint32(blkCount))
	blockIndex.SetBlockCount(uint32(blkCount))
	length += blockIndex.Length()
	var blkMetaBuf bytes.Buffer
	for i := uint16(0); i < blkCount; i++ {
		blkMeta := NewBlock(seqnum)
		blkMeta.BlockHeader().SetSequence(i)
		n := uint32(len(blkMeta))
		blockIndex.SetBlockMetaPos(uint32(i), length, n)
		length += n
		blkMetaBuf.Write(blkMeta)
	}
	meta.Write(objectMeta)
	meta.Write(blockIndex)
	meta.Write(blkMetaBuf.Bytes())
	return meta.Bytes()
}
