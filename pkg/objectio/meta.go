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

const ExtentSize = 16
const FooterSize = 8 /*Magic*/ + 4 /*metaStart*/ + 4 /*metaLen*/

type ObjectMeta []byte

func BuildObjectMeta(count uint16) ObjectMeta {
	length := headerLen + uint32(count)*objectColumnMetaLen
	buf := make([]byte, length)
	return buf[:]
}

func (o ObjectMeta) BlockHeader() BlockHeader {
	return BlockHeader(o[:headerLen])
}

func (o ObjectMeta) ObjectColumnMeta(idx uint16) ObjectColumnMeta {
	return GetObjectColumnMeta(idx, o[headerLen:])
}

func (o ObjectMeta) AddColumnMeta(idx uint16, col ObjectColumnMeta) {
	offset := headerLen + idx*objectColumnMetaLen
	copy(o[offset:offset+objectColumnMetaLen], col)
}

func (o ObjectMeta) Length() uint32 {
	return headerLen + uint32(o.BlockHeader().ColumnCount())*objectColumnMetaLen
}

func (o ObjectMeta) BlockCount() uint32 {
	return o.BlockHeader().BlockID()
}

func (o ObjectMeta) BlockIndex() BlockIndex {
	offset := o.Length()
	return BlockIndex(o[offset:])
}

func (o ObjectMeta) GetBlockMeta(id uint32) BlockObject {
	offset, length := o.BlockIndex().BlockMetaPos(id)
	return BlockObject(o[offset:length])
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

const (
	ndvLen              = 4
	nullCntOff          = ndvLen
	nullCntLen          = 4
	oZoneMapOff         = nullCntOff + nullCntLen
	oZoneMapLen         = 64
	objectColumnMetaLen = oZoneMapOff + oZoneMapLen
)

// ObjectColumnMeta len 4 + 4 + 64 = 72 bytes
type ObjectColumnMeta []byte

func GetObjectColumnMeta(idx uint16, data []byte) ObjectColumnMeta {
	offset := uint32(idx) * objectColumnMetaLen
	return data[offset : offset+objectColumnMetaLen]
}

func BuildObjectColumnMeta() ObjectColumnMeta {
	var buf [objectColumnMetaLen]byte
	return buf[:]
}

func (om ObjectColumnMeta) Ndv() uint32 {
	return types.DecodeUint32(om[:ndvLen])
}

func (om ObjectColumnMeta) SetNdv(cnt uint32) {
	copy(om[:ndvLen], types.EncodeUint32(&cnt))
}

func (om ObjectColumnMeta) NullCnt() uint32 {
	return types.DecodeUint32(om[nullCntOff : nullCntOff+nullCntLen])
}

func (om ObjectColumnMeta) SetNullCnt(cnt uint32) {
	copy(om[nullCntOff:nullCntOff+nullCntLen], types.EncodeUint32(&cnt))
}

func (om ObjectColumnMeta) ZoneMap() ZoneMap {
	return ZoneMap(om[oZoneMapOff : oZoneMapOff+oZoneMapLen])
}

func (om ObjectColumnMeta) SetZoneMap(zm []byte) {
	copy(om[oZoneMapOff:oZoneMapOff+oZoneMapLen], zm)
}

const (
	tableIDLen        = 8
	segmentIDOff      = tableIDLen
	segmentIDLen      = 8
	blockIDOff        = segmentIDOff + segmentIDLen
	blockIDLen        = 4
	rowsOff           = blockIDOff + blockIDLen
	rowsLen           = 4
	columnCountOff    = rowsOff + rowsLen
	columnCountLen    = 2
	headerDummyOff    = columnCountOff + columnCountLen
	headerDummyLen    = 18
	metaLocationOff   = headerDummyOff + headerDummyLen
	metaLocationLen   = 16
	headerCheckSumOff = metaLocationOff + metaLocationLen
	headerCheckSumLen = 4
	headerLen         = headerCheckSumOff + headerCheckSumLen
)

type BlockObject []byte

func BuildBlockMeta(count uint16) BlockObject {
	length := headerLen + uint32(count)*colMetaLen
	buf := make([]byte, length)
	return buf[:]
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
	offset := headerLen + idx*colMetaLen
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

func (bh BlockHeader) TableID() uint64 {
	return types.DecodeUint64(bh[:tableIDLen])
}

func (bh BlockHeader) SetTableID(id uint64) {
	copy(bh[:tableIDLen], types.EncodeUint64(&id))
}

func (bh BlockHeader) SegmentID() uint64 {
	return types.DecodeUint64(bh[segmentIDOff : segmentIDOff+segmentIDLen])
}

func (bh BlockHeader) SetSegmentID(id uint64) {
	copy(bh[segmentIDOff:segmentIDOff+segmentIDLen], types.EncodeUint64(&id))
}

func (bh BlockHeader) BlockID() uint32 {
	return types.DecodeUint32(bh[blockIDOff : blockIDOff+blockIDLen])
}

func (bh BlockHeader) SetBlockID(id uint32) {
	copy(bh[blockIDOff:blockIDOff+blockIDLen], types.EncodeUint32(&id))
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

func (bh BlockHeader) MetaLocation() *Extent {
	return (*Extent)(unsafe.Pointer(&bh[metaLocationOff]))
}

func (bh BlockHeader) SetMetaLocation(location *Extent) {
	copy(bh[metaLocationOff:metaLocationOff+metaLocationLen], location.Marshal())
}

func (bh BlockHeader) IsEmpty() bool {
	return len(bh) == 0
}

const (
	typeLen            = 1
	idxOff             = typeLen
	idxLen             = 2
	algOff             = idxOff + idxLen
	algLen             = 1
	locationOff        = algOff + algLen
	locationLen        = 16
	zoneMapOff         = locationOff + locationLen
	zoneMapLen         = 64
	bloomFilterOff     = zoneMapOff + zoneMapLen
	bloomFilterLen     = 16
	colMetaDummyOff    = bloomFilterOff + bloomFilterLen
	colMetaDummyLen    = 32
	colMetaChecksumOff = colMetaDummyOff + colMetaDummyLen
	colMetaChecksumLen = 4
	colMetaLen         = colMetaChecksumOff + colMetaChecksumLen
)

func GetColumnMeta(idx uint16, data []byte) ColumnMeta {
	offset := headerLen + uint32(idx)*colMetaLen
	return data[offset : offset+colMetaLen]
}

type ColumnMeta []byte

func BuildColumnMeta() ColumnMeta {
	var buf [colMetaLen]byte
	return buf[:]
}

func SetColumnMetaType(meta ColumnMeta, t uint8) {
	meta.setType(t)
}

func SetColumnMetaZoneMap(meta ColumnMeta, zm ZoneMap) {
	meta.setZoneMap(zm)
}

func (cm ColumnMeta) Type() uint8 {
	return types.DecodeUint8(cm[:typeLen])
}

func (cm ColumnMeta) setType(t uint8) {
	copy(cm[:typeLen], types.EncodeUint8(&t))
}

func (cm ColumnMeta) Idx() uint16 {
	return types.DecodeUint16(cm[idxOff : idxOff+idxLen])
}

func (cm ColumnMeta) setIdx(idx uint16) {
	copy(cm[idxOff:idxOff+idxLen], types.EncodeUint16(&idx))
}

func (cm ColumnMeta) Alg() uint8 {
	return types.DecodeUint8(cm[algOff : algOff+algLen])
}

func (cm ColumnMeta) setAlg(alg uint8) {
	copy(cm[algOff:algOff+algLen], types.EncodeUint8(&alg))
}

func (cm ColumnMeta) Location() *Extent {
	return (*Extent)(unsafe.Pointer(&cm[locationOff]))
}

func (cm ColumnMeta) setLocation(location *Extent) {
	copy(cm[locationOff:locationOff+locationLen], location.Marshal())
}

func (cm ColumnMeta) ZoneMap() ZoneMap {
	return ZoneMap(cm[zoneMapOff : zoneMapOff+zoneMapLen])
}

func (cm ColumnMeta) setZoneMap(zm ZoneMap) {
	copy(cm[zoneMapOff:zoneMapOff+zoneMapLen], zm)
}

func (cm ColumnMeta) BloomFilter() *Extent {
	return (*Extent)(unsafe.Pointer(&cm[bloomFilterOff]))
}

func (cm ColumnMeta) setBloomFilter(location *Extent) {
	copy(cm[bloomFilterOff:bloomFilterOff+bloomFilterLen], location.Marshal())
}

func (cm ColumnMeta) Checksum() uint32 {
	return types.DecodeUint32(cm[colMetaChecksumOff : colMetaChecksumOff+colMetaChecksumLen])
}

func (cm ColumnMeta) IsEmpty() bool {
	return len(cm) == 0
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
