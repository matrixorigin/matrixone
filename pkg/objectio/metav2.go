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
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type SchemaType uint16

const (
	SchemaData      SchemaType = 0
	SchemaTombstone SchemaType = 1

	CkpMeta         SchemaType = 100
	CkpSystemDB     SchemaType = 101
	CkpTxnNode      SchemaType = 102
	CkpDBDel        SchemaType = 103
	CkpDBDN         SchemaType = 104
	CkpSystemTable  SchemaType = 105
	CkpTblDN        SchemaType = 106
	CkpTblDel       SchemaType = 107
	CkpSystemColumn SchemaType = 108
	CkpColumnDel    SchemaType = 109
	CkpSegment      SchemaType = 110
	CkpSegmentDN    SchemaType = 111
	CkpDel          SchemaType = 112
	CkpBlkMeta      SchemaType = 113
	CkpBlkDN        SchemaType = 114
)

const (
	dataMetaCount         = 2
	dataMetaOffset        = 4
	tombstoneMetaCountOff = dataMetaCount + dataMetaOffset
	tombstoneMetaCount    = 2
	tombstoneMetaOffset   = 4
	metaDummyOff          = tombstoneMetaCountOff + tombstoneMetaCount + tombstoneMetaOffset
	metaDummy             = 20

	metaHeaderLen = metaDummyOff + metaDummy
)

const InvalidSchemaType = 0xFF

func ConvertToSchemaType(ckpIdx uint16) SchemaType {
	return 100 + SchemaType(ckpIdx)
}

type MetaHeader []byte

func buildMetaHeaderV1() MetaHeader {
	var buf [metaHeaderLen]byte
	return buf[:]
}

func (mh MetaHeader) DataMetaCount() uint16 {
	return types.DecodeUint16(mh[:dataMetaCount])
}

func (mh MetaHeader) TombstoneMetaCount() uint16 {
	return types.DecodeUint16(mh[tombstoneMetaCountOff : tombstoneMetaCountOff+tombstoneMetaCount])
}

func (mh MetaHeader) DataMeta() (BlockHeader, bool) {
	if mh.DataMetaCount() == 0 {
		return nil, false
	}
	offset := types.DecodeUint32(mh[dataMetaCount:dataMetaOffset])
	return BlockHeader(mh[offset : offset+headerLen]), true
}

func (mh MetaHeader) TombstoneMeta() (BlockHeader, bool) {
	if mh.TombstoneMetaCount() == 0 {
		return nil, false
	}
	offset := types.DecodeUint32(mh[tombstoneMetaCountOff+tombstoneMetaCount : metaDummyOff])
	return BlockHeader(mh[offset : offset+headerLen]), true
}

func (mh MetaHeader) SetDataMetaCount(count uint16) {
	copy(mh[:dataMetaCount], types.EncodeUint16(&count))
}

func (mh MetaHeader) SetDataMetaOffset(offset uint32) {
	copy(mh[dataMetaOffset:dataMetaOffset+headerLen], types.EncodeUint32(&offset))
}

func (mh MetaHeader) SetTombstoneMetaCount(count uint16) {
	copy(mh[tombstoneMetaCountOff:tombstoneMetaCountOff+tombstoneMetaCount], types.EncodeUint16(&count))
}

func (mh MetaHeader) SetTombstoneMetaOffset(offset uint32) {
	copy(mh[tombstoneMetaCountOff:tombstoneMetaCountOff+tombstoneMetaCount], types.EncodeUint32(&offset))
}

func (mh MetaHeader) SubMetaIndex() SubMetaIndex {
	return SubMetaIndex(mh[metaHeaderLen:])
}

const (
	schemaCountLen   = 2
	schemaType       = 2
	schemaBlockCount = 2
	schemaMetaOffset = 4
	typePosLen       = schemaType + schemaBlockCount + schemaMetaOffset
)

type SubMetaIndex []byte

func BuildSubMetaIndex(count uint16) SubMetaIndex {
	length := schemaCountLen + count*typePosLen
	buf := make([]byte, length)
	return buf[:]
}

func (oh SubMetaIndex) SubMetaCount() uint16 {
	return types.DecodeUint16(oh[:schemaCountLen])
}

func (oh SubMetaIndex) SetSubMetaCount(cnt uint16) {
	copy(oh[:schemaCountLen], types.EncodeUint16(&cnt))
}

func (oh SubMetaIndex) SubMeta(pos uint16) (BlockHeader, bool) {
	offStart := schemaCountLen + pos*typePosLen
	offEnd := schemaCountLen + pos*typePosLen + schemaType + schemaBlockCount
	offset := types.DecodeUint16(oh[offStart:offEnd])
	return BlockHeader(oh[offset : offset+headerLen]), true
}

func (oh SubMetaIndex) SubMetaTypes() []uint16 {
	cnt := oh.SubMetaCount()
	subMetaTypes := make([]uint16, cnt)
	for i := uint16(0); i < cnt; i++ {
		offStart := schemaCountLen + i*typePosLen
		offEnd := schemaCountLen + i*typePosLen + schemaType
		subMetaTypes[i] = types.DecodeUint16(oh[offStart:offEnd])
	}
	return subMetaTypes
}

func (oh SubMetaIndex) SetSchemaMeta(pos uint16, st uint16, count uint16) {
	offStart := schemaCountLen + pos*typePosLen
	offEnd := schemaCountLen + pos*typePosLen + schemaType
	copy(oh[offStart:offEnd], types.EncodeUint16(&st))
	copy(oh[offStart+schemaBlockCount:offEnd+schemaBlockCount], types.EncodeUint16(&count))
}

func (oh SubMetaIndex) Length() uint32 {
	return uint32(oh.SubMetaCount()*typePosLen + schemaCountLen)
}
