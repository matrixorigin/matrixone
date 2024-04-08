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

type DataMetaType uint16

const (
	SchemaData      DataMetaType = 0
	SchemaTombstone DataMetaType = 1

	CkpMetaStart DataMetaType = 2

	// CkpMetaEnd = CkpMetaStart + `MaxIDX`
	CkpMetaEnd DataMetaType = 28 + 2
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

func ConvertToSchemaType(ckpIdx uint16) DataMetaType {
	return CkpMetaStart + DataMetaType(ckpIdx)
}

func ConvertToCkpIdx(dataType uint16) uint16 {
	return dataType - uint16(CkpMetaStart)
}

type objectMetaV2 []byte

func (mh objectMetaV2) MustGetMeta(metaType DataMetaType) objectDataMetaV1 {
	if metaType == SchemaData {
		return mh.MustDataMeta()
	} else if metaType == SchemaTombstone {
		return mh.MustTombstoneMeta()
	}
	return nil
}

func (mh objectMetaV2) HeaderLength() uint32 {
	return metaHeaderLen
}

func (mh objectMetaV2) DataMetaCount() uint16 {
	return types.DecodeUint16(mh[:dataMetaCount])
}

func (mh objectMetaV2) TombstoneMetaCount() uint16 {
	return types.DecodeUint16(mh[tombstoneMetaCountOff : tombstoneMetaCountOff+tombstoneMetaCount])
}

func (mh objectMetaV2) DataMeta() (objectDataMetaV1, bool) {
	if mh.DataMetaCount() == 0 {
		return nil, false
	}
	offset := types.DecodeUint32(mh[dataMetaCount:tombstoneMetaCountOff])
	return objectDataMetaV1(mh[offset:]), true
}

func (mh objectMetaV2) MustDataMeta() objectDataMetaV1 {
	meta, ok := mh.DataMeta()
	if !ok {
		panic("no data meta")
	}
	return meta
}

func (mh objectMetaV2) TombstoneMeta() (objectDataMetaV1, bool) {
	if mh.TombstoneMetaCount() == 0 {
		return nil, false
	}
	offset := types.DecodeUint32(mh[tombstoneMetaCountOff+tombstoneMetaCount : metaDummyOff])
	return objectDataMetaV1(mh[offset:]), true
}

func (mh objectMetaV2) MustTombstoneMeta() objectDataMetaV1 {
	meta, ok := mh.TombstoneMeta()
	if !ok {
		meta = mh.MustDataMeta()
	}
	return meta
}

func (mh objectMetaV2) SetDataMetaCount(count uint16) {
	copy(mh[:dataMetaCount], types.EncodeUint16(&count))
}

func (mh objectMetaV2) SetDataMetaOffset(offset uint32) {
	copy(mh[dataMetaCount:dataMetaCount+dataMetaOffset], types.EncodeUint32(&offset))
}

func (mh objectMetaV2) SetTombstoneMetaCount(count uint16) {
	copy(mh[tombstoneMetaCountOff:tombstoneMetaCountOff+tombstoneMetaCount], types.EncodeUint16(&count))
}

func (mh objectMetaV2) SetTombstoneMetaOffset(offset uint32) {
	copy(mh[tombstoneMetaCountOff+tombstoneMetaCount:tombstoneMetaCountOff+tombstoneMetaCount+tombstoneMetaOffset], types.EncodeUint32(&offset))
}

func (mh objectMetaV2) SubMeta(pos uint16) (objectDataMetaV1, bool) {
	offStart := schemaCountLen + uint32(pos)*typePosLen + schemaType + schemaBlockCount + metaHeaderLen
	offEnd := schemaCountLen + uint32(pos)*typePosLen + typePosLen + metaHeaderLen
	offset := types.DecodeUint32(mh[offStart:offEnd])
	return objectDataMetaV1(mh[offset:]), true
}

func (mh objectMetaV2) SubMetaCount() uint16 {
	return types.DecodeUint16(mh[metaHeaderLen : metaHeaderLen+schemaCountLen])
}

func (mh objectMetaV2) SubMetaIndex() SubMetaIndex {
	return SubMetaIndex(mh[metaHeaderLen:])
}

func (mh objectMetaV2) SubMetaTypes() []uint16 {
	cnt := mh.SubMetaCount()
	subMetaTypes := make([]uint16, cnt)
	for i := uint16(0); i < cnt; i++ {
		offStart := schemaCountLen + i*typePosLen + metaHeaderLen
		offEnd := schemaCountLen + i*typePosLen + schemaType + metaHeaderLen
		subMetaTypes[i] = types.DecodeUint16(mh[offStart:offEnd])
	}
	return subMetaTypes
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

func (oh SubMetaIndex) SetSubMetaCount(cnt uint16) {
	copy(oh[:schemaCountLen], types.EncodeUint16(&cnt))
}

func (oh SubMetaIndex) SubMetaCount() uint16 {
	return types.DecodeUint16(oh[:schemaCountLen])
}

func (oh SubMetaIndex) SetSchemaMeta(pos uint16, st uint16, count uint16, offset uint32) {
	offStart := schemaCountLen + uint32(pos)*typePosLen
	offEnd := schemaCountLen + uint32(pos)*typePosLen + schemaType
	copy(oh[offStart:offEnd], types.EncodeUint16(&st))
	copy(oh[offStart+schemaType:offEnd+schemaBlockCount], types.EncodeUint16(&count))
	copy(oh[offStart+schemaType+schemaBlockCount:offEnd+schemaBlockCount+schemaMetaOffset], types.EncodeUint32(&offset))
}

func (oh SubMetaIndex) Length() uint32 {
	return uint32(oh.SubMetaCount()*typePosLen + schemaCountLen)
}
