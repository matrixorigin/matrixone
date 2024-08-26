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

type objectMetaV3 []byte

func buildObjectMetaV3() objectMetaV3 {
	var buf [metaHeaderLen]byte
	return buf[:]
}

func (mh objectMetaV3) MustGetMeta(metaType DataMetaType) objectDataMetaV1 {
	if metaType == SchemaData {
		return mh.MustDataMeta()
	} else if metaType == SchemaTombstone {
		return mh.MustTombstoneMeta()
	}
	return nil
}

func (mh objectMetaV3) HeaderLength() uint32 {
	return metaHeaderLen
}

func (mh objectMetaV3) DataMetaCount() uint16 {
	return types.DecodeUint16(mh[:dataMetaCount])
}

func (mh objectMetaV3) TombstoneMetaCount() uint16 {
	return types.DecodeUint16(mh[tombstoneMetaCountOff : tombstoneMetaCountOff+tombstoneMetaCount])
}

func (mh objectMetaV3) DataMeta() (objectDataMetaV1, bool) {
	if mh.DataMetaCount() == 0 {
		return nil, false
	}
	offset := types.DecodeUint32(mh[dataMetaCount:tombstoneMetaCountOff])
	return objectDataMetaV1(mh[offset:]), true
}

func (mh objectMetaV3) MustDataMeta() objectDataMetaV1 {
	meta, ok := mh.DataMeta()
	if !ok {
		panic("no data meta")
	}
	return meta
}

func (mh objectMetaV3) TombstoneMeta() (objectDataMetaV1, bool) {
	if mh.TombstoneMetaCount() == 0 {
		return nil, false
	}
	offset := types.DecodeUint32(mh[tombstoneMetaCountOff+tombstoneMetaCount : metaDummyOff])
	return objectDataMetaV1(mh[offset:]), true
}

func (mh objectMetaV3) MustTombstoneMeta() objectDataMetaV1 {
	return mh.MustDataMeta()
	meta, ok := mh.TombstoneMeta()
	if !ok {
		panic("no tombstone meta")
	}
	return meta
}

func (mh objectMetaV3) SetDataMetaCount(count uint16) {
	copy(mh[:dataMetaCount], types.EncodeUint16(&count))
}

func (mh objectMetaV3) SetDataMetaOffset(offset uint32) {
	copy(mh[dataMetaCount:dataMetaCount+dataMetaOffset], types.EncodeUint32(&offset))
}

func (mh objectMetaV3) SetTombstoneMetaCount(count uint16) {
	copy(mh[tombstoneMetaCountOff:tombstoneMetaCountOff+tombstoneMetaCount], types.EncodeUint16(&count))
}

func (mh objectMetaV3) SetTombstoneMetaOffset(offset uint32) {
	copy(mh[tombstoneMetaCountOff+tombstoneMetaCount:tombstoneMetaCountOff+tombstoneMetaCount+tombstoneMetaOffset], types.EncodeUint32(&offset))
}

func (mh objectMetaV3) SubMeta(pos uint16) (objectDataMetaV1, bool) {
	offStart := schemaCountLen + uint32(pos)*typePosLen + schemaType + schemaBlockCount + metaHeaderLen
	offEnd := schemaCountLen + uint32(pos)*typePosLen + typePosLen + metaHeaderLen
	offset := types.DecodeUint32(mh[offStart:offEnd])
	return objectDataMetaV1(mh[offset:]), true
}

func (mh objectMetaV3) SubMetaCount() uint16 {
	return types.DecodeUint16(mh[metaHeaderLen : metaHeaderLen+schemaCountLen])
}

func (mh objectMetaV3) SubMetaIndex() SubMetaIndex {
	return SubMetaIndex(mh[metaHeaderLen:])
}

func (mh objectMetaV3) SubMetaTypes() []uint16 {
	cnt := mh.SubMetaCount()
	subMetaTypes := make([]uint16, cnt)
	for i := uint16(0); i < cnt; i++ {
		offStart := schemaCountLen + i*typePosLen + metaHeaderLen
		offEnd := schemaCountLen + i*typePosLen + schemaType + metaHeaderLen
		subMetaTypes[i] = types.DecodeUint16(mh[offStart:offEnd])
	}
	return subMetaTypes
}
