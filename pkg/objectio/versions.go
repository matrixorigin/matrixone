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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type ObjectWriter = objectWriterV1

type ObjectReader = objectReaderV1

type ObjectDataMeta = objectDataMetaV1

var (
	BuildObjectMeta        = buildObjectDataMetaV1
	NewObjectWriterSpecial = newObjectWriterSpecialV1
	NewObjectWriter        = newObjectWriterV1
	NewObjectReaderWithStr = newObjectReaderWithStrV1
	NewObjectReader        = newObjectReaderV1
)

const (
	IOET_ObjectMeta_V1 = 1
	IOET_ObjectMeta_V2 = 2
	IOET_ObjectMeta_V3 = 3
	IOET_ColumnData_V1 = 1
	IOET_ColumnData_V2 = 2
	// V3 has the same vector payload as V2, but is a compatibility fence for
	// tagged invalid DATE/DATETIME values. Readers predating V3 must reject it
	// instead of decoding the new scalar representation as a legacy value.
	IOET_ColumnData_V3  = 3
	IOET_BloomFilter_V1 = 1
	IOET_BloomFilter_V2 = 2
	IOET_ZoneMap_V1     = 1

	IOET_ObjectMeta_CurrVer  = IOET_ObjectMeta_V3
	IOET_ColumnData_CurrVer  = IOET_ColumnData_V2
	IOET_BloomFilter_CurrVer = IOET_BloomFilter_V2
	IOET_ZoneMap_CurrVer     = IOET_ZoneMap_V1
)

func init() {
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ObjMeta, IOET_ObjectMeta_V1}, nil, DecodeObjectMetaV1)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ObjMeta, IOET_ObjectMeta_V2}, nil, DecodeObjectMetaV2)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ObjMeta, IOET_ObjectMeta_V3}, nil, DecodeObjectMetaV3)
	// NOTE:
	// Break by MustVector. Need to update MustVector to support new version.
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ColData, IOET_ColumnData_V1}, EncodeColumnDataV1, DecodeColumnDataV1)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ColData, IOET_ColumnData_V2}, EncodeColumnDataV1, DecodeColumnDataV2)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ColData, IOET_ColumnData_V3}, EncodeColumnDataV1, DecodeColumnDataV2)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_BF, IOET_BloomFilter_V1}, nil, nil)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_BF, IOET_BloomFilter_V2}, nil, nil)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ZM, IOET_ZoneMap_V1}, nil, nil)
}

// columnDataVersion selects the compatibility fence only when the payload
// actually contains a tagged invalid temporal value. Ordinary columns retain
// V2 byte-for-byte compatibility with existing readers.
func columnDataVersion(vec *vector.Vector) uint16 {
	if vec != nil {
		switch vec.GetType().Oid {
		case types.T_date:
			for _, value := range vector.MustFixedColNoTypeCheck[types.Date](vec) {
				if value.IsTaggedInvalid() {
					return IOET_ColumnData_V3
				}
			}
		case types.T_datetime:
			for _, value := range vector.MustFixedColNoTypeCheck[types.Datetime](vec) {
				if value.IsTaggedInvalid() {
					return IOET_ColumnData_V3
				}
			}
		}
	}
	return IOET_ColumnData_CurrVer
}

func EncodeColumnDataV1(ioe any) (buf []byte, err error) {
	return ioe.(*vector.Vector).MarshalBinary()
}

// NOTE:
// Break by MustVector. Need to update MustVector to support new version.
func DecodeColumnDataV1(buf []byte) (ioe any, err error) {
	vec := vector.NewVec(types.Type{})
	if err = vec.UnmarshalBinaryV1(buf); err != nil {
		return
	}
	return vec, err
}

// NOTE:
// Break by MustVector. Need to update MustVector to support new version.
func DecodeColumnDataV2(buf []byte) (ioe any, err error) {
	vec := vector.NewVec(types.Type{})
	if err = vec.UnmarshalBinary(buf); err != nil {
		return
	}
	return vec, err
}

func DecodeObjectMetaV1(buf []byte) (ioe any, err error) {
	return objectMetaV1(buf), nil
}

func DecodeObjectMetaV2(buf []byte) (ioe any, err error) {
	return objectMetaV2(buf), nil
}

func DecodeObjectMetaV3(buf []byte) (ioe any, err error) {
	return objectMetaV3(buf), nil
}
