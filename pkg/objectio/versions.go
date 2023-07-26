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

type ObjectMeta = objectMetaV1

var (
	BuildObjectMeta        = buildObjectDataMetaV1
	NewObjectWriterSpecial = newObjectWriterSpecialV1
	NewObjectWriter        = newObjectWriterV1
	NewObjectReaderWithStr = newObjectReaderWithStrV1
	NewObjectReader        = newObjectReaderV1
)

const (
	IOET_ObjectMeta_V1  = 1
	IOET_ColumnData_V1  = 1
	IOET_BloomFilter_V1 = 1
	IOET_ZoneMap_V1     = 1

	IOET_ObjectMeta_CurrVer  = IOET_ObjectMeta_V1
	IOET_ColumnData_CurrVer  = IOET_ColumnData_V1
	IOET_BloomFilter_CurrVer = IOET_BloomFilter_V1
	IOET_ZoneMap_CurrVer     = IOET_ZoneMap_V1
)

func init() {
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ObjMeta, IOET_ObjectMeta_V1}, nil, nil)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ColData, IOET_ColumnData_V1}, EncodeColumnDataV1, DecodeColumnDataV1)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_BF, IOET_BloomFilter_V1}, nil, nil)
	RegisterIOEnrtyCodec(IOEntryHeader{IOET_ZM, IOET_ZoneMap_V1}, nil, nil)
}

func EncodeColumnDataV1(ioe any) (buf []byte, err error) {
	return ioe.(*vector.Vector).MarshalBinary()
}

func DecodeColumnDataV1(buf []byte) (ioe any, err error) {
	vec := vector.NewVec(types.Type{})
	if err = vec.UnmarshalBinary(buf); err != nil {
		return
	}
	return vec, err
}
