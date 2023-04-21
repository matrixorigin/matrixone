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

const (
	dataTypeLen     = 1
	idxOff          = dataTypeLen
	idxLen          = 2
	ndvOff          = idxOff + idxLen
	ndvLen          = 4
	nullCntOff      = ndvOff + ndvLen
	nullCntLen      = 4
	locationOff     = nullCntOff + nullCntLen
	locationLen     = ExtentSize
	checkSumOff     = locationOff + locationLen
	checkSumLen     = 4
	zoneMapOff      = checkSumOff + checkSumLen
	zoneMapLen      = 64
	colMetaDummyOff = zoneMapOff + zoneMapLen
	colMetaDummyLen = 32
	colMetaLen      = colMetaDummyOff + colMetaDummyLen
)

func GetColumnMeta(idx uint16, data []byte) ColumnMeta {
	offset := headerLen + uint32(idx)*colMetaLen
	return data[offset : offset+colMetaLen]
}

type ColumnMeta []byte

func BuildColumnMeta() ColumnMeta {
	var buf [colMetaLen]byte
	meta := ColumnMeta(buf[:])
	return meta
}

func (cm ColumnMeta) DataType() uint8 {
	return types.DecodeUint8(cm[:dataTypeLen])
}

func (cm ColumnMeta) setDataType(t uint8) {
	copy(cm[:dataTypeLen], types.EncodeUint8(&t))
}

func (cm ColumnMeta) Idx() uint16 {
	return types.DecodeUint16(cm[idxOff : idxOff+idxLen])
}

func (cm ColumnMeta) setIdx(idx uint16) {
	copy(cm[idxOff:idxOff+idxLen], types.EncodeUint16(&idx))
}

func (cm ColumnMeta) Ndv() uint32 {
	return types.DecodeUint32(cm[:ndvLen])
}

func (cm ColumnMeta) SetNdv(cnt uint32) {
	copy(cm[:ndvLen], types.EncodeUint32(&cnt))
}

func (cm ColumnMeta) NullCnt() uint32 {
	return types.DecodeUint32(cm[nullCntOff : nullCntOff+nullCntLen])
}

func (cm ColumnMeta) SetNullCnt(cnt uint32) {
	copy(cm[nullCntOff:nullCntOff+nullCntLen], types.EncodeUint32(&cnt))
}

func (cm ColumnMeta) Location() Extent {
	return Extent(cm[locationOff : locationOff+locationLen])
}

func (cm ColumnMeta) setLocation(location Extent) {
	copy(cm[locationOff:locationOff+locationLen], location)
}

func (cm ColumnMeta) ZoneMap() ZoneMap {
	return ZoneMap(cm[zoneMapOff : zoneMapOff+zoneMapLen])
}

func (cm ColumnMeta) SetZoneMap(zm ZoneMap) {
	copy(cm[zoneMapOff:zoneMapOff+zoneMapLen], zm)
}

func (cm ColumnMeta) Checksum() uint32 {
	return types.DecodeUint32(cm[checkSumOff : checkSumOff+checkSumLen])
}

func (cm ColumnMeta) IsEmpty() bool {
	return len(cm) == 0
}
