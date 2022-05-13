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

package orderedcodec

type OrderedEncoder struct {
}

type EncodedItem struct {
	//count of encoded bytes
	size int
}

type OrderedDecoder struct {
}

type ValueType int

const (
	VALUE_TYPE_UNKNOWN  ValueType = 0x0
	VALUE_TYPE_NULL     ValueType = 0x1
	VALUE_TYPE_BOOL     ValueType = 0x2
	VALUE_TYPE_UINT64   ValueType = 0x3
	VALUE_TYPE_BYTES    ValueType = 0x4
	VALUE_TYPE_STRING   ValueType = 0x5
	VALUE_TYPE_INT8     ValueType = 0x6
	VALUE_TYPE_INT16    ValueType = 0x7
	VALUE_TYPE_INT32    ValueType = 0x8
	VALUE_TYPE_INT64    ValueType = 0x9
	VALUE_TYPE_UINT8    ValueType = 0xa
	VALUE_TYPE_UINT16   ValueType = 0xb
	VALUE_TYPE_UINT32   ValueType = 0xc
	VALUE_TYPE_FLOAT32  ValueType = 0xd
	VALUE_TYPE_FLOAT64  ValueType = 0xe
	VALUE_TYPE_DATE     ValueType = 0xf
	VALUE_TYPE_DATETIME ValueType = 0x10
)

type SectionType int

const (
	SECTION_TYPE_TABLEID           SectionType = 0x0
	SECTION_TYPE_INDEXID           SectionType = 0x1
	SECTION_TYPE_PRIMARYKEYFIELD   SectionType = 0x2
	SECTION_TYPE_SECONDARYKEYFIELD SectionType = 0x3
	SECTION_TYPE_IMPLICITFIELD     SectionType = 0x4
	SECTION_TYPE_COMPOSITEFIELD    SectionType = 0x5
	SECTION_TYPE_STOREFIELD        SectionType = 0x6
	SECTION_TYPE_COLUMNGROUP       SectionType = 0x7
	SECTION_TYPE_DATABASEID        SectionType = 0x8
	SECTION_TYPE_VALUE             SectionType = 0x9
)

type DecodedItem struct {
	Value                    interface{}
	ValueType                ValueType   // int,uint,uint64,...,float
	SectionType              SectionType //belongs to which section
	OffsetInUndecodedKey     int         //the position in undecoded bytes
	RawBytes                 []byte      //the byte that holds the item
	BytesCountInUndecodedKey int         //the count of bytes in undecoded bytes
	ID                       uint32      //the attribute id
}

func (di *DecodedItem) GetValue() interface{} {
	return di.Value
}

func (di *DecodedItem) SetValue(v interface{}) {
	di.Value = v
}

func (di *DecodedItem) GetValueType() ValueType {
	return di.ValueType
}

func (di *DecodedItem) SetValueType(vt ValueType) {
	di.ValueType = vt
}

func (di *DecodedItem) GetSectionType() SectionType {
	return di.SectionType
}

func (di *DecodedItem) GetOffsetInUndecodedKey() int {
	return di.OffsetInUndecodedKey
}

func (di *DecodedItem) SetOffsetInUndecodedKey(o int) {
	di.OffsetInUndecodedKey = o
}

func (di *DecodedItem) GetBytesCountInUndecodedKey() int {
	return di.BytesCountInUndecodedKey
}

func (di *DecodedItem) SetBytesCountInUndecodedKey(b int) {
	di.BytesCountInUndecodedKey = b
}

func (di *DecodedItem) GetID() uint32 {
	return di.ID
}

func (di *DecodedItem) SetID(id uint32) {
	di.ID = id
}

func (di *DecodedItem) IsValueType(vt ValueType) bool {
	return di.ValueType == vt
}

func (di *DecodedItem) IsSectionType(st SectionType) bool {
	return di.SectionType == st
}

func (di *DecodedItem) SetSectionType(st SectionType) {
	di.SectionType = st
}

func NewDecodeItem(v interface{}, vt ValueType, st SectionType, oiu int, bciu int) *DecodedItem {
	return &DecodedItem{
		Value:                    v,
		ValueType:                vt,
		SectionType:              st,
		OffsetInUndecodedKey:     oiu,
		BytesCountInUndecodedKey: bciu,
	}
}
