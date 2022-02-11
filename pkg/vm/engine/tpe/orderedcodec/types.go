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
	VALUE_TYPE_UNKOWN ValueType = 0x0
	VALUE_TYPE_NULL ValueType = 0x1
	VALUE_TYPE_UINT64 ValueType = 0x2
	VALUE_TYPE_BYTES ValueType = 0x3
)

type DecodedItem struct {
	value    interface{}
	valueType ValueType // int,uint,uint64,...,float
	sectionType int //belongs to which section
	offsetInUndecodedKey int    //the position in undecoded bytes
	bytesCountInUndecodedKey int //the count of bytes in undecoded bytes
}

func NewDecodeItem(v interface{}, vt ValueType, st int,oiu int,bciu int) *DecodedItem {
	return &DecodedItem{
		value:                    v,
		valueType:                vt,
		sectionType:              st,
		offsetInUndecodedKey:     oiu,
		bytesCountInUndecodedKey: bciu,
	}
}