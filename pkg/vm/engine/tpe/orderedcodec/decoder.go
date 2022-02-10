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

import "errors"

var (
	errorNoEnoughBytesForDecoding = errors.New("there is no enough bytes for decoding")
	errorIsNotNull = errors.New("it is not the null encoding")
)

//DecodeKey decodes
func (od *OrderedDecoder) DecodeKey(data []byte)([]byte, *DecodedItem,error){
	if data == nil || len(data) < 1 {
		return data,nil,errorNoEnoughBytesForDecoding
	}
	dataAfterNull,decodeItem,err := od.IsNull(data)
	if err == nil {
		return dataAfterNull,decodeItem,nil
	}
	return nil, nil, nil
}

// isNll decodes the NULL and returns the bytes after the null.
func (od *OrderedDecoder) IsNull(data []byte) ([]byte,*DecodedItem,error) {
	if data == nil || len(data) < 1 {
		return data,nil,errorNoEnoughBytesForDecoding
	}
	if data[0] != nullEncoding {
		return data,nil,errorIsNotNull
	}
	return data[1:], NewDecodeItem(nil,VALUE_TYPE_NULL,0,0,1), nil
}