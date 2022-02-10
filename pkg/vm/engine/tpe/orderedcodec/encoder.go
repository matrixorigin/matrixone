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

// EncodeKey encodes the value into the ordered bytes
func (oe *OrderedEncoder) EncodeKey(data []byte,value interface{})([]byte,*EncodedItem){
	if value == nil {
		return oe.EncodeNull(data)
	}
	return nil, nil
}

// EncodeNull encodes the NULL and appends the result to the buffer
func (oe *OrderedEncoder) EncodeNull(data []byte)([]byte,*EncodedItem){
	return append(data,nullEncoding), nil
}

// EncodeUint64 encodes the uint64 into ordered bytes and appends them to the buffer
func (oe *OrderedEncoder) EncodeUint64(data []byte,value uint64)([]byte,*EncodedItem) {
	return nil, nil
}