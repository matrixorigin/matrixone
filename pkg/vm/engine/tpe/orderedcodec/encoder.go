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

import (
	"bytes"
	"errors"
)

var (
	errorDoNotComeHere = errors.New("do not come here")
)

const (
	//Actually,the 0x00 represents the NULL in encoded bytes.
	//So, 0x00 escaped to {0x00 0xff}
	byteToBeEscaped byte = 0x00
	byteEscapedToFirstByte byte = 0x00
	byteEscapedToSecondByte byte = 0xFF

	//suffix for denoting the end of the bytes
	byteForBytesEnding byte = 0x01
)

// EncodeKey encodes the value into the ordered bytes
func (oe *OrderedEncoder) EncodeKey(data []byte,value interface{})([]byte,*EncodedItem){
	if value == nil {
		return oe.EncodeNull(data)
	}
	switch v:=value.(type) {
	case uint64:
		return oe.EncodeUint64(data,v)
	case []byte:
		return oe.EncodeBytes(data,v)
	case string:
		return oe.EncodeString(data,v)
	default:
		panic(errorDoNotComeHere)
	}
	return nil, nil
}

// EncodeNull encodes the NULL and appends the result to the buffer
func (oe *OrderedEncoder) EncodeNull(data []byte)([]byte,*EncodedItem){
	return append(data,nullEncoding), nil
}

// EncodeUint64 encodes the uint64 into ordered bytes with uvarint encoding
// and appends them to the buffer.
// The variable length is encoded into the first byte.
func (oe *OrderedEncoder) EncodeUint64(data []byte,value uint64)([]byte,*EncodedItem) {
	if value <= encodingPrefixForSplit {
		return append(data,encodingPrefixForIntegerZero+byte(value)),nil
	}else if value <= 0xff {
		return append(data,encodingPrefixForIntMax - 7,byte(value)),nil
	}else if value <= 0xffff {
		return append(data,encodingPrefixForIntMax - 6,
			byte(value >> 8),byte(value)),nil
	}else if value <= 0xffffff {
		return append(data,encodingPrefixForIntMax - 5,
			byte(value >> 16),byte(value >> 8),
			byte(value)),nil
	}else if value <= 0xffffffff {
		return append(data,encodingPrefixForIntMax - 4,
			byte(value >> 24),byte(value >> 16),
			byte(value >> 8),byte(value)),nil
	}else if value <= 0xffffffffff {
		return append(data,encodingPrefixForIntMax - 3,
			byte(value >> 32),byte(value >> 24),
			byte(value >> 16),byte(value >> 8),
			byte(value)),nil
	}else if value <= 0xffffffffffff {
		return append(data,encodingPrefixForIntMax - 2,
			byte(value >> 40),byte(value >> 32),
			byte(value >> 24),byte(value >> 16),
			byte(value >> 8),byte(value)),nil
	}else if value <= 0xffffffffffffff {
		return append(data,encodingPrefixForIntMax - 1,
			byte(value >> 48),byte(value >> 40),
			byte(value >> 32),byte(value >> 24),
			byte(value >> 16),byte(value >> 8),
			byte(value)),nil
	}else{
		return append(data,encodingPrefixForIntMax,
			byte(value >> 56),byte(value >> 48),
			byte(value >> 40),byte(value >> 32),
			byte(value >> 24), byte(value >> 16),
			byte(value >> 8), byte(value)),nil
	}
	return nil, nil
}

// EncodeBytes encodes the bytes with escaping and appends them to the buffer.
func (oe *OrderedEncoder) EncodeBytes(data []byte,value []byte)([]byte,*EncodedItem) {
	data = append(data,encodingPrefixForBytes)
	return oe.encodeBytesWithSuffix(data,value, byteForBytesEnding)
}

//encodeBytes encodes the bytes with escaping and suffix byte.
//The encoded bytes is appened with the suffix slice {0x00, suffix}.
func (oe *OrderedEncoder) encodeBytesWithSuffix(data []byte,value []byte,suffix byte)([]byte,*EncodedItem) {
	data,_ = oe.encodeBytes(data, value)
	return append(data, byteToBeEscaped, suffix),nil
}

//encodeBytes encodes the bytes with escaping
func (oe *OrderedEncoder) encodeBytes(data []byte,value []byte)([]byte,*EncodedItem) {
	for {
		p := bytes.IndexByte(value,byteToBeEscaped)
		if p == -1 {
			break
		}

		//bytes before the escaped byte
		data = append(data, value[:p]...)
		data = append(data, byteEscapedToFirstByte, byteEscapedToSecondByte)

		//rest bytes
		value = value[p + 1:]
	}
	return append(data, value...), nil
}

// EncodeString encods the string into bytes with escaping and appends them to the buffer.
func (oe *OrderedEncoder) EncodeString(data []byte,value string)([]byte,*EncodedItem) {
	return oe.EncodeBytes(data,[]byte(value))
}

func NewOrderedEncoder()*OrderedEncoder {
	return &OrderedEncoder{}
}