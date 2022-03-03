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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	case bool:
		return oe.EncodeBool(data,v)
	case int8:
		return oe.EncodeInt8(data, v)
	case int16:
		return oe.EncodeInt16(data, v)
	case int32:
		return oe.EncodeInt32(data, v)
	case int64:
		return oe.EncodeInt64(data, v)
	case types.Date:
		return oe.EncodeDate(data, v)
	case types.Datetime:
		return oe.EncodeDatetime(data, v)
	case uint8:
		return oe.EncodeUint8(data, v)
	case uint16:
		return oe.EncodeUint16(data, v)
	case uint32:
		return oe.EncodeUint32(data, v)
	case uint64:
		return oe.EncodeUint64(data,v)
	case float32:
		return oe.EncodeFloat32(data,v)
	case float64:
		return oe.EncodeFloat64(data,v)
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

func (oe *OrderedEncoder) EncodeBool(data []byte,value bool)([]byte,*EncodedItem) {
	if value {
		return oe.EncodeUint64(data, 1)
	}
	return oe.EncodeUint64(data, 0)
}

func (oe *OrderedEncoder) EncodeInt8(data []byte,value int8)([]byte,*EncodedItem) {
	return oe.EncodeInt64(data, int64(value))
}

func (oe *OrderedEncoder) EncodeInt16(data []byte,value int16)([]byte,*EncodedItem) {
	return oe.EncodeInt64(data, int64(value))
}

func (oe *OrderedEncoder) EncodeInt32(data []byte,value int32)([]byte,*EncodedItem) {
	return oe.EncodeInt64(data, int64(value))
}

func (oe *OrderedEncoder) EncodeInt64(data []byte,value int64)([]byte,*EncodedItem) {
	if value < 0 {
		if value >= -0xff {
			return append(data, encodingPrefixForIntegerMinimum + 7,byte(value)),nil
		} else if value >= -0xffff {
			return append(data, encodingPrefixForIntegerMinimum + 6, byte(value >> 8), byte(value)), nil
		} else if value >= -0xffffff {
			return append(data, encodingPrefixForIntegerMinimum + 5, byte(value >> 16), byte(value >> 8),
							byte(value)), nil
		} else if value >= -0xffffffff {
			return append(data, encodingPrefixForIntegerMinimum + 4, byte(value >> 24), byte(value >> 16),
							byte(value >> 8), byte(value)), nil
		} else if value >= -0xffffffffff {
			return append(data, encodingPrefixForIntegerMinimum + 3, byte(value >> 32), byte(value >> 24),
							byte(value >> 16), byte(value >> 8), byte(value)), nil
		} else if value >= -0xffffffffffff {
			return append(data, encodingPrefixForIntegerMinimum + 2, byte(value >> 40), byte(value >> 32),
							byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)), nil
		} else if value >= -0xffffffffffffff {
			return append(data, encodingPrefixForIntegerMinimum + 1, byte(value >> 48), byte(value >> 40),
							byte(value >> 32), byte(value >> 24), byte(value >> 16), byte(value >> 8), 
							byte(value)), nil
		} else {
			return append(data, encodingPrefixForIntegerMinimum, byte(value >> 56), byte(value >> 48),
							byte(value >> 40), byte(value >> 32), byte(value >> 24), byte(value >> 16),
							byte(value >> 8), byte(value)), nil
		}
	}
	return oe.EncodeUint64(data, uint64(value))
}

func (oe *OrderedEncoder) EncodeDate(data []byte,value types.Date)([]byte,*EncodedItem) {
	return oe.EncodeInt32(data, int32(value))
}

func (oe *OrderedEncoder) EncodeDatetime(data []byte,value types.Datetime)([]byte,*EncodedItem) {
	return oe.EncodeInt64(data, int64(value))
}

func (oe *OrderedEncoder) EncodeUint8(data []byte,value uint8)([]byte,*EncodedItem) {
	return oe.EncodeUint64(data, uint64(value))
}

func (oe *OrderedEncoder) EncodeUint16(data []byte,value uint16)([]byte,*EncodedItem) {
	return oe.EncodeUint64(data, uint64(value))
}

func (oe *OrderedEncoder) EncodeUint32(data []byte,value uint32)([]byte,*EncodedItem) {
	return oe.EncodeUint64(data, uint64(value))
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

func (oe *OrderedEncoder) EncodeUint64ForFloat(data []byte,value uint64)([]byte,*EncodedItem) {
	return append(data, byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32),
		byte(value>>24), byte(value>>16), byte(value>>8), byte(value)), nil
}

func (oe *OrderedEncoder) EncodeFloat32(data []byte,value float32)([]byte,*EncodedItem) {
	return oe.EncodeFloat64(data, float64(value))
}

func (oe *OrderedEncoder) EncodeFloat64(data []byte,value float64)([]byte,*EncodedItem) {
	if math.IsNaN(value) {
		return append(data, encodingfloatNaN), nil
	} else if value == 0 {
		return append(data, encodingfloatZero), nil
	}
	x := math.Float64bits(value)
	if x&(1<<63) != 0 {
		x = ^x
		data = append(data, encodingfloatNeg)
	} else {
		data = append(data, encodingfloatPos)
	}
	return oe.EncodeUint64ForFloat(data, x)
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