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

package tuplecodec

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"strconv"
)

type SerializerType int

const (
	ST_JSON SerializerType = iota
	ST_CONCISE SerializerType = iota + 1
)

//ValueSerializer for serializing the value
type ValueSerializer interface {
	SerializeValue(data []byte,value interface{})([]byte,*orderedcodec.EncodedItem,error)

	DeserializeValue(data []byte)([]byte,*orderedcodec.DecodedItem,error)
}

var _ ValueSerializer = &DefaultValueSerializer{}
var _ ValueSerializer = &ConciseSerializer{}

type DefaultValueSerializer struct {}

type DefaultValue struct {
	ValueType string `json:"value_type"`
	Value interface{} `json:"value,string"`
}

//SerializeValue serializes the value with json
func (dvs *DefaultValueSerializer) SerializeValue(data []byte,value interface{})([]byte,*orderedcodec.EncodedItem,error) {
	var err error
	data,_,err = appendValueType(data,value)
	if err != nil {
		return nil, nil, err
	}

	//value data
	marshal, err := json.Marshal(value)
	if err != nil {
		return nil, nil,err
	}

	data = appendValue(data,marshal)

	return data, nil, nil
}

func ParseInterge(actualData []byte, base int, bitSize int, flag bool) (interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(actualData))
	dec.UseNumber()

	var jnum json.Number
	err := dec.Decode(&jnum)
	if err != nil {
		return nil, err
	}

	var parseNum interface{}
	if flag {
		parseNum, err = strconv.ParseInt(jnum.String(), base, bitSize)
	} else {
		parseNum, err = strconv.ParseUint(jnum.String(), base, bitSize)
	}

	if err != nil {
		return nil, err
	}
	return parseNum, nil
}

func ParseFloat(actualData []byte, bitSize int) (interface{}, error) {
	dec := json.NewDecoder(bytes.NewReader(actualData))
	dec.UseNumber()

	var jnum json.Number
	err := dec.Decode(&jnum)
	if err != nil {
		return nil, err
	}

	var parseNum interface{}
	parseNum, err = strconv.ParseFloat(jnum.String(), bitSize)

	if err != nil {
		return nil, err
	}
	return parseNum, nil
}

// DeserializeValue deserializes the data
func (dvs *DefaultValueSerializer) DeserializeValue(data []byte)([]byte,*orderedcodec.DecodedItem,error) {
	actualData, dataEnd, bytesRead, vt, err := extractActualData(data)
	if err != nil {
		return nil, nil, err
	}

	//unmarshal value
	var value interface{}

	//use number decode
	switch vt {
	case orderedcodec.VALUE_TYPE_NULL:
		if string(actualData) != "null" {
			return nil, nil, errorWrongValueType
		}
		value = nil
	case orderedcodec.VALUE_TYPE_BOOL:
		if string(actualData) == "true" {
			value = true
		} else if string(actualData) == "false" {
			value = false
		} else {
			return nil, nil, errorWrongValueType
		}
	case orderedcodec.VALUE_TYPE_INT8:
		parseNum, err := ParseInterge(actualData, 10, 8, true)
		if err != nil {
			return nil, nil, err
		}
		value = int8(parseNum.(int64))
	case orderedcodec.VALUE_TYPE_INT16:
		parseNum, err := ParseInterge(actualData, 10, 16, true)
		if err != nil {
			return nil, nil, err
		}
		value = int16(parseNum.(int64))
	case orderedcodec.VALUE_TYPE_INT32:
		parseNum, err := ParseInterge(actualData, 10, 32, true)
		if err != nil {
			return nil, nil, err
		}
		value = int32(parseNum.(int64))
	case orderedcodec.VALUE_TYPE_INT64:
		parseNum, err := ParseInterge(actualData, 10, 64, true)
		if err != nil {
			return nil, nil, err
		}
		value = parseNum.(int64)
	case orderedcodec.VALUE_TYPE_UINT8:
		parseNum, err := ParseInterge(actualData, 10, 8, false)
		if err != nil {
			return nil, nil, err
		}
		value = uint8(parseNum.(uint64))
	case orderedcodec.VALUE_TYPE_UINT16:
		parseNum, err := ParseInterge(actualData, 10, 16, false)
		if err != nil {
			return nil, nil, err
		}
		value = uint16(parseNum.(uint64))
	case orderedcodec.VALUE_TYPE_UINT32:
		parseNum, err := ParseInterge(actualData, 10, 32, false)
		if err != nil {
			return nil, nil, err
		}
		value = uint32(parseNum.(uint64))
	case orderedcodec.VALUE_TYPE_UINT64:
		parseNum, err := ParseInterge(actualData, 10, 64, false)
		if err != nil {
			return nil, nil, err
		}
		value = parseNum.(uint64)
	case orderedcodec.VALUE_TYPE_FLOAT32:
		parseNum, err := ParseFloat(actualData, 32)
		if err != nil {
			return nil, nil, err
		}
		value = float32(parseNum.(float64))
	case orderedcodec.VALUE_TYPE_FLOAT64:
		parseNum, err := ParseFloat(actualData, 64)
		if err != nil {
			return nil, nil, err
		}
		value = parseNum.(float64)
	case orderedcodec.VALUE_TYPE_DATE:
		parseNum, err := ParseInterge(actualData, 10, 32, true)
		if err != nil {
			return nil, nil, err
		}
		value = types.Date(parseNum.(int64))
	case orderedcodec.VALUE_TYPE_DATETIME:
		parseNum, err := ParseInterge(actualData, 10, 64, true)
		if err != nil {
			return nil, nil, err
		}
		value = types.Datetime(parseNum.(int64))
	case orderedcodec.VALUE_TYPE_BYTES:
		var sliceValue []byte
		err := json.Unmarshal(actualData, &sliceValue)
		if err != nil {
			return nil, nil, err
		}
		value = sliceValue
	case orderedcodec.VALUE_TYPE_STRING:
		if len(actualData) < 2 {
			return nil, nil, errorWrongValueType
		}
		value = string(actualData[1:len(actualData) - 1])
	default:
		err := json.Unmarshal(actualData, &value)
		if err != nil {
			return nil, nil, err
		}
	}

	return data[dataEnd:],
		orderedcodec.NewDecodeItem(value,
			vt,
			orderedcodec.SECTION_TYPE_VALUE,
			0, 1 + bytesRead+int(len(actualData))),
		nil
}

type ConciseSerializer struct {
	bufReader bytes.Reader
}

func (cs *ConciseSerializer) SerializeValue(data []byte, value interface{}) ([]byte, *orderedcodec.EncodedItem, error) {
	var valueType byte
	var err error
	data,valueType,err = appendValueType(data,value)
	if err != nil {
		return nil, nil, err
	}

	//value data
	var marshal []byte
	if valueType == SERIAL_TYPE_STRING {
		s := value.(string)
		marshal = []byte(s)
	} else if valueType != SERIAL_TYPE_NULL {
		var buf bytes.Buffer
		err := binary.Write(&buf, binary.BigEndian, value)
		if err != nil {
			return nil, nil, err
		}

		marshal = buf.Bytes()
	}

	data = appendValue(data,marshal)

	return data, nil, nil
}

func (cs *ConciseSerializer) DeserializeValue(data []byte) ([]byte, *orderedcodec.DecodedItem, error) {
	actualData, dataEnd, bytesRead, vt, err := extractActualData(data)
	if err != nil {
		return nil, nil, err
	}

	cs.bufReader.Reset(actualData)

	//unmarshal value
	var value interface{}

	//use number decode
	switch vt {
	case orderedcodec.VALUE_TYPE_NULL:
		value = nil
	case orderedcodec.VALUE_TYPE_BOOL:
		var x bool
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT8:
		var x int8
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT16:
		var x int16
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT32:
		var x int32
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT64:
		var x int64
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT8:
		var x uint8
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT16:
		var x uint16
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT32:
		var x uint32
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT64:
		var x uint64
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_FLOAT32:
		var x float32
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_FLOAT64:
		var x float64
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_DATE:
		var x int32
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = types.Date(x)
	case orderedcodec.VALUE_TYPE_DATETIME:
		var x int64
		err := binary.Read(&cs.bufReader,binary.BigEndian,&x)
		if err != nil {
			return nil, nil, err
		}
		value = types.Datetime(x)
	case orderedcodec.VALUE_TYPE_BYTES:
		value = actualData
	case orderedcodec.VALUE_TYPE_STRING:
		value = string(actualData)
	default:
		return nil, nil, errorWrongValueType
	}

	return data[dataEnd:],
		orderedcodec.NewDecodeItem(value,
			vt,
			orderedcodec.SECTION_TYPE_VALUE,
			0, 1 + bytesRead+int(len(actualData))),
		nil
}

// appendValueType appends the valueType of the value to the buffer
func appendValueType(data []byte, value interface{}) ([]byte, byte, error) {
	valueType := SERIAL_TYPE_UNKNOWN
	if value == nil {
		valueType = SERIAL_TYPE_NULL
	}else{
		switch value.(type) {
		case bool:
			valueType = SERIAL_TYPE_BOOL
		case int8:
			valueType = SERIAL_TYPE_INT8
		case int16:
			valueType = SERIAL_TYPE_INT16
		case int32:
			valueType = SERIAL_TYPE_INT32
		case int64:
			valueType = SERIAL_TYPE_INT64
		case uint8:
			valueType = SERIAL_TYPE_UINT8
		case uint16:
			valueType = SERIAL_TYPE_UINT16
		case uint32:
			valueType = SERIAL_TYPE_UINT32
		case uint64:
			valueType = SERIAL_TYPE_UINT64
		case float32:
			valueType = SERIAL_TYPE_FLOAT32
		case float64:
			valueType = SERIAL_TYPE_FLOAT64
		case types.Date:
			valueType = SERIAL_TYPE_DATE
		case types.Datetime:
			valueType = SERIAL_TYPE_DATETIME
		case []byte:
			valueType = SERIAL_TYPE_BYTES
		case string:
			valueType = SERIAL_TYPE_STRING
		default:
			valueType = SERIAL_TYPE_UNKNOWN
		}
	}

	if valueType == SERIAL_TYPE_UNKNOWN {
		return nil,SERIAL_TYPE_UNKNOWN,errorUnknownValueType
	}

	//FirstByte is valueType
	data = append(data,valueType)
	return data,valueType,nil
}

// appendValue appends the marshal of the value to the buffer
func appendValue(data []byte,marshal []byte) []byte {
	//encode data len
	var lbuf [binary.MaxVarintLen64]byte
	bytesWritten := binary.PutVarint(lbuf[:], int64(len(marshal)))
	data = append(data,lbuf[:bytesWritten]...)
	data = append(data,marshal...)
	return data
}

// extractActualData extracts the real data part from the buffer
func extractActualData(data []byte) ([]byte, int64, int, orderedcodec.ValueType, error) {
	if len(data) == 0 {
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKOWN, errorNoEnoughBytes
	}

	vt := orderedcodec.VALUE_TYPE_UNKOWN
	switch data[0] {
	case SERIAL_TYPE_NULL:
		vt = orderedcodec.VALUE_TYPE_NULL
	case SERIAL_TYPE_BOOL:
		vt = orderedcodec.VALUE_TYPE_BOOL
	case SERIAL_TYPE_INT8:
		vt = orderedcodec.VALUE_TYPE_INT8
	case SERIAL_TYPE_INT16:
		vt = orderedcodec.VALUE_TYPE_INT16
	case SERIAL_TYPE_INT32:
		vt = orderedcodec.VALUE_TYPE_INT32
	case SERIAL_TYPE_INT64:
		vt = orderedcodec.VALUE_TYPE_INT64
	case SERIAL_TYPE_UINT8:
		vt = orderedcodec.VALUE_TYPE_UINT8
	case SERIAL_TYPE_UINT16:
		vt = orderedcodec.VALUE_TYPE_UINT16
	case SERIAL_TYPE_UINT32:
		vt = orderedcodec.VALUE_TYPE_UINT32
	case SERIAL_TYPE_UINT64:
		vt = orderedcodec.VALUE_TYPE_UINT64
	case SERIAL_TYPE_FLOAT32:
		vt = orderedcodec.VALUE_TYPE_FLOAT32
	case SERIAL_TYPE_FLOAT64:
		vt = orderedcodec.VALUE_TYPE_FLOAT64
	case SERIAL_TYPE_DATE:
		vt = orderedcodec.VALUE_TYPE_DATE
	case SERIAL_TYPE_DATETIME:
		vt = orderedcodec.VALUE_TYPE_DATETIME
	case SERIAL_TYPE_STRING:
		vt = orderedcodec.VALUE_TYPE_STRING
	case SERIAL_TYPE_BYTES:
		vt = orderedcodec.VALUE_TYPE_BYTES
	default:
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKOWN, errorWrongValueType
	}

	//decode data len
	dataLen, bytesRead := binary.Varint(data[1:])
	if bytesRead == 0 {
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKOWN, errorNoEnoughBytes
	}

	if bytesRead < 0 {
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKOWN, errorVarintOverflow
	}

	//skip the byte for value type and the bytes for data length
	dataOffset := 1 + bytesRead
	if int64(len(data[dataOffset:])) < dataLen {
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKOWN, errorNoEnoughBytes
	}
	dataEnd := int64(dataOffset) + dataLen

	actualData := data[dataOffset:dataEnd]
	return actualData, dataEnd, bytesRead, vt, nil
}