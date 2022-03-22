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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

type SerializerType int

const (
	ST_JSON SerializerType = iota
	ST_CONCISE SerializerType = iota + 1
)

var _ ValueSerializer = &ConciseSerializer{}

type ConciseSerializer struct {
	bufReader bytes.Reader
}

func (cs *ConciseSerializer) SerializeValue(data []byte, value interface{}) ([]byte, *orderedcodec.EncodedItem, error) {
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
		return nil,nil,errorUnknownValueType
	}

	//FirstByte is valueType
	data = append(data,valueType)

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

	//encode data len
	var lbuf [binary.MaxVarintLen64]byte
	bytesWritten := binary.PutVarint(lbuf[:], int64(len(marshal)))
	data = append(data,lbuf[:bytesWritten]...)
	data = append(data,marshal...)

	return data, nil, nil
}

func (cs *ConciseSerializer) DeserializeValue(data []byte) ([]byte, *orderedcodec.DecodedItem, error) {
	if len(data) == 0 {
		return nil, nil, errorNoEnoughBytes
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
		return nil, nil, errorWrongValueType
	}

	//decode data len
	dataLen, bytesRead := binary.Varint(data[1:])
	if bytesRead == 0 {
		return nil, nil, errorNoEnoughBytes
	}

	if bytesRead < 0 {
		return nil, nil, errorVarintOverflow
	}

	//skip the byte for value type and the bytes for data length
	dataOffset := 1 + bytesRead
	if int64(len(data[dataOffset:])) < dataLen {
		return nil, nil, errorNoEnoughBytes
	}
	dataEnd := int64(dataOffset) + dataLen

	actualData := data[dataOffset:dataEnd]

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
			0, 1 + bytesRead+int(dataLen)),
		nil
}
