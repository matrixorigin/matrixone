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
	"errors"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
)

var (
	errorTheUvarintOfTheColumnGroupIsTooLarge = errors.New("the uvarint of the column group is too large")
	errorSecondaryIndexIsUnsupported          = errors.New("secondary index is unsupported now")
	errorDuplicateAttributeIDInIndex          = errors.New("duplicate attribute id in the index")
	errorAttributeIDInFormatIsUnsupported     = errors.New("the attributeID in format is unsupported now")
	errorNoSuchAttributeInTuple               = errors.New("no such attribute in the tuple")
	errorGetOffsetArrayLenFailed              = errors.New("get offsetArrayLen failed")
	errorNoSuchField                          = errors.New("no such field")
)

type SerializerType int

const (
	ST_JSON    SerializerType = iota
	ST_CONCISE SerializerType = iota + 1
	ST_FLAT    SerializerType = iota + 2
)

// ValueDecodedItem for value deserialization
type ValueDecodedItem struct {
	OffsetInUndecodedKey     int    //the position in undecoded bytes
	RawBytes                 []byte //the byte that holds the item
	BytesCountInUndecodedKey int    //the count of bytes in undecoded bytes
	ID                       uint32 //the attribute id
	serializer               ValueSerializer
}

func (vdi *ValueDecodedItem) DecodeValue() (*orderedcodec.DecodedItem, error) {
	_, di, err := vdi.serializer.DeserializeValue(vdi.RawBytes)
	if err != nil {
		return nil, err
	}
	return di, nil
}

//ValueSerializer for serializing the value
//Stateless is better
type ValueSerializer interface {
	SerializeValue(data []byte, value interface{}) ([]byte, *orderedcodec.EncodedItem, error)

	DeserializeValue(data []byte) ([]byte, *orderedcodec.DecodedItem, error)
}

var _ ValueSerializer = &DefaultValueSerializer{}
var _ ValueSerializer = &ConciseSerializer{}

type DefaultValueSerializer struct{}

type DefaultValue struct {
	ValueType string      `json:"value_type"`
	Value     interface{} `json:"value,string"`
}

//SerializeValue serializes the value with json
func (dvs *DefaultValueSerializer) SerializeValue(data []byte, value interface{}) ([]byte, *orderedcodec.EncodedItem, error) {
	var err error
	data, _, err = appendValueType(data, value)
	if err != nil {
		return nil, nil, err
	}

	//value data
	marshal, err := json.Marshal(value)
	if err != nil {
		return nil, nil, err
	}

	data = appendValue(data, marshal)

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
func (dvs *DefaultValueSerializer) DeserializeValue(data []byte) ([]byte, *orderedcodec.DecodedItem, error) {
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
			logutil.Errorf("vt 5 %d", vt)
			return nil, nil, errorWrongValueType
		}
		value = nil
	case orderedcodec.VALUE_TYPE_BOOL:
		if string(actualData) == "true" {
			value = true
		} else if string(actualData) == "false" {
			value = false
		} else {
			logutil.Errorf("vt 4 %d", vt)
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
			logutil.Errorf("vt 3 %d", vt)
			return nil, nil, errorWrongValueType
		}
		value = string(actualData[1 : len(actualData)-1])
	default:
		err := json.Unmarshal(actualData, &value)
		if err != nil {
			return nil, nil, err
		}
	}

	return data[dataEnd:], orderedcodec.NewDecodeItem(value,
		vt,
		orderedcodec.SECTION_TYPE_VALUE,
		0, 1+bytesRead+int(len(actualData))), nil
}

type ConciseSerializer struct {
}

func (cs *ConciseSerializer) SerializeValue(data []byte, value interface{}) ([]byte, *orderedcodec.EncodedItem, error) {
	var valueType byte
	var err error
	data, valueType, err = appendValueType(data, value)
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

	data = appendValue(data, marshal)

	return data, nil, nil
}

func (cs *ConciseSerializer) DeserializeValue(data []byte) ([]byte, *orderedcodec.DecodedItem, error) {
	actualData, dataEnd, bytesRead, vt, err := extractActualData(data)
	if err != nil {
		return nil, nil, err
	}

	bufReader := bytes.NewReader(actualData)

	//unmarshal value
	var value interface{}

	//use number decode
	switch vt {
	case orderedcodec.VALUE_TYPE_NULL:
		value = nil
	case orderedcodec.VALUE_TYPE_BOOL:
		var x bool
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT8:
		var x int8
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT16:
		var x int16
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT32:
		var x int32
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT64:
		var x int64
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT8:
		var x uint8
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT16:
		var x uint16
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT32:
		var x uint32
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT64:
		var x uint64
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_FLOAT32:
		var x float32
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_FLOAT64:
		var x float64
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_DATE:
		var x int32
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = types.Date(x)
	case orderedcodec.VALUE_TYPE_DATETIME:
		var x int64
		err := binary.Read(bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = types.Datetime(x)
	case orderedcodec.VALUE_TYPE_BYTES:
		value = actualData
	case orderedcodec.VALUE_TYPE_STRING:
		value = string(actualData)
	default:
		logutil.Errorf("vt 1 %d", vt)
		return nil, nil, errorWrongValueType
	}

	return data[dataEnd:], orderedcodec.NewDecodeItem(value,
		vt,
		orderedcodec.SECTION_TYPE_VALUE,
		0, 1+bytesRead+int(len(actualData))), nil
}

type FlatSerializer struct {
}

func (fs *FlatSerializer) SerializeValue(data []byte, value interface{}) ([]byte, *orderedcodec.EncodedItem, error) {
	valueType := SERIAL_TYPE_UNKNOWN
	//value data
	var marshal []byte
	if value == nil {
		valueType = SERIAL_TYPE_NULL
	} else {
		switch v := value.(type) {
		case bool:
			valueType = SERIAL_TYPE_BOOL
			if v {
				marshal = []byte{1}
			} else {
				marshal = []byte{0}
			}
		case int8:
			valueType = SERIAL_TYPE_INT8
			marshal = []byte{byte(v)}
		case int16:
			valueType = SERIAL_TYPE_INT16
			marshal = []byte{0, 0}
			binary.BigEndian.PutUint16(marshal, uint16(v))
		case int32:
			valueType = SERIAL_TYPE_INT32
			marshal = []byte{0, 0, 0, 0}
			binary.BigEndian.PutUint32(marshal, uint32(v))
		case int64:
			valueType = SERIAL_TYPE_INT64
			marshal = []byte{0, 0, 0, 0, 0, 0, 0, 0}
			binary.BigEndian.PutUint64(marshal, uint64(v))
		case uint8:
			valueType = SERIAL_TYPE_UINT8
			marshal = []byte{v}
		case uint16:
			valueType = SERIAL_TYPE_UINT16
			marshal = []byte{0, 0}
			binary.BigEndian.PutUint16(marshal, v)
		case uint32:
			valueType = SERIAL_TYPE_UINT32
			marshal = []byte{0, 0, 0, 0}
			binary.BigEndian.PutUint32(marshal, v)
		case uint64:
			valueType = SERIAL_TYPE_UINT64
			marshal = []byte{0, 0, 0, 0, 0, 0, 0, 0}
			binary.BigEndian.PutUint64(marshal, v)
		case float32:
			valueType = SERIAL_TYPE_FLOAT32
			marshal = []byte{0, 0, 0, 0}
			uv := math.Float32bits(v)
			binary.BigEndian.PutUint32(marshal, uv)
		case float64:
			valueType = SERIAL_TYPE_FLOAT64
			marshal = []byte{0, 0, 0, 0, 0, 0, 0, 0}
			uv := math.Float64bits(v)
			binary.BigEndian.PutUint64(marshal, uv)
		case types.Date:
			valueType = SERIAL_TYPE_DATE
			marshal = []byte{0, 0, 0, 0}
			binary.BigEndian.PutUint32(marshal, uint32(v))
		case types.Datetime:
			valueType = SERIAL_TYPE_DATETIME
			marshal = []byte{0, 0, 0, 0}
			binary.BigEndian.PutUint32(marshal, uint32(v))
		case []byte:
			valueType = SERIAL_TYPE_BYTES
			marshal = v
		case string:
			valueType = SERIAL_TYPE_STRING
			marshal = []byte(v)
		default:
			valueType = SERIAL_TYPE_UNKNOWN
			return nil, nil, errorUnknownValueType
		}
	}

	//FirstByte is valueType
	data = append(data, valueType)
	data = appendValue(data, marshal)

	return data, nil, nil
}

func (fs *FlatSerializer) DeserializeValue(data []byte) ([]byte, *orderedcodec.DecodedItem, error) {
	actualData, dataEnd, bytesRead, vt, err := extractActualData(data)
	if err != nil {
		return nil, nil, err
	}

	//unmarshal value
	var value interface{}
	dataLen := len(actualData)

	//use number decode
	switch vt {
	case orderedcodec.VALUE_TYPE_NULL:
		value = nil
	case orderedcodec.VALUE_TYPE_BOOL:
		if dataLen != 1 {
			return nil, nil, errorWrongCountOfBytes
		}
		if actualData[0] == 1 {
			value = true
		} else {
			value = false
		}
	case orderedcodec.VALUE_TYPE_INT8:
		if dataLen != 1 {
			return nil, nil, errorWrongCountOfBytes
		}
		value = int8(actualData[0])
	case orderedcodec.VALUE_TYPE_INT16:
		if dataLen != 2 {
			return nil, nil, errorWrongCountOfBytes
		}
		value = int16(binary.BigEndian.Uint16(actualData))
	case orderedcodec.VALUE_TYPE_INT32:
		if dataLen != 4 {
			return nil, nil, errorWrongCountOfBytes
		}
		value = int32(binary.BigEndian.Uint32(actualData))
	case orderedcodec.VALUE_TYPE_INT64:
		if dataLen != 8 {
			return nil, nil, errorWrongCountOfBytes
		}
		value = int64(binary.BigEndian.Uint64(actualData))
	case orderedcodec.VALUE_TYPE_UINT8:
		if dataLen != 1 {
			return nil, nil, errorWrongCountOfBytes
		}
		value = uint8(actualData[0])
	case orderedcodec.VALUE_TYPE_UINT16:
		if dataLen != 2 {
			return nil, nil, errorWrongCountOfBytes
		}
		value = binary.BigEndian.Uint16(actualData)
	case orderedcodec.VALUE_TYPE_UINT32:
		if dataLen != 4 {
			return nil, nil, errorWrongCountOfBytes
		}
		value = binary.BigEndian.Uint32(actualData)
	case orderedcodec.VALUE_TYPE_UINT64:
		if dataLen != 8 {
			return nil, nil, errorWrongCountOfBytes
		}
		value = binary.BigEndian.Uint64(actualData)
	case orderedcodec.VALUE_TYPE_FLOAT32:
		if dataLen != 4 {
			return nil, nil, errorWrongCountOfBytes
		}
		x := binary.BigEndian.Uint32(actualData)
		value = math.Float32frombits(x)
	case orderedcodec.VALUE_TYPE_FLOAT64:
		if dataLen != 8 {
			return nil, nil, errorWrongCountOfBytes
		}
		x := binary.BigEndian.Uint64(actualData)
		value = math.Float64frombits(x)
	case orderedcodec.VALUE_TYPE_DATE:
		if dataLen != 4 {
			return nil, nil, errorWrongCountOfBytes
		}
		x := binary.BigEndian.Uint32(actualData)
		value = types.Date(x)
	case orderedcodec.VALUE_TYPE_DATETIME:
		if dataLen != 4 {
			return nil, nil, errorWrongCountOfBytes
		}
		x := binary.BigEndian.Uint32(actualData)
		value = types.Datetime(x)
	case orderedcodec.VALUE_TYPE_BYTES:
		value = actualData
	case orderedcodec.VALUE_TYPE_STRING:
		value = string(actualData)
	default:
		logutil.Errorf("vt 1 %d", vt)
		return nil, nil, errorWrongValueType
	}

	return data[dataEnd:], orderedcodec.NewDecodeItem(value,
		vt,
		orderedcodec.SECTION_TYPE_VALUE,
		0, 1+bytesRead+int(len(actualData))), nil
}

// appendValueType appends the valueType of the value to the buffer
func appendValueType(data []byte, value interface{}) ([]byte, byte, error) {
	valueType := SERIAL_TYPE_UNKNOWN
	if value == nil {
		valueType = SERIAL_TYPE_NULL
	} else {
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
		return nil, SERIAL_TYPE_UNKNOWN, errorUnknownValueType
	}

	//FirstByte is valueType
	data = append(data, valueType)
	return data, valueType, nil
}

// appendValue appends the marshal of the value to the buffer
func appendValue(data []byte, marshal []byte) []byte {
	//encode data len
	var lbuf [binary.MaxVarintLen64]byte
	bytesWritten := binary.PutVarint(lbuf[:], int64(len(marshal)))
	data = append(data, lbuf[:bytesWritten]...)
	data = append(data, marshal...)
	return data
}

// extractActualData extracts the real data part from the buffer
func extractActualData(data []byte) ([]byte, int64, int, orderedcodec.ValueType, error) {
	if len(data) == 0 {
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKNOWN, errorNoEnoughBytes
	}

	vt := orderedcodec.VALUE_TYPE_UNKNOWN
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
		logutil.Errorf("vt 2 %d", vt)
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKNOWN, errorWrongValueType
	}

	//decode data len
	dataLen, bytesRead := binary.Varint(data[1:])
	if bytesRead == 0 {
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKNOWN, errorNoEnoughBytes
	}

	if bytesRead < 0 {
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKNOWN, errorVarintOverflow
	}

	//skip the byte for value type and the bytes for data length
	dataOffset := 1 + bytesRead
	if int64(len(data[dataOffset:])) < dataLen {
		return nil, 0, 0, orderedcodec.VALUE_TYPE_UNKNOWN, errorNoEnoughBytes
	}
	dataEnd := int64(dataOffset) + dataLen

	actualData := data[dataOffset:dataEnd]
	return actualData, dataEnd, bytesRead, vt, nil
}

// extractSerializedBytes extracts the serialized bytes of the attribute
func extractSerializedBytes(data []byte) ([]byte, []byte, error) {
	if len(data) == 0 {
		return nil, nil, errorNoEnoughBytes
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

	return data[:dataEnd], data[dataEnd:], nil
}

type ValueLayoutContext struct {
	TableDesc *descriptor.RelationDesc
	IndexDesc *descriptor.IndexDesc

	//write control for the attribute
	AttributeStates []AttributeStateForWrite
	Tuple           Tuple
	Fields          []interface{}

	ColumnGroup uint64
}

// ValueLayoutSerializer defines the layout of the value serialization
type ValueLayoutSerializer interface {
	Serialize(out []byte, ctx *ValueLayoutContext) ([]byte, error)
	Deserialize(data []byte, amForValue *AttributeMap) ([]byte, []*orderedcodec.DecodedItem, []*ValueDecodedItem, error)
	GetPositionsOfAttributesInTheValue(tableDesc *descriptor.RelationDesc, indexDesc *descriptor.IndexDesc) map[uint32]int
}

var _ ValueLayoutSerializer = &CompactValueLayoutSerializer{}
var _ ValueLayoutSerializer = &DefaultValueLayoutSerializer{}

// CompactValueLayoutSerializer defines a compact layout of the value
/*
The Layout of the value:
| The Control Byte(first byte) | The Column Group (option) | The Attributes Storage Section                                                   |
|                                                          | Attributes Offset Section                                                        |  Attributes Section
|                                                          |              The count of Attributes | Attribute Offset Array                    |  Attribute 1 Format, Attribute 2 Format,....
|                                                                                                 | Attribute 1 offset, Attribute 2 offset,...|

The layout of the Attribute X Format:
| The Value Type(first byte) | Attribute X's ID (option) | The serialized bytes of the Attribute X |

Control Byte :
bit7:0-primary index；1-secondary index。
bit6: 0-no column group; 1-has column group。
bit5: 0-The Attribute X Format does not have Attribute ID；1-The Attribute X Format has Attribute ID
bit4~bit3 : used in secondary index
bit0~bit2: the offset of the Attributes Offset Section
*/
type CompactValueLayoutSerializer struct {
	needColumnGroup         bool
	needAttributeIDInFormat bool
	Serializer              ValueSerializer
}

// offset records the position in the buffer
type offset struct {
	//attribute id
	ID uint32
	//original position of the bytes of the attribute in the buffer
	bytesOffset int
	//the bytes count of the attribute
	bytesCount int
}

func (o offset) id() uint32 {
	return o.ID
}

// adjustBytesOffset returns adj + offset.bytesOffset
func (o offset) adjustBytesOffset(adj int) int {
	return o.bytesOffset + adj
}

type offsetArray struct {
	offsets []offset
	//total bytes count in the offsets
	bytesCount int
}

func (os *offsetArray) Append(o offset) {
	os.offsets = append(os.offsets, o)
	os.bytesCount += o.adjustBytesOffset(0)
}

func (os *offsetArray) Count() int {
	return len(os.offsets)
}

func (os *offsetArray) Get(p int) offset {
	return os.offsets[p]
}

const (
	bit7mask byte = (1 << 7)
	bit6mask byte = (1 << 6)
	bit5mask byte = (1 << 5)
	bit4mask byte = (1 << 4)
	bit3mask byte = (1 << 3)
	bit2mask byte = (1 << 2)
	bit1mask byte = (1 << 1)
	bit0mask byte = (1 << 0)
)

//Serialize serializes the tuple
//Now we just consider the primary index
func (cvls *CompactValueLayoutSerializer) Serialize(out []byte, ctx *ValueLayoutContext) ([]byte, error) {
	//Section 1 : the control bytes
	//1, Fill The control byte
	var controlByte byte = 0
	//bit7:0-primary index；1-secondary index。
	if ctx.IndexDesc.ID == PrimaryIndexID {
		controlByte &= ^bit7mask
	} else {
		controlByte |= bit7mask
		return nil, errorSecondaryIndexIsUnsupported
	}

	//bit6: 0-no column group; 1-has column group
	if cvls.needColumnGroup {
		controlByte |= bit6mask
	} else {
		controlByte &= ^bit6mask
	}

	//bit5: 0-The Attribute X Format does not have Attribute ID；1-The Attribute X Format has Attribute ID
	if cvls.needAttributeIDInFormat {
		controlByte |= bit5mask
		return nil, errorAttributeIDInFormatIsUnsupported
	} else {
		controlByte &= ^bit5mask
	}

	buf8bytes := make([]byte, 8)
	//bit0~bit2: the offset of the Attributes Offset Section
	if cvls.needColumnGroup {
		binary.BigEndian.PutUint32(buf8bytes, uint32(ctx.ColumnGroup))

		//fixed 4 bytes
		controlByte |= (bit2mask | bit1mask | bit0mask)

		out = append(out, controlByte)
		out = append(out, buf8bytes[:4]...)
	} else {
		controlByte &= ^(bit2mask | bit1mask | bit0mask)
		//offset is 1
		controlByte |= bit0mask
		out = append(out, controlByte)
	}

	//collect attribute id in the index
	attributeIDInTheIndex := make(map[uint32]int8)

	for _, attribute := range ctx.IndexDesc.Attributes {
		if _, exist := attributeIDInTheIndex[attribute.ID]; !exist {
			attributeIDInTheIndex[attribute.ID] = 1
		} else {
			return nil, errorDuplicateAttributeIDInIndex
		}
	}

	attributeCount := uint32(0)

	//Section 2 :the offset
	attributeCountOffset := len(out)

	//append 4 bytes space for the count of the attributes
	out = append(out, buf8bytes[:4]...)

	//count the number of attributes need to be stored
	//serialize the value when needed
	for _, state := range ctx.AttributeStates {
		//only store the attribute that is not in the index
		if _, exist := attributeIDInTheIndex[state.AttrDesc.ID]; !exist {
			attributeCount++

			//store the id of the attribute
			binary.BigEndian.PutUint32(buf8bytes, state.AttrDesc.ID)
			out = append(out, buf8bytes[:4]...)

			//append 4 bytes space for the offset of the attribute
			out = append(out, buf8bytes[:4]...)
		}
	}

	//store the count of attributes
	binary.BigEndian.PutUint32(out[attributeCountOffset:], attributeCount)

	var value interface{}
	var err error
	var serialized []byte

	attrCount, err := ctx.Tuple.GetAttributeCount()
	if err != nil {
		return nil, err
	}

	calcAttributeOffsetFunc := func(attrIndex int) int {
		return attributeCountOffset +
			4 + // 4 bytes of the attribute count
			(attrIndex * 8) // 8 bytes of the attribute id and offset for previouse attributes
	}

	//Section 3: the attribute data
	//append 4 bytes for the bytes of the attribute data
	out = append(out, buf8bytes[:4]...)

	attributeDataOffset := len(out)

	attributeIndex := 0
	//serialize the value when needed
	for _, state := range ctx.AttributeStates {
		//only store the attribute that is not in the index
		if _, exist := attributeIDInTheIndex[state.AttrDesc.ID]; !exist {
			//the logic for implicit primary key or default expr
			if state.NeedGenerated {
				if state.AttrDesc.Default.Exist { //default expr
					value = state.AttrDesc.Default.Value
				} else {
					//get the implicit primary key
					value = state.ImplicitPrimaryKey
				}
			} else {
				if uint32(state.PositionInBatch) >= attrCount {
					return nil, errorNoSuchAttributeInTuple
				}
				if ctx.Tuple != nil {
					value, err = ctx.Tuple.GetValue(uint32(state.PositionInBatch))
					if err != nil {
						return nil, err
					}
				} else {
					if state.PositionInBatch >= len(ctx.Fields) || state.PositionInBatch < 0 {
						return nil, errorNoSuchField
					}
					value = ctx.Fields[state.PositionInBatch]
				}
			}

			lastOffset := len(out)

			//serial value
			serialized, _, err = cvls.Serializer.SerializeValue(out, value)
			if err != nil {
				return nil, err
			}
			out = serialized

			off := calcAttributeOffsetFunc(attributeIndex)
			binary.BigEndian.PutUint32(out[off+4:], uint32(lastOffset))

			attributeIndex++
		}
	}

	//store the bytes of the attribute data
	binary.BigEndian.PutUint32(out[attributeDataOffset-4:], uint32(len(out)-attributeDataOffset))
	return out, nil
}

func (cvls *CompactValueLayoutSerializer) Deserialize(data []byte, amForValue *AttributeMap) ([]byte, []*orderedcodec.DecodedItem, []*ValueDecodedItem, error) {
	if len(data) < 1 {
		return nil, nil, nil, errorNoEnoughBytes
	}
	controlByte := data[0]

	//bit7:0-primary index；1-secondary index
	if controlByte&bit7mask != 0 {
		return nil, nil, nil, errorSecondaryIndexIsUnsupported
	}

	hasColumnGroup := false

	//bit6: 0-no column group; 1-has column group
	if controlByte&bit6mask != 0 {
		hasColumnGroup = true
	}

	//bit5: 0-The Attribute X Format does not have Attribute ID；1-The Attribute X Format has Attribute ID
	if controlByte&bit5mask != 0 {
		return nil, nil, nil, errorAttributeIDInFormatIsUnsupported
	}

	offsetSectionPos := 1
	if hasColumnGroup {
		offsetSectionPos += 4
	}

	if len(data[offsetSectionPos:]) < 4 {
		return nil, nil, nil, errorNoEnoughBytes
	}

	offsetArrayLen := binary.BigEndian.Uint32(data[offsetSectionPos:])
	offsetSectionPos += 4

	excessLengthOfTheData := func(p int) bool {
		return p >= len(data)
	}

	if excessLengthOfTheData(offsetSectionPos) {
		return nil, nil, nil, errorNoEnoughBytes
	}

	var valueDis []*ValueDecodedItem

	wantIDIndex := 0
	for i := 0; i < int(offsetArrayLen) && wantIDIndex < amForValue.Length(); i++ {
		if excessLengthOfTheData(offsetSectionPos + 3) {
			return nil, nil, nil, errorNoEnoughBytes
		}
		id := binary.BigEndian.Uint32(data[offsetSectionPos:])

		offsetSectionPos += 4
		if excessLengthOfTheData(offsetSectionPos + 3) {
			return nil, nil, nil, errorNoEnoughBytes
		}
		off := binary.BigEndian.Uint32(data[offsetSectionPos:])

		if id == uint32(amForValue.GetAttributeAtSortedIndex(wantIDIndex)) {
			vdi := &ValueDecodedItem{
				OffsetInUndecodedKey: int(off),
				RawBytes:             data[off:],
				ID:                   id,
				serializer:           cvls.Serializer,
			}
			wantIDIndex++
			valueDis = append(valueDis, vdi)
		}

		offsetSectionPos += 4
	}

	// skip the rest of the data
	if excessLengthOfTheData(offsetSectionPos + 3) {
		return nil, nil, nil, errorNoEnoughBytes
	}

	dataLen := int(binary.BigEndian.Uint32(data[offsetSectionPos:]))
	offsetSectionPos += 4
	if excessLengthOfTheData(offsetSectionPos + dataLen - 1) {
		return nil, nil, nil, errorNoEnoughBytes
	}
	offsetSectionPos += dataLen
	return data[offsetSectionPos:], nil, valueDis, nil
}

func (cvls *CompactValueLayoutSerializer) GetPositionsOfAttributesInTheValue(tableDesc *descriptor.RelationDesc, indexDesc *descriptor.IndexDesc) map[uint32]int {
	indexAttrIDs := make(map[uint32]int8)
	for _, attr := range indexDesc.Attributes {
		indexAttrIDs[attr.ID] = 1
	}
	attr2pos := make(map[uint32]int)
	positionInValue := 0
	for _, attr := range tableDesc.Attributes {
		if _, exist := indexAttrIDs[attr.ID]; !exist {
			attr2pos[attr.ID] = positionInValue
			positionInValue++
		}
	}
	return attr2pos
}

// DefaultValueLayoutSerializer defines a default layout of the value
/*
The Layout of the value:

The attributes in the value keep the same order as they defined in the relation.

| Attribute1,Attribute2,... |

*/
type DefaultValueLayoutSerializer struct {
	Serializer ValueSerializer
}

func (dvls *DefaultValueLayoutSerializer) Serialize(out []byte, ctx *ValueLayoutContext) ([]byte, error) {
	var value interface{}
	var err error
	//fill value into the row from the tuple
	for _, state := range ctx.AttributeStates {
		//the logic for implicit primary key or default expr
		if state.NeedGenerated {
			if state.AttrDesc.Default.Exist { //default expr
				value = state.AttrDesc.Default.Value
			} else {
				//get the implicit primary key
				value = state.ImplicitPrimaryKey
			}
		} else {
			if ctx.Tuple != nil {
				value, err = ctx.Tuple.GetValue(uint32(state.PositionInBatch))
				if err != nil {
					return nil, err
				}
			} else {
				if state.PositionInBatch >= len(ctx.Fields) || state.PositionInBatch < 0 {
					return nil, errorNoSuchField
				}
				value = ctx.Fields[state.PositionInBatch]
			}

		}
		//serial value
		serialized, _, err := dvls.Serializer.SerializeValue(out, value)
		if err != nil {
			return nil, err
		}
		out = serialized
	}
	return out, nil
}

func (dvls *DefaultValueLayoutSerializer) Deserialize(data []byte, amForValue *AttributeMap) ([]byte, []*orderedcodec.DecodedItem, []*ValueDecodedItem, error) {
	var vdis []*ValueDecodedItem
	id := uint32(0)
	wantIDIndex := 0
	for len(data) != 0 && wantIDIndex < amForValue.Length() {
		serializedBytes, next, err := extractSerializedBytes(data)
		if err != nil {
			return nil, nil, nil, err
		}

		if id == uint32(amForValue.GetAttributeAtSortedIndex(wantIDIndex)) {
			vdis = append(vdis, &ValueDecodedItem{
				OffsetInUndecodedKey:     -1,
				RawBytes:                 serializedBytes,
				BytesCountInUndecodedKey: -1,
				ID:                       id,
				serializer:               dvls.Serializer,
			})
			wantIDIndex++
		}

		id++
		data = next
	}
	return data, nil, vdis, nil
}

func (dvls *DefaultValueLayoutSerializer) GetPositionsOfAttributesInTheValue(tableDesc *descriptor.RelationDesc, indexDesc *descriptor.IndexDesc) map[uint32]int {
	positions := make(map[uint32]int)
	for _, attr := range tableDesc.Attributes {
		positions[attr.ID] = int(attr.ID)
	}
	return positions
}
