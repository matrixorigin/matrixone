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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"strconv"
)

var (
	errorTheUvarintOfTheColumnGroupIsTooLarge = errors.New("the uvarint of the column group is too large")
	errorSecondaryIndexIsUnsupported          = errors.New("secondary index is unsupported now")
	errorDuplicateAttributeIDInIndex          = errors.New("duplicate attribute id in the index")
	errorAttributeIDInFormatIsUnsupported     = errors.New("the attributeID in format is unsupported now")
	errorNoSuchAttributeInTuple               = errors.New("no such attribute in the tuple")
	errorGetOffsetArrayLenFailed              = errors.New("get offsetArrayLen failed")
)

type SerializerType int

const (
	ST_JSON    SerializerType = iota
	ST_CONCISE SerializerType = iota + 1
)

//ValueSerializer for serializing the value
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
		value = string(actualData[1 : len(actualData)-1])
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
			0, 1+bytesRead+int(len(actualData))),
		nil
}

type ConciseSerializer struct {
	bufReader bytes.Reader
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

	cs.bufReader.Reset(actualData)

	//unmarshal value
	var value interface{}

	//use number decode
	switch vt {
	case orderedcodec.VALUE_TYPE_NULL:
		value = nil
	case orderedcodec.VALUE_TYPE_BOOL:
		var x bool
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT8:
		var x int8
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT16:
		var x int16
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT32:
		var x int32
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_INT64:
		var x int64
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT8:
		var x uint8
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT16:
		var x uint16
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT32:
		var x uint32
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_UINT64:
		var x uint64
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_FLOAT32:
		var x float32
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_FLOAT64:
		var x float64
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = x
	case orderedcodec.VALUE_TYPE_DATE:
		var x int32
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
		if err != nil {
			return nil, nil, err
		}
		value = types.Date(x)
	case orderedcodec.VALUE_TYPE_DATETIME:
		var x int64
		err := binary.Read(&cs.bufReader, binary.BigEndian, &x)
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
			0, 1+bytesRead+int(len(actualData))),
		nil
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

type ValueLayoutContext struct {
	TableDesc *descriptor.RelationDesc
	IndexDesc *descriptor.IndexDesc

	//write control for the attribute
	AttributeStates []AttributeStateForWrite
	Tuple           Tuple

	ColumnGroup uint64
}

// ValueLayoutSerializer defines the layout of the value serialization
type ValueLayoutSerializer interface {
	Serialize(ctx *ValueLayoutContext) ([]byte, error)
	Deserialize(data []byte) ([]byte, []*orderedcodec.DecodedItem, error)
}

var _ ValueLayoutSerializer = &DefaultValueLayoutSerializer{}

// DefaultValueLayoutSerializer defines a compact layout of the value
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
type DefaultValueLayoutSerializer struct {
	needColumnGroup         bool
	needAttributeIDInFormat bool
	serializer              ValueSerializer
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
func (dvls *DefaultValueLayoutSerializer) Serialize(ctx *ValueLayoutContext) ([]byte, error) {
	var out []byte
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
	if dvls.needColumnGroup {
		controlByte |= bit6mask
	} else {
		controlByte &= ^bit6mask
	}

	//bit5: 0-The Attribute X Format does not have Attribute ID；1-The Attribute X Format has Attribute ID
	if dvls.needAttributeIDInFormat {
		controlByte |= bit5mask
		return nil, errorAttributeIDInFormatIsUnsupported
	} else {
		controlByte &= ^bit5mask
	}

	//bit0~bit2: the offset of the Attributes Offset Section
	if dvls.needColumnGroup {
		cgBuf := make([]byte, binary.MaxVarintLen32)
		//!!!NOTE: just use the low 32 bit
		cg := uint32(ctx.ColumnGroup)
		cgLen := binary.PutUvarint(cgBuf, uint64(cg))
		if cgLen > int(byte(bit2mask|bit1mask|bit0mask)) {
			return nil, errorTheUvarintOfTheColumnGroupIsTooLarge
		}

		controlByte |= byte(cgLen + 1)

		out = append(out, controlByte)
		out = append(out, cgBuf[:cgLen]...)
	} else {
		controlByte &= ^(bit2mask | bit1mask | bit0mask)
		//offset is 1
		controlByte |= bit0mask
		out = append(out, controlByte)
	}

	//collect attribute id in the index
	attributeIDInTheInex := make(map[uint32]int8)

	for _, attribute := range ctx.IndexDesc.Attributes {
		if _, exist := attributeIDInTheInex[attribute.ID]; !exist {
			attributeIDInTheInex[attribute.ID] = 1
		} else {
			return nil, errorDuplicateAttributeIDInIndex
		}
	}

	var value interface{}
	var err error
	var serialized []byte
	var dataStorage []byte
	var offsetArr offsetArray

	attrCount, err := ctx.Tuple.GetAttributeCount()
	if err != nil {
		return nil, err
	}

	//serialize the value when needed
	for _, state := range ctx.AttributeStates {
		//only store the attribute that is not in the index
		if _, exist := attributeIDInTheInex[state.AttrDesc.ID]; !exist {
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
				value, err = ctx.Tuple.GetValue(uint32(state.PositionInBatch))
				if err != nil {
					return nil, err
				}
			}

			lastOffset := len(dataStorage)

			//serial value
			serialized, _, err = dvls.serializer.SerializeValue(dataStorage, value)
			if err != nil {
				return nil, err
			}
			offsetArr.Append(offset{
				ID:          state.AttrDesc.ID,
				bytesOffset: lastOffset,
				bytesCount:  len(serialized),
			})
			dataStorage = serialized
		}
	}

	//make out the output
	var offsetStorage []byte
	//1.make out the offset section
	//a.calc the count of the attribute
	tempBuf := make([]byte, binary.MaxVarintLen64)
	offsetArrayLen := offsetArr.Count()
	offsetAttributeCountByteCount := binary.PutUvarint(tempBuf, uint64(offsetArrayLen))

	offsetStorage = append(offsetStorage, tempBuf[:offsetAttributeCountByteCount]...)

	adjustLen := len(out) + offsetAttributeCountByteCount + (8+8)*offsetArrayLen + 8

	//b.setup offset
	//can not use var encoding here
	for i := 0; i < offsetArrayLen; i++ {
		of := offsetArr.Get(i)
		//id
		binary.BigEndian.PutUint64(tempBuf, uint64(of.id()))

		offsetStorage = append(offsetStorage, tempBuf[:8]...)

		//adjusted offset
		binary.BigEndian.PutUint64(tempBuf, uint64(of.adjustBytesOffset(adjustLen)))

		offsetStorage = append(offsetStorage, tempBuf[:8]...)
	}

	//2. composite the final
	out = append(out, offsetStorage...)

	//add the length of the dataStorage
	binary.BigEndian.PutUint64(tempBuf, uint64(len(dataStorage)))
	out = append(out, tempBuf[:8]...)

	out = append(out, dataStorage...)
	return out, nil
}

func (dvls *DefaultValueLayoutSerializer) Deserialize(data []byte) ([]byte, []*orderedcodec.DecodedItem, error) {
	if len(data) < 1 {
		return nil, nil, errorNoEnoughBytes
	}
	controlByte := data[0]

	//bit7:0-primary index；1-secondary index
	if controlByte&bit7mask != 0 {
		return nil, nil, errorSecondaryIndexIsUnsupported
	}

	hasColumnGroup := false

	//bit6: 0-no column group; 1-has column group
	if controlByte&bit6mask != 0 {
		hasColumnGroup = true
	}

	//bit5: 0-The Attribute X Format does not have Attribute ID；1-The Attribute X Format has Attribute ID
	if controlByte&bit5mask != 0 {
		return nil, nil, errorAttributeIDInFormatIsUnsupported
	}

	offsetSectionPos := 1
	if hasColumnGroup {
		offsetSectionPos = int(controlByte & byte(bit2mask|bit1mask|bit0mask))
	}

	offsetArrayLen, bytesRead := binary.Uvarint(data[offsetSectionPos:])
	if bytesRead <= 0 {
		return nil, nil, errorGetOffsetArrayLenFailed
	}

	offsetSectionPos += bytesRead

	excessLengthOfTheData := func(p int) bool {
		return p >= len(data)
	}

	if excessLengthOfTheData(offsetSectionPos) {
		return nil, nil, errorNoEnoughBytes
	}

	var dis []*orderedcodec.DecodedItem

	for i := 0; i < int(offsetArrayLen); i++ {
		if excessLengthOfTheData(offsetSectionPos + 7) {
			return nil, nil, errorNoEnoughBytes
		}
		id := binary.BigEndian.Uint64(data[offsetSectionPos:])

		offsetSectionPos += 8
		if excessLengthOfTheData(offsetSectionPos + 7) {
			return nil, nil, errorNoEnoughBytes
		}
		offset := binary.BigEndian.Uint64(data[offsetSectionPos:])

		di := &orderedcodec.DecodedItem{}
		di.ID = uint32(id)
		di.OffsetInUndecodedKey = int(offset)
		dis = append(dis, di)

		offsetSectionPos += 8
	}

	// skip the rest of the data
	if excessLengthOfTheData(offsetSectionPos + 7) {
		return nil, nil, errorNoEnoughBytes
	}

	dataLen := int(binary.BigEndian.Uint64(data[offsetSectionPos:]))
	offsetSectionPos += 8
	if excessLengthOfTheData(offsetSectionPos + dataLen - 1) {
		return nil, nil, errorNoEnoughBytes
	}
	offsetSectionPos += dataLen
	return data[offsetSectionPos:], dis, nil
}
