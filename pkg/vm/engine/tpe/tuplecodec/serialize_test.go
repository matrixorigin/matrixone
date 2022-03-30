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
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	mock_tuplecodec "github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec/test"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestDefaultValueSerializer_SerializeValue(t *testing.T) {
	type args struct {
		value     interface{}
		valueType byte
	}

	convey.Convey("serialize value 1", t, func() {
		kases := []args{
			{nil, SERIAL_TYPE_NULL},
			{uint64(0), SERIAL_TYPE_UINT64},
			{"abc", SERIAL_TYPE_STRING},
			{[]byte{1, 2, 3}, SERIAL_TYPE_BYTES},
			{true, SERIAL_TYPE_BOOL},
			{int8(127), SERIAL_TYPE_INT8},
			{int16(127), SERIAL_TYPE_INT16},
			{int32(127), SERIAL_TYPE_INT32},
			{int64(127), SERIAL_TYPE_INT64},
			{uint8(127), SERIAL_TYPE_UINT8},
			{uint16(127), SERIAL_TYPE_UINT16},
			{uint32(127), SERIAL_TYPE_UINT32},
			{float32(1.0), SERIAL_TYPE_FLOAT32},
			{float64(1.0), SERIAL_TYPE_FLOAT64},
			{types.Date(1), SERIAL_TYPE_DATE},
			{types.Datetime(1), SERIAL_TYPE_DATETIME},
		}

		serial := &DefaultValueSerializer{}
		for _, kase := range kases {
			data, _, err := serial.SerializeValue(nil, kase.value)
			convey.So(err, convey.ShouldBeNil)

			//value data
			marshal, err := json.Marshal(kase.value)
			convey.So(err, convey.ShouldBeNil)

			var buf [20]byte
			byteWritten := binary.PutVarint(buf[:], int64(len(marshal)))

			convey.So(data[0], convey.ShouldEqual, kase.valueType)
			convey.So(data[1:1+byteWritten], should.Resemble, buf[:byteWritten])
			convey.So(data[1+byteWritten:1+byteWritten+len(marshal)],
				should.Resemble,
				marshal)
		}
	})
}

func TestDefaultValueSerializer_DeserializeValue(t *testing.T) {
	type args struct {
		value     interface{}
		valueType byte
	}

	convey.Convey("serialize value 1", t, func() {
		kases := []args{
			{nil, byte(orderedcodec.VALUE_TYPE_NULL)},
			{uint64(0), byte(orderedcodec.VALUE_TYPE_UINT64)},
			{"abc", byte(orderedcodec.VALUE_TYPE_STRING)},
			{[]byte{1, 2, 3}, byte(orderedcodec.VALUE_TYPE_BYTES)},
			{int8(1), byte(orderedcodec.VALUE_TYPE_INT8)},
			{int16(2), byte(orderedcodec.VALUE_TYPE_INT16)},
			{int32(3), byte(orderedcodec.VALUE_TYPE_INT32)},
			{int64(4), byte(orderedcodec.VALUE_TYPE_INT64)},
			{uint8(5), byte(orderedcodec.VALUE_TYPE_UINT8)},
			{uint16(6), byte(orderedcodec.VALUE_TYPE_UINT16)},
			{uint32(7), byte(orderedcodec.VALUE_TYPE_UINT32)},
			{float32(1.0), byte(orderedcodec.VALUE_TYPE_FLOAT32)},
			{float64(2.0), byte(orderedcodec.VALUE_TYPE_FLOAT64)},
			{types.Date(8), byte(orderedcodec.VALUE_TYPE_DATE)},
			{types.Datetime(9), byte(orderedcodec.VALUE_TYPE_DATETIME)},
		}

		serial := &DefaultValueSerializer{}
		for _, kase := range kases {
			data, _, err := serial.SerializeValue(nil, kase.value)
			convey.So(err, convey.ShouldBeNil)

			rest, dis, err := serial.DeserializeValue(data)
			convey.So(err, convey.ShouldBeNil)
			convey.So(dis.ValueType, should.Equal, kase.valueType)
			convey.So(reflect.DeepEqual(dis.Value, kase.value), convey.ShouldBeTrue)
			convey.So(rest, convey.ShouldBeEmpty)
		}
	})

	convey.Convey("serialize value 2", t, func() {
		kases := []args{
			{nil, byte(orderedcodec.VALUE_TYPE_NULL)},
			{uint64(0), byte(orderedcodec.VALUE_TYPE_UINT64)},
			{"abc", byte(orderedcodec.VALUE_TYPE_STRING)},
			{[]byte{1, 2, 3}, byte(orderedcodec.VALUE_TYPE_BYTES)},
		}

		serial := &DefaultValueSerializer{}
		var data []byte
		for _, kase := range kases {
			res, _, err := serial.SerializeValue(data, kase.value)
			convey.So(err, convey.ShouldBeNil)
			data = res
		}

		for _, kase := range kases {
			rest, dis, err := serial.DeserializeValue(data)
			convey.So(err, convey.ShouldBeNil)
			convey.So(dis.ValueType, should.Equal, kase.valueType)
			convey.So(reflect.DeepEqual(dis.Value, kase.value), convey.ShouldBeTrue)
			data = rest
		}
	})
}

func TestConciseSerializer_SerializeValue(t *testing.T) {
	type args struct {
		value     interface{}
		valueType byte
	}

	convey.Convey("serialize value 1", t, func() {
		kases := []args{
			{nil, SERIAL_TYPE_NULL},
			{uint64(0), SERIAL_TYPE_UINT64},
			{"abc", SERIAL_TYPE_STRING},
			{[]byte{1, 2, 3}, SERIAL_TYPE_BYTES},
			{true, SERIAL_TYPE_BOOL},
			{int8(127), SERIAL_TYPE_INT8},
			{int16(127), SERIAL_TYPE_INT16},
			{int32(127), SERIAL_TYPE_INT32},
			{int64(127), SERIAL_TYPE_INT64},
			{uint8(127), SERIAL_TYPE_UINT8},
			{uint16(127), SERIAL_TYPE_UINT16},
			{uint32(127), SERIAL_TYPE_UINT32},
			{float32(1.0), SERIAL_TYPE_FLOAT32},
			{float64(1.0), SERIAL_TYPE_FLOAT64},
			{types.Date(1), SERIAL_TYPE_DATE},
			{types.Datetime(1), SERIAL_TYPE_DATETIME},
		}

		serial := &ConciseSerializer{}
		for i, kase := range kases {
			fmt.Printf("i %d value %v\n", i, kase.value)
			data, _, err := serial.SerializeValue(nil, kase.value)
			convey.So(err, convey.ShouldBeNil)

			//value data
			var marshal []byte
			if kase.valueType == SERIAL_TYPE_STRING {
				s := kase.value.(string)
				marshal = []byte(s)
			} else if kase.valueType != SERIAL_TYPE_NULL {
				var writeBuf bytes.Buffer
				err = binary.Write(&writeBuf, binary.BigEndian, kase.value)
				convey.So(err, convey.ShouldBeNil)

				marshal = writeBuf.Bytes()
			}

			var buf [20]byte
			byteWritten := binary.PutVarint(buf[:], int64(len(marshal)))

			convey.So(data[0], convey.ShouldEqual, kase.valueType)
			convey.So(data[1:1+byteWritten], should.Resemble, buf[:byteWritten])
			if len(marshal) == 0 {
				xlen := len(data[1+byteWritten : 1+byteWritten+len(marshal)])
				convey.So(xlen, convey.ShouldEqual, 0)
			} else {
				convey.So(data[1+byteWritten:1+byteWritten+len(marshal)],
					should.Resemble,
					marshal)
			}
		}
	})
}

func TestConciseSerializer_DeserializeValue(t *testing.T) {
	type args struct {
		value     interface{}
		valueType byte
	}

	convey.Convey("serialize value 1", t, func() {
		kases := []args{
			{nil, byte(orderedcodec.VALUE_TYPE_NULL)},
			{uint64(0), byte(orderedcodec.VALUE_TYPE_UINT64)},
			{"abc", byte(orderedcodec.VALUE_TYPE_STRING)},
			{[]byte{1, 2, 3}, byte(orderedcodec.VALUE_TYPE_BYTES)},
			{int8(1), byte(orderedcodec.VALUE_TYPE_INT8)},
			{int16(2), byte(orderedcodec.VALUE_TYPE_INT16)},
			{int32(3), byte(orderedcodec.VALUE_TYPE_INT32)},
			{int64(4), byte(orderedcodec.VALUE_TYPE_INT64)},
			{uint8(5), byte(orderedcodec.VALUE_TYPE_UINT8)},
			{uint16(6), byte(orderedcodec.VALUE_TYPE_UINT16)},
			{uint32(7), byte(orderedcodec.VALUE_TYPE_UINT32)},
			{float32(1.0), byte(orderedcodec.VALUE_TYPE_FLOAT32)},
			{float64(2.0), byte(orderedcodec.VALUE_TYPE_FLOAT64)},
			{types.Date(8), byte(orderedcodec.VALUE_TYPE_DATE)},
			{types.Datetime(9), byte(orderedcodec.VALUE_TYPE_DATETIME)},
		}

		serial := &ConciseSerializer{}
		for _, kase := range kases {
			data, _, err := serial.SerializeValue(nil, kase.value)
			convey.So(err, convey.ShouldBeNil)

			rest, dis, err := serial.DeserializeValue(data)
			convey.So(err, convey.ShouldBeNil)
			convey.So(dis.ValueType, should.Equal, kase.valueType)
			convey.So(reflect.DeepEqual(dis.Value, kase.value), convey.ShouldBeTrue)
			convey.So(rest, convey.ShouldBeEmpty)
		}
	})

	convey.Convey("serialize value 2", t, func() {
		kases := []args{
			{nil, byte(orderedcodec.VALUE_TYPE_NULL)},
			{uint64(0), byte(orderedcodec.VALUE_TYPE_UINT64)},
			{"abc", byte(orderedcodec.VALUE_TYPE_STRING)},
			{[]byte{1, 2, 3}, byte(orderedcodec.VALUE_TYPE_BYTES)},
		}

		serial := &ConciseSerializer{}
		var data []byte
		for _, kase := range kases {
			res, _, err := serial.SerializeValue(data, kase.value)
			convey.So(err, convey.ShouldBeNil)
			data = res
		}

		for _, kase := range kases {
			rest, dis, err := serial.DeserializeValue(data)
			convey.So(err, convey.ShouldBeNil)
			convey.So(dis.ValueType, should.Equal, kase.valueType)
			convey.So(reflect.DeepEqual(dis.Value, kase.value), convey.ShouldBeTrue)
			data = rest
		}
	})
}

func TestDefaultValueLayoutSerializer_Serialize(t *testing.T) {
	type args struct {
		value     interface{}
		valueType byte
	}
	convey.Convey("layout test", t, func() {
		kases := []args{
			{nil, byte(orderedcodec.VALUE_TYPE_NULL)},
			{uint64(0), byte(orderedcodec.VALUE_TYPE_UINT64)},
			{"abc", byte(orderedcodec.VALUE_TYPE_STRING)},
			{[]byte{1, 2, 3}, byte(orderedcodec.VALUE_TYPE_BYTES)},
			{int8(1), byte(orderedcodec.VALUE_TYPE_INT8)},
			{int16(2), byte(orderedcodec.VALUE_TYPE_INT16)},
			{int32(3), byte(orderedcodec.VALUE_TYPE_INT32)},
			{int64(4), byte(orderedcodec.VALUE_TYPE_INT64)},
			{uint8(5), byte(orderedcodec.VALUE_TYPE_UINT8)},
			{uint16(6), byte(orderedcodec.VALUE_TYPE_UINT16)},
			{uint32(7), byte(orderedcodec.VALUE_TYPE_UINT32)},
			{float32(1.0), byte(orderedcodec.VALUE_TYPE_FLOAT32)},
			{float64(2.0), byte(orderedcodec.VALUE_TYPE_FLOAT64)},
			{types.Date(8), byte(orderedcodec.VALUE_TYPE_DATE)},
			{types.Datetime(9), byte(orderedcodec.VALUE_TYPE_DATETIME)},
		}

		relDesc := &descriptor.RelationDesc{}
		indexDesc := &descriptor.IndexDesc{
			ID: PrimaryIndexID,
		}
		attrStates := []AttributeStateForWrite{}
		for i, kase := range kases {
			attrDesc := descriptor.AttributeDesc{
				ID:                uint32(i),
				Name:              fmt.Sprintf("name%d", i),
				Ttype:             orderedcodec.ValueType(kase.valueType),
				TypesType:         types.Type{},
				Default:           engine.DefaultExpr{},
				DefaultVal:        descriptor.DefaultValue{},
				Is_null:           false,
				Default_value:     "",
				Is_hidden:         false,
				Is_auto_increment: false,
				Is_unique:         true,
				Is_primarykey:     true,
				Comment:           "",
				References:        nil,
				Constrains:        nil,
			}
			relDesc.Attributes = append(relDesc.Attributes, attrDesc)
			if i == 0 {
				indexDesc.Attributes = append(indexDesc.Attributes, descriptor.IndexDesc_Attribute{
					Name:      attrDesc.Name,
					Direction: 0,
					ID:        attrDesc.ID,
					Type:      attrDesc.Ttype,
					TypesType: types.Type{},
				})
			}

			state := AttributeStateForWrite{
				PositionInBatch:    i,
				NeedGenerated:      false,
				AttrDesc:           attrDesc,
				ImplicitPrimaryKey: nil,
			}
			attrStates = append(attrStates, state)
		}

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		make_tuple := func() Tuple {
			tuple := mock_tuplecodec.NewMockTuple(ctrl)
			tuple.EXPECT().GetAttributeCount().Return(uint32(len(kases)), nil).AnyTimes()
			for i, kase := range kases {
				tuple.EXPECT().GetValue(uint32(i)).Return(kase.value, nil).AnyTimes()
			}
			return tuple
		}

		layout := &DefaultValueLayoutSerializer{
			needColumnGroup:         false,
			needAttributeIDInFormat: false,
			serializer:              &ConciseSerializer{},
		}

		ctx := &ValueLayoutContext{
			TableDesc:       relDesc,
			IndexDesc:       indexDesc,
			AttributeStates: attrStates,
			Tuple:           make_tuple(),
			ColumnGroup:     0,
		}

		got, err := layout.Serialize(ctx)
		convey.So(err, convey.ShouldBeNil)

		var want []byte

		//make wanted bytes
		var controlByte byte = 0
		//bit7 0 primary index
		controlByte &= ^bit7mask
		//bit6 0-no column group
		controlByte &= ^bit6mask
		//bit5: 0-The Attribute X Format does not have Attribute ID
		controlByte &= ^bit5mask
		//bit0~bit2: the offset of the Attributes Offset Section
		controlByte &= ^(bit2mask | bit1mask | bit0mask)
		//offset is 1
		controlByte |= bit0mask

		want = append(want, controlByte)

		//

		var dataStorage []byte
		var offsetArr offsetArray

		attrCount, err := ctx.Tuple.GetAttributeCount()
		convey.So(err, convey.ShouldBeNil)

		//serialize the value when needed
		for i, state := range ctx.AttributeStates {
			//only store the attribute that is not in the index
			if i == 0 {
				//first attribute is pk
				continue
			}

			//the logic for implicit primary key or default expr
			convey.So(uint32(state.PositionInBatch) >= attrCount, convey.ShouldBeFalse)

			value, err := ctx.Tuple.GetValue(uint32(state.PositionInBatch))
			convey.So(err, convey.ShouldBeNil)

			lastOffset := len(dataStorage)

			//serial value
			serialized, _, err := layout.serializer.SerializeValue(dataStorage, value)

			convey.So(err, convey.ShouldBeNil)

			offsetArr.Append(offset{
				ID:          state.AttrDesc.ID,
				bytesOffset: lastOffset,
				bytesCount:  len(serialized),
			})
			dataStorage = serialized
		}

		var offsetSection []byte
		buf := make([]byte, binary.MaxVarintLen64)
		offsetArrayLen := offsetArr.Count()
		l := binary.PutUvarint(buf, uint64(offsetArrayLen))

		offsetSection = append(offsetSection, buf[:l]...)

		adjust := len(want) + l + (16 * offsetArrayLen) + 8

		//can not use var encoding here
		for i := 0; i < offsetArrayLen; i++ {
			of := offsetArr.Get(i)
			//id
			binary.BigEndian.PutUint64(buf, uint64(of.id()))

			offsetSection = append(offsetSection, buf[:8]...)

			//adjusted offset
			binary.BigEndian.PutUint64(buf, uint64(of.adjustBytesOffset(adjust)))

			offsetSection = append(offsetSection, buf[:8]...)
		}

		want = append(want, offsetSection...)

		//the length of the dataStorage
		binary.BigEndian.PutUint64(buf, uint64(len(dataStorage)))
		want = append(want, buf[:8]...)

		want = append(want, dataStorage...)

		convey.So(got, convey.ShouldResemble, want)

		rest, dis, err := layout.Deserialize(want)
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(rest), convey.ShouldBeZeroValue)
		convey.So(len(dis), convey.ShouldEqual, offsetArrayLen)
		for i := 0; i < len(dis); i++ {
			di := dis[i]
			off := offsetArr.Get(i)
			convey.So(di.ID, convey.ShouldEqual, off.id())
			convey.So(di.OffsetInUndecodedKey, convey.ShouldEqual, off.adjustBytesOffset(adjust))
		}
	})
}
