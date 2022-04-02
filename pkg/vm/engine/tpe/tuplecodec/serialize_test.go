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
	"math"
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

func TestFlatSerializer_SerializeValue(t *testing.T) {
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

		serial := &FlatSerializer{}
		for i, kase := range kases {
			fmt.Printf("i %d value %v\n", i, kase.value)
			data, _, err := serial.SerializeValue(nil, kase.value)
			convey.So(err, convey.ShouldBeNil)

			//value data
			var marshal []byte
			if kase.value == nil {
			} else {
				switch v := kase.value.(type) {
				case bool:
					if v {
						marshal = []byte{1}
					} else {
						marshal = []byte{0}
					}
				case int8:
					marshal = []byte{byte(v)}
				case int16:
					marshal = []byte{0, 0}
					binary.BigEndian.PutUint16(marshal, uint16(v))
				case int32:
					marshal = []byte{0, 0, 0, 0}
					binary.BigEndian.PutUint32(marshal, uint32(v))
				case int64:
					marshal = []byte{0, 0, 0, 0, 0, 0, 0, 0}
					binary.BigEndian.PutUint64(marshal, uint64(v))
				case uint8:
					marshal = []byte{v}
				case uint16:
					marshal = []byte{0, 0}
					binary.BigEndian.PutUint16(marshal, v)
				case uint32:
					marshal = []byte{0, 0, 0, 0}
					binary.BigEndian.PutUint32(marshal, v)
				case uint64:
					marshal = []byte{0, 0, 0, 0, 0, 0, 0, 0}
					binary.BigEndian.PutUint64(marshal, v)
				case float32:
					marshal = []byte{0, 0, 0, 0}
					uv := math.Float32bits(v)
					binary.BigEndian.PutUint32(marshal, uv)
				case float64:
					marshal = []byte{0, 0, 0, 0, 0, 0, 0, 0}
					uv := math.Float64bits(v)
					binary.BigEndian.PutUint64(marshal, uv)
				case types.Date:
					marshal = []byte{0, 0, 0, 0}
					binary.BigEndian.PutUint32(marshal, uint32(v))
				case types.Datetime:
					marshal = []byte{0, 0, 0, 0}
					binary.BigEndian.PutUint32(marshal, uint32(v))
				case []byte:
					marshal = v
				case string:
					marshal = []byte(v)
				default:

				}
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

func TestFlatSerializer_DeserializeValue(t *testing.T) {
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

		serial := &FlatSerializer{}
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

func TestCompactValueLayoutSerializer_Serialize(t *testing.T) {
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

		layout := &CompactValueLayoutSerializer{
			needColumnGroup:         false,
			needAttributeIDInFormat: false,
			Serializer:              &ConciseSerializer{},
		}

		ctx := &ValueLayoutContext{
			TableDesc:       relDesc,
			IndexDesc:       indexDesc,
			AttributeStates: attrStates,
			Tuple:           make_tuple(),
			ColumnGroup:     0,
		}

		got, err := layout.Serialize(nil, ctx)
		convey.So(err, convey.ShouldBeNil)

		am := AttributeMap{}

		for i, attr := range relDesc.Attributes {
			if i == 0 {
				continue
			}
			am.Append(int(attr.ID), i, i)
		}
		am.BuildPositionInDecodedItemArray()

		rest, _, vdis, err := layout.Deserialize(got, &am)
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(rest), convey.ShouldBeZeroValue)
		convey.So(len(vdis), convey.ShouldEqual, len(kases)-1)
		for i := 0; i < len(vdis); i++ {
			di, err := vdis[i].DecodeValue()
			convey.So(err, convey.ShouldBeNil)

			convey.So(reflect.DeepEqual(di.GetValue(), kases[i+1].value), convey.ShouldBeTrue)
		}
	})
}
