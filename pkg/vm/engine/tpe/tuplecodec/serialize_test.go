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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/orderedcodec"
	"github.com/smartystreets/assertions/should"
	"github.com/smartystreets/goconvey/convey"
	"reflect"
	"testing"
)

func TestConciseSerializer_SerializeValue(t *testing.T) {
	type args struct {
		value interface{}
		valueType byte
	}

	convey.Convey("serialize value 1",t, func() {
		kases := []args{
			{nil,SERIAL_TYPE_NULL},
			{uint64(0),SERIAL_TYPE_UINT64},
			{"abc",SERIAL_TYPE_STRING},
			{[]byte{1,2,3},SERIAL_TYPE_BYTES},
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
			fmt.Printf("i %d value %v\n",i,kase.value)
			data, _, err := serial.SerializeValue(nil,kase.value)
			convey.So(err,convey.ShouldBeNil)

			//value data
			var marshal []byte
			if kase.valueType == SERIAL_TYPE_STRING {
				s := kase.value.(string)
				marshal = []byte(s)
			}else if kase.valueType != SERIAL_TYPE_NULL {
				var writeBuf bytes.Buffer
				err = binary.Write(&writeBuf, binary.BigEndian, kase.value)
				convey.So(err,convey.ShouldBeNil)

				marshal = writeBuf.Bytes()
			}

			var buf [20]byte
			byteWritten := binary.PutVarint(buf[:],int64(len(marshal)))

			convey.So(data[0],convey.ShouldEqual,kase.valueType)
			convey.So(data[1:1+byteWritten],should.Resemble,buf[:byteWritten])
			if len(marshal) == 0 {
				xlen := len(data[1+byteWritten:1+byteWritten+len(marshal)])
				convey.So(xlen,convey.ShouldEqual,0)
			}else{
				convey.So(data[1+byteWritten:1+byteWritten+len(marshal)],
					should.Resemble,
					marshal)
			}
		}
	})
}

func TestConciseSerializer_DeserializeValue(t *testing.T) {
	type args struct {
		value interface{}
		valueType byte
	}

	convey.Convey("serialize value 1",t, func() {
		kases := []args{
			{nil, byte(orderedcodec.VALUE_TYPE_NULL)},
			{uint64(0), byte(orderedcodec.VALUE_TYPE_UINT64)},
			{"abc", byte(orderedcodec.VALUE_TYPE_STRING)},
			{[]byte{1,2,3}, byte(orderedcodec.VALUE_TYPE_BYTES)},
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
			data, _, err := serial.SerializeValue(nil,kase.value)
			convey.So(err,convey.ShouldBeNil)

			rest, dis, err := serial.DeserializeValue(data)
			convey.So(err,convey.ShouldBeNil)
			convey.So(dis.ValueType,should.Equal,kase.valueType)
			convey.So(reflect.DeepEqual(dis.Value,kase.value),convey.ShouldBeTrue)
			convey.So(rest,convey.ShouldBeEmpty)
		}
	})

	convey.Convey("serialize value 2",t, func() {
		kases := []args{
			{nil, byte(orderedcodec.VALUE_TYPE_NULL)},
			{uint64(0), byte(orderedcodec.VALUE_TYPE_UINT64)},
			{"abc", byte(orderedcodec.VALUE_TYPE_STRING)},
			{[]byte{1,2,3}, byte(orderedcodec.VALUE_TYPE_BYTES)},
		}

		serial := &ConciseSerializer{}
		var data []byte
		for _, kase := range kases {
			res, _, err := serial.SerializeValue(data,kase.value)
			convey.So(err,convey.ShouldBeNil)
			data = res
		}

		for _, kase := range kases {
			rest, dis, err := serial.DeserializeValue(data)
			convey.So(err,convey.ShouldBeNil)
			convey.So(dis.ValueType,should.Equal,kase.valueType)
			convey.So(reflect.DeepEqual(dis.Value,kase.value),convey.ShouldBeTrue)
			data = rest
		}
	})
}