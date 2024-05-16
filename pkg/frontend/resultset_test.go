// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"testing"

	"github.com/smartystreets/goconvey/convey"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func Test_GetInt64(t *testing.T) {
	var ret int64
	var err error
	var colNum = 16
	convey.Convey("GetInt64 succ", t, func() {
		mrs := &MysqlResultSet{}
		mrs.Data = make([][]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			mrs.Data[i] = make([]interface{}, 1)
		}
		ret, err = mrs.GetInt64(context.TODO(), 1, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)

		ret, err = mrs.GetInt64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)

		mrs.Data[0][0] = true
		mrs.Columns = make([]Column, colNum)
		ret, err = mrs.GetInt64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[0][0] = false
		ret, err = mrs.GetInt64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[1][0] = uint8(1)
		ret, err = mrs.GetInt64(context.TODO(), 1, 0)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[2][0] = uint16(2)
		ret, err = mrs.GetInt64(context.TODO(), 2, 0)
		convey.So(ret, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[3][0] = uint32(3)
		ret, err = mrs.GetInt64(context.TODO(), 3, 0)
		convey.So(ret, convey.ShouldEqual, 3)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[4][0] = uint64(4)
		ret, err = mrs.GetInt64(context.TODO(), 4, 0)
		convey.So(ret, convey.ShouldEqual, 4)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[5][0] = int8(5)
		ret, err = mrs.GetInt64(context.TODO(), 5, 0)
		convey.So(ret, convey.ShouldEqual, 5)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[6][0] = int16(6)
		ret, err = mrs.GetInt64(context.TODO(), 6, 0)
		convey.So(ret, convey.ShouldEqual, 6)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[7][0] = int32(7)
		ret, err = mrs.GetInt64(context.TODO(), 7, 0)
		convey.So(ret, convey.ShouldEqual, 7)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[8][0] = int64(8)
		ret, err = mrs.GetInt64(context.TODO(), 8, 0)
		convey.So(ret, convey.ShouldEqual, 8)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[9][0] = float32(9)
		ret, err = mrs.GetInt64(context.TODO(), 9, 0)
		convey.So(ret, convey.ShouldEqual, 9)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[10][0] = float64(10)
		ret, err = mrs.GetInt64(context.TODO(), 10, 0)
		convey.So(ret, convey.ShouldEqual, 10)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[11][0] = "11"
		ret, err = mrs.GetInt64(context.TODO(), 11, 0)
		convey.So(ret, convey.ShouldEqual, 11)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[12][0] = []byte("12")
		ret, err = mrs.GetInt64(context.TODO(), 12, 0)
		convey.So(ret, convey.ShouldEqual, 12)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[13][0] = int(13)
		ret, err = mrs.GetInt64(context.TODO(), 13, 0)
		convey.So(ret, convey.ShouldEqual, 13)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[14][0] = uint(14)
		ret, err = mrs.GetInt64(context.TODO(), 14, 0)
		convey.So(ret, convey.ShouldEqual, 14)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[15][0] = types.Decimal64(15)
		ret, err = mrs.GetInt64(context.TODO(), 15, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_GetUint64(t *testing.T) {
	var ret uint64
	var err error
	var colNum = 16
	convey.Convey("GetUint64 succ", t, func() {
		mrs := &MysqlResultSet{}
		mrs.Data = make([][]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			mrs.Data[i] = make([]interface{}, 1)
		}
		ret, err = mrs.GetUint64(context.TODO(), 1, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)

		ret, err = mrs.GetUint64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)

		mrs.Data[0][0] = true
		mrs.Columns = make([]Column, colNum)
		ret, err = mrs.GetUint64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[0][0] = false
		ret, err = mrs.GetUint64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[1][0] = uint8(1)
		ret, err = mrs.GetUint64(context.TODO(), 1, 0)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[2][0] = uint16(2)
		ret, err = mrs.GetUint64(context.TODO(), 2, 0)
		convey.So(ret, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[3][0] = uint32(3)
		ret, err = mrs.GetUint64(context.TODO(), 3, 0)
		convey.So(ret, convey.ShouldEqual, 3)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[4][0] = uint64(4)
		ret, err = mrs.GetUint64(context.TODO(), 4, 0)
		convey.So(ret, convey.ShouldEqual, 4)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[5][0] = int8(5)
		ret, err = mrs.GetUint64(context.TODO(), 5, 0)
		convey.So(ret, convey.ShouldEqual, 5)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[6][0] = int16(6)
		ret, err = mrs.GetUint64(context.TODO(), 6, 0)
		convey.So(ret, convey.ShouldEqual, 6)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[7][0] = int32(7)
		ret, err = mrs.GetUint64(context.TODO(), 7, 0)
		convey.So(ret, convey.ShouldEqual, 7)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[8][0] = int64(8)
		ret, err = mrs.GetUint64(context.TODO(), 8, 0)
		convey.So(ret, convey.ShouldEqual, 8)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[9][0] = float32(9)
		ret, err = mrs.GetUint64(context.TODO(), 9, 0)
		convey.So(ret, convey.ShouldEqual, 9)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[10][0] = float64(10)
		ret, err = mrs.GetUint64(context.TODO(), 10, 0)
		convey.So(ret, convey.ShouldEqual, 10)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[11][0] = "11"
		ret, err = mrs.GetUint64(context.TODO(), 11, 0)
		convey.So(ret, convey.ShouldEqual, 11)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[12][0] = []byte("12")
		ret, err = mrs.GetUint64(context.TODO(), 12, 0)
		convey.So(ret, convey.ShouldEqual, 12)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[13][0] = int(13)
		ret, err = mrs.GetUint64(context.TODO(), 13, 0)
		convey.So(ret, convey.ShouldEqual, 13)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[14][0] = uint(14)
		ret, err = mrs.GetUint64(context.TODO(), 14, 0)
		convey.So(ret, convey.ShouldEqual, 14)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[15][0] = types.Decimal64(15)
		ret, err = mrs.GetUint64(context.TODO(), 15, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_GetFloat64(t *testing.T) {
	var ret float64
	var err error
	var colNum = 16
	convey.Convey("GetFloat64 succ", t, func() {
		mrs := &MysqlResultSet{}
		mrs.Data = make([][]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			mrs.Data[i] = make([]interface{}, 1)
		}
		ret, err = mrs.GetFloat64(context.TODO(), 1, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)

		ret, err = mrs.GetFloat64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)

		mrs.Data[0][0] = true
		mrs.Columns = make([]Column, colNum)
		ret, err = mrs.GetFloat64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[0][0] = false
		ret, err = mrs.GetFloat64(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[1][0] = uint8(1)
		ret, err = mrs.GetFloat64(context.TODO(), 1, 0)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[2][0] = uint16(2)
		ret, err = mrs.GetFloat64(context.TODO(), 2, 0)
		convey.So(ret, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[3][0] = uint32(3)
		ret, err = mrs.GetFloat64(context.TODO(), 3, 0)
		convey.So(ret, convey.ShouldEqual, 3)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[4][0] = uint64(4)
		ret, err = mrs.GetFloat64(context.TODO(), 4, 0)
		convey.So(ret, convey.ShouldEqual, 4)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[5][0] = int8(5)
		ret, err = mrs.GetFloat64(context.TODO(), 5, 0)
		convey.So(ret, convey.ShouldEqual, 5)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[6][0] = int16(6)
		ret, err = mrs.GetFloat64(context.TODO(), 6, 0)
		convey.So(ret, convey.ShouldEqual, 6)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[7][0] = int32(7)
		ret, err = mrs.GetFloat64(context.TODO(), 7, 0)
		convey.So(ret, convey.ShouldEqual, 7)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[8][0] = int64(8)
		ret, err = mrs.GetFloat64(context.TODO(), 8, 0)
		convey.So(ret, convey.ShouldEqual, 8)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[9][0] = float32(9)
		ret, err = mrs.GetFloat64(context.TODO(), 9, 0)
		convey.So(ret, convey.ShouldEqual, 9)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[10][0] = float64(10)
		ret, err = mrs.GetFloat64(context.TODO(), 10, 0)
		convey.So(ret, convey.ShouldEqual, 10)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[11][0] = "11"
		ret, err = mrs.GetFloat64(context.TODO(), 11, 0)
		convey.So(ret, convey.ShouldEqual, 11)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[12][0] = []byte("12")
		ret, err = mrs.GetFloat64(context.TODO(), 12, 0)
		convey.So(ret, convey.ShouldEqual, 12)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[13][0] = int(13)
		ret, err = mrs.GetFloat64(context.TODO(), 13, 0)
		convey.So(ret, convey.ShouldEqual, 13)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[14][0] = uint(14)
		ret, err = mrs.GetFloat64(context.TODO(), 14, 0)
		convey.So(ret, convey.ShouldEqual, 14)
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[15][0] = types.Decimal64(15)
		ret, err = mrs.GetFloat64(context.TODO(), 15, 0)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_GetString(t *testing.T) {
	var ret string
	var err error
	var colNum = 19
	convey.Convey("GetString succ", t, func() {
		mrs := &MysqlResultSet{}
		mrs.Data = make([][]interface{}, colNum)
		for i := 0; i < colNum; i++ {
			mrs.Data[i] = make([]interface{}, 1)
		}
		ret, err = mrs.GetString(context.TODO(), 1, 0)
		convey.So(ret, convey.ShouldEqual, "")
		convey.So(err, convey.ShouldNotBeNil)

		ret, err = mrs.GetString(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, "")
		convey.So(err, convey.ShouldNotBeNil)

		mrs.Data[0][0] = true
		mrs.Columns = make([]Column, colNum)
		ret, err = mrs.GetString(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, "true")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[0][0] = false
		ret, err = mrs.GetString(context.TODO(), 0, 0)
		convey.So(ret, convey.ShouldEqual, "false")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[1][0] = uint8(1)
		ret, err = mrs.GetString(context.TODO(), 1, 0)
		convey.So(ret, convey.ShouldEqual, "1")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[2][0] = uint16(2)
		ret, err = mrs.GetString(context.TODO(), 2, 0)
		convey.So(ret, convey.ShouldEqual, "2")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[3][0] = uint32(3)
		ret, err = mrs.GetString(context.TODO(), 3, 0)
		convey.So(ret, convey.ShouldEqual, "3")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[4][0] = uint64(4)
		ret, err = mrs.GetString(context.TODO(), 4, 0)
		convey.So(ret, convey.ShouldEqual, "4")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[5][0] = int8(5)
		ret, err = mrs.GetString(context.TODO(), 5, 0)
		convey.So(ret, convey.ShouldEqual, "5")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[6][0] = int16(6)
		ret, err = mrs.GetString(context.TODO(), 6, 0)
		convey.So(ret, convey.ShouldEqual, "6")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[7][0] = int32(7)
		ret, err = mrs.GetString(context.TODO(), 7, 0)
		convey.So(ret, convey.ShouldEqual, "7")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[8][0] = int64(8)
		ret, err = mrs.GetString(context.TODO(), 8, 0)
		convey.So(ret, convey.ShouldEqual, "8")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[9][0] = float32(9)
		ret, err = mrs.GetString(context.TODO(), 9, 0)
		convey.So(ret, convey.ShouldEqual, "9")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[10][0] = float64(10)
		ret, err = mrs.GetString(context.TODO(), 10, 0)
		convey.So(ret, convey.ShouldEqual, "10")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[11][0] = "11"
		ret, err = mrs.GetString(context.TODO(), 11, 0)
		convey.So(ret, convey.ShouldEqual, "11")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[12][0] = []byte("12")
		ret, err = mrs.GetString(context.TODO(), 12, 0)
		convey.So(ret, convey.ShouldEqual, "12")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[13][0] = int(13)
		ret, err = mrs.GetString(context.TODO(), 13, 0)
		convey.So(ret, convey.ShouldEqual, "13")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[14][0] = uint(14)
		ret, err = mrs.GetString(context.TODO(), 14, 0)
		convey.So(ret, convey.ShouldEqual, "14")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[15][0] = types.Datetime(15)
		ret, err = mrs.GetString(context.TODO(), 15, 0)
		convey.So(ret, convey.ShouldEqual, "0001-01-01 00:00:00")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[16][0] = types.Decimal64(15)
		ret, err = mrs.GetString(context.TODO(), 16, 0)
		convey.So(ret, convey.ShouldEqual, "")
		convey.So(err, convey.ShouldNotBeNil)

		mrs.Data[17][0] = []float32{1, 2, 3}
		ret, err = mrs.GetString(context.TODO(), 17, 0)
		convey.So(ret, convey.ShouldEqual, "[1, 2, 3]")
		convey.So(err, convey.ShouldBeNil)

		mrs.Data[18][0] = []float64{1, 2, 3}
		ret, err = mrs.GetString(context.TODO(), 18, 0)
		convey.So(ret, convey.ShouldEqual, "[1, 2, 3]")
		convey.So(err, convey.ShouldBeNil)
	})
}
