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
	"errors"
	"go/constant"
	"math"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/smartystreets/goconvey/convey"
)

func Test_handleInsertValues(t *testing.T) {

}

var tableDefs = []engine.TableDef{
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: []byte("123")},
			Type:    types.Type{Oid: types.T_char},
			Name:    "a"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: []byte("123")},
			Type:    types.Type{Oid: types.T_varchar, Width: 10},
			Name:    "b"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: uint8(1)},
			Type:    types.Type{Oid: types.T_uint8},
			Name:    "c"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: int8(1)},
			Type:    types.Type{Oid: types.T_int8},
			Name:    "d"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: uint16(1)},
			Type:    types.Type{Oid: types.T_uint16},
			Name:    "e"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: int16(1)},
			Type:    types.Type{Oid: types.T_int16},
			Name:    "f"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: uint32(1)},
			Type:    types.Type{Oid: types.T_uint32},
			Name:    "g"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: int32(1)},
			Type:    types.Type{Oid: types.T_int32},
			Name:    "h"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: uint64(1)},
			Type:    types.Type{Oid: types.T_uint64},
			Name:    "i"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: int64(1)},
			Type:    types.Type{Oid: types.T_int64},
			Name:    "j"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: float32(1)},
			Type:    types.Type{Oid: types.T_float32},
			Name:    "k"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: float64(1)},
			Type:    types.Type{Oid: types.T_float64},
			Name:    "l"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: types.Date(100)},
			Type:    types.Type{Oid: types.T_date},
			Name:    "m"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: types.Datetime(100)},
			Type:    types.Type{Oid: types.T_datetime},
			Name:    "n"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: types.Decimal64_One},
			Type: types.Type{
				Oid:       types.T_decimal64,
				Size:      0,
				Width:     10,
				Scale:     2,
				Precision: 0,
			},
			Name: "o"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: types.Decimal128_One},
			Type: types.Type{
				Oid:       types.T_decimal128,
				Size:      0,
				Width:     20,
				Scale:     2,
				Precision: 0,
			},
			Name: "p"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: types.NowUTC()},
			Type: types.Type{
				Oid:       types.T_timestamp,
				Size:      0,
				Width:     0,
				Scale:     0,
				Precision: 0,
			},
			Name: "q"}},
	&engine.AttributeDef{
		Attr: engine.Attribute{
			Default: engine.DefaultExpr{Exist: true, Value: true},
			Type: types.Type{
				Oid:       types.T_bool,
				Size:      0,
				Width:     1,
				Scale:     0,
				Precision: 0,
			},
			Name: "r"}},
}

func Test_buildInsertValues(t *testing.T) {
	var stmt = &tree.Insert{}
	var plan = &InsertValues{}
	var snapshot engine.Snapshot
	var num = &tree.NumVal{}
	var err error
	convey.Convey("buildInsertValues failed", t, func() {
		err = buildInsertValues(stmt, plan, nil, snapshot)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("buildInsertValues succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		db := mock_frontend.NewMockDatabase(ctrl)
		rel := mock_frontend.NewMockRelation(ctrl)
		eng.EXPECT().Database(gomock.Any(), nil).Return(nil, errors.New("1")).Times(1)

		table := &tree.TableName{}
		clause := &tree.ValuesClause{}
		stmt.Table = table
		stmt.Rows = &tree.Select{}
		stmt.Rows.Select = clause
		plan.currentDb = "ssb"
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		eng.EXPECT().Database(gomock.Any(), nil).Return(db, nil).AnyTimes()
		db.EXPECT().Relation(gomock.Any(), nil).Return(nil, errors.New("1")).Times(1)
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		db.EXPECT().Relation(gomock.Any(), nil).Return(rel, nil).AnyTimes()
		rel.EXPECT().TableDefs(nil).Return(tableDefs).AnyTimes()
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		stmt.Columns = make([]tree.Identifier, 1)
		stmt.Columns[0] = "a"
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		clause = &tree.ValuesClause{Rows: []tree.Exprs{make([]tree.Expr, 0)}}
		stmt.Rows.Select = clause
		err = buildInsertValues(stmt, plan, eng, snapshot)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1234", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][0] = num
		tableDefs[0].(*engine.AttributeDef).Attr.Default = engine.DefaultExpr{}
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][0] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][2] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUint64(math.MaxUint8+1), "256", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][2] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][2] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][3] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUint64(math.MaxInt8+1), "128", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][3] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][3] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][4] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUint64(math.MaxUint16+1), "65536", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][4] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][4] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][5] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUint64(math.MaxInt16+1), "32768", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][5] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][5] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][6] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUint64(math.MaxUint32+1), "4294967296", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][6] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][6] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][7] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUint64(math.MaxInt32+1), "2147483648", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][7] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][7] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][8] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][8] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][9] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][9] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][10] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(math.MaxFloat32*2), "2147483648", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][10] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][10] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][11] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][11] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][12] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("123"), "123", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][12] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][12] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][13] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("123"), "123", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][13] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][13] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][14] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(math.MaxFloat64), "123", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][14] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][14] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][15] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(math.MaxFloat64), "123", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][15] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][15] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][16] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][16] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "1", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][17] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeUnknown(), "", false)
		stmt.Rows.Select.(*tree.ValuesClause).Rows[0][17] = num
		err = buildInsertValues(stmt, plan, eng, snapshot)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_makeExprFromVal(t *testing.T) {
	var typ types.Type
	var value interface{}
	var ret tree.Expr
	var isNull bool
	convey.Convey("makeExprFromVal NULL", t, func() {
		isNull = true
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUnknown())
		convey.So(tmp.String(), convey.ShouldEqual, "NULL")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	isNull = false
	convey.Convey("makeExprFromVal bool", t, func() {
		typ.Oid = types.T_bool
		value = true
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeBool(true))
		convey.So(tmp.String(), convey.ShouldEqual, "true")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal int8", t, func() {
		typ.Oid = types.T_int8
		value = int8(-1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUint64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "-1")
		convey.So(tmp.Negative(), convey.ShouldBeTrue)

		value = int8(1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok = ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeInt64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "1")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal int16", t, func() {
		typ.Oid = types.T_int16
		value = int16(-1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUint64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "-1")
		convey.So(tmp.Negative(), convey.ShouldBeTrue)

		value = int16(1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok = ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeInt64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "1")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal int32", t, func() {
		typ.Oid = types.T_int32
		value = int32(-1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUint64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "-1")
		convey.So(tmp.Negative(), convey.ShouldBeTrue)

		value = int32(1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok = ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeInt64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "1")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal int64", t, func() {
		typ.Oid = types.T_int64
		value = int64(-1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUint64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "-1")
		convey.So(tmp.Negative(), convey.ShouldBeTrue)

		value = int64(1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok = ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeInt64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "1")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal uint8", t, func() {
		typ.Oid = types.T_uint8
		value = uint8(1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUint64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "1")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal uint16", t, func() {
		typ.Oid = types.T_uint16
		value = uint16(1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUint64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "1")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal uint32", t, func() {
		typ.Oid = types.T_uint32
		value = uint32(1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUint64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "1")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal uint64", t, func() {
		typ.Oid = types.T_uint64
		value = uint64(1)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUint64(1))
		convey.So(tmp.String(), convey.ShouldEqual, "1")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal float32", t, func() {
		typ.Oid = types.T_float32
		value = float32(16)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeFloat64(16))
		convey.So(tmp.String(), convey.ShouldEqual, "16.0000000000")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal float64", t, func() {
		typ.Oid = types.T_float64
		value = float64(16)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeFloat64(16))
		convey.So(tmp.String(), convey.ShouldEqual, "16.0000000000")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal char", t, func() {
		typ.Oid = types.T_char
		value = []byte("123")
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeString("123"))
		convey.So(tmp.String(), convey.ShouldEqual, "123")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal date", t, func() {
		typ.Oid = types.T_date
		value = types.Date(100)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeString("0001-04-11"))
		convey.So(tmp.String(), convey.ShouldEqual, "0001-04-11")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal datetime", t, func() {
		typ.Oid = types.T_datetime
		value = types.Datetime(100)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeString("0001-01-01 00:00:00"))
		convey.So(tmp.String(), convey.ShouldEqual, "0001-01-01 00:00:00")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal decimal64", t, func() {
		typ.Oid = types.T_decimal64
		value = types.Decimal64FromInt32(100)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeString("1E+2"))
		convey.So(tmp.String(), convey.ShouldEqual, "1E+2")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal decimal128", t, func() {
		typ.Oid = types.T_decimal128
		value = types.Decimal128FromInt32(100)
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeString("1E+2"))
		convey.So(tmp.String(), convey.ShouldEqual, "1E+2")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})

	convey.Convey("makeExprFromVal decimal128", t, func() {
		typ.Oid = types.T_json
		ret = makeExprFromVal(typ, value, isNull)
		tmp, ok := ret.(*tree.NumVal)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(tmp.Value, convey.ShouldResemble, constant.MakeUnknown())
		convey.So(tmp.String(), convey.ShouldEqual, "NULL")
		convey.So(tmp.Negative(), convey.ShouldBeFalse)
	})
}

func Test_rewriteInsertRows(t *testing.T) {
	var noInsertTarget = true
	var finalInsertTargets []string
	var relationAttrs []string
	var rows []tree.Exprs
	var defaultExprs = make(map[string]tree.Expr)
	var ret []tree.Exprs
	var str []string
	var err error
	var num = &tree.NumVal{}
	convey.Convey("rewriteInsertRows succ", t, func() {
		finalInsertTargets = []string{"a", "b"}
		relationAttrs = []string{"a", "b", "c", "d"}
		ret, str, err = rewriteInsertRows(noInsertTarget, finalInsertTargets, relationAttrs, rows, defaultExprs)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(str, convey.ShouldResemble, relationAttrs)
		convey.So(err, convey.ShouldBeNil)

		rows = make([]tree.Exprs, 11)
		num.Value = constant.MakeUnknown()
		defaultExprs["a"] = num
		ret, str, err = rewriteInsertRows(noInsertTarget, finalInsertTargets, relationAttrs, rows, defaultExprs)
		convey.So(ret, convey.ShouldResemble, []tree.Exprs(nil))
		convey.So(str, convey.ShouldResemble, []string(nil))
		convey.So(err, convey.ShouldNotBeNil)

		rows[0] = make([]tree.Expr, 2)
		rows[0][0] = num
		ret, str, err = rewriteInsertRows(noInsertTarget, finalInsertTargets, relationAttrs, rows, defaultExprs)
		convey.So(ret, convey.ShouldResemble, []tree.Exprs(nil))
		convey.So(str, convey.ShouldResemble, []string(nil))
		convey.So(err, convey.ShouldNotBeNil)

		rows = make([]tree.Exprs, 1)
		rows[0] = make([]tree.Expr, 2)
		rows[0][0] = num
		ret, str, err = rewriteInsertRows(noInsertTarget, finalInsertTargets, relationAttrs, rows, defaultExprs)
		convey.So(ret, convey.ShouldResemble, []tree.Exprs(nil))
		convey.So(str, convey.ShouldResemble, []string(nil))
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_buildConstant(t *testing.T) {
	var typ types.Type
	var n tree.Expr
	var ret interface{}
	var num = &tree.NumVal{}
	var err error

	convey.Convey("buildConstant ParenExpr", t, func() {
		tmp := &tree.ParenExpr{}
		num.Value = constant.MakeUnknown()
		tmp.Expr = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("buildConstant NumVal", t, func() {
		num.Value = constant.MakeUnknown()
		n = num
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("buildConstant UnaryExpr", t, func() {
		tmp := &tree.UnaryExpr{Op: tree.UNARY_PLUS}
		num.Value = constant.MakeUnknown()
		tmp.Expr = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)

		tmp = &tree.UnaryExpr{Op: tree.UNARY_MINUS}
		tmp.Expr = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)

		tmp.Expr = nil
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num.Value = constant.MakeInt64(1)
		tmp.Expr = &tree.ParenExpr{Expr: num}
		n = tmp
		typ.Oid = types.T_int64
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, -1)
		convey.So(err, convey.ShouldBeNil)

		num.Value = constant.MakeUint64(1)
		tmp.Expr = &tree.ParenExpr{Expr: num}
		n = tmp
		typ.Oid = types.T_uint64
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num.Value = constant.MakeUint64(0)
		tmp.Expr = &tree.ParenExpr{Expr: num}
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)

		num.Value = constant.MakeFloat64(1.2)
		tmp.Expr = &tree.ParenExpr{Expr: num}
		n = tmp
		typ.Oid = types.T_float32
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, -1.2)
		convey.So(err, convey.ShouldBeNil)

		num.Value = constant.MakeFloat64(1.2)
		tmp.Expr = &tree.ParenExpr{Expr: num}
		n = tmp
		typ.Oid = types.T_float64
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, -1.2)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("buildConstant BinaryExpr", t, func() {
		tmp := &tree.BinaryExpr{Op: tree.PLUS}
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1), "1", false)
		typ.Oid = types.T_int64
		tmp.Left = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		tmp.Right = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(math.MaxFloat64), "1", false)
		tmp.Left = num
		tmp.Right = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		num = tree.NewNumVal(constant.MakeFloat64(-math.MaxFloat64), "1", false)
		tmp.Left = num
		tmp.Right = tree.NewNumVal(constant.MakeFloat64(math.MaxFloat64), "1", false)
		tmp.Op = tree.MINUS
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		num = tree.NewNumVal(constant.MakeFloat64(-math.MaxFloat64), "1", false)
		tmp.Left = num
		tmp.Right = num
		tmp.Op = tree.MULTI
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		num = tree.NewNumVal(constant.MakeFloat64(-math.MaxFloat64), "1", false)
		tmp.Left = num
		tmp.Right = tree.NewNumVal(constant.MakeFloat64(0.1), "1", false)
		tmp.Op = tree.DIV
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		tmp.Right = tree.NewNumVal(constant.MakeFloat64(0), "1", false)
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, ErrDivByZero)

		num = tree.NewNumVal(constant.MakeFloat64(-math.MaxFloat64), "1", false)
		tmp.Left = num
		tmp.Right = tree.NewNumVal(constant.MakeFloat64(0), "1", false)
		tmp.Op = tree.INTEGER_DIV
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, ErrDivByZero)

		tmp.Right = tree.NewNumVal(constant.MakeFloat64(0.1), "1", false)
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		tmp.Left = tree.NewNumVal(constant.MakeFloat64(1), "1", false)
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 10)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(-math.MaxFloat64), "1", false)
		tmp.Left = num
		tmp.Right = tree.NewNumVal(constant.MakeFloat64(0), "1", false)
		tmp.Op = tree.MOD
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, ErrZeroModulus)

		tmp.Left = tree.NewNumVal(constant.MakeFloat64(10), "1", false)
		tmp.Right = tree.NewNumVal(constant.MakeFloat64(3), "1", false)
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		tmp.Op = tree.MOD + 1
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("buildConstant BinaryExpr2", t, func() {
		tmp := &tree.BinaryExpr{Op: tree.PLUS}
		num = tree.NewNumVal(constant.MakeFloat64(math.MaxInt64), "1", false)
		tmp.Left = num
		tmp.Right = num
		n = tmp
		typ.Oid = types.T_int8
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		num = tree.NewNumVal(constant.MakeFloat64(-math.MaxInt64), "1", false)
		tmp.Left = num
		tmp.Right = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		num = tree.NewNumVal(constant.MakeFloat64(0), "1", false)
		tmp.Left = num
		tmp.Right = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(-1), "1", false)
		tmp.Left = num
		tmp.Right = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, -2)
		convey.So(err, convey.ShouldBeNil)

		typ.Oid = types.T_uint8
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		num = tree.NewNumVal(constant.MakeFloat64(1), "1", false)
		tmp.Left = num
		tmp.Right = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldBeNil)

		typ.Oid = types.T_float32
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(0), "1", false)
		tmp.Left = num
		tmp.Right = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(math.MaxFloat32), "1", false)
		tmp.Left = num
		tmp.Right = num
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		typ.Oid = types.T_float64
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 2*math.MaxFloat32)
		convey.So(err, convey.ShouldBeNil)

		typ.Oid = types.T_bool
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("buildConstant UnresolvedName", t, func() {
		tmp := &tree.UnresolvedName{}
		tmp.Parts[0] = "1.2a"
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		tmp.Parts[0] = strconv.FormatFloat(2*math.MaxInt64, 'f', -1, 32)
		n = tmp
		typ.Oid = types.T_int8
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		tmp.Parts[0] = "0"
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)

		tmp.Parts[0] = "123"
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 123)
		convey.So(err, convey.ShouldBeNil)

		tmp.Parts[0] = strconv.FormatFloat(2*math.MinInt64, 'f', -1, 32)
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		tmp.Parts[0] = "-123"
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, -123)
		convey.So(err, convey.ShouldBeNil)

		tmp.Parts[0] = "-123"
		n = tmp
		typ.Oid = types.T_uint8
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		tmp.Parts[0] = "123"
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 123)
		convey.So(err, convey.ShouldBeNil)

		tmp.Parts[0] = "0"
		n = tmp
		typ.Oid = types.T_float32
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)

		tmp.Parts[0] = strconv.FormatFloat(2*math.MaxFloat32, 'f', -1, 32)
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldEqual, errBinaryOutRange)

		tmp.Parts[0] = "123"
		n = tmp
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 123)
		convey.So(err, convey.ShouldBeNil)

		tmp.Parts[0] = "123"
		n = tmp
		typ.Oid = types.T_float64
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldEqual, 123)
		convey.So(err, convey.ShouldBeNil)

		typ.Oid = types.T_bool
		ret, err = buildConstant(typ, n)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		ret, err = buildConstant(typ, nil)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_buildConstantValue(t *testing.T) {
	var typ types.Type
	var num = &tree.NumVal{}
	var ret interface{}
	var err error
	convey.Convey("buildConstantValue Unknown", t, func() {
		num.Value = constant.MakeUnknown()
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("buildConstantValue Bool", t, func() {
		num.Value = constant.MakeBool(true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)

		num.Value = constant.MakeBool(false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeFalse)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("buildConstantValue Int", t, func() {
		num.Value = constant.MakeInt64(1)
		typ.Oid = types.T_bool
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)

		num.Value = constant.MakeInt64(2)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "-1", true)
		typ.Oid = types.T_int8
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(-1), "1", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(-1), "1", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1.0", false)
		typ.Width = 10
		typ.Scale = 1
		typ.Oid = types.T_decimal64
		ret, err = buildConstantValue(typ, num)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1.2", false)
		typ.Width = 38
		typ.Scale = 1
		typ.Oid = types.T_decimal128
		ret, err = buildConstantValue(typ, num)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1", false)
		typ.Oid = types.T_uint8
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1", false)
		typ.Oid = types.T_float32
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "-1", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1", false)
		typ.Oid = types.T_float64
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "-1", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1", false)
		typ.Oid = types.T_timestamp
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeInt64(1), "1", false)
		typ.Oid = types.T_char
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("buildConstantValue Float", t, func() {
		num = tree.NewNumVal(constant.MakeFloat64(1), "1", false)
		typ.Oid = types.T_int8
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1), "1a.1", false)
		typ.Oid = types.T_int8
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "1.5", false)
		typ.Oid = types.T_int64
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(math.MaxInt64+0.5), strconv.Itoa(math.MaxInt64)+".5", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "-1.5", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -2)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(-math.MinInt64+0.5), strconv.Itoa(math.MinInt64)+".5", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "1.5", false)
		typ.Oid = types.T_uint64
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 2)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "1a.5", false)
		typ.Oid = types.T_uint64
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(math.MaxInt64+0.5), strconv.FormatUint(math.MaxUint64, 10)+".5", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "1.5", false)
		typ.Oid = types.T_float32
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1.5)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "-1.5", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1.5)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "1.5", false)
		typ.Oid = types.T_float64
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1.5)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "-1.5", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1.5)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "1.5", false)
		typ.Oid = types.T_decimal64
		typ.Width = 34
		typ.Scale = 1
		ret, err = buildConstantValue(typ, num)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "-1.5a", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "1.5", false)
		typ.Oid = types.T_decimal128
		typ.Width = 34
		typ.Scale = 1
		ret, err = buildConstantValue(typ, num)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeFloat64(1.5), "-1.5a", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		typ.Oid = types.T_bool
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("buildConstantValue String", t, func() {
		num = tree.NewNumVal(constant.MakeString("true"), "true", false)
		typ.Oid = types.T_bool
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("false"), "false", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeFalse)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("abc"), "abc", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("-1"), "-1", true)
		typ.Oid = types.T_int8
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("-1a"), "-1a", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("-1"), "-1", true)
		typ.Oid = types.T_int16
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("-1a"), "-1a", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("-1"), "-1", true)
		typ.Oid = types.T_int32
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("-1a"), "-1a", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("-1"), "-1", true)
		typ.Oid = types.T_int64
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, -1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("-1a"), "-1a", true)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("1"), "1", false)
		typ.Oid = types.T_uint8
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("1a"), "1a", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("1"), "1", false)
		typ.Oid = types.T_uint16
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("1a"), "1a", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("1"), "1", false)
		typ.Oid = types.T_uint32
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("1a"), "1a", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("1"), "1", false)
		typ.Oid = types.T_uint64
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("1a"), "1a", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("1.2"), "1.2", false)
		typ.Oid = types.T_float32
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1.2)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("1.2a"), "1.2a", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("1.2"), "1.2", false)
		typ.Oid = types.T_float64
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, 1.2)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("1.2a"), "1.2a", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("1.2"), "1.2", false)
		typ.Oid = types.T_decimal64
		ret, err = buildConstantValue(typ, num)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("1.2a"), "1.2a", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("1.2"), "1.2", false)
		typ.Oid = types.T_decimal128
		ret, err = buildConstantValue(typ, num)
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("1.2a"), "1.2a", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("123"), "123", false)
		typ.Oid = types.T_char
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, "123")
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("2022-07-13"), "2022-07-13", false)
		typ.Oid = types.T_date
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, types.Date(738348))
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("2022-07-130"), "2022-07-130", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("2022-07-13 11:11:11.1234"), "2022-07-13 11:11:11.1234", false)
		typ.Oid = types.T_datetime
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldEqual, types.Datetime(66892131174711296))
		convey.So(err, convey.ShouldBeNil)

		num = tree.NewNumVal(constant.MakeString("2022-07-130 11:11:11.1234"), "2022-07-130 11:11:11.1234", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		num = tree.NewNumVal(constant.MakeString("2022-07-130 11:11:11.1234"), "2022-07-130 11:11:11.1234", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("buildConstantValue Complex", t, func() {
		num = tree.NewNumVal(constant.MakeImag(constant.MakeInt64(1)), "123", false)
		ret, err = buildConstantValue(typ, num)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_rangeCheck(t *testing.T) {
	var value interface{}
	var typ types.Type
	columnName := "col1"
	rowNumber := 1
	var ret interface{}
	var err error
	convey.Convey("rangeCheck int64", t, func() {
		value = int64(1)
		typ = types.Type{Oid: types.T_int8}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_int16}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_int32}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_int64}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_bool}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		value = int64(128)
		typ = types.Type{Oid: types.T_int8}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("rangeCheck uint64", t, func() {
		value = uint64(1)
		typ = types.Type{Oid: types.T_uint8}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_uint16}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_uint32}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_uint64}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_bool}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		value = uint64(256)
		typ = types.Type{Oid: types.T_uint8}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("rangeCheck float32", t, func() {
		value = float32(1.2)
		typ = types.Type{Oid: types.T_float32}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, 1.2)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_float64}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("rangeCheck float64", t, func() {
		value = math.MaxFloat32
		typ = types.Type{Oid: types.T_float32}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, math.MaxFloat32)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_float64}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, math.MaxFloat32)
		convey.So(err, convey.ShouldBeNil)

		typ = types.Type{Oid: types.T_bool}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		value = math.MaxFloat64
		typ = types.Type{Oid: types.T_float32}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("rangeCheck string", t, func() {
		value = "123"
		typ = types.Type{Oid: types.T_char, Width: 4}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldEqual, "123")
		convey.So(err, convey.ShouldBeNil)

		typ.Width = 1
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)

		typ = types.Type{Oid: types.T_bool}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("rangeCheck bool", t, func() {
		value = true
		typ = types.Type{Oid: types.T_bool}
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)

		value = typ
		ret, err = rangeCheck(value, typ, columnName, rowNumber)
		convey.So(ret, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}
