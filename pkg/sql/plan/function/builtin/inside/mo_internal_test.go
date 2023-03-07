// Copyright 2021 - 2022 Matrix Origin
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

package inside

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/smartystreets/goconvey/convey"
)

// InternalCharSize Implementation of mo internal function 'internal_char_size'
func TestInternalCharSize(t *testing.T) {
	convey.Convey("Test01", t, func() {
		cases := []struct {
			typ    types.Type
			expect int
		}{
			{
				typ:    types.New(types.T_char, 50, 0),
				expect: types.TypeSize(types.T_char) * 50,
			},
			{
				typ:    types.New(types.T_char, 10, 0),
				expect: types.TypeSize(types.T_char) * 10,
			},
			{
				typ:    types.New(types.T_varchar, 50, 0),
				expect: types.TypeSize(types.T_varchar) * 50,
			},
			{
				typ:    types.New(types.T_varchar, 10, 0),
				expect: types.TypeSize(types.T_varchar) * 10,
			},
			{
				typ:    types.New(types.T_blob, 50, 0),
				expect: types.TypeSize(types.T_blob) * 50,
			},
		}

		var srcStrs []string
		var expects []int64
		for _, c := range cases {
			bytes, err := types.Encode(c.typ)
			if err != nil {
				t.Fatal(err)
			}
			srcStrs = append(srcStrs, string(bytes))
			expects = append(expects, int64(c.expect))
		}

		srcVector := testutil.MakeVarcharVector(srcStrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, nil)

		proc := testutil.NewProc()
		result, err := InternalCharSize([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Test02", t, func() {
		cases := struct {
			typ    types.Type
			expect int
		}{
			typ:    types.New(types.T_varchar, 50, 0),
			expect: types.TypeSize(types.T_varchar) * 50,
		}

		bytes, _ := types.Encode(cases.typ)
		srcVector := testutil.MakeScalarVarchar(string(bytes), 1)
		expectVector := testutil.MakeScalarInt64(int64(cases.expect), 1)

		proc := testutil.NewProc()
		result, err := InternalCharSize([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

// InternalCharLength Implementation of mo internal function 'internal_char_length'
func TestInternalCharLength(t *testing.T) {
	convey.Convey("Test03", t, func() {
		cases := []struct {
			typ    types.Type
			expect int64
		}{
			{
				typ:    types.New(types.T_char, 50, 0),
				expect: 50,
			},
			{
				typ:    types.New(types.T_char, 10, 0),
				expect: 10,
			},
			{
				typ:    types.New(types.T_varchar, 50, 0),
				expect: 50,
			},
			{
				typ:    types.New(types.T_varchar, 10, 0),
				expect: 10,
			},
			{
				typ:    types.New(types.T_blob, 50, 0),
				expect: 50,
			},
		}

		var srcStrs []string
		var expects []int64
		for _, c := range cases {
			bytes, err := types.Encode(c.typ)
			if err != nil {
				t.Fatal(err)
			}
			srcStrs = append(srcStrs, string(bytes))
			expects = append(expects, c.expect)
		}

		srcVector := testutil.MakeVarcharVector(srcStrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, nil)

		proc := testutil.NewProc()
		result, err := InternalCharLength([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Test04", t, func() {
		cases := struct {
			typ    types.Type
			expect int
		}{
			typ:    types.New(types.T_varchar, 50, 0),
			expect: 50,
		}

		bytes, _ := types.Encode(cases.typ)
		srcVector := testutil.MakeScalarVarchar(string(bytes), 1)
		expectVector := testutil.MakeScalarInt64(int64(cases.expect), 1)

		proc := testutil.NewProc()
		result, err := InternalCharLength([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

// InternalNumericPrecision Implementation of mo internal function 'internal_numeric_precision'
func TestInternalNumericPrecision(t *testing.T) {
	convey.Convey("Test05", t, func() {
		cases := []struct {
			typ    types.Type
			expect int64
		}{
			{
				typ:    types.New(types.T_decimal64, 0, 10),
				expect: 10,
			},
			{
				typ:    types.New(types.T_decimal64, 0, 7),
				expect: 7,
			},
			{
				typ:    types.New(types.T_decimal128, 0, 15),
				expect: 15,
			},
			{
				typ:    types.New(types.T_decimal128, 0, 12),
				expect: 12,
			},
			{
				typ:    types.New(types.T_decimal128, 0, 20),
				expect: 20,
			},
		}

		var srcStrs []string
		var expects []int64
		for _, c := range cases {
			bytes, err := types.Encode(c.typ)
			if err != nil {
				t.Fatal(err)
			}
			srcStrs = append(srcStrs, string(bytes))
			expects = append(expects, c.expect)
		}

		srcVector := testutil.MakeVarcharVector(srcStrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, nil)

		proc := testutil.NewProc()
		result, err := InternalNumericScale([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Test06", t, func() {
		cases := struct {
			typ    types.Type
			expect int
		}{
			typ:    types.New(types.T_decimal128, 0, 12),
			expect: 12,
		}

		bytes, _ := types.Encode(cases.typ)
		srcVector := testutil.MakeScalarVarchar(string(bytes), 1)
		expectVector := testutil.MakeScalarInt64(int64(cases.expect), 1)

		proc := testutil.NewProc()
		result, err := InternalNumericScale([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

// InternalNumericScale Implementation of mo internal function 'internal_numeric_scale'
func TestInternalNumericScale(t *testing.T) {
	convey.Convey("Test07", t, func() {
		cases := []struct {
			typ    types.Type
			expect int64
		}{
			{
				typ:    types.New(types.T_decimal64, 0, 10),
				expect: 10,
			},
			{
				typ:    types.New(types.T_decimal64, 0, 7),
				expect: 7,
			},
			{
				typ:    types.New(types.T_decimal128, 0, 15),
				expect: 15,
			},
			{
				typ:    types.New(types.T_decimal128, 0, 12),
				expect: 12,
			},
			{
				typ:    types.New(types.T_decimal128, 0, 20),
				expect: 20,
			},
		}

		var srcStrs []string
		var expects []int64
		for _, c := range cases {
			bytes, err := types.Encode(c.typ)
			if err != nil {
				t.Fatal(err)
			}
			srcStrs = append(srcStrs, string(bytes))
			expects = append(expects, c.expect)
		}

		srcVector := testutil.MakeVarcharVector(srcStrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, nil)

		proc := testutil.NewProc()
		result, err := InternalNumericScale([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Test08", t, func() {
		cases := struct {
			typ    types.Type
			expect int
		}{
			typ:    types.New(types.T_decimal128, 0, 5),
			expect: 5,
		}

		bytes, _ := types.Encode(cases.typ)
		srcVector := testutil.MakeScalarVarchar(string(bytes), 1)
		expectVector := testutil.MakeScalarInt64(int64(cases.expect), 1)

		proc := testutil.NewProc()
		result, err := InternalNumericScale([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

// InternalDatetimePrecision Implementation of mo internal function 'internal_datetime_precision'
func TestInternalDatetimePrecision(t *testing.T) {
	convey.Convey("Test09", t, func() {
		cases := []struct {
			typ    types.Type
			expect int64
		}{
			{
				typ:    types.New(types.T_datetime, 0, 3),
				expect: 3,
			},
			{
				typ:    types.New(types.T_datetime, 0, 2),
				expect: 2,
			},
			{
				typ:    types.New(types.T_datetime, 0, 5),
				expect: 5,
			},
			{
				typ:    types.New(types.T_datetime, 0, 4),
				expect: 4,
			},
			{
				typ:    types.New(types.T_datetime, 0, 6),
				expect: 6,
			},
		}

		var srcStrs []string
		var expects []int64
		for _, c := range cases {
			bytes, err := types.Encode(c.typ)
			if err != nil {
				t.Fatal(err)
			}
			srcStrs = append(srcStrs, string(bytes))
			expects = append(expects, c.expect)
		}

		srcVector := testutil.MakeVarcharVector(srcStrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, nil)

		proc := testutil.NewProc()
		result, err := InternalDatetimeScale([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})

	convey.Convey("Test10", t, func() {
		cases := struct {
			typ    types.Type
			expect int64
		}{
			typ:    types.New(types.T_datetime, 0, 3),
			expect: 3,
		}

		bytes, _ := types.Encode(cases.typ)

		srcVector := testutil.MakeScalarVarchar(string(bytes), 1)
		expectVector := testutil.MakeScalarInt64(cases.expect, 1)

		proc := testutil.NewProc()
		result, err := InternalDatetimeScale([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}

// InternalColumnCharacterSet  Implementation of mo internal function 'internal_column_character_set'
func TestInternalColumnCharacterSet(t *testing.T) {
	convey.Convey("Test11", t, func() {
		cases := []struct {
			typ    types.Type
			expect int64
		}{
			{
				typ:    types.New(types.T_datetime, 0, 3),
				expect: 0,
			},
			{
				typ:    types.New(types.T_varchar, 50, 0),
				expect: 0,
			},
			{
				typ:    types.New(types.T_char, 20, 0),
				expect: 0,
			},
			{
				typ:    types.New(types.T_blob, 0, 0),
				expect: 1,
			},
			{
				typ:    types.New(types.T_text, 0, 0),
				expect: 0,
			},
		}

		var srcStrs []string
		var expects []int64
		for _, c := range cases {
			bytes, err := types.Encode(c.typ)
			if err != nil {
				t.Fatal(err)
			}
			srcStrs = append(srcStrs, string(bytes))
			expects = append(expects, c.expect)
		}

		srcVector := testutil.MakeVarcharVector(srcStrs, nil)
		expectVector := testutil.MakeInt64Vector(expects, []uint64{0})

		proc := testutil.NewProc()
		result, err := InternalColumnCharacterSet([]*vector.Vector{srcVector}, proc)
		if err != nil {
			t.Fatal(err)
		}
		convey.So(err, convey.ShouldBeNil)
		compare := testutil.CompareVectors(expectVector, result)
		convey.So(compare, convey.ShouldBeTrue)
	})
}
