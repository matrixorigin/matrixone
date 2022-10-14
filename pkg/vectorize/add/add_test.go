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

package add

import (
	"github.com/smartystreets/goconvey/convey"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestI32Of(t *testing.T) {
	as := make([]int32, 2)
	bs := make([]int32, 2)
	for i := 0; i < 2; i++ {
		as[i] = math.MaxInt32
		bs[i] = int32(i)
	}
	cs := make([]int32, 2)
	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeInt32Vector(cs, nil)

	err := NumericAddSigned[int32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestU32Of(t *testing.T) {
	as := make([]uint32, 2)
	bs := make([]uint32, 2)
	for i := 0; i < 2; i++ {
		as[i] = math.MaxUint32
		bs[i] = uint32(i)
	}
	cs := make([]uint32, 2)
	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	err := NumericAddUnsigned[uint32](av, bv, cv)
	if err == nil {
		t.Fatalf("should have overflowed.")
	}
}

func TestDec64(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i)
		bs[i] = int64(3 * i)
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeDecimal64Vector(cs, nil, types.T_decimal64.ToType())

	err := Decimal64VecAdd(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 add failed")
	}

	res := vector.MustTCols[types.Decimal64](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal64_FromInt64(as[i]+bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal64 add wrong result")
		}
	}
}

func TestDec128(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i)
		bs[i] = int64(3 * i)
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	err := Decimal128VecAdd(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 add failed")
	}

	res := vector.MustTCols[types.Decimal128](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal128_FromInt64(as[i]+bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal128 add wrong result")
		}
	}
}

func TestDec64OfOppNumber(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(-i)
		bs[i] = int64(i)
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeDecimal64Vector(cs, nil, types.T_decimal64.ToType())

	err := Decimal64VecAdd(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal64 add failed")
	}

	res := vector.MustTCols[types.Decimal64](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal64_FromInt64(as[i]+bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal64 add wrong result")
		}
	}
}

func TestDec128OfOppNumber(t *testing.T) {
	as := make([]int64, 10)
	bs := make([]int64, 10)
	cs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		as[i] = int64(i)
		bs[i] = int64(-i)
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	err := Decimal128VecAdd(av, bv, cv)
	if err != nil {
		t.Fatalf("decimal128 add failed")
	}

	res := vector.MustTCols[types.Decimal128](cv)
	for i := 0; i < 10; i++ {
		d, _ := types.Decimal128_FromInt64(as[i]+bs[i], 64, 0)
		if !res[i].Eq(d) {
			t.Fatalf("decimal128 add wrong result")
		}
	}
}

func BenchmarkAddI32(b *testing.B) {
	as := make([]int32, 8192)
	bs := make([]int32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int32(i)
		bs[i] = 1
	}

	cs := make([]int32, 8192)

	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeInt32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := goNumericAddSigned[int32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddI32_C(b *testing.B) {
	as := make([]int32, 8192)
	bs := make([]int32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int32(i)
		bs[i] = 1
	}

	cs := make([]int32, 8192)

	av := testutil.MakeInt32Vector(as, nil)
	bv := testutil.MakeInt32Vector(bs, nil)
	cv := testutil.MakeInt32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := cNumericAddSigned[int32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddUI32(b *testing.B) {
	as := make([]uint32, 8192)
	bs := make([]uint32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = uint32(i)
		bs[i] = 1
	}

	cs := make([]uint32, 8192)

	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := goNumericAddUnsigned[uint32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddUI32_C(b *testing.B) {
	as := make([]uint32, 8192)
	bs := make([]uint32, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = uint32(i)
		bs[i] = 1
	}

	cs := make([]uint32, 8192)

	av := testutil.MakeUint32Vector(as, nil)
	bv := testutil.MakeUint32Vector(bs, nil)
	cv := testutil.MakeUint32Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := cNumericAddUnsigned[uint32](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddF64(b *testing.B) {
	as := make([]float64, 8192)
	bs := make([]float64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = float64(i)
		bs[i] = 1
	}

	cs := make([]float64, 8192)

	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := goNumericAddFloat[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddF64_C(b *testing.B) {
	as := make([]float64, 8192)
	bs := make([]float64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = float64(i)
		bs[i] = 1
	}

	cs := make([]float64, 8192)

	av := testutil.MakeFloat64Vector(as, nil)
	bv := testutil.MakeFloat64Vector(bs, nil)
	cv := testutil.MakeFloat64Vector(cs, nil)

	for i := 0; i < b.N; i++ {
		if err := cNumericAddFloat[float64](av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddDec64(b *testing.B) {
	as := make([]int64, 8192)
	bs := make([]int64, 8192)
	cs := make([]int64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int64(i)
		bs[i] = 1
	}

	av := testutil.MakeDecimal64Vector(as, nil, types.T_decimal64.ToType())
	bv := testutil.MakeDecimal64Vector(bs, nil, types.T_decimal64.ToType())
	cv := testutil.MakeDecimal64Vector(cs, nil, types.T_decimal64.ToType())

	for i := 0; i < b.N; i++ {
		if err := Decimal64VecAdd(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func BenchmarkAddDec128(b *testing.B) {
	as := make([]int64, 8192)
	bs := make([]int64, 8192)
	cs := make([]int64, 8192)
	for i := 0; i < 8192; i++ {
		as[i] = int64(i)
		bs[i] = 1
	}

	av := testutil.MakeDecimal128Vector(as, nil, types.T_decimal128.ToType())
	bv := testutil.MakeDecimal128Vector(bs, nil, types.T_decimal128.ToType())
	cv := testutil.MakeDecimal128Vector(cs, nil, types.T_decimal128.ToType())

	for i := 0; i < b.N; i++ {
		if err := Decimal128VecAdd(av, bv, cv); err != nil {
			b.Fail()
		}
	}
}

func TestStringAddString(t *testing.T) {
	convey.Convey("test01", t, func() {
		type kase struct {
			left  string
			right string
			want  float64
		}

		kases := []kase{
			{
				left:  "7",
				right: "1",
				want:  8,
			},
			{
				left:  "7",
				right: "0",
				want:  7,
			},
			{
				left:  "7",
				right: "12",
				want:  19,
			},
			{
				left:  "7",
				right: "10",
				want:  17,
			},
			{
				left:  "7",
				right: "8.5",
				want:  15.5,
			},
			{
				left:  "7",
				right: "1.5",
				want:  8.5,
			},
			{
				left:  "7",
				right: "12.5",
				want:  19.5,
			},
			{
				left:  "7",
				right: "a",
				want:  7,
			},
			{
				left:  "7",
				right: "a",
				want:  7,
			},
			{
				left:  "7",
				right: "xc",
				want:  7,
			},
			{
				left:  "7",
				right: "xc",
				want:  7,
			},
		}

		var leftStrs []string
		var rightStrs []string
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeVarcharVector(leftStrs, nil)
		rv := testutil.MakeVarcharVector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddString(lv, rv, retVec)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test02", t, func() {
		type kase struct {
			left  string
			right string
			want  float64
		}

		kases := []kase{
			{
				left:  "7",
				right: "1",
				want:  8,
			},
			{
				left:  "7",
				right: "0",
				want:  7,
			},
			{
				left:  "7",
				right: "12",
				want:  19,
			},
			{
				left:  "7",
				right: "10",
				want:  17,
			},
			{
				left:  "7",
				right: "8.5",
				want:  15.5,
			},
			{
				left:  "7",
				right: "1.5",
				want:  8.5,
			},
			{
				left:  "7",
				right: "12.5",
				want:  19.5,
			},
			{
				left:  "7",
				right: "a",
				want:  7,
			},
			{
				left:  "7",
				right: "a",
				want:  7,
			},
			{
				left:  "7",
				right: "xc",
				want:  7,
			},
			{
				left:  "7",
				right: "xc",
				want:  7,
			},
		}

		var leftStrs []string
		var rightStrs []string
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeScalarVarchar("7", 10)
		rv := testutil.MakeVarcharVector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddString(lv, rv, retVec)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test03", t, func() {
		type kase struct {
			left string
			want float64
		}

		kases := []kase{
			{
				left: "1",
				want: 8,
			},
			{
				left: "0",
				want: 7,
			},
			{
				left: "12",
				want: 19,
			},
			{
				left: "10",
				want: 17,
			},
			{
				left: "8.5",
				want: 15.5,
			},
			{
				left: "1.5",
				want: 8.5,
			},
			{
				left: "12.5",
				want: 19.5,
			},
			{
				left: "a",
				want: 7,
			},
			{
				left: "a",
				want: 7,
			},
			{
				left: "xc",
				want: 7,
			},
			{
				left: "xc",
				want: 7,
			},
		}

		var leftStrs []string
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeVarcharVector(leftStrs, nil)
		rv := testutil.MakeScalarVarchar("7", 10)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddString(lv, rv, retVec)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestStringAddSigned(t *testing.T) {
	convey.Convey("test01", t, func() {
		type kase struct {
			left  string
			right int64
			want  float64
		}

		kases := []kase{
			{

				left:  "1",
				right: 7,
				want:  8,
			},
			{

				left:  "0",
				right: 7,
				want:  7,
			},
			{
				left:  "12",
				right: 7,
				want:  19,
			},
			{
				left:  "10",
				right: 7,
				want:  17,
			},
			{

				left:  "8.5",
				right: 7,
				want:  15.5,
			},
			{

				left:  "1.5",
				right: 7,
				want:  8.5,
			},
			{

				left:  "12.5",
				right: 7,
				want:  19.5,
			},
			{

				left:  "a",
				right: 7,
				want:  7,
			},
			{
				left:  "a",
				right: 7,
				want:  7,
			},
			{
				left:  "xc",
				right: 7,
				want:  7,
			},
			{
				left:  "xc",
				right: 7,
				want:  7,
			},
		}

		var leftStrs []string
		var rightInt []int64
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			rightInt = append(rightInt, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeVarcharVector(leftStrs, nil)
		rv := testutil.MakeInt64Vector(rightInt, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddSigned[int64](lv, rv, retVec)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test02", t, func() {
		type kase struct {
			left string
			want float64
		}

		kases := []kase{
			{

				left: "1",
				want: 8,
			},
			{

				left: "0",
				want: 7,
			},
			{
				left: "12",
				want: 19,
			},
			{
				left: "10",
				want: 17,
			},
			{

				left: "8.5",
				want: 15.5,
			},
			{

				left: "1.5",
				want: 8.5,
			},
			{

				left: "12.5",
				want: 19.5,
			},
			{

				left: "a",
				want: 7,
			},
			{
				left: "a",
				want: 7,
			},
			{
				left: "xc",
				want: 7,
			},
			{
				left: "xc",
				want: 7,
			},
		}

		var leftStrs []string
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeVarcharVector(leftStrs, nil)
		rv := testutil.MakeScalarInt64(7, 10)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddSigned[int64](lv, rv, retVec)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test03", t, func() {
		type kase struct {
			right int64
			want  float64
		}

		kases := []kase{
			{
				right: 1,
				want:  8,
			},
			{
				right: 0,
				want:  7,
			},
			{
				right: 12,
				want:  19,
			},
			{
				right: 10,
				want:  17,
			},
		}

		var rightStrs []int64
		var wants []float64
		for _, k := range kases {
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeScalarVarchar("7", 5)
		rv := testutil.MakeInt64Vector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(rightStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddSigned[int64](lv, rv, retVec)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestStringAddUnSigned(t *testing.T) {
	convey.Convey("test01", t, func() {
		type kase struct {
			left  string
			right uint64
			want  float64
		}

		kases := []kase{
			{

				left:  "1",
				right: 7,
				want:  8,
			},
			{

				left:  "0",
				right: 7,
				want:  7,
			},
			{
				left:  "12",
				right: 7,
				want:  19,
			},
			{
				left:  "10",
				right: 7,
				want:  17,
			},
			{

				left:  "8.5",
				right: 7,
				want:  15.5,
			},
			{

				left:  "1.5",
				right: 7,
				want:  8.5,
			},
			{

				left:  "12.5",
				right: 7,
				want:  19.5,
			},
			{

				left:  "a",
				right: 7,
				want:  7,
			},
			{
				left:  "a",
				right: 7,
				want:  7,
			},
			{
				left:  "xc",
				right: 7,
				want:  7,
			},
			{
				left:  "xc",
				right: 7,
				want:  7,
			},
		}

		var leftStrs []string
		var rightInt []uint64
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			rightInt = append(rightInt, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeVarcharVector(leftStrs, nil)
		rv := testutil.MakeUint64Vector(rightInt, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddUnsigned[uint64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test02", t, func() {
		type kase struct {
			left string
			want float64
		}

		kases := []kase{
			{

				left: "1",
				want: 8,
			},
			{

				left: "0",
				want: 7,
			},
			{
				left: "12",
				want: 19,
			},
			{
				left: "10",
				want: 17,
			},
			{

				left: "8.5",
				want: 15.5,
			},
			{

				left: "1.5",
				want: 8.5,
			},
			{

				left: "12.5",
				want: 19.5,
			},
			{

				left: "a",
				want: 7,
			},
			{
				left: "a",
				want: 7,
			},
			{
				left: "xc",
				want: 7,
			},
			{
				left: "xc",
				want: 7,
			},
		}

		var leftStrs []string
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeVarcharVector(leftStrs, nil)
		rv := testutil.MakeScalarUint64(7, 10)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddUnsigned[uint64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test03", t, func() {
		type kase struct {
			right uint64
			want  float64
		}

		kases := []kase{
			{
				right: 1,
				want:  8,
			},
			{
				right: 0,
				want:  7,
			},
			{
				right: 12,
				want:  19,
			},
			{
				right: 10,
				want:  17,
			},
		}

		var rightStrs []uint64
		var wants []float64
		for _, k := range kases {
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeScalarVarchar("7", 5)
		rv := testutil.MakeUint64Vector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(rightStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddUnsigned[uint64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestStringAddFloat(t *testing.T) {
	convey.Convey("test01", t, func() {
		type kase struct {
			left  string
			right float64
			want  float64
		}

		kases := []kase{
			{left: "1", right: 5.3, want: 6.3},
			{left: "0", right: 5.3, want: 5.3},
			{left: "12", right: 5.3, want: 17.3},
			{left: "10", right: 5.3, want: 15.3},
			{left: "8.5", right: 5.3, want: 13.8},
			{left: "1.5", right: 5.3, want: 6.8},
			{left: "12.5", right: 5.3, want: 17.8},
			{left: "a", right: 5.3, want: 5.3},
			{left: "a", right: 5.3, want: 5.3},
			{left: "xc", right: 5.3, want: 5.3},
			{left: "xc", right: 5.3, want: 5.3},
		}

		var leftStrs []string
		var rightFloat []float64
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			rightFloat = append(rightFloat, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeVarcharVector(leftStrs, nil)
		rv := testutil.MakeFloat64Vector(rightFloat, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddFloat[float64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test02", t, func() {
		type kase struct {
			left  string
			right float64
			want  float64
		}

		kases := []kase{
			{left: "1", right: 5.3, want: 6.3},
			{left: "0", right: 5.3, want: 5.3},
			{left: "12", right: 5.3, want: 17.3},
			{left: "10", right: 5.3, want: 15.3},
			{left: "8.5", right: 5.3, want: 13.8},
			{left: "1.5", right: 5.3, want: 6.8},
			{left: "12.5", right: 5.3, want: 17.8},
			{left: "a", right: 5.3, want: 5.3},
			{left: "a", right: 5.3, want: 5.3},
			{left: "xc", right: 5.3, want: 5.3},
			{left: "xc", right: 5.3, want: 5.3},
		}

		var leftStrs []string
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeVarcharVector(leftStrs, nil)
		rv := testutil.MakeScalarFloat64(5.3, 10)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddFloat[float64](lv, rv, retVec)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test02", t, func() {
		type kase struct {
			left  string
			right float64
			want  float64
		}

		kases := []kase{
			{left: "5.3", right: 1, want: 6.3},
			{left: "5.3", right: 0, want: 5.3},
			{left: "5.3", right: 12, want: 17.3},
			{left: "5.3", right: 10, want: 15.3},
			{left: "5.3", right: 8.5, want: 13.8},
			{left: "5.3", right: 1.5, want: 6.8},
			{left: "5.3", right: 12.5, want: 17.8},
		}

		var leftStrs []string
		var rightFloat []float64
		var wants []float64
		for _, k := range kases {
			leftStrs = append(leftStrs, k.left)
			rightFloat = append(rightFloat, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeScalarVarchar("5.3", 10)
		rv := testutil.MakeFloat64Vector(rightFloat, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := StringAddFloat[float64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestSignedAddString(t *testing.T) {
	convey.Convey("test01", t, func() {
		type kase struct {
			left  int64
			right string
			want  float64
		}

		kases := []kase{
			{
				left:  7,
				right: "1",
				want:  8,
			},
			{
				left:  7,
				right: "0",
				want:  7,
			},
			{
				left:  7,
				right: "12",
				want:  19,
			},
			{
				left:  7,
				right: "10",
				want:  17,
			},
			{
				left:  7,
				right: "8.5",
				want:  15.5,
			},
			{
				left:  7,
				right: "1.5",
				want:  8.5,
			},
			{
				left:  7,
				right: "12.5",
				want:  19.5,
			},
			{
				left:  7,
				right: "a",
				want:  7,
			},
			{
				left:  7,
				right: "a",
				want:  7,
			},
			{
				left:  7,
				right: "xc",
				want:  7,
			},
			{
				left:  7,
				right: "xc",
				want:  7,
			},
		}

		var leftInt []int64
		var rightStrs []string
		var wants []float64
		for _, k := range kases {
			leftInt = append(leftInt, k.left)
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeInt64Vector(leftInt, nil)
		rv := testutil.MakeVarcharVector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftInt)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := SignedAddString[int64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test02", t, func() {
		type kase struct {
			right string
			want  float64
		}

		kases := []kase{
			{
				right: "1",
				want:  8,
			},
			{
				right: "0",
				want:  7,
			},
			{
				right: "12",
				want:  19,
			},
			{
				right: "10",
				want:  17,
			},
			{
				right: "8.5",
				want:  15.5,
			},
			{
				right: "1.5",
				want:  8.5,
			},
			{
				right: "12.5",
				want:  19.5,
			},
			{
				right: "a",
				want:  7,
			},
			{
				right: "a",
				want:  7,
			},
			{
				right: "xc",
				want:  7,
			},
			{
				right: "xc",
				want:  7,
			},
		}

		var rightStrs []string
		var wants []float64
		for _, k := range kases {
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeScalarInt64(7, 10)
		rv := testutil.MakeVarcharVector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(rightStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := SignedAddString[int64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test03", t, func() {
		type kase struct {
			left int64
			want float64
		}

		kases := []kase{
			{
				left: 1,
				want: 8,
			},
			{
				left: 0,
				want: 7,
			},
			{
				left: 12,
				want: 19,
			},
			{
				left: 10,
				want: 17,
			},
		}

		var leftInt []int64
		var wants []float64
		for _, k := range kases {
			leftInt = append(leftInt, k.left)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeInt64Vector(leftInt, nil)
		rv := testutil.MakeScalarVarchar("7", 5)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftInt)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := SignedAddString[int64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestUnsignedAddString(t *testing.T) {
	convey.Convey("test01", t, func() {
		type kase struct {
			left  uint64
			right string
			want  float64
		}

		kases := []kase{
			{
				left:  7,
				right: "1",
				want:  8,
			},
			{
				left:  7,
				right: "0",
				want:  7,
			},
			{
				left:  7,
				right: "12",
				want:  19,
			},
			{
				left:  7,
				right: "10",
				want:  17,
			},
			{
				left:  7,
				right: "8.5",
				want:  15.5,
			},
			{
				left:  7,
				right: "1.5",
				want:  8.5,
			},
			{
				left:  7,
				right: "12.5",
				want:  19.5,
			},
			{
				left:  7,
				right: "a",
				want:  7,
			},
			{
				left:  7,
				right: "a",
				want:  7,
			},
			{
				left:  7,
				right: "xc",
				want:  7,
			},
			{
				left:  7,
				right: "xc",
				want:  7,
			},
		}

		var leftInt []uint64
		var rightStrs []string
		var wants []float64
		for _, k := range kases {
			leftInt = append(leftInt, k.left)
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeUint64Vector(leftInt, nil)
		rv := testutil.MakeVarcharVector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftInt)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := UnsignedAddString[uint64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test02", t, func() {
		type kase struct {
			right string
			want  float64
		}

		kases := []kase{
			{
				right: "1",
				want:  8,
			},
			{
				right: "0",
				want:  7,
			},
			{
				right: "12",
				want:  19,
			},
			{
				right: "10",
				want:  17,
			},
			{
				right: "8.5",
				want:  15.5,
			},
			{
				right: "1.5",
				want:  8.5,
			},
			{
				right: "12.5",
				want:  19.5,
			},
			{
				right: "a",
				want:  7,
			},
			{
				right: "a",
				want:  7,
			},
			{
				right: "xc",
				want:  7,
			},
			{
				right: "xc",
				want:  7,
			},
		}

		var rightStrs []string
		var wants []float64
		for _, k := range kases {
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeScalarUint64(7, 10)
		rv := testutil.MakeVarcharVector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(rightStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := UnsignedAddString[uint64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test03", t, func() {
		type kase struct {
			left uint64
			want float64
		}

		kases := []kase{
			{
				left: 1,
				want: 8,
			},
			{
				left: 0,
				want: 7,
			},
			{
				left: 12,
				want: 19,
			},
			{
				left: 10,
				want: 17,
			},
		}

		var leftInt []uint64
		var wants []float64
		for _, k := range kases {
			leftInt = append(leftInt, k.left)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeUint64Vector(leftInt, nil)
		rv := testutil.MakeScalarVarchar("7", 5)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftInt)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := UnsignedAddString[uint64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestFloatAddString(t *testing.T) {
	convey.Convey("test01", t, func() {
		type kase struct {
			left  float64
			right string
			want  float64
		}

		kases := []kase{
			{left: 5.3, right: "1", want: 6.3},
			{left: 5.3, right: "0", want: 5.3},
			{left: 5.3, right: "12", want: 17.3},
			{left: 5.3, right: "10", want: 15.3},
			{left: 5.3, right: "8.5", want: 13.8},
			{left: 5.3, right: "1.5", want: 6.8},
			{left: 5.3, right: "12.5", want: 17.8},
			{left: 5.3, right: "a", want: 5.3},
			{left: 5.3, right: "a", want: 5.3},
			{left: 5.3, right: "xc", want: 5.3},
			{left: 5.3, right: "xc", want: 5.3},
		}

		var leftFloat []float64
		var rightStrs []string
		var wants []float64
		for _, k := range kases {
			leftFloat = append(leftFloat, k.left)
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeFloat64Vector(leftFloat, nil)
		rv := testutil.MakeVarcharVector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftFloat)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := FloatAddString[float64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test02", t, func() {
		type kase struct {
			left  float64
			right string
			want  float64
		}

		kases := []kase{
			{left: 5.3, right: "1", want: 6.3},
			{left: 5.3, right: "0", want: 5.3},
			{left: 5.3, right: "12", want: 17.3},
			{left: 5.3, right: "10", want: 15.3},
			{left: 5.3, right: "8.5", want: 13.8},
			{left: 5.3, right: "1.5", want: 6.8},
			{left: 5.3, right: "12.5", want: 17.8},
			{left: 5.3, right: "a", want: 5.3},
			{left: 5.3, right: "a", want: 5.3},
			{left: 5.3, right: "xc", want: 5.3},
			{left: 5.3, right: "xc", want: 5.3},
		}

		var rightStrs []string
		var wants []float64
		for _, k := range kases {
			rightStrs = append(rightStrs, k.right)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeScalarFloat64(5.3, 10)
		rv := testutil.MakeVarcharVector(rightStrs, nil)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(rightStrs)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := FloatAddString[float64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})

	convey.Convey("test03", t, func() {
		type kase struct {
			left  float64
			right string
			want  float64
		}

		kases := []kase{
			{left: 1, right: "5.3", want: 6.3},
			{left: 0, right: "5.3", want: 5.3},
			{left: 12, right: "5.3", want: 17.3},
			{left: 10, right: "5.3", want: 15.3},
			{left: 8.5, right: "5.3", want: 13.8},
			{left: 1.5, right: "5.3", want: 6.8},
			{left: 12.5, right: "5.3", want: 17.8},
		}

		var leftFloat []float64
		var wants []float64
		for _, k := range kases {
			leftFloat = append(leftFloat, k.left)
			wants = append(wants, k.want)
		}

		lv := testutil.MakeFloat64Vector(leftFloat, nil)
		rv := testutil.MakeScalarVarchar("5.3", 5)
		retVec := testutil.MakeFloat64Vector(make([]float64, len(leftFloat)), nil)
		wantVec := testutil.MakeFloat64Vector(wants, nil)
		err := FloatAddString[float64](lv, rv, retVec)

		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantVec, retVec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}
