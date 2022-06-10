package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/testutil"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestLengthUTF8(t *testing.T) {
	convey.Convey("right case", t, func() {
		type kase struct {
			s    string
			want uint64
		}

		kases := []kase{
			{"abc", 3},
			{"", 0},
			{"   ", 3},
			{"中国123", 5},
			{"abc😄", 4},
			{"中国中国中国中国中国中国中国中国中国中国1234", 24},
			{"中国中国中国中国中国中国中国中国中国中国1234😄ggg!", 29},
			{
				"你好",
				2,
			},
			{
				"français",
				8,
			},
			{
				"にほんご",
				4,
			},
			{
				"Español",
				7,
			},
			{
				"123456",
				6,
			},
			{
				"андрей",
				6,
			},
			{
				"\\",
				1,
			},
			{
				string(rune(0x0c)),
				1,
			},
			{
				string('"'),
				1,
			},
			{
				string('\a'),
				1,
			},
			{
				string('\b'),
				1,
			},
			{
				string('\t'),
				1,
			},
			{
				string('\n'),
				1,
			},
			{
				string('\r'),
				1,
			},
			{
				string(rune(0x10)),
				1,
			},
			{
				"你好",
				2,
			},
			{
				"再见",
				2,
			},
			{
				"今天",
				2,
			},
			{
				"日期时间",
				4,
			},
			{
				"明天",
				2,
			},
			{
				"\n\t\r\b" + string(rune(0)) + "\\_\\%\\",
				10,
			},
		}

		var input []string
		var output []uint64
		for _, k := range kases {
			input = append(input, k.s)
			output = append(output, k.want)
		}

		ivec := testutil.MakeVarcharVector(input, nil)
		wantvec := testutil.MakeUint64Vector(output, nil)
		proc := testutil.NewProc()
		ovec, err := LengthUTF8([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, ovec)
		convey.So(ret, convey.ShouldBeTrue)
	})
	convey.Convey("null", t, func() {
		ivec := testutil.MakeScalarNull(10)
		wantvec := testutil.MakeScalarNull(10)
		proc := testutil.NewProc()
		ovec, err := LengthUTF8([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, ovec)
		convey.So(ret, convey.ShouldBeTrue)

	})
	convey.Convey("tinyint", t, func() {
		ivec := testutil.MakeInt8Vector([]int8{
			1, 71, 1, 1}, nil)
		proc := testutil.NewProc()
		_, err := LengthUTF8([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldNotBeNil)

		ivec2 := testutil.MakeScalarInt8(1, 10)
		_, err = LengthUTF8([]*vector.Vector{ivec2}, proc)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("nil", t, func() {
		proc := testutil.NewProc()
		_, err := LengthUTF8([]*vector.Vector{}, proc)
		convey.So(err, convey.ShouldNotBeNil)

		_, err = LengthUTF8([]*vector.Vector{}, nil)
		convey.So(err, convey.ShouldNotBeNil)

		_, err = LengthUTF8([]*vector.Vector{nil}, proc)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("scalar", t, func() {
		var ss []string
		var ll []uint64
		s := "abc"
		for i := 0; i < 5; i++ {
			ss = append(ss, s)
			ll = append(ll, uint64(len(s)))
		}
		ivec := testutil.MakeVarcharVector(ss, nil)
		wantvec := testutil.MakeUint64Vector(ll, nil)
		ivec.IsConst = true
		ivec.Length = len(ss)

		proc := testutil.NewProc()
		ovec, err := LengthUTF8([]*vector.Vector{ivec}, proc)
		convey.So(err, convey.ShouldBeNil)
		ret := testutil.CompareVectors(wantvec, ovec)
		convey.So(ret, convey.ShouldBeTrue)
	})
}
