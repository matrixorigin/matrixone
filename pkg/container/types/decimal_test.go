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

package types

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestBasic(t *testing.T) {
	require.True(t, Decimal64_Zero.Lt(Decimal64_One))
	require.True(t, Decimal64_Zero.Lt(Decimal64_Ten))
	require.True(t, Decimal64_Zero.Lt(Decimal64Max))
	require.True(t, Decimal64_Zero.Gt(Decimal64Min))
	require.True(t, Decimal128_Zero.Lt(Decimal128_One))
	require.True(t, Decimal128_Zero.Lt(Decimal128_Ten))
	require.True(t, Decimal128_Zero.Lt(Decimal128Max))
	require.True(t, Decimal128_Zero.Gt(Decimal128Min))

	require.True(t, Decimal64_Ten.Eq(Decimal64FromInt32(10)))
	require.True(t, Decimal64_Ten.Eq(MustDecimal64FromString("10")))
	require.True(t, Decimal64_Ten.Eq(MustDecimal64FromString("10.000")))
	require.True(t, Decimal128_Ten.Eq(Decimal128FromInt32(10)))
	require.True(t, Decimal128_Ten.Eq(MustDecimal128FromString("10")))
	require.True(t, Decimal128_Ten.Eq(MustDecimal128FromString("10.000")))
}

func TestParse(t *testing.T) {
	d64, err := Decimal64_FromString("1.23456789")
	require.True(t, err == nil)
	require.True(t, d64.Gt(Decimal64_One))
	require.True(t, d64.Lt(Decimal64_Ten))

	d128, err := Decimal128_FromString("1.23456789")
	require.True(t, err == nil)
	dd, err := d128.ToDecimal64(64, 10)
	require.True(t, d64.Eq(dd))
	require.True(t, err == nil)

	longstr := "1.23456789999999999999999"

	d64, err = Decimal64_FromString(longstr)
	require.True(t, err != nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDataTruncated))
	require.True(t, d64.Gt(Decimal64_One))
	require.True(t, d64.Lt(Decimal64_Ten))

	d128, err = Decimal128_FromString(longstr)
	require.True(t, err == nil)
	dd, err = d128.ToDecimal64(33, 15)
	require.True(t, d64.Eq(dd))
	require.True(t, err == nil)
}

func TestNeg(t *testing.T) {
	s := "123456.789"
	d128, err := ParseStringToDecimal128(s, 20, 5, false)

	require.True(t, err == nil)
	require.Equal(t, d128.ToString(), s)

	nd := NegDecimal128(d128)
	require.Equal(t, nd.ToString(), "-"+s)

	z := d128.Add(nd)
	require.True(t, z.Eq(Decimal128_Zero))
}

func TestAdd(t *testing.T) {
	require.True(t, Decimal64_One.Eq(Decimal64_Zero.Add(Decimal64_One)))
	require.True(t, Decimal64_Ten.Eq(Decimal64_Zero.Add(Decimal64_Ten)))
	require.True(t, Decimal64Max.Eq(Decimal64_Zero.Add(Decimal64Max)))
	require.True(t, Decimal128_One.Eq(Decimal128_Zero.Add(Decimal128_One)))
	require.True(t, Decimal128_Ten.Eq(Decimal128_Zero.Add(Decimal128_Ten)))
	require.True(t, Decimal128Max.Eq(Decimal128_Zero.Add(Decimal128Max)))
}

func TestBits(t *testing.T) {
	d1, err := Decimal64_FromStringWithScale("9.2234", 5, 4)
	require.True(t, err == nil)
	d2, err := Decimal64_FromStringWithScale("9.22337777675788773437747747747347377", 5, 4)
	require.True(t, err == nil)

	require.Equal(t, d1.ToInt64(), d2.ToInt64())
	require.Equal(t, Decimal64ToInt64Raw(d1), Decimal64ToInt64Raw(d2))
}

func Test_ParseStringToDecimal64(t *testing.T) {
	convey.Convey("ParseStringToDecimal64 succ", t, func() {
		M := 15
		for i := 1; i <= M; i++ {
			for j := 0; j <= i; j++ {
				str := strings.Repeat("9", i-j) + "." + strings.Repeat("9", j)
				_, err := ParseStringToDecimal64(str+"4", int32(i), int32(j), false)
				convey.So(err, convey.ShouldBeNil)

				_, err = ParseStringToDecimal64(str+"5", int32(i), int32(j), false)
				convey.So(err, convey.ShouldNotBeNil)
			}
		}
	})
}

func Test_ParseStringToDecimal128(t *testing.T) {
	convey.Convey("ParseStringToDecimal64 succ", t, func() {
		M := 33
		for i := 16; i <= M; i++ {
			for j := 0; j <= i && j <= 33; j++ {
				str := strings.Repeat("9", i-j) + "." + strings.Repeat("9", j)
				_, err := ParseStringToDecimal128(str+"4", int32(i), int32(j), false)
				convey.So(err, convey.ShouldBeNil)

				_, err = ParseStringToDecimal128(str+"5", int32(i), int32(j), false)
				convey.So(err, convey.ShouldNotBeNil)
			}
		}
	})
}
