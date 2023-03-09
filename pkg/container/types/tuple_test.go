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

package types

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleTupleAllTypes(t *testing.T) {
	tests := []struct {
		args Tuple
	}{
		{
			args: Tuple{true, int8(1), int16(2), int32(3), int64(4), uint8(5), uint16(6), uint32(7), uint64(8), float32(1),
				float64(1),
				DateFromCalendar(2000, 1, 1), DatetimeFromClock(2000, 1, 1, 1, 1, 0, 0),
				FromClockUTC(2000, 2, 2, 2, 2, 0, 0), Decimal64_FromInt32(123),
				Decimal128_FromInt32(123), []byte{1, 2, 3}},
		},
	}
	for _, test := range tests {
		tuple := test.args
		mp := mpool.MustNewZero()
		packer := NewPacker(mp)
		encodeBufToPacker(tuple, packer)
		tt, _ := Unpack(packer.GetBuf())
		require.Equal(t, tuple.String(), tt.String())
		for i := range tuple {
			require.Equal(t, tuple[i], tt[i])
		}
	}
}

func TestSingleTypeTuple(t *testing.T) {
	tests := []struct {
		args Tuple
	}{
		{
			args: Tuple{true, false},
		},
		{
			args: randomTuple(int8Code, 100),
		},
		{
			args: randomTuple(int16Code, 100),
		},
		{
			args: randomTuple(int32Code, 100),
		},
		{
			args: randomTuple(int64Code, 100),
		},
		{
			args: randomTuple(uint8Code, 100),
		},
		{
			randomTuple(uint16Code, 100),
		},
		{
			randomTuple(uint32Code, 100),
		},
		{
			randomTuple(uint64Code, 100),
		},
		{
			randomTuple(dateCode, 100),
		},
		{
			randomTuple(datetimeCode, 100),
		},
		{
			randomTuple(timestampCode, 100),
		},
		{
			randomTuple(decimal64Code, 100),
		},
		{
			randomTuple(decimal128Code, 100),
		},
		{
			randomTuple(stringTypeCode, 100),
		},
		{
			randomTuple(float32Code, 100),
		},
		{
			randomTuple(float64Code, 100),
		},
	}
	for _, test := range tests {
		tuple := test.args
		mp := mpool.MustNewZero()
		packer := NewPacker(mp)
		encodeBufToPacker(tuple, packer)
		tt, _ := Unpack(packer.GetBuf())
		for i := range tuple {
			require.Equal(t, tuple[i], tt[i])
		}
	}
}

func TestMulTypeTuple(t *testing.T) {
	tests := []struct {
		args Tuple
	}{
		{
			args: randomTypeTuple(1),
		},
		{
			args: randomTypeTuple(10),
		},
		{
			args: randomTypeTuple(100),
		},
		{
			args: randomTypeTuple(1000),
		},
	}

	for _, test := range tests {
		tuple := test.args
		mp := mpool.MustNewZero()
		packer := NewPacker(mp)
		encodeBufToPacker(tuple, packer)
		tt, _ := Unpack(packer.GetBuf())
		for i := range tuple {
			require.Equal(t, tuple[i], tt[i])
		}
	}
}

func randomTuple(code int, si int) Tuple {
	var tuple Tuple
	switch code {
	case float32Code:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, rand.Float32())
		}
	case float64Code:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, rand.NormFloat64())
		}
	case trueCode:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, true)
		}
	case falseCode:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, false)
		}
	case int8Code:
		for i := 0; i < si; i++ {
			x := randPositiveInt8()
			tuple = addTupleElement(tuple, x)
			tuple = addTupleElement(tuple, -x)
		}
		tuple = addTupleElement(tuple, int8(math.MaxInt8))
		tuple = addTupleElement(tuple, int8(math.MinInt8))
	case int16Code:
		for i := 0; i < si; i++ {
			x := randPositiveInt16()
			tuple = addTupleElement(tuple, x)
			tuple = addTupleElement(tuple, -x)
		}
		tuple = addTupleElement(tuple, int16(math.MaxInt16))
		tuple = addTupleElement(tuple, int16(math.MinInt16))
	case int32Code:
		for i := 0; i < si; i++ {
			x := randPositiveInt32()
			tuple = addTupleElement(tuple, x)
			tuple = addTupleElement(tuple, -x)
		}
		tuple = addTupleElement(tuple, int32(math.MaxInt32))
		tuple = addTupleElement(tuple, int32(math.MinInt32))
	case int64Code:
		for i := 0; i < si; i++ {
			x := randPositiveInt64()
			tuple = addTupleElement(tuple, x)
			tuple = addTupleElement(tuple, -x)
		}
		tuple = addTupleElement(tuple, int64(math.MaxInt64))
		tuple = addTupleElement(tuple, int64(math.MinInt64))
	case uint8Code:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randUint8())
		}
	case uint16Code:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randUint16())
		}
	case uint32Code:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randUint32())
		}
	case uint64Code:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randUint64())
		}
	case dateCode:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randDate())
		}
	case datetimeCode:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randDatetime())
		}
	case timestampCode:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randTimestamp())
		}
	case decimal64Code:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randDecimal64())
		}
	case decimal128Code:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randDecimal128())
		}
	case stringTypeCode:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randStringType())
		}
	}
	return tuple
}

func randomTypeTuple(si int) Tuple {
	var tuple Tuple
	for i := 0; i < si; i++ {
		randTypeId := rand.Intn(30)
		switch randTypeId {
		case 0:
			tuple = addTupleElement(tuple, true)
		case 1:
			tuple = addTupleElement(tuple, false)
		case 2:
			x := randPositiveInt8()
			tuple = addTupleElement(tuple, x)
		case 3:
			x := randPositiveInt8()
			tuple = addTupleElement(tuple, -x)
		case 4:
			tuple = addTupleElement(tuple, int8(math.MaxInt8))
		case 5:
			tuple = addTupleElement(tuple, int8(math.MinInt8))
		case 6:
			x := randPositiveInt16()
			tuple = addTupleElement(tuple, x)
		case 7:
			x := randPositiveInt16()
			tuple = addTupleElement(tuple, -x)
		case 8:
			tuple = addTupleElement(tuple, int16(math.MaxInt16))
		case 9:
			tuple = addTupleElement(tuple, int16(math.MinInt16))
		case 10:
			x := randPositiveInt32()
			tuple = addTupleElement(tuple, x)
		case 11:
			x := randPositiveInt32()
			tuple = addTupleElement(tuple, -x)
		case 12:
			tuple = addTupleElement(tuple, int32(math.MaxInt32))
		case 13:
			tuple = addTupleElement(tuple, int32(math.MinInt32))
		case 14:
			x := randPositiveInt64()
			tuple = addTupleElement(tuple, x)
		case 15:
			x := randPositiveInt64()
			tuple = addTupleElement(tuple, -x)
		case 16:
			tuple = addTupleElement(tuple, int64(math.MaxInt64))
		case 17:
			tuple = addTupleElement(tuple, int64(math.MinInt64))
		case 18:
			tuple = addTupleElement(tuple, randUint8())
		case 19:
			tuple = addTupleElement(tuple, randUint16())
		case 20:
			tuple = addTupleElement(tuple, randUint32())
		case 21:
			tuple = addTupleElement(tuple, randUint64())
		case 22:
			tuple = addTupleElement(tuple, randDate())
		case 23:
			tuple = addTupleElement(tuple, randDatetime())
		case 24:
			tuple = addTupleElement(tuple, randTimestamp())
		case 25:
			tuple = addTupleElement(tuple, randDecimal64())
		case 26:
			tuple = addTupleElement(tuple, randDecimal128())
		case 27:
			tuple = addTupleElement(tuple, randStringType())
		case 28:
			tuple = addTupleElement(tuple, rand.Float32())
		case 29:
			tuple = addTupleElement(tuple, rand.Float64())
		}
	}
	return tuple
}
func addTupleElement(tuple Tuple, element interface{}) Tuple {
	return append(tuple, element)
}

func randPositiveInt8() int8 {
	return int8(rand.Int31n(math.MaxInt8 + 1))
}

func randPositiveInt16() int16 {
	return int16(rand.Int31n(math.MaxInt16 + 1))
}

func randPositiveInt32() int32 {
	return rand.Int31()
}

func randPositiveInt64() int64 {
	return rand.Int63()
}

func randUint8() uint8 {
	return uint8(rand.Int31n(math.MaxUint8 + 1))
}

func randUint16() uint16 {
	return uint16(rand.Int31n(math.MaxUint16 + 1))
}

func randUint32() uint32 {
	return rand.Uint32()
}

func randUint64() uint64 {
	return rand.Uint64()
}

func randDate() Date {
	year := rand.Intn(MaxDateYear) + MinDateYear
	month := rand.Intn(12) + 1
	day := rand.Intn(int(LastDay(int32(year), uint8(month)))) + 1
	return DateFromCalendar(int32(year), uint8(month), uint8(day))
}

func randDatetime() Datetime {
	year := rand.Intn(MaxDatetimeYear) + MinDatetimeYear
	month := rand.Intn(12) + 1
	day := rand.Intn(int(LastDay(int32(year), uint8(month)))) + 1
	hour := rand.Intn(24)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	microSecond := rand.Intn(1e6)
	return DatetimeFromClock(int32(year), uint8(month), uint8(day), uint8(hour), uint8(minute), uint8(second), uint32(microSecond))
}

func randTimestamp() Timestamp {
	year := rand.Intn(MaxDatetimeYear) + MinDatetimeYear
	month := rand.Intn(12) + 1
	day := rand.Intn(int(LastDay(int32(year), uint8(month)))) + 1
	hour := rand.Intn(24)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	microSecond := rand.Intn(1e6)
	return FromClockUTC(int32(year), uint8(month), uint8(day), uint8(hour), uint8(minute), uint8(second), uint32(microSecond))
}

func randDecimal64() Decimal64 {
	decimal, _ := Decimal64FromFloat64(rand.Float64(), 20, 10)
	return decimal
}

func randDecimal128() Decimal128 {
	decimal, _ := Decimal128FromFloat64(rand.Float64(), 20, 10)
	return decimal
}

func randStringType() []byte {
	b := make([]byte, 1024)
	rand.Read(b)
	return b
}

func encodeBufToPacker(tuple Tuple, p *Packer) {
	for _, e := range tuple {
		switch e := e.(type) {
		case bool:
			p.EncodeBool(e)
		case int8:
			p.EncodeInt8(e)
		case int16:
			p.EncodeInt16(e)
		case int32:
			p.EncodeInt32(e)
		case int64:
			p.EncodeInt64(e)
		case uint8:
			p.EncodeUint8(e)
		case uint16:
			p.EncodeUint16(e)
		case uint32:
			p.EncodeUint32(e)
		case uint64:
			p.EncodeUint64(e)
		case float32:
			p.EncodeFloat32(e)
		case float64:
			p.encodeFloat64(e)
		case Date:
			p.EncodeDate(e)
		case Datetime:
			p.EncodeDatetime(e)
		case Timestamp:
			p.EncodeTimestamp(e)
		case Decimal64:
			p.EncodeDecimal64(e)
		case Decimal128:
			p.EncodeDecimal128(e)
		case []byte:
			p.EncodeStringType(e)
		}
	}
}
