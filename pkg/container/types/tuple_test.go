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
	"bytes"
	crand "crypto/rand"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimpleTupleAllTypes(t *testing.T) {
	uuid, _ := BuildUuid()
	tests := []struct {
		args Tuple
	}{
		{
			args: Tuple{
				true, int8(1), int16(2), int32(3), int64(4), uint8(5), uint16(6), uint32(7), uint64(8), float32(1),
				float64(1),
				DateFromCalendar(2000, 1, 1), DatetimeFromClock(2000, 1, 1, 1, 1, 0, 0),
				FromClockUTC(2000, 2, 2, 2, 2, 0, 0), Decimal64(123),
				Decimal128{123, 0},
				[]byte{1, 2, 3},
				uuid,
			},
		},
		{
			args: Tuple{
				uuid,
				float64(1),
				uuid,
				true, int8(1), int16(2), int32(3), int64(4), uint8(5), uint16(6), uint32(7), uint64(8), float32(1),
				uuid,
				FromClockUTC(2000, 2, 2, 2, 2, 0, 0), Decimal64(123),
				uuid,
				DateFromCalendar(2000, 1, 1), DatetimeFromClock(2000, 1, 1, 1, 1, 0, 0),
				uuid,
				Decimal128{123, 0},
				uuid,
				[]byte{1, 2, 3},
			},
		},
	}
	for _, test := range tests {
		tuple := test.args
		packer := NewPacker()
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
			Tuple{true, false},
		},
		{
			randomTuple(int8Code, 100),
		},
		{
			randomTuple(int16Code, 100),
		},
		{
			randomTuple(int32Code, 100),
		},
		{
			randomTuple(int64Code, 100),
		},
		{
			randomTuple(uint8Code, 100),
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
		{
			randomTuple(uuidCode, 100),
		},
	}
	for _, test := range tests {
		tuple := test.args
		packer := NewPacker()
		defer packer.Close()
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
		packer := NewPacker()
		defer packer.Close()
		encodeBufToPacker(tuple, packer)
		tt, _ := Unpack(packer.GetBuf())
		for i := range tuple {
			require.Equal(t, tuple[i], tt[i])
		}
	}
}

func TestDecimalOrderTuple(t *testing.T) {
	tests := []struct {
		args Tuple
	}{
		{
			args: Tuple{
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
				Decimal128{uint64(rand.Int()), uint64(rand.Int())},
			},
		},
	}
	for _, test := range tests {
		tuple := test.args
		packer := NewPacker()
		encodeBufToPacker(tuple, packer)
		for i := 1; i < 10; i++ {
			for j := i; j > 0; j-- {
				left := make([]byte, 17)
				right := make([]byte, 17)
				copy(left, packer.buffer[(j-1)*17:j*17])
				copy(right, packer.buffer[j*17:(j+1)*17])
				if bytes.Compare(left, right) > 0 {
					copy(packer.buffer[(j-1)*17:j*17], right)
					copy(packer.buffer[j*17:(j+1)*17], left)
				}
			}
		}
		tt, _ := Unpack(packer.GetBuf())
		sort.Slice(tuple, func(i, j int) bool {
			switch left := tuple[i].(type) {
			case Decimal128:
				switch right := tuple[j].(type) {
				case Decimal128:
					return left.Less(right)
				}
			}
			return false
		})
		require.Equal(t, tuple.String(), tt.String())
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
	case uuidCode:
		for i := 0; i < si; i++ {
			tuple = addTupleElement(tuple, randUuid())
		}

	}
	return tuple
}

func randomTypeTuple(si int) Tuple {
	var tuple Tuple
	for i := 0; i < si; i++ {
		randTypeId := rand.Intn(31)
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
		case 30:
			tuple = addTupleElement(tuple, randUuid())
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
	decimal := Decimal64(rand.Int() % 10000000000)
	return decimal
}

func randDecimal128() Decimal128 {
	decimal := Decimal128{uint64(rand.Int() % 10000000000), 0}
	return decimal
}

func randStringType() []byte {
	b := make([]byte, 1024)
	crand.Read(b)
	return b
}

func randUuid() []byte {
	b := make([]byte, 16)
	crand.Read(b)
	return b
}

func encodeBufToPacker(tuple Tuple, p *Packer) {
	for _, e := range tuple {
		if e == nil {
			p.EncodeNull()
			continue
		}
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
		case Uuid:
			p.EncodeUuid(e)
		}
	}
}

func TestTupleSQLStrings(t *testing.T) {
	uuid, _ := BuildUuid()
	date := DateFromCalendar(2024, 11, 6)
	time := TimeFromClock(false, 14, 30, 45, 123456)
	datetime := DatetimeFromClock(2024, 11, 6, 14, 30, 45, 123456)
	timestamp := FromClockUTC(2024, 11, 6, 14, 30, 45, 123456)

	tests := []struct {
		name     string
		tuple    Tuple
		scales   []int32
		expected []string
	}{
		{
			name:     "empty tuple",
			tuple:    Tuple{},
			scales:   []int32{},
			expected: []string{},
		},
		{
			name:     "integers",
			tuple:    Tuple{int8(10), int16(20), int32(30), int64(40)},
			scales:   []int32{0, 0, 0, 0},
			expected: []string{"10", "20", "30", "40"},
		},
		{
			name:     "unsigned integers",
			tuple:    Tuple{uint8(10), uint16(20), uint32(30), uint64(40)},
			scales:   []int32{0, 0, 0, 0},
			expected: []string{"10", "20", "30", "40"},
		},
		{
			name:     "floats",
			tuple:    Tuple{float32(1.5), float64(2.5)},
			scales:   []int32{0, 0},
			expected: []string{"1.5", "2.5"},
		},
		{
			name:     "boolean",
			tuple:    Tuple{true, false},
			scales:   []int32{0, 0},
			expected: []string{"true", "false"},
		},
		{
			name:     "string",
			tuple:    Tuple{[]byte("hello"), []byte("world")},
			scales:   []int32{0, 0},
			expected: []string{"hello", "world"},
		},
		{
			name:     "time type",
			tuple:    Tuple{time},
			scales:   []int32{6},
			expected: []string{"14:30:45"},
		},
		{
			name:     "date types",
			tuple:    Tuple{date, datetime, timestamp},
			scales:   []int32{0, 6, 6},
			expected: []string{"2024-11-06", "2024-11-06 14:30:45", "2024-11-06 14:30:45.123456 UTC"},
		},
		{
			name:     "decimal64 with scale 0",
			tuple:    Tuple{Decimal64(1250)},
			scales:   []int32{0},
			expected: []string{"1250"},
		},
		{
			name:     "decimal64 with scale 2",
			tuple:    Tuple{Decimal64(1250)},
			scales:   []int32{2},
			expected: []string{"12.50"},
		},
		{
			name:     "decimal128 with scale 0",
			tuple:    Tuple{Decimal128{830, 0}},
			scales:   []int32{0},
			expected: []string{"830"},
		},
		{
			name:     "decimal128 with scale 2",
			tuple:    Tuple{Decimal128{830, 0}},
			scales:   []int32{2},
			expected: []string{"8.30"},
		},
		{
			name:     "uuid",
			tuple:    Tuple{uuid},
			scales:   []int32{0},
			expected: []string{uuid.String()},
		},
		{
			name:     "mixed types",
			tuple:    Tuple{int32(100), []byte("test"), Decimal64(1250)},
			scales:   []int32{0, 0, 2},
			expected: []string{"100", "test", "12.50"},
		},
		{
			name:     "decimal64 with insufficient scales array",
			tuple:    Tuple{Decimal64(1250), Decimal64(830)},
			scales:   []int32{2}, // Only one scale provided, second decimal uses default
			expected: []string{"12.50", "830"},
		},
		{
			name:     "decimal128 with insufficient scales array",
			tuple:    Tuple{Decimal128{1250, 0}, Decimal128{830, 0}},
			scales:   []int32{2}, // Only one scale provided, second decimal uses default
			expected: []string{"12.50", "830"},
		},
		{
			name:     "decimal with nil scales",
			tuple:    Tuple{Decimal64(1250), Decimal128{830, 0}},
			scales:   nil,
			expected: []string{"1250", "830"},
		},
		{
			name:     "decimal with empty scales",
			tuple:    Tuple{Decimal64(1250)},
			scales:   []int32{},
			expected: []string{"1250"},
		},
		{
			name:     "negative decimal64",
			tuple:    Tuple{Decimal64(1250).Minus()},
			scales:   []int32{2},
			expected: []string{"-12.50"},
		},
		{
			name:     "negative decimal128",
			tuple:    Tuple{Decimal128{830, 0}.Minus()},
			scales:   []int32{2},
			expected: []string{"-8.30"},
		},
		{
			name:     "negative integers",
			tuple:    Tuple{int8(-10), int16(-20), int32(-30), int64(-40)},
			scales:   []int32{0, 0, 0, 0},
			expected: []string{"-10", "-20", "-30", "-40"},
		},
		{
			name:     "negative floats",
			tuple:    Tuple{float32(-1.5), float64(-2.5)},
			scales:   []int32{0, 0},
			expected: []string{"-1.5", "-2.5"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.tuple.SQLStrings(tt.scales)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTupleSQLStringsWithEncodeDecode(t *testing.T) {
	// Test SQLStrings after encode/decode cycle
	tests := []struct {
		name     string
		tuple    Tuple
		scales   []int32
		expected []string
	}{
		{
			name:     "composite index varchar+int",
			tuple:    Tuple{[]byte("Apple"), int32(100)},
			scales:   []int32{0, 0},
			expected: []string{"Apple", "100"},
		},
		{
			name:     "composite index decimal+int",
			tuple:    Tuple{Decimal64(1250), int32(100)},
			scales:   []int32{2, 0},
			expected: []string{"12.50", "100"},
		},
		{
			name:     "composite index varchar+decimal",
			tuple:    Tuple{[]byte("Fruit"), Decimal64(10)},
			scales:   []int32{0, 2},
			expected: []string{"Fruit", "0.10"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			packer := NewPacker()
			encodeBufToPacker(tt.tuple, packer)

			// Decode
			decoded, err := Unpack(packer.GetBuf())
			require.NoError(t, err)

			// Test SQLStrings
			result := decoded.SQLStrings(tt.scales)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDecodeTupleErrorHandling(t *testing.T) {
	tests := []struct {
		name           string
		input          []byte
		expectError    bool
		errorMsg       string
		expectNilTuple bool
	}{
		{
			name:           "empty byte array",
			input:          []byte{},
			expectError:    false, // empty is valid, returns empty tuple
			expectNilTuple: true,  // but the tuple itself is nil (not a tuple with zero elements)
		},
		{
			name:        "unknown type code",
			input:       []byte{0xFF},
			expectError: true,
			errorMsg:    "unknown typecode",
		},
		{
			name:        "int8 with insufficient bytes",
			input:       []byte{int8Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "int16 with insufficient bytes",
			input:       []byte{int16Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "int32 with insufficient bytes",
			input:       []byte{int32Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "int64 with insufficient bytes",
			input:       []byte{int64Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "uint8 with insufficient bytes",
			input:       []byte{uint8Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "uint16 with insufficient bytes",
			input:       []byte{uint16Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "uint32 with insufficient bytes",
			input:       []byte{uint32Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "uint64 with insufficient bytes",
			input:       []byte{uint64Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "float32 with insufficient bytes - 0 bytes",
			input:       []byte{float32Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "float32 with insufficient bytes - 3 bytes",
			input:       []byte{float32Code, 0x00, 0x00, 0x00},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "float64 with insufficient bytes - 0 bytes",
			input:       []byte{float64Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "float64 with insufficient bytes - 7 bytes",
			input:       []byte{float64Code, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "decimal64 with insufficient bytes - 0 bytes",
			input:       []byte{decimal64Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "decimal64 with insufficient bytes - 7 bytes",
			input:       []byte{decimal64Code, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "decimal128 with insufficient bytes - 0 bytes",
			input:       []byte{decimal128Code},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "decimal128 with insufficient bytes - 15 bytes",
			input:       []byte{decimal128Code, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "uuid with insufficient bytes - 0 bytes",
			input:       []byte{uuidCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "uuid with insufficient bytes - 15 bytes",
			input:       []byte{uuidCode, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "objectId with insufficient bytes - 0 bytes",
			input:       []byte{objectIdCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "objectId with insufficient bytes - 17 bytes (only uuid part)",
			input:       []byte{objectIdCode, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "date with insufficient bytes",
			input:       []byte{dateCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "datetime with insufficient bytes",
			input:       []byte{datetimeCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "timestamp with insufficient bytes",
			input:       []byte{timestampCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "time with insufficient bytes",
			input:       []byte{timeCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "string with insufficient bytes",
			input:       []byte{stringTypeCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "bit with insufficient bytes",
			input:       []byte{bitCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "enum with insufficient bytes",
			input:       []byte{enumCode},
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "partial data in middle of multi-element tuple",
			input:       []byte{int8Code, intZeroCode, float32Code, 0x00, 0x00}, // valid int8, incomplete float32
			expectError: true,
			errorMsg:    "insufficient bytes",
		},
		{
			name:        "random non-tuple bytes",
			input:       []byte{0x99, 0xAA, 0xBB, 0xCC},
			expectError: true,
			errorMsg:    "unknown typecode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Unpack(tt.input)

			if tt.expectError {
				require.Error(t, err, "Expected error but got none")
				if tt.errorMsg != "" {
					require.Contains(t, err.Error(), tt.errorMsg, "Error message should contain expected text")
				}
				require.Nil(t, result, "Result should be nil when error occurs")
			} else {
				require.NoError(t, err, "Expected no error")
				if tt.expectNilTuple {
					require.Nil(t, result, "Result should be nil for empty input")
				} else {
					require.NotNil(t, result, "Result should not be nil")
				}
			}
		})
	}
}

func TestDecodeTupleValidEdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		tuple    Tuple
		expected Tuple
	}{
		{
			name:     "nil value",
			tuple:    Tuple{nil},
			expected: Tuple{nil},
		},
		{
			name:     "zero values",
			tuple:    Tuple{int8(0), int16(0), int32(0), int64(0), uint8(0), uint16(0), uint32(0), uint64(0)},
			expected: Tuple{int8(0), int16(0), int32(0), int64(0), uint8(0), uint16(0), uint32(0), uint64(0)},
		},
		{
			name:     "negative values",
			tuple:    Tuple{int8(-128), int16(-32768), int32(-2147483648), int64(-9223372036854775808)},
			expected: Tuple{int8(-128), int16(-32768), int32(-2147483648), int64(-9223372036854775808)},
		},
		{
			name:     "max values",
			tuple:    Tuple{int8(127), int16(32767), int32(2147483647), int64(9223372036854775807), uint8(255), uint16(65535), uint32(4294967295), uint64(18446744073709551615)},
			expected: Tuple{int8(127), int16(32767), int32(2147483647), int64(9223372036854775807), uint8(255), uint16(65535), uint32(4294967295), uint64(18446744073709551615)},
		},
		{
			name:     "empty string",
			tuple:    Tuple{[]byte("")},
			expected: Tuple{[]byte(nil)}, // empty string is decoded as nil byte slice
		},
		{
			name:     "string with null bytes",
			tuple:    Tuple{[]byte{0x00, 0x01, 0x02}},
			expected: Tuple{[]byte{0x00, 0x01, 0x02}},
		},
		{
			name:     "boolean values",
			tuple:    Tuple{true, false, true, false},
			expected: Tuple{true, false, true, false},
		},
		{
			name:     "zero float values",
			tuple:    Tuple{float32(0.0), float64(0.0)},
			expected: Tuple{float32(0.0), float64(0.0)},
		},
		{
			name:     "multiple nil values",
			tuple:    Tuple{nil, nil, nil},
			expected: Tuple{nil, nil, nil},
		},
		{
			name:     "mixed types with nil",
			tuple:    Tuple{int32(42), nil, []byte("test"), nil, true},
			expected: Tuple{int32(42), nil, []byte("test"), nil, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			packer := NewPacker()
			encodeBufToPacker(tt.tuple, packer)
			encoded := packer.GetBuf()

			// Decode
			result, err := Unpack(encoded)
			require.NoError(t, err, "Decoding should not return error")
			require.Equal(t, len(tt.expected), len(result), "Tuple length mismatch")

			// Compare elements
			for i := range tt.expected {
				require.Equal(t, tt.expected[i], result[i], "Element %d mismatch", i)
			}
		})
	}
}

func TestDecodeTupleWithSchema(t *testing.T) {
	tests := []struct {
		name           string
		tuple          Tuple
		expectedSchema []T
	}{
		{
			name:           "single int8",
			tuple:          Tuple{int8(42)},
			expectedSchema: []T{T_int8},
		},
		{
			name:           "mixed types",
			tuple:          Tuple{int32(100), []byte("hello"), true, float64(3.14)},
			expectedSchema: []T{T_int32, T_varchar, T_bool, T_float64},
		},
		{
			name:           "with nil",
			tuple:          Tuple{nil, int64(42), nil},
			expectedSchema: []T{T_any, T_int64, T_any},
		},
		{
			name:           "all numeric types",
			tuple:          Tuple{int8(1), int16(2), int32(3), int64(4), uint8(5), uint16(6), uint32(7), uint64(8)},
			expectedSchema: []T{T_int8, T_int16, T_int32, T_int64, T_uint8, T_uint16, T_uint32, T_uint64},
		},
		{
			name:           "date/time types",
			tuple:          Tuple{DateFromCalendar(2000, 1, 1), DatetimeFromClock(2000, 1, 1, 1, 1, 0, 0), FromClockUTC(2000, 2, 2, 2, 2, 0, 0)},
			expectedSchema: []T{T_date, T_datetime, T_timestamp},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			packer := NewPacker()
			encodeBufToPacker(tt.tuple, packer)
			encoded := packer.GetBuf()

			// Decode with schema
			result, schema, err := UnpackWithSchema(encoded)
			require.NoError(t, err, "Decoding should not return error")
			require.NotNil(t, result, "Result should not be nil")
			require.Equal(t, tt.expectedSchema, schema, "Schema mismatch")
			require.Equal(t, len(tt.tuple), len(result), "Tuple length mismatch")
		})
	}
}
