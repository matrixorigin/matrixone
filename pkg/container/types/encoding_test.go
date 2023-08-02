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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntToUint(t *testing.T) {
	require.Equal(t, int32(1), Uint32ToInt32(Int32ToUint32(1)))
	require.Equal(t, int32(0), Uint32ToInt32(Int32ToUint32(0)))
	require.Equal(t, int32(-1), Uint32ToInt32(Int32ToUint32(-1)))
}

func TestEncodeInt8(t *testing.T) {
	nums := []int8{math.MinInt8, math.MaxInt8, 0}
	for _, num := range nums {
		if DecodeInt8(EncodeInt8(&num)) != num {
			t.Fatalf("Int8 Encoding Error\n")
		}
	}
}

func TestEncodeInt8Slice(t *testing.T) {
	nums := []int8{math.MinInt8, math.MaxInt8, 0}
	numsDecode := DecodeSlice[int8](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("Int8Slice Encoding Error\n")
		}
	}
}

func TestEncodeUint8(t *testing.T) {
	nums := []uint8{math.MaxUint8, 0}
	for _, num := range nums {
		if DecodeUint8(EncodeUint8(&num)) != num {
			t.Fatalf("Uint8 Encoding Error\n")
		}
	}
}

func TestEncodeUint8Slice(t *testing.T) {
	nums := []uint8{math.MaxUint8, 0}
	numsDecode := DecodeSlice[uint8](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("Uint8Slice Encoding Error\n")
		}
	}
}

func TestEncodeInt16(t *testing.T) {
	nums := []int16{math.MinInt16, math.MaxInt16, 0}
	for _, num := range nums {
		if DecodeInt16(EncodeInt16(&num)) != num {
			t.Fatalf("Int16 Encoding Error\n")
		}
	}
}

func TestEncodeInt16Slice(t *testing.T) {
	nums := []int16{math.MaxInt16, math.MaxInt16, 0}
	numsDecode := DecodeSlice[int16](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("int16Slice Encoding Error\n")
		}
	}
}

func TestEncodeUint16(t *testing.T) {
	nums := []uint16{math.MaxUint16, 0}
	for _, num := range nums {
		if DecodeUint16(EncodeUint16(&num)) != num {
			t.Fatalf("Uint16 Encoding Error\n")
		}
	}
}

func TestEncodeUint16Slice(t *testing.T) {
	nums := []uint16{math.MaxUint16, math.MaxUint16, 0}
	numsDecode := DecodeSlice[uint16](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("int16Slice Encoding Error\n")
		}
	}
}

func TestEncodeInt32(t *testing.T) {
	nums := []int32{math.MinInt32, math.MaxInt32, 0}
	for _, num := range nums {
		if DecodeInt32(EncodeInt32(&num)) != num {
			t.Fatalf("Int32 Encoding Error\n")
		}
	}
}

func TestEncodeInt32Slice(t *testing.T) {
	nums := []int32{math.MaxInt32, math.MaxInt32, 0}
	numsDecode := DecodeSlice[int32](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("int32Slice Encoding Error\n")
		}
	}
}

func TestEncodeUint32(t *testing.T) {
	nums := []uint32{math.MaxUint32, 0}
	for _, num := range nums {
		if DecodeUint32(EncodeUint32(&num)) != num {
			t.Fatalf("Uint32 Encoding Error\n")
		}
	}
}

func TestEncodeUint32Slice(t *testing.T) {
	nums := []uint32{math.MaxUint32, math.MaxUint32, 0}
	numsDecode := DecodeSlice[uint32](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("uint32Slice Encoding Error\n")
		}
	}
}

func TestEncodeInt64(t *testing.T) {
	nums := []int64{math.MinInt64, math.MaxInt64, 0}
	for _, num := range nums {
		if DecodeInt64(EncodeInt64(&num)) != num {
			t.Fatalf("Int64 Encoding Error\n")
		}
	}
}

func TestEncodeInt64Slice(t *testing.T) {
	nums := []int64{math.MaxInt64, math.MaxInt64, 0}
	numsDecode := DecodeSlice[int64](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("int64Slice Encoding Error\n")
		}
	}
}

func TestEncodeUint64(t *testing.T) {
	nums := []uint64{0, math.MaxUint64}
	for _, num := range nums {
		if DecodeUint64(EncodeUint64(&num)) != num {
			t.Fatalf("Uint64 Encoding Error\n")
		}
	}
}

func TestEncodeUint64Slice(t *testing.T) {
	nums := []uint64{math.MaxUint64, math.MaxUint64, 0}
	numsDecode := DecodeSlice[uint64](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("uint64Slice Encoding Error\n")
		}
	}
}

func TestEncodeFloat32(t *testing.T) {
	nums := []float32{math.MaxFloat32, math.SmallestNonzeroFloat32, -math.MaxFloat32, -math.SmallestNonzeroFloat32}
	for _, num := range nums {
		if DecodeFloat32(EncodeFloat32(&num)) != num {
			t.Fatalf("Float32 Encoding Error\n")
		}
	}
}

func TestEncodeFloat32Slice(t *testing.T) {
	nums := []float32{math.MaxFloat32, math.SmallestNonzeroFloat32, -math.MaxFloat32, -math.SmallestNonzeroFloat32}
	numsDecode := DecodeSlice[float32](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("Float32Slice Encoding Error\n")
		}
	}
}

func TestEncodeFloat64(t *testing.T) {
	nums := []float64{math.MaxFloat64, math.SmallestNonzeroFloat64, -math.MaxFloat64, -math.SmallestNonzeroFloat64}
	for _, num := range nums {
		if DecodeFloat64(EncodeFloat64(&num)) != num {
			t.Fatalf("Float64 Encoding Error\n")
		}
	}
}

func TestEncodeFloat64Slice(t *testing.T) {
	nums := []float64{math.MaxFloat64, math.SmallestNonzeroFloat64, -math.MaxFloat64, -math.SmallestNonzeroFloat64}
	numsDecode := DecodeSlice[float64](EncodeSlice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("Float64Slice Encoding Error\n")
		}
	}
}

func TestEncodeDate(t *testing.T) {
	nums := []Date{0, math.MaxInt32}
	for _, num := range nums {
		if DecodeDate(EncodeDate(&num)) != num {
			t.Fatalf("Date Encoding Error\n")
		}
	}
}

func TestEncodeDateSlice(t *testing.T) {
	dates := []Date{0, math.MaxInt32}
	datesDecode := DecodeSlice[Date](EncodeSlice(dates))
	for i, date := range dates {
		if datesDecode[i] != date {
			t.Fatalf("Date Encoding Error\n")
		}
	}
}

func TestEncodeDatetime(t *testing.T) {
	nums := []Datetime{math.MinInt64, math.MaxInt64, 0}
	for _, num := range nums {
		if DecodeDatetime(EncodeDatetime(&num)) != num {
			t.Fatalf("Int64 Encoding Error\n")
		}
	}
}

func TestEncodeDatetimeSlice(t *testing.T) {
	dateTimes := []Datetime{0, math.MaxInt64}
	dateTimesDecode := DecodeSlice[Datetime](EncodeSlice(dateTimes))
	for i, date := range dateTimes {
		if dateTimesDecode[i] != date {
			t.Fatalf("Date Encoding Error\n")
		}
	}
}

func TestStringSliceEncoding(t *testing.T) {
	xs := []string{"a", "bc", "d"}
	data := EncodeStringSlice(xs)
	ys := DecodeStringSlice(data)
	fmt.Printf("ys: %v\n", ys)
}

func BenchmarkEncodeSliceFloat64(b *testing.B) {
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := 0; i < b.N; i++ {
		x := EncodeSlice(v)
		y := DecodeSlice[float64](x)
		if len(y) != len(v) {
			panic("Encode decode error")
		}
	}
}

func BenchmarkEncodeType(b *testing.B) {
	v := New(10, 10, 10)
	for i := 0; i < b.N; i++ {
		x, _ := EncodeType(&v)
		y := DecodeType(x)
		if !y.TypeEqual(&v) {
			panic("Encode decode error")
		}
	}
}

func BenchmarkEncodeTypeWithEnum(b *testing.B) {
	v := New(10, 10, 10)
	v.EnumValues = []string{"asdadfasdf", "asdfadsfadf", "asdfasdfadfa",
		"asdfajfalksdjflka", "kkkkkkkkkkkkkkmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"}
	for i := 0; i < b.N; i++ {
		x, _ := EncodeType(&v)
		y := DecodeType(x)
		if !y.TypeEqual(&v) {
			panic("Encode decode error")
		}
	}
}

func BenchmarkTypeEncode(b *testing.B) {
	v := New(10, 10, 10)
	v.EnumValues = []string{"asdadfasdf", "asdfadsfadf", "asdfasdfadfa",
		"asdfajfalksdjflka", "kkkkkkkkkkkkkkmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"}
	for i := 0; i < b.N; i++ {
		x, _ := v.Marshal()
		var y Type
		y.Unmarshal(x)
		if !y.TypeEqual(&v) {
			panic("Encode decode error")
		}
	}
}
