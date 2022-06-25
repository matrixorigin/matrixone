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

package encoding

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestEncodeType(t *testing.T) {
	typesConst := []int{0, 1, 2, 3, 5, 6, 7, 9, 10, 11, 12, 13, 15, 18, 20, 21, 32, 200, 201}
	for _, typeId := range typesConst {
		typeStruct := types.T(typeId).ToType()
		typeStruct.Width = int32(rand.Intn(100))
		typeStruct.Precision = int32(rand.Intn(100))
		if DecodeType(EncodeType(typeStruct)) != typeStruct {
			t.Fatalf("Type Encoding Error\n")
		}
	}
}

func TestEncodeInt8(t *testing.T) {
	nums := []int8{math.MinInt8, math.MaxInt8, 0}
	for _, num := range nums {
		if DecodeInt8(EncodeInt8(num)) != num {
			t.Fatalf("Int8 Encoding Error\n")
		}
	}
}

func TestEncodeInt8Slice(t *testing.T) {
	nums := []int8{math.MinInt8, math.MaxInt8, 0}
	numsDecode := DecodeInt8Slice(EncodeInt8Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("Int8Slice Encoding Error\n")
		}
	}
}

func TestEncodeUint8(t *testing.T) {
	nums := []uint8{math.MaxUint8, 0}
	for _, num := range nums {
		if DecodeUint8(EncodeUint8(num)) != num {
			t.Fatalf("Uint8 Encoding Error\n")
		}
	}
}

func TestEncodeUint8Slice(t *testing.T) {
	nums := []uint8{math.MaxUint8, 0}
	numsDecode := DecodeUint8Slice(EncodeUint8Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("Uint8Slice Encoding Error\n")
		}
	}
}

func TestEncodeInt16(t *testing.T) {
	nums := []int16{math.MinInt16, math.MaxInt16, 0}
	for _, num := range nums {
		if DecodeInt16(EncodeInt16(num)) != num {
			t.Fatalf("Int16 Encoding Error\n")
		}
	}
}

func TestEncodeInt16Slice(t *testing.T) {
	nums := []int16{math.MaxInt16, math.MaxInt16, 0}
	numsDecode := DecodeInt16Slice(EncodeInt16Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("int16Slice Encoding Error\n")
		}
	}
}

func TestEncodeUint16(t *testing.T) {
	nums := []uint16{math.MaxUint16, 0}
	for _, num := range nums {
		if DecodeUint16(EncodeUint16(num)) != num {
			t.Fatalf("Uint16 Encoding Error\n")
		}
	}
}

func TestEncodeUint16Slice(t *testing.T) {
	nums := []uint16{math.MaxUint16, math.MaxUint16, 0}
	numsDecode := DecodeUint16Slice(EncodeUint16Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("int16Slice Encoding Error\n")
		}
	}
}

func TestEncodeInt32(t *testing.T) {
	nums := []int32{math.MinInt32, math.MaxInt32, 0}
	for _, num := range nums {
		if DecodeInt32(EncodeInt32(num)) != num {
			t.Fatalf("Int32 Encoding Error\n")
		}
	}
}

func TestEncodeInt32Slice(t *testing.T) {
	nums := []int32{math.MaxInt32, math.MaxInt32, 0}
	numsDecode := DecodeInt32Slice(EncodeInt32Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("int32Slice Encoding Error\n")
		}
	}
}

func TestEncodeUint32(t *testing.T) {
	nums := []uint32{math.MaxUint32, 0}
	for _, num := range nums {
		if DecodeUint32(EncodeUint32(num)) != num {
			t.Fatalf("Uint32 Encoding Error\n")
		}
	}
}

func TestEncodeUint32Slice(t *testing.T) {
	nums := []uint32{math.MaxUint32, math.MaxUint32, 0}
	numsDecode := DecodeUint32Slice(EncodeUint32Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("uint32Slice Encoding Error\n")
		}
	}
}

func TestEncodeInt64(t *testing.T) {
	nums := []int64{math.MinInt64, math.MaxInt64, 0}
	for _, num := range nums {
		if DecodeInt64(EncodeInt64(num)) != num {
			t.Fatalf("Int64 Encoding Error\n")
		}
	}
}

func TestEncodeInt64Slice(t *testing.T) {
	nums := []int64{math.MaxInt64, math.MaxInt64, 0}
	numsDecode := DecodeInt64Slice(EncodeInt64Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("int64Slice Encoding Error\n")
		}
	}
}

func TestEncodeUint64(t *testing.T) {
	nums := []uint64{0, math.MaxUint64}
	for _, num := range nums {
		if DecodeUint64(EncodeUint64(num)) != num {
			t.Fatalf("Uint64 Encoding Error\n")
		}
	}
}

func TestEncodeUint64Slice(t *testing.T) {
	nums := []uint64{math.MaxUint64, math.MaxUint64, 0}
	numsDecode := DecodeUint64Slice(EncodeUint64Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("uint64Slice Encoding Error\n")
		}
	}
}

func TestEncodeFloat32(t *testing.T) {
	nums := []float32{math.MaxFloat32, math.SmallestNonzeroFloat32, -math.MaxFloat32, -math.SmallestNonzeroFloat32}
	for _, num := range nums {
		if DecodeFloat32(EncodeFloat32(num)) != num {
			t.Fatalf("Float32 Encoding Error\n")
		}
	}
}

func TestEncodeFloat32Slice(t *testing.T) {
	nums := []float32{math.MaxFloat32, math.SmallestNonzeroFloat32, -math.MaxFloat32, -math.SmallestNonzeroFloat32}
	numsDecode := DecodeFloat32Slice(EncodeFloat32Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("Float32Slice Encoding Error\n")
		}
	}
}

func TestEncodeFloat64(t *testing.T) {
	nums := []float64{math.MaxFloat64, math.SmallestNonzeroFloat64, -math.MaxFloat64, -math.SmallestNonzeroFloat64}
	for _, num := range nums {
		if DecodeFloat64(EncodeFloat64(num)) != num {
			t.Fatalf("Float64 Encoding Error\n")
		}
	}
}

func TestEncodeFloat64Slice(t *testing.T) {
	nums := []float64{math.MaxFloat64, math.SmallestNonzeroFloat64, -math.MaxFloat64, -math.SmallestNonzeroFloat64}
	numsDecode := DecodeFloat64Slice(EncodeFloat64Slice(nums))
	for i, num := range nums {
		if numsDecode[i] != num {
			t.Fatalf("Float64Slice Encoding Error\n")
		}
	}
}

func TestEncodeDate(t *testing.T) {
	nums := []types.Date{0, math.MaxInt32}
	for _, num := range nums {
		if DecodeDate(EncodeDate(num)) != num {
			t.Fatalf("Date Encoding Error\n")
		}
	}
}

func TestEncodeDateSlice(t *testing.T) {
	dates := []types.Date{0, math.MaxInt32}
	datesDecode := DecodeDateSlice(EncodeDateSlice(dates))
	for i, date := range dates {
		if datesDecode[i] != date {
			t.Fatalf("Date Encoding Error\n")
		}
	}
}

func TestEncodeDatetime(t *testing.T) {
	nums := []types.Datetime{math.MinInt64, math.MaxInt64, 0}
	for _, num := range nums {
		if DecodeDatetime(EncodeDatetime(num)) != num {
			t.Fatalf("Int64 Encoding Error\n")
		}
	}
}

func TestEncodeDatetimeSlice(t *testing.T) {
	dateTimes := []types.Datetime{0, math.MaxInt64}
	dateTimesDecode := DecodeDatetimeSlice(EncodeDatetimeSlice(dateTimes))
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
		x := EncodeFloat64Slice(v)
		y := DecodeFloat64Slice(x)
		if len(y) != len(v) {
			panic("Encode decode error")
		}
	}
}

func BenchmarkEncodeSliceFloat64_Old(b *testing.B) {
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := 0; i < b.N; i++ {
		x := EncodeFloat64SliceForBenchmark(v)
		y := DecodeFloat64SliceForBenchmark(x)
		if len(y) != len(v) {
			panic("Encode decode error")
		}
	}
}
