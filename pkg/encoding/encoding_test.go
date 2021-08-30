package encoding

import (
	"fmt"
	"math"
	"math/rand"
	"matrixone/pkg/container/types"
	"testing"
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
	var num int8
	for num = math.MinInt8; num != math.MaxInt8; num++ {
		if DecodeInt8(EncodeInt8(num)) != num {
			t.Fatalf("Int8 Encoding Error\n")
		}
	}
	if DecodeInt8(EncodeInt8(math.MaxInt8)) != math.MaxInt8 {
		t.Fatalf("Int8 Encoding Error\n")
	}
}

func TestEncodeUint8(t *testing.T) {
	var num uint8
	for num = 0; num != math.MaxUint8; num++ {
		if DecodeUint8(EncodeUint8(num)) != num {
			t.Fatalf("Uint8 Encoding Error\n")
		}
	}
	if DecodeUint8(EncodeUint8(math.MaxUint8)) != math.MaxUint8 {
		t.Fatalf("UInt8 Encoding Error\n")
	}
}

func TestEncodeInt16(t *testing.T) {
	var num int16
	for num = math.MinInt16; num != math.MaxInt16; num++ {
		if DecodeInt16(EncodeInt16(num)) != num {
			t.Fatalf("Int16 Encoding Error\n")
		}
	}
	if DecodeInt16(EncodeInt16(math.MaxInt16)) != math.MaxInt16 {
		t.Fatalf("Int16 Encoding Error\n")
	}
}

func TestEncodeUint16(t *testing.T) {
	var num uint16
	for num = 0; num != math.MaxUint16; num++ {
		if DecodeUint16(EncodeUint16(num)) != num {
			t.Fatalf("Uint16 Encoding Error\n")
		}
	}
	if DecodeUint16(EncodeUint16(num)) != num {
		t.Fatalf("Uint16 Encoding Error\n")
	}
}

func TestEncodeInt32(t *testing.T) {
	var num int32
	for num = math.MinInt32; num != math.MaxInt32; num++ {
		if DecodeInt32(EncodeInt32(num)) != num {
			t.Fatalf("Int32 Encoding Error\n")
		}
	}
	if DecodeInt32(EncodeInt32(num)) != num {
		t.Fatalf("Int32 Encoding Error\n")
	}
}

func TestEncodeUint32(t *testing.T) {
	var num uint32
	for num = 0; num != math.MaxUint32; num++ {
		if DecodeUint32(EncodeUint32(num)) != num {
			t.Fatalf("Uint32 Encoding Error\n")
		}
	}
	if DecodeUint32(EncodeUint32(num)) != num {
		t.Fatalf("Uint32 Encoding Error\n")
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

func TestEncodeUint64(t *testing.T) {
	nums := []uint64{0, math.MaxUint64}
	for _, num := range nums {
		if DecodeUint64(EncodeUint64(num)) != num {
			t.Fatalf("Int64 Encoding Error\n")
		}
	}
}

func TestEncodeFloat32(t *testing.T) {
	nums := []float32{math.MaxFloat32,math.SmallestNonzeroFloat32,-math.MaxFloat32,-math.SmallestNonzeroFloat32}
	for _, num := range nums {
		if DecodeFloat32(EncodeFloat32(num)) != num {
			t.Fatalf("Float32 Encoding Error\n")
		}
	}
}

func TestEncodeFloat64(t *testing.T) {
	nums := []float64{math.MaxFloat64,math.SmallestNonzeroFloat64,-math.MaxFloat64,-math.SmallestNonzeroFloat64}
	for _, num := range nums {
		if DecodeFloat64(EncodeFloat64(num)) != num {
			t.Fatalf("Float64 Encoding Error\n")
		}
	}
}

func TestEncodeDate(t *testing.T) {
	var num types.Date
	for num = 0; num != math.MaxInt32; num++ {
		if DecodeDate(EncodeDate(num)) != num {
			t.Fatalf("Date Encoding Error\n")
		}
	}
	if DecodeDate(EncodeDate(num)) != num {
		t.Fatalf("Date Encoding Error\n")
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

func TestStringSliceEncoding(t *testing.T) {
	xs := []string{"a", "bc", "d"}
	data := EncodeStringSlice(xs)
	ys := DecodeStringSlice(data)
	fmt.Printf("ys: %v\n", ys)
}
