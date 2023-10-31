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
	"math"
	"testing"

	"golang.org/x/exp/constraints"

	"github.com/stretchr/testify/require"
)

func TestBlockidMarshalAndUnmarshal(t *testing.T) {
	var blockid Blockid
	for i := 0; i < len(blockid); i++ {
		blockid[i] = byte(i)
	}

	data, err := blockid.Marshal()
	require.NoError(t, err)

	var ret Blockid
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, blockid, ret)
}

func TestUuidMarshalAndUnmarshal(t *testing.T) {
	var uuid Uuid
	for i := 0; i < len(uuid); i++ {
		uuid[i] = byte(i)
	}

	data, err := uuid.Marshal()
	require.NoError(t, err)

	var ret Uuid
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, uuid, ret)
}

func TestTSMarshalAndUnmarshal(t *testing.T) {
	var ts TS
	for i := 0; i < len(ts); i++ {
		ts[i] = byte(i)
	}

	data, err := ts.Marshal()
	require.NoError(t, err)

	var ret TS
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, ts, ret)
}

func TestDecimal64MarshalAndUnmarshal(t *testing.T) {
	d := Decimal64(100)
	data, err := d.Marshal()
	require.NoError(t, err)

	var ret Decimal64
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, d, ret)
}

func TestDecimal128MarshalAndUnmarshal(t *testing.T) {
	d := Decimal128{B0_63: 1, B64_127: 100}
	data, err := d.Marshal()
	require.NoError(t, err)

	var ret Decimal128
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, d, ret)
}

func TestTypeMarshalAndUnmarshal(t *testing.T) {
	typ := Type{
		Oid:     T(1),
		Charset: 2,
		notNull: 0,
		dummy2:  4,
		Size:    5,
		Width:   6,
		Scale:   -1,
	}

	size := typ.ProtoSize()
	data := make([]byte, size)
	n, err := typ.MarshalTo(data)
	require.NoError(t, err)
	require.Equal(t, size, n)

	var ret Type
	err = ret.Unmarshal(data)
	require.NoError(t, err)
	require.Equal(t, typ, ret)
}

func TestType_String(t *testing.T) {
	myType := T_int64.ToType()
	require.Equal(t, "BIGINT", myType.String())
}

func TestType_Eq(t *testing.T) {
	myType := T_int64.ToType()
	myType1 := T_int64.ToType()
	require.True(t, myType.Eq(myType1))
}

func TestT_ToType(t *testing.T) {
	require.Equal(t, int32(1), T_int8.ToType().Size)
	require.Equal(t, int32(2), T_int16.ToType().Size)
	require.Equal(t, int32(4), T_int32.ToType().Size)
	require.Equal(t, int32(8), T_int64.ToType().Size)
	require.Equal(t, int32(1), T_uint8.ToType().Size)
	require.Equal(t, int32(2), T_uint16.ToType().Size)
	require.Equal(t, int32(4), T_uint32.ToType().Size)
	require.Equal(t, int32(8), T_uint64.ToType().Size)
}

func TestT_String(t *testing.T) {
	require.Equal(t, "TINYINT", T_int8.String())
	require.Equal(t, "SMALLINT", T_int16.String())
	require.Equal(t, "INT", T_int32.String())
}

func TestT_OidString(t *testing.T) {
	require.Equal(t, "T_int8", T_int8.OidString())
	require.Equal(t, "T_int16", T_int16.OidString())
	require.Equal(t, "T_int32", T_int32.OidString())
	require.Equal(t, "T_int64", T_int64.OidString())

	require.Equal(t, "T_uint8", T_uint8.OidString())
	require.Equal(t, "T_uint16", T_uint16.OidString())
	require.Equal(t, "T_uint32", T_uint32.OidString())
	require.Equal(t, "T_uint64", T_uint64.OidString())

	require.Equal(t, "T_float32", T_float32.OidString())
	require.Equal(t, "T_float64", T_float64.OidString())
}

func sliceCopy(a, b []float64) {
	for i := range a {
		b[i] = a[i] + a[i]
	}
}

func BenchmarkCopy(b *testing.B) {
	x := make([]float64, 512)
	y := make([]float64, 512)
	for i := 0; i < 512; i++ {
		x[i] = float64(i)
	}

	for n := 0; n < b.N; n++ {
		sliceCopy(x, y)
	}
}

func sliceCopyG[T constraints.Ordered](a, b []T) {
	for i := range a {
		b[i] = a[i] + a[i]
	}
}

func BenchmarkCopyG(b *testing.B) {
	x := make([]float64, 512)
	y := make([]float64, 512)
	for i := 0; i < 512; i++ {
		x[i] = float64(i)
	}

	for n := 0; n < b.N; n++ {
		sliceCopyG(x, y)
	}
}

func BenchmarkCastA(b *testing.B) {
	x := make([]int16, 8192)
	y := make([]float64, 8192)
	for i := 0; i < 8192; i++ {
		x[i] = int16(i)
	}
	for n := 0; n < b.N; n++ {
		for i := 0; i < 8192; i++ {
			y[i] = math.Log(float64(x[i]))
		}
	}
}

func BenchmarkCastB(b *testing.B) {
	x := make([]int16, 8192)
	y := make([]float64, 8192)
	z := make([]float64, 8192)
	for i := 0; i < 8192; i++ {
		x[i] = int16(i)
	}
	for n := 0; n < b.N; n++ {
		for i := 0; i < 8192; i++ {
			y[i] = float64(x[i])
		}
		for i := 0; i < 8192; i++ {
			z[i] = math.Log(y[i])
		}
	}
}

func TestType_DescString(t *testing.T) {
	require.Equal(t, Type{
		Oid:   T_char,
		Width: 10,
	}.DescString(), "CHAR(10)")

	require.Equal(t, Type{
		Oid:   T_varchar,
		Width: 20,
	}.DescString(), "VARCHAR(20)")

	require.Equal(t, Type{
		Oid:   T_binary,
		Width: 0,
	}.DescString(), "BINARY(0)")

	require.Equal(t, Type{
		Oid:   T_varbinary,
		Width: 0,
	}.DescString(), "VARBINARY(0)")

	require.Equal(t, Type{
		Oid:   T_decimal64,
		Width: 5,
		Scale: 2,
	}.DescString(), "DECIMAL(5,2)")

	require.Equal(t, Type{
		Oid:   T_decimal128,
		Width: 20,
		Scale: 10,
	}.DescString(), "DECIMAL(20,10)")
}
