// Copyright 2022 Matrix Origin
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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestOctUint8(t *testing.T) {
	procs := testutil.NewProc()
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeUint8Vector([]uint8{12, 99, 100, 255}, nil)
	e1, _ := types.Decimal128_FromStringWithScale("14", 64, 0)
	e2, _ := types.Decimal128_FromStringWithScale("143", 64, 0)
	e3, _ := types.Decimal128_FromStringWithScale("144", 64, 0)
	e4, _ := types.Decimal128_FromStringWithScale("377", 64, 0)
	expected := []types.Decimal128{e1, e2, e3, e4}

	t.Run("oct uin8 test", func(t *testing.T) {
		result, err := Oct[uint8](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}

		checkOctResult(t, result, expected, false)
	})
}

func TestOctUint16(t *testing.T) {
	procs := testutil.NewProc()
	vecs := make([]*vector.Vector, 1)
	vecs[0] = testutil.MakeUint16Vector([]uint16{12, 99, 100, 255, 1024, 10000, 65535}, nil)
	e1, _ := types.Decimal128_FromStringWithScale("14", 64, 0)
	e2, _ := types.Decimal128_FromStringWithScale("143", 64, 0)
	e3, _ := types.Decimal128_FromStringWithScale("144", 64, 0)
	e4, _ := types.Decimal128_FromStringWithScale("377", 64, 0)
	e5, _ := types.Decimal128_FromStringWithScale("2000", 64, 0)
	e6, _ := types.Decimal128_FromStringWithScale("23420", 64, 0)
	e7, _ := types.Decimal128_FromStringWithScale("177777", 64, 0)
	expected := []types.Decimal128{e1, e2, e3, e4, e5, e6, e7}

	t.Run("oct uin16 test", func(t *testing.T) {
		result, err := Oct[uint16](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkOctResult(t, result, expected, false)
	})
}

func TestOctUint32(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeUint32Vector([]uint32{12, 99, 100, 255, 1024, 10000, 65535, 4294967295}, nil)}
	e1, _ := types.Decimal128_FromStringWithScale("14", 64, 0)
	e2, _ := types.Decimal128_FromStringWithScale("143", 64, 0)
	e3, _ := types.Decimal128_FromStringWithScale("144", 64, 0)
	e4, _ := types.Decimal128_FromStringWithScale("377", 64, 0)
	e5, _ := types.Decimal128_FromStringWithScale("2000", 64, 0)
	e6, _ := types.Decimal128_FromStringWithScale("23420", 64, 0)
	e7, _ := types.Decimal128_FromStringWithScale("177777", 64, 0)
	e8, _ := types.Decimal128_FromStringWithScale("37777777777", 64, 0)
	expected := []types.Decimal128{e1, e2, e3, e4, e5, e6, e7, e8}

	t.Run("oct uin32 test", func(t *testing.T) {
		result, err := Oct[uint32](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}

		checkOctResult(t, result, expected, false)
	})
}

func TestOctUint64(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeUint64Vector([]uint64{12, 99, 100, 255, 1024, 10000, 65535, 4294967295, 18446744073709551615}, nil)}
	e1, _ := types.Decimal128_FromStringWithScale("14", 64, 0)
	e2, _ := types.Decimal128_FromStringWithScale("143", 64, 0)
	e3, _ := types.Decimal128_FromStringWithScale("144", 64, 0)
	e4, _ := types.Decimal128_FromStringWithScale("377", 64, 0)
	e5, _ := types.Decimal128_FromStringWithScale("2000", 64, 0)
	e6, _ := types.Decimal128_FromStringWithScale("23420", 64, 0)
	e7, _ := types.Decimal128_FromStringWithScale("177777", 64, 0)
	e8, _ := types.Decimal128_FromStringWithScale("37777777777", 64, 0)
	e9, _ := types.Decimal128_FromStringWithScale("1777777777777777777777", 64, 0)
	expected := []types.Decimal128{e1, e2, e3, e4, e5, e6, e7, e8, e9}

	t.Run("oct uin64 test", func(t *testing.T) {
		result, err := Oct[uint64](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkOctResult(t, result, expected, false)
	})
}

func TestOctInt8(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt8Vector([]int8{-128, -1, 127}, nil)}
	e1, _ := types.Decimal128_FromStringWithScale("1777777777777777777600", 64, 0)
	e2, _ := types.Decimal128_FromStringWithScale("1777777777777777777777", 64, 0)
	e3, _ := types.Decimal128_FromStringWithScale("177", 64, 0)
	expected := []types.Decimal128{e1, e2, e3}

	t.Run("oct int8 test", func(t *testing.T) {
		result, err := Oct[int8](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkOctResult(t, result, expected, false)
	})
}

func TestOctInt16(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt16Vector([]int16{-32768}, nil)}
	e1, _ := types.Decimal128_FromStringWithScale("1777777777777777700000", 64, 0)
	expected := []types.Decimal128{e1}

	t.Run("oct int16 test", func(t *testing.T) {
		result, err := Oct[int16](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkOctResult(t, result, expected, false)
	})
}

func TestOctInt32(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt32Vector([]int32{-2147483648}, nil)}
	e1, _ := types.Decimal128_FromStringWithScale("1777777777760000000000", 64, 0)
	expected := []types.Decimal128{e1}

	t.Run("oct int32 test", func(t *testing.T) {
		result, err := Oct[int32](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkOctResult(t, result, expected, false)
	})
}

func TestOctInt64(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt64Vector([]int64{-9223372036854775808}, nil)}
	e1, _ := types.Decimal128_FromStringWithScale("1000000000000000000000", 64, 0)
	expected := []types.Decimal128{e1}

	t.Run("oct int64 test", func(t *testing.T) {
		result, err := Oct[int64](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkOctResult(t, result, expected, false)
	})
}

func TestOctScalar(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt64Vector([]int64{-9223372036854775808}, nil)}
	vecs[0].MakeScalar(1)
	e1, _ := types.Decimal128_FromStringWithScale("1000000000000000000000", 64, 0)
	expected := []types.Decimal128{e1}

	t.Run("oct scalar test", func(t *testing.T) {
		result, err := Oct[int64](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkOctResult(t, result, expected, true)
	})
}

func checkOctResult(t *testing.T, result *vector.Vector, expected []types.Decimal128, isScalar bool) {
	col := result.Col.([]types.Decimal128)

	require.Equal(t, expected, col)
	require.Equal(t, isScalar, result.IsScalar())
}
