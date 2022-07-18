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
	expected := &types.Bytes{
		Data:    []byte("14143144377"),
		Lengths: []uint32{2, 3, 3, 3},
		Offsets: []uint32{0, 2, 5, 8},
	}

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
	expected := &types.Bytes{
		Data:    []byte("14143144377200023420177777"),
		Lengths: []uint32{2, 3, 3, 3, 4, 5, 6},
		Offsets: []uint32{0, 2, 5, 8, 11, 15, 20},
	}

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
	expected := &types.Bytes{
		Data:    []byte("1414314437720002342017777737777777777"),
		Lengths: []uint32{2, 3, 3, 3, 4, 5, 6, 11},
		Offsets: []uint32{0, 2, 5, 8, 11, 15, 20, 26},
	}

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
	expected := &types.Bytes{
		Data:    []byte("14143144377200023420177777377777777771777777777777777777777"),
		Lengths: []uint32{2, 3, 3, 3, 4, 5, 6, 11, 22},
		Offsets: []uint32{0, 2, 5, 8, 11, 15, 20, 26, 37},
	}

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
	expected := &types.Bytes{
		Data:    []byte("17777777777777777776001777777777777777777777177"),
		Lengths: []uint32{22, 22, 3},
		Offsets: []uint32{0, 22, 44},
	}

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
	expected := &types.Bytes{
		Data:    []byte("1777777777777777700000"),
		Lengths: []uint32{22},
		Offsets: []uint32{0},
	}

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
	expected := &types.Bytes{
		Data:    []byte("1777777777760000000000"),
		Lengths: []uint32{22},
		Offsets: []uint32{0},
	}

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
	expected := &types.Bytes{
		Data:    []byte("1000000000000000000000"),
		Lengths: []uint32{22},
		Offsets: []uint32{0},
	}

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
	vecs[0].IsConst = true
	expected := &types.Bytes{
		Data:    []byte("1000000000000000000000"),
		Lengths: []uint32{22},
		Offsets: []uint32{0},
	}

	t.Run("oct scalar test", func(t *testing.T) {
		result, err := Oct[int64](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkOctResult(t, result, expected, true)
	})
}

func checkOctResult(t *testing.T, result *vector.Vector, expected *types.Bytes, isScalar bool) {
	col := result.Col.(*types.Bytes)

	require.Equal(t, expected, col)
	require.Equal(t, isScalar, result.IsScalar())
}
