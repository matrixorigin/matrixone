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

package operator

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func TestPlus(t *testing.T) {
	plusIntAndFloat[int8](t, types.T_int8, 21, -41, -20)
	plusIntAndFloat[int16](t, types.T_int16, 21, -41, -20)
	plusIntAndFloat[int32](t, types.T_int32, 21, -41, -20)
	plusIntAndFloat[int64](t, types.T_int64, 21, -41, -20)

	plusIntAndFloat[uint8](t, types.T_uint8, 21, 47, 68)
	plusIntAndFloat[uint16](t, types.T_uint16, 21, 47, 68)
	plusIntAndFloat[uint32](t, types.T_uint32, 21, 47, 68)
	plusIntAndFloat[uint64](t, types.T_uint64, 21, 47, 68)

	plusIntAndFloat[float32](t, types.T_float32, 21.45, 40.55, 62)
	plusIntAndFloat[float64](t, types.T_float64, 21.45, 40.55, 62)

	leftType1 := types.Type{Oid: types.T_decimal64, Size: 8, Width: 10, Scale: 5}
	rightType1 := types.Type{Oid: types.T_decimal64, Size: 8, Width: 10, Scale: 5}
	resType1 := types.Type{Oid: types.T_decimal64, Size: types.DECIMAL64_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: 5}
	plusDecimal64(t, types.Decimal64FromInt32(33333300), leftType1, types.Decimal64FromInt32(-123450000), rightType1, types.Decimal64FromInt32(-90116700), resType1)

	leftType2 := types.Type{Oid: types.T_decimal128, Size: 16, Width: 20, Scale: 5}
	rightType2 := types.Type{Oid: types.T_decimal128, Size: 16, Width: 20, Scale: 5}
	resType2 := types.Type{Oid: types.T_decimal128, Size: types.DECIMAL128_NBYTES, Width: types.DECIMAL128_WIDTH, Scale: 5}
	plusDecimal128(t, types.Decimal128FromInt32(33333300), leftType2, types.Decimal128FromInt32(-123450000), rightType2, types.Decimal128FromInt32(-90116700), resType2)
}

// Unit test input for int and float type parameters of the plus operator
func plusIntAndFloat[T constraints.Integer | constraints.Float](t *testing.T, typ types.T, left T, right T, res T) {
	procs := makeProcess()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makePlusVectors(left, true, right, true, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makePlusVectors(left, false, right, true, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
		{
			name:       "TEST03",
			vecs:       makePlusVectors(left, true, right, false, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
		{
			name:       "TEST04",
			vecs:       makePlusVectors(left, false, right, false, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := Plus[T](c.vecs, c.proc, c.vecs[0].Typ)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameter of the plus operator
func makePlusVectors[T constraints.Integer | constraints.Float](left T, leftScalar bool, right T, rightScalar bool, t types.T) []*vector.Vector {
	vectors := make([]*vector.Vector, 2)
	vectors[0] = &vector.Vector{
		Col:     []T{left},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: t},
		IsConst: leftScalar,
		Length:  1,
	}
	vectors[1] = &vector.Vector{
		Col:     []T{right},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: t},
		IsConst: rightScalar,
		Length:  1,
	}
	return vectors
}

// Unit test input of decimal64 parameters of plus operator
func plusDecimal64(t *testing.T, left types.Decimal64, leftType types.Type, right types.Decimal64, rightType types.Type,
	res types.Decimal64, restType types.Type) {
	procs := makeProcess()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantType   types.Type
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makeDecimal64Vectors(left, leftType, true, right, rightType, true),
			proc:       procs,
			wantBytes:  []types.Decimal64{res},
			wantType:   restType,
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makeDecimal64Vectors(left, leftType, false, right, rightType, true),
			proc:       procs,
			wantBytes:  []types.Decimal64{res},
			wantType:   restType,
			wantScalar: false,
		},
		{
			name:       "TEST03",
			vecs:       makeDecimal64Vectors(left, leftType, true, right, rightType, false),
			proc:       procs,
			wantBytes:  []types.Decimal64{res},
			wantType:   restType,
			wantScalar: false,
		},
		{
			name:       "TEST04",
			vecs:       makeDecimal64Vectors(left, leftType, false, right, rightType, false),
			proc:       procs,
			wantBytes:  []types.Decimal64{res},
			wantType:   leftType,
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			decimalres, err := PlusDecimal64(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}

			a := c.wantBytes.([]types.Decimal64)
			b := decimalres.Col.([]types.Decimal64)
			require.Equal(t, a[0].ToStringWithScale(restType.Scale), b[0].ToStringWithScale(decimalres.Typ.Scale))
			require.Equal(t, c.wantBytes, decimalres.Col)
			require.Equal(t, c.wantType, decimalres.Typ)
			require.Equal(t, c.wantScalar, decimalres.IsScalar())
		})
	}
}

// Unit test input of decimal128 parameter of plus operator
func plusDecimal128(t *testing.T, left types.Decimal128, leftType types.Type, right types.Decimal128, rightType types.Type,
	res types.Decimal128, resType types.Type) {
	//leftType := types.Type{Oid: types.T_decimal128, Size: 16, Width: 20, Scale: 5}
	//rightType := types.Type{Oid: types.T_decimal128, Size: 16, Width: 20, Scale: 5}

	procs := makeProcess()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantType   types.Type
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makeDecimal128Vectors(left, leftType, true, right, rightType, true),
			proc:       procs,
			wantBytes:  []types.Decimal128{res},
			wantType:   resType,
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makeDecimal128Vectors(left, leftType, false, right, rightType, true),
			proc:       procs,
			wantBytes:  []types.Decimal128{res},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "TEST03",
			vecs:       makeDecimal128Vectors(left, leftType, true, right, rightType, false),
			proc:       procs,
			wantBytes:  []types.Decimal128{res},
			wantType:   resType,
			wantScalar: false,
		},
		{
			name:       "TEST04",
			vecs:       makeDecimal128Vectors(left, leftType, false, right, rightType, false),
			proc:       procs,
			wantBytes:  []types.Decimal128{res},
			wantType:   leftType,
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			decimalres, err := PlusDecimal128(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}

			a := c.wantBytes.([]types.Decimal128)
			b := decimalres.Col.([]types.Decimal128)
			require.Equal(t, a[0].ToStringWithScale(c.wantType.Scale), b[0].ToStringWithScale(decimalres.Typ.Scale))
			require.Equal(t, c.wantType, decimalres.Typ)
			require.Equal(t, c.wantScalar, decimalres.IsScalar())
		})
	}
}
