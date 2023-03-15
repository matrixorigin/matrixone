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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestUnary(t *testing.T) {
	input := testutil.MakeInt64Vector([]int64{5, -5, 0}, []uint64{2})
	testProc := testutil.NewProc()
	output, err := UnaryTilde[int64]([]*vector.Vector{input}, testProc)
	require.NoError(t, err)
	expected := testutil.MakeUint64Vector([]uint64{18446744073709551610, 4, 0}, []uint64{2})
	require.True(t, testutil.CompareVectors(expected, output), "got vector is different with expected")
}

func TestUnaryMinus(t *testing.T) {
	input := testutil.MakeInt64Vector([]int64{123, 234, 345, 0}, []uint64{3})
	testProc := testutil.NewProc()
	output, err := UnaryMinus[int64]([]*vector.Vector{input}, testProc)
	require.NoError(t, err)
	expected := testutil.MakeInt64Vector([]int64{-123, -234, -345, 0}, []uint64{3})
	require.True(t, testutil.CompareVectors(expected, output), "got vector is different with expected")
}

func TestUnaryMinusDecimal64(t *testing.T) {
	input := testutil.MakeDecimal64Vector([]int64{123, 234, 345, 0}, []uint64{3}, testutil.MakeDecimal64Type(6, 2))
	testProc := testutil.NewProc()
	output, err := UnaryMinusDecimal64([]*vector.Vector{input}, testProc)
	require.NoError(t, err)
	outputCol := vector.MustFixedCol[types.Decimal64](output)
	expectedCol := []types.Decimal64{types.Decimal64(123).Minus(), types.Decimal64(234).Minus(), types.Decimal64(345).Minus(), types.Decimal64(0)}
	for i, c := range outputCol {
		require.True(t, c.Compare(expectedCol[i]) == 0)
	}
}

func TestUnaryMinusDecimal128(t *testing.T) {
	input := testutil.MakeDecimal128Vector([]int64{123, 234, 345, 0}, []uint64{3}, testutil.MakeDecimal128Type(38, 10))
	testProc := testutil.NewProc()
	output, err := UnaryMinusDecimal128([]*vector.Vector{input}, testProc)
	require.NoError(t, err)
	outputCol := vector.MustFixedCol[types.Decimal128](output)
	expectedCol := []types.Decimal128{types.Decimal128{B0_63: 123, B64_127: 0}.Minus(), types.Decimal128{B0_63: 234, B64_127: 0}.Minus(), types.Decimal128{B0_63: 345, B64_127: 0}.Minus(), {B0_63: 0, B64_127: 0}}
	for i, c := range outputCol {
		require.True(t, c.Compare(expectedCol[i]) == 0)
	}
}
