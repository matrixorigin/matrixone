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

func TestUnaryMinusDecimal64(t *testing.T) {
	input := testutil.MakeDecimal64Vector([]int64{123, 234, 345, 0}, []uint64{3}, testutil.MakeDecimal64Type(6, 2))
	testProc := testutil.NewProc()
	output, err := UnaryMinusDecimal64([]*vector.Vector{input}, testProc)
	require.NoError(t, err)
	outputCol := vector.MustTCols[types.Decimal64](output)
	expectedCol := []types.Decimal64{types.Decimal64FromInt32(-123), types.Decimal64FromInt32(-234), types.Decimal64FromInt32(-345), types.Decimal64_Zero}
	for i, c := range outputCol {
		require.True(t, c.Eq(expectedCol[i]))
	}
}

func TestUnaryMinusDecimal128(t *testing.T) {
	input := testutil.MakeDecimal128Vector([]int64{123, 234, 345, 0}, []uint64{3}, testutil.MakeDecimal128Type(38, 10))
	testProc := testutil.NewProc()
	output, err := UnaryMinusDecimal128([]*vector.Vector{input}, testProc)
	require.NoError(t, err)
	outputCol := vector.MustTCols[types.Decimal128](output)
	expectedCol := []types.Decimal128{types.Decimal128FromInt32(-123), types.Decimal128FromInt32(-234), types.Decimal128FromInt32(-345), types.Decimal128_Zero}
	for i, c := range outputCol {
		require.True(t, c.Eq(expectedCol[i]))
	}
}
