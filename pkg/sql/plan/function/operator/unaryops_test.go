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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestUnaryMinusDecimal64(t *testing.T) {
	input := testutil.MakeDecimal64Vector([]int64{123, 234, 345, 0}, []uint64{3}, testutil.MakeDecimal64Type(6, 2))
	testProc := testutil.NewProc()
	output, err := UnaryMinusDecimal64([]*vector.Vector{input}, testProc)
	require.NoError(t, err)
	outputCol := vector.MustTCols[types.Decimal64](output)

	require.Equal(t, []types.Decimal64{-123, -234, -345, 0}, outputCol)
}

func TestUnaryMinusDecimal128(t *testing.T) {
	input := testutil.MakeDecimal128Vector([]uint64{123, 234, 345, 0}, []uint64{3}, testutil.MakeDecimal128Type(38, 10))
	testProc := testutil.NewProc()
	output, err := UnaryMinusDecimal128([]*vector.Vector{input}, testProc)
	require.NoError(t, err)
	outputCol := vector.MustTCols[types.Decimal128](output)

	require.Equal(t, []types.Decimal128{{Lo: -123, Hi: -1}, {Lo: -234, Hi: -1}, {Lo: -345, Hi: -1}, {Lo: 0, Hi: 0}}, outputCol)
}
