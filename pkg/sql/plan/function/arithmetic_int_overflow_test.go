// Copyright 2026 Matrix Origin
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

package function

import (
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func requireOutOfRange(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange),
		"expected out-of-range error, got: %v", err)
}

func checkSignedBoundaries[T signedInt](t *testing.T, minVal, maxVal T) {
	ctx := context.Background()

	// add: normal, positive overflow, negative overflow
	r, err := addSignedWithOverflowCheck(ctx, maxVal-1, 1)
	require.NoError(t, err)
	require.Equal(t, maxVal, r)
	_, err = addSignedWithOverflowCheck(ctx, maxVal, 1)
	requireOutOfRange(t, err)
	_, err = addSignedWithOverflowCheck(ctx, minVal, -1)
	requireOutOfRange(t, err)

	// sub: normal, negative overflow, positive overflow, and 0 - MinInt
	r, err = subSignedWithOverflowCheck(ctx, minVal+1, 1)
	require.NoError(t, err)
	require.Equal(t, minVal, r)
	_, err = subSignedWithOverflowCheck(ctx, minVal, 1)
	requireOutOfRange(t, err)
	_, err = subSignedWithOverflowCheck(ctx, maxVal, -1)
	requireOutOfRange(t, err)
	_, err = subSignedWithOverflowCheck(ctx, 0, minVal)
	requireOutOfRange(t, err)
	r, err = subSignedWithOverflowCheck(ctx, 0, maxVal)
	require.NoError(t, err)
	require.Equal(t, minVal+1, r)

	// mul: zero, normal, MinInt * -1 on both sides, round-trip overflow
	r, err = mulSignedWithOverflowCheck(ctx, minVal, 0)
	require.NoError(t, err)
	require.Equal(t, T(0), r)
	r, err = mulSignedWithOverflowCheck(ctx, maxVal, -1)
	require.NoError(t, err)
	require.Equal(t, minVal+1, r)
	_, err = mulSignedWithOverflowCheck(ctx, minVal, -1)
	requireOutOfRange(t, err)
	_, err = mulSignedWithOverflowCheck(ctx, -1, minVal)
	requireOutOfRange(t, err)
	_, err = mulSignedWithOverflowCheck(ctx, maxVal, 2)
	requireOutOfRange(t, err)
	_, err = mulSignedWithOverflowCheck(ctx, maxVal/2+1, 2)
	requireOutOfRange(t, err)
}

func checkUnsignedBoundaries[T unsignedInt](t *testing.T, maxVal T) {
	ctx := context.Background()

	// add: normal, overflow
	r, err := addUnsignedWithOverflowCheck(ctx, maxVal-1, 1)
	require.NoError(t, err)
	require.Equal(t, maxVal, r)
	_, err = addUnsignedWithOverflowCheck(ctx, maxVal, 1)
	requireOutOfRange(t, err)

	// sub: normal, underflow
	r, err = subUnsignedWithOverflowCheck(ctx, maxVal, maxVal)
	require.NoError(t, err)
	require.Equal(t, T(0), r)
	_, err = subUnsignedWithOverflowCheck(ctx, T(0), T(1))
	requireOutOfRange(t, err)

	// mul: zero, normal, overflow
	r, err = mulUnsignedWithOverflowCheck(ctx, maxVal, 0)
	require.NoError(t, err)
	require.Equal(t, T(0), r)
	r, err = mulUnsignedWithOverflowCheck(ctx, maxVal/2, 2)
	require.NoError(t, err)
	require.Equal(t, maxVal-1, r)
	_, err = mulUnsignedWithOverflowCheck(ctx, maxVal/2+1, 2)
	requireOutOfRange(t, err)
	_, err = mulUnsignedWithOverflowCheck(ctx, maxVal, maxVal)
	requireOutOfRange(t, err)
}

func TestSignedArithmeticOverflowBoundaries(t *testing.T) {
	checkSignedBoundaries[int8](t, math.MinInt8, math.MaxInt8)
	checkSignedBoundaries[int16](t, math.MinInt16, math.MaxInt16)
	checkSignedBoundaries[int32](t, math.MinInt32, math.MaxInt32)
	checkSignedBoundaries[int64](t, math.MinInt64, math.MaxInt64)
}

func TestUnsignedArithmeticOverflowBoundaries(t *testing.T) {
	checkUnsignedBoundaries[uint8](t, math.MaxUint8)
	checkUnsignedBoundaries[uint16](t, math.MaxUint16)
	checkUnsignedBoundaries[uint32](t, math.MaxUint32)
	checkUnsignedBoundaries[uint64](t, math.MaxUint64)
}

func TestIntegerTypeNameInErrors(t *testing.T) {
	ctx := context.Background()
	_, err := addSignedWithOverflowCheck[int8](ctx, math.MaxInt8, 1)
	require.ErrorContains(t, err, "int8")
	_, err = addSignedWithOverflowCheck[int16](ctx, math.MaxInt16, 1)
	require.ErrorContains(t, err, "int16")
	_, err = addSignedWithOverflowCheck[int32](ctx, math.MaxInt32, 1)
	require.ErrorContains(t, err, "int32")
	_, err = addSignedWithOverflowCheck[int64](ctx, math.MaxInt64, 1)
	require.ErrorContains(t, err, "int64")
	_, err = addUnsignedWithOverflowCheck[uint8](ctx, math.MaxUint8, 1)
	require.ErrorContains(t, err, "uint8")
	_, err = addUnsignedWithOverflowCheck[uint16](ctx, math.MaxUint16, 1)
	require.ErrorContains(t, err, "uint16")
	_, err = addUnsignedWithOverflowCheck[uint32](ctx, math.MaxUint32, 1)
	require.ErrorContains(t, err, "uint32")
	_, err = addUnsignedWithOverflowCheck[uint64](ctx, math.MaxUint64, 1)
	require.ErrorContains(t, err, "uint64")
}
