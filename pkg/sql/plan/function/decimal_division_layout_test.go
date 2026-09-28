// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDirectDecimalDivisionVectorRoundTrip(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal128, 38, 0)
	left, err := vector.NewConstFixed(typ, types.Decimal128{B0_63: 1}, 1, proc.Mp())
	require.NoError(t, err)
	defer left.Free(proc.Mp())
	right, err := vector.NewConstFixed(typ, types.Decimal128{B0_63: 3}, 1, proc.Mp())
	require.NoError(t, err)
	defer right.Free(proc.Mp())
	result, err := RunFunctionDirectly(proc, EncodeOverloadID(DIV, 0), []*vector.Vector{left, right}, 1)
	require.NoError(t, err)
	defer result.Free(proc.Mp())
	require.Equal(t, types.T_decimal128, result.GetType().Oid)
	require.Equal(t, int32(16), result.GetType().Size)
	data, err := result.MarshalBinary()
	require.NoError(t, err)
	decoded := vector.NewVec(typ)
	defer decoded.Free(proc.Mp())
	require.NoError(t, decoded.UnmarshalBinaryWithCopy(data, proc.Mp()))
	require.Equal(t, result.GetType(), decoded.GetType())
}

func TestLegacyDecimalDivisionPlanUsesTaggedScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputType := types.New(types.T_decimal64, 10, 2)
	left, err := vector.NewConstFixed(inputType, types.Decimal64(100), 1, proc.Mp())
	require.NoError(t, err)
	defer left.Free(proc.Mp())
	right, err := vector.NewConstFixed(inputType, types.Decimal64(300), 1, proc.Mp())
	require.NoError(t, err)
	defer right.Free(proc.Mp())

	// A v96 sender tags its DIV/0 result as DECIMAL128(38,8).
	// The upgraded executor must honor that tag for old-to-new RPCs.
	legacyType := types.New(types.T_decimal128, 38, 8)
	result := vector.NewFunctionResultWrapper(legacyType, proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))
	overload, err := GetFunctionById(proc.Ctx, EncodeOverloadID(DIV, 0))
	require.NoError(t, err)
	execute, _, _, _ := overload.GetExecuteMethod()
	require.NoError(t, execute([]*vector.Vector{left, right}, result, proc, 1, nil))
	require.Equal(t, legacyType, *result.GetResultVector().GetType())
	require.Equal(t, types.Decimal128{B0_63: 33333333}, vector.MustFixedColNoTypeCheck[types.Decimal128](result.GetResultVector())[0])
}
