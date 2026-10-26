// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// MySQL evaluates BIT as a number in a boolean context, so the logical
// operators must bind a BIT operand by casting it to BOOL.
func TestLogicalOperatorsAcceptBitOperand(t *testing.T) {
	ctx := context.Background()
	bit8 := types.New(types.T_bit, 8, 0)
	boolType := types.T_bool.ToType()
	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{"and", []types.Type{bit8, boolType}},
		{"and", []types.Type{boolType, bit8}},
		{"and", []types.Type{bit8, bit8}},
		{"or", []types.Type{bit8, boolType}},
		{"xor", []types.Type{bit8, boolType}},
		{"not", []types.Type{bit8}},
	} {
		resolved, err := GetFunctionByName(ctx, tc.name, tc.args)
		require.NoError(t, err, tc.name)
		require.True(t, resolved.needCast, tc.name)
		for i := range tc.args {
			require.Equal(t, types.T_bool, resolved.targetTypes[i].Oid, tc.name)
		}
	}
}

// BIT -> BOOL keeps MySQL truthiness: any non-zero value is TRUE.
func TestBitToBoolCastTruthiness(t *testing.T) {
	proc := testutil.NewProcess(t)
	fc := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.New(types.T_bit, 8, 0), []uint64{0, 1, 128, 0}, []bool{false, false, false, true}),
			NewFunctionTestInput(types.T_bool.ToType(), []bool{}, nil),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false, true, true, false}, []bool{false, false, false, true}),
		NewCast)
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
}
