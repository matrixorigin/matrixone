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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestStatementDigestFunctions(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int
		text bool
		want string
	}{
		{"statement_digest", STATEMENT_DIGEST, false, "d1b44b0c19af710b5a679907e284acd2ddc285201794bc69a2389d77baedddae"},
		{"statement_digest_text", STATEMENT_DIGEST_TEXT, true, "SELECT ?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			registration := statementDigestFunction(tc.id, tc.text)
			op := registration.Overloads[0]
			require.True(t, op.IsVolatile())
			require.True(t, op.executionStable)
			require.False(t, DeduceNotNullableForBinding(proc.Ctx, EncodeOverloadID(int32(tc.id), 0),
				[]*plan.Expr{{Typ: plan.Type{Id: int32(types.T_varchar), NotNullable: true}}}))
			for _, oid := range []types.T{types.T_varchar, types.T_char, types.T_text, types.T_varbinary, types.T_blob, types.T_int64} {
				_, err := GetFunctionByName(proc.Ctx, tc.name, []types.Type{oid.ToType()})
				require.NoError(t, err)
			}
			_, err := GetFunctionByName(proc.Ctx, tc.name, nil)
			require.Error(t, err)
			_, err = GetFunctionByName(proc.Ctx, tc.name, []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()})
			require.Error(t, err)

			result := vector.NewFunctionResultWrapper(op.retType(nil), proc.Mp())
			defer result.Free()
			input := testutil.NewVectorWithNulls(4, types.T_varchar.ToType(), proc.Mp(), false,
				[]bool{false, false, true, false}, []string{"SELECT 1", "invalid SQL", "also invalid", "SELECT 2"})
			defer input.Free(proc.Mp())
			call := op.newOp()
			require.NoError(t, result.PreExtendAndReset(4))
			mask := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true, true}}
			require.NoError(t, call([]*vector.Vector{input}, result, proc, 4, mask))
			require.Equal(t, tc.want, result.GetResultVector().GetStringAt(0))
			require.True(t, result.GetResultVector().IsNull(1))
			require.True(t, result.GetResultVector().IsNull(2))
			require.Equal(t, tc.want, result.GetResultVector().GetStringAt(3))
			require.NoError(t, result.PreExtendAndReset(4))
			require.Error(t, call([]*vector.Vector{input}, result, proc, 4, nil))
			require.NoError(t, result.PreExtendAndReset(4))
			require.NoError(t, call([]*vector.Vector{input}, result, proc, 4, &FunctionSelectList{AllNull: true}))
			require.NoError(t, result.PreExtendAndReset(0))
			require.NoError(t, call([]*vector.Vector{input}, result, proc, 0, nil))

			constant, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("SELECT 1"), 3, proc.Mp())
			require.NoError(t, err)
			defer constant.Free(proc.Mp())
			require.NoError(t, result.PreExtendAndReset(3))
			require.NoError(t, call([]*vector.Vector{constant}, result, proc, 3, nil))
			for i := range 3 {
				require.Equal(t, tc.want, result.GetResultVector().GetStringAt(i))
			}
			null := vector.NewConstNull(types.T_varchar.ToType(), 3, proc.Mp())
			defer null.Free(proc.Mp())
			require.NoError(t, result.PreExtendAndReset(3))
			require.NoError(t, call([]*vector.Vector{null}, result, proc, 3, nil))
			require.True(t, result.GetResultVector().IsNull(0))
		})
	}
}

func TestStatementDigestExecutionSQLMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte(`SELECT "a"`), 1, proc.Mp())
	require.NoError(t, err)
	defer input.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(types.T_text.ToType(), proc.Mp())
	defer result.Free()
	call := statementDigestFunction(STATEMENT_DIGEST_TEXT, true).Overloads[0].newOp()
	mode := "ANSI_QUOTES"
	proc.Base.SessionInfo.SqlMode = mode
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return mode, nil })
	for _, tc := range []struct{ mode, want string }{
		{"ANSI_QUOTES", "SELECT `a`"},
		{process.EmptySqlModeSentinel, "SELECT ?"},
		{"", "SELECT `a`"}, // Empty background resolver must not erase the CN snapshot.
	} {
		mode = tc.mode
		require.NoError(t, result.PreExtendAndReset(1))
		require.NoError(t, call([]*vector.Vector{input}, result, proc, 1, nil))
		require.Equal(t, tc.want, result.GetResultVector().GetStringAt(0))
	}
	proc.Base.IsFrontend = true
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, call([]*vector.Vector{input}, result, proc, 1, nil))
	require.Equal(t, "SELECT ?", result.GetResultVector().GetStringAt(0))
	proc.SetResolveVariableFunc(nil)
	require.NoError(t, result.PreExtendAndReset(1))
	require.NoError(t, call([]*vector.Vector{input}, result, proc, 1, nil))
	require.Equal(t, "SELECT `a`", result.GetResultVector().GetStringAt(0))
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx
	require.NoError(t, result.PreExtendAndReset(1))
	require.ErrorIs(t, call([]*vector.Vector{input}, result, proc, 1, nil), context.Canceled)
}
