// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// issue #30729: an unsized CAST(v AS BINARY) produces a T_binary(-1, -1)
// branch. When the other IF value branch is an untyped NULL (T_any), the
// binary+T_any early returns in both IF admission ladders must preserve the
// binary branch's width and scale exactly. Clamping the negative width to
// MaxVarBinaryLen (65535) turns the inserted branch CAST into BINARY(65535),
// which explicitly pads the selected bytes with 65525 NULs: LENGTH() reports
// 65535 instead of the preserved 10 for VARCHAR(20) 'hello-long', and
// CTAS-readback stores the padded bytes.
//
// Both compatibility ladders are pinned: the default MySQL-contract statement
// (iffCheckTypes) and the native-mode legacy statement
// (legacyIffBooleanCheck via the D11 gate), because the two checkers carry a
// copy of the same early return.
func TestIfUnsizedBinaryUntypedNullPreservesBranchType(t *testing.T) {
	mysqlCtx := context.Background()
	nativeCtx := WithMySQLConditionalBooleanContracts(context.Background(), false)
	condition := types.T_bool.ToType()
	unsizedBinary := types.New(types.T_binary, -1, -1)
	untypedNull := types.T_any.ToType()

	for _, ladder := range []struct {
		name string
		ctx  context.Context
	}{
		{"mysql contracts", mysqlCtx},
		{"native legacy", nativeCtx},
	} {
		for _, order := range []struct {
			name string
			args []types.Type
		}{
			{"null in else", []types.Type{condition, unsizedBinary, untypedNull}},
			{"null in then", []types.Type{condition, untypedNull, unsizedBinary}},
		} {
			t.Run(ladder.name+"/"+order.name, func(t *testing.T) {
				res, err := GetFunctionByName(ladder.ctx, "if", order.args)
				require.NoError(t, err, "IF must admit binary + untyped NULL value branches")

				targets, needCast := res.ShouldDoImplicitTypeCast()
				require.True(t, needCast, "the untyped NULL branch still widens to the binary target")
				require.Len(t, targets, 3)
				require.Equal(t, types.T_bool, targets[0].Oid)
				require.Equal(t, types.T_binary, targets[1].Oid)
				require.Equal(t, int32(-1), targets[1].Width,
					"unsized CAST AS BINARY must keep width -1, not clamp to 65535 (#30729)")
				require.Equal(t, int32(-1), targets[1].Scale)
				require.Equal(t, targets[1], targets[2])

				ret := res.GetReturnType()
				require.Equal(t, types.T_binary, ret.Oid)
				require.Equal(t, int32(-1), ret.Width,
					"the projected IF column must keep the unsized binary width")
				require.Equal(t, int32(-1), ret.Scale)
			})
		}
	}
}

// Controls: explicit BINARY(n) and T_varbinary branches keep their declared
// width next to an untyped NULL in both ladders, so the fixed-width
// truncate/pad semantics of explicit CAST(v AS BINARY(n)) stay untouched by
// the #30729 closure.
func TestIfBinaryUntypedNullKeepsExplicitWidth(t *testing.T) {
	mysqlCtx := context.Background()
	nativeCtx := WithMySQLConditionalBooleanContracts(context.Background(), false)
	condition := types.T_bool.ToType()
	untypedNull := types.T_any.ToType()
	fixedBinary := types.New(types.T_binary, 3, -1)
	varbinaryCol := types.New(types.T_varbinary, 100, 0)

	for _, tc := range []struct {
		name      string
		args      []types.Type
		wantWidth int32
	}{
		{"binary(3)", []types.Type{condition, fixedBinary, untypedNull}, 3},
		{"binary(3) null first", []types.Type{condition, untypedNull, fixedBinary}, 3},
		{"varbinary column", []types.Type{condition, varbinaryCol, untypedNull}, 100},
		{"varbinary column null first", []types.Type{condition, untypedNull, varbinaryCol}, 100},
	} {
		for _, ladder := range []struct {
			name string
			ctx  context.Context
		}{
			{"mysql contracts", mysqlCtx},
			{"native legacy", nativeCtx},
		} {
			t.Run(tc.name+"/"+ladder.name, func(t *testing.T) {
				res, err := GetFunctionByName(ladder.ctx, "if", tc.args)
				require.NoError(t, err)
				targets, needCast := res.ShouldDoImplicitTypeCast()
				require.True(t, needCast)
				require.Len(t, targets, 3)
				require.Equal(t, tc.wantWidth, targets[1].Width)
				require.Equal(t, tc.wantWidth, targets[2].Width)
				require.Equal(t, tc.wantWidth, res.GetReturnType().Width)
			})
		}
	}
}

// Control for the sibling IF shape the early return never covered: two
// concrete binary value branches resolve through binaryStringCommonType and
// must keep the unsized width -1 (no 65535 clamp) before and after the
// #30729 fix.
func TestIfTwoUnsizedBinaryValueBranchesUnchanged(t *testing.T) {
	condition := types.T_bool.ToType()
	branchA := types.New(types.T_binary, -1, -1)
	branchB := types.New(types.T_binary, -1, -1)
	res, err := GetFunctionByName(context.Background(), "if",
		[]types.Type{condition, branchA, branchB})
	require.NoError(t, err)
	targets, needCast := res.ShouldDoImplicitTypeCast()
	require.True(t, needCast)
	require.Len(t, targets, 3)
	require.Equal(t, types.T_binary, targets[1].Oid)
	require.Equal(t, int32(-1), targets[1].Width)
	require.Equal(t, int32(-1), targets[2].Width)
	require.Equal(t, int32(-1), res.GetReturnType().Width)
}

// Operator-level guard for the #30729 consumer chain: strIffFn is the string
// overload runtime for binary IF results. It must copy the selected branch
// bytes unchanged (10 bytes for 'hello-long', no synthetic NUL padding) and
// surface the NULL branch as NULL, so the operator itself is provably not the
// padding producer while the binder-level fix changes the inserted CASTs.
func TestStrIffFnBinarySelectsBranchBytes(t *testing.T) {
	proc := testutil.NewProcess(t)
	mp := proc.Mp()
	binType := types.New(types.T_binary, -1, -1)

	condVec := vector.NewVec(types.T_bool.ToType())
	require.NoError(t, vector.AppendFixedList(condVec, []bool{true, false}, nil, mp))
	thenVec := testutil.MakeVarlenaVector([][]byte{[]byte("hello-long"), []byte("hello-long")}, nil, binType, mp)
	elseVec := testutil.MakeVarlenaVector([][]byte{nil, nil}, []uint64{0, 1}, binType, mp)

	result := vector.NewFunctionResultWrapper(binType, mp)
	t.Cleanup(func() {
		condVec.Free(mp)
		thenVec.Free(mp)
		elseVec.Free(mp)
		result.Free()
	})
	require.NoError(t, result.PreExtendAndReset(2))
	require.NoError(t, strIffFn([]*vector.Vector{condVec, thenVec, elseVec}, result, proc, 2, nil))

	// cond row 0 is true -> the then bytes survive unchanged; row 1 is false
	// -> the all-null else branch surfaces as NULL.
	out := result.GetResultVector()
	require.Equal(t, types.T_binary, out.GetType().Oid)
	require.Equal(t, int32(-1), out.GetType().Width)
	require.False(t, out.IsNull(0))
	got := out.GetBytesAt(0)
	require.Len(t, got, 10, "selected bytes must not be padded: %d bytes", len(got))
	require.Equal(t, []byte("hello-long"), got)
	require.True(t, out.IsNull(1))
}
