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

package plan

import (
	"hash/crc32"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// A new JSON binding must hash complete visible JSON text while an already
// bound legacy expression still hashes the stored binary representation. The
// function identity assertion separately checks that the fix uses CRC32's
// existing signature; neither assertion can stand in for the other.
func TestCRC32JSONTextWithExistingFunctionIdentity(t *testing.T) {
	proc := newPlanTestProcess(t)
	defer proc.Free()
	legacy := legacyCRC32Expr()
	fresh, err := BindFuncExprImplByPlanExpr(proc.Ctx, "crc32", []*Expr{DeepCopyExpr(legacy.GetF().Args[0])})
	require.NoError(t, err)

	input := batch.NewWithSize(1)
	defer input.Clean(proc.Mp())
	input.Vecs[0] = vector.NewVec(types.T_json.ToType())
	texts := []string{
		`{"t1": "a"}`,
		`"x"`,
		`{"a": 1, "b": 2}`,
		`[1, true, "x"]`,
		`1`,
		`true`,
		`false`,
		`null`,
		`"` + strings.Repeat("x", 65532) + `"`,
		`"` + strings.Repeat("x", 65533) + `"`,
		`"` + strings.Repeat("x", 65534) + `"`,
		`"` + strings.Repeat("x", 131070) + `"`,
	}
	for _, text := range texts {
		value, err := types.ParseStringToByteJson(text)
		require.NoError(t, err)
		encoded, err := types.EncodeJson(value)
		require.NoError(t, err)
		require.NoError(t, vector.AppendBytes(input.Vecs[0], encoded, false, proc.Mp()))
	}
	require.NoError(t, vector.AppendBytes(input.Vecs[0], nil, true, proc.Mp()))
	input.SetRowCount(len(texts) + 1)

	legacyExec, err := colexec.NewExpressionExecutor(proc, legacy)
	require.NoError(t, err)
	defer legacyExec.Free()
	legacyResult, err := legacyExec.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3719146973), vector.GetFixedAtNoTypeCheck[uint64](legacyResult, 0))
	require.True(t, legacyResult.IsNull(uint64(len(texts))))

	// A prepared DML path can replace the legacy column reference with a
	// parameter before rebinding. Preserving only the CRC32 ID is insufficient
	// if that rebinding also inserts a text conversion under the old call.
	legacyParam := legacyCRC32Expr()
	legacyParam.GetF().Args[0] = &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_json)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	rule := NewResetParamRefRule(proc.Ctx, []*Expr{DeepCopyExpr(legacy.GetF().Args[0])})
	rebound, err := rule.ApplyExpr(legacyParam)
	require.NoError(t, err)
	reboundExec, err := colexec.NewExpressionExecutor(proc, rebound)
	require.NoError(t, err)
	defer reboundExec.Free()
	reboundResult, err := reboundExec.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3719146973), vector.GetFixedAtNoTypeCheck[uint64](reboundResult, 0))

	freshExec, err := colexec.NewExpressionExecutor(proc, fresh)
	require.NoError(t, err)
	defer freshExec.Free()
	result, err := freshExec.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	for i, text := range texts {
		require.False(t, result.IsNull(uint64(i)))
		require.Equal(t, uint64(crc32.ChecksumIEEE([]byte(text))), vector.GetFixedAtNoTypeCheck[uint64](result, i), "row %d", i)
	}
	require.True(t, result.IsNull(uint64(len(texts))))
	require.NotNil(t, fresh.GetF())
	require.Equal(t, function.EncodeOverloadID(function.CRC32, 0), fresh.GetF().Func.Obj,
		"new binding must not add a CRC32 overload")
	_, err = function.GetFunctionById(proc.Ctx, function.EncodeOverloadID(function.CRC32, 1))
	require.Error(t, err, "the CRC32 function must not register an additional overload")

	// A selected-out row with invalid stored JSON must not be evaluated by
	// the real CAST -> CRC32 expression, while evaluating it must report error.
	stored, isNull := vector.GenerateFunctionStrParameter(input.Vecs[0]).GetStrValue(0)
	require.False(t, isNull)
	stored[0] = 0xff
	selection := make([]bool, len(texts)+1)
	for i := range selection {
		selection[i] = true
	}
	selection[0] = false
	masked, err := freshExec.Eval(proc, []*batch.Batch{input}, selection)
	require.NoError(t, err)
	require.True(t, masked.IsNull(0))
	require.Equal(t, uint64(crc32.ChecksumIEEE([]byte(texts[1]))), vector.GetFixedAtNoTypeCheck[uint64](masked, 1))
	_, err = freshExec.Eval(proc, []*batch.Batch{input}, nil)
	require.Error(t, err)
}

// This is a typed rebinding test, not a substitute for SQL/binary PREPARE
// execution. The same initially unresolved marker must accept JSON, text,
// binary bytes and SQL NULL without changing the cached expression.
func TestCRC32UntypedPreparedRebindingPreservesDomains(t *testing.T) {
	proc := newPlanTestProcess(t)
	defer proc.Free()
	marker := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_any)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	prepared, err := BindFuncExprImplByPlanExpr(proc.Ctx, "crc32", []*Expr{marker})
	require.NoError(t, err)
	original, err := prepared.Marshal()
	require.NoError(t, err)
	jsonValue, err := types.ParseStringToByteJson(`{"t1":"a"}`)
	require.NoError(t, err)
	jsonBytes, err := types.EncodeJson(jsonValue)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		typ    types.T
		value  []byte
		isNull bool
		want   uint64
	}{
		{name: "json", typ: types.T_json, value: jsonBytes, want: 4012824821},
		{name: "text", typ: types.T_varchar, value: []byte("abc"), want: uint64(crc32.ChecksumIEEE([]byte("abc")))},
		{name: "binary", typ: types.T_varbinary, value: []byte{0, 0xff}, want: 1826356594},
		{name: "null", typ: types.T_json, isNull: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arg := &planpb.Expr{Typ: planpb.Type{Id: int32(tc.typ)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			rule := NewResetParamRefRule(proc.Ctx, []*Expr{arg})
			bound, err := rule.ApplyExpr(DeepCopyExpr(prepared))
			require.NoError(t, err)
			input := batch.NewWithSize(1)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(tc.typ.ToType())
			require.NoError(t, vector.AppendBytes(input.Vecs[0], tc.value, tc.isNull, proc.Mp()))
			input.SetRowCount(1)
			exec, err := colexec.NewExpressionExecutor(proc, bound)
			require.NoError(t, err)
			defer exec.Free()
			result, err := exec.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.isNull, result.IsNull(0))
			if !tc.isNull {
				require.Equal(t, tc.want, vector.GetFixedAtNoTypeCheck[uint64](result, 0))
			}
			require.NotNil(t, bound.GetF())
			require.Equal(t, function.EncodeOverloadID(function.CRC32, 0), bound.GetF().Func.Obj)
		})
	}
	after, err := prepared.Marshal()
	require.NoError(t, err)
	require.Equal(t, original, after, "execute-time rebinding must not mutate the prepared expression")
}
