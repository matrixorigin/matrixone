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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSerialExtractPreservesEnumDomain(t *testing.T) {
	original := planpb.Type{
		Id:            int32(types.T_enum),
		Enumvalues:    "2#3:a,b1:c",
		EnumEncoding:  types.EnumEncodingFramed,
		EnumCollation: uint32(types.EnumCollationUTF8Bin),
	}
	source := GetColExpr(planpb.Type{Id: int32(types.T_varchar)}, 0, 0)
	got, err := MakeSerialExtractExpr(t.Context(), source, original, 0)
	require.NoError(t, err)
	require.Equal(t, original.Enumvalues, got.Typ.Enumvalues)
	require.Equal(t, original.EnumEncoding, got.Typ.EnumEncoding)
	require.Equal(t, original.EnumCollation, got.Typ.EnumCollation)
}

func TestSerialExtractBinderPreservesEnumDomain(t *testing.T) {
	original := planpb.Type{
		Id:            int32(types.T_enum),
		Enumvalues:    "2#3:a,b1:c",
		EnumEncoding:  types.EnumEncodingFramed,
		EnumCollation: uint32(types.EnumCollationUTF8Bin),
	}
	got, err := BindFuncExprImplByPlanExpr(t.Context(), "serial_extract", []*planpb.Expr{
		GetColExpr(planpb.Type{Id: int32(types.T_varchar)}, 0, 0),
		makePlan2Int64ConstExprWithType(0),
		{Typ: original, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
	})
	require.NoError(t, err)
	require.Equal(t, original.Enumvalues, got.Typ.Enumvalues)
	require.Equal(t, original.EnumEncoding, got.Typ.EnumEncoding)
	require.Equal(t, original.EnumCollation, got.Typ.EnumCollation)
}

func TestSerialExtractExecutorPreservesEnumIdentity(t *testing.T) {
	for _, tc := range []struct {
		name      string
		collation uint32
	}{{"binary", types.EnumCollationUTF8Bin}, {"general_ci", types.EnumCollationGeneralCI}} {
		t.Run(tc.name, func(t *testing.T) {
			original := planpb.Type{
				Id: int32(types.T_enum), Enumvalues: "2#3:a,b1:c",
				EnumEncoding: types.EnumEncodingFramed, EnumCollation: tc.collation,
			}
			proc := testutil.NewProcess(t)
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			packer := types.NewPacker()
			defer packer.Close()
			packer.EncodeEnum(2)
			require.NoError(t, vector.AppendBytes(input.Vecs[0], packer.Bytes(), false, proc.Mp()))
			input.SetRowCount(1)

			target := &planpb.Expr{Typ: original, Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}
			targetExecutor, err := colexec.NewExpressionExecutor(proc, target)
			require.NoError(t, err)
			t.Cleanup(targetExecutor.Free)
			targetResult, err := targetExecutor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, original.EnumEncoding, targetResult.GetType().GetEnumEncoding())
			require.Equal(t, original.EnumCollation, targetResult.GetType().GetEnumCollation())

			source := GetColExpr(planpb.Type{Id: int32(types.T_varchar)}, 0, 0)
			expr, err := MakeSerialExtractExpr(t.Context(), source, original, 0)
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, original.EnumEncoding, result.GetType().GetEnumEncoding())
			require.Equal(t, original.EnumCollation, result.GetType().GetEnumCollation())
			require.Equal(t, []types.Enum{2}, vector.MustFixedColWithTypeCheck[types.Enum](result))
		})
	}
}
