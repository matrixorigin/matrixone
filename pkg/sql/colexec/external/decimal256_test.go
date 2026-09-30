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

package external

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func decimal256ExternalParam(format, jsonData string, typ plan.Type) *ExternalParam {
	return &ExternalParam{
		ExParamConst: ExParamConst{
			Ctx: context.Background(),
			Attrs: []plan.ExternAttr{{
				ColName:       "d",
				ColIndex:      0,
				ColFieldIndex: 0,
			}},
			Cols:          []*plan.ColDef{{Name: "d", Typ: typ}},
			ColumnListLen: 1,
			StrictSqlMode: true,
			Extern: &tree.ExternParam{
				ExParamConst: tree.ExParamConst{Format: format, Tail: &tree.TailParameter{}},
				ExParam: tree.ExParam{
					ExternType: int32(plan.ExternType_EXTERNAL_TB),
					JsonData:   jsonData,
				},
			},
		},
		ExParam: ExParam{Fileparam: &ExFileparam{FileCnt: 1, Filepath: "/tmp/decimal256.dat"}},
	}
}

func TestGetColDataDecimal256PrecisionAndScale(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	cases := []struct {
		name  string
		width int32
		scale int32
		value string
	}{
		{
			name:  "precision 39",
			width: 39,
			scale: 2,
			value: "1234567890123456789012345678901234567.89",
		},
		{
			name:  "precision 40 negative",
			width: 40,
			scale: 3,
			value: "-1234567890123456789012345678901234567.123",
		},
		{
			name:  "precision 50",
			width: 50,
			scale: 10,
			value: "1234567890123456789012345678901234567890.1234567890",
		},
		{
			name:  "precision 76",
			width: 76,
			scale: 5,
			value: "12345678901234567890123456789012345678901234567890123456789012345678901.12345",
		},
		{
			name:  "scale rounds half up",
			width: 10,
			scale: 2,
			value: "-1.235",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ := plan.Type{Id: int32(types.T_decimal256), Width: tc.width, Scale: tc.scale}
			param := decimal256ExternalParam(tree.CSV, "", typ)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal256, tc.width, tc.scale))
			defer bat.Clean(proc.Mp())

			err := getColData(
				bat,
				[]csvparser.Field{{Val: tc.value}},
				0,
				param,
				proc.Mp(),
				param.Attrs[0],
				proc,
			)
			require.NoError(t, err)

			want, err := types.ParseDecimal256(tc.value, tc.width, tc.scale)
			require.NoError(t, err)
			require.Equal(t, []types.Decimal256{want}, vector.MustFixedColWithTypeCheck[types.Decimal256](bat.Vecs[0]))
		})
	}
}

func TestGetColDataDecimal256NullEmptyInvalidAndOverflow(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	typ := plan.Type{Id: int32(types.T_decimal256), Width: 39, Scale: 2}
	param := decimal256ExternalParam(tree.CSV, "", typ)

	t.Run("null", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal256, typ.Width, typ.Scale))
		defer bat.Clean(proc.Mp())

		err := getColData(bat, []csvparser.Field{{IsNull: true}}, 0, param, proc.Mp(), param.Attrs[0], proc)
		require.NoError(t, err)
		require.True(t, bat.Vecs[0].GetNulls().Contains(0))
	})

	t.Run("empty remains null", func(t *testing.T) {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal256, typ.Width, typ.Scale))
		defer bat.Clean(proc.Mp())

		err := getColData(bat, []csvparser.Field{{Val: ""}}, 0, param, proc.Mp(), param.Attrs[0], proc)
		require.NoError(t, err)
		require.True(t, bat.Vecs[0].GetNulls().Contains(0))
	})

	for _, value := range []string{
		"not-a-decimal",
		"12345678901234567890123456789012345678.90",
	} {
		t.Run(value, func(t *testing.T) {
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal256, typ.Width, typ.Scale))
			defer bat.Clean(proc.Mp())

			err := getColData(bat, []csvparser.Field{{Val: value}}, 0, param, proc.Mp(), param.Attrs[0], proc)
			require.Error(t, err)
			require.Contains(t, err.Error(), "invalid Decimal256 type")
		})
	}
}

func TestIsLegalLineDecimal256AndParallelBoundary(t *testing.T) {
	valid := "1234567890123456789012345678901234567.89"
	param := &tree.ExternParam{
		ExParamConst: tree.ExParamConst{Format: tree.CSV},
		ExParam:      tree.ExParam{Parallel: true},
	}
	cols := []*plan.ColDef{{
		Name: "d",
		Typ:  plan.Type{Id: int32(types.T_decimal256), Width: 39, Scale: 2},
	}}

	require.True(t, isLegalLine(param, cols, []csvparser.Field{{Val: valid}}))
	require.False(t, isLegalLine(param, cols, []csvparser.Field{{Val: "not-a-decimal"}}))
	require.False(t, isLegalLine(param, cols, []csvparser.Field{{Val: "12345678901234567890123456789012345678.90"}}))
	require.True(t, isLegalLine(param, cols, []csvparser.Field{{IsNull: true}}))
	require.True(t, isLoadNumericZeroFillType(types.T_decimal256))
	require.True(t, isLoadNumericAdjustedValueType(types.T_decimal256))
	require.False(t, isDirectParallelLoadType(types.T_decimal256))
	require.Equal(t, types.T_varchar, makeType(&cols[0].Typ, true).Oid)

	proc := testutil.NewProcess(t)
	defer proc.Free()
	parallelParam := decimal256ExternalParam(tree.CSV, "", cols[0].Typ)
	parallelParam.ParallelLoad = true
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	defer bat.Clean(proc.Mp())

	err := getColData(bat, []csvparser.Field{{Val: valid}}, 0, parallelParam, proc.Mp(), parallelParam.Attrs[0], proc)
	require.NoError(t, err)
	require.Equal(t, valid, string(bat.Vecs[0].GetBytesAt(0)))
}

func TestAppendLoadEmptyNumericZeroDecimal256(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	vec := vector.NewVec(types.New(types.T_decimal256, 50, 10))
	defer vec.Free(proc.Mp())

	require.NoError(t, appendLoadEmptyNumericZero(vec, types.T_decimal256, false, proc.Mp()))
	require.Equal(t, []types.Decimal256{{}}, vector.MustFixedColWithTypeCheck[types.Decimal256](vec))
	require.False(t, vec.GetNulls().Contains(0))
}

func TestDecimal256CSVAndJSONLineObjectArray(t *testing.T) {
	for _, tc := range []struct {
		name     string
		format   string
		jsonData string
		content  string
	}{
		{name: "csv", format: tree.CSV, content: "1234567890123456789012345678901234567.89\n"},
		{name: "json object", format: tree.JSONLINE, jsonData: tree.OBJECT, content: "{\"d\":\"1234567890123456789012345678901234567.89\"}\n"},
		{name: "json array", format: tree.JSONLINE, jsonData: tree.ARRAY, content: "[\"1234567890123456789012345678901234567.89\"]\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()

			typ := plan.Type{Id: int32(types.T_decimal256), Width: 39, Scale: 2}
			param := decimal256ExternalParam(tc.format, tc.jsonData, typ)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = vector.NewVec(types.New(types.T_decimal256, typ.Width, typ.Scale))
			defer bat.Clean(proc.Mp())

			err := readAllText(t, param, proc, bat, tc.content)
			require.NoError(t, err)
			require.Equal(t, 1, bat.RowCount())
			want, err := types.ParseDecimal256("1234567890123456789012345678901234567.89", typ.Width, typ.Scale)
			require.NoError(t, err)
			require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Decimal256](bat.Vecs[0], 0))
		})
	}
}
