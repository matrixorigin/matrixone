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
	"testing"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDecimalAssignmentChecksDeclaredPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	typ := types.New(types.T_decimal64, 5, 2)
	source := vector.NewVec(typ, proc.Mp())
	defer source.Free(proc.Mp())
	require.NoError(t, vector.AppendFixed(source, types.Decimal64(100000), false, proc.Mp()))
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "sql_mode" {
			return "STRICT_TRANS_TABLES", nil
		}
		return nil, nil
	})
	for _, width := range []int32{5, 6} {
		targetType := types.New(types.T_decimal64, width, 2)
		target := vector.NewConstNull(targetType, 1, proc.Mp())
		defer target.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(targetType, proc.Mp())
		defer result.Free()
		if width == 6 {
			// An invalid computed coefficient can overflow even a wider target.
			vector.MustFixedColNoTypeCheck[types.Decimal64](source)[0] = 1000000
		}
		for _, cast := range []executeLogicOfOverload{NewStrictCast, NewAssignCast} {
			require.NoError(t, result.PreExtendAndReset(1))
			require.Error(t, cast([]*vector.Vector{source, target}, result, proc, 1))
		}
	}
}

func TestDecimalAssignmentReportsScaleLoss(t *testing.T) {
	source, err := types.ParseDecimal128("123.456", 6, 3)
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		input FunctionTestInput
		want  types.Decimal64
	}{
		{"numeric source", NewFunctionTestInput(types.New(types.T_decimal128, 6, 3), []types.Decimal128{source}, nil), 1235},
		{"quoted source", NewFunctionTestInput(types.T_varchar.ToType(), []string{"12.34567"}, nil), 123},
		{"exact trailing zeros", NewFunctionTestInput(types.T_varchar.ToType(), []string{"12.30000"}, nil), 123},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				if name == "sql_mode" {
					return "", nil
				}
				return nil, nil
			})
			warnings := &numericWarningSession{}
			proc.WarningSink = warnings
			target := types.New(types.T_decimal64, 4, 1)
			fc := NewFunctionTestCase(proc, []FunctionTestInput{
				tc.input,
				NewFunctionTestInput(target, []types.Decimal64{0}, nil),
			}, NewFunctionTestResult(target, false, []types.Decimal64{tc.want}, nil), NewAssignCast)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
			if tc.name == "exact trailing zeros" {
				require.Empty(t, warnings.warnings)
			} else {
				require.Equal(t, []numericWarning{{code: moerr.WARN_DATA_TRUNCATED}},
					stripWarningMessages(warnings.warnings))
			}
		})
	}
}

func TestDecimalAssignmentScaleLossWithExponentAndTrailingZeros(t *testing.T) {
	for _, tc := range []struct {
		value string
		scale int32
		loss  bool
	}{
		{"123.456", 1, true},
		{"12.30000", 1, false},
		{"1.23e2", 1, false},
		{"1.2345e2", 1, true},
		{"1e-20", 1, true},
		{"0.000000", 1, false},
	} {
		require.Equal(t, tc.loss, DecimalAssignmentLosesScale(tc.value, tc.scale), tc.value)
	}
}

func TestDecimalAssignmentRangeAdjustsOutsideStrictMode(t *testing.T) {
	for _, tc := range []struct {
		name, sqlMode string
		ignore        bool
		wantError     bool
	}{
		{"strict", "STRICT_TRANS_TABLES", false, true},
		{"nonstrict", "", false, false},
		{"ignore", "STRICT_TRANS_TABLES", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				if name == "sql_mode" {
					return tc.sqlMode, nil
				}
				return nil, nil
			})
			warnings := &numericWarningSession{}
			proc.WarningSink = warnings
			sourceType := types.New(types.T_decimal64, 7, 2)
			targetType := types.New(types.T_decimal64, 5, 2)
			source := vector.NewVec(sourceType, proc.Mp())
			defer source.Free(proc.Mp())
			require.NoError(t, vector.AppendFixed(source, types.Decimal64(123456), false, proc.Mp()))
			target := vector.NewConstNull(targetType, 1, proc.Mp())
			defer target.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(targetType, proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))
			fn := NewAssignCast
			if tc.ignore {
				fn = NewAssignIgnoreCast
			}
			err := fn([]*vector.Vector{source, target}, result, proc, 1)
			if tc.wantError {
				require.Error(t, err)
				require.Empty(t, warnings.warnings)
				return
			}
			require.NoError(t, err)
			require.Equal(t, types.Decimal64(99999), vector.GetFixedAtNoTypeCheck[types.Decimal64](result.GetResultVector(), 0))
			require.Len(t, warnings.warnings, 1)
		})
	}
}

func TestQuotedDecimalAssignmentRangeAdjustsOutsideStrictMode(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "sql_mode" {
			return "", nil
		}
		return nil, nil
	})
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings
	targetType := types.New(types.T_decimal64, 5, 2)
	fc := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"1234.56"}, nil),
		NewFunctionTestInput(targetType, []types.Decimal64{0}, nil),
	}, NewFunctionTestResult(targetType, false, []types.Decimal64{99999}, nil), NewAssignCast)
	ok, info := fc.RunAndFree()
	require.True(t, ok, info)
	require.Len(t, warnings.warnings, 1)
}

func TestDecimalAssignmentWidening(t *testing.T) {
	proc := testutil.NewProcess(t)
	warnings := &numericWarningSession{}
	proc.WarningSink = warnings
	from := types.New(types.T_decimal64, 12, 2)
	to := types.New(types.T_decimal64, 14, 2)
	negative := types.Decimal64(999999999999).Minus()
	for _, tc := range []struct {
		name             string
		values, want     []types.Decimal64
		nulls, wantNulls []bool
	}{
		{"exact", []types.Decimal64{negative, 17, 999999999999}, []types.Decimal64{negative, 0, 999999999999},
			[]bool{false, true, false}, []bool{false, true, false}},
		{"outside source precision only", []types.Decimal64{1000000000000}, []types.Decimal64{1000000000000}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			test := NewFunctionTestCase(proc, []FunctionTestInput{
				NewFunctionTestInput(from, tc.values, tc.nulls),
				NewFunctionTestInput(to, []types.Decimal64{0}, nil),
			}, NewFunctionTestResult(to, false, tc.want, tc.wantNulls), NewAssignCast)
			ok, info := test.RunAndFree()
			require.True(t, ok, info)
			require.Empty(t, warnings.warnings)
		})
	}

	wide := types.New(types.T_decimal256, 40, 3)
	scaled, err := types.Decimal256FromDecimal128(types.Decimal128{B0_63: uint64(negative), B64_127: ^uint64(0)}).Scale(1)
	require.NoError(t, err)
	test := NewFunctionTestCase(proc, []FunctionTestInput{
		NewFunctionTestInput(from, []types.Decimal64{negative, 0}, []bool{false, true}),
		NewFunctionTestInput(wide, []types.Decimal256{{}}, nil),
	}, NewFunctionTestResult(wide, false, []types.Decimal256{scaled, {}}, []bool{false, true}), NewAssignCast)
	ok, info := test.RunAndFree()
	require.True(t, ok, info)
	require.Empty(t, warnings.warnings)
}

func BenchmarkDecimalAssignmentWidening(b *testing.B) {
	const n = 8192
	for _, tc := range []struct {
		name     string
		src, dst types.Type
	}{
		{"same_scale_wider", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 14, 2)},
		{"wider_scale", types.New(types.T_decimal64, 12, 2), types.New(types.T_decimal64, 14, 3)},
	} {
		for _, mode := range []struct {
			name string
			fn   executeLogicOfOverload
		}{{"ordinary_cast", NewCast}, {"assignment_cast", NewAssignCast}} {
			b.Run(tc.name+"/"+mode.name, func(b *testing.B) {
				proc := testutil.NewProcess(b)
				proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return "STRICT_TRANS_TABLES", nil })
				src := vector.NewVec(tc.src, proc.Mp())
				defer src.Free(proc.Mp())
				for i := 0; i < n; i++ {
					if err := vector.AppendFixed(src, types.Decimal64(i*101), false, proc.Mp()); err != nil {
						b.Fatal(err)
					}
				}
				target := vector.NewConstNull(tc.dst, n, proc.Mp())
				defer target.Free(proc.Mp())
				result := vector.NewFunctionResultWrapper(tc.dst, proc.Mp())
				defer result.Free()
				b.ReportAllocs()
				b.SetBytes(n * 8)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := result.PreExtendAndReset(n); err != nil {
						b.Fatal(err)
					}
					if err := mode.fn([]*vector.Vector{src, target}, result, proc, n); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				want := types.Decimal64(101)
				if tc.dst.Scale == 3 {
					want = 1010
				}
				if got := vector.GetFixedAtNoTypeCheck[types.Decimal64](result.GetResultVector(), 1); got != want {
					b.Fatalf("got %v want %v", got, want)
				}
			})
		}
	}
}

func TestDecimalAssignmentFromMaterializedByteString(t *testing.T) {
	for _, tc := range []struct {
		name, text string
		strict     bool
		wantCode   uint16
		wantValue  int64
	}{
		{"strict invalid bytes", "\x03\xe8", true, moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, 0},
		{"strict encoded radix", "0x10", true, moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, 0},
		{"strict decimal overflows", "1000", true, moerr.ER_WARN_DATA_OUT_OF_RANGE, 0},
		{"ignore invalid syntax", "NaN", false, moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, 0},
		{"ignore invalid bytes", "\x03\xe8", false, moerr.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, 0},
		{"ignore numeric prefix with invalid bytes", "1\xe8", false, moerr.WARN_DATA_TRUNCATED, 10},
		{"ignore retains decimal prefix", "-0x10", false, moerr.WARN_DATA_TRUNCATED, 0},
		{"ignore clips overflow", "1000", false, moerr.ER_WARN_DATA_OUT_OF_RANGE, 9999},
		{"valid ASCII digits", "-1.5", true, 0, -15},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newMemoryFunctionTestProcess(t)
			defer proc.Free()
			warnings := &numericWarningSession{}
			proc.WarningSink = warnings
			source := vector.NewVec(types.New(types.T_varbinary, 8, 0), proc.Mp())
			defer source.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(source, []byte(tc.text), false, proc.Mp()))
			typ := types.New(types.T_decimal64, 4, 1)
			target := vector.NewConstNull(typ, 1, proc.Mp())
			defer target.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(typ, proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(1))

			cast := NewStrictCast
			if !tc.strict {
				cast = NewAssignIgnoreCast
			}
			err := cast([]*vector.Vector{source, target}, result, proc, 1)
			if tc.strict && tc.wantCode != 0 {
				var diagnostic *moerr.Error
				require.ErrorAs(t, err, &diagnostic)
				require.Equal(t, tc.wantCode, diagnostic.MySQLCode())
				require.Empty(t, warnings.warnings)
				return
			}
			require.NoError(t, err)
			value := vector.GetFixedAtWithTypeCheck[types.Decimal64](result.GetResultVector(), 0)
			require.Equal(t, tc.wantValue, int64(value))
			if tc.wantCode == 0 {
				require.Empty(t, warnings.warnings)
			} else {
				require.Equal(t, []numericWarning{{code: tc.wantCode}}, stripWarningMessages(warnings.warnings))
				require.True(t, utf8.ValidString(warnings.warnings[0].msg))
			}
		})
	}
}
