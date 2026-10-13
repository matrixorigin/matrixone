// Copyright 2021 - 2026 Matrix Origin
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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFormatBytesValues(t *testing.T) {
	// Exact strings follow MySQL 8.0.46's native_func_format_bytes results and
	// libc formatting, including padding and selection before rounding.
	for _, tc := range []struct {
		input float64
		want  string
	}{
		{0, "   0 bytes"}, {math.Copysign(0, -1), "   0 bytes"},
		{0.9, "   0 bytes"}, {-0.9, "   0 bytes"},
		{512, " 512 bytes"}, {-512.9, "-512 bytes"},
		{1023, "1023 bytes"}, {-1023, "-1023 bytes"},
		{1024, "1.00 KiB"}, {-1024, "-1.00 KiB"},
		{1048575, "1024.00 KiB"}, {1 << 20, "1.00 MiB"},
		{(1 << 30) - 1, "1024.00 MiB"}, {1 << 30, "1.00 GiB"},
		{(1 << 40) - 1, "1024.00 GiB"}, {1 << 40, "1.00 TiB"},
		{(1 << 50) - 1, "1024.00 TiB"}, {1 << 50, "1.00 PiB"},
		{math.Nextafter(1<<60, 0), "1024.00 PiB"}, {1 << 60, "1.00 EiB"},
		{float64(uint64(math.MaxUint64)), "16.00 EiB"},
		{118059162071741143500099, "1.02e+05 EiB"},
		{-118059162071741143500099, "-1.02e+05 EiB"},
		{math.Ldexp(1, 400), "2.24e+102 EiB"},
		{math.Nextafter(100000*(1<<60), 0), "100000.00 EiB"},
		{100000 * (1 << 60), "1.00e+05 EiB"},
		{math.MaxFloat64, "1.56e+290 EiB"},
		{1152, "1.12 KiB"}, {math.Nextafter(1152, math.Inf(1)), "1.13 KiB"},
		{-1152, "-1.12 KiB"},
		{math.NaN(), " NaN bytes"}, {math.Inf(1), "+Inf EiB"}, {math.Inf(-1), "-Inf EiB"},
	} {
		require.Equal(t, tc.want, formatBytesValue(tc.input), "input %.17g", tc.input)
		if tc.input >= 1024 && !math.IsInf(tc.input, 0) {
			require.Equal(t, "-"+tc.want, formatBytesValue(-tc.input), "negative input %.17g", tc.input)
		}
	}
}

func TestFormatPicoTimeValues(t *testing.T) {
	for _, tc := range []struct {
		input float64
		want  string
	}{
		{0, "  0 ps"}, {math.Copysign(0, -1), "  0 ps"},
		{0.9, "  0 ps"}, {-0.9, "  0 ps"}, {12.9, " 12 ps"},
		{999, "999 ps"}, {-999, "-999 ps"},
		{1000, "1.00 ns"}, {-1000, "-1.00 ns"}, {3501, "3.50 ns"},
		{999999, "1000.00 ns"}, {1e6, "1.00 us"},
		{999999999, "1000.00 us"}, {1e9, "1.00 ms"},
		{999999999999, "1000.00 ms"}, {1e12, "1.00 s"},
		{60e12 - 1, "60.00 s"}, {60e12, "1.00 min"},
		{math.Nextafter(3600e12, 0), "60.00 min"}, {3600e12, "1.00 h"},
		{math.Nextafter(86400e12, 0), "24.00 h"}, {86400e12, "1.00 d"},
		{188732396662000, "3.15 min"}, {36000000.495523, "36.00 us"},
		{float64(uint64(math.MaxUint64)), "213.50 d"},
		{8650000000000000000099, "1.00e+05 d"},
		{-8650000000000000000099, "-1.00e+05 d"},
		{math.Nextafter(100000*86400e12, 0), "100000.00 d"},
		{100000 * 86400e12, "1.00e+05 d"},
		{math.MaxFloat64, "2.08e+291 d"},
		{1125, "1.12 ns"}, {math.Nextafter(1125, math.Inf(1)), "1.13 ns"},
		{-1125, "-1.12 ns"},
		{math.NaN(), " NaN ps"}, {math.Inf(1), "+Inf d"}, {math.Inf(-1), "-Inf d"},
	} {
		require.Equal(t, tc.want, formatPicoTimeValue(tc.input), "input %.17g", tc.input)
		if tc.input >= 1000 && !math.IsInf(tc.input, 0) {
			require.Equal(t, "-"+tc.want, formatPicoTimeValue(-tc.input), "negative input %.17g", tc.input)
		}
	}
}

func TestFormatUnitsVectorExecution(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int32
		fn   executeLogicOfOverload
		want string
	}{
		{"format_bytes", FORMAT_BYTES, formatBytes, "1.00 KiB"},
		{"format_pico_time", FORMAT_PICO_TIME, formatPicoTime, "1.02 ns"},
	} {
		for _, constant := range []bool{false, true} {
			name := tc.name + "/vector"
			if constant {
				name = tc.name + "/constant"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				defer proc.Free()
				input := NewFunctionTestInput(types.T_float64.ToType(), []float64{1024, 1024, 1024}, []bool{false, true, false})
				if constant {
					input = NewFunctionTestConstInput(types.T_float64.ToType(), []float64{1024}, nil)
				}
				fc := NewFunctionTestCase(proc, []FunctionTestInput{input},
					NewFunctionTestResult(types.T_varchar.ToType(), false, nil, nil), tc.fn)
				defer fc.Free()
				fc.fnLength = 3
				check := func(want []string, nulls []bool) {
					t.Helper()
					result, err := fc.DebugRun()
					require.NoError(t, err)
					assertFormatUnitsRows(t, result, want, nulls)
				}
				check([]string{tc.want, tc.want, tc.want}, []bool{false, !constant, false})
				fc = fc.WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, false}})
				check([]string{tc.want, "", ""}, []bool{false, true, true})
				fc = fc.WithSelectList(&FunctionSelectList{AllNull: true})
				check([]string{"", "", ""}, []bool{true, true, true})
				fc = fc.WithSelectList(nil)
				fc.parameters[0].GetNulls().Add(0)
				check([]string{"", tc.want, tc.want}, []bool{true, true, constant})
				fc.fnLength = 0
				check(nil, nil)
				fc.parameters[0].GetNulls().Reset()
				fc.fnLength = 3
				check([]string{tc.want, tc.want, tc.want}, nil)

				result, err := RunFunctionDirectly(proc, EncodeOverloadID(tc.id, 0), fc.parameters, 3)
				require.NoError(t, err)
				defer result.Free(proc.Mp())
				assertFormatUnitsRows(t, result, []string{tc.want, tc.want, tc.want}, nil)
			})
		}
	}
}

func TestFormatUnitsResolution(t *testing.T) {
	for _, name := range []string{"format_bytes", "format_pico_time"} {
		for _, oid := range []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_bool, types.T_char, types.T_varchar, types.T_any} {
			resolved, err := GetFunctionByName(context.Background(), name, []types.Type{oid.ToType()})
			require.NoError(t, err, "%s %s", name, oid)
			require.Equal(t, types.T_varchar.ToType(), resolved.GetReturnType())
			require.Equal(t, EncodeOverloadID(functionIdRegister[name], 0), resolved.GetEncodedOverloadID())
		}
		for _, args := range [][]types.Type{nil, {types.T_int64.ToType(), types.T_int64.ToType()}, {types.T_date.ToType()}} {
			_, err := GetFunctionByName(context.Background(), name, args)
			require.Error(t, err, "%s %v", name, args)
		}
	}
}

func assertFormatUnitsRows(t *testing.T, result *vector.Vector, want []string, wantNulls []bool) {
	t.Helper()
	require.Equal(t, types.T_varchar.ToType(), *result.GetType())
	require.Equal(t, len(want), result.Length())
	for i, expected := range want {
		isNull := len(wantNulls) != 0 && wantNulls[i]
		require.Equal(t, isNull, result.GetNulls().Contains(uint64(i)), "row %d", i)
		if !isNull {
			require.Equal(t, expected, string(result.GetBytesAt(i)), "row %d", i)
		}
	}
}
