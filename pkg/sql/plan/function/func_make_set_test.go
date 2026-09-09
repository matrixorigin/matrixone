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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMakeSetDecimal(t *testing.T) {
	type row struct{ value, want string }
	halves := []row{
		{"0", ""}, {"0.4", ""}, {"0.5", "a"}, {"0.6", "a"},
		{"1.4", "a"}, {"1.5", "b"}, {"1.9", "b"},
		{"-0.4", ""}, {"-0.5", "a,b,c,d,high"}, {"-0.6", "a,b,c,d,high"},
		{"-1.4", "a,b,c,d,high"}, {"-1.5", "b,c,d,high"}, {"-1.9", "b,c,d,high"},
	}
	boundaries := []row{
		{"1.4999999999999999", "a"}, {"1.5000000000000000", "b"},
		{"1.5000000000000001", "b"}, {"1.9999999999999999", "b"},
		{"-1.4999999999999999", "a,b,c,d,high"},
		{"-1.5000000000000000", "b,c,d,high"},
		{"-1.5000000000000001", "b,c,d,high"},
	}
	for _, group := range []struct {
		name string
		typ  types.Type
		rows []row
	}{
		{"decimal64/halves", types.New(types.T_decimal64, 18, 1), halves},
		{"decimal128/halves", types.New(types.T_decimal128, 38, 1), halves},
		{"decimal64/precision", types.New(types.T_decimal64, 18, 16), boundaries},
		{"decimal128/precision", types.New(types.T_decimal128, 38, 16), boundaries},
		{"decimal64/max_scale", types.New(types.T_decimal64, 18, 17), []row{
			{"0.49999999999999999", ""}, {"0.50000000000000000", "a"},
			{"-0.49999999999999999", ""}, {"-0.50000000000000000", "a,b,c,d,high"},
		}},
		{"decimal128/double_round", types.New(types.T_decimal128, 38, 20), []row{
			{"1.49999999999999999999", "a"}, {"1.50000000000000000000", "b"},
			{"-1.49999999999999999999", "a,b,c,d,high"}, {"-1.50000000000000000000", "b,c,d,high"},
		}},
		{"decimal128/max_scale", types.New(types.T_decimal128, 38, 37), []row{
			{"0.4999999999999999999999999999999999999", ""},
			{"0.5000000000000000000000000000000000000", "a"},
			{"-0.499999999999999999999999999999999999", ""},
			{"-0.500000000000000000000000000000000000", "a,b,c,d,high"},
		}},
		{"decimal64/scale_zero", types.New(types.T_decimal64, 18, 0), []row{
			{"1", "a"}, {"2", "b"}, {"-1", "a,b,c,d,high"}, {"-2", "b,c,d,high"},
		}},
		{"decimal128/endpoints", types.New(types.T_decimal128, 38, 0), []row{
			{"9007199254740993", "a"},
			{"9223372036854775806", "b,c,d"}, {"9223372036854775807", "a,b,c,d"},
			{"9223372036854775808", "a,b,c,d"},
			{"18446744073709551615", "a,b,c,d"}, {"18446744073709551616", "a,b,c,d"},
			{"99999999999999999999999999999999999999", "a,b,c,d"},
			{"-9223372036854775807", "a,high"}, {"-9223372036854775808", "high"},
			{"-9223372036854775809", "high"},
			{"-99999999999999999999999999999999999999", "high"},
		}},
		{"decimal128/round_to_endpoint", types.New(types.T_decimal128, 38, 1), []row{
			{"9223372036854775806.4", "b,c,d"}, {"9223372036854775806.5", "a,b,c,d"},
			{"9223372036854775807.5", "a,b,c,d"},
			{"-9223372036854775807.4", "a,high"}, {"-9223372036854775807.5", "high"},
			{"-9223372036854775808.5", "high"},
		}},
	} {
		t.Run(group.name, func(t *testing.T) {
			run := func(t *testing.T, rows []row, constant bool) {
				proc := testutil.NewProcess(t)
				t.Cleanup(proc.Free)
				n := len(rows)
				if constant {
					n = 3
				}
				bits := vector.NewVec(group.typ)
				t.Cleanup(func() { bits.Free(proc.Mp()) })
				for _, r := range rows {
					isNull := r.value == "NULL"
					value := r.value
					if isNull {
						value = "0"
					}
					if group.typ.Oid == types.T_decimal64 {
						val, err := types.ParseDecimal64(value, group.typ.Width, group.typ.Scale)
						require.NoError(t, err)
						require.NoError(t, vector.AppendFixed(bits, val, isNull, proc.Mp()))
					} else {
						val, err := types.ParseDecimal128(value, group.typ.Width, group.typ.Scale)
						require.NoError(t, err)
						require.NoError(t, vector.AppendFixed(bits, val, isNull, proc.Mp()))
					}
				}
				if constant {
					bits.SetClass(vector.CONSTANT)
					bits.SetLength(n)
				}
				inputs := []*vector.Vector{bits}
				// Only low bits and the sign bit have non-NULL members. This
				// distinguishes signed saturation from unsigned saturation.
				for i := 0; i < 64; i++ {
					member := vector.NewConstNull(types.T_varchar.ToType(), n, proc.Mp())
					if i < 4 || i == 63 {
						member.Free(proc.Mp())
						name := "high"
						if i < 4 {
							name = string(rune('a' + i))
						}
						var err error
						member, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(name), n, proc.Mp())
						require.NoError(t, err)
					}
					t.Cleanup(func() { member.Free(proc.Mp()) })
					inputs = append(inputs, member)
				}
				result := vector.NewFunctionResultWrapper(types.T_varchar.ToType(), proc.Mp())
				t.Cleanup(result.Free)
				for _, masked := range []bool{false, true} {
					var selection *FunctionSelectList
					if masked {
						selected := make([]bool, n)
						for i := range selected {
							selected[i] = true
						}
						selected[0] = false
						selection = &FunctionSelectList{AnyNull: true, SelectList: selected}
					}
					require.NoError(t, result.PreExtendAndReset(n))
					require.NoError(t, MakeSet(inputs, result, proc, n, selection))
					require.Equal(t, n, result.GetResultVector().Length())
					output := vector.GenerateFunctionStrParameter(result.GetResultVector())
					for i := 0; i < n; i++ {
						r := rows[0]
						if !constant {
							r = rows[i]
						}
						got, isNull := output.GetStrValue(uint64(i))
						wantNull := r.value == "NULL" || (masked && i == 0)
						require.Equalf(t, wantNull, isNull, "value=%s masked=%v row=%d", r.value, masked, i)
						if !wantNull {
							require.Equalf(t, r.want, string(got), "value=%s", r.value)
						}
					}
				}
			}
			rows := append(append([]row(nil), group.rows...), row{"NULL", ""})
			t.Run("vector", func(t *testing.T) { run(t, rows, false) })
			for _, r := range rows {
				t.Run("const/"+r.value, func(t *testing.T) { run(t, []row{r}, true) })
			}
		})
	}
}
