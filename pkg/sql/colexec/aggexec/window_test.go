// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

// Frames are explicit so callers retain control of GroupGrow and missing-current-row cases.
func fillValueWindowFrames(t *testing.T, exec AggFuncExec, input *vector.Vector, frames [][]int) {
	t.Helper()
	for group, frame := range frames {
		for _, row := range frame {
			require.NoError(t, exec.Fill(group, row, []*vector.Vector{input}))
		}
	}
}

func flushValueWindowForTest(t *testing.T, exec AggFuncExec, mp *mpool.MPool) *vector.Vector {
	t.Helper()
	results, err := exec.Flush()
	for _, result := range results {
		t.Cleanup(func() { result.Free(mp) })
	}
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.NotNil(t, results[0])
	return results[0]
}

func checkValueWindowNulls(t *testing.T, result *vector.Vector, typ types.Type, want []bool) {
	t.Helper()
	require.Equal(t, typ, *result.GetType())
	require.Equal(t, len(want), result.Length())
	for row, isNull := range want {
		require.Equal(t, isNull, result.IsNull(uint64(row)), "row %d", row)
	}
}

func runValueWindowFixedCase[T types.FixedSizeTExceptStrType](t *testing.T, id int64, typ types.Type, input []T, nulls []bool, want []T, wantNulls []bool) {
	t.Helper()
	mp := newAggExecTestPool(t)
	vec := vector.NewVec(typ)
	t.Cleanup(func() { vec.Free(mp) })
	require.NoError(t, vector.AppendFixedList(vec, input, nulls, mp))
	exec, err := makeValueWindowExec(mp, id, false, []types.Type{typ})
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	require.NoError(t, exec.GroupGrow(3))
	fillValueWindowFrames(t, exec, vec, [][]int{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}})
	result := flushValueWindowForTest(t, exec, mp)
	checkValueWindowNulls(t, result, typ, wantNulls)
	require.Len(t, want, len(wantNulls))
	values := vector.MustFixedColWithTypeCheck[T](result)
	for row, isNull := range wantNulls {
		if !isNull {
			require.Equal(t, want[row], values[row], "row %d", row)
		}
	}
}

func TestValueWindowExec_APIContracts(t *testing.T) {
	mp := newAggExecTestPool(t)
	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	require.NoError(t, exec.GroupGrow(3))
	require.NoError(t, exec.PreAllocateGroups(2))
	require.Nil(t, exec.GetOptResult())
	require.GreaterOrEqual(t, exec.Size(), int64(0))
	require.NoError(t, exec.SetExtraInformation(nil, 0))
	for _, test := range []struct {
		name string
		call func() error
	}{
		{"BulkFill", func() error { return exec.BulkFill(0, nil) }},
		{"BatchFill", func() error { return exec.BatchFill(0, nil, nil) }},
		{"Merge", func() error { return exec.Merge(nil, 0, 0) }},
		{"BatchMerge", func() error { return exec.BatchMerge(nil, 0, nil) }},
		{"SaveIntermediateResult", func() error { return exec.SaveIntermediateResult(0, nil, nil) }},
		{"SaveIntermediateResultOfChunk", func() error { return exec.SaveIntermediateResultOfChunk(0, nil) }},
		{"UnmarshalFromReader", func() error { return exec.UnmarshalFromReader(nil, mp) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
			require.Contains(t, err.Error(), test.name)
		})
	}
	t.Run("distinct rejected", func(t *testing.T) {
		rejected, err := makeValueWindowExec(mp, WinIdOfLag, true, []types.Type{types.T_int64.ToType()})
		require.Nil(t, rejected)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
		require.Contains(t, err.Error(), "distinct")
	})
	for _, test := range []struct {
		name   string
		params []types.Type
		want   types.Type
	}{
		{"no parameters", nil, types.T_any.ToType()},
		{"complete type", []types.Type{{Oid: types.T_decimal64, Width: 18, Scale: 4}}, types.Type{Oid: types.T_decimal64, Width: 18, Scale: 4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := newAggExecTestPool(t)
			typed, err := makeValueWindowExec(mp, WinIdOfLag, false, test.params)
			require.NoError(t, err)
			t.Cleanup(typed.Free)
			args, result := typed.TypesInfo()
			require.Equal(t, []types.Type{test.want}, args)
			require.Equal(t, test.want, result)
			require.Equal(t, WinIdOfLag, typed.AggID())
			require.False(t, typed.IsDistinct())
		})
	}

}

func TestValueWindowExec_FillAndFlush(t *testing.T) {
	for _, test := range []struct {
		name      string
		id        int64
		input     []int64
		nulls     []bool
		want      []int64
		wantNulls []bool
	}{
		{"lag", WinIdOfLag, []int64{100, 200, 300}, nil, []int64{0, 100, 200}, []bool{true, false, false}},
		{"lead", WinIdOfLead, []int64{100, 200, 300}, nil, []int64{200, 300, 0}, []bool{false, false, true}},
		{"first", WinIdOfFirstValue, []int64{100, 200, 300}, nil, []int64{100, 100, 100}, []bool{false, false, false}},
		{"last", WinIdOfLastValue, []int64{100, 200, 300}, nil, []int64{300, 300, 300}, []bool{false, false, false}},
		{"nth default one", WinIdOfNthValue, []int64{100, 200, 300}, nil, []int64{100, 100, 100}, []bool{false, false, false}},
		{"lag null predecessor", WinIdOfLag, []int64{100, 0, 300}, []bool{false, true, false}, []int64{0, 100, 0}, []bool{true, false, true}},
		{"first null endpoint", WinIdOfFirstValue, []int64{0, 200, 300}, []bool{true, false, false}, []int64{0, 0, 0}, []bool{true, true, true}},
		{"last null endpoint", WinIdOfLastValue, []int64{100, 200, 0}, []bool{false, false, true}, []int64{0, 0, 0}, []bool{true, true, true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runValueWindowFixedCase(t, test.id, types.T_int64.ToType(), test.input, test.nulls, test.want, test.wantNulls)
		})
	}
}

func TestValueWindowExec_VarlenTypes(t *testing.T) {
	for _, test := range []struct {
		name  string
		id    int64
		want  []string
		nulls []bool
	}{
		{"lag", WinIdOfLag, []string{"", "aaa", "bbb"}, []bool{true, false, false}},
		{"lead", WinIdOfLead, []string{"bbb", "ccc", ""}, []bool{false, false, true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := newAggExecTestPool(t)
			vec := vector.NewVec(types.T_varchar.ToType())
			t.Cleanup(func() { vec.Free(mp) })
			require.NoError(t, vector.AppendStringList(vec, []string{"aaa", "bbb", "ccc"}, nil, mp))
			exec, err := makeValueWindowExec(mp, test.id, false, []types.Type{types.T_varchar.ToType()})
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			require.NoError(t, exec.GroupGrow(3))
			fillValueWindowFrames(t, exec, vec, [][]int{{0, 1, 2}, {0, 1, 2}, {0, 1, 2}})
			result := flushValueWindowForTest(t, exec, mp)
			checkValueWindowNulls(t, result, types.T_varchar.ToType(), test.nulls)
			for row, isNull := range test.nulls {
				if !isNull {
					require.Equal(t, test.want[row], string(result.GetBytesAt(row)))
				}
			}
		})
	}
}

func TestValueWindowExec_BinaryStringProvenance(t *testing.T) {
	tests := []struct {
		name   string
		id     int64
		want   []bool
		values []string
		nulls  []bool
	}{
		{name: "lag", id: WinIdOfLag, want: []bool{false, true, false}, values: []string{"", "binary-0", "text-1"}, nulls: []bool{true, false, false}},
		{name: "lead", id: WinIdOfLead, want: []bool{false, true, false}, values: []string{"text-1", "binary-2", ""}, nulls: []bool{false, false, true}},
		{name: "first_value", id: WinIdOfFirstValue, want: []bool{true, true, true}, values: []string{"binary-0", "binary-0", "binary-0"}, nulls: []bool{false, false, false}},
		{name: "last_value", id: WinIdOfLastValue, want: []bool{true, true, true}, values: []string{"binary-2", "binary-2", "binary-2"}, nulls: []bool{false, false, false}},
		{name: "nth_value", id: WinIdOfNthValue, want: []bool{true, true, true}, values: []string{"binary-0", "binary-0", "binary-0"}, nulls: []bool{false, false, false}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := newAggExecTestPool(t)

			exec, err := makeValueWindowExec(mp, tc.id, false, []types.Type{types.T_varchar.ToType()})
			require.NoError(t, err)
			t.Cleanup(exec.Free)

			vec := vector.NewVec(types.T_varchar.ToType())
			t.Cleanup(func() { vec.Free(mp) })
			require.NoError(t, vector.AppendStringList(vec, []string{"binary-0", "text-1", "binary-2"}, nil, mp))

			require.NoError(t, vec.SetIsBinaryStringAt(0, true, mp))
			require.NoError(t, vec.SetIsBinaryStringAt(2, true, mp))

			require.NoError(t, exec.GroupGrow(3))
			for outputRow := 0; outputRow < 3; outputRow++ {
				for frameRow := 0; frameRow < 3; frameRow++ {
					require.NoError(t, exec.Fill(outputRow, frameRow, []*vector.Vector{vec}))
				}
			}

			result := flushValueWindowForTest(t, exec, mp)
			checkValueWindowNulls(t, result, types.T_varchar.ToType(), tc.nulls)
			for row, isNull := range tc.nulls {
				if !isNull {
					require.Equal(t, tc.values[row], string(result.GetBytesAt(row)))
				}
			}

			for row, want := range tc.want {
				require.Equal(t, want, result.GetBinaryStringMetadataAt(row), "row %d", row)
			}
			if tc.id == WinIdOfLag {
				require.True(t, result.IsNull(0))
				require.False(t, result.GetBinaryStringMetadataAt(0))
			}
			if tc.id == WinIdOfLead {
				require.True(t, result.IsNull(2))
				require.False(t, result.GetBinaryStringMetadataAt(2))
			}
		})
	}
}

func TestValueWindowExec_EmptyVectors(t *testing.T) {
	mp := newAggExecTestPool(t)
	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.Fill(0, 0, nil))
	checkValueWindowNulls(t, flushValueWindowForTest(t, exec, mp), types.T_int64.ToType(), []bool{true})
}

func TestValueWindowExec_FillWithoutGroupGrow(t *testing.T) {
	mp := newAggExecTestPool(t)
	vec := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { vec.Free(mp) })
	require.NoError(t, vector.AppendFixedList(vec, []int64{100, 200}, nil, mp))
	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	// No GroupGrow: second group has a nonempty frame but no current row.
	fillValueWindowFrames(t, exec, vec, [][]int{{0}, {0}})
	checkValueWindowNulls(t, flushValueWindowForTest(t, exec, mp), types.T_int64.ToType(), []bool{true, true})
}

func TestValueWindowExec_SizeWithData(t *testing.T) {
	mp := newAggExecTestPool(t)
	vec := vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { vec.Free(mp) })
	require.NoError(t, vector.AppendFixedList(vec, []int64{100, 200, 300}, nil, mp))
	exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	require.NoError(t, exec.GroupGrow(2))
	fillValueWindowFrames(t, exec, vec, [][]int{{0, 1, 2}})
	require.Positive(t, exec.Size())
	exec.Free()
	require.Zero(t, exec.Size())
	concrete := exec.(*valueWindowExec)
	require.Nil(t, concrete.frameValues)
	require.Nil(t, concrete.currentRowPosition)
}

func TestValueWindowExec_ResultOwnership(t *testing.T) {
	for _, transfer := range []bool{false, true} {
		name := "caller owns output"
		if transfer {
			name = "executor owns transferred output"
		}
		t.Run(name, func(t *testing.T) {
			mp := newAggExecTestPool(t)
			vec := vector.NewVec(types.T_int64.ToType())
			t.Cleanup(func() { vec.Free(mp) })
			require.NoError(t, vector.AppendFixedList(vec, []int64{100, 200}, nil, mp))
			exec, err := makeValueWindowExec(mp, WinIdOfLag, false, []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			require.NoError(t, exec.GroupGrow(2))
			fillValueWindowFrames(t, exec, vec, [][]int{{0, 1}, {0, 1}})
			results, err := exec.Flush()
			callerOwns := true
			t.Cleanup(func() {
				if callerOwns {
					for _, result := range results {
						result.Free(mp)
					}
				}
			})
			require.NoError(t, err)
			require.Len(t, results, 1)
			checkValueWindowNulls(t, results[0], types.T_int64.ToType(), []bool{true, false})
			require.Equal(t, int64(100), vector.GetFixedAtWithTypeCheck[int64](results[0], 1))
			require.Positive(t, mp.CurrNB())
			if transfer {
				exec.(*valueWindowExec).resultVec = results[0]
				callerOwns = false
			}
			exec.Free()
			require.Zero(t, exec.Size())
			if transfer {
				require.Zero(t, mp.CurrNB())
			} else {
				require.Positive(t, mp.CurrNB())
				require.Equal(t, int64(100), vector.GetFixedAtWithTypeCheck[int64](results[0], 1))
			}
		})
	}
}

func TestValueWindowExecPreservesRowStringSources(t *testing.T) {
	for _, window := range []struct {
		name   string
		id     int64
		want   []types.StringSource
		values []string
		nulls  []bool
	}{
		{
			name: "lag",
			id:   WinIdOfLag, values: []string{"", "a", "b", "c", "d"}, nulls: []bool{true, false, false, false, false},
			want: []types.StringSource{
				types.StringSourceExpression,
				types.StringSourceExpression,
				types.StringSourceLiteral,
				types.StringSourceUserVariable,
				types.StringSourceSQLPrepare,
			},
		},
		{
			name: "lead",
			id:   WinIdOfLead, values: []string{"b", "c", "d", "e", ""}, nulls: []bool{false, false, false, false, true},
			want: []types.StringSource{
				types.StringSourceLiteral,
				types.StringSourceUserVariable,
				types.StringSourceSQLPrepare,
				types.StringSourceCOMStmt,
				types.StringSourceExpression,
			},
		},
	} {
		t.Run(window.name, func(t *testing.T) {
			mp := newAggExecTestPool(t)
			input := vector.NewVec(types.T_text.ToType())
			t.Cleanup(func() { input.Free(mp) })
			sources := []types.StringSource{
				types.StringSourceExpression,
				types.StringSourceLiteral,
				types.StringSourceUserVariable,
				types.StringSourceSQLPrepare,
				types.StringSourceCOMStmt,
			}
			for i := range sources {
				require.NoError(t, vector.AppendBytes(input, []byte{byte('a' + i)}, false, mp))
			}
			require.NoError(t, input.SetStringSourcesWithMP(sources, mp))
			exec, err := makeValueWindowExec(mp, window.id, false, []types.Type{types.T_text.ToType()})
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			require.NoError(t, exec.GroupGrow(len(sources)))
			for group := range sources {
				for row := range sources {
					require.NoError(t, exec.Fill(group, row, []*vector.Vector{input}))
				}
			}
			result := flushValueWindowForTest(t, exec, mp)
			checkValueWindowNulls(t, result, types.T_text.ToType(), window.nulls)
			for row, isNull := range window.nulls {
				if !isNull {
					require.Equal(t, window.values[row], string(result.GetBytesAt(row)))
				}
			}

			for row, source := range window.want {
				require.Equal(t, source, result.GetStringSourceAt(row))
			}
		})
	}
}

func TestValueWindowExec_FixedTypes(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_bool.ToType(), []bool{true, false, true}, nil, []bool{false, true, false}, []bool{true, false, false})
	})
	t.Run("int8", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_int8.ToType(), []int8{1, 2, 3}, nil, []int8{0, 1, 2}, []bool{true, false, false})
	})
	t.Run("int16", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_int16.ToType(), []int16{100, 200, 300}, nil, []int16{0, 100, 200}, []bool{true, false, false})
	})
	t.Run("int32", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_int32.ToType(), []int32{1000, 2000, 3000}, nil, []int32{0, 1000, 2000}, []bool{true, false, false})
	})
	t.Run("uint8", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_uint8.ToType(), []uint8{1, 2, 3}, nil, []uint8{0, 1, 2}, []bool{true, false, false})
	})
	t.Run("uint16", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_uint16.ToType(), []uint16{100, 200, 300}, nil, []uint16{0, 100, 200}, []bool{true, false, false})
	})
	t.Run("uint32", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_uint32.ToType(), []uint32{1000, 2000, 3000}, nil, []uint32{0, 1000, 2000}, []bool{true, false, false})
	})
	t.Run("uint64", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_uint64.ToType(), []uint64{10000, 20000, 30000}, nil, []uint64{0, 10000, 20000}, []bool{true, false, false})
	})
	t.Run("float32", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_float32.ToType(), []float32{1.1, 2.2, 3.3}, nil, []float32{0, 1.1, 2.2}, []bool{true, false, false})
	})
	t.Run("float64", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_float64.ToType(), []float64{1.11, 2.22, 3.33}, nil, []float64{0, 1.11, 2.22}, []bool{true, false, false})
	})
	t.Run("date", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_date.ToType(), []types.Date{1, 2, 3}, nil, []types.Date{0, 1, 2}, []bool{true, false, false})
	})
	t.Run("datetime", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_datetime.ToType(), []types.Datetime{1000, 2000, 3000}, nil, []types.Datetime{0, 1000, 2000}, []bool{true, false, false})
	})
	t.Run("decimal64", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_decimal64.ToType(), []types.Decimal64{100, 200, 300}, nil, []types.Decimal64{0, 100, 200}, []bool{true, false, false})
	})
	t.Run("bit", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_bit.ToType(), []uint64{1, 2, 3}, nil, []uint64{0, 1, 2}, []bool{true, false, false})
	})
	t.Run("time", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_time.ToType(), []types.Time{1000, 2000, 3000}, nil, []types.Time{0, 1000, 2000}, []bool{true, false, false})
	})
	t.Run("timestamp", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_timestamp.ToType(), []types.Timestamp{1000, 2000, 3000}, nil, []types.Timestamp{0, 1000, 2000}, []bool{true, false, false})
	})
	t.Run("decimal128", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_decimal128.ToType(), []types.Decimal128{{B0_63: 100}, {B0_63: 200}, {B0_63: 300}}, nil, []types.Decimal128{{}, {B0_63: 100}, {B0_63: 200}}, []bool{true, false, false})
	})
	t.Run("uuid", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_uuid.ToType(), []types.Uuid{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}}, nil, []types.Uuid{{}, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}}, []bool{true, false, false})
	})
	t.Run("enum", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_enum.ToType(), []types.Enum{1, 2, 3}, nil, []types.Enum{0, 1, 2}, []bool{true, false, false})
	})
	t.Run("Rowid", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_Rowid.ToType(), []types.Rowid{{1, 2, 3, 4, 5, 6}, {2, 3, 4, 5, 6, 7}, {3, 4, 5, 6, 7, 8}}, nil, []types.Rowid{{}, {1, 2, 3, 4, 5, 6}, {2, 3, 4, 5, 6, 7}}, []bool{true, false, false})
	})
	t.Run("Blockid", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_Blockid.ToType(), []types.Blockid{{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, {3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}}, nil, []types.Blockid{{}, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}}, []bool{true, false, false})
	})
	t.Run("TS", func(t *testing.T) {
		runValueWindowFixedCase(t, WinIdOfLag, types.T_TS.ToType(), []types.TS{types.BuildTS(1, 1), types.BuildTS(2, 2), types.BuildTS(3, 3)}, nil, []types.TS{{}, types.BuildTS(1, 1), types.BuildTS(2, 2)}, []bool{true, false, false})
	})
}

func TestValueWindowExec_EmptyFrameAllFunctions(t *testing.T) {
	for _, test := range []struct {
		name string
		id   int64
	}{
		{"lag", WinIdOfLag}, {"lead", WinIdOfLead}, {"first", WinIdOfFirstValue}, {"last", WinIdOfLastValue}, {"nth default one", WinIdOfNthValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := newAggExecTestPool(t)
			exec, err := makeValueWindowExec(mp, test.id, false, []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			require.NoError(t, exec.GroupGrow(1))
			checkValueWindowNulls(t, flushValueWindowForTest(t, exec, mp), types.T_int64.ToType(), []bool{true})
		})
	}
}

func TestValueWindowExec_InvalidAggID(t *testing.T) {
	mp := newAggExecTestPool(t)
	exec := &valueWindowExec{singleAggInfo: singleAggInfo{aggID: -999, argType: types.T_int64.ToType(), retType: types.T_int64.ToType(), emptyNull: true}, mp: mp}
	t.Cleanup(exec.Free)
	require.NoError(t, exec.GroupGrow(1))
	results, err := exec.Flush()
	for _, result := range results {
		t.Cleanup(func() { result.Free(mp) })
	}
	require.Nil(t, results)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	require.Contains(t, err.Error(), "invalid value window function")
}

func TestNtileWindowExec(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("NTILE_basic_10_rows_3_buckets", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		// Simulate window operator: one partition with 10 rows
		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{3, 3, 3, 3, 3, 3, 3, 3, 3, 3}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		err = exec.GroupGrow(10)
		require.NoError(t, err)

		// Fill with groupIndex=0, row=0..10 (indices into os array)
		for o := 0; o <= 10; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 10, resultVec.Length())

		// Expected: 1,1,1,1,2,2,2,3,3,3 (4+3+3 distribution)
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		expected := []int64{1, 1, 1, 1, 2, 2, 2, 3, 3, 3}
		for i := 0; i < 10; i++ {
			require.Equal(t, expected[i], col[i], "row %d", i)
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("NTILE_9_rows_3_buckets", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{3, 3, 3, 3, 3, 3, 3, 3, 3}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		err = exec.GroupGrow(9)
		require.NoError(t, err)

		for o := 0; o <= 9; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 9, resultVec.Length())

		// Expected: 1,1,1,2,2,2,3,3,3 (3+3+3 distribution)
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		expected := []int64{1, 1, 1, 2, 2, 2, 3, 3, 3}
		for i := 0; i < 9; i++ {
			require.Equal(t, expected[i], col[i], "row %d", i)
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("NTILE_5_rows_3_buckets", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{3, 3, 3, 3, 3}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		err = exec.GroupGrow(5)
		require.NoError(t, err)

		for o := 0; o <= 5; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 5, resultVec.Length())

		// Expected: 1,1,2,2,3 (2+2+1 distribution)
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		expected := []int64{1, 1, 2, 2, 3}
		for i := 0; i < 5; i++ {
			require.Equal(t, expected[i], col[i], "row %d", i)
		}

		resultVec.Free(mp)
		exec.Free()
	})
}

func TestPercentRank(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("creation", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		require.NotNil(t, exec)
		exec.Free()
	})

	t.Run("distinct_not_supported", func(t *testing.T) {
		_, err := makePercentRankExec(mp, WinIdOfPercentRank, true)
		require.Error(t, err)
	})

	t.Run("group_grow_and_preallocate", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		err = exec.GroupGrow(3)
		require.NoError(t, err)
		err = exec.PreAllocateGroups(2)
		require.NoError(t, err)
		exec.Free()
	})

	t.Run("fill_and_flush_single_row", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		// 分配1个结果槽位
		err = exec.GroupGrow(1)
		require.NoError(t, err)

		// 单行分区: [0, 1] 表示从0到1共1行
		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		// Fill两次: 第一次是起始位置0，第二次是结束位置1
		err = exec.Fill(0, 0, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec.Fill(0, 1, []*vector.Vector{vec})
		require.NoError(t, err)

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		// 单行 percent_rank = 0
		vals := vector.MustFixedColWithTypeCheck[float64](results[0])
		require.Equal(t, float64(0), vals[0])
		for _, r := range results {
			r.Free(mp)
		}
		exec.Free()
	})

	t.Run("fill_and_flush_multiple_rows", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		// 分配4个结果槽位（4行数据）
		err = exec.GroupGrow(4)
		require.NoError(t, err)

		// 4行分区，每行rank不同: [0, 1, 2, 3, 4]
		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		for i := 0; i < 5; i++ {
			err = exec.Fill(0, i, []*vector.Vector{vec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		vals := vector.MustFixedColWithTypeCheck[float64](results[0])
		// totalRows = 4, percent_rank = (rank-1)/(totalRows-1)
		require.InDelta(t, 0.0, vals[0], 0.0001)
		require.InDelta(t, 1.0/3.0, vals[1], 0.0001)
		require.InDelta(t, 2.0/3.0, vals[2], 0.0001)
		require.InDelta(t, 1.0, vals[3], 0.0001)
		for _, r := range results {
			r.Free(mp)
		}
		exec.Free()
	})

	t.Run("merge", func(t *testing.T) {
		exec1, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		exec2, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)

		err = exec1.GroupGrow(1)
		require.NoError(t, err)
		err = exec2.GroupGrow(1)
		require.NoError(t, err)

		vec1 := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec1, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer vec1.Free(mp)

		vec2 := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec2, []int64{1, 2}, nil, mp)
		require.NoError(t, err)
		defer vec2.Free(mp)

		err = exec1.Fill(0, 0, []*vector.Vector{vec1})
		require.NoError(t, err)
		err = exec1.Fill(0, 1, []*vector.Vector{vec1})
		require.NoError(t, err)
		err = exec2.Fill(0, 0, []*vector.Vector{vec2})
		require.NoError(t, err)
		err = exec2.Fill(0, 1, []*vector.Vector{vec2})
		require.NoError(t, err)

		err = exec1.Merge(exec2, 0, 0)
		require.NoError(t, err)

		exec1.Free()
		exec2.Free()
	})

	t.Run("batch_merge", func(t *testing.T) {
		exec1, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		exec2, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)

		err = exec1.GroupGrow(2)
		require.NoError(t, err)
		err = exec2.GroupGrow(2)
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		for i := 0; i < 2; i++ {
			err = exec1.Fill(0, i, []*vector.Vector{vec})
			require.NoError(t, err)
			err = exec2.Fill(1, i, []*vector.Vector{vec})
			require.NoError(t, err)
		}

		groups := []uint64{1, GroupNotMatched}
		err = exec1.BatchMerge(exec2, 0, groups)
		require.NoError(t, err)

		exec1.Free()
		exec2.Free()
	})

	t.Run("size", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		size := exec.Size()
		require.GreaterOrEqual(t, size, int64(0))
		exec.Free()
	})

	t.Run("get_opt_result", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		result := exec.GetOptResult()
		require.NotNil(t, result)
		exec.Free()
	})

	t.Run("save_intermediate_result", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		err = exec.GroupGrow(2)
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		err = exec.Fill(0, 0, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec.Fill(0, 1, []*vector.Vector{vec})
		require.NoError(t, err)

		prExec := exec.(*percentRankExec)
		var buf bytes.Buffer
		flags := [][]uint8{{1, 1}}
		err = prExec.SaveIntermediateResult(2, flags, &buf)
		require.NoError(t, err)

		exec.Free()
	})

	t.Run("unmarshal_from_reader", func(t *testing.T) {
		exec1, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		err = exec1.GroupGrow(2)
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1, 2}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		for i := 0; i < 3; i++ {
			err = exec1.Fill(0, i, []*vector.Vector{vec})
			require.NoError(t, err)
		}

		prExec1 := exec1.(*percentRankExec)
		var buf bytes.Buffer
		flags := [][]uint8{{1, 1}}
		err = prExec1.SaveIntermediateResult(2, flags, &buf)
		require.NoError(t, err)

		exec2, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		prExec2 := exec2.(*percentRankExec)
		reader := bytes.NewReader(buf.Bytes())
		err = prExec2.UnmarshalFromReader(reader, mp)
		require.NoError(t, err)

		exec1.Free()
		exec2.Free()
	})

	t.Run("save_intermediate_result_of_chunk", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		err = exec.GroupGrow(2)
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		err = exec.Fill(0, 0, []*vector.Vector{vec})
		require.NoError(t, err)

		prExec := exec.(*percentRankExec)
		var buf bytes.Buffer
		err = prExec.SaveIntermediateResultOfChunk(0, &buf)
		require.NoError(t, err)

		exec.Free()
	})

	t.Run("flush_multiple_groups", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		err = exec.GroupGrow(2)
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1, 2, 3}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		// 第一个分区: [0, 1] - 1行
		err = exec.Fill(0, 0, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec.Fill(0, 1, []*vector.Vector{vec})
		require.NoError(t, err)

		// 第二个分区: [2, 3] - 1行
		err = exec.Fill(1, 2, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec.Fill(1, 3, []*vector.Vector{vec})
		require.NoError(t, err)

		results, err := exec.Flush()
		require.NoError(t, err)
		require.NotEmpty(t, results)

		for _, r := range results {
			r.Free(mp)
		}
		exec.Free()
	})

	t.Run("flush_with_empty_group", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		err = exec.GroupGrow(2)
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		// 只填充第一个分区，第二个分区为空
		err = exec.Fill(0, 0, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec.Fill(0, 1, []*vector.Vector{vec})
		require.NoError(t, err)

		results, err := exec.Flush()
		require.NoError(t, err)
		require.NotEmpty(t, results)

		for _, r := range results {
			r.Free(mp)
		}
		exec.Free()
	})

	t.Run("size_with_groups", func(t *testing.T) {
		exec, err := makePercentRankExec(mp, WinIdOfPercentRank, false)
		require.NoError(t, err)
		err = exec.GroupGrow(2)
		require.NoError(t, err)

		vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(vec, []int64{0, 1, 2}, nil, mp)
		require.NoError(t, err)
		defer vec.Free(mp)

		for i := 0; i < 3; i++ {
			err = exec.Fill(0, i, []*vector.Vector{vec})
			require.NoError(t, err)
		}

		size := exec.Size()
		require.Greater(t, size, int64(0))
		exec.Free()
	})
}

func TestRankingWindowUnsignedResults(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	osVec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(osVec, []int64{0, 2, 3, 5}, nil, mp))
	defer osVec.Free(mp)

	tests := []struct {
		name string
		id   int64
		want []uint64
	}{
		{name: "rank", id: WinIdOfRank, want: []uint64{1, 1, 3, 4, 4}},
		{name: "dense_rank", id: WinIdOfDenseRank, want: []uint64{1, 1, 2, 3, 3}},
		{name: "row_number", id: WinIdOfRowNumber, want: []uint64{1, 2, 3, 4, 5}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec, err := makeWindowExec(mp, test.id, false)
			require.NoError(t, err)
			require.NoError(t, exec.GroupGrow(len(test.want)))
			for row := 0; row < osVec.Length(); row++ {
				require.NoError(t, exec.Fill(0, row, []*vector.Vector{osVec}))
			}

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, types.T_uint64, results[0].GetType().Oid)
			require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[uint64](results[0]))

			results[0].Free(mp)
			exec.Free()
		})
	}
}

// TestNtileExec_ParameterValidation tests parameter validation for makeNtileExec
func TestNtileExec_ParameterValidation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("distinct_not_supported", func(t *testing.T) {
		_, err := makeNtileExec(mp, WinIdOfNtile, true, []types.Type{types.T_int64.ToType()})
		require.Error(t, err)
		require.Contains(t, err.Error(), "distinct")
	})

	t.Run("wrong_param_count", func(t *testing.T) {
		_, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "exactly one argument")
	})

	t.Run("valid_creation", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)
		require.NotNil(t, exec)
		exec.Free()
	})
}

// TestNtileExec_IntegerTypes tests Fill with various integer types
func TestNtileExec_IntegerTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	testCases := []struct {
		name     string
		typ      types.Type
		values   interface{}
		expected int64
	}{
		{"int8", types.T_int8.ToType(), []int8{5}, 5},
		{"int16", types.T_int16.ToType(), []int16{5}, 5},
		{"int32", types.T_int32.ToType(), []int32{5}, 5},
		{"int64", types.T_int64.ToType(), []int64{5}, 5},
		{"uint8", types.T_uint8.ToType(), []uint8{5}, 5},
		{"uint16", types.T_uint16.ToType(), []uint16{5}, 5},
		{"uint32", types.T_uint32.ToType(), []uint32{5}, 5},
		{"uint64", types.T_uint64.ToType(), []uint64{5}, 5},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{tc.typ})
			require.NoError(t, err)

			osVec := vector.NewVec(types.T_int64.ToType())
			err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6}, nil, mp)
			require.NoError(t, err)
			defer osVec.Free(mp)

			bucketVec := vector.NewVec(tc.typ)
			switch v := tc.values.(type) {
			case []int8:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []int16:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []int32:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []int64:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []uint8:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []uint16:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []uint32:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			case []uint64:
				err = vector.AppendFixedList(bucketVec, v, nil, mp)
			}
			require.NoError(t, err)
			defer bucketVec.Free(mp)

			err = exec.GroupGrow(5)
			require.NoError(t, err)

			for o := 0; o <= 5; o++ {
				err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
				require.NoError(t, err)
			}

			ntileExec := exec.(*ntileWindowExec)
			require.Equal(t, tc.expected, ntileExec.bucketCounts[0])

			exec.Free()
		})
	}
}

// TestNtileExec_BoundaryConditions tests boundary conditions
func TestNtileExec_BoundaryConditions(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("negative_bucket_count", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{-1}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		err = exec.Fill(0, 0, []*vector.Vector{osVec, bucketVec})
		require.Error(t, err)
		require.Contains(t, err.Error(), "positive")

		exec.Free()
	})

	t.Run("zero_bucket_count", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{0}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		err = exec.Fill(0, 0, []*vector.Vector{osVec, bucketVec})
		require.Error(t, err)

		exec.Free()
	})

	t.Run("null_bucket_count", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{0}, []bool{true}, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		err = exec.Fill(0, 0, []*vector.Vector{osVec, bucketVec})
		require.ErrorContains(t, err, "ntile bucket count cannot be NULL")

		ntileExec := exec.(*ntileWindowExec)
		require.Empty(t, ntileExec.groups[0])
		require.Zero(t, ntileExec.bucketCounts[0])

		exec.Free()
	})

	t.Run("empty_vectors", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		err = exec.Fill(0, 0, []*vector.Vector{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "requires vectors")

		exec.Free()
	})

	t.Run("only_os_vector", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		err = exec.GroupGrow(2)
		require.NoError(t, err)

		for o := 0; o <= 2; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec})
			require.NoError(t, err)
		}

		ntileExec := exec.(*ntileWindowExec)
		require.Equal(t, int64(1), ntileExec.bucketCounts[0])

		exec.Free()
	})
}

// TestNtileExec_AlgorithmEdgeCases tests flushNtile algorithm edge cases
func TestNtileExec_AlgorithmEdgeCases(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("1_row_1_bucket", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{1}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		for o := 0; o <= 1; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 1, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		require.Equal(t, int64(1), col[0])

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("1_row_multiple_buckets", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{5}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(1)
		require.NoError(t, err)

		for o := 0; o <= 1; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 1, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		require.Equal(t, int64(1), col[0])

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("multiple_rows_1_bucket", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{1}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(4)
		require.NoError(t, err)

		for o := 0; o <= 4; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 4, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		for i := 0; i < 4; i++ {
			require.Equal(t, int64(1), col[i])
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("even_distribution", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{3}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(6)
		require.NoError(t, err)

		for o := 0; o <= 6; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 6, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)
		expected := []int64{1, 1, 2, 2, 3, 3}
		for i := 0; i < 6; i++ {
			require.Equal(t, expected[i], col[i], "row %d", i)
		}

		resultVec.Free(mp)
		exec.Free()
	})

	t.Run("empty_group", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.GroupGrow(2)
		require.NoError(t, err)

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 2, resultVec.Length())

		resultVec.Free(mp)
		exec.Free()
	})
}

// TestNtileExec_MultiplePartitions tests multiple groups with different configurations
func TestNtileExec_MultiplePartitions(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("different_bucket_counts", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucket2Vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucket2Vec, []int64{2}, nil, mp)
		require.NoError(t, err)
		defer bucket2Vec.Free(mp)

		bucket3Vec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucket3Vec, []int64{3}, nil, mp)
		require.NoError(t, err)
		defer bucket3Vec.Free(mp)

		err = exec.GroupGrow(6)
		require.NoError(t, err)

		// Group 0: 3 rows, 2 buckets
		for o := 0; o <= 3; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucket2Vec})
			require.NoError(t, err)
		}

		// Group 1: 3 rows, 3 buckets
		for o := 0; o <= 3; o++ {
			err = exec.Fill(1, o, []*vector.Vector{osVec, bucket3Vec})
			require.NoError(t, err)
		}

		results, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)

		resultVec := results[0]
		require.Equal(t, 6, resultVec.Length())
		col := vector.MustFixedColNoTypeCheck[int64](resultVec)

		// Group 0: 1,1,2
		require.Equal(t, int64(1), col[0])
		require.Equal(t, int64(1), col[1])
		require.Equal(t, int64(2), col[2])

		// Group 1: 1,2,3
		require.Equal(t, int64(1), col[3])
		require.Equal(t, int64(2), col[4])
		require.Equal(t, int64(3), col[5])

		resultVec.Free(mp)
		exec.Free()
	})
}

// TestNtileExec_MemoryManagement tests memory-related methods
func TestNtileExec_MemoryManagement(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	t.Run("size_calculation", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		initialSize := exec.Size()
		require.GreaterOrEqual(t, initialSize, int64(0))

		osVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(osVec, []int64{0, 1, 2, 3}, nil, mp)
		require.NoError(t, err)
		defer osVec.Free(mp)

		bucketVec := vector.NewVec(types.T_int64.ToType())
		err = vector.AppendFixedList(bucketVec, []int64{2}, nil, mp)
		require.NoError(t, err)
		defer bucketVec.Free(mp)

		err = exec.GroupGrow(2)
		require.NoError(t, err)

		for o := 0; o <= 3; o++ {
			err = exec.Fill(0, o, []*vector.Vector{osVec, bucketVec})
			require.NoError(t, err)
		}

		sizeWithData := exec.Size()
		require.Greater(t, sizeWithData, initialSize)

		exec.Free()
	})

	t.Run("group_grow", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.GroupGrow(5)
		require.NoError(t, err)

		ntileExec := exec.(*ntileWindowExec)
		require.Len(t, ntileExec.groups, 5)
		require.Len(t, ntileExec.bucketCounts, 5)

		err = exec.GroupGrow(3)
		require.NoError(t, err)

		require.Len(t, ntileExec.groups, 8)
		require.Len(t, ntileExec.bucketCounts, 8)

		exec.Free()
	})

	t.Run("pre_allocate_groups", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		err = exec.PreAllocateGroups(10)
		require.NoError(t, err)

		exec.Free()
	})

	t.Run("get_opt_result", func(t *testing.T) {
		exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
		require.NoError(t, err)

		result := exec.GetOptResult()
		require.NotNil(t, result)

		exec.Free()
	})
}

// TestNtileExec_ErrorMethods tests methods that should panic or return errors
func TestNtileExec_ErrorMethods(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	ntileExec := exec.(*ntileWindowExec)

	t.Run("bulk_fill_panics", func(t *testing.T) {
		require.Panics(t, func() {
			_ = ntileExec.BulkFill(0, nil)
		})
	})

	t.Run("set_extra_information_panics", func(t *testing.T) {
		require.Panics(t, func() {
			_ = ntileExec.SetExtraInformation(nil, 0)
		})
	})

	t.Run("batch_fill_returns_nil", func(t *testing.T) {
		err := ntileExec.BatchFill(0, nil, nil)
		require.NoError(t, err)
	})

	t.Run("merge_returns_nil", func(t *testing.T) {
		err := ntileExec.Merge(nil, 0, 0)
		require.NoError(t, err)
	})

	t.Run("batch_merge_returns_nil", func(t *testing.T) {
		err := ntileExec.BatchMerge(nil, 0, nil)
		require.NoError(t, err)
	})

	exec.Free()
}

// TestNtileExec_InvalidBucketType tests Fill with invalid bucket type
func TestNtileExec_InvalidBucketType(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeNtileExec(mp, WinIdOfNtile, false, []types.Type{types.T_varchar.ToType()})
	require.NoError(t, err)

	osVec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(osVec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer osVec.Free(mp)

	bucketVec := vector.NewVec(types.T_varchar.ToType())
	err = vector.AppendStringList(bucketVec, []string{"invalid"}, nil, mp)
	require.NoError(t, err)
	defer bucketVec.Free(mp)

	err = exec.GroupGrow(1)
	require.NoError(t, err)

	err = exec.Fill(0, 0, []*vector.Vector{osVec, bucketVec})
	require.Error(t, err)
	require.Contains(t, err.Error(), "integer type")

	exec.Free()
}

// TestCumeDistWindowExec_BasicOperations tests basic operations of cumeDistWindowExec
func TestCumeDistWindowExec_BasicOperations(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)
	require.NotNil(t, exec)

	// Test PreAllocateGroups
	err = exec.PreAllocateGroups(5)
	require.NoError(t, err)

	// Test GetOptResult
	result := exec.GetOptResult()
	require.NotNil(t, result)

	// Test Size
	size := exec.Size()
	require.GreaterOrEqual(t, size, int64(0))

	exec.Free()
}

// TestCumeDistWindowExec_FillAndFlush tests Fill and Flush operations
func TestCumeDistWindowExec_FillAndFlush(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2, 3}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(4)
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)

	resultVec := results[0]
	require.Equal(t, 4, resultVec.Length())

	resultVec.Free(mp)
	exec.Free()
}

// TestCumeDistWindowExec_Merge tests Merge operation
func TestCumeDistWindowExec_Merge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec1, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec1.GroupGrow(2)
	require.NoError(t, err)
	err = exec2.GroupGrow(2)
	require.NoError(t, err)

	err = exec1.Fill(0, 0, []*vector.Vector{vec})
	require.NoError(t, err)
	err = exec2.Fill(0, 0, []*vector.Vector{vec})
	require.NoError(t, err)

	err = exec1.Merge(exec2, 0, 0)
	require.NoError(t, err)

	exec1.Free()
	exec2.Free()
}

// TestCumeDistWindowExec_BatchMerge tests BatchMerge operation
func TestCumeDistWindowExec_BatchMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec1, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec1.GroupGrow(3)
	require.NoError(t, err)
	err = exec2.GroupGrow(3)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = exec1.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec2.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	groups := []uint64{1, 2, 3}
	err = exec1.BatchMerge(exec2, 0, groups)
	require.NoError(t, err)

	exec1.Free()
	exec2.Free()
}

// TestCumeDistWindowExec_BatchMergeWithNotMatched tests BatchMerge with GroupNotMatched
func TestCumeDistWindowExec_BatchMergeWithNotMatched(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec1, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec1.GroupGrow(3)
	require.NoError(t, err)
	err = exec2.GroupGrow(3)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = exec1.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
		err = exec2.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	groups := []uint64{GroupNotMatched, 2, GroupNotMatched}
	err = exec1.BatchMerge(exec2, 0, groups)
	require.NoError(t, err)

	exec1.Free()
	exec2.Free()
}

// TestCumeDistWindowExec_PanicMethods tests methods that should panic
func TestCumeDistWindowExec_PanicMethods(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	cExec := exec.(*cumeDistWindowExec)

	// Test BulkFill (should panic)
	require.Panics(t, func() {
		_ = cExec.BulkFill(0, nil)
	})

	// Test BatchFill (should panic)
	require.Panics(t, func() {
		_ = cExec.BatchFill(0, nil, nil)
	})

	// Test SetExtraInformation (should panic)
	require.Panics(t, func() {
		_ = cExec.SetExtraInformation(nil, 0)
	})

	exec.Free()
}

// TestCumeDistWindowExec_SaveIntermediateResult tests SaveIntermediateResult
func TestCumeDistWindowExec_SaveIntermediateResult(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(2)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)

	// Test SaveIntermediateResult
	var buf bytes.Buffer
	flags := [][]uint8{{1, 1}}
	err = cExec.SaveIntermediateResult(2, flags, &buf)
	require.NoError(t, err)

	exec.Free()
}

// TestCumeDistWindowExec_SaveIntermediateResultOfChunk tests SaveIntermediateResultOfChunk
func TestCumeDistWindowExec_SaveIntermediateResultOfChunk(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(2)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)

	// Test SaveIntermediateResultOfChunk
	var buf bytes.Buffer
	err = cExec.SaveIntermediateResultOfChunk(0, &buf)
	require.NoError(t, err)

	exec.Free()
}

// TestCumeDistWindowExec_UnmarshalFromReader tests UnmarshalFromReader
func TestCumeDistWindowExec_UnmarshalFromReader(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(2)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)

	// Save to buffer
	var buf bytes.Buffer
	flags := [][]uint8{{1, 1}}
	err = cExec.SaveIntermediateResult(2, flags, &buf)
	require.NoError(t, err)

	// Create new exec and unmarshal
	exec2, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)
	cExec2 := exec2.(*cumeDistWindowExec)

	err = cExec2.UnmarshalFromReader(&buf, mp)
	require.NoError(t, err)

	exec.Free()
	exec2.Free()
}

func TestSingleWindowFailedUnmarshalPreservesOwnedState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	info := singleAggInfo{
		aggID:     WinIdOfRowNumber,
		retType:   types.T_uint64.ToType(),
		emptyNull: false,
	}
	input := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		input, []int64{7, 8, 9}, nil, mp))
	defer input.Free(mp)

	target := makeRankDenseRankRowNumber(mp, info).(*singleWindowExec)
	require.NoError(t, target.GroupGrow(1))
	require.NoError(t, target.Fill(0, 0, []*vector.Vector{input}))

	source := makeRankDenseRankRowNumber(mp, info).(*singleWindowExec)
	require.NoError(t, source.GroupGrow(1))
	require.NoError(t, source.Fill(0, 1, []*vector.Vector{input}))
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(
		1, [][]uint8{{1}}, &encoded))
	broken := encoded.Bytes()[:encoded.Len()-1]

	require.Error(t, target.UnmarshalFromReader(bytes.NewReader(broken), mp))
	require.Equal(t, []i64Slice{{7}}, target.groups)
	require.Len(t, target.ret.resultList, 1)
	require.Equal(t, 1, target.ret.resultList[0].Length())

	source.Free()
	target.Free()
}

// TestCumeDistWindowExec_SizeWithData tests Size with actual data
func TestCumeDistWindowExec_SizeWithData(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	exec, err := makeWindowExec(mp, WinIdOfCumeDist, false)
	require.NoError(t, err)

	vec := vector.NewVec(types.T_int64.ToType())
	err = vector.AppendFixedList(vec, []int64{0, 1, 2, 3, 4}, nil, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	err = exec.GroupGrow(5)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		err = exec.Fill(i, i, []*vector.Vector{vec})
		require.NoError(t, err)
	}

	cExec := exec.(*cumeDistWindowExec)
	size := cExec.Size()
	require.Greater(t, size, int64(0))

	exec.Free()
}
