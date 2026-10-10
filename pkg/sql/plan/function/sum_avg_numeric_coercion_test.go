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
	"bytes"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSumAvgNumericCoercionExecAndMerge(t *testing.T) {
	datetime1, err := types.ParseDatetime("2026-01-01 00:00:00.100000", 6)
	require.NoError(t, err)
	datetime2, err := types.ParseDatetime("2026-01-01 00:00:00.200000", 6)
	require.NoError(t, err)
	time1, err := types.ParseTime("-00:00:00.100000", 6)
	require.NoError(t, err)
	time2, err := types.ParseTime("-00:00:00.200000", 6)
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		input    FunctionTestInput
		sum, avg string
	}{
		{"string-prefix", NewFunctionTestInput(types.T_varchar.ToType(),
			[]string{"1", "2.5", " 3 ", "-4e0", "12x", "", "abc", "ignored"},
			[]bool{false, false, false, false, false, false, false, true}), "14.5", "2.0714285714285716"},
		{"date", NewFunctionTestInput(types.T_date.ToType(),
			[]types.Date{types.DateFromCalendar(2026, 1, 1), types.DateFromCalendar(2026, 1, 2), 0},
			[]bool{false, false, true}), "40520203", "20260101.5000"},
		{"negative-time", NewFunctionTestInput(types.T_time.ToTypeWithScale(6),
			[]types.Time{time1, time2, 0}, []bool{false, false, true}), "-0.300000", "-0.1500000000"},
		{"datetime", NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6),
			[]types.Datetime{datetime1, datetime2, 0}, []bool{false, false, true}),
			"40520202000000.300000", "20260101000000.1500000000"},
		{"timestamp", NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6),
			[]types.Timestamp{datetime1.ToTimestamp(time.UTC), datetime2.ToTimestamp(time.UTC), 0},
			[]bool{false, false, true}), "40520202000000.300000", "20260101000000.1500000000"},
	} {
		for _, name := range []string{"sum", "avg"} {
			for _, merge := range []bool{false, true} {
				mode := "resident"
				if merge {
					mode = "partial-round-trip"
				}
				t.Run(name+"/"+tc.name+"/"+mode, func(t *testing.T) {
					mp := mpool.MustNewZeroNoFixed()
					t.Cleanup(func() {
						defer mpool.DeleteMPool(mp)
						require.Zero(t, mp.CurrNB())
					})
					proc := testutil.NewProcess(t, testutil.WithMPool(mp))
					proc.GetSessionInfo().TimeZone = time.UTC
					bound, err := GetFunctionByName(proc.Ctx, name, []types.Type{tc.input.typ})
					require.NoError(t, err)
					targets, shouldCast := bound.ShouldDoImplicitTypeCast()
					require.True(t, shouldCast)
					nsp := nulls.NewWithSize(len(tc.input.nullList))
					for i, isNull := range tc.input.nullList {
						if isNull {
							nsp.Set(uint64(i))
						}
					}
					source := newVectorByType(mp, tc.input.typ, tc.input.values, nsp)
					t.Cleanup(func() { source.Free(mp) })
					target := vector.NewVec(targets[0])
					t.Cleanup(func() { target.Free(mp) })
					cast, err := GetFunctionByName(proc.Ctx, "cast", []types.Type{tc.input.typ, targets[0]})
					require.NoError(t, err)
					values, err := RunFunctionDirectly(proc, cast.GetEncodedOverloadID(),
						[]*vector.Vector{source, target}, source.Length())
					require.NoError(t, err)
					t.Cleanup(func() { values.Free(mp) })

					makeExec := func() aggexec.AggFuncExec {
						exec, err := aggexec.MakeAgg(mp, bound.GetEncodedOverloadID(), false, targets[0])
						require.NoError(t, err)
						t.Cleanup(exec.Free)
						return exec
					}
					exec := makeExec()
					require.NoError(t, exec.GroupGrow(1))
					if merge {
						other := makeExec()
						require.NoError(t, other.GroupGrow(1))
						for row := range values.Length() {
							partial := exec
							if row%2 != 0 {
								partial = other
							}
							require.NoError(t, partial.Fill(0, row, []*vector.Vector{values}))
						}
						var wire bytes.Buffer
						require.NoError(t, other.SaveIntermediateResult(1, [][]uint8{{1}}, &wire))
						restored := makeExec()
						require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(wire.Bytes()), mp))
						require.NoError(t, exec.Merge(restored, 0, 0))
					} else {
						require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))
					}
					results, err := exec.Flush()
					require.NoError(t, err)
					require.Len(t, results, 1)
					t.Cleanup(func() { results[0].Free(mp) })
					require.Equal(t, bound.GetReturnType(), *results[0].GetType())
					require.False(t, results[0].IsNull(0))
					want := tc.sum
					if name == "avg" {
						want = tc.avg
					}
					if results[0].GetType().Oid == types.T_float64 {
						expected := 14.5
						if name == "avg" {
							expected = 14.5 / 7
						}
						require.InDelta(t, expected, vector.GetFixedAtNoTypeCheck[float64](results[0], 0), 1e-14)
					} else {
						require.Equal(t, want,
							vector.GetFixedAtNoTypeCheck[types.Decimal256](results[0], 0).Format(results[0].GetType().Scale))
					}
				})
			}
		}
	}
}
