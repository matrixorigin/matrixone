// Copyright 2026 Matrix Origin
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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalReleaseOverloadABI(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	dt, err := types.ParseDatetime("2024-01-02 03:04:05.123456", 6)
	require.NoError(t, err)
	// These IDs, result types and padded strings come from v4.2.4's
	// registry/execution, not the current binder. Exercise registry lookup as
	// persisted DEFAULT/ON UPDATE expressions do after an upgrade.
	for _, tc := range []struct {
		id    int32
		input FunctionTestInput
		want  FunctionTestResult
	}{
		{0, NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{dt}, nil), NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"04"}, nil)},
		{1, NewFunctionTestInput(types.T_date.ToType(), []types.Date{dt.ToDate()}, nil), NewFunctionTestResult(types.T_uint32.ToType(), false, []uint32{1}, nil)},
		{2, NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{types.TimeFromClock(false, 3, 4, 5, 123456)}, nil), NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"04"}, nil)},
		{3, NewFunctionTestInput(types.T_varchar.ToType(), []string{"2024-01-02 03:04:05.123456"}, nil), NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"04"}, nil)},
		{4, NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{dt.ToTimestamp(time.UTC)}, nil), NewFunctionTestResult(types.T_varchar.ToType(), false, []string{"04"}, nil)},
	} {
		t.Run(fmt.Sprintf("extract/%d", tc.id), func(t *testing.T) {
			unit := "minute"
			if tc.id == 1 {
				unit = "month"
			}
			ov, err := GetFunctionById(proc.Ctx, EncodeOverloadID(EXTRACT, tc.id))
			require.NoError(t, err)
			inputs := []FunctionTestInput{NewFunctionTestConstInput(types.T_varchar.ToType(), []string{unit}, nil), tc.input}
			fc := NewFunctionTestCase(proc, inputs, tc.want, fEvalFn(ov.newOp()))
			ok, info := fc.Run()
			require.True(t, ok, info)
			argTypes := []types.Type{types.T_varchar.ToType(), ov.args[1].ToType()}
			bound, err := GetFunctionByName(proc.Ctx, "extract", argTypes)
			require.NoError(t, err)
			require.Equal(t, EncodeOverloadID(EXTRACT, tc.id+5), bound.GetEncodedOverloadID())
			require.Equal(t, types.T_int64, bound.GetReturnType().Oid)
		})
	}
	for _, fn := range []struct {
		id   int32
		name string
		want types.Datetime
	}{
		{ADDTIME, "addtime", dt + types.Datetime(types.MicroSecsPerSec)},
		{SUBTIME, "subtime", dt - types.Datetime(types.MicroSecsPerSec)},
	} {
		signatures := [][2]types.T{{types.T_varchar, types.T_varchar}, {types.T_char, types.T_varchar}}
		if fn.id == SUBTIME {
			signatures = append(signatures, [2]types.T{types.T_varchar, types.T_char}, [2]types.T{types.T_char, types.T_char})
		}
		signatures = append(signatures, [2]types.T{types.T_text, types.T_varchar})
		for i, sig := range signatures {
			typ := sig[0]
			t.Run(fmt.Sprintf("%s/%s", fn.name, typ), func(t *testing.T) {
				ov, err := GetFunctionById(proc.Ctx, EncodeOverloadID(fn.id, int32(6+i)))
				require.NoError(t, err)
				inputs := []FunctionTestInput{
					NewFunctionTestInput(typ.ToType(), []string{"2024-01-02 03:04:05.123456", "bad", "", "bad"}, []bool{false, false, true, false}),
					NewFunctionTestConstInput(sig[1].ToType(), []string{"00:00:01"}, nil),
				}
				want := NewFunctionTestResult(types.T_datetime.ToTypeWithScale(6), false, []types.Datetime{fn.want, 0, 0, 0}, []bool{false, true, true, true})
				fc := NewFunctionTestCase(proc, inputs, want, fEvalFn(ov.newOp())).
					WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, true, true, false}})
				ok, info := fc.Run()
				require.True(t, ok, info)
				bound, err := GetFunctionByName(proc.Ctx, fn.name, []types.Type{typ.ToType(), sig[1].ToType()})
				require.NoError(t, err)
				require.Equal(t, EncodeOverloadID(fn.id, int32(6+len(signatures)+i)), bound.GetEncodedOverloadID())
				require.Equal(t, types.T_varchar, bound.GetReturnType().Oid)
			})
		}
	}
}
