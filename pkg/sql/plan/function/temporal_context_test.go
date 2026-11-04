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
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTemporalContextResolution(t *testing.T) {
	for _, oid := range []types.T{types.T_date, types.T_datetime, types.T_time, types.T_timestamp} {
		source := oid.ToTypeWithScale(6)
		for _, name := range []string{"not", "unary_mark", "istrue", "isfalse", "isnottrue", "isnotfalse", "and", "or", "xor"} {
			t.Run(oid.String()+"/"+name, func(t *testing.T) {
				args := []types.Type{source}
				if name == "and" || name == "or" || name == "xor" {
					args = append(args, types.T_bool.ToType())
				}
				resolved, err := GetFunctionByName(context.Background(), name, args)
				require.NoError(t, err)
				require.True(t, resolved.needCast)
				require.Equal(t, types.T_bool, resolved.targetTypes[0].Oid)
				require.Equal(t, types.T_bool, resolved.retType.Oid)
			})
		}
		for _, name := range []string{"abs", "sign", "sqrt", "floor", "ceil", "ceiling", "round", "truncate"} {
			t.Run(oid.String()+"/"+name, func(t *testing.T) {
				args := []types.Type{source}
				if name == "round" || name == "truncate" {
					args = append(args, types.T_int64.ToType())
				}
				resolved, err := GetFunctionByName(context.Background(), name, args)
				require.NoError(t, err)
				require.True(t, resolved.needCast)
				require.Equal(t, types.T_float64, resolved.targetTypes[0].Oid)
				if name == "sign" {
					require.Equal(t, types.T_int64, resolved.retType.Oid)
				} else {
					require.Equal(t, types.T_float64, resolved.retType.Oid)
				}
			})
		}
		for _, name := range []string{"round", "truncate"} {
			resolved, err := GetFunctionByName(context.Background(), name, []types.Type{source, types.T_bool.ToType()})
			require.NoError(t, err)
			require.Equal(t, types.T_float64, resolved.targetTypes[0].Oid)
			require.Equal(t, types.T_int64, resolved.targetTypes[1].Oid)
		}
		for _, target := range []types.T{types.T_bool, types.T_float64} {
			require.True(t, IfTypeCastSupported(oid, target))
			can, _ := fixedImplicitTypeCast(source, target)
			require.False(t, can, "temporal acceptance must stay local to the consuming context")
		}
	}
}

func TestTemporalContextConstantAndTimezone(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	datetime, err := types.ParseDatetime("2024-01-02 23:04:05.125000", 6)
	require.NoError(t, err)
	stamp := datetime.ToTimestamp(time.UTC)
	for _, zone := range []struct {
		name   string
		offset int
		want   float64
	}{
		{"UTC", 0, 20240102230405.125},
		{"UTC+8", 8 * 3600, 20240103070405.125},
	} {
		t.Run(zone.name, func(t *testing.T) {
			proc.GetSessionInfo().TimeZone = time.FixedZone(zone.name, zone.offset)
			for _, isNull := range []bool{false, true} {
				fc := NewFunctionTestCase(proc, []FunctionTestInput{
					NewFunctionTestConstInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{stamp}, []bool{isNull}),
					NewFunctionTestInput(types.T_float64.ToType(), []float64{}, nil),
				}, NewFunctionTestResult(types.T_float64.ToType(), false, []float64{zone.want, zone.want, zone.want}, []bool{isNull, isNull, isNull}), NewCast)
				fc.fnLength = 3
				ok, info := fc.RunAndFree()
				require.True(t, ok, info)
			}
		})
	}
}

func TestTemporalContextCasts(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	date, err := types.ParseDateCast("2024-01-02")
	require.NoError(t, err)
	datetime, err := types.ParseDatetime("2024-01-02 03:04:05.125000", 6)
	require.NoError(t, err)
	timestamp := datetime.ToTimestamp(time.UTC)
	for _, tc := range []struct {
		name   string
		input  FunctionTestInput
		floats []float64
		bools  []bool
	}{
		{"date", NewFunctionTestInput(types.T_date.ToType(), []types.Date{date, types.ZeroDate, 0, date}, []bool{false, false, false, true}), []float64{20240102, 0, 10101, 0}, []bool{true, false, true, false}},
		{"datetime", NewFunctionTestInput(types.T_datetime.ToTypeWithScale(6), []types.Datetime{datetime, types.ZeroDatetime, 0, datetime}, []bool{false, false, false, true}), []float64{20240102030405.125, 0, 10101000000, 0}, []bool{true, false, true, false}},
		{"timestamp", NewFunctionTestInput(types.T_timestamp.ToTypeWithScale(6), []types.Timestamp{timestamp, types.ZeroTimestamp, 0, timestamp}, []bool{false, false, false, true}), []float64{20240102030405.125, 0, 10101000000, 0}, []bool{true, false, true, false}},
		{"time", NewFunctionTestInput(types.T_time.ToTypeWithScale(6), []types.Time{1, -1, 0, 0}, []bool{false, false, false, true}), []float64{0.000001, -0.000001, 0, 0}, []bool{true, true, false, false}},
	} {
		for _, target := range []types.T{types.T_bool, types.T_float64} {
			t.Run(tc.name+"/"+target.String(), func(t *testing.T) {
				var empty, want any = []bool{}, tc.bools
				if target == types.T_float64 {
					empty, want = []float64{}, tc.floats
				}
				for _, selection := range []*FunctionSelectList{nil, {AnyNull: true, SelectList: []bool{true, false, true, true}}, {AllNull: true}} {
					nulls := []bool{false, false, false, true}
					if selection != nil {
						if selection.AllNull {
							nulls = []bool{true, true, true, true}
						} else {
							nulls[1] = true
						}
					}
					fc := NewFunctionTestCase(proc, []FunctionTestInput{tc.input, NewFunctionTestInput(target.ToType(), empty, nil)}, NewFunctionTestResult(target.ToType(), false, want, nulls), NewCast).WithSelectList(selection)
					ok, info := fc.RunAndFree()
					require.True(t, ok, info)
				}
			})
		}
	}
}
