// Copyright 2021 - 2022 Matrix Origin
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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const utcFunctionTestUnixNano = int64(1704150245123456789) // 2024-01-01 23:04:05.123456789 UTC

func runUTCFunction(t *testing.T, proc *process.Process, name string, inputs []*vector.Vector) *vector.Vector {
	t.Helper()
	args := make([]types.Type, len(inputs))
	for i, input := range inputs {
		args[i] = *input.GetType()
	}
	fn, err := GetFunctionByName(proc.Ctx, name, args)
	require.NoErrorf(t, err, "%s must resolve", name)
	out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), inputs, 1)
	require.NoErrorf(t, err, "%s must execute", name)
	return out
}

func utcScaleInput(t *testing.T, proc *process.Process, scale int64) *vector.Vector {
	t.Helper()
	input, err := vector.NewConstFixed(types.T_int64.ToType(), scale, 1, proc.Mp())
	require.NoError(t, err)
	return input
}

func TestUTCFunctionsUseStatementTimeAndUTC(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.UnixTime = utcFunctionTestUnixNano
	proc.GetSessionInfo().TimeZone = time.FixedZone("UTC+8", 8*60*60)

	wantDate, err := types.ParseDateCast("2024-01-01")
	require.NoError(t, err)
	wantTime, err := types.ParseTime("23:04:05.123456", 6)
	require.NoError(t, err)
	wantDatetime, err := types.ParseDatetime("2024-01-01 23:04:05.123456", 6)
	require.NoError(t, err)

	date := runUTCFunction(t, proc, "utc_date", nil)
	defer date.Free(proc.Mp())
	require.Equal(t, types.T_date, date.GetType().Oid)
	require.Equal(t, wantDate, vector.MustFixedColWithTypeCheck[types.Date](date)[0])

	timeInput := utcScaleInput(t, proc, 6)
	defer timeInput.Free(proc.Mp())
	utcTime := runUTCFunction(t, proc, "utc_time", []*vector.Vector{timeInput})
	defer utcTime.Free(proc.Mp())
	require.Equal(t, types.New(types.T_time, 0, 6), *utcTime.GetType())
	require.Equal(t, wantTime, vector.MustFixedColWithTypeCheck[types.Time](utcTime)[0])

	timestampInput := utcScaleInput(t, proc, 6)
	defer timestampInput.Free(proc.Mp())
	utcTimestamp := runUTCFunction(t, proc, "utc_timestamp", []*vector.Vector{timestampInput})
	defer utcTimestamp.Free(proc.Mp())
	require.Equal(t, types.New(types.T_datetime, 0, 6), *utcTimestamp.GetType())
	require.Equal(t, wantDatetime, vector.MustFixedColWithTypeCheck[types.Datetime](utcTimestamp)[0])
}

func TestUTCFunctionsHonorFractionalSecondPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.UnixTime = utcFunctionTestUnixNano

	for _, scale := range []int64{0, 3, 6} {
		t.Run("utc_time", func(t *testing.T) {
			input := utcScaleInput(t, proc, scale)
			defer input.Free(proc.Mp())
			out := runUTCFunction(t, proc, "utc_time", []*vector.Vector{input})
			defer out.Free(proc.Mp())
			require.Equal(t, types.New(types.T_time, 0, int32(scale)), *out.GetType())
			want, err := types.ParseTime("23:04:05.123456", int32(scale))
			require.NoError(t, err)
			require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Time](out)[0])
		})

		t.Run("utc_timestamp", func(t *testing.T) {
			input := utcScaleInput(t, proc, scale)
			defer input.Free(proc.Mp())
			out := runUTCFunction(t, proc, "utc_timestamp", []*vector.Vector{input})
			defer out.Free(proc.Mp())
			require.Equal(t, types.New(types.T_datetime, 0, int32(scale)), *out.GetType())
			want, err := types.ParseDatetime("2024-01-01 23:04:05.123456", int32(scale))
			require.NoError(t, err)
			require.Equal(t, want, vector.MustFixedColWithTypeCheck[types.Datetime](out)[0])
		})
	}
}

func TestUTCFunctionsRejectInvalidFractionalSecondPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, name := range []string{"utc_time", "utc_timestamp"} {
		for _, scale := range []int64{-1, 7, 2147483648} {
			t.Run(fmt.Sprintf("%s/%d", name, scale), func(t *testing.T) {
				input := utcScaleInput(t, proc, scale)
				defer input.Free(proc.Mp())
				fn, err := GetFunctionByName(proc.Ctx, name, []types.Type{types.T_int64.ToType()})
				require.NoError(t, err)
				out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
				require.Error(t, err)
				if out != nil {
					out.Free(proc.Mp())
				}
				require.Contains(t, err.Error(), name)
				if scale < 0 {
					require.Contains(t, err.Error(), "negative precision -1 specified")
					require.NotContains(t, err.Error(), "Too-big precision")
				} else {
					require.Contains(t, err.Error(), fmt.Sprintf("Too-big precision %d specified", scale))
					require.NotContains(t, err.Error(), "negative precision")
				}
			})
		}
	}
}

func TestUTCFunctionsRejectNonConstantFractionalSecondPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)

	nonConstantInputs := [][]int64{
		{0, 6},
		{6, 0},
	}
	for _, name := range []string{"utc_time", "utc_timestamp"} {
		for _, values := range nonConstantInputs {
			t.Run(name, func(t *testing.T) {
				input := vector.NewVec(types.T_int64.ToType())
				defer input.Free(proc.Mp())
				require.NoError(t, vector.AppendFixedList(input, values, nil, proc.Mp()))

				fn, err := GetFunctionByName(proc.Ctx, name, []types.Type{types.T_int64.ToType()})
				require.NoError(t, err)
				out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, len(values))
				require.ErrorContains(t, err, "constant integer between 0 and 6")
				if out != nil {
					out.Free(proc.Mp())
				}
			})
		}
	}
}

func TestUTCFunctionsRejectNullFractionalSecondPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, name := range []string{"utc_time", "utc_timestamp"} {
		t.Run(name, func(t *testing.T) {
			input := vector.NewConstNull(types.T_int64.ToType(), 1, proc.Mp())
			defer input.Free(proc.Mp())
			fn, err := GetFunctionByName(proc.Ctx, name, []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)
			out, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{input}, 1)
			require.ErrorContains(t, err, "constant integer between 0 and 6")
			if out != nil {
				out.Free(proc.Mp())
			}
		})
	}
}
