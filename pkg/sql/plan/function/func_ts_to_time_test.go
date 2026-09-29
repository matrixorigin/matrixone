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
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestTSToTimestampPreservesAbsoluteInstant(t *testing.T) {
	physical := time.Date(2026, time.August, 26, 5, 45, 12, 654321987, time.UTC).UnixNano()
	values := []types.TS{
		types.BuildTS(physical, 0),
		types.BuildTS(physical, 42),
		types.BuildTS(physical, 99),
	}
	want := types.UnixNanoToTimestamp(physical)

	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		zone *time.Location
	}{
		{name: "UTC", zone: time.UTC},
		{name: "UTC+8", zone: time.FixedZone("UTC+8", 8*60*60)},
		{name: "America/New_York", zone: newYork},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().TimeZone = tc.zone
			input := testutil.MakeTSVector(values, []uint64{2}, proc.Mp())
			defer input.Free(proc.Mp())

			result := vector.NewFunctionResultWrapper(types.T_timestamp.ToTypeWithScale(6), proc.Mp())
			defer result.Free()
			require.NoError(t, result.PreExtendAndReset(len(values)))
			require.NoError(t, TSToTimestamp(
				[]*vector.Vector{input}, result, proc, len(values), nil))

			output := result.GetResultVector()
			require.Equal(t, int32(6), output.GetType().Scale)
			got := vector.MustFixedColWithTypeCheck[types.Timestamp](output)
			require.Equal(t, want, got[0])
			// The HLC logical component has no representation in SQL TIMESTAMP.
			require.Equal(t, got[0], got[1])
			require.True(t, output.GetNulls().Contains(2))
		})
	}
}

func TestCastTSToTimestampPreservesAbsoluteInstant(t *testing.T) {
	physical := time.Date(2026, time.August, 26, 5, 45, 12, 654321987, time.UTC).UnixNano()
	values := []types.TS{types.BuildTS(physical, 7), types.BuildTS(physical, 8)}
	want := types.UnixNanoToTimestamp(physical)

	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		zone *time.Location
	}{
		{name: "UTC", zone: time.UTC},
		{name: "UTC+8", zone: time.FixedZone("UTC+8", 8*60*60)},
		{name: "America/New_York", zone: newYork},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().TimeZone = tc.zone
			input := testutil.MakeTSVector(values, []uint64{1}, proc.Mp())
			defer input.Free(proc.Mp())
			target := vector.NewConstNull(types.T_timestamp.ToTypeWithScale(6), len(values), proc.Mp())
			defer target.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_timestamp.ToTypeWithScale(6), proc.Mp())
			defer result.Free()

			require.NoError(t, result.PreExtendAndReset(len(values)))
			require.NoError(t, NewCast(
				[]*vector.Vector{input, target}, result, proc, len(values), nil))

			output := result.GetResultVector()
			got := vector.MustFixedColWithTypeCheck[types.Timestamp](output)
			require.Equal(t, want, got[0])
			require.True(t, output.GetNulls().Contains(1))
		})
	}
}

func TestTSToTimestampPrecisionContract(t *testing.T) {
	physical := time.Date(2026, time.August, 26, 5, 45, 12, 125678987, time.UTC).UnixNano()
	value := types.BuildTS(physical, 1)

	for _, tc := range []struct {
		name  string
		scale int64
	}{
		{name: "scale-0", scale: 0},
		{name: "scale-2", scale: 2},
		{name: "scale-6", scale: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			input := testutil.MakeTSVector([]types.TS{value}, nil, proc.Mp())
			defer input.Free(proc.Mp())
			precision, err := vector.NewConstFixed(types.T_int64.ToType(), tc.scale, 1, proc.Mp())
			require.NoError(t, err)
			defer precision.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_timestamp.ToTypeWithScale(6), proc.Mp())
			defer result.Free()

			require.NoError(t, result.PreExtendAndReset(1))
			require.NoError(t, TSToTimestamp(
				[]*vector.Vector{input, precision}, result, proc, 1, nil))

			output := result.GetResultVector()
			got := vector.MustFixedColWithTypeCheck[types.Timestamp](output)
			require.Equal(t, int32(tc.scale), output.GetType().Scale)
			require.Equal(t,
				types.UnixNanoToTimestamp(physical).TruncateToScale(int32(tc.scale)), got[0])

		})
	}
}

func TestTSToTimestampRejectsInvalidPrecision(t *testing.T) {
	for _, tc := range []struct {
		name  string
		scale int64
	}{
		{name: "negative", scale: -1},
		{name: "too-large", scale: 7},
		{name: "int32-overflow", scale: 1<<32 + 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			input := testutil.MakeTSVector([]types.TS{types.BuildTS(1, 0)}, nil, proc.Mp())
			defer input.Free(proc.Mp())
			precision, err := vector.NewConstFixed(types.T_int64.ToType(), tc.scale, 1, proc.Mp())
			require.NoError(t, err)
			defer precision.Free(proc.Mp())
			result := vector.NewFunctionResultWrapper(types.T_timestamp.ToTypeWithScale(6), proc.Mp())
			defer result.Free()

			require.NoError(t, result.PreExtendAndReset(1))
			err = TSToTimestamp([]*vector.Vector{input, precision}, result, proc, 1, nil)
			require.Error(t, err)
			require.Contains(t, err.Error(), "precision")

		})
	}
}

func TestTSToTimestampPrecisionMustBeConstantOrNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	values := []types.TS{types.BuildTS(1, 0), types.BuildTS(2, 0)}
	input := testutil.MakeTSVector(values, nil, proc.Mp())
	defer input.Free(proc.Mp())

	t.Run("non-constant", func(t *testing.T) {
		precision := testutil.MakeInt64Vector([]int64{2, 3}, nil, proc.Mp())
		defer precision.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_timestamp.ToTypeWithScale(6), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(len(values)))

		err := TSToTimestamp(
			[]*vector.Vector{input, precision}, result, proc, len(values), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be constant")
	})

	t.Run("constant-null", func(t *testing.T) {
		precision := vector.NewConstNull(types.T_int64.ToType(), len(values), proc.Mp())
		defer precision.Free(proc.Mp())
		result := vector.NewFunctionResultWrapper(types.T_timestamp.ToTypeWithScale(6), proc.Mp())
		defer result.Free()
		require.NoError(t, result.PreExtendAndReset(len(values)))

		require.NoError(t, TSToTimestamp(
			[]*vector.Vector{input, precision}, result, proc, len(values), nil))
		require.True(t, result.GetResultVector().GetNulls().Contains(0))
		require.True(t, result.GetResultVector().GetNulls().Contains(1))
	})

	t.Run("untyped-null-resolves", func(t *testing.T) {
		_, err := GetFunctionByName(context.Background(), "ts_to_time", []types.Type{
			types.T_TS.ToType(),
			types.T_any.ToType(),
		})
		require.NoError(t, err)
	})
}

var benchmarkTransactionTimestamp types.Timestamp

func BenchmarkTransactionTSToTimestamp(b *testing.B) {
	physical := time.Date(2026, time.August, 26, 5, 45, 12, 654321987, time.UTC).UnixNano()
	ts := types.BuildTS(physical, 42)

	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkTransactionTimestamp = timestampFromTransactionTS(ts, 6)
		}
	})

	b.Run("legacy-string-round-trip", func(b *testing.B) {
		b.ReportAllocs()
		var err error
		for i := 0; i < b.N; i++ {
			physical := ts.Physical()
			t := time.Unix(physical/1e9, physical%1e9).UTC()
			benchmarkTransactionTimestamp, err = types.ParseTimestamp(
				time.UTC, t.Format("2006-01-02 15:04:05.999999"), 6)
		}
		require.NoError(b, err)
	})
}
