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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

var fixedTruncBenchmarkSink types.Datetime

func BenchmarkDateTruncFixedCore(b *testing.B) {
	modern := make([]types.Datetime, 8192)
	base := time.Date(2026, 1, 1, 0, 0, 0, 123456000, time.UTC)
	for index := range modern {
		modern[index] = fixedTruncDatetime(base.Add(time.Duration(index) * 10 * time.Second))
	}
	for _, tc := range []struct {
		name, unit string
		values     []types.Datetime
	}{
		{"second", "second", modern}, {"minute", "minute", modern},
		{"hour", "hour", modern}, {"day", "day", modern}, {"month_control", "month", modern},
		{"negative_control", "second", []types.Datetime{-1_000_000, -60_000_000}},
		{"above_max_control", "minute", []types.Datetime{fixedTruncDatetime(time.Date(65537, 5, 19, 12, 34, 56, 123456000, time.UTC))}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for index := 0; index < b.N; index++ {
				value, err := dateTruncCore(tc.unit, tc.values[index%len(tc.values)])
				if err != nil {
					b.Fatal(err)
				}
				fixedTruncBenchmarkSink = value
			}
		})
	}
}

func BenchmarkDateTruncFixedVector(b *testing.B) {
	const count = 8192
	for _, unit := range []string{"hour", "minute", "mixed_nulls"} {
		b.Run(unit, func(b *testing.B) {
			proc := fixedTruncProcess(b)
			base := time.Date(2026, 1, 1, 0, 0, 0, 123456000, time.UTC)
			values := make([]types.Datetime, count)
			valueNulls := make([]bool, count)
			for index := range values {
				values[index] = fixedTruncDatetime(base.Add(time.Duration(index) * 10 * time.Second))
				valueNulls[index] = unit == "mixed_nulls" && index%17 == 0
			}
			input := fixedTruncValues(b, proc, values, valueNulls)
			var units *vector.Vector
			lastUnit := unit
			if unit == "mixed_nulls" {
				units = vector.NewVec(types.T_varchar.ToType())
				b.Cleanup(func() { units.Free(proc.Mp()) })
				for index := range values {
					name := []string{"second", "minute", "hour", "day"}[index%4]
					require.NoError(b, vector.AppendBytes(units, []byte(name), index%19 == 0, proc.Mp()))
					lastUnit = name
				}
			} else {
				var err error
				units, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(unit), count, proc.Mp())
				require.NoError(b, err)
				b.Cleanup(func() { units.Free(proc.Mp()) })
			}
			result := vector.NewFunctionResultWrapper(types.T_datetime.ToTypeWithScale(6), proc.Mp())
			b.Cleanup(result.Free)
			require.NoError(b, result.PreExtendAndReset(count))
			b.SetBytes(count * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := result.PreExtendAndReset(count); err != nil {
					b.Fatal(err)
				}
				if err := DateTrunc([]*vector.Vector{units, input}, result, proc, count, nil); err != nil {
					b.Fatal(err)
				}
				fixedTruncBenchmarkSink = vector.GetFixedAtWithTypeCheck[types.Datetime](result.GetResultVector(), count-1)
			}
			b.StopTimer()
			last := base.Add(time.Duration(count-1) * 10 * time.Second)
			require.Equal(b, fixedTruncCalendar(strings.ToLower(lastUnit), last), fixedTruncBenchmarkSink)
			b.ReportMetric(count, "rows/op")
		})
	}
}
