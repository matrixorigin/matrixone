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

package types

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTemporalCandidateFixedZoneScalarOracle(t *testing.T) {
	ctx := context.Background()
	maxDate := int64(DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
	modern := int64(DatetimeFromClock(2026, 6, 1, 12, 0, 0, 123000))
	for _, offset := range []int{-14 * 3600, -61, 0, 14 * 3600} {
		zone := time.FixedZone("fixed", offset)
		for scale := int32(0); scale <= 6; scale++ {
			q := int64(scaleTable[scale])
			for _, bound := range []int64{-q - 1, -1, 0, 1, q, q + 1, modern, modern + 1, maxDate} {
				for op := ComparisonEqual; op <= ComparisonGreaterEqual; op++ {
					candidate, err := BuildTemporalCandidate(ctx, T_datetime, scale, bound, false, op, zone)
					require.NoError(t, err)
					wall := bound + int64(offset)*MicroSecsPerSec
					rows := []int64{-1, 0, 1, maxDate, modern, wall - q - 1, wall - q/2, wall - 1, wall, wall + q/2, wall + q + 1}
					for _, raw := range rows {
						if raw != -1 && (raw < 0 || raw > maxDate) {
							continue
						}
						truth := temporalCompare(int64(Datetime(raw).ToTimestamp(zone).TruncateToScale(scale)), bound, op)
						if truth {
							require.False(t, candidate.Empty, "offset=%d scale=%d bound=%d op=%d row=%d", offset, scale, bound, op, raw)
							require.True(t, candidate.Full || raw == candidate.Sentinel || (raw >= candidate.Lo && raw <= candidate.Hi),
								"offset=%d scale=%d bound=%d op=%d row=%d candidate=%+v", offset, scale, bound, op, raw, candidate)
						}
					}
				}
			}
		}
	}
	candidate, err := BuildTemporalCandidate(ctx, T_datetime, 3, modern, false, ComparisonEqual, time.UTC)
	require.NoError(t, err)
	require.Equal(t, modern-500, candidate.Lo)
	require.Equal(t, modern+499, candidate.Hi)
	candidate, err = BuildTemporalCandidate(ctx, T_datetime, 3, modern+1, false, ComparisonEqual, time.UTC)
	require.NoError(t, err)
	require.True(t, candidate.Empty)
}

func TestTemporalCandidateTimestampKeyAndFailure(t *testing.T) {
	ctx := context.Background()
	zone := time.FixedZone("fixed", 8*3600)
	bound := int64(DatetimeFromClock(2026, 6, 1, 12, 0, 0, 123456))
	for scale := int32(0); scale <= 6; scale++ {
		converted := int64(Datetime(bound).ToTimestamp(zone).TruncateToScale(scale))
		for op := ComparisonEqual; op <= ComparisonGreaterEqual; op++ {
			candidate, err := BuildTemporalCandidate(ctx, T_timestamp, scale, bound, false, op, zone)
			require.NoError(t, err)
			for _, value := range []int64{converted - 1, converted, converted + 1} {
				if temporalCompare(value, converted, op) {
					require.True(t, candidate.Full || (value >= candidate.Lo && value <= candidate.Hi))
				}
			}
		}
	}
	candidate, err := BuildTemporalCandidate(ctx, T_datetime, 3, 0, true, ComparisonEqual, zone)
	require.NoError(t, err)
	require.True(t, candidate.Empty)
	candidate, err = BuildTemporalCandidate(ctx, T_datetime, 3, math.MaxInt64, false, ComparisonEqual, zone)
	require.NoError(t, err)
	require.True(t, candidate.Full)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = BuildTemporalCandidate(cancelled, T_datetime, 3, 0, false, ComparisonEqual, zone)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTemporalCandidateNamedZoneFallsBack(t *testing.T) {
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	for _, kind := range []T{T_datetime, T_timestamp} {
		for op := ComparisonEqual; op <= ComparisonGreaterEqual; op++ {
			candidate, err := BuildTemporalCandidate(context.Background(), kind, 6, 123456, false, op, zone)
			require.NoError(t, err)
			require.True(t, candidate.Full, "kind=%v op=%v", kind, op)
		}
	}
}

func TestTimestampRoundingPositiveOffGridCannotMatch(t *testing.T) {
	for scale := int32(0); scale < 6; scale++ {
		q := int64(scaleTable[scale])
		for _, value := range []int64{math.MinInt64, math.MinInt64 + 1, -q - 1, -1, 0, 1, q - 1, q, math.MaxInt64 - q, math.MaxInt64 - 1, math.MaxInt64} {
			rounded := int64(Timestamp(value).TruncateToScale(scale))
			if rounded > q {
				require.Zero(t, rounded%q, "scale=%d value=%d rounded=%d", scale, value, rounded)
			}
		}
	}
}

// The planner rejects both Empty and Full. Off-grid equality must fall into
// one of those cases, including near overflow, before its old guard is removed.
func TestTemporalOffGridEqualityIsNotCostable(t *testing.T) {
	for scale := int32(0); scale < 6; scale++ {
		q := int64(scaleTable[scale])
		for _, bound := range []int64{q + 1, int64(DatetimeFromClock(2026, 1, 1, 0, 0, 0, 0)) + 1, math.MaxInt64 - q, math.MaxInt64 - 1, math.MaxInt64} {
			if bound <= q || bound%q == 0 {
				continue
			}
			for _, offset := range []int{-14 * 3600, 0, 14 * 3600} {
				candidate, err := BuildTemporalCandidate(context.Background(), T_datetime, scale, bound, false, ComparisonEqual, time.FixedZone("fixed", offset))
				require.NoError(t, err)
				require.True(t, candidate.Empty || candidate.Full, "scale=%d bound=%d offset=%d", scale, bound, offset)
			}
		}
	}
}
