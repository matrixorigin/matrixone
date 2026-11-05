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
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"strconv"
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
				for op := TemporalEqual; op <= TemporalGreaterEqual; op++ {
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
	candidate, err := BuildTemporalCandidate(ctx, T_datetime, 3, modern, false, TemporalEqual, time.UTC)
	require.NoError(t, err)
	require.Equal(t, modern-500, candidate.Lo)
	require.Equal(t, modern+499, candidate.Hi)
	candidate, err = BuildTemporalCandidate(ctx, T_datetime, 3, modern+1, false, TemporalEqual, time.UTC)
	require.NoError(t, err)
	require.True(t, candidate.Empty)
}

func TestTemporalCandidateTimestampKeyAndFailure(t *testing.T) {
	ctx := context.Background()
	zone := time.FixedZone("fixed", 8*3600)
	bound := int64(DatetimeFromClock(2026, 6, 1, 12, 0, 0, 123456))
	for scale := int32(0); scale <= 6; scale++ {
		converted := int64(Datetime(bound).ToTimestamp(zone).TruncateToScale(scale))
		for op := TemporalEqual; op <= TemporalGreaterEqual; op++ {
			candidate, err := BuildTemporalCandidate(ctx, T_timestamp, scale, bound, false, op, zone)
			require.NoError(t, err)
			for _, value := range []int64{converted - 1, converted, converted + 1} {
				if temporalCompare(value, converted, op) {
					require.True(t, candidate.Full || (value >= candidate.Lo && value <= candidate.Hi))
				}
			}
		}
	}
	candidate, err := BuildTemporalCandidate(ctx, T_datetime, 3, 0, true, TemporalEqual, zone)
	require.NoError(t, err)
	require.True(t, candidate.Empty)
	candidate, err = BuildTemporalCandidate(ctx, T_datetime, 3, math.MaxInt64, false, TemporalEqual, zone)
	require.NoError(t, err)
	require.True(t, candidate.Full)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = BuildTemporalCandidate(cancelled, T_datetime, 3, 0, false, TemporalEqual, zone)
	require.ErrorIs(t, err, context.Canceled)
}

func TestTemporalCandidateNamedZoneScalarOracle(t *testing.T) {
	for _, tc := range []struct {
		zone             string
		year             int32
		month, day, hour uint8
	}{
		{"America/New_York", 2024, 3, 10, 2},
		{"America/New_York", 2024, 11, 3, 1},
		{"Europe/Amsterdam", 1900, 1, 1, 12},
		{"America/New_York", 1969, 12, 31, 23},
		{"Pacific/Apia", 2011, 12, 30, 12},
	} {
		t.Run(tc.zone+"/"+strconv.Itoa(int(tc.year))+"/"+strconv.Itoa(int(tc.month)), func(t *testing.T) {
			zone, err := time.LoadLocation(tc.zone)
			require.NoError(t, err)
			center := int64(DatetimeFromClock(tc.year, tc.month, tc.day, tc.hour, 0, 0, 123456))
			for scale := int32(0); scale <= 6; scale++ {
				for _, boundRow := range []int64{center - 3600*MicroSecsPerSec, center, center + 3600*MicroSecsPerSec} {
					bound := int64(Datetime(boundRow).ToTimestamp(zone).TruncateToScale(scale))
					for op := TemporalEqual; op <= TemporalGreaterEqual; op++ {
						candidate, err := BuildTemporalCandidate(context.Background(), T_datetime, scale, bound, false, op, zone)
						require.NoError(t, err)
						require.False(t, candidate.Full, "unexpected loss of pruning for %s scale=%d op=%d bound=%d", tc.zone, scale, op, bound)
						for minute := int64(-180); minute <= 180; minute++ {
							row := center + minute*60*MicroSecsPerSec
							truth := temporalCompare(int64(Datetime(row).ToTimestamp(zone).TruncateToScale(scale)), bound, op)
							if truth {
								require.True(t, !candidate.Empty && (row == candidate.Sentinel || (row >= candidate.Lo && row <= candidate.Hi)),
									"scale=%d bound=%d op=%d row=%d candidate=%+v", scale, bound, op, row, candidate)
							}
						}
					}
				}
			}
		})
	}
}

func TestTemporalNamedZonePeriodLimits(t *testing.T) {
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	center := int64(DatetimeFromClock(2026, 6, 1, 12, 0, 0, 123000))
	bound := int64(Datetime(center).ToTimestamp(zone).TruncateToScale(3))
	candidate, err := BuildTemporalCandidate(context.Background(), T_datetime, 3, bound, false, TemporalEqual, zone)
	require.NoError(t, err)
	require.False(t, candidate.Full)
	require.Equal(t, int64(999), candidate.Hi-candidate.Lo)

	count := temporalZonePeriodLimit
	_, _, ok, err := temporalZonePeriods(context.Background(), zone, center, center, &count, false, nil)
	require.NoError(t, err)
	require.False(t, ok)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	count = 63
	_, _, ok, err = temporalZonePeriods(ctx, zone, center, center, &count, false, nil)
	require.False(t, ok)
	require.ErrorIs(t, err, context.Canceled)
}

// Synthetic TZif data exercises the Go POSIX extension even before 1970;
// ordinary IANA files usually retain explicit transitions for those years.
func temporalPOSIXZone(t *testing.T, rule string) *time.Location {
	t.Helper()
	var data bytes.Buffer
	for range 2 {
		data.WriteString("TZif2")
		data.Write(make([]byte, 15))
		for _, count := range []uint32{0, 0, 0, 0, 1, 4} {
			require.NoError(t, binary.Write(&data, binary.BigEndian, count))
		}
		data.Write([]byte{0, 0, 0, 0, 0, 0})
		data.WriteString("UTC\x00")
	}
	data.WriteByte('\n')
	data.WriteString(rule)
	data.WriteByte('\n')
	zone, err := time.LoadLocationFromTZData("synthetic", data.Bytes())
	require.NoError(t, err)
	return zone
}

func TestTemporalCandidatePOSIXYearBoundaries(t *testing.T) {
	for _, rule := range []string{
		"STD0DST,M3.2.0/0,M11.1.0/0",
		"STD0DST,M12.5.0/167,M1.1.0/-167",
	} {
		zone := temporalPOSIXZone(t, rule)
		for _, year := range []int32{1968, 1969, 1970, 2040} {
			center := int64(DatetimeFromClock(year, 12, 31, 23, 59, 59, 0))
			for _, delta := range []int64{-24 * 3600, -1, 0, 1, 2, 24 * 3600} {
				row := center + delta*MicroSecsPerSec
				bound := int64(Datetime(row).ToTimestamp(zone).TruncateToScale(3))
				candidate, err := BuildTemporalCandidate(context.Background(), T_datetime, 3, bound, false, TemporalEqual, zone)
				require.NoError(t, err)
				require.True(t, candidate.Full || (!candidate.Empty && (row == candidate.Sentinel || (row >= candidate.Lo && row <= candidate.Hi))),
					"rule=%s year=%d delta=%d row=%d candidate=%+v", rule, year, delta, row, candidate)
			}
		}
	}
}

func temporalOutOfCalendarGapZone(t *testing.T, shift int32) *time.Location {
	t.Helper()
	var data bytes.Buffer
	header := func(transitions, zones uint32) {
		data.WriteString("TZif2")
		data.Write(make([]byte, 15))
		for _, count := range []uint32{0, 0, 0, transitions, zones, 4} {
			require.NoError(t, binary.Write(&data, binary.BigEndian, count))
		}
	}
	header(0, 1)
	data.Write([]byte{0, 0, 0, 0, 0, 0})
	data.WriteString("UTC\x00")
	header(2, 3)
	for _, transition := range []time.Time{
		time.Date(0, 12, 31, 12, 0, 0, 0, time.UTC),
		time.Date(1, 1, 1, 11, 30, 0, 0, time.UTC),
	} {
		require.NoError(t, binary.Write(&data, binary.BigEndian, transition.Unix()-int64(shift)))
	}
	data.Write([]byte{1, 2})
	for _, offset := range []int32{0, 3600, 7200} {
		require.NoError(t, binary.Write(&data, binary.BigEndian, offset+shift))
		data.Write([]byte{0, 0})
	}
	data.WriteString("UTC\x00")
	zone, err := time.LoadLocationFromTZData("year-zero", data.Bytes())
	require.NoError(t, err)
	return zone
}

func TestTemporalCandidateOutOfCalendarGapFailsOpen(t *testing.T) {
	for _, shift := range []int32{0, math.MaxInt32 - 7200} {
		zone := temporalOutOfCalendarGapZone(t, shift)
		row := DatetimeFromClock(1, 1, 1, 12, 45, 0, 0)
		bound := row.ToTimestamp(zone)
		want := Timestamp(time.Date(0, 12, 31, 12, 0, 0, 0, time.UTC).UnixMicro() + unixEpochMicroSecs - int64(shift)*MicroSecsPerSec)
		require.Equal(t, want, bound)
		candidate, err := BuildTemporalCandidate(context.Background(), T_datetime, 6, int64(bound), false, TemporalEqual, zone)
		require.NoError(t, err)
		require.True(t, candidate.Full, "shift=%d bound=%d candidate=%+v", shift, bound, candidate)
	}
}

func TestTemporalCandidateNamedZoneRandomizedSuperset(t *testing.T) {
	rng := rand.New(rand.NewSource(31619))
	zones := []string{"America/New_York", "Europe/London", "Europe/Amsterdam", "Pacific/Apia"}
	for _, name := range zones {
		zone, err := time.LoadLocation(name)
		require.NoError(t, err)
		for n := 0; n < 64; n++ {
			year := int32(1880 + rng.Intn(180))
			month := uint8(1 + rng.Intn(12))
			day := uint8(1 + rng.Intn(28))
			center := int64(DatetimeFromClock(year, month, day, uint8(rng.Intn(24)), uint8(rng.Intn(60)), uint8(rng.Intn(60)), uint32(rng.Intn(1000000))))
			scale := int32(rng.Intn(7))
			bound := int64(Datetime(center).ToTimestamp(zone).TruncateToScale(scale))
			op := TemporalOp(rng.Intn(5))
			candidate, err := BuildTemporalCandidate(context.Background(), T_datetime, scale, bound, false, op, zone)
			require.NoError(t, err)
			for j := 0; j < 32; j++ {
				row := center + rng.Int63n(48*SecsPerHour*MicroSecsPerSec) - 24*SecsPerHour*MicroSecsPerSec
				if temporalCompare(int64(Datetime(row).ToTimestamp(zone).TruncateToScale(scale)), bound, op) {
					require.True(t, candidate.Full || (!candidate.Empty && (row == candidate.Sentinel || (row >= candidate.Lo && row <= candidate.Hi))),
						"zone=%s row=%d bound=%d op=%d scale=%d candidate=%+v", name, row, bound, op, scale, candidate)
				}
			}
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

func TestTemporalZonePeriodsCoverNegativeMidnightAndYear(t *testing.T) {
	for _, rule := range []string{"STD0DST,M3.2.0/0,M11.1.0/0", "STD0DST,M12.5.0/167,M1.1.0/-167"} {
		zone := temporalPOSIXZone(t, rule)
		for _, center := range []time.Time{
			time.Date(1968, 12, 31, 0, 0, 0, 0, time.UTC),
			time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
			time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2040, 12, 31, 0, 0, 0, 0, time.UTC),
		} {
			lo := center.Add(-24*time.Hour).UnixMicro() + unixEpochMicroSecs
			hi := center.Add(24*time.Hour).UnixMicro() + unixEpochMicroSecs
			count := 0
			minOffset, maxOffset, ok, err := temporalZonePeriods(context.Background(), zone, lo, hi, &count, false, nil)
			require.NoError(t, err)
			if !ok {
				// A non-advancing negative POSIX bound is deliberately not
				// guessed; the enclosing candidate must retain Full.
				row := Datetime(center.UnixMicro() + unixEpochMicroSecs)
				bound := row.ToTimestamp(zone)
				candidate, err := BuildTemporalCandidate(context.Background(), T_datetime, 6, int64(bound), false, TemporalEqual, zone)
				require.NoError(t, err)
				require.True(t, candidate.Full, "rule=%s center=%s", rule, center)
				continue
			}
			for h := -24; h <= 24; h++ {
				point := center.Add(time.Duration(h) * time.Hour)
				for _, delta := range []time.Duration{-time.Microsecond, 0, time.Microsecond, time.Second, time.Second + time.Microsecond} {
					probe := point.Add(delta)
					if probe.Before(center.Add(-24*time.Hour)) || probe.After(center.Add(24*time.Hour)) {
						continue
					}
					_, offset := probe.In(zone).Zone()
					require.GreaterOrEqual(t, int64(offset)*MicroSecsPerSec, minOffset, "rule=%s time=%s", rule, probe)
					require.LessOrEqual(t, int64(offset)*MicroSecsPerSec, maxOffset, "rule=%s time=%s", rule, probe)
				}
			}
		}
	}
}
