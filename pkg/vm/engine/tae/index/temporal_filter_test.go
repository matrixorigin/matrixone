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

package index

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestMixedTemporalZoneMatcher(t *testing.T) {
	zone := time.UTC
	value, err := types.ParseDatetime("2024-01-01 12:00:00.123100", 6)
	require.NoError(t, err)
	bound, err := types.ParseTimestamp(zone, "2024-01-01 12:00:00.123", 3)
	require.NoError(t, err)
	encoded := int64(bound)
	zm := NewZM(types.T_datetime, 6)
	v := int64(value)
	UpdateZM(zm, types.EncodeInt64(&v))
	wrongType := NewZM(types.T_timestamp, 6)
	UpdateZM(wrongType, types.EncodeInt64(&encoded))
	for _, tc := range []struct {
		op   string
		want bool
	}{{"=", true}, {"<", false}, {"<=", true}, {">", false}, {">=", true}} {
		match, ok := MakeMixedTemporalMatcher(types.T_datetime, types.EncodeInt64(&encoded), types.T_timestamp, 3, zone, tc.op)
		require.True(t, ok)
		got, known := match(zm)
		require.True(t, known, tc.op)
		require.Equal(t, tc.want, got, tc.op)
		for _, unknown := range []ZM{nil, {0}, NewZM(types.T_datetime, 6), wrongType} {
			_, known = match(unknown)
			require.False(t, known, "unknown or incompatible metadata cannot prune")
		}
	}
	_, ok := MakeMixedTemporalMatcher(types.T_datetime, types.EncodeInt64(&encoded), types.T_timestamp, 7, zone, "=")
	require.False(t, ok)
	_, ok = MakeMixedTemporalMatcher(types.T_datetime, types.EncodeInt64(&encoded), types.T_timestamp, 3, nil, "=")
	require.False(t, ok)
	_, ok = MakeMixedTemporalMatcher(types.T_datetime, types.EncodeInt64(&encoded), types.T_timestamp, 3, zone, "invalid")
	require.False(t, ok)

	// The opposite physical type converts the DATETIME bound at the
	// TIMESTAMP column's scale before comparing against its zone map.
	match, ok := MakeMixedTemporalMatcher(types.T_timestamp, types.EncodeInt64(&v), types.T_datetime, 3, zone, "=")
	require.True(t, ok)
	matched, known := match(wrongType)
	require.True(t, known)
	require.True(t, matched)
}

func TestMixedTemporalZoneMatcherSentinelRanges(t *testing.T) {
	zone := time.FixedZone("+02", 7200)
	// These two valid DATETIMEs convert to raw timestamps -2 and -1. At
	// scale 0 their scalar results are 0 and ZeroTimestamp, in that order.
	values := []types.Datetime{7199999998, 7199999999}
	require.Equal(t, types.Timestamp(0), values[0].ToTimestamp(zone).TruncateToScale(0))
	require.Equal(t, types.ZeroTimestamp, values[1].ToTimestamp(zone).TruncateToScale(0))
	for _, tc := range []struct {
		name string
		rows []types.Datetime
	}{
		{"rounding crosses timestamp sentinel", values},
		{"datetime sentinel inside physical range", []types.Datetime{-2, types.ZeroDatetime, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			zm := NewZM(types.T_datetime, 6)
			for _, row := range tc.rows {
				UpdateZM(zm, types.EncodeDatetime(&row))
			}
			bound := types.Timestamp(0)
			match, ok := MakeMixedTemporalMatcher(types.T_datetime, types.EncodeTimestamp(&bound), types.T_timestamp, 0, zone, "=")
			require.True(t, ok)
			_, known := match(zm)
			require.False(t, known, "non-monotone intervals cannot establish exclusion or a lower-bound hit")
		})
	}
}

func TestMixedTemporalZoneMatcherMalformedEndpoints(t *testing.T) {
	for _, column := range []types.T{types.T_datetime, types.T_timestamp} {
		boundType := types.T_timestamp
		if column == types.T_timestamp {
			boundType = types.T_datetime
		}
		zero := int64(0)
		match, ok := MakeMixedTemporalMatcher(column, types.EncodeInt64(&zero), boundType, 6, time.UTC, "=")
		require.True(t, ok)
		zm := NewZM(column, 6)
		UpdateZM(zm, types.EncodeInt64(&zero))
		got, known := match(zm)
		require.True(t, got && known)
		for _, endpoint := range []int{30, 61} {
			for _, width := range []byte{0, 7, 9} {
				bad := append(ZM(nil), zm...)
				bad[endpoint] = width
				_, known := match(bad)
				require.False(t, known, "type=%s endpoint=%d width=%d", column, endpoint, width)
			}
		}
	}
}

func TestMixedTemporalZoneMatcherScalarOracle(t *testing.T) {
	named, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	maxDate := types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999)
	ordinary := types.DatetimeFromClock(2024, 3, 10, 1, 59, 59, 900000)
	wideOffset := int64(math.MaxInt32) + 1
	zones := []*time.Location{time.UTC, time.FixedZone("+02", 7200), time.FixedZone("-08", -28800),
		time.FixedZone("max-offset", math.MaxInt32), time.FixedZone("min-offset", math.MinInt32),
		time.FixedZone("unsupported-offset", int(wideOffset)), named, time.Local}
	ranges := [][2]types.Datetime{{0, 1}, {0, 10_000_001}, {7199999998, 7199999999},
		{ordinary, ordinary + 2*types.MicroSecsPerSec}, {ordinary, ordinary + 3*types.SecsPerHour*types.MicroSecsPerSec},
		{maxDate - 1, maxDate}, {-2, 0}, {maxDate, maxDate + 1}, {math.MinInt64, math.MaxInt64},
		{types.ZeroDatetime, types.ZeroDatetime}, {math.MinInt64, math.MinInt64}, {math.MaxInt64, math.MaxInt64}, {2, 1}}
	compare := func(a, b types.Timestamp, op string) bool {
		switch op {
		case "=":
			return a == b
		case "<":
			return a < b
		case "<=":
			return a <= b
		case ">":
			return a > b
		default:
			return a >= b
		}
	}
	for _, zone := range zones {
		t.Run(zone.String(), func(t *testing.T) {
			for _, r := range ranges {
				zm := NewZM(types.T_datetime, 6)
				UpdateZM(zm, types.EncodeDatetime(&r[0]))
				UpdateZM(zm, types.EncodeDatetime(&r[1]))
				if r[0] > r[1] {
					copy(zm.GetMinBuf(), types.EncodeDatetime(&r[0]))
					copy(zm.GetMaxBuf(), types.EncodeDatetime(&r[1]))
				}
				rows := []types.Datetime{r[0], r[1]}
				if r[0] >= 0 && r[1] <= maxDate && r[0] < r[1] {
					rows = append(rows, r[0]+1, r[1]-1, r[0]+(r[1]-r[0])/2)
				}
				for scale := int32(0); scale <= 6; scale++ {
					lo, hi, ordered := types.DatetimeRangeToTimestampRange(r[0], r[1], zone)
					crossesSentinel := scale < 6 && lo <= types.ZeroTimestamp && hi >= types.ZeroTimestamp
					for _, bound := range []types.Timestamp{types.ZeroTimestamp, 0, rows[0].ToTimestamp(zone).TruncateToScale(scale), rows[1].ToTimestamp(zone).TruncateToScale(scale)} {
						for _, op := range []string{"=", "<", "<=", ">", ">="} {
							match, ok := MakeMixedTemporalMatcher(types.T_datetime, types.EncodeTimestamp(&bound), types.T_timestamp, scale, zone, op)
							require.True(t, ok)
							got, known := match(zm)
							if r[0] > r[1] || r[0] != r[1] && (r[0] < 0 || r[1] > maxDate) {
								require.False(t, known)
								continue
							}
							any := false
							for _, row := range rows {
								any = any || compare(row.ToTimestamp(zone).TruncateToScale(scale), bound, op)
							}
							if known && !got {
								require.False(t, any, "range=%v scale=%d bound=%d op=%s", r, scale, bound, op)
							}
							if r[0] == r[1] {
								require.True(t, known)
								require.Equal(t, any, got)
							} else if ordered && !crossesSentinel {
								require.True(t, known, "ordinary ordered metadata must remain usable")
								lo, hi := lo.TruncateToScale(scale), hi.TruncateToScale(scale)
								want := compare(lo, bound, op) || compare(hi, bound, op)
								if op == "=" {
									want = lo <= bound && bound <= hi
								}
								require.Equal(t, want, got, "range=%v scale=%d bound=%d op=%s", r, scale, bound, op)
							} else {
								require.False(t, known)
							}
						}
					}
				}
			}
		})
	}
}

func BenchmarkMixedTemporalZoneMatcher(b *testing.B) {
	for _, offset := range []int{0, 7200, -28800} {
		zone := time.FixedZone(fmt.Sprint(offset), offset)
		low := types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0)
		high := low + types.MicroSecsPerSec
		bound := low.ToTimestamp(zone)
		zm := NewZM(types.T_datetime, 6)
		UpdateZM(zm, types.EncodeDatetime(&low))
		UpdateZM(zm, types.EncodeDatetime(&high))
		fast, ok := MakeMixedTemporalMatcher(types.T_datetime, types.EncodeTimestamp(&bound), types.T_timestamp, 0, zone, "=")
		require.True(b, ok)
		calendar := func(zm ZM) (bool, bool) {
			lo, hi, known := types.DatetimeRangeToTimestampRange(zm.GetMin().(types.Datetime), zm.GetMax().(types.Datetime), zone)
			if !known {
				return false, false
			}
			return lo.TruncateToScale(0) <= bound && hi.TruncateToScale(0) >= bound, true
		}
		for _, test := range []struct {
			name  string
			match func(ZM) (bool, bool)
		}{{"calendar", calendar}, {"fixed", fast}} {
			b.Run(fmt.Sprintf("offset=%d/%s", offset, test.name), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					test.match(zm)
				}
			})
		}
	}
}
