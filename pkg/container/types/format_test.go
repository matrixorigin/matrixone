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

package types

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// formatOutcome returns f's result, or the panic it raised.
func formatOutcome(f func() string) (s string) {
	defer func() {
		if r := recover(); r != nil {
			s = fmt.Sprintf("panic(%v)", r)
		}
	}()
	return f()
}

const formatRandomCases = 20000

// TestFormatMatchesLegacy requires each formatter to return exactly what its
// fmt-based implementation returned, for edge values and random values
// across the whole representable range, and every scale a caller can pass.
func TestFormatMatchesLegacy(t *testing.T) {
	rng := rand.New(rand.NewSource(20261126))
	scales := []int32{-3, -1, 0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 12, 18, 19, 20, 30, 38, 40}

	t.Run("Decimal64", func(t *testing.T) {
		values := []Decimal64{0, 1, 9, 10, 99, 100, 12345, Decimal64(math.MaxInt64), Decimal64(1 << 63),
			Decimal64(math.MaxUint64), Decimal64(math.MaxUint64 - 9)}
		for p := uint64(1); p < 1e19; p *= 10 {
			values = append(values, Decimal64(p), Decimal64(p-1), Decimal64(p+1), Decimal64(p).Minus())
		}
		for i := 0; i < formatRandomCases; i++ {
			values = append(values, Decimal64(rng.Uint64()>>rng.Intn(64)))
		}
		for _, v := range values {
			for _, scale := range scales {
				require.Equal(t, formatOutcome(func() string { return legacyDecimal64Format(v, scale) }),
					formatOutcome(func() string { return v.Format(scale) }), "%d scale %d", uint64(v), scale)
			}
		}
	})

	t.Run("Decimal128", func(t *testing.T) {
		values := []Decimal128{{0, 0}, {1, 0}, {9, 0}, {10, 0}, {math.MaxUint64, 0}, {0, 1},
			{math.MaxUint64, math.MaxInt64}, {0, 1 << 63}, {math.MaxUint64, math.MaxUint64}}
		for i := 0; i < formatRandomCases; i++ {
			values = append(values, Decimal128{rng.Uint64(), rng.Uint64() >> rng.Intn(65)})
		}
		for _, v := range values {
			for _, scale := range scales {
				require.Equal(t, formatOutcome(func() string { return legacyDecimal128Format(v, scale) }),
					formatOutcome(func() string { return v.Format(scale) }), "%v scale %d", v, scale)
			}
		}
	})

	t.Run("Datetime", func(t *testing.T) {
		values := []Datetime{ZeroDatetime, 1, -1, math.MaxInt64, math.MinInt64,
			DatetimeFromClock(1, 1, 1, 0, 0, 0, 0), DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999),
			DatetimeFromClock(2026, 11, 26, 13, 5, 9, 123456), DatetimeFromClock(1970, 1, 1, 0, 0, 0, 1)}
		for i := 0; i < formatRandomCases; i++ {
			switch i % 3 {
			case 0:
				values = append(values, Datetime(rng.Int63()))
			case 1:
				values = append(values, Datetime(-rng.Int63()))
			default: // the SQL range
				values = append(values, DatetimeFromClock(int32(1+rng.Intn(9999)), uint8(1+rng.Intn(12)), uint8(1+rng.Intn(28)),
					uint8(rng.Intn(24)), uint8(rng.Intn(60)), uint8(rng.Intn(60)), uint32(rng.Intn(1000000))))
			}
		}
		for _, v := range values {
			for _, scale := range scales[:13] {
				require.Equal(t, formatOutcome(func() string { return legacyDatetimeString2(v, scale) }),
					formatOutcome(func() string { return v.String2(scale) }), "%d scale %d", int64(v), scale)
			}
		}
	})

	t.Run("Date", func(t *testing.T) {
		values := []Date{0, 1, -1, math.MaxInt32, math.MinInt32, DateFromCalendar(1, 1, 1), DateFromCalendar(9999, 12, 31)}
		for i := 0; i < formatRandomCases; i++ {
			if i%2 == 0 {
				values = append(values, Date(rng.Int31()))
			} else {
				values = append(values, DateFromCalendar(int32(rng.Intn(10000)), uint8(1+rng.Intn(12)), uint8(1+rng.Intn(28))))
			}
		}
		for _, v := range values {
			require.Equal(t, formatOutcome(func() string { return legacyDateString(v) }),
				formatOutcome(func() string { return v.String() }), "%d", int32(v))
		}
	})

	t.Run("Time", func(t *testing.T) {
		values := []Time{0, 1, -1, math.MaxInt64, math.MinInt64, math.MinInt64 + 1,
			TimeFromClock(false, 99, 59, 59, 999999), TimeFromClock(false, 100, 0, 0, 0),
			TimeFromClock(true, 838, 59, 59, 0), TimeFromClock(true, 0, 0, 0, 1)}
		for i := 0; i < formatRandomCases; i++ {
			switch i % 3 {
			case 0:
				values = append(values, Time(rng.Int63()))
			case 1:
				values = append(values, Time(-rng.Int63()))
			default:
				values = append(values, TimeFromClock(rng.Intn(2) == 0, uint64(rng.Intn(900)), uint8(rng.Intn(60)),
					uint8(rng.Intn(60)), uint32(rng.Intn(1000000))))
			}
		}
		for _, v := range values {
			for _, scale := range scales[:13] {
				require.Equal(t, formatOutcome(func() string { return legacyTimeString2(v, scale) }),
					formatOutcome(func() string { return v.String2(scale) }), "%d scale %d", int64(v), scale)
			}
		}
	})

	t.Run("Timestamp", func(t *testing.T) {
		locs := []*time.Location{time.UTC, time.FixedZone("UTC+8", 8*3600), time.FixedZone("UTC-5:30", -(5*3600 + 1800)),
			time.FixedZone("UTC+14", 14*3600)}
		values := []Timestamp{ZeroTimestamp, 1, -1, math.MaxInt64, math.MinInt64, TimestampMinValue, TimestampMaxValue,
			TimestampMinValue - 1, TimestampMaxValue + 1}
		for i := 0; i < formatRandomCases/4; i++ {
			if i%2 == 0 {
				values = append(values, Timestamp(rng.Int63()>>rng.Intn(20)))
			} else {
				values = append(values, TimestampMinValue+Timestamp(rng.Int63n(int64(TimestampMaxValue-TimestampMinValue))))
			}
		}
		for _, loc := range locs {
			for _, v := range values {
				for _, scale := range scales[:10] {
					require.Equal(t, formatOutcome(func() string { return legacyTimestampString2(v, loc, scale) }),
						formatOutcome(func() string { return v.String2(loc, scale) }), "%d %s scale %d", int64(v), loc, scale)
				}
			}
		}
	})
}

var formatSink string

// BenchmarkFormat measures the formatters on in-range values, new and old.
func BenchmarkFormat(b *testing.B) {
	dt := DatetimeFromClock(2026, 11, 26, 13, 5, 9, 123456)
	ts := FromClockUTC(2026, 11, 26, 13, 5, 9, 123456)
	loc := time.FixedZone("UTC+8", 8*3600)
	tm0, tm3 := TimeFromClock(false, 13, 5, 9, 0), TimeFromClock(false, 13, 5, 9, 123000)
	d64, d64b, d128 := Decimal64(1234567), Decimal64(1234567890123), Decimal128{123456789012345, 0}
	for _, c := range []struct {
		name     string
		now, old func() string
	}{
		{"Decimal64/scale2", func() string { return d64.Format(2) }, func() string { return legacyDecimal64Format(d64, 2) }},
		{"Decimal64/scale0", func() string { return d64b.Format(0) }, func() string { return legacyDecimal64Format(d64b, 0) }},
		{"Decimal128/scale4", func() string { return d128.Format(4) }, func() string { return legacyDecimal128Format(d128, 4) }},
		{"Datetime/scale0", func() string { return dt.String2(0) }, func() string { return legacyDatetimeString2(dt, 0) }},
		{"Datetime/scale6", func() string { return dt.String2(6) }, func() string { return legacyDatetimeString2(dt, 6) }},
		{"Date", func() string { return Date(739000).String() }, func() string { return legacyDateString(Date(739000)) }},
		{"Time/scale0", func() string { return tm0.String2(0) }, func() string { return legacyTimeString2(tm0, 0) }},
		{"Time/scale3", func() string { return tm3.String2(3) }, func() string { return legacyTimeString2(tm3, 3) }},
		{"Timestamp/scale0", func() string { return ts.String2(loc, 0) }, func() string { return legacyTimestampString2(ts, loc, 0) }},
		{"Timestamp/scale6", func() string { return ts.String2(loc, 6) }, func() string { return legacyTimestampString2(ts, loc, 6) }},
	} {
		for _, impl := range []struct {
			name string
			f    func() string
		}{{"new", c.now}, {"old", c.old}} {
			b.Run(c.name+"/"+impl.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					formatSink = impl.f()
				}
			})
		}
	}
}
