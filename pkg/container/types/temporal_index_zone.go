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
	"time"
)

const temporalZonePeriodLimit = 4096

// temporalZonePeriods visits complete location periods intersecting [lo, hi],
// expressed in microseconds from the DATETIME epoch. The count is shared by
// every pass in one candidate evaluation.
func temporalZonePeriods(ctx context.Context, loc *time.Location, lo, hi int64, count *int, coarse bool,
	transition func(time.Time, int64, int64) bool,
) (int64, int64, bool, error) {
	loUnix, ok := temporalAdd(lo, -unixEpochMicroSecs)
	if !ok {
		return 0, 0, false, nil
	}
	hiUnix, ok := temporalAdd(hi, -unixEpochMicroSecs)
	if !ok || lo > hi {
		return 0, 0, false, nil
	}
	probe := time.UnixMicro(loUnix).In(loc)
	limit := time.UnixMicro(hiUnix)
	low, high := int64(math.MaxInt64), int64(math.MinInt64)
	if coarse && loUnix < 0 {
		// The POSIX extension's offsets are at most 168:59:59 plus the
		// default DST hour; include another second for gap normalization.
		// Explicit TZif periods are still enumerated below because their
		// offsets can be much larger.
		low, high = -612000*MicroSecsPerSec, 612000*MicroSecsPerSec
	}
	for {
		*count++
		if *count > temporalZonePeriodLimit {
			return 0, 0, false, nil
		}
		if *count%64 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, 0, false, err
			}
		}
		_, off := probe.Zone()
		if int64(off) < math.MinInt32 || int64(off) > math.MaxInt32 {
			return 0, 0, false, nil
		}
		offset := int64(off) * MicroSecsPerSec
		low, high = min(low, offset), max(high, offset)
		_, next := probe.ZoneBounds()
		if next.IsZero() {
			// A real transition at year 1 has the same representation as an
			// infinite bound. Decline if the scan could cross that instant.
			if probe.Before(time.Time{}) && !limit.Before(time.Time{}) {
				return 0, 0, false, nil
			}
			next = time.Date(probe.UTC().Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
		} else if !next.After(probe) {
			// Go 1.27's POSIX extension reports a 365-day year end even
			// in a leap year. In that final UTC day tzset's last branch
			// has already passed both rules, so its offset is constant up
			// to the next UTC year. The following lookup still checks a
			// possible change exactly at the year boundary.
			year := probe.UTC().Year()
			start := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
			end := time.Date(year+1, 1, 1, 0, 0, 0, 0, time.UTC)
			stale := start.AddDate(0, 0, 365)
			if probe.Unix() < 0 || end.Sub(start) != 366*24*time.Hour || !next.Equal(stale) || probe.Before(stale) || !probe.Before(end) {
				return 0, 0, false, nil
			}
			next = end
		}
		// POSIX transition rules can cross a UTC year boundary. For
		// negative Unix seconds Go's remainder also changes at midnight
		// and again at the following second, so refine those segments at
		// both boundaries on the second pass.
		utc := probe.UTC()
		yearEnd := time.Date(utc.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
		if yearEnd.Before(next) {
			next = yearEnd
		}
		if !coarse && probe.Unix() < 0 {
			midnight := time.Date(utc.Year(), utc.Month(), utc.Day()+1, 0, 0, 0, 0, time.UTC)
			if utc.Hour() == 0 && utc.Minute() == 0 && utc.Second() == 0 {
				midnight = time.Unix(probe.Unix()+1, 0)
			}
			if midnight.Before(next) {
				next = midnight
			}
		}
		if next.After(limit) {
			return low, high, true, nil
		}
		after := next.In(loc)
		_, nextOff := after.Zone()
		if int64(nextOff) < math.MinInt32 || int64(nextOff) > math.MaxInt32 {
			return 0, 0, false, nil
		}
		if transition != nil && !transition(next, offset, int64(nextOff)*MicroSecsPerSec) {
			return 0, 0, false, nil
		}
		probe = after
	}
}

// temporalNamedZoneRange refines a conservative representation bound only
// within the uncertainty strip at the raw comparison boundary. Its period
// budget is shared by both refinements and all three Go lookup windows.
func temporalNamedZoneRange(ctx context.Context, loc *time.Location, rawLo, rawHi, maxDatetime int64) (int64, int64, bool, error) {
	_, epochOffset := time.Unix(0, 0).In(loc).Zone()
	if int64(epochOffset) < math.MinInt32 || int64(epochOffset) > math.MaxInt32 {
		return 0, 0, false, nil
	}
	// Even when the scalar gap detector uses an out-of-calendar wall label,
	// it returns the Go candidate or a transition within its 72-hour
	// lookback, preserving the input fractional second.
	const envelope = (int64(1)<<31 + 72*SecsPerHour + 2) * MicroSecsPerSec
	lo, hi := int64(0), maxDatetime
	if rawLo != math.MinInt64 {
		v, ok := temporalAdd(rawLo, -envelope)
		if !ok {
			return 0, 0, false, nil
		}
		lo = max(lo, v)
	}
	if rawHi != math.MaxInt64 {
		v, ok := temporalAdd(rawHi, envelope)
		if !ok {
			return 0, 0, false, nil
		}
		hi = min(hi, v)
	}
	if lo > hi {
		// The scalar's out-of-calendar gap handling has not yet been
		// excluded at this point, so an apparently disjoint seed is not
		// proof that the calendar component cannot match.
		return 0, 0, false, nil
	}
	wallLo, wallHi := lo, hi
	if rawLo == math.MinInt64 {
		v, ok := temporalAdd(rawHi, -envelope)
		if !ok {
			return 0, 0, false, nil
		}
		wallLo = max(wallLo, v)
	}
	if rawHi == math.MaxInt64 {
		v, ok := temporalAdd(rawLo, envelope)
		if !ok {
			return 0, 0, false, nil
		}
		wallHi = min(wallHi, v)
	}
	periods := 0
	for iteration := 0; iteration < 2 && wallLo <= wallHi; iteration++ {
		min0, max0, ok, err := temporalZonePeriods(ctx, loc, wallLo, wallHi, &periods, iteration == 0, nil)
		if err != nil || !ok {
			return 0, 0, false, err
		}
		i1Lo, ok := temporalAdd(wallLo, -max0)
		if !ok {
			return 0, 0, false, nil
		}
		i1Hi, ok := temporalAdd(wallHi, -min0)
		if !ok {
			return 0, 0, false, nil
		}
		min1, max1, ok, err := temporalZonePeriods(ctx, loc, i1Lo, i1Hi, &periods, iteration == 0, nil)
		if err != nil || !ok {
			return 0, 0, false, err
		}
		emin, emax := min(min0, min1), max(max0, max1)
		i2Lo, ok := temporalAdd(wallLo, -emax-72*SecsPerHour*MicroSecsPerSec-1)
		if !ok {
			return 0, 0, false, nil
		}
		i2Hi, ok := temporalAdd(wallHi, -emin+48*SecsPerHour*MicroSecsPerSec)
		if !ok {
			return 0, 0, false, nil
		}
		// The scalar gap detector converts transition wall labels using
		// DatetimeFromClock, whose out-of-calendar mapping differs from
		// UTC+offset. An unseen POSIX transition can occur anywhere in I2;
		// reserve the full TZif offset range when proving its local year.
		const maxTZifOffset = (int64(1) << 31) * MicroSecsPerSec
		if i2Lo <= maxTZifOffset || i2Hi > maxDatetime-maxTZifOffset {
			return 0, 0, false, nil
		}
		if iteration == 0 && i2Lo < unixEpochMicroSecs {
			// The coarse pass may miss POSIX gap transitions in I2 even
			// when the two Date lookup windows are wholly after the epoch.
			emin = min(emin, -612000*MicroSecsPerSec)
			emax = max(emax, 612000*MicroSecsPerSec)
		}
		_, _, ok, err = temporalZonePeriods(ctx, loc, i2Lo, i2Hi, &periods, iteration == 0, func(at time.Time, before, after int64) bool {
			if after <= before {
				return true
			}
			beforeYear := at.Add(-time.Microsecond).In(loc).Year()
			afterYear := at.In(loc).Year()
			// DatetimeFromClock uses a different calendar mapping outside
			// the SQL year range. The scalar gap detector uses that mapping,
			// so UTC-plus-offset is not a proof for these transitions.
			if beforeYear < MinDatetimeYear || beforeYear > MaxDatetimeYear ||
				afterYear < MinDatetimeYear || afterYear > MaxDatetimeYear {
				return false
			}
			utc, ok := temporalAdd(at.UnixMicro(), unixEpochMicroSecs)
			if !ok {
				return false
			}
			beforeWall, ok := temporalAdd(utc, before-1)
			if !ok {
				return false
			}
			afterWall, ok := temporalAdd(utc, after)
			if !ok {
				return false
			}
			if wallHi >= beforeWall && wallLo < afterWall {
				emin = min(emin, before-MicroSecsPerSec)
				emax = max(emax, after+MicroSecsPerSec)
			}
			return true
		})
		if err != nil || !ok {
			return 0, 0, false, err
		}
		if rawLo != math.MinInt64 {
			v, ok := temporalAdd(rawLo, emin)
			if !ok {
				return 0, 0, false, nil
			}
			lo = max(lo, v)
		}
		if rawHi != math.MaxInt64 {
			v, ok := temporalAdd(rawHi, emax)
			if !ok {
				return 0, 0, false, nil
			}
			hi = min(hi, v)
		}
		if lo > hi {
			return 0, 0, true, nil
		}
		if rawLo == math.MinInt64 {
			v, ok := temporalAdd(rawHi, emin)
			if !ok {
				return 0, 0, false, nil
			}
			wallLo = max(wallLo, v)
			v, ok = temporalAdd(rawHi, emax)
			if !ok {
				return 0, 0, false, nil
			}
			wallHi = min(wallHi, v)
		} else if rawHi == math.MaxInt64 {
			v, ok := temporalAdd(rawLo, emin)
			if !ok {
				return 0, 0, false, nil
			}
			wallLo = max(wallLo, v)
			v, ok = temporalAdd(rawLo, emax)
			if !ok {
				return 0, 0, false, nil
			}
			wallHi = min(wallHi, v)
		} else {
			wallLo, wallHi = max(wallLo, lo), min(wallHi, hi)
		}
	}
	return lo, hi, true, nil
}
