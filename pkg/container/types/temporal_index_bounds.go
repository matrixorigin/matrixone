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

// TemporalOp compares the indexed component with the bound, in that order.
type TemporalOp uint8

const (
	TemporalEqual TemporalOp = iota
	TemporalLess
	TemporalLessEqual
	TemporalGreater
	TemporalGreaterEqual
)

// TemporalCandidate is a closed candidate interval and an optional zero point.
// DATETIME consumers must additionally include the physical tails outside the
// supported calendar domain. Full and Empty are mutually exclusive.
type TemporalCandidate struct {
	Lo, Hi   int64
	Sentinel int64
	Full     bool
	Empty    bool
}

func temporalFull() TemporalCandidate {
	return TemporalCandidate{Lo: math.MinInt64, Hi: math.MaxInt64, Sentinel: math.MinInt64, Full: true}
}

func temporalCompare(a, b int64, op TemporalOp) bool {
	switch op {
	case TemporalEqual:
		return a == b
	case TemporalLess:
		return a < b
	case TemporalLessEqual:
		return a <= b
	case TemporalGreater:
		return a > b
	case TemporalGreaterEqual:
		return a >= b
	default:
		return false
	}
}

func temporalAdd(a, b int64) (int64, bool) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, false
	}
	return a + b, true
}

// BuildTemporalCandidate builds a necessary physical bound for a mixed
// DATETIME/TIMESTAMP comparison. Unsupported calculations retain Full.
func BuildTemporalCandidate(ctx context.Context, keyKind T, timestampScale int32,
	bound int64, boundIsNull bool, op TemporalOp, zone *time.Location,
) (TemporalCandidate, error) {
	if err := ctx.Err(); err != nil {
		return TemporalCandidate{}, err
	}
	if boundIsNull {
		return TemporalCandidate{Empty: true}, nil
	}
	if timestampScale < 0 || timestampScale > 6 || op > TemporalGreaterEqual || zone == nil {
		return temporalFull(), nil
	}
	maxDatetime := int64(DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
	if keyKind == T_timestamp {
		if bound != int64(ZeroDatetime) && (bound < 0 || bound > maxDatetime) {
			return temporalFull(), nil
		}
		value := int64(Datetime(bound).ToTimestamp(zone).TruncateToScale(timestampScale))
		lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
		switch op {
		case TemporalEqual:
			lo, hi = value, value
		case TemporalLess:
			var ok bool
			hi, ok = temporalAdd(value, -1)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalLessEqual:
			hi = value
		case TemporalGreater:
			var ok bool
			lo, ok = temporalAdd(value, 1)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalGreaterEqual:
			lo = value
		}
		return TemporalCandidate{Lo: lo, Hi: hi, Sentinel: lo}, nil
	}
	if keyKind != T_datetime {
		return temporalFull(), nil
	}
	offset, fixed := FixedZoneOffset(zone)
	if fixed && (int64(offset) < math.MinInt32 || int64(offset) > math.MaxInt32) {
		return temporalFull(), nil
	}
	// These bounds cover the calendar domain only; the caller adds physical
	// out-of-calendar tails once per candidate group.
	q := int64(scaleTable[timestampScale])
	lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
	var ok bool
	if timestampScale == 6 {
		switch op {
		case TemporalEqual:
			lo, hi = bound, bound
		case TemporalLess:
			hi, ok = temporalAdd(bound, -1)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalLessEqual:
			hi = bound
		case TemporalGreater:
			lo, ok = temporalAdd(bound, 1)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalGreaterEqual:
			lo = bound
		}
	} else if bound > q {
		floor := bound / q * q
		ceil := floor
		if bound%q != 0 {
			ceil, ok = temporalAdd(floor, q)
			if !ok {
				return temporalFull(), nil
			}
		}
		half := q / 2
		switch op {
		case TemporalEqual:
			if bound%q != 0 {
				return TemporalCandidate{Empty: true}, nil
			}
			lo, ok = temporalAdd(bound, -half)
			if !ok {
				return temporalFull(), nil
			}
			hi, ok = temporalAdd(bound, half-1)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalLess:
			hi, ok = temporalAdd(ceil, -half-1)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalLessEqual:
			hi, ok = temporalAdd(floor, half-1)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalGreater:
			lo, ok = temporalAdd(floor, half)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalGreaterEqual:
			lo, ok = temporalAdd(ceil, -half)
			if !ok {
				return temporalFull(), nil
			}
		}
	} else {
		switch op {
		case TemporalEqual:
			lo, ok = temporalAdd(bound, -q)
			if !ok {
				return temporalFull(), nil
			}
			hi, ok = temporalAdd(bound, q)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalLess, TemporalLessEqual:
			hi, ok = temporalAdd(bound, q)
			if !ok {
				return temporalFull(), nil
			}
		case TemporalGreater, TemporalGreaterEqual:
			lo, ok = temporalAdd(bound, -q)
			if !ok {
				return temporalFull(), nil
			}
		}
	}
	if fixed {
		offsetMicros := int64(offset) * MicroSecsPerSec
		if lo != math.MinInt64 {
			lo, ok = temporalAdd(lo, offsetMicros)
			if !ok {
				return temporalFull(), nil
			}
		}
		if hi != math.MaxInt64 {
			hi, ok = temporalAdd(hi, offsetMicros)
			if !ok {
				return temporalFull(), nil
			}
		}
		lo = max(lo, 0)
		hi = min(hi, maxDatetime)
	} else {
		var err error
		lo, hi, ok, err = temporalNamedZoneRange(ctx, zone, lo, hi, maxDatetime)
		if err != nil {
			return TemporalCandidate{}, err
		}
		if !ok {
			return temporalFull(), nil
		}
	}
	if lo > hi {
		lo, hi = 0, 0
	}
	sentinel := lo
	if temporalCompare(int64(ZeroTimestamp), bound, op) {
		sentinel = int64(ZeroDatetime)
	}
	return TemporalCandidate{Lo: lo, Hi: hi, Sentinel: sentinel}, nil
}
