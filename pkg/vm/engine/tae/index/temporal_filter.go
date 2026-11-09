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
	"math"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type temporalRange struct {
	min, max types.Timestamp
}

// makeMixedTemporalMatcher returns the zone-map test used for mixed
// DATETIME/TIMESTAMP predicates. An unknown result must retain the block.
// The bound is the encoded scalar value in its SQL type; it is never a
// candidate index boundary. Scale is the TIMESTAMP precision used by the SQL
// comparison, which need not equal the DATETIME zone-map scale.
func makeMixedTemporalMatcher(columnType types.T, value []byte, valueType types.T, scale int32, zone *time.Location, op types.ComparisonOp) (func(ZM) ZoneMapMatch, bool) {
	if zone == nil || scale < 0 || scale > 6 || len(value) != 8 ||
		!((columnType == types.T_datetime && valueType == types.T_timestamp) || (columnType == types.T_timestamp && valueType == types.T_datetime)) {
		return nil, false
	}
	var scalar types.Timestamp
	if valueType == types.T_timestamp {
		scalar = types.DecodeTimestamp(value)
	} else {
		scalar = types.DecodeDatetime(value).ToTimestamp(zone).TruncateToScale(scale)
	}
	bound := temporalRange{min: scalar, max: scalar}
	var offsetMicros int64
	fixed := false
	// Local may initialize lazily. Do not discover it for an unevaluated
	// matcher, and never infer a fixed zone from just its current offset.
	if columnType == types.T_datetime && zone != time.Local {
		if offset, ok := types.FixedZoneOffset(zone); ok && int64(offset) >= math.MinInt32 && int64(offset) <= math.MaxInt32 {
			offsetMicros, fixed = int64(offset)*types.MicroSecsPerSec, true
		}
	}
	maxDatetime := types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999)
	return func(zm ZM) ZoneMapMatch {
		if !zm.Valid() || zm.GetType() != columnType {
			return ZoneMapMatch{}
		}
		low, high := zm.GetMinBuf(), zm.GetMaxBuf()
		if len(low) != 8 || len(high) != 8 {
			return ZoneMapMatch{}
		}
		var values temporalRange
		switch columnType {
		case types.T_timestamp:
			minimum, maximum := types.DecodeTimestamp(low), types.DecodeTimestamp(high)
			values = temporalRange{min: minimum, max: maximum}
		case types.T_datetime:
			minimum, maximum := types.DecodeDatetime(low), types.DecodeDatetime(high)
			if minimum == maximum {
				v := minimum.ToTimestamp(zone).TruncateToScale(scale)
				values = temporalRange{min: v, max: v}
			} else {
				// Noncanonical intervals may contain ZeroDatetime inside them,
				// whose scalar conversion cannot be bounded by their endpoints.
				if minimum < 0 || minimum > maximum || maximum > maxDatetime {
					return ZoneMapMatch{}
				}
				var minTS, maxTS types.Timestamp
				if fixed {
					// Canonical dates and int32-second offsets cannot overflow.
					minTS = types.Timestamp(int64(minimum) - offsetMicros)
					maxTS = types.Timestamp(int64(maximum) - offsetMicros)
				} else {
					var ok bool
					minTS, maxTS, ok = types.DatetimeRangeToTimestampRange(minimum, maximum, zone)
					if !ok {
						return ZoneMapMatch{}
					}
				}
				// -1 is preserved as ZeroTimestamp, while nearby negative
				// values can round to zero. Rounding endpoints is not monotone
				// across that sentinel; retain the block instead of pruning.
				if scale < 6 && minTS <= types.ZeroTimestamp && maxTS >= types.ZeroTimestamp {
					return ZoneMapMatch{}
				}
				values = temporalRange{min: minTS.TruncateToScale(scale), max: maxTS.TruncateToScale(scale)}
			}
		}
		switch op {
		case types.ComparisonEqual:
			return ZoneMapMatch{Match: values.max >= bound.min && values.min <= bound.max, Known: true}
		case types.ComparisonLess:
			return ZoneMapMatch{Match: values.min < bound.max, Known: true}
		case types.ComparisonLessEqual:
			return ZoneMapMatch{Match: values.min <= bound.max, Known: true}
		case types.ComparisonGreater:
			return ZoneMapMatch{Match: values.max > bound.min, Known: true}
		case types.ComparisonGreaterEqual:
			return ZoneMapMatch{Match: values.max >= bound.min, Known: true}
		default:
			return ZoneMapMatch{}
		}
	}, true
}
