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

package index

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// MakeScalarZoneMapMatcher proves a scalar comparison in its SQL domain before
// applying it to persisted bounds. Both readers and optional cost observations
// use this owner; an unknown match never authorizes pruning or a cost credit.
// The returned bound is the one decimal carrier shared by match, seek and bloom.
func MakeScalarZoneMapMatcher(columnType types.Type, value []byte, valueType types.Type, comparisonScale int32, zone *time.Location, op types.ComparisonOp) (func(ZM) ZoneMapMatch, ZM, bool) {
	if op > types.ComparisonGreaterEqual {
		return nil, nil, false
	}
	if !RawScalarZoneMapTypesCompatible(columnType.Oid, valueType.Oid) {
		match, ok := makeMixedTemporalMatcher(columnType.Oid, value, valueType.Oid, comparisonScale, zone, op)
		return match, nil, ok
	}
	kind := columnType.Oid
	if kind == types.T_decimal256 || types.IsUnicodeCollation(columnType.Charset) || types.IsUnicodeCollation(valueType.Charset) ||
		kind.FixedLength() > 0 && len(value) != kind.FixedLength() {
		return nil, nil, false
	}
	var bound ZM
	if kind.IsDecimal() {
		bound = NewZM(kind, valueType.Scale)
		UpdateZM(bound, value)
	}
	switch op {
	case types.ComparisonEqual:
		if kind == types.T_varbinary && valueType.Oid == kind {
			// Both operands use byte equality, so the stored minimum remains
			// useful even if the maximum cannot exclude a value.
			return func(zm ZM) ZoneMapMatch {
				if !RawZoneMapComparable(zm, kind) {
					return ZoneMapMatch{}
				}
				return ZoneMapMatch{Match: zm.containsString(value, true), Known: true}
			}, bound, true
		}
		return func(zm ZM) ZoneMapMatch { return IntersectsBound(zm, value, bound, kind) }, bound, true
	case types.ComparisonLess:
		return func(zm ZM) ZoneMapMatch { return AnyLTByBound(zm, value, bound, kind) }, bound, true
	case types.ComparisonLessEqual:
		return func(zm ZM) ZoneMapMatch { return AnyLEByBound(zm, value, bound, kind) }, bound, true
	case types.ComparisonGreater:
		return func(zm ZM) ZoneMapMatch { return AnyGTByBound(zm, value, bound, kind) }, bound, true
	default: // ComparisonGreaterEqual; invalid operators were rejected above.
		return func(zm ZM) ZoneMapMatch { return AnyGEByBound(zm, value, bound, kind) }, bound, true
	}
}

// RawScalarZoneMapTypesCompatible identifies byte-compatible scalar carriers.
// CHAR/VARCHAR/TEXT/binary bounds share the reader's existing string carrier;
// collation admission is separate. Other cross-type pairs require conversion.
func RawScalarZoneMapTypesCompatible(column, value types.T) bool {
	return column == value || column.IsMySQLString() && value.IsMySQLString()
}

// ZoneMapMatch distinguishes a proved result from an unsupported comparison.
// Unknown results retain blocks in the reader and give no lower-cost credit.
type ZoneMapMatch struct {
	Match bool
	Known bool
}

func (m ZoneMapMatch) MayMatch() bool {
	return !m.Known || m.Match
}

func (m ZoneMapMatch) Excludes() bool {
	return m.Known && !m.Match
}

func (m ZoneMapMatch) And(other ZoneMapMatch) ZoneMapMatch {
	if m.Excludes() || other.Excludes() {
		return ZoneMapMatch{Known: true}
	}
	if m.Known && other.Known {
		return ZoneMapMatch{Match: true, Known: true}
	}
	return ZoneMapMatch{}
}

// Or is known true if either input is known true; a known false result needs
// both inputs to be known false.
func (m ZoneMapMatch) Or(other ZoneMapMatch) ZoneMapMatch {
	if m.Known && m.Match || other.Known && other.Match {
		return ZoneMapMatch{Match: true, Known: true}
	}
	if m.Known && other.Known {
		return ZoneMapMatch{Known: true}
	}
	return ZoneMapMatch{}
}

func RawZoneMapComparable(zm ZM, columnType types.T) bool {
	return columnType != types.T_json && zm.IsInited() && zm.GetType() == columnType
}

func AnyLTByBound(
	zm ZM, value []byte, bound ZM, columnType types.T,
) ZoneMapMatch {
	if bound == nil {
		if !RawZoneMapComparable(zm, columnType) {
			return ZoneMapMatch{}
		}
		return ZoneMapMatch{Match: zm.AnyLTByValue(value), Known: true}
	}
	result, ok := zm.AnyLT(bound)
	return ZoneMapMatch{Match: result, Known: ok}
}

func AnyLEByBound(
	zm ZM, value []byte, bound ZM, columnType types.T,
) ZoneMapMatch {
	if bound == nil {
		if !RawZoneMapComparable(zm, columnType) {
			return ZoneMapMatch{}
		}
		return ZoneMapMatch{Match: zm.AnyLEByValue(value), Known: true}
	}
	result, ok := zm.AnyLE(bound)
	return ZoneMapMatch{Match: result, Known: ok}
}

func AnyGTByBound(
	zm ZM, value []byte, bound ZM, columnType types.T,
) ZoneMapMatch {
	if bound == nil {
		if !RawZoneMapComparable(zm, columnType) {
			return ZoneMapMatch{}
		}
		return ZoneMapMatch{Match: zm.AnyGTByValue(value), Known: true}
	}
	result, ok := zm.AnyGT(bound)
	return ZoneMapMatch{Match: result, Known: ok}
}

func AnyGEByBound(
	zm ZM, value []byte, bound ZM, columnType types.T,
) ZoneMapMatch {
	if bound == nil {
		if !RawZoneMapComparable(zm, columnType) {
			return ZoneMapMatch{}
		}
		return ZoneMapMatch{Match: zm.AnyGEByValue(value), Known: true}
	}
	result, ok := zm.AnyGE(bound)
	return ZoneMapMatch{Match: result, Known: ok}
}

func IntersectsBound(
	zm ZM, value []byte, bound ZM, columnType types.T,
) ZoneMapMatch {
	if bound == nil {
		if !RawZoneMapComparable(zm, columnType) {
			return ZoneMapMatch{}
		}
		return ZoneMapMatch{Match: zm.ContainsKey(value), Known: true}
	}
	result, ok := zm.Intersect(bound)
	return ZoneMapMatch{Match: result, Known: ok}
}

func AnyBetweenBounds(
	zm ZM,
	lowerValue, upperValue []byte,
	lowerBound, upperBound ZM,
	columnType types.T,
) ZoneMapMatch {
	if lowerBound == nil {
		if !RawZoneMapComparable(zm, columnType) {
			return ZoneMapMatch{}
		}
		return ZoneMapMatch{Match: zm.Between(lowerValue, upperValue), Known: true}
	}
	result, ok := zm.AnyBetween(lowerBound, upperBound)
	return ZoneMapMatch{Match: result, Known: ok}
}

func InRangeBounds(
	zm ZM,
	lowerValue, upperValue []byte,
	lowerBound, upperBound ZM,
	hint uint8,
	columnType types.T,
) ZoneMapMatch {
	switch hint {
	case 1: // (lb, ub]
		return AnyGTByBound(zm, lowerValue, lowerBound, columnType).
			And(AnyLEByBound(zm, upperValue, upperBound, columnType))
	case 2: // [lb, ub)
		return AnyGEByBound(zm, lowerValue, lowerBound, columnType).
			And(AnyLTByBound(zm, upperValue, upperBound, columnType))
	case 3: // (lb, ub)
		return AnyGTByBound(zm, lowerValue, lowerBound, columnType).
			And(AnyLTByBound(zm, upperValue, upperBound, columnType))
	default: // [lb, ub]
		return AnyGEByBound(zm, lowerValue, lowerBound, columnType).
			And(AnyLEByBound(zm, upperValue, upperBound, columnType))
	}
}

func ZoneMapVectorComparable(
	zm ZM, vec *vector.Vector, columnType types.T,
) bool {
	if !RawZoneMapComparable(zm, columnType) || vec == nil || vec.GetType().Oid != columnType {
		return false
	}
	return !columnType.IsDecimal() || zm.GetScale() == vec.GetType().Scale
}

func AnyInVector(
	zm ZM, vec *vector.Vector, columnType types.T,
) ZoneMapMatch {
	if !ZoneMapVectorComparable(zm, vec, columnType) {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{Match: zm.AnyIn(vec), Known: true}
}

func PrefixEqByValue(zm ZM, value []byte, columnType types.T) ZoneMapMatch {
	if !RawZoneMapComparable(zm, columnType) {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{Match: zm.PrefixEq(value), Known: true}
}

func PrefixBetweenByValue(
	zm ZM, lower, upper []byte, columnType types.T,
) ZoneMapMatch {
	if !RawZoneMapComparable(zm, columnType) {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{Match: zm.PrefixBetween(lower, upper), Known: true}
}

func PrefixInRangeByValue(
	zm ZM, lower, upper []byte, hint uint8, columnType types.T,
) ZoneMapMatch {
	if !RawZoneMapComparable(zm, columnType) {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{Match: zm.PrefixInRange(lower, upper, hint), Known: true}
}

func PrefixInVector(
	zm ZM, vec *vector.Vector, columnType types.T,
) ZoneMapMatch {
	if !ZoneMapVectorComparable(zm, vec, columnType) {
		return ZoneMapMatch{}
	}
	if vec.IsConstNull() || vec.GetNulls().Any() {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{Match: zm.PrefixIn(vec), Known: true}
}

func AnyPrefixLTByValue(zm ZM, value []byte, columnType types.T) ZoneMapMatch {
	if !RawZoneMapComparable(zm, columnType) {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{
		Match: types.PrefixCompare(zm.GetMinBuf(), value) < 0,
		Known: true,
	}
}

func AnyPrefixLEByValue(zm ZM, value []byte, columnType types.T) ZoneMapMatch {
	if !RawZoneMapComparable(zm, columnType) {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{
		Match: types.PrefixCompare(zm.GetMinBuf(), value) <= 0,
		Known: true,
	}
}

func AnyPrefixGTByValue(zm ZM, value []byte, columnType types.T) ZoneMapMatch {
	if !RawZoneMapComparable(zm, columnType) {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{
		Match: zm.prefixMaxCompare(value) > 0,
		Known: true,
	}
}

func AnyPrefixGEByValue(zm ZM, value []byte, columnType types.T) ZoneMapMatch {
	if !RawZoneMapComparable(zm, columnType) {
		return ZoneMapMatch{}
	}
	return ZoneMapMatch{
		Match: zm.prefixMaxCompare(value) >= 0,
		Known: true,
	}
}
