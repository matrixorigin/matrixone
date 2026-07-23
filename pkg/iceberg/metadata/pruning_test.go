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

package metadata

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/stretchr/testify/require"
)

func TestPruningHelperPartitionLookupAndAnyValueConversion(t *testing.T) {
	value, ok := lookupPartitionValue(map[string]any{" Month_Key ": int32(42)}, "month_key")
	require.True(t, ok)
	require.Equal(t, int32(42), value)

	_, ok = lookupPartitionValue(nil, "month_key")
	require.False(t, ok)
	_, ok = lookupPartitionValue(map[string]any{"other": 1}, "month_key")
	require.False(t, ok)

	type customInt int32
	type customUint uint32
	type customFloat float32

	intCases := []any{int(-1), int8(-2), int16(-3), int32(-4), int64(-5), uint(6), uint8(7), uint16(8), uint32(9), uint64(10), customInt(11), customUint(12)}
	for _, raw := range intCases {
		got, ok := pruneInt64FromAny(raw)
		require.Truef(t, ok, "raw=%T", raw)
		require.NotZero(t, got)
	}
	_, ok = pruneInt64FromAny(uint64(math.MaxInt64) + 1)
	require.False(t, ok)
	_, ok = pruneInt64FromAny("not-int")
	require.False(t, ok)

	floatCases := []any{float32(1.25), float64(2.5), customFloat(3.75)}
	for _, raw := range floatCases {
		got, ok := pruneFloat64FromAny(raw)
		require.Truef(t, ok, "raw=%T", raw)
		require.NotZero(t, got)
	}
	_, ok = pruneFloat64FromAny("not-float")
	require.False(t, ok)
}

func TestPruneValueFromAnyAndLiteralEdgeCases(t *testing.T) {
	got, ok := pruneValueFromAny(api.IcebergType{Kind: api.TypeLong}, int32(12))
	require.True(t, ok)
	require.Equal(t, pruneValue{kind: pruneValueInt64, i64: 12}, got)

	got, ok = pruneValueFromAny(api.IcebergType{Kind: api.TypeDouble}, float32(1.5))
	require.True(t, ok)
	require.Equal(t, pruneValueFloat64, got.kind)
	require.InDelta(t, 1.5, got.f64, 0.0001)

	got, ok = pruneValueFromAny(api.IcebergType{Kind: api.TypeString}, "ksa")
	require.True(t, ok)
	require.Equal(t, pruneValue{kind: pruneValueString, str: "ksa"}, got)

	got, ok = pruneValueFromAny(api.IcebergType{Kind: api.TypeTimestampTZ}, int64(123456))
	require.True(t, ok)
	require.Equal(t, pruneValue{kind: pruneValueInt64, i64: 123456}, got)

	for _, tc := range []struct {
		name string
		typ  api.IcebergType
		raw  any
	}{
		{name: "nil", typ: api.IcebergType{Kind: api.TypeLong}, raw: nil},
		{name: "nan", typ: api.IcebergType{Kind: api.TypeDouble}, raw: math.NaN()},
		{name: "string mismatch", typ: api.IcebergType{Kind: api.TypeString}, raw: 1},
		{name: "unsupported type", typ: api.IcebergType{Kind: api.TypeBoolean}, raw: 1},
	} {
		_, ok := pruneValueFromAny(tc.typ, tc.raw)
		require.Falsef(t, ok, tc.name)
	}

	_, ok = literalPruneValue(api.IcebergType{Kind: api.TypeLong}, api.PruneLiteral{Kind: api.TypeString, String: "bad"})
	require.False(t, ok)
	_, ok = literalPruneValue(api.IcebergType{Kind: api.TypeDouble}, api.PruneLiteral{Kind: api.TypeDouble, Float64: math.NaN()})
	require.False(t, ok)
	_, ok = literalPruneValue(api.IcebergType{Kind: api.TypeTimestampTZ}, api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: 1})
	require.False(t, ok)
	got, ok = literalPruneValue(api.IcebergType{Kind: api.TypeTimestampTZ}, api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: 1, Normalized: true})
	require.True(t, ok)
	require.Equal(t, pruneValue{kind: pruneValueInt64, i64: 1}, got)
}

func TestRangeAndBoundPruningEdgeCases(t *testing.T) {
	lit := pruneValue{kind: pruneValueInt64, i64: 10}
	lower := pruneValue{kind: pruneValueInt64, i64: 10}
	upper := pruneValue{kind: pruneValueInt64, i64: 10}

	require.True(t, rangePrunes(api.PruneOpGT, lit, &lower, &upper))
	require.False(t, partitionRangePrunes(api.PruneOpGT, lit, &lower, &upper, false))
	require.True(t, rangePrunes(api.PruneOpLT, lit, &lower, &upper))
	require.False(t, partitionRangePrunes(api.PruneOpLT, lit, &lower, &upper, false))
	require.False(t, rangePrunes(api.PruneOp("unknown"), lit, &lower, &upper))
	require.Zero(t, comparePruneValue(pruneValue{kind: pruneValueFloat64, f64: math.NaN()}, pruneValue{kind: pruneValueFloat64, f64: 1}))
	require.Zero(t, comparePruneValue(pruneValue{kind: pruneValueString, str: "a"}, lit))
	require.Nil(t, optionalPruneValue(pruneValue{}, false))
	require.NotNil(t, optionalPruneValue(lit, true))

	shortInt := []byte{1, 2, 3}
	_, ok := decodePruneBound(api.IcebergType{Kind: api.TypeInt}, shortInt)
	require.False(t, ok)
	_, ok = decodePruneBound(api.IcebergType{Kind: api.TypeLong}, make([]byte, 7))
	require.False(t, ok)

	floatNaN := make([]byte, 4)
	binary.LittleEndian.PutUint32(floatNaN, math.Float32bits(float32(math.NaN())))
	_, ok = decodePruneBound(api.IcebergType{Kind: api.TypeFloat}, floatNaN)
	require.False(t, ok)

	doubleNaN := make([]byte, 8)
	binary.LittleEndian.PutUint64(doubleNaN, math.Float64bits(math.NaN()))
	_, ok = decodePruneBound(api.IcebergType{Kind: api.TypeDouble}, doubleNaN)
	require.False(t, ok)

	_, ok = decodePruneBound(api.IcebergType{Kind: api.TypeBoolean}, []byte{1})
	require.False(t, ok)
}
