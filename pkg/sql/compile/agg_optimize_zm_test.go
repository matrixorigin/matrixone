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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func zmOf(t types.T, vals ...[]byte) objectio.ZoneMap {
	zm := index.NewZM(t, 0)
	for _, v := range vals {
		index.UpdateZM(zm, v)
	}
	return zm
}

// TestMergeGroupCodecCoversZoneMapTypes round-trips, through
// EncodeMergeGroup/DecodeMergeGroup, a partial result of every fixed-size
// type the zone map records, so a type the metadata MIN/MAX path can produce
// cannot be dropped on its way to a remote merge (it was for T_year and the
// low-precision floats).
func TestMergeGroupCodecCoversZoneMapTypes(t *testing.T) {
	covered := 0
	for _, oid := range types.AllOids() {
		typ, ok := oid.TryToType()
		if !ok || !typ.IsFixedLen() || typ.TypeSize() <= 0 {
			continue
		}
		n := typ.TypeSize()
		buf := make([]byte, n)
		for i := range buf {
			buf[i] = byte(i + 1)
		}
		buf[0] = 1 // a valid bool
		zm := zmOf(oid, buf)
		if !zm.IsInited() {
			continue // no zone map kept for the type (decimal256)
		}
		val := zm.GetMin()
		require.NotNil(t, val, oid.String())

		merge := &group.MergeGroup{
			PartialResults:     []any{val, nil, int64(7)},
			PartialResultTypes: []types.T{oid, types.T_any, types.T_int64},
		}
		pipe := &pipeline.Group{}
		require.NotPanics(t, func() { EncodeMergeGroup(merge, pipe) }, oid.String())
		got := &group.MergeGroup{}
		DecodeMergeGroup(got, pipe)
		require.Equal(t, merge.PartialResultTypes, got.PartialResultTypes, oid.String())
		require.Equal(t, merge.PartialResults, got.PartialResults, oid.String())
		covered++
	}
	require.Greater(t, covered, 25)
}

func TestMergeZoneMapPartialYear(t *testing.T) {
	y := func(v int16) []byte { return types.EncodeFixed(types.MoYear(v)) }
	blocks := []objectio.ZoneMap{
		zmOf(types.T_year, y(2001), y(2005)),
		zmOf(types.T_year, y(1999), y(2003)),
		zmOf(types.T_year, y(2002), y(2010)),
	}
	var minV, maxV any
	var minT, maxT types.T
	for _, zm := range blocks {
		require.NoError(t, mergeZoneMapPartial(true, zm, 2, 0, &minV, &minT))
		require.NoError(t, mergeZoneMapPartial(false, zm, 2, 0, &maxV, &maxT))
	}
	require.Equal(t, types.MoYear(1999), minV)
	require.Equal(t, types.MoYear(2010), maxV)
	require.Equal(t, types.T_year, minT)
	require.Equal(t, types.T_year, maxT)
}

func TestMergeZoneMapPartialSigned(t *testing.T) {
	v := func(x int32) []byte { return types.EncodeFixed(x) }
	var minV, maxV any
	var minT, maxT types.T
	for _, zm := range []objectio.ZoneMap{
		zmOf(types.T_int32, v(-3), v(4)),
		zmOf(types.T_int32, v(-10), v(-5)),
		zmOf(types.T_int32, v(0), v(9)),
	} {
		require.NoError(t, mergeZoneMapPartial(true, zm, 2, 0, &minV, &minT))
		require.NoError(t, mergeZoneMapPartial(false, zm, 2, 0, &maxV, &maxT))
	}
	require.Equal(t, int32(-10), minV)
	require.Equal(t, int32(9), maxV)
}

// TestMergeZoneMapPartialUninitialized: an all-NULL block has no zone map and
// contributes nothing; a block with values but no zone map declines the
// metadata path instead of contributing a wrong value.
func TestMergeZoneMapPartialUninitialized(t *testing.T) {
	var p any
	var pt types.T
	empty := index.NewZM(types.T_year, 0)
	require.NoError(t, mergeZoneMapPartial(true, empty, 4, 4, &p, &pt))
	require.Nil(t, p)
	require.Equal(t, types.T_any, pt)

	require.Error(t, mergeZoneMapPartial(true, empty, 4, 1, &p, &pt))

	require.Error(t, mergeZoneMapPartial(true, index.NewZM(types.T_varchar, 0), 1, 0, &p, &pt))
}

// TestMergeGroupCodecKeepsPositions: a nil partial result between two values
// must not shift the later value onto the earlier aggregate's slot.
func TestMergeGroupCodecKeepsPositions(t *testing.T) {
	merge := &group.MergeGroup{
		PartialResults:     []any{int64(3), nil, types.MoYear(2001)},
		PartialResultTypes: []types.T{types.T_int64, types.T_any, types.T_year},
	}
	pipe := &pipeline.Group{}
	EncodeMergeGroup(merge, pipe)
	got := &group.MergeGroup{}
	DecodeMergeGroup(got, pipe)
	require.Equal(t, merge.PartialResults, got.PartialResults)
	require.Equal(t, merge.PartialResultTypes, got.PartialResultTypes)
}
