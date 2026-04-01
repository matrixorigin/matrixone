// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestSegmentBuilder_SingleValue(t *testing.T) {
	pkType := types.T_int32.ToType()
	sb := newSegmentBuilder(pkType)

	v := int32(42)
	sb.observe(types.EncodeInt32(&v))

	segments := sb.finalize()
	require.Len(t, segments, 1)

	zm := index.ZM(segments[0])
	require.True(t, zm.IsInited())
	require.Equal(t, types.T_int32, zm.GetType())
}

func TestSegmentBuilder_ConsecutiveValues_SingleSegment(t *testing.T) {
	// 1, 2, 3, ..., 10 — all close together, should be one segment.
	pkType := types.T_int64.ToType()
	sb := newSegmentBuilder(pkType)

	for i := int64(1); i <= 10; i++ {
		sb.observe(types.EncodeInt64(&i))
	}

	segments := sb.finalize()
	require.Len(t, segments, 1)

	zm := index.ZM(segments[0])
	min := types.DecodeInt64(zm.GetMinBuf())
	max := types.DecodeInt64(zm.GetMaxBuf())
	require.Equal(t, int64(1), min)
	require.Equal(t, int64(10), max)
}

func TestSegmentBuilder_LargeGap_SplitsSegment(t *testing.T) {
	// Values: 1, 2, 3, 4, 5, 1000000, 1000001, 1000002, 1000003, 1000004
	// The huge gap (5 → 1000000) should trigger a split after enough samples.
	pkType := types.T_int64.ToType()
	sb := newSegmentBuilder(pkType)

	// First cluster: 1-5
	for i := int64(1); i <= 5; i++ {
		sb.observe(types.EncodeInt64(&i))
	}
	// Second cluster: 1000000-1000004
	for i := int64(1000000); i <= 1000004; i++ {
		sb.observe(types.EncodeInt64(&i))
	}

	segments := sb.finalize()
	require.Equal(t, 2, len(segments), "expected 2 segments for 2 clusters")

	// Segment 0: [1, 5]
	zm0 := index.ZM(segments[0])
	require.Equal(t, int64(1), types.DecodeInt64(zm0.GetMinBuf()))
	require.Equal(t, int64(5), types.DecodeInt64(zm0.GetMaxBuf()))

	// Segment 1: [1000000, 1000004]
	zm1 := index.ZM(segments[1])
	require.Equal(t, int64(1000000), types.DecodeInt64(zm1.GetMinBuf()))
	require.Equal(t, int64(1000004), types.DecodeInt64(zm1.GetMaxBuf()))
}

func TestSegmentBuilder_StringType_CountBased(t *testing.T) {
	// String types use count-based splitting (maxSegmentSize cap).
	// With only a few values, should be one segment.
	pkType := types.T_varchar.ToType()
	sb := newSegmentBuilder(pkType)

	sb.observe([]byte("apple"))
	sb.observe([]byte("banana"))
	sb.observe([]byte("cherry"))
	sb.observe([]byte("date"))

	segments := sb.finalize()
	require.Len(t, segments, 1)

	zm := index.ZM(segments[0])
	require.Equal(t, types.T_varchar, zm.GetType())
}

func TestBuildSegmentsFromSortedVec_Int32(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int32.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	// Add values: 10, 20, 30 (already sorted, close together).
	for _, v := range []int32{10, 20, 30} {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}

	segments := buildSegmentsFromSortedVec(vec, pkType)
	require.NotEmpty(t, segments)

	// All values are close → single segment.
	// (minGapSample=4 means gap-based splitting needs at least 4 gaps,
	// but we only have 2 gaps, so no splitting occurs.)
	require.Len(t, segments, 1)
}

func TestBuildSegmentsFromSortedVec_Empty(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int64.ToType()
	vec := vector.NewVec(pkType)
	defer vec.Free(mp)

	segments := buildSegmentsFromSortedVec(vec, pkType)
	require.Nil(t, segments)
}

func TestBuildPKFilterFromVec_ProducesValidFilter(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	pkType := types.T_int64.ToType()
	vec := vector.NewVec(pkType)

	// Two clusters: [1..5] and [1000..1005]
	for i := int64(1); i <= 5; i++ {
		require.NoError(t, vector.AppendFixed(vec, i, false, mp))
	}
	for i := int64(1000); i <= 1005; i++ {
		require.NoError(t, vector.AppendFixed(vec, i, false, mp))
	}

	pkFilter := buildPKFilterFromVec(vec, pkType, 0)
	// vec is NOT freed inside buildPKFilterFromVec — caller is responsible.
	vec.Free(mp)

	require.NotNil(t, pkFilter)
	require.NotEmpty(t, pkFilter.Segments)
	require.Equal(t, 0, pkFilter.PrimarySeqnum)
}

func TestBuildPKFilterFromVec_NilVec(t *testing.T) {
	pkFilter := buildPKFilterFromVec(nil, types.T_int32.ToType(), 0)
	require.Nil(t, pkFilter)
}

func TestNumericGap_Int64(t *testing.T) {
	pkType := types.T_int64.ToType()
	a := int64(10)
	b := int64(50)
	gap := numericGap(types.EncodeInt64(&a), types.EncodeInt64(&b), pkType)
	require.InDelta(t, 40.0, gap, 0.001)
}

func TestNumericGap_Uint32(t *testing.T) {
	pkType := types.T_uint32.ToType()
	a := uint32(100)
	b := uint32(300)
	gap := numericGap(types.EncodeUint32(&a), types.EncodeUint32(&b), pkType)
	require.InDelta(t, 200.0, gap, 0.001)
}
