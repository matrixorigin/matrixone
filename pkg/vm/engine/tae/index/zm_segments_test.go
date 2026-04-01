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

package index

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// makeSegment creates a ZoneMap segment [min, max] for int32 values.
func makeInt32Segment(min, max int32) []byte {
	zm := NewZM(types.T_int32, 0)
	UpdateZM(zm, types.EncodeInt32(&min))
	UpdateZM(zm, types.EncodeInt32(&max))
	return []byte(zm)
}

// makeObjZMInt32 creates an object-level ZoneMap [min, max] for int32.
func makeObjZMInt32(min, max int32) ZM {
	zm := NewZM(types.T_int32, 0)
	UpdateZM(zm, types.EncodeInt32(&min))
	UpdateZM(zm, types.EncodeInt32(&max))
	return zm
}

func TestAnySegmentOverlaps_EmptySegments(t *testing.T) {
	// No filter → match everything.
	objZM := makeObjZMInt32(10, 20)
	require.True(t, AnySegmentOverlaps(objZM, nil))
	require.True(t, AnySegmentOverlaps(objZM, [][]byte{}))
}

func TestAnySegmentOverlaps_UninitedObjZM(t *testing.T) {
	// Uninitialised object ZM → no match.
	objZM := NewZM(types.T_int32, 0)
	segments := [][]byte{makeInt32Segment(1, 10)}
	require.False(t, AnySegmentOverlaps(objZM, segments))
}

func TestAnySegmentOverlaps_SingleSegment(t *testing.T) {
	// Segment [10, 20].
	segments := [][]byte{makeInt32Segment(10, 20)}

	// Object fully inside segment.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(12, 18), segments))

	// Object overlapping left edge.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(5, 15), segments))

	// Object overlapping right edge.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(15, 25), segments))

	// Object fully containing segment.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(5, 25), segments))

	// Exact match.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(10, 20), segments))

	// Object entirely below.
	require.False(t, AnySegmentOverlaps(makeObjZMInt32(1, 9), segments))

	// Object entirely above.
	require.False(t, AnySegmentOverlaps(makeObjZMInt32(21, 30), segments))

	// Touching boundaries.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(1, 10), segments))
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(20, 30), segments))
}

func TestAnySegmentOverlaps_MultipleSegments(t *testing.T) {
	// Segments: [10, 20], [50, 60], [100, 200].
	segments := [][]byte{
		makeInt32Segment(10, 20),
		makeInt32Segment(50, 60),
		makeInt32Segment(100, 200),
	}

	// Object in gap between seg 0 and seg 1.
	require.False(t, AnySegmentOverlaps(makeObjZMInt32(25, 45), segments))

	// Object in gap between seg 1 and seg 2.
	require.False(t, AnySegmentOverlaps(makeObjZMInt32(65, 95), segments))

	// Object overlapping seg 0 only.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(5, 15), segments))

	// Object overlapping seg 1 only.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(55, 65), segments))

	// Object overlapping seg 2 only.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(150, 250), segments))

	// Object below all segments.
	require.False(t, AnySegmentOverlaps(makeObjZMInt32(1, 5), segments))

	// Object above all segments.
	require.False(t, AnySegmentOverlaps(makeObjZMInt32(300, 400), segments))

	// Object spanning from gap into seg 1.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(30, 55), segments))

	// Object spanning multiple segments.
	require.True(t, AnySegmentOverlaps(makeObjZMInt32(15, 150), segments))
}

func TestAnySegmentOverlaps_StringType(t *testing.T) {
	makeStrSegment := func(min, max string) []byte {
		zm := NewZM(types.T_varchar, 0)
		UpdateZM(zm, []byte(min))
		UpdateZM(zm, []byte(max))
		return []byte(zm)
	}
	makeStrObjZM := func(min, max string) ZM {
		zm := NewZM(types.T_varchar, 0)
		UpdateZM(zm, []byte(min))
		UpdateZM(zm, []byte(max))
		return zm
	}

	// Segments: ["apple", "banana"], ["mango", "orange"].
	segments := [][]byte{
		makeStrSegment("apple", "banana"),
		makeStrSegment("mango", "orange"),
	}

	// Object in first segment range.
	require.True(t, AnySegmentOverlaps(makeStrObjZM("avocado", "berry"), segments))

	// Object in gap.
	require.False(t, AnySegmentOverlaps(makeStrObjZM("cherry", "lemon"), segments))

	// Object in second segment range.
	require.True(t, AnySegmentOverlaps(makeStrObjZM("melon", "nectarine"), segments))

	// Object below all.
	require.False(t, AnySegmentOverlaps(makeStrObjZM("aaa", "ant"), segments))

	// Object above all.
	require.False(t, AnySegmentOverlaps(makeStrObjZM("peach", "plum"), segments))
}
