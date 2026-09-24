// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextValueInRangeAgainstEnumeration(t *testing.T) {
	check := func(from, to, step, increment, offset uint64) {
		t.Helper()
		var want uint64
		var found bool
		for v := from; v < to; {
			if v >= offset && (v-offset)%increment == 0 {
				want, found = v, true
				break
			}
			if v > math.MaxUint64-step {
				break
			}
			v += step
		}
		value, _, ok := nextValueInRange(from, to, step, increment, offset)
		if ok != found || ok && value != want {
			t.Fatalf("range=[%d,%d) step=%d series=%d/%d: got (%d,%t), want (%d,%t)",
				from, to, step, increment, offset, value, ok, want, found)
		}
	}
	for from := uint64(1); from <= 12; from++ {
		for step := uint64(1); step <= 4; step++ {
			for increment := uint64(1); increment <= 9; increment++ {
				for offset := uint64(1); offset <= increment; offset++ {
					check(from, 24, step, increment, offset)
					check(from, from, step, increment, offset)
				}
			}
		}
	}
	for _, increment := range []uint64{3, 64, 65535} {
		for _, offset := range []uint64{1, 2, increment} {
			check(math.MaxUint64-8, math.MaxUint64, 1, increment, offset)
		}
	}
}

func TestRangeCount(t *testing.T) {
	r := &ranges{step: 1, values: []uint64{1, 2, 2, 3, 3, 4}}
	assert.Equal(t, 3, r.rangeCount())

	r = &ranges{step: 2, values: []uint64{1, 3, 3, 5, 5, 7}}
	assert.Equal(t, 3, r.rangeCount())
}

func TestRangeEmpty(t *testing.T) {
	r := &ranges{}
	assert.True(t, r.empty())

	r = &ranges{step: 2, values: []uint64{3, 3, 5, 5, 7, 7}}
	assert.True(t, r.empty())

	r = &ranges{step: 2, values: []uint64{1, 3}}
	assert.False(t, r.empty())

	r = &ranges{step: 2, values: []uint64{3, 3, 5, 5, 7, 9}}
	assert.False(t, r.empty())
}

func TestRangeNext(t *testing.T) {
	r := &ranges{}
	assert.Equal(t, uint64(0), r.next())

	r = &ranges{step: 1, values: []uint64{2, 2}}
	assert.Equal(t, uint64(0), r.next())

	r = &ranges{step: 1, values: []uint64{1, 2}}
	assert.Equal(t, uint64(1), r.next())
	assert.Equal(t, 0, len(r.values))

	r = &ranges{step: 1, values: []uint64{2, 2, 2, 3}}
	assert.Equal(t, uint64(2), r.next())
	assert.Equal(t, 0, len(r.values))

	r = &ranges{step: 1, values: []uint64{1000, 1001}}
	assert.Equal(t, uint64(1000), r.next())
	assert.Equal(t, 0, len(r.values))
}

func TestRangeNextForStatementSeries(t *testing.T) {
	tests := []struct {
		name      string
		step      uint64
		values    []uint64
		increment uint64
		offset    uint64
		want      []uint64
	}{
		{
			name:      "unit range selects offset residue",
			step:      1,
			values:    []uint64{1, 10},
			increment: 3,
			offset:    2,
			want:      []uint64{2, 5, 8},
		},
		{
			name:      "unit range selects first residue",
			step:      1,
			values:    []uint64{1, 10},
			increment: 3,
			offset:    1,
			want:      []uint64{1, 4, 7},
		},
		{
			name:      "non unit range uses congruence",
			step:      2,
			values:    []uint64{1, 15},
			increment: 4,
			offset:    3,
			want:      []uint64{3, 7, 11},
		},
		{
			name:      "incompatible residues are discarded",
			step:      2,
			values:    []uint64{1, 15},
			increment: 4,
			offset:    2,
			want:      []uint64{},
		},
		{
			name:      "later range remains usable",
			step:      2,
			values:    []uint64{1, 4, 11, 18},
			increment: 3,
			offset:    2,
			want:      []uint64{11, 17},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ranges{step: tt.step, values: append([]uint64(nil), tt.values...)}
			options := NormalizeAutoIncrementOptions(tt.increment, tt.offset)
			got := make([]uint64, 0, len(tt.want))
			for {
				value := r.nextFor(options)
				if value == 0 {
					break
				}
				got = append(got, value)
			}
			require.Equal(t, tt.want, got)
			require.True(t, r.empty())
			require.Equal(t, tt.step, r.step)
		})
	}
}

func TestRangeNextForZeroAndInvalidOptionsNormalizeSafely(t *testing.T) {
	r := &ranges{step: 1, values: []uint64{1, 4}}
	// A zero increment and an offset outside the series are normalized to the
	// ordinary MySQL default instead of turning the range into an infinite or
	// invalid arithmetic path.
	options := NormalizeAutoIncrementOptions(0, 99)
	require.Equal(t, AutoIncrementOptions{Increment: 1, Offset: 1}, options)
	require.Equal(t, uint64(1), r.nextFor(options))
	require.Equal(t, uint64(2), r.nextFor(options))
}

func TestRangeLeft(t *testing.T) {
	r := &ranges{}
	assert.Equal(t, 0, r.left())

	r = &ranges{step: 2, values: []uint64{3, 3, 5, 5, 7, 7}}
	assert.Equal(t, 0, r.left())

	r = &ranges{step: 2, values: []uint64{1, 3}}
	assert.Equal(t, 1, r.left())

	r = &ranges{step: 2, values: []uint64{3, 3, 5, 7, 7, 9}}
	assert.Equal(t, 2, r.left())
}

func TestRangeAdd(t *testing.T) {
	r := &ranges{step: 1}
	r.add(1, 2)
	assert.Equal(t, 1, r.left())
	assert.Equal(t, 2, len(r.values))

	r.add(2, 3)
	assert.Equal(t, 2, r.left())
	assert.Equal(t, 4, len(r.values))

	r.add(1, 2)
	assert.Equal(t, 2, r.left())
	assert.Equal(t, 4, len(r.values))
}

func TestRangeAllocateTimestampTracksMutations(t *testing.T) {
	ts1 := timestamp.Timestamp{PhysicalTime: 1000, LogicalTime: 1}
	ts2 := timestamp.Timestamp{PhysicalTime: 2000, LogicalTime: 2}
	r := &ranges{step: 1}
	r.addWithTimestamp(1, 5, ts1)
	r.addWithTimestamp(5, 9, ts2)

	skipped := &ranges{step: 1}
	r.setManual(2, skipped)
	require.Equal(t, []uint64{3, 5, 5, 9}, r.values)
	require.Equal(t, []timestamp.Timestamp{ts1, ts2}, r.allocatedAt)
	require.Equal(t, ts1, r.oldestAllocateAt())

	require.True(t, r.updateTo(5))
	require.Equal(t, []uint64{5, 9}, r.values)
	require.Equal(t, []timestamp.Timestamp{ts2}, r.allocatedAt)
	require.Equal(t, ts2, r.oldestAllocateAt())

	for value := uint64(5); value < 9; value++ {
		require.Equal(t, value, r.next())
	}
	require.True(t, r.oldestAllocateAt().IsEmpty())
	require.Empty(t, r.allocatedAt)
}

func TestSetManualReusesRangeBackingStorage(t *testing.T) {
	values := make([]uint64, 4, 4)
	copy(values, []uint64{2, 5, 5, 9})
	allocatedAt := make([]timestamp.Timestamp, 2, 2)
	copy(allocatedAt, []timestamp.Timestamp{
		{PhysicalTime: 1000, LogicalTime: 1},
		{PhysicalTime: 2000, LogicalTime: 2},
	})

	r := ranges{
		step:        1,
		values:      values,
		allocatedAt: allocatedAt,
	}
	skipped := &ranges{step: 1}
	require.Zero(t, testing.AllocsPerRun(1000, func() {
		r.values = values
		r.allocatedAt = allocatedAt
		r.setManual(1, skipped)
	}))
}

func TestUpdateTo(t *testing.T) {
	cases := []struct {
		values   []uint64
		updateTo uint64
		expected []uint64
		contains bool
	}{
		{
			values:   []uint64{},
			updateTo: 1,
			contains: false,
			expected: []uint64{},
		},
		{
			values:   []uint64{1, 2},
			updateTo: 1,
			contains: true,
			expected: []uint64{1, 2},
		},
		{
			values:   []uint64{1, 3},
			updateTo: 2,
			contains: true,
			expected: []uint64{2, 3},
		},
		{
			values:   []uint64{1, 3},
			updateTo: 3,
			contains: false,
			expected: []uint64{},
		},
		{
			values:   []uint64{1, 2, 2, 4},
			updateTo: 2,
			contains: true,
			expected: []uint64{2, 4},
		},
		{
			values:   []uint64{1, 2, 2, 4},
			updateTo: 3,
			contains: true,
			expected: []uint64{3, 4},
		},
		{
			values:   []uint64{1, 2, 2, 4},
			updateTo: 4,
			contains: false,
			expected: []uint64{},
		},
		{
			values:   []uint64{101, 1001},
			updateTo: 100,
			contains: true,
			expected: []uint64{101, 1001},
		},
	}

	for _, c := range cases {
		r := &ranges{values: c.values, step: 1}
		require.Equal(t, c.contains, r.updateTo(c.updateTo))
		assert.Equal(t, c.expected, r.values)
	}
}

func TestSetManual(t *testing.T) {
	cases := []struct {
		values          []uint64
		manual          uint64
		expectedSkipped []uint64
		expectLeft      []uint64
	}{
		{
			values:          []uint64{},
			manual:          1,
			expectedSkipped: []uint64(nil),
			expectLeft:      []uint64{},
		},
		{
			values:          []uint64{2, 3},
			manual:          1,
			expectedSkipped: []uint64(nil),
			expectLeft:      []uint64{2, 3},
		},
		{
			values:          []uint64{2, 3},
			manual:          2,
			expectedSkipped: []uint64(nil),
			expectLeft:      []uint64{},
		},
		{
			values:          []uint64{1, 3},
			manual:          2,
			expectedSkipped: []uint64{1, 2},
			expectLeft:      []uint64{},
		},
		{
			values:          []uint64{1, 4},
			manual:          2,
			expectedSkipped: []uint64{1, 2},
			expectLeft:      []uint64{3, 4},
		},
		{
			values:          []uint64{1, 4},
			manual:          3,
			expectedSkipped: []uint64{1, 3},
			expectLeft:      []uint64{},
		},
		{
			values:          []uint64{1, 4},
			manual:          4,
			expectedSkipped: []uint64{1, 4},
			expectLeft:      []uint64{},
		},
		{
			values:          []uint64{1, 3},
			manual:          4,
			expectedSkipped: []uint64{1, 3},
			expectLeft:      []uint64{},
		},
	}

	for _, c := range cases {
		r := &ranges{values: c.values, step: 1}
		skipped := &ranges{}
		r.setManual(c.manual, skipped)
		require.Equal(t, c.expectedSkipped, skipped.values)
		assert.Equal(t, c.expectLeft, r.values)
	}
}
