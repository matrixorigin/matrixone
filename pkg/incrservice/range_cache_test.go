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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
